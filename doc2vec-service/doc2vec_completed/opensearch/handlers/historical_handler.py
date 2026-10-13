from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload, load_only

from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    HistoricalResult,
    NextQueueMessage,
    TspId,
)
from doc2vec_completed.opensearch.errors import QueueSendError
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)
from ta_db_connection.atables import SentencesVector, TextSimilarityInput
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class HistoricalHandler:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch: OpenSearchHelper,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Save the tools used to process one historical document."""

        self.config = config
        self.session_factory = session_factory
        self.opensearch = opensearch
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.batch_processor = SentenceBatchProcessor(
            config,
            session_factory,
            opensearch,
            custom_logger,
        )

    def process(self, tsp_id: TspId) -> HistoricalResult | None:
        """Move one document's saved sentences into OpenSearch.

        Input: tsp_id=1040.
        Output: sentence counts after every row is indexed, or None on failure.
        """

        self.logger.info("Sentence processing started.")
        total = retry_operation(
            "Count document sentences",
            self._count,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            self.logger.error(
                "No sentences_vector rows are available for this document."
            )
            return None

        self.logger.info(f"Found {total} sentence(s) to process.")
        indexed = 0
        after_global_id = 0
        page_number = 0
        catalog_alias: str | None = None

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="sentence",
        ) as executor:
            while True:
                page_number += 1
                rows = retry_operation(
                    f"Load sentence page {page_number}",
                    self._load_page,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    break

                if catalog_alias is None:
                    catalog_alias = self.batch_processor.catalog_alias(
                        rows[0].tsp.analysis_group
                    )

                # Example: 3,000 rows become 100 jobs with 30 rows in each job.
                # The workers can run those small jobs at the same time.
                worker_batches = list(batches(rows, self.config.worker_batch_size))
                self.logger.info(
                    f"Processing sentence page {page_number} with {len(rows)} "
                    f"sentence(s) in {len(worker_batches)} worker batch(es); "
                    f"starting_global_id={rows[0].global_id}, "
                    f"ending_global_id={rows[-1].global_id}."
                )
                page_indexed = sum(executor.map(self._run_worker, worker_batches))
                indexed += page_indexed
                after_global_id = rows[-1].global_id
                self.logger.info(
                    f"Sentence page {page_number} completed. Indexed "
                    f"{page_indexed} sentence(s) from this page and {indexed} "
                    f"sentence(s) in total through global ID {after_global_id}."
                )

        if catalog_alias is None:
            self.logger.error(
                "The document sentences disappeared before they could be loaded."
            )
            return None

        # Refresh once after all writes. This makes the new sentences searchable
        # before Sentence Similarity receives its SQS message.
        retry_operation(
            "Refresh indexed sentences",
            self.opensearch.refresh_indices,
            catalog_alias,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        # Tell Sentence Similarity only after every sentence is in OpenSearch.
        retry_operation(
            "Send the completed document message",
            self._publish,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence processing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
        )

        return {
            "reason": "HISTORICAL",
            "tsp_id": tsp_id,
            "source_count": total,
            "indexed_count": indexed,
        }

    def _count(self, tsp_id: TspId) -> int:
        """Count the sentence rows that belong to one tsp_id.

        Input: tsp_id=1040. Output: 65 when the document has 65 sentences.
        """

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(SentencesVector.tsp_id == tsp_id)
            )
            return int(session.scalar(statement) or 0)

    def _load_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
    ) -> list[SentencesVector]:
        """Read the next page of sentences in global_id order.

        Input: tsp_id=1040 and after_global_id=3000.
        Output: rows whose global_id is greater than 3000.
        """

        # Example: 100 workers with 30 rows each load at most 3,000 rows.
        # This fills every worker without loading the whole document at once.
        page_size = self.config.worker_batch_size * self.config.thread_worker_count
        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    # Load only the fields used below. Do not load embedding_list
                    # because old rows may hold NumPy data that this flow cannot read.
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.tsp_id,
                        SentencesVector.sent_id_local,
                        SentencesVector.sentence_content,
                        SentencesVector.sentence_key,
                        SentencesVector.source_type,
                        SentencesVector.last_modified,
                    ),
                    joinedload(SentencesVector.tsp).load_only(
                        TextSimilarityInput.tsp_id,
                        TextSimilarityInput.application_id,
                        TextSimilarityInput.section_name,
                        TextSimilarityInput.analysis_group,
                        TextSimilarityInput.is_current,
                    ),
                )
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                )
                .order_by(SentencesVector.global_id)
                .limit(page_size)
            )
            return list(session.scalars(statement))

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Send one small sentence list to the batch processor."""

        label = (
            f"Worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )
        return self.batch_processor.process(rows, label)

    def _publish(self, tsp_id: TspId) -> None:
        """Send the tsp_id to Sentence Similarity after indexing is complete."""

        queue_url = self.config.sentence_similarity_backfill_queue_url
        if not queue_url:
            return

        message: NextQueueMessage = {
            "tsp_id": tsp_id,
            "reason": "HISTORICAL",
        }
        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=queue_url,
            message_body=message,
            is_fifo=queue_url.endswith(".fifo"),
        )
        if not sent:
            raise QueueSendError(
                "Failed to send the completed message to Sentence Similarity"
            )
