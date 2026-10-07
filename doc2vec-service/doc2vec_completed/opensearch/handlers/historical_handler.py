from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload, load_only

from doc2vec_completed.opensearch.AwsSQS import AwsSQS
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    HistoricalResult,
    NextQueueMessage,
    TspId,
)
from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.models import SentencesVector, TextSimilarityInput
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)


class HistoricalHandler:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch: OpenSearchHelper,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep everything required for one HISTORICAL message."""

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

    def process(self, tsp_id: TspId) -> HistoricalResult:
        """Index already-split historical sentences and send the next message."""

        self.logger.info("Historical sentence processing started.")
        self.logger.info("Counting historical sentences.")
        total = retry_operation(
            "Count historical sentences",
            self._count,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            raise PermanentDataError(
                "sentences_vector has no rows for the requested historical document"
            )

        self.logger.info(f"Found {total} historical sentence(s) to process.")
        indexed = 0
        after_global_id = 0
        page_number = 0
        catalog_alias: str | None = None

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="historical-sentence",
        ) as executor:
            while True:
                page_number += 1
                self.logger.info(
                    f"Loading historical page {page_number} after global ID "
                    f"{after_global_id}."
                )
                rows = retry_operation(
                    f"Load historical page {page_number}",
                    self._load_page,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    self.logger.info(
                        f"Historical page {page_number} is empty. All pages "
                        "have been loaded."
                    )
                    break

                self.logger.info(
                    f"Historical page {page_number} loaded {len(rows)} "
                    f"sentence(s), global IDs {rows[0].global_id} through "
                    f"{rows[-1].global_id}."
                )
                if catalog_alias is None:
                    catalog_alias = self.batch_processor.catalog_alias(
                        rows[0].tsp.analysis_group
                    )

                # Split this database page into small jobs. Each worker prepares
                # missing keys and writes its catalog and occurrence documents.
                worker_batches = list(batches(rows, self.config.worker_batch_size))
                self.logger.info(
                    f"Starting {len(worker_batches)} worker batch(es) for "
                    f"historical page {page_number}."
                )
                page_indexed = sum(executor.map(self._run_worker, worker_batches))
                indexed += page_indexed
                after_global_id = rows[-1].global_id
                self.logger.info(
                    f"Historical page {page_number} completed. Indexed "
                    f"{page_indexed} sentence(s) from this page and {indexed} "
                    f"sentence(s) in total through global ID {after_global_id}."
                )

        if catalog_alias is None:
            raise PermanentDataError(
                "the historical document did not provide an analysis-group catalog"
            )

        # All worker writes have returned. Refresh once here so Sentence
        # Similarity cannot receive the next message before these writes can be
        # found through normal OpenSearch search requests.
        retry_operation(
            "Refresh indexed historical sentences",
            self.opensearch.refresh_indices,
            catalog_alias,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        # Publish only after every historical sentence is safely indexed. The
        # SQS processor deletes the source message after this method returns.
        self.logger.info("Sending the completed historical message to the next queue.")
        retry_operation(
            "Send the completed historical message",
            self._publish,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info("Sent the completed historical message to the next queue.")
        self.logger.info(
            f"Historical sentence processing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
        )

        return {
            "reason": "HISTORICAL",
            "tsp_id": tsp_id,
            "source_count": total,
            "indexed_count": indexed,
        }

    def _count(self, tsp_id: TspId) -> int:
        """Count every historical sentence row for one document."""

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
        """Read the next historical page in stable global-ID order."""

        # Load enough rows to give every worker one full batch. This keeps all
        # workers busy without loading every sentence for the document at once.
        page_size = self.config.worker_batch_size * self.config.thread_worker_count
        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    # embedding_list and the other omitted columns are not used
                    # by this flow. Excluding them also avoids legacy NumPy data.
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
        """Retry one historical worker batch up to the configured limit."""

        label = (
            f"Historical worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )
        self.logger.info(f"{label} started with {len(rows)} sentence(s).")
        indexed_count = retry_operation(
            label,
            self.batch_processor.process,
            rows,
            label,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"{label} completed with {indexed_count} indexed sentence(s)."
        )
        return indexed_count

    def _publish(self, tsp_id: TspId) -> None:
        """Send the completed historical document to Sentence Similarity."""

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
            raise RuntimeError(
                "failed to send the historical message to Sentence Similarity"
            )
