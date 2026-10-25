from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    HistoricalResult,
    NextQueueMessage,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.errors import QueueSendError
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)
from ta_db_connection.atables import SentencesVector
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class HistoricalHandler:
    """Load historical sentence pages and coordinate their OpenSearch indexing."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_helper: OpenSearchHelper,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.opensearch_helper = opensearch_helper
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.embedding_db_store = EmbeddingDBStore(session_factory)
        self.batch_processor = SentenceBatchProcessor(
            config,
            self.sentence_vector_db_store,
            self.embedding_db_store,
            opensearch_helper,
            custom_logger,
        )

    def process(self, tsp_id: TspId) -> HistoricalResult | None:
        """Index every saved sentence for one historical document. Pages are
        processed concurrently, both indexes are refreshed, and Sentence
        Similarity is notified only after indexing completes.

        Input: tsp_id=1040
        Output: {"reason": "HISTORICAL", "tsp_id": 1040,
        "source_count": 65, "indexed_count": 65}

        The method returns None when the document has no sentence rows or its
        rows disappear before they can be loaded.
        """

        started_at = perf_counter()
        self.logger.info("Sentence processing started.")
        total = retry_operation(
            "Count document sentences",
            self.sentence_vector_db_store.count_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
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
            max_workers=constants.THREAD_WORKER_COUNT,
            thread_name_prefix="sentence",
        ) as executor:
            while True:
                page_number += 1
                rows = retry_operation(
                    f"Load sentence page {page_number}",
                    self.sentence_vector_db_store.load_historical_sentence_page,
                    tsp_id,
                    after_global_id,
                    constants.WORKER_BATCH_SIZE * constants.THREAD_WORKER_COUNT,
                    wait_seconds=constants.RETRY_SECONDS,
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
                worker_batches = list(batches(rows, constants.WORKER_BATCH_SIZE))
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
            self.opensearch_helper.refresh_aliases,
            [catalog_alias, constants.SENTENCE_OCCURRENCE_ALIAS],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        # Tell Sentence Similarity only after every sentence is in OpenSearch.
        retry_operation(
            "Send the completed document message",
            self._publish,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence processing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
        )
        self.logger.info(
            f"Sentence processing took {perf_counter() - started_at:.2f} seconds to complete."
        )

        return {
            "reason": "HISTORICAL",
            "tsp_id": tsp_id,
            "source_count": total,
            "indexed_count": indexed,
        }

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Send one sentence batch to the shared batch processor.

        Input: rows containing global IDs 3001 through 3030
        Output: 30 after every occurrence in the batch is indexed.
        """

        label = (
            f"Worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )
        return self.batch_processor.process(rows, label)

    def _publish(self, tsp_id: TspId) -> None:
        """Send the completed document to Sentence Similarity.

        Input: tsp_id=1040

        The queue receives {"reason": "HISTORICAL", "tsp_id": 1040}. When no
        backfill queue is configured, the method returns without sending.

        Raises:
            QueueSendError: If SQS does not accept the message.
        """

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
