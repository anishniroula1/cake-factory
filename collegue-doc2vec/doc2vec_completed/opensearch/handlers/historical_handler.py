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
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation, safe_error_summary
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)
from ta_db_connection.atatables import SentencesVector
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

        The method returns None when the document has no sentence records or
        those records disappear before they can be loaded.
        """

        started_at = perf_counter()
        self.logger.info("Historical sentence processing started.")
        self.logger.info("Counting historical sentences.")
        total = retry_operation(
            "Count document sentences",
            self.sentence_vector_db_store.count_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        if total == 0:
            self.logger.error(
                "No sentences_vector records are available for this document."
            )
            return None

        self.logger.info(f"Found {total} historical sentence(s) to process.")
        indexed = 0
        after_global_id = 0
        page_number = 0
        catalog_alias: str | None = None

        with ThreadPoolExecutor(
            max_workers=constants.THREAD_WORKER_COUNT,
            thread_name_prefix="historical-sentence",
        ) as executor:
            while True:
                page_number += 1
                self.logger.info(
                    f"Loading historical page {page_number} after global ID "
                    f"{after_global_id}."
                )
                sentence_vector_records = retry_operation(
                    f"Load sentence page {page_number}",
                    self.sentence_vector_db_store.load_sentences_batch_by_tsp_id,
                    tsp_id,
                    after_global_id,
                    constants.WORKER_BATCH_SIZE * constants.THREAD_WORKER_COUNT,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                if not sentence_vector_records:
                    self.logger.info(
                        f"Historical page {page_number} is empty. All pages "
                        "have been loaded."
                    )
                    break

                if catalog_alias is None:
                    catalog_alias = self.batch_processor.select_catalog_alias(
                        sentence_vector_records[0].tsp.analysis_group
                    )

                self.logger.info(
                    f"Historical page {page_number} loaded "
                    f"{len(sentence_vector_records)} sentence(s), global IDs "
                    f"{sentence_vector_records[0].global_id} through "
                    f"{sentence_vector_records[-1].global_id}."
                )

                # Example: 3,000 records become 100 jobs with 30 records each.
                # The workers can run those small jobs at the same time.
                worker_batches = list(
                    batches(
                        sentence_vector_records,
                        constants.WORKER_BATCH_SIZE,
                    )
                )
                self.logger.info(
                    f"Starting {len(worker_batches)} worker batch(es) for "
                    f"historical page {page_number}."
                )
                page_indexed = sum(
                    executor.map(
                        self._save_historical_sentence_batch_in_opensearch,
                        worker_batches,
                    )
                )
                indexed += page_indexed
                after_global_id = sentence_vector_records[-1].global_id
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

        # Bulk writes can finish before OpenSearch's automatic refresh runs.
        # Refresh both aliases once so Sentence Similarity can immediately find
        # every historical occurrence and its catalog vector after SQS arrives.
        retry_operation(
            "Refresh indexed sentences",
            self.opensearch_helper.refresh_aliases,
            [catalog_alias, constants.SENTENCE_OCCURRENCE_ALIAS],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        # Tell Sentence Similarity only after every sentence is in OpenSearch.
        self.logger.info("Sending the completed historical message to the next queue.")
        retry_operation(
            "Send sentence processing result to next queue",
            self._send_sqs_message,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info("Sent the completed historical message to the next queue.")
        self.logger.info(
            f"Historical sentence processing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
        )
        self.logger.info(
            f"Historical sentence processing took "
            f"{perf_counter() - started_at:.2f} seconds to complete."
        )

        return {
            "reason": "HISTORICAL",
            "tsp_id": tsp_id,
            "source_count": total,
            "indexed_count": indexed,
        }

    def _save_historical_sentence_batch_in_opensearch(
        self,
        sentence_vector_records: list[SentencesVector],
    ) -> int:
        """Prepare and save one historical sentence batch in OpenSearch.

        Input: SentencesVector records containing global IDs 3001 through 3030
        Output: 30 after catalog and occurrence documents are saved.

        The shared processor saves missing sentence keys and reuses database
        embeddings before asking Bedrock to create any remaining vectors.
        """

        label = (
            f"Historical worker batch for global IDs "
            f"{sentence_vector_records[0].global_id} through "
            f"{sentence_vector_records[-1].global_id}"
        )
        self.logger.info(
            f"{label} started with {len(sentence_vector_records)} sentence(s)."
        )
        try:
            indexed_count = self.batch_processor.save_historical_sentence_batch(
                sentence_vector_records,
                label,
                self.sentence_vector_db_store,
                self.embedding_db_store,
            )
        except Exception as error:
            # Keep the global-ID range in the log so the failed batch can be
            # found without printing sentence content or database parameters.
            self.logger.error(f"{label} failed: {safe_error_summary(error)}")
            raise
        self.logger.info(
            f"{label} completed with {indexed_count} indexed sentence(s)."
        )
        return indexed_count

    def _send_sqs_message(self, tsp_id: TspId) -> None:
        """Send the completed historical document to Sentence Similarity.

        Input: tsp_id=1040
        Output message: {"reason": "HISTORICAL", "tsp_id": 1040}

        Raises:
            QueueSendError: If the queue is missing or SQS rejects the message.
        """

        queue_url = self.config.aws_sqs_send_event_queue_endpoint
        if not queue_url:
            raise QueueSendError("AWS SQS send event queue endpoint is not configured")

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

