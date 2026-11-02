import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    HistoricalBatchResult,
    HistoricalResult,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.errors import CriticalProcessingError, QueueSendError
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
        historical_send_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.opensearch_helper = opensearch_helper
        self.historical_send_sqs = historical_send_sqs
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.embedding_db_store = EmbeddingDBStore(session_factory)
        self.batch_processor = SentenceBatchProcessor(
            opensearch_helper,
            custom_logger,
        )

    def process(self, tsp_ids: list[TspId]) -> HistoricalBatchResult | None:
        """Index every historical document carried by one SQS message.

        Input: tsp_ids=[1040, 1041]
        Output: combined document, source-sentence, and indexed-sentence counts.

        Each document still uses the existing sentence worker pool. OpenSearch
        is refreshed once after every document finishes, and the downstream
        document messages are sent with the SQS batch API.
        """

        started_at = perf_counter()
        self.logger.info(
            f"Historical sentence processing started for {len(tsp_ids)} document(s)."
        )
        document_results: list[HistoricalResult] = []
        failed_tsp_ids: list[TspId] = []
        catalog_aliases: set[str] = set()

        for document_number, tsp_id in enumerate(tsp_ids, start=1):
            self.logger.set_generic_message(f"tsp_id={tsp_id}: ")
            self.logger.info(
                f"Processing historical document {document_number} of {len(tsp_ids)}."
            )
            try:
                document_output = self._process_historical_document(tsp_id)
            except CriticalProcessingError as error:
                failed_tsp_ids.append(tsp_id)
                self.logger.error(
                    "Historical document failed and was skipped. Cause: "
                    f"{safe_error_summary(error)}"
                )
                continue
            if document_output is None:
                failed_tsp_ids.append(tsp_id)
                continue
            document_result, catalog_alias = document_output
            document_results.append(document_result)
            catalog_aliases.add(catalog_alias)

        successful_tsp_ids = [result["tsp_id"] for result in document_results]
        if successful_tsp_ids:
            # Bulk writes can finish before OpenSearch's automatic refresh runs.
            # Refresh only the catalogs used by successful documents, then send
            # only those documents to Sentence Similarity.
            aliases_to_refresh = sorted(catalog_aliases)
            aliases_to_refresh.append(constants.SENTENCE_OCCURRENCE_ALIAS)
            retry_operation(
                "Refresh all indexed historical sentences",
                self.opensearch_helper.refresh_aliases,
                aliases_to_refresh,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )

            self.logger.info(
                f"Sending {len(successful_tsp_ids)} completed historical "
                "document message(s) to the next queue."
            )
            retry_operation(
                "Send historical document batch to next queue",
                self._send_sqs_messages,
                successful_tsp_ids,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )

        total_source_count = sum(result["source_count"] for result in document_results)
        total_indexed_count = sum(result["indexed_count"] for result in document_results)
        self.logger.set_generic_message(
            f"historical_document_count={len(tsp_ids)}: "
        )
        if failed_tsp_ids:
            failed_id_list = json.dumps([str(tsp_id) for tsp_id in failed_tsp_ids])
            self.logger.error(
                f"Failed historical tsp_id(s) requiring manual reprocessing: "
                f"{failed_id_list}"
            )
        self.logger.info(
            f"Historical sentence processing completed for "
            f"{len(successful_tsp_ids)} of {len(tsp_ids)} document(s). "
            f"Failed document count: {len(failed_tsp_ids)}. Source count: "
            f"{total_source_count}. Indexed count: {total_indexed_count}."
        )
        self.logger.info(
            f"Historical sentence processing took "
            f"{perf_counter() - started_at:.2f} seconds to complete."
        )
        return {
            "reason": "HISTORICAL",
            "tsp_ids": successful_tsp_ids,
            "document_count": len(document_results),
            "source_count": total_source_count,
            "indexed_count": total_indexed_count,
        }

    def _process_historical_document(
        self, tsp_id: TspId
    ) -> tuple[HistoricalResult, str] | None:
        """Index all saved sentence pages for one historical document.

        Output: the document counts and the catalog alias that received its
        unique sentence vectors. Refresh and downstream SQS work happen later.
        """

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

        self.logger.info(
            f"Historical document indexing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
        )
        return (
            {
                "reason": "HISTORICAL",
                "tsp_id": tsp_id,
                "source_count": total,
                "indexed_count": indexed,
            },
            catalog_alias,
        )

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

    def _send_sqs_messages(self, tsp_ids: list[TspId]) -> None:
        """Batch-send completed historical documents to Sentence Similarity.

        Input: tsp_ids=[1040, 1041]
        Output: one JSON-string message per document in batches of at most ten.

        Raises:
            QueueSendError: If the queue is missing or SQS rejects the message.
        """

        queue_url = self.config.aws_sqs_send_event_queue_endpoint
        if not queue_url:
            raise QueueSendError("AWS SQS send event queue endpoint is not configured")

        messages = [
            json.dumps(
                {
                    "source": "P2",
                    "tsp_id": str(tsp_id),
                    "sent_by": "doc2vec",
                }
            )
            for tsp_id in tsp_ids
        ]
        # SQS accepts no more than ten messages in one batch request. Split the
        # list here because the shared SQS library does not enforce that limit.
        for message_batch in batches(messages, constants.SQS_SEND_BATCH_SIZE):
            result = self.historical_send_sqs.send_batch_messages(
                send_queue_url=queue_url,
                batch_messages=message_batch,
                is_fifo=queue_url.endswith(".fifo"),
                retries=3,
            )
            if result.get("status") != "SUCCEED":
                raise QueueSendError(
                    "Failed to send completed messages to Sentence Similarity"
                )

