from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from opensearchpy import OpenSearch
from sqlalchemy.orm import Session

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import CatalogRecord, ProcessResult, SentenceResult, TspId
from similarity_completed.opensearch.db_store import SentenceVectorDBStore, TitanMatchesDBStore
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.opensearch_store import (
    OccurrenceOpenSearchStore,
    SentenceCatalogOpenSearchStore,
    TracerOpenSearchStore,
)
from similarity_completed.opensearch.retry import retry_operation
from similarity_completed.opensearch.tracer_filtering_service import TracerFilteringService
from similarity_completed.opensearch.utill.catalog import select_catalog_alias
from ta_db_connection.atatables import SentencesVector
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceSimilarityHandler:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_client: OpenSearch,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep everything required to process one document message."""

        self.config = config
        self.opensearch_client = opensearch_client
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(config, session_factory)
        self.titan_matches_db_store = TitanMatchesDBStore(config, session_factory)
        self.occurrence_open_search_store = OccurrenceOpenSearchStore(config, opensearch_client)
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(config, opensearch_client)
        self.tracer_open_search_store = TracerOpenSearchStore(config, opensearch_client)
        self.tracer_filtering_service = TracerFilteringService(
            self.sentence_vector_db_store,
            self.occurrence_open_search_store,
            self.sentence_catalog_open_search_store,
            self.tracer_open_search_store,
        )

    def process(self, tsp_id: TspId) -> ProcessResult:
        """Classify and match every sentence belonging to one document."""

        self.logger.info("Sentence Similarity processing started.")
        total = retry_operation(
            "Count source sentences",
            self.sentence_vector_db_store.count_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            raise PermanentDataError("sentences_vector has no rows for the requested document")
        self.logger.info(f"Found {total} source sentence(s).")

        processed = 0
        non_matchers = 0
        saved_relationships = 0
        after_global_id = 0
        batch_number = 0

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count, thread_name_prefix="sentence-similarity"
        ) as executor:
            while True:
                batch_number += 1
                rows = retry_operation(
                    f"Load source sentence batch {batch_number}",
                    self.sentence_vector_db_store.load_sentence_batch_by_tsp_id,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    break

                catalog_alias = select_catalog_alias(self.config, rows[0].tsp.analysis_group)
                source_records = retry_operation(
                    f"Load catalog vectors for sentence batch {batch_number}",
                    self.sentence_catalog_open_search_store.load_records_by_sentence_keys,
                    catalog_alias,
                    [row.sentence_key for row in rows],
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )

                jobs = [
                    executor.submit(
                        self._process_sentence_with_retry, row, catalog_alias, source_records[row.sentence_key]
                    )
                    for row in rows
                ]
                for job in jobs:
                    result = job.result()
                    processed += 1
                    non_matchers += int(result["is_non_matcher"])
                    saved_relationships += result["saved_relationships"]

                after_global_id = rows[-1].global_id
                self.logger.info(f"Completed source sentence batch {batch_number} with {len(rows)} sentence(s).")

        retry_operation(
            "Send completed document to the next queue",
            self._send_to_next_queue,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence Similarity completed {processed} sentence(s), including "
            f"{non_matchers} non-matcher(s) and {saved_relationships} saved "
            "relationships."
        )
        return {
            "tsp_id": tsp_id,
            "source_count": total,
            "processed_count": processed,
            "non_matcher_count": non_matchers,
            "saved_relationship_count": saved_relationships,
        }

    def _process_sentence_with_retry(
        self, source: SentencesVector, catalog_alias: str, source_record: CatalogRecord
    ) -> SentenceResult:
        """Retry the complete classification and match flow for one sentence."""

        return retry_operation(
            f"Process source global ID {source.global_id}",
            self._process_sentence,
            source,
            catalog_alias,
            source_record,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

    def _process_sentence(
        self, source: SentencesVector, catalog_alias: str, source_record: CatalogRecord
    ) -> SentenceResult:
        """Classify one source, find eligible occurrences, and save matches."""

        stage = "source classification"
        try:
            classification = self.tracer_filtering_service.classify_and_update_source(
                source, catalog_alias, source_record
            )

            if self.tracer_filtering_service.should_skip_sentence_catalog_search(classification):
                return {"is_non_matcher": True, "saved_relationships": 0}

            stage = "semantic catalog search"
            key_distances = self.sentence_catalog_open_search_store.search_semantic_matches_by_vector(
                catalog_alias, source.sentence_key, source_record["vector"]
            )

            stage = "target occurrence expansion"
            targets = self.tracer_filtering_service.find_eligible_match_targets(
                source, catalog_alias, key_distances, source_record
            )

            stage = "bidirectional Titan match write"
            saved = self.titan_matches_db_store.save_bidirectional_matches(source, targets)
            return {"is_non_matcher": False, "saved_relationships": saved}
        except Exception as error:
            self.logger.error(f"Source global ID {source.global_id} failed during {stage}: {str(error)[:500]}")
            raise

    def _send_to_next_queue(self, tsp_id: TspId) -> None:
        """Send the completed document to the configured next service."""

        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.sent_sqs_endpoint,
            message_body={"tsp_id": tsp_id},
            is_fifo=self.config.sent_sqs_endpoint.endswith(".fifo"),
        )
        if not sent:
            raise RuntimeError("failed to send the completed message to the next queue")
