import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from opensearchpy import OpenSearch
from sqlalchemy.orm import Session

from similarity_completed.opensearch.config import OCCURRENCE_READINESS_WAIT_SECONDS, Config
from similarity_completed.opensearch.contracts import CatalogRecord, ProcessResult, SentenceResult, TspId
from similarity_completed.opensearch.db_store import SentenceVectorDBStore, TitanMatchesDBStore
from similarity_completed.opensearch.errors import QueueSendError
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
        self.titan_matches_db_store = TitanMatchesDBStore(config, session_factory, custom_logger)
        self.occurrence_open_search_store = OccurrenceOpenSearchStore(config, opensearch_client)
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(config, opensearch_client)
        self.tracer_open_search_store = TracerOpenSearchStore(config, opensearch_client)
        self.tracer_filtering_service = TracerFilteringService(
            config,
            self.sentence_vector_db_store,
            self.occurrence_open_search_store,
            self.sentence_catalog_open_search_store,
            self.tracer_open_search_store,
            custom_logger,
        )

    def process(self, tsp_id: TspId) -> ProcessResult | None:
        """Classify and match every sentence belonging to one document."""

        self.logger.info("Sentence Similarity processing started.")
        if not self._wait_for_searchable_occurrences(tsp_id):
            return None
        total = retry_operation(
            "Count source sentences",
            self.sentence_vector_db_store.count_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            self.logger.error("No sentences_vector rows are available for the document.")
            return None
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
                missing_sources = [row for row in rows if row.sentence_key not in source_records]
                if missing_sources:
                    missing_details = ", ".join(
                        f"global_id={row.global_id}, sentence_key={row.sentence_key}" for row in missing_sources
                    )
                    self.logger.error(f"Source catalog vectors are missing for these sentences: {missing_details}.")
                    return None

                jobs = [
                    executor.submit(self._process_sentence, row, catalog_alias, source_records[row.sentence_key])
                    for row in rows
                ]
                starting_global_id = rows[0].global_id
                ending_global_id = rows[-1].global_id
                self.logger.info(
                    f"Processing source sentence batch {batch_number} with {len(rows)} sentence(s); "
                    f"starting_global_id={starting_global_id}, ending_global_id={ending_global_id}."
                )
                for job in jobs:
                    result = job.result()
                    if result is None:
                        return None
                    processed += 1
                    non_matchers += int(result["is_non_matcher"])
                    saved_relationships += result["saved_relationships"]

                after_global_id = ending_global_id
                self.logger.info(
                    f"Completed source sentence batch {batch_number} with {len(rows)} sentence(s); "
                    f"starting_global_id={starting_global_id}, ending_global_id={ending_global_id}."
                )

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

    def _process_sentence(
        self, source: SentencesVector, catalog_alias: str, source_record: CatalogRecord
    ) -> SentenceResult | None:
        """Classify one source, find eligible occurrences, and save matches."""

        classification = self.tracer_filtering_service.classify_and_update_source(source, catalog_alias, source_record)

        if self.tracer_filtering_service.should_skip_sentence_catalog_search(classification):
            self.logger.info(
                f"Skipping sentence-catalog matching for global_id={source.global_id}; "
                f"is_non_matcher={classification['is_non_matcher']}, "
                f"is_tracer={classification['is_tracer']}, "
                f"is_form_language={classification['is_form_language']}."
            )
            return {"is_non_matcher": True, "saved_relationships": 0}

        key_distances = retry_operation(
            f"Search sentence catalog for source global_id={source.global_id}",
            self.sentence_catalog_open_search_store.search_semantic_matches_by_vector,
            catalog_alias,
            source.sentence_key,
            source_record["vector"],
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        targets = self.tracer_filtering_service.find_eligible_match_targets(
            source, catalog_alias, key_distances, source_record
        )
        if targets is None:
            return None

        saved = self.titan_matches_db_store.save_bidirectional_matches(source, targets)
        return {"is_non_matcher": False, "saved_relationships": saved}

    def _wait_for_searchable_occurrences(self, tsp_id: TspId) -> bool:
        """Return whether occurrence rows are searchable after one optional wait."""

        self.logger.info("Checking whether sentence occurrence records are searchable.")
        if self._has_searchable_occurrences(tsp_id):
            self.logger.info("Sentence occurrence records are searchable. Continuing processing.")
            return True

        # The prior service can publish its SQS message just before an
        # OpenSearch refresh becomes visible. Waiting 30 seconds gives the
        # index time to refresh and gives both services a short breathing point.
        self.logger.warning(
            "Sentence occurrence records are not searchable yet. "
            f"Waiting {OCCURRENCE_READINESS_WAIT_SECONDS} seconds before checking once more."
        )
        time.sleep(OCCURRENCE_READINESS_WAIT_SECONDS)

        if self._has_searchable_occurrences(tsp_id):
            self.logger.info("Sentence occurrence records became searchable. Continuing processing.")
            return True

        message = (
            "Sentence occurrence records are still not searchable after the "
            f"{OCCURRENCE_READINESS_WAIT_SECONDS}-second wait."
        )
        self.logger.error(message)
        return False

    def _has_searchable_occurrences(self, tsp_id: TspId) -> bool:
        """Check occurrence readiness and retry only OpenSearch request errors."""

        return retry_operation(
            "Check sentence occurrence readiness",
            self.occurrence_open_search_store.has_occurrences_for_tsp_id,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

    def _send_to_next_queue(self, tsp_id: TspId) -> None:
        """Send the completed document to the configured next service."""

        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.sent_sqs_endpoint,
            message_body={"tsp_id": tsp_id},
            is_fifo=self.config.sent_sqs_endpoint.endswith(".fifo"),
        )
        if not sent:
            raise QueueSendError("failed to send the completed message to the next queue")
