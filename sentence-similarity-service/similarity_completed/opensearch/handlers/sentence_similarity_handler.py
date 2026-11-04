import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from opensearchpy import OpenSearch
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import (
    CatalogRecord,
    MatchTarget,
    ProcessResult,
    SentenceResult,
    TitanMatchRelationship,
    TspId,
)
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
    """Coordinate sentence classification, similarity search, and match storage for one document."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_client: OpenSearch,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.opensearch_client = opensearch_client
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.titan_matches_db_store = TitanMatchesDBStore(session_factory, custom_logger)
        self.occurrence_open_search_store = OccurrenceOpenSearchStore(opensearch_client)
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(opensearch_client)
        self.tracer_open_search_store = TracerOpenSearchStore(opensearch_client)
        self.tracer_filtering_service = TracerFilteringService(
            self.sentence_vector_db_store,
            self.occurrence_open_search_store,
            self.sentence_catalog_open_search_store,
            self.tracer_open_search_store,
            custom_logger,
        )

    def process_document_sentences(self, tsp_id: TspId) -> ProcessResult | None:
        """Process every source sentence associated with one document. The flow
        waits for occurrences, loads catalog vectors, processes sentences in
        parallel, and publishes the completed document to the next queue.

        Input: tsp_id=8205
        Output: {"tsp_id": 8205, "source_count": 30, "processed_count": 30,
        "non_matcher_count": 2, "tracer_count": 1, "form_language_count": 1,
        "matched_sentence_count": 20, "saved_relationship_count": 125}

        The method returns None when occurrence or catalog data is not ready.
        """

        started_at = perf_counter()
        self.logger.info("Sentence Similarity processing started.")
        if not self._wait_for_document_occurrences(tsp_id):
            return None
        total_source_sentence_count = retry_operation(
            "Count source sentences",
            self.sentence_vector_db_store.count_document_sentences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        if total_source_sentence_count == 0:
            self.logger.error("No sentences_vector rows are available for the document.")
            return None
        self.logger.info(f"Found {total_source_sentence_count} source sentence(s).")

        processed_sentence_count = 0
        non_matcher_count = 0
        tracer_count = 0
        form_language_count = 0
        matched_sentence_count = 0
        collected_match_relationships: list[TitanMatchRelationship] = []
        after_global_id = 0
        page_number = 0

        with ThreadPoolExecutor(
            max_workers=constants.THREAD_WORKER_COUNT, thread_name_prefix="sentence-similarity"
        ) as executor:
            while True:
                page_number += 1
                sentence_vector_records = retry_operation(
                    f"Load source sentence page {page_number}",
                    self.sentence_vector_db_store.load_document_sentence_batch,
                    tsp_id,
                    after_global_id,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                if not sentence_vector_records:
                    break

                catalog_alias = select_catalog_alias(sentence_vector_records[0].tsp.analysis_group)
                source_catalog_records = retry_operation(
                    f"Load catalog vectors for sentence page {page_number}",
                    self.sentence_catalog_open_search_store.load_catalog_records_by_sentence_keys,
                    catalog_alias,
                    [record.sentence_key for record in sentence_vector_records],
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                missing_catalog_records = [
                    record
                    for record in sentence_vector_records
                    if record.sentence_key not in source_catalog_records
                ]
                if missing_catalog_records:
                    missing_details = ", ".join(
                        f"global_id={record.global_id}" for record in missing_catalog_records
                    )
                    self.logger.error(
                        "Source catalog vectors are missing for these sentences: "
                        f"{missing_details}."
                    )
                    return None

                sentence_jobs = [
                    executor.submit(
                        self._process_source_sentence,
                        record,
                        catalog_alias,
                        source_catalog_records[record.sentence_key],
                    )
                    for record in sentence_vector_records
                ]
                after_global_id = sentence_vector_records[-1].global_id
                self.logger.info(
                    f"Processing source sentence page {page_number} with "
                    f"{len(sentence_vector_records)} sentence(s); "
                    f"starting_global_id={sentence_vector_records[0].global_id}, "
                    f"ending_global_id={after_global_id}."
                )

                for sentence_job in sentence_jobs:
                    sentence_result = sentence_job.result()
                    if sentence_result is None:
                        for unfinished_job in sentence_jobs:
                            unfinished_job.cancel()
                        return None

                    match_relationships = sentence_result["match_relationships"]
                    processed_sentence_count += 1
                    non_matcher_count += int(sentence_result["is_non_matcher"])
                    tracer_count += int(sentence_result["is_tracer"])
                    form_language_count += int(sentence_result["is_form_language"])
                    matched_sentence_count += int(bool(match_relationships))
                    collected_match_relationships.extend(match_relationships)

        self.logger.info(
            f"Starting titan_matches database writes for "
            f"{len(collected_match_relationships)} collected relationship(s)."
        )
        titan_match_write_started_at = perf_counter()
        saved_relationship_count = self.titan_matches_db_store.save_bidirectional_match_relationships(
            collected_match_relationships
        )
        self.logger.info(
            f"titan_matches database writes completed in "
            f"{perf_counter() - titan_match_write_started_at:.2f} seconds and "
            f"saved {saved_relationship_count} unique relationship(s)."
        )

        retry_operation(
            "Send completed document to the next queue",
            self._send_similarity_result_to_next_queue,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence Similarity completed {processed_sentence_count} sentence(s): "
            f"{matched_sentence_count} sentence(s) had matches, "
            f"{saved_relationship_count} relationship(s) were saved, {tracer_count} tracer classification(s), "
            f"{form_language_count} form-language classification(s), and "
            f"{non_matcher_count} non-matcher classification(s)."
        )
        self.logger.info(f"Sentence Similarity took {perf_counter() - started_at:.2f} seconds to complete.")
        return {
            "tsp_id": tsp_id,
            "source_count": total_source_sentence_count,
            "processed_count": processed_sentence_count,
            "non_matcher_count": non_matcher_count,
            "tracer_count": tracer_count,
            "form_language_count": form_language_count,
            "matched_sentence_count": matched_sentence_count,
            "saved_relationship_count": saved_relationship_count,
        }

    def _process_source_sentence(
        self, source_sentence: SentencesVector, catalog_alias: str, source_catalog_record: CatalogRecord
    ) -> SentenceResult | None:
        """Classify one source sentence and collect its eligible relationships.
        Ineligible classifications stop immediately; eligible sentences continue
        through catalog search, target filtering, and relationship preparation.

        Input: source_sentence.global_id=39205,
        catalog_alias="sentence_semantic_catalog_asylee", and a source_catalog_record
        containing a 512-value vector.
        Output: {"is_non_matcher": False, "is_tracer": False,
        "is_form_language": False, "match_relationships": [{...}]}

        The method returns None when a required target catalog record is missing.
        """

        sentence_classification = self.tracer_filtering_service.classify_and_update_source_sentence(
            source_sentence, catalog_alias, source_catalog_record
        )

        if self.tracer_filtering_service.should_skip_sentence_catalog_search(sentence_classification):
            self.logger.info(
                f"Skipping sentence-catalog matching for global_id={source_sentence.global_id}; "
                f"is_non_matcher={sentence_classification['is_non_matcher']}, "
                f"is_tracer={sentence_classification['is_tracer']}, "
                f"is_form_language={sentence_classification['is_form_language']}."
            )
            return {
                "is_non_matcher": sentence_classification["is_non_matcher"],
                "is_tracer": sentence_classification["is_tracer"],
                "is_form_language": sentence_classification["is_form_language"],
                "match_relationships": [],
            }

        # Short sentences are still checked against the tracer catalog, but
        # they do not search the main catalog or create Titan relationships.
        if source_catalog_record["word_count"] < constants.MINIMUM_MATCH_WORD_COUNT:
            self.logger.info(
                f"Skipping sentence-catalog matching for global_id={source_sentence.global_id}; "
                f"word_count={source_catalog_record['word_count']} is below "
                f"{constants.MINIMUM_MATCH_WORD_COUNT}."
            )
            return {
                "is_non_matcher": sentence_classification["is_non_matcher"],
                "is_tracer": sentence_classification["is_tracer"],
                "is_form_language": sentence_classification["is_form_language"],
                "match_relationships": [],
            }

        sentence_key_distances = retry_operation(
            f"Search sentence catalog for source global_id={source_sentence.global_id}",
            self.sentence_catalog_open_search_store.search_matching_sentence_keys_by_vector,
            catalog_alias,
            source_sentence.sentence_key,
            source_catalog_record["vector"],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        eligible_target_occurrences = self.tracer_filtering_service.find_eligible_target_occurrences(
            source_sentence, sentence_key_distances
        )

        return {
            "is_non_matcher": sentence_classification["is_non_matcher"],
            "is_tracer": sentence_classification["is_tracer"],
            "is_form_language": sentence_classification["is_form_language"],
            "match_relationships": self._build_titan_match_relationships(
                source_sentence, eligible_target_occurrences
            ),
        }

    @staticmethod
    def _build_titan_match_relationships(
        source_sentence: SentencesVector,
        eligible_target_occurrences: dict[str, MatchTarget],
    ) -> list[TitanMatchRelationship]:
        """Convert one sentence's matches into relationships grouped later for database writes."""

        return [
            {
                "source_global_id": source_sentence.global_id,
                "source_tsp_id": source_sentence.tsp_id,
                "source_application_id": source_sentence.tsp.application_id,
                "target_global_id": int(target_global_id),
                "target_tsp_id": target["tsp_id"],
                "target_application_id": target["application_id"],
                "distance": target["distance"],
            }
            for target_global_id, target in eligible_target_occurrences.items()
        ]

    def _wait_for_document_occurrences(self, tsp_id: TspId) -> bool:
        """Wait once for Doc2Vec occurrence records to become searchable.
        OpenSearch refreshes asynchronously, so a message can arrive before its
        occurrence records become visible.

        Input: tsp_id=8205
        Output: True when at least one occurrence is searchable, or False when
        both readiness checks return zero records.
        """

        self.logger.info("Checking whether sentence occurrence records are searchable.")
        if self._document_occurrences_are_searchable(tsp_id):
            self.logger.info("Sentence occurrence records are searchable. Continuing processing.")
            return True

        # The prior service can publish its SQS message just before an
        # OpenSearch refresh becomes visible. Waiting 30 seconds gives the
        # index time to refresh and gives both services a short breathing point.
        self.logger.warning(
            "Sentence occurrence records are not searchable yet. "
            f"Waiting {constants.OCCURRENCE_READINESS_WAIT_SECONDS} seconds before checking once more."
        )
        time.sleep(constants.OCCURRENCE_READINESS_WAIT_SECONDS)

        if self._document_occurrences_are_searchable(tsp_id):
            self.logger.info("Sentence occurrence records became searchable. Continuing processing.")
            return True

        readiness_error_message = (
            "Sentence occurrence records are still not searchable after the "
            f"{constants.OCCURRENCE_READINESS_WAIT_SECONDS}-second wait."
        )
        self.logger.error(readiness_error_message)
        return False

    def _document_occurrences_are_searchable(self, tsp_id: TspId) -> bool:
        """Check occurrence-index readiness with transient-error retries.

        Input: tsp_id=8205
        Output: True when OpenSearch contains at least one occurrence; otherwise False.
        """

        return retry_operation(
            "Check sentence occurrence readiness",
            self.occurrence_open_search_store.has_document_occurrences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

    def _send_similarity_result_to_next_queue(self, tsp_id: TspId) -> None:
        """Publish the completed document to the downstream queue. The outgoing
        message identifies P2 as the source and Sentence Similarity as the sender.

        Input: tsp_id=8205

        The queue receives {"source": "P2", "tsp_id": "8205",
        "sent_by": "sentence_similarity"}.

        Raises:
            QueueSendError: If SQS does not accept the message.
        """

        message_sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.send_sqs_endpoint,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "sentence-similarity"},
            is_fifo=self.config.send_sqs_endpoint.endswith(".fifo"),
        )
        if not message_sent:
            raise QueueSendError("failed to send the completed message to the next queue")
