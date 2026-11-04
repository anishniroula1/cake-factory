from collections import defaultdict
from time import perf_counter

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import CatalogRecord, Classification, MatchTarget, TargetOccurrence
from similarity_completed.opensearch.db_store import SentenceVectorDBStore
from similarity_completed.opensearch.opensearch_store import (
    OccurrenceOpenSearchStore,
    SentenceCatalogOpenSearchStore,
    TracerOpenSearchStore,
)
from similarity_completed.opensearch.retry import retry_operation
from ta_db_connection.atatables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class TracerFilteringService:
    """Classify source and target sentences and exclude ineligible semantic matches."""

    def __init__(
        self,
        sentence_vector_db_store: SentenceVectorDBStore,
        occurrence_open_search_store: OccurrenceOpenSearchStore,
        sentence_catalog_open_search_store: SentenceCatalogOpenSearchStore,
        tracer_open_search_store: TracerOpenSearchStore,
        custom_logger: CustomLogger,
    ) -> None:
        self.sentence_vector_db_store = sentence_vector_db_store
        self.occurrence_open_search_store = occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.tracer_open_search_store = tracer_open_search_store
        self.logger = custom_logger

    @staticmethod
    def should_skip_sentence_catalog_search(classification: Classification) -> bool:
        """Determine whether a classification is ineligible for semantic matching.
        Any exclusion flag prevents the sentence from creating Titan matches.

        Input: classification={"is_non_matcher": True, "is_tracer": False,
        "is_form_language": False}
        Output: True because is_non_matcher is set.

        The method returns False only when all three flags are False.
        """

        return classification["is_tracer"] or classification["is_non_matcher"] or classification["is_form_language"]

    def classify_and_update_source_sentence(
        self, source_sentence: SentencesVector, catalog_alias: str, source_catalog_record: CatalogRecord
    ) -> Classification:
        """Classify a source sentence and persist the result. The catalog stores
        the reusable key classification, while occurrence records store it by ID.

        Input: source_sentence.global_id=39205,
        catalog_alias="sentence_semantic_catalog_asylee", and a source_catalog_record
        containing a vector and optional cached classification.
        Output: {"is_non_matcher": False, "is_tracer": True,
        "is_form_language": False}
        """

        sentence_classification = self.resolve_sentence_classification(
            catalog_alias, source_catalog_record, source_sentence.tsp.analysis_group, source_sentence.global_id
        )
        source_classification_update_started_at = perf_counter()
        retry_operation(
            f"Update source classification for global_id={source_sentence.global_id}",
            self.sentence_vector_db_store.update_sentence_classification_by_global_ids,
            [source_sentence.global_id],
            sentence_classification,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Source classification database update for "
            f"global_id={source_sentence.global_id} took "
            f"{perf_counter() - source_classification_update_started_at:.2f} seconds."
        )
        return sentence_classification

    def resolve_sentence_classification(
        self, catalog_alias: str, catalog_record: CatalogRecord, analysis_group: str, source_global_id: int
    ) -> Classification:
        """Resolve a sentence classification from cache or tracer search. A new
        result is saved on the catalog for future occurrences of the same key.

        Input: catalog_alias="sentence_semantic_catalog_refugee",
        catalog_record.sentence_key="abc123", analysis_group="Refugee",
        source_global_id=39205
        Output: {"is_non_matcher": True, "is_tracer": False,
        "is_form_language": True}
        """

        saved_classification = catalog_record["classification"]
        # All three catalog fields are present, so this sentence key was
        # already classified. Reuse both allowed and excluded decisions.
        if saved_classification is not None:
            return saved_classification

        sentence_key = catalog_record["sentence_key"]
        sentence_classification = retry_operation(
            f"Search tracer catalog for source global_id={source_global_id}",
            self.tracer_open_search_store.search_tracer_classification_by_vector,
            catalog_record["vector"],
            analysis_group,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Save catalog classification for source global_id={source_global_id}",
            self.sentence_catalog_open_search_store.update_catalog_classification_by_sentence_key,
            catalog_alias,
            sentence_key,
            sentence_classification,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        catalog_record["classification"] = sentence_classification
        return sentence_classification

    def find_eligible_target_occurrences(
        self,
        source_sentence: SentencesVector,
        catalog_alias: str,
        sentence_key_distances: dict[str, float],
        source_catalog_record: CatalogRecord,
    ) -> dict[str, MatchTarget] | None:
        """Expand semantic matches and retain eligible target occurrences. New
        target classifications are saved, and ineligible targets are removed.

        Input: source_sentence.global_id=39205, catalog_alias="sentence_semantic_catalog_asylee",
        sentence_key_distances={"abc123": 0.0, "def456": 0.08}
        Output: {"50001": {"tsp_id": 9001, "application_id": 200,
        "distance": 0.08}}

        The method returns an empty mapping when no target qualifies or None
        when a required target catalog record is missing.
        """

        target_processing_started_at = perf_counter()
        occurrence_search_started_at = perf_counter()
        target_occurrences = retry_operation(
            f"Expand matching occurrences for source global_id={source_sentence.global_id}",
            self.occurrence_open_search_store.search_target_occurrences_outside_source_application,
            sentence_key_distances,
            source_sentence.tsp.analysis_group,
            source_sentence.tsp.application_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        occurrence_search_seconds = perf_counter() - occurrence_search_started_at
        if not target_occurrences:
            self._log_slow_target_processing(
                source_sentence.global_id,
                target_processing_started_at,
                occurrence_search_seconds,
                0.0,
                0.0,
                len(sentence_key_distances),
                0,
                0,
            )
            return {}

        occurrences_by_key: dict[str, list[TargetOccurrence]] = defaultdict(list)
        for target_occurrence in target_occurrences:
            occurrences_by_key[target_occurrence["sentence_key"]].append(target_occurrence)

        target_catalog_load_started_at = perf_counter()
        target_catalog_records = retry_operation(
            f"Load target catalog vectors for source global_id={source_sentence.global_id}",
            self.sentence_catalog_open_search_store.load_catalog_records_by_sentence_keys,
            catalog_alias,
            list(occurrences_by_key),
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        target_catalog_load_seconds = perf_counter() - target_catalog_load_started_at
        target_catalog_records[source_catalog_record["sentence_key"]] = source_catalog_record

        # Example: occurrence keys are {A, B, C}, but catalog records contain
        # only {A, C}. The subtraction leaves missing_keys=[B].
        missing_keys = sorted(set(occurrences_by_key) - set(target_catalog_records))
        if missing_keys:
            self.logger.error(
                f"Target catalog vectors are missing while processing source global_id={source_sentence.global_id}; "
                f"missing catalog key count: {len(missing_keys)}."
            )
            self._log_slow_target_processing(
                source_sentence.global_id,
                target_processing_started_at,
                occurrence_search_seconds,
                target_catalog_load_seconds,
                0.0,
                len(sentence_key_distances),
                len(target_occurrences),
                0,
            )
            return None

        eligible_target_occurrences: dict[str, MatchTarget] = {}
        target_classification_started_at = perf_counter()
        for sentence_key, matching_occurrences in occurrences_by_key.items():
            target_catalog_record = target_catalog_records[sentence_key]
            target_global_ids = [item["global_id"] for item in matching_occurrences]
            target_classification = self.resolve_sentence_classification(
                catalog_alias, target_catalog_record, source_sentence.tsp.analysis_group, source_sentence.global_id
            )
            target_classification_update_started_at = perf_counter()
            retry_operation(
                f"Update {len(target_global_ids)} target classification(s) for source "
                f"global_id={source_sentence.global_id}",
                self.sentence_vector_db_store.update_sentence_classification_by_global_ids,
                target_global_ids,
                target_classification,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            self.logger.info(
                f"Target classification database update for source_global_id="
                f"{source_sentence.global_id}, target_count={len(target_global_ids)} "
                f"took {perf_counter() - target_classification_update_started_at:.2f} seconds."
            )
            if self.should_skip_sentence_catalog_search(target_classification):
                continue

            for target_occurrence in matching_occurrences:
                eligible_target_occurrences[str(target_occurrence["global_id"])] = {
                    "tsp_id": target_occurrence["tsp_id"],
                    "application_id": target_occurrence["application_id"],
                    "distance": target_occurrence["distance"],
                }
        target_classification_seconds = perf_counter() - target_classification_started_at
        self._log_slow_target_processing(
            source_sentence.global_id,
            target_processing_started_at,
            occurrence_search_seconds,
            target_catalog_load_seconds,
            target_classification_seconds,
            len(sentence_key_distances),
            len(target_occurrences),
            len(eligible_target_occurrences),
        )
        return eligible_target_occurrences

    def _log_slow_target_processing(
        self,
        source_global_id: int,
        processing_started_at: float,
        occurrence_search_seconds: float,
        catalog_load_seconds: float,
        classification_seconds: float,
        matching_key_count: int,
        target_occurrence_count: int,
        eligible_relationship_count: int,
    ) -> None:
        """Log the stage times when target processing takes at least five seconds.

        Input example: source_global_id=39205, occurrence_search_seconds=8.2,
        catalog_load_seconds=0.4, classification_seconds=3.1.

        The log shows which stage used the most time and how many target records
        were handled. Fast source sentences do not create this warning.
        """

        total_seconds = perf_counter() - processing_started_at
        if total_seconds < constants.SLOW_TARGET_PROCESSING_SECONDS:
            return

        self.logger.warning(
            f"Slow target processing for source global_id={source_global_id} "
            f"took {total_seconds:.2f} seconds: "
            f"occurrence_search={occurrence_search_seconds:.2f}, "
            f"target_catalog_load={catalog_load_seconds:.2f}, "
            f"target_classification={classification_seconds:.2f}; "
            f"matching_key_count={matching_key_count}, "
            f"target_occurrence_count={target_occurrence_count}, "
            f"eligible_relationship_count={eligible_relationship_count}."
        )
