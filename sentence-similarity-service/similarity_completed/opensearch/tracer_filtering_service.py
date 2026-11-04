from time import perf_counter

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import CatalogRecord, Classification, MatchTarget
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
    """Classify source sentences and expand eligible semantic matches."""

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
        sentence_key_distances: dict[str, float],
    ) -> dict[str, MatchTarget]:
        """Expand eligible sentence keys into target occurrence records.

        Input: source_sentence.global_id=39205,
        sentence_key_distances={"abc123": 0.0, "def456": 0.08}
        Output: {"50001": {"tsp_id": 9001, "application_id": 200,
        "distance": 0.08}}

        The sentence catalog search already removes non-matchers, tracers, form
        language, and sentences with fewer than five words. This method only
        expands those accepted sentence keys through the occurrence index.
        """

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
        if occurrence_search_seconds >= constants.SLOW_OCCURRENCE_SEARCH_SECONDS:
            self.logger.warning(
                f"Occurrence expansion for source global_id={source_sentence.global_id} "
                f"took {occurrence_search_seconds:.2f} seconds; "
                f"matching_key_count={len(sentence_key_distances)}, "
                f"target_occurrence_count={len(target_occurrences)}."
            )

        return {
            str(target_occurrence["global_id"]): {
                "tsp_id": target_occurrence["tsp_id"],
                "application_id": target_occurrence["application_id"],
                "distance": target_occurrence["distance"],
            }
            for target_occurrence in target_occurrences
        }
