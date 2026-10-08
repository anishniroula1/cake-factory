from collections import defaultdict

from similarity_completed.opensearch.config import Config
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
    def __init__(
        self,
        config: Config,
        sentence_vector_db_store: SentenceVectorDBStore,
        occurrence_open_search_store: OccurrenceOpenSearchStore,
        sentence_catalog_open_search_store: SentenceCatalogOpenSearchStore,
        tracer_open_search_store: TracerOpenSearchStore,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep sentence classification and match-filtering logic together."""

        self.config = config
        self.sentence_vector_db_store = sentence_vector_db_store
        self.occurrence_open_search_store = occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.tracer_open_search_store = tracer_open_search_store
        self.logger = custom_logger

    @staticmethod
    def should_skip_sentence_catalog_search(classification: Classification) -> bool:
        """Stop normal matching for tracer, form-language, or non-matcher text.

        Input: any one of isTracer, isFormLanguage, or isNonMatcher is true.
        Output: True, so the sentence does not search the sentence catalog.
        """

        return classification["is_tracer"] or classification["is_non_matcher"] or classification["is_form_language"]

    def classify_and_update_source(
        self, source: SentencesVector, catalog_alias: str, source_record: CatalogRecord
    ) -> Classification:
        """Classify one source sentence and save the flags by global ID."""

        classification = self.get_sentence_classification(catalog_alias, source_record, source.tsp.analysis_group)
        retry_operation(
            f"Update source classification for global ID {source.global_id}",
            self.sentence_vector_db_store.update_classification_by_global_ids,
            [source.global_id],
            classification,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        return classification

    def get_sentence_classification(
        self, catalog_alias: str, record: CatalogRecord, analysis_group: str
    ) -> Classification:
        """Reuse an allowed decision or search and save a tracer decision."""

        saved = record["classification"]
        # A saved excluded sentence is searched again because only the three
        # catalog flags are reusable. Allowed sentences can be reused.
        if saved is not None and not self.should_skip_sentence_catalog_search(saved):
            return saved

        sentence_key = record["sentence_key"]
        classification = retry_operation(
            f"Search tracer catalog for sentence key {sentence_key}",
            self.tracer_open_search_store.classify_sentence_by_vector,
            record["vector"],
            analysis_group,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Save catalog classification for sentence key {sentence_key}",
            self.sentence_catalog_open_search_store.update_classification_by_sentence_key,
            catalog_alias,
            sentence_key,
            classification,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        record["classification"] = classification
        return classification

    def find_eligible_match_targets(
        self, source: SentencesVector, catalog_alias: str, key_distances: dict[str, float], source_record: CatalogRecord
    ) -> dict[str, MatchTarget] | None:
        """Expand matching keys and remove every excluded target occurrence."""

        occurrences = retry_operation(
            f"Expand matching occurrences for source global ID {source.global_id}",
            self.occurrence_open_search_store.search_matching_occurrences_outside_application,
            key_distances,
            source.tsp.analysis_group,
            source.tsp.application_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if not occurrences:
            return {}

        occurrences_by_key: dict[str, list[TargetOccurrence]] = defaultdict(list)
        for occurrence in occurrences:
            occurrences_by_key[occurrence["sentence_key"]].append(occurrence)

        records = retry_operation(
            f"Load target catalog vectors for source global ID {source.global_id}",
            self.sentence_catalog_open_search_store.load_records_by_sentence_keys,
            catalog_alias,
            list(occurrences_by_key),
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        records[source_record["sentence_key"]] = source_record

        # Example: occurrence keys are {A, B, C}, but catalog records contain
        # only {A, C}. The subtraction leaves missing_keys=[B].
        missing_keys = sorted(set(occurrences_by_key) - set(records))
        if missing_keys:
            self.logger.error(f"Target catalog vectors are missing for sentence keys: {', '.join(missing_keys)}.")
            return None

        targets: dict[str, MatchTarget] = {}
        for sentence_key, matching_occurrences in occurrences_by_key.items():
            record = records[sentence_key]
            classification = self.get_sentence_classification(catalog_alias, record, source.tsp.analysis_group)
            target_global_ids = [item["global_id"] for item in matching_occurrences]
            retry_operation(
                f"Update {len(target_global_ids)} target classification(s) for sentence key {sentence_key}",
                self.sentence_vector_db_store.update_classification_by_global_ids,
                target_global_ids,
                classification,
                wait_seconds=self.config.retry_seconds,
                custom_logger=self.logger,
            )
            if self.should_skip_sentence_catalog_search(classification):
                continue

            for occurrence in matching_occurrences:
                targets[str(occurrence["global_id"])] = {
                    "tsp_id": occurrence["tsp_id"],
                    "application_id": occurrence["application_id"],
                    "distance": occurrence["distance"],
                }
        return targets
