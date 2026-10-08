from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from similarity_completed.opensearch.contracts import (
    CatalogRecord,
    Classification,
    MatchTarget,
    TargetOccurrence,
)
from similarity_completed.opensearch.errors import PermanentDataError
from ta_db_connection.atables import SentencesVector

if TYPE_CHECKING:
    from similarity_completed.opensearch.db_store.sentence_vector_db_store import (
        SentenceVectorDBStore,
    )
    from similarity_completed.opensearch.opensearch_store.occurrence_open_search_store import (
        OccurrenceOpenSearchStore,
    )
    from similarity_completed.opensearch.opensearch_store.sentence_catalog_open_search_store import (
        SentenceCatalogOpenSearchStore,
    )
    from similarity_completed.opensearch.opensearch_store.tracer_open_search_store import (
        TracerOpenSearchStore,
    )


class TracerFilteringService:
    def __init__(
        self,
        sentence_vector_db_store: SentenceVectorDBStore,
        occurrence_open_search_store: OccurrenceOpenSearchStore,
        sentence_catalog_open_search_store: SentenceCatalogOpenSearchStore,
        tracer_open_search_store: TracerOpenSearchStore,
    ) -> None:
        """Keep sentence classification and match-filtering logic together."""

        self.sentence_vector_db_store = sentence_vector_db_store
        self.occurrence_open_search_store = occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.tracer_open_search_store = tracer_open_search_store

    @staticmethod
    def derive_is_tracer(
        tracer_match_found: bool,
        is_non_matcher: bool,
        is_form_language: bool,
    ) -> bool:
        """Decide whether a tracer-catalog result is a tracer sentence.

        Input: match found with both saved flags false.
        Output: True because the matching reference is a tracer sentence.

        Input: no match found with both saved flags false.
        Output: False because nothing matched in the tracer catalog.
        """

        return tracer_match_found and not is_non_matcher and not is_form_language

    @staticmethod
    def should_skip_sentence_catalog_search(
        classification: Classification,
    ) -> bool:
        """Stop normal matching for tracer, form-language, or non-matcher text.

        Input: any one of isTracer, isFormLanguage, or isNonMatcher is true.
        Output: True, so the sentence does not search the sentence catalog.
        """

        return (
            classification["is_tracer"]
            or classification["is_non_matcher"]
            or classification["is_form_language"]
        )

    def classify_and_update_source(
        self,
        source: SentencesVector,
        catalog_alias: str,
        source_record: CatalogRecord,
    ) -> Classification:
        """Classify one source sentence and save the flags by global ID."""

        classification = self.get_sentence_classification(
            catalog_alias,
            source_record,
            source.tsp.analysis_group,
        )
        self.sentence_vector_db_store.update_classification_by_global_ids(
            [source.global_id],
            classification,
        )
        return classification

    def get_sentence_classification(
        self,
        catalog_alias: str,
        record: CatalogRecord,
        analysis_group: str,
    ) -> Classification:
        """Reuse an allowed decision or search and save a tracer decision."""

        saved = record["classification"]
        # A saved excluded sentence is searched again so sentences_vector gets
        # the closest tracer ID and score. Allowed sentences can be reused.
        if saved is not None and not self.should_skip_sentence_catalog_search(saved):
            return saved

        classification = self.tracer_open_search_store.classify_sentence_by_vector(
            record["vector"],
            analysis_group,
        )
        self.sentence_catalog_open_search_store.update_classification_by_sentence_key(
            catalog_alias,
            record["sentence_key"],
            classification,
        )
        record["classification"] = classification
        return classification

    def find_eligible_match_targets(
        self,
        source: SentencesVector,
        catalog_alias: str,
        key_distances: dict[str, float],
        source_record: CatalogRecord,
    ) -> dict[str, MatchTarget]:
        """Expand matching keys and remove every excluded target occurrence."""

        occurrences = (
            self.occurrence_open_search_store.search_matching_occurrences_outside_application(
                key_distances,
                source.tsp.analysis_group,
                source.tsp.application_id,
            )
        )
        if not occurrences:
            return {}

        occurrences_by_key: dict[str, list[TargetOccurrence]] = defaultdict(list)
        for occurrence in occurrences:
            occurrences_by_key[occurrence["sentence_key"]].append(occurrence)

        records = self.sentence_catalog_open_search_store.load_records_by_sentence_keys(
            catalog_alias,
            list(occurrences_by_key),
        )
        records[source_record["sentence_key"]] = source_record

        missing_keys = sorted(set(occurrences_by_key) - set(records))
        if missing_keys:
            raise PermanentDataError(
                f"catalog vector is missing for sentence key {missing_keys[0]}"
            )

        targets: dict[str, MatchTarget] = {}
        for sentence_key, matching_occurrences in occurrences_by_key.items():
            record = records[sentence_key]
            classification = self.get_sentence_classification(
                catalog_alias,
                record,
                source.tsp.analysis_group,
            )
            self.sentence_vector_db_store.update_classification_by_global_ids(
                [item["global_id"] for item in matching_occurrences],
                classification,
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
