from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import TargetOccurrence


class OccurrenceOpenSearchStore:
    """Query sentence occurrences and expand matched sentence keys into target records."""

    def __init__(self, client) -> None:
        self.client = client

    def has_document_occurrences(self, tsp_id: int | str) -> bool:
        """Determine whether at least one occurrence is searchable for a document.

        Input: tsp_id=8205
        Output: True when a matching occurrence exists; otherwise False.
        """

        occurrence_count_response = self.client.count(
            index=constants.SENTENCE_OCCURRENCE_ALIAS, body={"query": {"term": {"tspId": tsp_id}}}
        )
        return int(occurrence_count_response.get("count", 0)) > 0

    def search_target_occurrences_outside_source_application(
        self, sentence_key_distances: dict[str, float], analysis_group: str, source_application_id: int
    ) -> list[TargetOccurrence]:
        """Expand matching sentence keys into target occurrence records. Results
        stay within the requested analysis group and exclude the source application.

        Input: sentence_key_distances={"abc123": 0.0, "def456": 0.08},
        analysis_group="Asylee", source_application_id=100
        Output: target occurrences such as {"global_id": 50001,
        "application_id": 200, "sentence_key": "def456", "distance": 0.08}.
        """

        sentence_keys = list(sentence_key_distances)
        matching_occurrences: list[TargetOccurrence] = []

        # Split the terms list, then page every matching occurrence. One key
        # can occur more than 10,000 times, so one search response is not enough.
        batch_size = constants.OPENSEARCH_TERMS_BATCH_SIZE
        for offset in range(0, len(sentence_keys), batch_size):
            sentence_key_batch = sentence_keys[offset : offset + batch_size]
            matching_occurrences.extend(
                self._search_target_occurrences_for_key_batch(
                    sentence_key_batch, sentence_key_distances, analysis_group, source_application_id
                )
            )
        return matching_occurrences

    def _search_target_occurrences_for_key_batch(
        self,
        sentence_keys: list[str],
        sentence_key_distances: dict[str, float],
        analysis_group: str,
        source_application_id: int,
    ) -> list[TargetOccurrence]:
        """Retrieve every paginated occurrence for one sentence-key batch.
        search_after is used because one key can exceed one response page.

        Input: sentence_keys=["abc123", "def456"],
        sentence_key_distances={"abc123": 0.0, "def456": 0.08},
        analysis_group="Refugee", source_application_id=100
        Output: one combined list containing target occurrences from every page.

        The method returns an empty list when none of the supplied keys occurs
        outside the source application.
        """

        if not sentence_keys:
            return []

        target_occurrences: list[TargetOccurrence] = []
        search_after = None
        while True:
            search_body = {
                "size": constants.OPENSEARCH_TERMS_BATCH_SIZE,
                "track_total_hits": False,
                "_source": ["globalId", "tspId", "applicationId", "sentenceKey"],
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"sentenceKey": sentence_keys}},
                            {"term": {"analysisGroup": analysis_group}},
                        ],
                        "must_not": [{"term": {"applicationId": source_application_id}}],
                    }
                },
                # globalId is unique, so the last value is a stable next-page
                # token across all occurrence-index shards.
                "sort": [{"globalId": {"order": "asc"}}],
            }
            if search_after is not None:
                search_body["search_after"] = search_after

            search_response = self.client.search(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=search_body)
            occurrence_hits = search_response["hits"]["hits"]
            if not occurrence_hits:
                return target_occurrences

            for occurrence_hit in occurrence_hits:
                occurrence_source = occurrence_hit["_source"]
                sentence_key = occurrence_source["sentenceKey"]
                target_occurrences.append(
                    {
                        "global_id": int(occurrence_source["globalId"]),
                        "tsp_id": int(occurrence_source["tspId"]),
                        "application_id": int(occurrence_source["applicationId"]),
                        "sentence_key": sentence_key,
                        "distance": sentence_key_distances[sentence_key],
                    }
                )

            if len(occurrence_hits) < constants.OPENSEARCH_TERMS_BATCH_SIZE:
                return target_occurrences
            search_after = occurrence_hits[-1]["sort"]
