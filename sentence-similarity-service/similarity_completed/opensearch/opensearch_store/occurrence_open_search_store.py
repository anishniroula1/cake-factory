from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import TargetOccurrence


class OccurrenceOpenSearchStore:
    """Query sentence occurrences and expand matched sentence keys into target records."""

    def __init__(self, client) -> None:
        self.client = client

    def has_occurrences_for_tsp_id(self, tsp_id: int | str) -> bool:
        """Determine whether at least one occurrence is searchable for a document.

        Input: tsp_id=8205
        Output: True when a matching occurrence exists; otherwise False.
        """

        response = self.client.count(
            index=constants.SENTENCE_OCCURRENCE_ALIAS, body={"query": {"term": {"tspId": tsp_id}}}
        )
        return int(response.get("count", 0)) > 0

    def search_matching_occurrences_outside_application(
        self, key_distances: dict[str, float], analysis_group: str, source_application_id: int
    ) -> list[TargetOccurrence]:
        """Expand matching sentence keys into target occurrence records. Results
        stay within the requested analysis group and exclude the source application.

        Input: key_distances={"abc123": 0.0, "def456": 0.08},
        analysis_group="Asylee", source_application_id=100
        Output: target occurrences such as {"global_id": 50001,
        "application_id": 200, "sentence_key": "def456", "distance": 0.08}.
        """

        sentence_keys = list(key_distances)
        occurrences: list[TargetOccurrence] = []

        # Split the terms list, then page every matching occurrence. One key
        # can occur more than 10,000 times, so one search response is not enough.
        batch_size = constants.OPENSEARCH_TERMS_BATCH_SIZE
        for offset in range(0, len(sentence_keys), batch_size):
            key_batch = sentence_keys[offset : offset + batch_size]
            occurrences.extend(
                self._search_matching_occurrences_for_key_batch(
                    key_batch, key_distances, analysis_group, source_application_id
                )
            )
        return occurrences

    def _search_matching_occurrences_for_key_batch(
        self, sentence_keys: list[str], key_distances: dict[str, float], analysis_group: str, source_application_id: int
    ) -> list[TargetOccurrence]:
        """Retrieve every paginated occurrence for one sentence-key batch.
        search_after is used because one key can exceed one response page.

        Input: sentence_keys=["abc123", "def456"],
        key_distances={"abc123": 0.0, "def456": 0.08},
        analysis_group="Refugee", source_application_id=100
        Output: one combined list containing target occurrences from every page.

        The method returns an empty list when none of the supplied keys occurs
        outside the source application.
        """

        if not sentence_keys:
            return []

        occurrences: list[TargetOccurrence] = []
        search_after = None
        while True:
            body = {
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
                body["search_after"] = search_after

            response = self.client.search(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=body)
            hits = response["hits"]["hits"]
            if not hits:
                return occurrences

            for hit in hits:
                source = hit["_source"]
                sentence_key = source["sentenceKey"]
                occurrences.append(
                    {
                        "global_id": int(source["globalId"]),
                        "tsp_id": int(source["tspId"]),
                        "application_id": int(source["applicationId"]),
                        "sentence_key": sentence_key,
                        "distance": key_distances[sentence_key],
                    }
                )

            if len(hits) < constants.OPENSEARCH_TERMS_BATCH_SIZE:
                return occurrences
            search_after = hits[-1]["sort"]
