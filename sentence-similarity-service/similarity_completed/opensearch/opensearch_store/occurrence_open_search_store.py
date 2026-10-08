from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import TargetOccurrence


class OccurrenceOpenSearchStore:
    def __init__(self, config: Config, client) -> None:
        """Keep OpenSearch operations for the sentence occurrence index."""

        self.config = config
        self.client = client

    def search_matching_occurrences_outside_application(
        self,
        key_distances: dict[str, float],
        analysis_group: str,
        source_application_id: int,
    ) -> list[TargetOccurrence]:
        """Find matching occurrences outside the source application.

        Input: key_distances={KEY-A: 0.0, KEY-B: 0.08},
        analysis_group=Asylee, and source_application_id=100
        Output: occurrences for KEY-A and KEY-B outside application 100.
        """

        sentence_keys = list(key_distances)
        occurrences: list[TargetOccurrence] = []

        # Split the terms list, then page every matching occurrence. One key
        # can occur more than 10,000 times, so one search response is not enough.
        batch_size = self.config.opensearch_terms_batch_size
        for offset in range(0, len(sentence_keys), batch_size):
            key_batch = sentence_keys[offset : offset + batch_size]
            occurrences.extend(
                self._search_matching_occurrences_for_key_batch(
                    key_batch,
                    key_distances,
                    analysis_group,
                    source_application_id,
                )
            )
        return occurrences

    def _search_matching_occurrences_for_key_batch(
        self,
        sentence_keys: list[str],
        key_distances: dict[str, float],
        analysis_group: str,
        source_application_id: int,
    ) -> list[TargetOccurrence]:
        """Read all occurrence pages for one sentence-key batch."""

        if not sentence_keys:
            return []

        occurrences: list[TargetOccurrence] = []
        search_after = None
        while True:
            body = {
                "size": self.config.opensearch_terms_batch_size,
                "track_total_hits": False,
                "_source": [
                    "globalId",
                    "tspId",
                    "applicationId",
                    "sentenceKey",
                ],
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"sentenceKey": sentence_keys}},
                            {"term": {"analysisGroup": analysis_group}},
                        ],
                        "must_not": [
                            {"term": {"applicationId": source_application_id}},
                        ],
                    }
                },
                # globalId is unique, so the last value is a stable next-page
                # token across all occurrence-index shards.
                "sort": [{"globalId": {"order": "asc"}}],
            }
            if search_after is not None:
                body["search_after"] = search_after

            response = self.client.search(
                index=self.config.occurrence_alias,
                body=body,
            )
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

            if len(hits) < self.config.opensearch_terms_batch_size:
                return occurrences
            search_after = hits[-1]["sort"]
