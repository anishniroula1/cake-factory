from opensearchpy.exceptions import OpenSearchException

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.contracts import BulkDeleteAction, OccurrenceRecord, TspId


class SentenceOccurrenceOpenSearchStore:
    """Read and delete occurrence documents, then find sentence keys still in use."""

    def __init__(self, client) -> None:
        self.client = client

    def load_occurrences_by_tsp_id(self, tsp_id: TspId) -> list[OccurrenceRecord]:
        """Load every occurrence belonging to one document.

        Input: tsp_id=8205
        Output: records containing each global ID, sentence key, and analysis
        group. The global ID is also the OpenSearch document ID.

        Search-after pagination is used because a document can contain more
        sentences than one OpenSearch response page.
        """

        records: list[OccurrenceRecord] = []
        search_after = None
        while True:
            body = {
                "size": constants.OPENSEARCH_PAGE_SIZE,
                "track_total_hits": False,
                "_source": ["globalId", "sentenceKey", "analysisGroup"],
                "query": {"term": {"tspId": tsp_id}},
                "sort": [{"globalId": {"order": "asc"}}],
            }
            if search_after is not None:
                body["search_after"] = search_after

            response = self.client.search(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=body)
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return records

            for hit in hits:
                source = hit["_source"]
                records.append(
                    {
                        "global_id": int(source["globalId"]),
                        "sentence_key": str(source["sentenceKey"]),
                        "analysis_group": str(source["analysisGroup"]),
                    }
                )

            if len(hits) < constants.OPENSEARCH_PAGE_SIZE:
                return records
            search_after = hits[-1]["sort"]

    def delete_occurrences(self, global_ids: list[int]) -> int:
        """Delete occurrence documents and wait until searches see the deletion.

        Input: global_ids=[100, 101]
        Output: 2

        Waiting for refresh makes the completed delete visible immediately.
        """

        deleted = 0
        for offset in range(0, len(global_ids), constants.OPENSEARCH_PAGE_SIZE):
            batch = global_ids[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            actions: list[BulkDeleteAction] = [
                {"delete": {"_index": constants.SENTENCE_OCCURRENCE_ALIAS, "_id": str(global_id)}}
                for global_id in batch
            ]
            response = self.client.bulk(body=actions, refresh="wait_for")
            self._raise_for_bulk_errors(response, "occurrence")
            deleted += len(batch)
        return deleted

    def find_referenced_sentence_keys(
        self,
        sentence_keys: list[str],
        analysis_group: str,
        excluded_tsp_id: TspId,
    ) -> set[str]:
        """Return keys used by another document in the same analysis group.

        Input: sentence_keys=["key-a", "key-b"], analysis_group="Asylee",
        excluded_tsp_id=8205
        Output: {"key-b"} when key-a belonged only to the deleted document but
        another Asylee occurrence still uses key-b.

        The current tsp_id is excluded because its occurrences are deliberately
        kept until catalog cleanup succeeds.
        """

        referenced: set[str] = set()
        unique_keys = list(dict.fromkeys(sentence_keys))
        for offset in range(0, len(unique_keys), constants.OPENSEARCH_PAGE_SIZE):
            key_batch = unique_keys[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            if not key_batch:
                continue

            # Exclude the document being deleted, then return only the unique
            # sentence keys that are still used by another occurrence.
            response = self.client.search(
                index=constants.SENTENCE_OCCURRENCE_ALIAS,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {
                        "bool": {
                            "filter": [
                                {"terms": {"sentenceKey": key_batch}},
                                {"term": {"analysisGroup": analysis_group}},
                            ],
                            "must_not": [{"term": {"tspId": excluded_tsp_id}}],
                        }
                    },
                    "aggs": {
                        "remaining_keys": {
                            # The aggregation returns the keys without loading
                            # every matching occurrence document into Python.
                            "composite": {
                                "size": len(key_batch),
                                "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
                            }
                        }
                    },
                },
            )
            buckets = response.get("aggregations", {}).get("remaining_keys", {}).get("buckets", [])
            referenced.update(str(bucket["key"]["sentenceKey"]) for bucket in buckets)
        return referenced

    @staticmethod
    def _raise_for_bulk_errors(response: dict, operation: str) -> None:
        """Raise one useful error when a bulk request contains failed items."""

        if not response.get("errors"):
            return
        failures = []
        for item in response.get("items", []):
            details = item.get("delete", {})
            if details.get("error"):
                failures.append(
                    {"id": details.get("_id"), "status": details.get("status"), "error": details.get("error")}
                )
        if not failures:
            return
        raise OpenSearchException(f"{operation} bulk deletion failed for {len(failures)} item(s): {failures[:10]}")
