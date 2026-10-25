from opensearchpy.exceptions import OpenSearchException

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.contracts import BulkDeleteAction, OccurrenceRecord, TspId


class SentenceOccurrenceOpenSearchStore:
    """Read and delete occurrence documents, then find sentence keys still in use."""

    def __init__(self, client) -> None:
        self.client = client

    def load_document_occurrences(self, tsp_id: TspId) -> list[OccurrenceRecord]:
        """Load every occurrence belonging to one document.

        Input: tsp_id=8205
        Output: records containing each global ID, sentence key, and analysis
        group. The global ID is also the OpenSearch document ID.

        Search-after pagination is used because a document can contain more
        sentences than one OpenSearch response page.
        """

        occurrence_records: list[OccurrenceRecord] = []
        search_after = None
        while True:
            search_body = {
                "size": constants.OPENSEARCH_PAGE_SIZE,
                "track_total_hits": False,
                "_source": ["globalId", "sentenceKey", "analysisGroup"],
                "query": {"term": {"tspId": tsp_id}},
                "sort": [{"globalId": {"order": "asc"}}],
            }
            if search_after is not None:
                search_body["search_after"] = search_after

            search_response = self.client.search(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=search_body)
            occurrence_hits = search_response.get("hits", {}).get("hits", [])
            if not occurrence_hits:
                return occurrence_records

            for occurrence_hit in occurrence_hits:
                occurrence_source = occurrence_hit["_source"]
                occurrence_records.append(
                    {
                        "global_id": int(occurrence_source["globalId"]),
                        "sentence_key": str(occurrence_source["sentenceKey"]),
                        "analysis_group": str(occurrence_source["analysisGroup"]),
                    }
                )

            if len(occurrence_hits) < constants.OPENSEARCH_PAGE_SIZE:
                return occurrence_records
            search_after = occurrence_hits[-1]["sort"]

    def delete_occurrences_by_global_id(self, global_ids: list[int]) -> int:
        """Delete occurrence documents and wait until searches see the deletion.

        Input: global_ids=[100, 101]
        Output: 2

        Waiting for refresh makes the completed delete visible immediately.
        """

        deleted_occurrence_count = 0
        for offset in range(0, len(global_ids), constants.OPENSEARCH_PAGE_SIZE):
            global_id_batch = global_ids[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            delete_actions: list[BulkDeleteAction] = [
                {"delete": {"_index": constants.SENTENCE_OCCURRENCE_ALIAS, "_id": str(global_id)}}
                for global_id in global_id_batch
            ]
            bulk_response = self.client.bulk(body=delete_actions, refresh="wait_for")
            self._raise_for_failed_bulk_deletes(bulk_response, "occurrence")
            deleted_occurrence_count += len(global_id_batch)
        return deleted_occurrence_count

    def find_sentence_keys_used_by_other_documents(
        self, sentence_keys: list[str], analysis_group: str, excluded_tsp_id: TspId
    ) -> set[str]:
        """Return keys used by another document in the same analysis group.

        Input: sentence_keys=["key-a", "key-b"], analysis_group="Asylee",
        excluded_tsp_id=8205
        Output: {"key-b"} when key-a belonged only to the deleted document but
        another Asylee occurrence still uses key-b.

        The current tsp_id is excluded because its occurrences are deliberately
        kept until catalog cleanup succeeds.
        """

        shared_sentence_keys: set[str] = set()
        unique_sentence_keys = list(dict.fromkeys(sentence_keys))
        for offset in range(0, len(unique_sentence_keys), constants.OPENSEARCH_PAGE_SIZE):
            sentence_key_batch = unique_sentence_keys[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            if not sentence_key_batch:
                continue

            # Exclude the document being deleted, then return only the unique
            # sentence keys that are still used by another occurrence.
            search_response = self.client.search(
                index=constants.SENTENCE_OCCURRENCE_ALIAS,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {
                        "bool": {
                            "filter": [
                                {"terms": {"sentenceKey": sentence_key_batch}},
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
                                "size": len(sentence_key_batch),
                                "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
                            }
                        }
                    },
                },
            )
            sentence_key_buckets = search_response.get("aggregations", {}).get("remaining_keys", {}).get("buckets", [])
            shared_sentence_keys.update(
                str(sentence_key_bucket["key"]["sentenceKey"]) for sentence_key_bucket in sentence_key_buckets
            )
        return shared_sentence_keys

    @staticmethod
    def _raise_for_failed_bulk_deletes(bulk_response: dict, operation: str) -> None:
        """Raise one useful error when a bulk request contains failed items."""

        if not bulk_response.get("errors"):
            return
        failed_deletes = []
        for bulk_item in bulk_response.get("items", []):
            delete_details = bulk_item.get("delete", {})
            if delete_details.get("error"):
                failed_deletes.append(
                    {
                        "id": delete_details.get("_id"),
                        "status": delete_details.get("status"),
                        "error": delete_details.get("error"),
                    }
                )
        if not failed_deletes:
            return
        raise OpenSearchException(
            f"{operation} bulk deletion failed for {len(failed_deletes)} item(s): {failed_deletes[:10]}"
        )
