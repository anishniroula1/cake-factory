from opensearchpy.exceptions import OpenSearchException

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.contracts import BulkDeleteAction
from doc_delete_completed.opensearch.errors import PermanentDataError


class SentenceCatalogOpenSearchStore:
    """Delete catalog vectors only after no occurrence references their keys."""

    def select_catalog_alias(self, analysis_group: str) -> str:
        """Return the catalog alias assigned to an analysis group.

        Input: analysis_group="Refugee"
        Output: "sentence_semantic_catalog_refugee"
        """

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return constants.ASYLEE_CATALOG_ALIAS
        if normalized_group == "refugee":
            return constants.REFUGEE_CATALOG_ALIAS
        raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")

    def __init__(self, client) -> None:
        self.client = client

    def delete_catalog_documents_by_sentence_key(self, analysis_group: str, sentence_keys: list[str]) -> int:
        """Delete unique catalog documents by sentence key.

        Input: analysis_group="Asylee", sentence_keys=["key-a", "key-b"]
        Output: 2

        Calling this method again is safe because deleting an already missing
        document does not recreate or change another catalog document.
        """

        catalog_alias = self.select_catalog_alias(analysis_group)
        unique_sentence_keys = list(dict.fromkeys(sentence_keys))
        deleted_catalog_count = 0
        for offset in range(0, len(unique_sentence_keys), constants.OPENSEARCH_PAGE_SIZE):
            sentence_key_batch = unique_sentence_keys[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            delete_actions: list[BulkDeleteAction] = [
                {"delete": {"_index": catalog_alias, "_id": sentence_key}} for sentence_key in sentence_key_batch
            ]
            bulk_response = self.client.bulk(body=delete_actions, refresh="wait_for")
            self._raise_for_failed_bulk_deletes(bulk_response)
            deleted_catalog_count += len(sentence_key_batch)
        return deleted_catalog_count

    @staticmethod
    def _raise_for_failed_bulk_deletes(bulk_response: dict) -> None:
        """Raise one useful error when catalog bulk deletion contains failures."""

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
            f"catalog bulk deletion failed for {len(failed_deletes)} item(s): {failed_deletes[:10]}"
        )
