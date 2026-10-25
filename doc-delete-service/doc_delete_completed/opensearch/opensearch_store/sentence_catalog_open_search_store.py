from opensearchpy.exceptions import OpenSearchException

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.contracts import BulkDeleteAction
from doc_delete_completed.opensearch.errors import PermanentDataError


class SentenceCatalogOpenSearchStore:
    """Delete catalog vectors only after no occurrence references their keys."""

    def select_alias(self, analysis_group: str) -> str:
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

    def delete_sentence_keys(self, analysis_group: str, sentence_keys: list[str]) -> int:
        """Delete unique catalog documents by sentence key.

        Input: analysis_group="Asylee", sentence_keys=["key-a", "key-b"]
        Output: 2

        Calling this method again is safe because deleting an already missing
        document does not recreate or change another catalog document.
        """

        alias = self.select_alias(analysis_group)
        unique_keys = list(dict.fromkeys(sentence_keys))
        deleted = 0
        for offset in range(0, len(unique_keys), constants.OPENSEARCH_PAGE_SIZE):
            key_batch = unique_keys[offset : offset + constants.OPENSEARCH_PAGE_SIZE]
            actions: list[BulkDeleteAction] = [
                {"delete": {"_index": alias, "_id": sentence_key}} for sentence_key in key_batch
            ]
            response = self.client.bulk(body=actions, refresh="wait_for")
            self._raise_for_bulk_errors(response)
            deleted += len(key_batch)
        return deleted

    @staticmethod
    def _raise_for_bulk_errors(response: dict) -> None:
        """Raise one useful error when catalog bulk deletion contains failures."""

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
        raise OpenSearchException(f"catalog bulk deletion failed for {len(failures)} item(s): {failures[:10]}")
