from sentence_search.models import SentenceOccurrence
from sentence_search.opensearch_store import OpenSearchStore


class SentenceService:
    def __init__(self, opensearch: OpenSearchStore):
        """Save the OpenSearch store used when a sentence is added."""

        self.opensearch = opensearch

    def add_sentence(
        self,
        sentence: SentenceOccurrence,
    ) -> dict:
        """Index one sentence and create its catalog vector when needed."""

        source = sentence.model_dump(mode="json")
        key = source["sentenceKey"]
        existing = self.opensearch.existing_catalog_keys([key])
        catalog_indexed = False
        if key not in existing:
            self.opensearch.bulk_index_catalog([self._catalog_source(source)])
            catalog_indexed = True

        self.opensearch.bulk_index_occurrences([source])
        self.opensearch.refresh_indices()
        return {
            "globalId": source["globalId"],
            "sentenceKey": key,
            "catalogDocumentIndexed": catalog_indexed,
            "occurrenceIndexed": True,
            "status": "completed",
        }

    @staticmethod
    def _catalog_source(source: dict) -> dict:
        """Keep only fields allowed by the strict catalog mapping."""

        return {
            "sentenceContent": source["sentenceContent"],
            "sentenceKey": source["sentenceKey"],
            "createdAt": source["createdAt"],
            "updatedAt": source["updatedAt"],
        }
