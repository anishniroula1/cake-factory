from datetime import UTC, datetime

from opensearchpy.exceptions import OpenSearchException

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import CatalogRecord, Classification
from similarity_completed.opensearch.utill.classification import saved_classification
from similarity_completed.opensearch.utill.score import cosine_distance, minimum_opensearch_score
from similarity_completed.opensearch.utill.vector import read_catalog_vector, validate_vector


class SentenceCatalogOpenSearchStore:
    """Read vectors, search semantic matches, and update sentence catalog classifications."""

    def __init__(self, client) -> None:
        self.client = client

    def load_catalog_records_by_sentence_keys(
        self, catalog_alias: str, sentence_keys: list[str]
    ) -> dict[str, CatalogRecord]:
        """Load vectors and classifications for unique sentence keys. The vector
        is read as a numeric array from the catalog document source.

        Input: catalog_alias="sentence_semantic_catalog_asylee",
        sentence_keys=["abc123", "def456", "abc123"]
        Output: {"abc123": {"sentence_key": "abc123", "word_count": 8,
        "vector": [...], "classification": None}, "def456": {...}}

        Duplicate keys are loaded once. Documents without a source vector are
        omitted from the returned mapping.
        """

        unique_sentence_keys = list(dict.fromkeys(sentence_keys))
        catalog_records: dict[str, CatalogRecord] = {}

        batch_size = constants.OPENSEARCH_TERMS_BATCH_SIZE
        for offset in range(0, len(unique_sentence_keys), batch_size):
            sentence_key_batch = unique_sentence_keys[offset : offset + batch_size]
            if not sentence_key_batch:
                continue

            # IVF-SQ16 changes the internal search representation, not the
            # numeric vector kept in the document source. Read that array
            # directly so this flow does not depend on binary doc-value support.
            search_response = self.client.search(
                index=catalog_alias,
                body={
                    "size": len(sentence_key_batch),
                    "track_total_hits": False,
                    "_source": [
                        "sentenceKey",
                        "wordCount",
                        "isNonMatcher",
                        "isTracer",
                        "isFormLanguage",
                        constants.VECTOR_FIELD,
                    ],
                    "query": {"ids": {"values": sentence_key_batch}},
                },
            )
            for catalog_hit in search_response["hits"]["hits"]:
                catalog_source = catalog_hit.get("_source", {})
                vector_value = catalog_source.get(constants.VECTOR_FIELD)
                if vector_value is None:
                    continue
                sentence_key = catalog_source["sentenceKey"]
                catalog_records[sentence_key] = {
                    "sentence_key": sentence_key,
                    "word_count": int(catalog_source["wordCount"]),
                    "vector": read_catalog_vector(vector_value, constants.VECTOR_DIMENSION),
                    "classification": saved_classification(catalog_source),
                }
        return catalog_records

    def search_matching_sentence_keys_by_vector(
        self, catalog_alias: str, source_sentence_key: str, vector: list[float]
    ) -> dict[str, float]:
        """Find sentence keys whose vectors satisfy the similarity threshold.
        Radial search and composite aggregation return every qualifying key.

        Input: catalog_alias="sentence_semantic_catalog_refugee",
        source_sentence_key="abc123", vector=[512 Titan float values]
        Output: {"abc123": 0.0, "def456": 0.08}

        The source key is always included with distance 0.0. Other keys are
        returned only when they satisfy the configured similarity threshold
        and contain at least five words.
        """

        validate_vector(vector, constants.VECTOR_DIMENSION)
        sentence_key_distances = {source_sentence_key: 0.0}
        after_key = None
        while True:
            sentence_key_page, after_key = self._search_matching_sentence_key_page(catalog_alias, vector, after_key)
            sentence_key_distances.update(sentence_key_page)
            if after_key is None:
                return sentence_key_distances

    def update_catalog_classification_by_sentence_key(
        self, catalog_alias: str, sentence_key: str, classification: Classification
    ) -> None:
        """Persist reusable classification flags with a one-item bulk update.
        This prevents repeated tracer searches for the same sentence key.

        Input: catalog_alias="sentence_semantic_catalog_asylee",
        sentence_key="abc123", classification={"is_non_matcher": True,
        "is_tracer": False, "is_form_language": False}
        """

        classification_document = {
            "isNonMatcher": classification["is_non_matcher"],
            "isTracer": classification["is_tracer"],
            "isFormLanguage": classification["is_form_language"],
            "updatedAt": datetime.now(UTC).isoformat(),
        }

        # The single-document update endpoint returned 403 in the environment.
        # Keep the old call here for reference while the one-item bulk update is tested.
        # self.client.update(
        #     index=catalog_alias,
        #     id=sentence_key,
        #     body={"doc": document},
        # )

        bulk_response = self.client.bulk(
            body=[{"update": {"_index": catalog_alias, "_id": sentence_key}}, {"doc": classification_document}]
        )
        if bulk_response.get("errors"):
            raise OpenSearchException(f"catalog classification bulk update failed: {bulk_response.get('items', [])}")

    def _search_matching_sentence_key_page(
        self, catalog_alias: str, vector: list[float], after_key: dict | None
    ) -> tuple[dict[str, float], dict | None]:
        """Retrieve one composite-aggregation page of semantic matches. The
        returned token is passed to the next request until no after_key remains.

        Input: catalog_alias="sentence_semantic_catalog_asylee",
        vector=[512 Titan float values], after_key={"sentenceKey": "def456"}
        Output: ({"ghi789": 0.06}, {"sentenceKey": "ghi789"})

        Pass after_key=None for the first page. The returned token is None on
        the final page.
        """

        composite = {
            "size": constants.OPENSEARCH_TERMS_BATCH_SIZE,
            "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
        }
        if after_key:
            composite["after"] = after_key

        search_response = self.client.search(
            index=catalog_alias,
            body={
                "size": 0,
                "track_total_hits": False,
                "query": {
                    "knn": {
                        constants.VECTOR_FIELD: {
                            "vector": vector,
                            "min_score": minimum_opensearch_score(constants.MATCH_THRESHOLD),
                            "method_parameters": {"nprobes": constants.IVF_NPROBES},
                            "filter": {"range": {"wordCount": {"gte": constants.MINIMUM_MATCH_WORD_COUNT}}},
                        }
                    }
                },
                "aggs": {
                    "matches": {
                        "composite": composite,
                        "aggs": {"sample": {"top_hits": {"size": 1, "_source": ["sentenceKey"]}}},
                    }
                },
            },
        )
        matching_key_aggregation = search_response["aggregations"]["matches"]
        sentence_key_distances = {}
        for sentence_key_bucket in matching_key_aggregation["buckets"]:
            catalog_hit = sentence_key_bucket["sample"]["hits"]["hits"][0]
            sentence_key = catalog_hit["_source"]["sentenceKey"]
            sentence_key_distances[sentence_key] = cosine_distance(float(catalog_hit["_score"]))
        return sentence_key_distances, matching_key_aggregation.get("after_key")
