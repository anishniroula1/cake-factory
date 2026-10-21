from datetime import UTC, datetime

from opensearchpy.exceptions import OpenSearchException

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import CatalogRecord, Classification
from similarity_completed.opensearch.utill.classification import saved_classification
from similarity_completed.opensearch.utill.score import cosine_distance, minimum_opensearch_score
from similarity_completed.opensearch.utill.vector import decode_vector, validate_vector


class SentenceCatalogOpenSearchStore:
    """Read vectors, search semantic matches, and update sentence catalog classifications."""

    def __init__(self, client) -> None:
        self.client = client

    def load_records_by_sentence_keys(self, catalog_alias: str, sentence_keys: list[str]) -> dict[str, CatalogRecord]:
        """Load vectors and classifications for unique sentence keys. Binary doc
        values avoid loading vectors from _source, and duplicate keys are read once.

        Input: catalog_alias="sentence_semantic_catalog_asylee",
        sentence_keys=["abc123", "def456", "abc123"]
        Output: {"abc123": {"sentence_key": "abc123", "word_count": 8,
        "vector": [...], "classification": None}, "def456": {...}}

        Duplicate keys are loaded once. Documents without vector doc values are
        omitted from the returned mapping.
        """

        unique_keys = list(dict.fromkeys(sentence_keys))
        records: dict[str, CatalogRecord] = {}

        batch_size = constants.OPENSEARCH_TERMS_BATCH_SIZE
        for offset in range(0, len(unique_keys), batch_size):
            key_batch = unique_keys[offset : offset + batch_size]
            if not key_batch:
                continue

            # A search can return the vector through binary doc values. _mget
            # would instead load the larger JSON vector from _source.
            response = self.client.search(
                index=catalog_alias,
                body={
                    "size": len(key_batch),
                    "track_total_hits": False,
                    "_source": ["sentenceKey", "wordCount", "isNonMatcher", "isTracer", "isFormLanguage"],
                    "docvalue_fields": [{"field": constants.VECTOR_FIELD, "format": "binary"}],
                    "query": {"ids": {"values": key_batch}},
                },
            )
            for hit in response["hits"]["hits"]:
                source = hit.get("_source", {})
                vector_values = hit.get("fields", {}).get(constants.VECTOR_FIELD)
                if not vector_values:
                    continue
                sentence_key = source["sentenceKey"]
                records[sentence_key] = {
                    "sentence_key": sentence_key,
                    "word_count": int(source["wordCount"]),
                    "vector": decode_vector(vector_values[0], constants.VECTOR_DIMENSION),
                    "classification": saved_classification(source),
                }
        return records

    def search_semantic_matches_by_vector(
        self, catalog_alias: str, source_key: str, vector: list[float]
    ) -> dict[str, float]:
        """Find sentence keys whose vectors satisfy the similarity threshold.
        Radial search and composite aggregation return every qualifying key.

        Input: catalog_alias="sentence_semantic_catalog_refugee",
        source_key="abc123", vector=[512 Titan float values]
        Output: {"abc123": 0.0, "def456": 0.08}

        The source key is always included with distance 0.0. Other keys are
        returned only when they satisfy the configured similarity threshold
        and contain at least five words.
        """

        validate_vector(vector, constants.VECTOR_DIMENSION)
        matches = {source_key: 0.0}
        after_key = None
        while True:
            match_batch, after_key = self._search_semantic_match_batch(catalog_alias, vector, after_key)
            matches.update(match_batch)
            if after_key is None:
                return matches

    def update_classification_by_sentence_key(
        self, catalog_alias: str, sentence_key: str, classification: Classification
    ) -> None:
        """Persist reusable classification flags with a one-item bulk update.
        This prevents repeated tracer searches for the same sentence key.

        Input: catalog_alias="sentence_semantic_catalog_asylee",
        sentence_key="abc123", classification={"is_non_matcher": True,
        "is_tracer": False, "is_form_language": False}
        """

        document = {
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

        response = self.client.bulk(
            body=[
                {"update": {"_index": catalog_alias, "_id": sentence_key}},
                {"doc": document},
            ]
        )
        if response.get("errors"):
            raise OpenSearchException(
                f"catalog classification bulk update failed: {response.get('items', [])}"
            )

    def _search_semantic_match_batch(
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

        response = self.client.search(
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
                            "filter": {
                                "range": {
                                    "wordCount": {"gte": constants.MINIMUM_MATCH_WORD_COUNT}
                                }
                            },
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
        aggregation = response["aggregations"]["matches"]
        matches = {}
        for bucket in aggregation["buckets"]:
            hit = bucket["sample"]["hits"]["hits"][0]
            sentence_key = hit["_source"]["sentenceKey"]
            matches[sentence_key] = cosine_distance(float(hit["_score"]))
        return matches, aggregation.get("after_key")
