from datetime import UTC, datetime

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import CatalogRecord, Classification
from similarity_completed.opensearch.utill.classification import saved_classification
from similarity_completed.opensearch.utill.constants import VECTOR_FIELD
from similarity_completed.opensearch.utill.score import (
    cosine_distance,
    minimum_opensearch_score,
)
from similarity_completed.opensearch.utill.vector import decode_vector, validate_vector


class SentenceCatalogStore:
    def __init__(self, config: Config, client) -> None:
        """Keep OpenSearch operations for an Asylee or Refugee catalog."""

        self.config = config
        self.client = client

    def load_records_by_sentence_keys(
        self,
        catalog_alias: str,
        sentence_keys: list[str],
    ) -> dict[str, CatalogRecord]:
        """Load vectors and saved flags for requested sentence keys.

        Input: catalog_alias=sentence_semantic_catalog_asylee and
        sentence_keys=[KEY-A, KEY-B, KEY-A]
        Output: KEY-A and KEY-B mapped to vectors and saved classifications.

        The sentence key is the document ID. Duplicate keys are searched once,
        and a document without a vector is not returned.
        """

        unique_keys = list(dict.fromkeys(sentence_keys))
        records: dict[str, CatalogRecord] = {}

        batch_size = self.config.opensearch_terms_batch_size
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
                    "_source": [
                        "sentenceKey",
                        "isNonMatcher",
                        "isTracer",
                        "isFormLanguage",
                    ],
                    "docvalue_fields": [
                        {"field": VECTOR_FIELD, "format": "binary"},
                    ],
                    "query": {"ids": {"values": key_batch}},
                },
            )
            for hit in response["hits"]["hits"]:
                source = hit.get("_source", {})
                vector_values = hit.get("fields", {}).get(VECTOR_FIELD)
                if not vector_values:
                    continue
                sentence_key = source["sentenceKey"]
                records[sentence_key] = {
                    "sentence_key": sentence_key,
                    "vector": decode_vector(
                        vector_values[0],
                        self.config.vector_dimension,
                    ),
                    "classification": saved_classification(source),
                }
        return records

    def search_semantic_matches_by_vector(
        self,
        catalog_alias: str,
        source_key: str,
        vector: list[float],
    ) -> dict[str, float]:
        """Find every sentence key meeting the similarity threshold.

        Input: source_key=KEY-A and vector=[512 Titan values]
        Output: {KEY-A: 0.0, KEY-B: 0.08}

        Distance 0.0 is exact. Distance 0.08 means 92% similarity.
        """

        validate_vector(vector, self.config.vector_dimension)
        matches = {source_key: 0.0}
        after_key = None
        while True:
            match_batch, after_key = self._search_semantic_match_batch(
                catalog_alias,
                vector,
                after_key,
            )
            matches.update(match_batch)
            if after_key is None:
                return matches

    def update_classification_by_sentence_key(
        self,
        catalog_alias: str,
        sentence_key: str,
        classification: Classification,
    ) -> None:
        """Save reusable classification flags on one catalog document.

        Input: sentence_key=KEY-A and classification is a tracer.
        Output: KEY-A contains the flags and a new updatedAt value.
        """

        self.client.update(
            index=catalog_alias,
            id=sentence_key,
            body={
                "doc": {
                    "isNonMatcher": classification["is_non_matcher"],
                    "isTracer": classification["is_tracer"],
                    "isFormLanguage": classification["is_form_language"],
                    "updatedAt": datetime.now(UTC).isoformat(),
                }
            },
        )

    def _search_semantic_match_batch(
        self,
        catalog_alias: str,
        vector: list[float],
        after_key: dict | None,
    ) -> tuple[dict[str, float], dict | None]:
        """Read one page of unique matching keys and its next-page token."""

        composite = {
            "size": self.config.opensearch_terms_batch_size,
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
                        VECTOR_FIELD: {
                            "vector": vector,
                            "min_score": minimum_opensearch_score(
                                self.config.match_threshold
                            ),
                            "method_parameters": {
                                "nprobes": self.config.ivf_nprobes,
                            },
                        }
                    }
                },
                "aggs": {
                    "matches": {
                        "composite": composite,
                        "aggs": {
                            "sample": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": ["sentenceKey"],
                                }
                            }
                        },
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
