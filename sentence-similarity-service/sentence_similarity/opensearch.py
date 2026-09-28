import base64
import binascii
import struct

from sentence_similarity.errors import PermanentDataError

VECTOR_FIELD = "sentenceContentVector"


def minimumOpenSearchScore(threshold: int) -> float:
    """Convert a cosine percentage into the score OpenSearch expects."""

    return (1.0 + (threshold / 100.0)) / 2.0


def cosineDistance(opensearchScore: float) -> float:
    """Convert an OpenSearch cosine score into distance where zero is exact."""

    cosineSimilarity = max(-1.0, min(1.0, (2.0 * opensearchScore) - 1.0))
    return round(1.0 - cosineSimilarity, 8)


class OpenSearchStore:
    def __init__(self, config, client):
        """Keep the three OpenSearch indexes used by Sentence Similarity."""

        self.config = config
        self.client = client

    def catalogVectors(self, sentenceKeys: list) -> dict:
        """Load saved 512-dimension vectors for a source sentence page."""

        uniqueKeys = list(dict.fromkeys(sentenceKeys))
        if not uniqueKeys:
            return {}
        response = self.client.search(
            index=self.config.catalogAlias,
            body={
                "size": len(uniqueKeys),
                "track_total_hits": False,
                "_source": False,
                "stored_fields": "_none_",
                "docvalue_fields": [
                    "sentenceKey",
                    {"field": VECTOR_FIELD, "format": "binary"},
                ],
                "query": {"ids": {"values": uniqueKeys}},
            },
        )
        vectors = {}
        for hit in response["hits"]["hits"]:
            fields = hit.get("fields", {})
            keyValues = fields.get("sentenceKey")
            vectorValues = fields.get(VECTOR_FIELD)
            if keyValues and vectorValues:
                vectors[str(keyValues[0])] = self._decodeVector(vectorValues[0])
        return vectors

    def tracerClassification(self, vector: list, analysisGroup: str) -> dict:
        """Return tracer and form-language flags found above the threshold."""

        self._validateVector(vector)
        response = self.client.search(
            index=self.config.tracerAlias,
            body={
                "size": 100,
                "track_total_hits": False,
                "_source": ["isTracer", "isFormLanguage"],
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"analysisGroup": analysisGroup}},
                            {"term": {"isNonMatcher": True}},
                        ],
                        "must": [
                            {
                                "knn": {
                                    VECTOR_FIELD: {
                                        "vector": vector,
                                        "min_score": minimumOpenSearchScore(
                                            self.config.tracerThreshold
                                        ),
                                    }
                                }
                            }
                        ],
                    }
                },
            },
        )
        hits = response["hits"]["hits"]
        return {
            "is_non_matcher": bool(hits),
            "is_tracer": any(
                bool(hit["_source"].get("isTracer")) for hit in hits
            ),
            "is_form_language": any(
                bool(hit["_source"].get("isFormLanguage")) for hit in hits
            ),
        }

    def catalogMatches(self, sourceKey: str, vector: list) -> dict:
        """Return every unique catalog key meeting the similarity threshold."""

        self._validateVector(vector)
        matches = {}
        afterKey = None
        while True:
            page, afterKey = self._catalogMatchPage(sourceKey, vector, afterKey)
            matches.update(page)
            if afterKey is None:
                return matches

    def updateOccurrence(self, globalId: int, classification: dict):
        """Copy one tracer decision to the matching occurrence document."""

        self.client.update(
            index=self.config.occurrenceAlias,
            id=str(globalId),
            body={
                "doc": {
                    "isNonMatcher": classification["is_non_matcher"],
                    "isTracer": classification["is_tracer"],
                    "isFormLanguage": classification["is_form_language"],
                }
            },
        )

    def _catalogMatchPage(
        self,
        sourceKey: str,
        vector: list,
        afterKey: dict | None,
    ) -> tuple:
        """Read one composite aggregation page from the semantic catalog."""

        composite = {
            "size": self.config.catalogPageSize,
            "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
        }
        if afterKey:
            composite["after"] = afterKey
        response = self.client.search(
            index=self.config.catalogAlias,
            body={
                "size": 0,
                "track_total_hits": False,
                "query": {
                    "knn": {
                        VECTOR_FIELD: {
                            "vector": vector,
                            "min_score": minimumOpenSearchScore(
                                self.config.matchThreshold
                            ),
                            "method_parameters": {
                                "nprobes": self.config.ivfNprobes,
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
            candidateKey = str(hit["_source"]["sentenceKey"])
            if candidateKey != sourceKey:
                matches[candidateKey] = cosineDistance(float(hit["_score"]))
        return matches, aggregation.get("after_key")

    def _decodeVector(self, encodedVector: str) -> list:
        """Convert one binary doc-value vector into float values."""

        try:
            vectorBytes = base64.b64decode(encodedVector, validate=True)
        except (binascii.Error, ValueError) as error:
            raise PermanentDataError("OpenSearch returned an invalid vector") from error
        expectedBytes = self.config.vectorDimension * 4
        if len(vectorBytes) != expectedBytes:
            raise PermanentDataError(
                f"vector has {len(vectorBytes)} bytes; expected {expectedBytes}"
            )
        return list(
            struct.unpack(
                f"<{self.config.vectorDimension}f",
                vectorBytes,
            )
        )

    def _validateVector(self, vector: list):
        """Reject a vector that does not match the catalog mapping."""

        if len(vector) != self.config.vectorDimension:
            raise PermanentDataError(
                f"vector has {len(vector)} values; "
                f"expected {self.config.vectorDimension}"
            )
