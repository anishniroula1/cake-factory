import base64
import struct

import pytest

from sentence_similarity.errors import PermanentDataError
from sentence_similarity.opensearch import (
    OpenSearchStore,
    cosineDistance,
    minimumOpenSearchScore,
)


class Client:
    def __init__(self, responses=None):
        self.responses = responses or []
        self.searches = []
        self.updates = []

    def search(self, **values):
        self.searches.append(values)
        return self.responses.pop(0)

    def update(self, **values):
        self.updates.append(values)


def encodedVector(values):
    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode()


def matchBucket(sentenceKey, score):
    return {
        "key": {"sentenceKey": sentenceKey},
        "sample": {
            "hits": {
                "hits": [
                    {
                        "_score": score,
                        "_source": {"sentenceKey": sentenceKey},
                    }
                ]
            }
        },
    }


def test_score_and_distance_conversions():
    assert minimumOpenSearchScore(90) == pytest.approx(0.95)
    assert cosineDistance(0.95) == pytest.approx(0.1)
    assert cosineDistance(2.0) == 0.0


def test_catalog_vectors_reads_binary_doc_values(serviceConfig):
    client = Client(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "fields": {
                                "sentenceKey": ["a" * 64],
                                "sentenceContentVector": [
                                    encodedVector([0.25, 0.75])
                                ],
                            }
                        },
                        {"fields": {}},
                    ]
                }
            }
        ]
    )
    store = OpenSearchStore(serviceConfig, client)

    vectors = store.catalogVectors(["a" * 64, "a" * 64])

    assert vectors["a" * 64] == pytest.approx([0.25, 0.75])
    assert store.catalogVectors([]) == {}


def test_invalid_binary_vector_is_rejected(serviceConfig):
    store = OpenSearchStore(serviceConfig, Client())
    with pytest.raises(PermanentDataError):
        store._decodeVector("not-base64")
    with pytest.raises(PermanentDataError):
        store._decodeVector(encodedVector([0.1]))


def test_tracer_classification_combines_reference_flags(serviceConfig):
    client = Client(
        [
            {
                "hits": {
                    "hits": [
                        {"_source": {"isTracer": True}},
                        {"_source": {"isFormLanguage": True}},
                    ]
                }
            }
        ]
    )
    store = OpenSearchStore(serviceConfig, client)

    result = store.tracerClassification([0.1, 0.2], "Asylee")

    assert result == {
        "is_non_matcher": True,
        "is_tracer": True,
        "is_form_language": True,
    }
    filters = client.searches[0]["body"]["query"]["bool"]["filter"]
    assert {"term": {"analysisGroup": "Asylee"}} in filters


def test_empty_tracer_result_is_eligible(serviceConfig):
    store = OpenSearchStore(serviceConfig, Client([{"hits": {"hits": []}}]))
    assert store.tracerClassification([0.1, 0.2], "Asylee") == {
        "is_non_matcher": False,
        "is_tracer": False,
        "is_form_language": False,
    }


def test_catalog_matches_reads_every_composite_page(serviceConfig):
    sourceKey = "a" * 64
    matchingKey = "b" * 64
    client = Client(
        [
            {
                "aggregations": {
                    "matches": {
                        "buckets": [
                            matchBucket(sourceKey, 1.0),
                            matchBucket(matchingKey, 0.95),
                        ],
                        "after_key": {"sentenceKey": matchingKey},
                    }
                }
            },
            {"aggregations": {"matches": {"buckets": []}}},
        ]
    )
    store = OpenSearchStore(serviceConfig, client)

    matches = store.catalogMatches(sourceKey, [0.1, 0.2])

    assert matches == {matchingKey: pytest.approx(0.1)}
    assert len(client.searches) == 2
    query = client.searches[0]["body"]["query"]["knn"]
    assert query["sentenceContentVector"]["method_parameters"] == {
        "nprobes": 16
    }


def test_occurrence_update_uses_global_id(serviceConfig):
    client = Client()
    store = OpenSearchStore(serviceConfig, client)
    classification = {
        "is_non_matcher": True,
        "is_tracer": False,
        "is_form_language": True,
    }

    store.updateOccurrence(101, classification)

    assert client.updates[0]["id"] == "101"
    assert client.updates[0]["body"]["doc"]["isFormLanguage"] is True


def test_wrong_vector_dimension_is_rejected(serviceConfig):
    store = OpenSearchStore(serviceConfig, Client())
    with pytest.raises(PermanentDataError):
        store.tracerClassification([0.1], "Asylee")
    with pytest.raises(PermanentDataError):
        store.catalogMatches("a" * 64, [0.1])
