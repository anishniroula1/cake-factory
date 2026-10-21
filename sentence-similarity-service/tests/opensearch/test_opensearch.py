import base64
import struct

import pytest

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.opensearch_store import (
    OccurrenceOpenSearchStore,
    SentenceCatalogOpenSearchStore,
    TracerOpenSearchStore,
)
from similarity_completed.opensearch.utill.catalog import select_catalog_alias
from similarity_completed.opensearch.utill.score import cosine_distance, cosine_similarity, minimum_opensearch_score
from similarity_completed.opensearch.utill.vector import decode_vector


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


def encoded_vector(values):
    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode()


def match_bucket(sentence_key, score):
    return {"sample": {"hits": {"hits": [{"_score": score, "_source": {"sentenceKey": sentence_key}}]}}}


def test_score_conversion_and_catalog_selection(service_config):
    assert (minimum_opensearch_score(90), cosine_distance(0.95), cosine_similarity(0.95)) == pytest.approx(
        (0.95, 0.1, 0.9)
    )
    assert [select_catalog_alias(group) for group in ("Asylee", "refugee")] == [
        "catalog-asylee",
        "catalog-refugee",
    ]
    with pytest.raises(PermanentDataError):
        select_catalog_alias("unknown")


def test_catalog_records_load_vectors_and_optional_classification(service_config):
    sentence_key = "a" * 64
    client = Client(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": sentence_key,
                            "_source": {
                                "sentenceKey": sentence_key,
                                "wordCount": 5,
                                "isNonMatcher": False,
                                "isTracer": False,
                                "isFormLanguage": False,
                            },
                            "fields": {"sentenceContentVector": [encoded_vector([0.25, 0.75])]},
                        },
                        {"_id": "missing-vector", "_source": {}, "fields": {}},
                    ]
                }
            }
        ]
    )
    store = SentenceCatalogOpenSearchStore(client)
    records = store.load_records_by_sentence_keys("catalog-asylee", [sentence_key, sentence_key])

    assert records[sentence_key]["vector"] == pytest.approx([0.25, 0.75])
    assert store.load_records_by_sentence_keys("catalog-asylee", []) == {}


def test_tracer_classification_and_catalog_update(service_config):
    client = Client(
        [
            {
                "hits": {
                    "hits": [
                        {"_score": 0.98, "_source": {"isNonMatcher": True, "isFormLanguage": False}},
                        {"_score": 0.95, "_source": {"isNonMatcher": True, "isFormLanguage": True}},
                        {"_score": 0.90, "_source": {"isNonMatcher": False, "isFormLanguage": False}},
                        {"_score": 0.84, "_source": {"isNonMatcher": False, "isFormLanguage": True}},
                    ]
                }
            }
        ]
    )
    classification = TracerOpenSearchStore(client).classify_sentence_by_vector([0.1, 0.2], "Asylee")
    SentenceCatalogOpenSearchStore(client).update_classification_by_sentence_key(
        "catalog-asylee", "a" * 64, classification
    )

    assert classification == {"is_non_matcher": True, "is_tracer": True, "is_form_language": True}
    knn = client.searches[0]["body"]["query"]["bool"]["must"][0]["knn"]["sentenceContentVector"]
    assert knn == {"vector": [0.1, 0.2], "k": 100}


def test_empty_tracer_result_allows_matching(service_config):
    client = Client([{"hits": {"hits": []}}])
    assert TracerOpenSearchStore(client).classify_sentence_by_vector([0.1, 0.2], "Asylee") == {
        "is_non_matcher": False,
        "is_tracer": False,
        "is_form_language": False,
    }


def test_unflagged_tracer_reference_is_derived_as_tracer(service_config):
    client = Client(
        [{"hits": {"hits": [{"_score": 0.97, "_source": {"isNonMatcher": False, "isFormLanguage": False}}]}}]
    )
    assert TracerOpenSearchStore(client).classify_sentence_by_vector([0.1, 0.2], "Refugee") == {
        "is_non_matcher": False,
        "is_tracer": True,
        "is_form_language": False,
    }


def test_catalog_matches_reads_all_pages_and_keeps_exact_key(service_config):
    source_key = "a" * 64
    target_key = "b" * 64
    client = Client(
        [
            {
                "aggregations": {
                    "matches": {"buckets": [match_bucket(target_key, 0.95)], "after_key": {"sentenceKey": target_key}}
                }
            },
            {"aggregations": {"matches": {"buckets": []}}},
        ]
    )
    matches = SentenceCatalogOpenSearchStore(client).search_semantic_matches_by_vector(
        "catalog-asylee", source_key, [0.1, 0.2]
    )

    assert matches == {source_key: 0.0, target_key: pytest.approx(0.1)}
    knn = client.searches[0]["body"]["query"]["knn"]
    assert knn["sentenceContentVector"]["method_parameters"] == {"nprobes": 64}
    assert knn["sentenceContentVector"]["filter"] == {"range": {"wordCount": {"gte": 5}}}


def test_target_occurrences_searches_every_page_and_excludes_source_application(monkeypatch, service_config):
    monkeypatch.setattr(constants, "OPENSEARCH_TERMS_BATCH_SIZE", 1)
    sentence_key = "a" * 64
    client = Client(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_source": {"globalId": 200, "tspId": 20, "applicationId": 2, "sentenceKey": sentence_key},
                            "sort": [200],
                        }
                    ]
                }
            },
            {
                "hits": {
                    "hits": [
                        {
                            "_source": {"globalId": 201, "tspId": 21, "applicationId": 3, "sentenceKey": sentence_key},
                            "sort": [201],
                        }
                    ]
                }
            },
            {"hits": {"hits": []}},
        ]
    )

    occurrences = OccurrenceOpenSearchStore(client).search_matching_occurrences_outside_application(
        {sentence_key: 0.08}, "Asylee", 1
    )

    assert [item["global_id"] for item in occurrences] == [200, 201]
    assert client.searches[1]["body"]["search_after"] == [200]


def test_invalid_vector_data_is_rejected(service_config):
    with pytest.raises(PermanentDataError):
        decode_vector("not-base64", constants.VECTOR_DIMENSION)
    with pytest.raises(PermanentDataError):
        decode_vector(encoded_vector([0.1]), constants.VECTOR_DIMENSION)
    with pytest.raises(PermanentDataError):
        TracerOpenSearchStore(Client()).classify_sentence_by_vector([0.1], "Asylee")
