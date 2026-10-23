import base64
import json
import struct
from dataclasses import replace

import pytest

from sentence_search.config import config
from sentence_search.opensearch_store import (
    OpenSearchStore,
    catalog_eligibility_filters,
)


class EmptySearchClient:
    def __init__(self):
        self.request = None

    def search(self, **request):
        self.request = request
        return {"hits": {"total": {"value": 0}, "hits": []}}


class ApplicationComparisonClient:
    def __init__(self):
        self.requests = []

    def search(self, **request):
        self.requests.append(request)
        if request["body"]["size"] == 0:
            return {
                "aggregations": {
                    "matches": {
                        "buckets": [],
                    }
                }
            }
        return {"hits": {"hits": []}}


class CatalogMatchesClient:
    def __init__(self):
        self.request = None

    def search(self, **request):
        self.request = request
        return {"aggregations": {"matches": {"buckets": []}}}


class CatalogMultiSearchClient:
    def __init__(self):
        self.request = None

    def msearch(self, **request):
        self.request = request
        return {"responses": [{"hits": {"hits": []}}]}


class CatalogPageClient:
    def __init__(self):
        self.request = None

    def search(self, **request):
        self.request = request
        return {
            "aggregations": {
                "matches": {
                    "buckets": [
                        self._bucket("key-1", 1.0),
                        self._bucket("key-2", 0.96),
                        self._bucket("key-3", 0.955),
                    ]
                }
            }
        }

    @staticmethod
    def _bucket(sentence_key, score):
        return {
            "key": {"sentenceKey": sentence_key},
            "sample": {
                "hits": {
                    "hits": [
                        {
                            "_score": score,
                            "_source": {
                                "sentenceKey": sentence_key,
                                "sentenceContent": f"Sentence {sentence_key}",
                            },
                        }
                    ]
                }
            },
        }


class EligibleSentenceClient:
    def __init__(self):
        self.catalog_request = None
        self.count_request = None

    def search(self, **request):
        self.catalog_request = request
        return {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "sentenceKey": "key-1",
                            "sentenceContent": "A long eligible sentence here",
                            "wordCount": 5,
                            "isNonMatcher": False,
                            "isTracer": False,
                            "isFormLanguage": False,
                        }
                    }
                ]
            }
        }

    def count(self, **request):
        self.count_request = request
        return {"count": 1}


class CatalogPreviewClient:
    def __init__(self):
        self.request = None

    def search(self, **request):
        self.request = request
        vector_bytes = struct.pack("<512f", *([0.25] * 512))
        return {
            "hits": {
                "hits": [
                    {
                        "_id": "key-1",
                        "_source": {
                            "sentenceKey": "key-1",
                            "sentenceContent": "A sentence",
                        },
                        "fields": {
                            "sentenceContentVector": [
                                base64.b64encode(vector_bytes).decode("ascii")
                            ]
                        },
                    }
                ]
            }
        }


class CatalogVectorsWithoutDocumentIdClient:
    def __init__(self):
        self.request = None

    def search(self, **request):
        self.request = request
        vector_bytes = struct.pack("<512f", *([0.5] * 512))
        return {
            "hits": {
                "hits": [
                    {
                        "fields": {
                            "sentenceKey": ["key-1"],
                            "sentenceContentVector": [
                                base64.b64encode(vector_bytes).decode("ascii")
                            ],
                        }
                    }
                ]
            }
        }


class OccurrenceCatalogJoinClient:
    def __init__(self):
        self.requests = []

    def search(self, **request):
        self.requests.append(request)
        if request["index"] == config.occurrence_alias:
            return {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "globalId": 1,
                                "applicationId": 10,
                                "tspId": 100,
                                "sentenceKey": "eligible-key",
                                "analysisGroup": "Asylee",
                            },
                            "sort": [1],
                        },
                        {
                            "_source": {
                                "globalId": 2,
                                "applicationId": 10,
                                "tspId": 100,
                                "sentenceKey": "short-key",
                                "analysisGroup": "Asylee",
                            },
                            "sort": [2],
                        },
                    ]
                }
            }
        return {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "sentenceKey": "eligible-key",
                            "sentenceContent": "This sentence has five useful words",
                            "wordCount": 6,
                        }
                    }
                ]
            }
        }


def test_catalog_eligibility_accepts_missing_flags_and_excludes_true_flags():
    filters = catalog_eligibility_filters()

    assert filters == [
        {
            "bool": {
                "must_not": [
                    {"term": {"isNonMatcher": True}},
                    {"term": {"isTracer": True}},
                    {"term": {"isFormLanguage": True}},
                ]
            }
        },
        {"range": {"wordCount": {"gt": 4}}},
    ]


def test_analysis_group_is_normalized_for_occurrence_search():
    client = EmptySearchClient()
    store = OpenSearchStore(config, client)

    store.application_occurrence_page("10", "refugee", 100, None, True)

    filters = client.request["body"]["query"]["bool"]["filter"]
    assert {"term": {"analysisGroup": "Refugee"}} in filters


def test_catalog_radial_search_uses_configured_ivf_nprobes():
    client = CatalogMatchesClient()
    store = OpenSearchStore(config, client)

    matches = store.catalog_matches(
        "key-1",
        [0.0] * 512,
        90,
        "Asylee",
    )

    knn = client.request["body"]["query"]["bool"]["should"][1]["knn"]
    vector_query = knn["sentenceContentVector"]
    assert matches == []
    assert client.request["body"]["query"]["bool"][
        "filter"
    ] == catalog_eligibility_filters()
    assert vector_query["min_score"] == pytest.approx(0.95)
    assert vector_query["method_parameters"] == {"nprobes": 64}


def test_catalog_search_omits_nprobes_when_ivf_tuning_is_disabled():
    client = CatalogMatchesClient()
    store = OpenSearchStore(
        replace(config, ivf_nprobes_enabled=False),
        client,
    )

    store.catalog_matches("key-1", [0.0] * 512, 90, "Asylee")

    vector_query = client.request["body"]["query"]["bool"]["should"][1][
        "knn"
    ]["sentenceContentVector"]
    assert "method_parameters" not in vector_query


def test_catalog_msearch_adds_the_catalog_alias_before_each_body():
    client = CatalogMultiSearchClient()
    store = OpenSearchStore(config, client)
    body = {"size": 0, "query": {"match_all": {}}}

    responses = store.multi_search_catalog([body], "Refugee")

    assert client.request["body"] == [
        {"index": config.refugee_catalog_alias},
        body,
    ]
    assert responses == [{"hits": {"hits": []}}]


def test_catalog_page_returns_unique_keys_and_composite_cursor():
    client = CatalogPageClient()
    store = OpenSearchStore(config, client)

    result = store.catalog_match_page(
        "key-1",
        [0.0] * 512,
        90,
        2,
        None,
        "Asylee",
    )

    composite = client.request["body"]["aggs"]["matches"]["composite"]
    assert composite["size"] == 3
    assert [match["sentenceKey"] for match in result["matches"]] == [
        "key-1",
        "key-2",
    ]
    assert result["matches"][0]["matchType"] == "exact"
    assert result["matches"][1]["similarityPercentage"] == 92.0
    assert result["nextAfterKey"] == {"sentenceKey": "key-2"}


def test_source_sentence_eligibility_checks_catalog_then_occurrence():
    client = EligibleSentenceClient()
    store = OpenSearchStore(config, client)

    exists = store.eligible_sentence_exists(
        "key-1",
        "Asylee",
    )

    assert exists is True
    assert client.catalog_request["index"] == config.asylee_catalog_alias
    assert client.catalog_request["body"]["query"]["bool"]["filter"] == [
        {"ids": {"values": ["key-1"]}},
        *catalog_eligibility_filters(),
    ]
    assert client.count_request["body"]["query"]["bool"]["filter"] == [
        {"term": {"sentenceKey": "key-1"}},
        {"term": {"analysisGroup": "Asylee"}},
    ]














def test_catalog_preview_returns_the_decoded_embedding_list():
    client = CatalogPreviewClient()
    store = OpenSearchStore(config, client)

    result = store.preview(config.asylee_catalog_alias, 10)

    document = result["documents"][0]
    assert client.request["body"]["docvalue_fields"] == [
        {"field": "sentenceContentVector", "format": "binary"}
    ]
    assert len(document["sentenceContentVector"]) == 512
    assert document["sentenceContentVector"][0] == pytest.approx(0.25)


def test_catalog_vectors_use_sentence_key_when_search_hit_has_no_id():
    client = CatalogVectorsWithoutDocumentIdClient()
    store = OpenSearchStore(config, client)

    vectors = store.catalog_vectors(["key-1"], "Refugee")

    assert client.request["body"]["docvalue_fields"] == [
        "sentenceKey",
        {"field": "sentenceContentVector", "format": "binary"},
    ]
    assert len(vectors["key-1"]) == 512
    assert vectors["key-1"][0] == pytest.approx(0.5)
    assert client.request["index"] == config.refugee_catalog_alias




def test_application_page_queries_only_fields_in_the_occurrence_mapping():
    client = EmptySearchClient()
    store = OpenSearchStore(config, client)

    result = store.application_occurrence_page(
        "A1",
        "Asylee",
        100,
        None,
        True,
    )

    filters = client.request["body"]["query"]["bool"]["filter"]
    assert result["sentences"] == []
    assert {"term": {"applicationId": "A1"}} in filters
    assert {"term": {"analysisGroup": "Asylee"}} in filters
    for eligibility_filter in catalog_eligibility_filters():
        assert eligibility_filter not in filters


def test_application_page_filters_and_hydrates_from_the_asylee_catalog():
    client = OccurrenceCatalogJoinClient()
    store = OpenSearchStore(config, client)

    result = store.application_occurrence_page(
        "10",
        "Asylee",
        100,
        None,
        True,
    )

    assert result["totalSentences"] == 1
    assert result["sentences"] == [
        {
            "globalId": 1,
            "applicationId": 10,
            "tspId": 100,
            "sentenceKey": "eligible-key",
            "analysisGroup": "Asylee",
            "sentenceContent": "This sentence has five useful words",
            "wordCount": 6,
            "isNonMatcher": False,
            "isTracer": False,
            "isFormLanguage": False,
        }
    ]
    assert client.requests[1]["index"] == config.asylee_catalog_alias


def test_tsp_page_queries_only_fields_in_the_occurrence_mapping():
    client = EmptySearchClient()
    store = OpenSearchStore(config, client)

    result = store.tsp_occurrence_page("TSP-1", "Refugee", 100, None)

    filters = client.request["body"]["query"]["bool"]["filter"]
    assert result["sentences"] == []
    assert {"term": {"tspId": "TSP-1"}} in filters
    assert {"term": {"analysisGroup": "Refugee"}} in filters
    for eligibility_filter in catalog_eligibility_filters():
        assert eligibility_filter not in filters


def test_sentence_key_search_uses_terms_and_never_knn():
    client = EmptySearchClient()
    store = OpenSearchStore(config, client)

    store.matching_occurrence_page(
        ["key-1", "key-2"],
        "A1",
        "Asylee",
        100,
        None,
        False,
    )

    body = client.request["body"]
    query_text = json.dumps(body["query"])
    assert '"terms": {"sentenceKey": ["key-1", "key-2"]}' in query_text
    assert '"knn"' not in query_text
    assert {"term": {"applicationId": "A1"}} in body["query"]["bool"]["must_not"]


def test_tsp_csv_target_search_excludes_only_the_source_tsp():
    client = EmptySearchClient()
    store = OpenSearchStore(config, client)

    store.matching_occurrence_page(
        ["key-1"],
        None,
        "Asylee",
        100,
        None,
        False,
        "501",
    )

    must_not = client.request["body"]["query"]["bool"]["must_not"]
    assert must_not == [{"term": {"tspId": "501"}}]


def test_application_comparison_counts_only_eligible_other_applications():
    client = ApplicationComparisonClient()
    store = OpenSearchStore(config, client)

    result = store.occurrence_counts_by_application_and_key(
        ["key-1", "key-2"],
        "A1",
        "Asylee",
    )

    assert result == []
    body = client.requests[0]["body"]
    filters = body["query"]["bool"]["filter"]
    assert {"term": {"analysisGroup": "Asylee"}} in filters
    for eligibility_filter in catalog_eligibility_filters():
        assert eligibility_filter not in filters
    assert {"term": {"applicationId": "A1"}} in body["query"]["bool"]["must_not"]
    sources = body["aggs"]["matches"]["composite"]["sources"]
    assert sources == [
        {"applicationId": {"terms": {"field": "applicationId"}}},
        {"sentenceKey": {"terms": {"field": "sentenceKey"}}},
    ]


def test_application_comparison_details_filter_selected_applications():
    client = ApplicationComparisonClient()
    store = OpenSearchStore(config, client)

    result = store.matching_occurrences_for_applications(
        ["key-1"],
        ["A2", "A3"],
        "A1",
        "Asylee",
    )

    assert result == []
    body = client.requests[0]["body"]
    filters = body["query"]["bool"]["filter"]
    assert {"terms": {"applicationId": ["A2", "A3"]}} in filters
    assert {"term": {"analysisGroup": "Asylee"}} in filters
    for eligibility_filter in catalog_eligibility_filters():
        assert eligibility_filter not in filters
    assert {"term": {"applicationId": "A1"}} in body["query"]["bool"]["must_not"]
