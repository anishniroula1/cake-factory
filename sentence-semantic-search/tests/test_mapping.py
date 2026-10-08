import base64
import json
import struct

import pytest
from opensearchpy.helpers.errors import BulkIndexError

from sentence_search.config import config
from sentence_search.opensearch_store import (
    BULK_RETRY_SECONDS,
    OpenSearchStore,
)


class DeleteByQueryClient:
    def __init__(self):
        self.request = None

    def delete_by_query(self, **request):
        self.request = request
        return {"deleted": 3}


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
        self.request = None

    def count(self, **request):
        self.request = request
        return {"count": 1}


class AliasIndexClient:
    class Indices:
        def __init__(self):
            self.deleted = []

        def exists_alias(self, name):
            return name in {
                config.occurrence_alias,
                config.catalog_alias,
                config.tracer_alias,
            }

        def get_alias(self, name):
            return {f"physical-{name}": {}}

        def exists(self, index):
            return True

        def delete(self, index):
            self.deleted.append(index)

    def __init__(self):
        self.indices = self.Indices()


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


def test_catalog_radial_search_uses_configured_ivf_nprobes():
    client = CatalogMatchesClient()
    store = OpenSearchStore(config, client)

    matches = store.catalog_matches("key-1", [0.0] * 512, 90)

    knn = client.request["body"]["query"]["bool"]["should"][1]["knn"]
    vector_query = knn["sentenceContentVector"]
    assert matches == []
    assert vector_query["min_score"] == pytest.approx(0.95)
    assert vector_query["method_parameters"] == {"nprobes": 64}


def test_catalog_msearch_adds_the_catalog_alias_before_each_body():
    client = CatalogMultiSearchClient()
    store = OpenSearchStore(config, client)
    body = {"size": 0, "query": {"match_all": {}}}

    responses = store.multi_search_catalog([body])

    assert client.request["body"] == [
        {"index": config.catalog_alias},
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


def test_source_sentence_eligibility_uses_all_matching_filters():
    client = EligibleSentenceClient()
    store = OpenSearchStore(config, client)

    exists = store.eligible_sentence_exists(
        "key-1",
        "Asylee",
    )

    assert exists is True
    assert client.request["body"]["query"]["bool"]["filter"] == [
        {"term": {"sentenceKey": "key-1"}},
        {"term": {"analysisGroup": "Asylee"}},
        {"term": {"isTracer": False}},
        {"term": {"isFormLanguage": False}},
    ]


def test_storage_delete_resolves_all_indexes_from_configured_aliases():
    client = AliasIndexClient()
    store = OpenSearchStore(config, client)

    result = store.delete_indices()

    expected = sorted(
        {
            f"physical-{config.occurrence_alias}",
            f"physical-{config.catalog_alias}",
            f"physical-{config.tracer_alias}",
        }
    )
    assert result == {"deletedIndexes": expected}
    assert client.indices.deleted == expected


@pytest.mark.parametrize(
    "status",
    [408, 424, 429, 500, 502, 503, 504],
)
def test_catalog_retry_keeps_only_temporary_failed_documents(status):
    store = OpenSearchStore(config, EmptySearchClient())
    actions = [
        {"_id": "key-1", "_source": {"sentenceKey": "key-1"}},
        {"_id": "key-2", "_source": {"sentenceKey": "key-2"}},
    ]
    error = BulkIndexError(
        "one temporary failure",
        [{"index": {"_id": "key-2", "status": status}}],
    )

    retry_actions = store._retryable_actions(error, actions)

    assert retry_actions == [actions[1]]


def test_occurrence_retry_waits_only_inside_the_failed_worker(monkeypatch):
    bulk_calls = []
    sleep_calls = []
    actions = [
        {"_id": "key-1", "_source": {"sentenceKey": "key-1"}},
        {"_id": "key-2", "_source": {"sentenceKey": "key-2"}},
    ]

    def fail_once(client, pending, chunk_size, raise_on_error):
        bulk_calls.append(list(pending))
        if len(bulk_calls) == 1:
            raise BulkIndexError(
                "one temporary failure",
                [{"index": {"_id": "key-2", "status": 500}}],
            )

    def capture_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr("sentence_search.opensearch_store.helpers.bulk", fail_once)
    monkeypatch.setattr("sentence_search.opensearch_store.time.sleep", capture_sleep)
    store = OpenSearchStore(config, EmptySearchClient())

    store._bulk_with_retry(actions, "occurrence")

    assert bulk_calls == [actions, [actions[1]]]
    assert sleep_calls == [BULK_RETRY_SECONDS]


def test_catalog_retry_uses_the_shared_cooldown(monkeypatch):
    bulk_calls = []
    cooldown_waits = []
    cooldown_starts = []
    documents = [
        {"sentenceKey": "key-1", "sentenceContent": "First"},
        {"sentenceKey": "key-2", "sentenceContent": "Second"},
    ]

    def fail_second_document_once(client, actions, chunk_size, raise_on_error):
        bulk_calls.append(list(actions))
        if len(bulk_calls) == 1:
            raise BulkIndexError(
                "one temporary catalog failure",
                [{"index": {"_id": "key-2", "status": 429}}],
            )

    def capture_cooldown_wait():
        cooldown_waits.append(True)

    def capture_cooldown_start():
        cooldown_starts.append(True)

    monkeypatch.setattr(
        "sentence_search.opensearch_store.helpers.bulk",
        fail_second_document_once,
    )
    store = OpenSearchStore(config, EmptySearchClient())
    monkeypatch.setattr(store, "_wait_for_embedding_cooldown", capture_cooldown_wait)
    monkeypatch.setattr(store, "_start_embedding_cooldown", capture_cooldown_start)

    indexed = store.bulk_index_catalog(documents)

    assert indexed == 2
    assert [action["_id"] for action in bulk_calls[0]] == ["key-1", "key-2"]
    assert [action["_id"] for action in bulk_calls[1]] == ["key-2"]
    assert len(cooldown_starts) == 1
    assert len(cooldown_waits) == 2


def test_tracer_bulk_uses_tracer_id_and_configured_alias(monkeypatch):
    bulk_calls = []
    documents = [
        {
            "tracerId": 91,
            "sentenceContent": "This is tracer text.",
            "analysisGroup": "Asylee",
            "isNonMatcher": False,
            "isFormLanguage": False,
        }
    ]

    def capture_bulk(client, actions, chunk_size, raise_on_error):
        bulk_calls.append(list(actions))

    monkeypatch.setattr(
        "sentence_search.opensearch_store.helpers.bulk",
        capture_bulk,
    )
    store = OpenSearchStore(config, EmptySearchClient())

    indexed = store.bulk_index_tracer_sentences(documents)

    assert indexed == 1
    assert bulk_calls[0] == [
        {
            "_op_type": "index",
            "_index": config.tracer_alias,
            "_id": "91",
            "_source": documents[0],
        }
    ]
    assert "sentenceContentVector" not in bulk_calls[0][0]["_source"]


def test_occurrence_bulk_retries_only_its_failed_documents(monkeypatch):
    bulk_calls = []
    sleep_calls = []
    documents = [
        {"globalId": 101, "sentenceContent": "First"},
        {"globalId": 102, "sentenceContent": "Second"},
    ]

    def fail_second_document_once(client, actions, chunk_size, raise_on_error):
        bulk_calls.append(list(actions))
        if len(bulk_calls) == 1:
            raise BulkIndexError(
                "one temporary occurrence failure",
                [{"index": {"_id": "102", "status": 503}}],
            )

    def capture_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(
        "sentence_search.opensearch_store.helpers.bulk",
        fail_second_document_once,
    )
    monkeypatch.setattr("sentence_search.opensearch_store.time.sleep", capture_sleep)
    store = OpenSearchStore(config, EmptySearchClient())

    indexed = store.bulk_index_occurrences(documents)

    assert indexed == 2
    assert [action["_id"] for action in bulk_calls[0]] == ["101", "102"]
    assert [action["_id"] for action in bulk_calls[1]] == ["102"]
    assert sleep_calls == [BULK_RETRY_SECONDS]


def test_catalog_preview_returns_the_decoded_embedding_list():
    client = CatalogPreviewClient()
    store = OpenSearchStore(config, client)

    result = store.preview(config.catalog_alias, 10)

    document = result["documents"][0]
    assert client.request["body"]["docvalue_fields"] == [
        {"field": "sentenceContentVector", "format": "binary"}
    ]
    assert len(document["sentenceContentVector"]) == 512
    assert document["sentenceContentVector"][0] == pytest.approx(0.25)


def test_catalog_vectors_use_sentence_key_when_search_hit_has_no_id():
    client = CatalogVectorsWithoutDocumentIdClient()
    store = OpenSearchStore(config, client)

    vectors = store.catalog_vectors(["key-1"])

    assert client.request["body"]["docvalue_fields"] == [
        "sentenceKey",
        {"field": "sentenceContentVector", "format": "binary"},
    ]
    assert len(vectors["key-1"]) == 512
    assert vectors["key-1"][0] == pytest.approx(0.5)


def test_tsp_deletion_uses_camel_case_opensearch_fields():
    client = DeleteByQueryClient()
    store = OpenSearchStore(config, client)

    deleted = store.delete_occurrences("A1", "TSP-1")

    filters = client.request["body"]["query"]["bool"]["filter"]
    assert filters == [
        {"term": {"applicationId": "A1"}},
        {"term": {"tspId": "TSP-1"}},
    ]
    assert deleted == 3


def test_application_page_excludes_tracer_and_form_language():
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
    assert {"term": {"isTracer": False}} in filters
    assert {"term": {"isFormLanguage": False}} in filters


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
    assert {"term": {"isTracer": False}} in filters
    assert {"term": {"isFormLanguage": False}} in filters
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
    assert {"term": {"isTracer": False}} in filters
    assert {"term": {"isFormLanguage": False}} in filters
    assert {"term": {"applicationId": "A1"}} in body["query"]["bool"]["must_not"]


def test_occurrence_bulk_uses_global_id_as_an_index_upsert(monkeypatch):
    actions_sent = []

    def capture_bulk(client, actions, chunk_size, raise_on_error):
        actions_sent.extend(actions)

    monkeypatch.setattr(
        "sentence_search.opensearch_store.helpers.bulk",
        capture_bulk,
    )
    store = OpenSearchStore(config, object())
    first = {"globalId": 101, "sentenceContent": "Old sentence"}
    replacement = {"globalId": 101, "sentenceContent": "New sentence"}

    store.bulk_index_occurrences([first, replacement])

    assert [action["_id"] for action in actions_sent] == ["101", "101"]
    assert all(action["_op_type"] == "index" for action in actions_sent)
    assert actions_sent[-1]["_source"] == replacement
