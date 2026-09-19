import base64
import json
import struct

import pytest
from opensearchpy.helpers.errors import BulkIndexError

from sentence_search.config import config
from sentence_search.opensearch_store import (
    OpenSearchStore,
    catalog_index_definition,
    occurrence_index_definition,
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
            }

        def get_alias(self, name):
            if name == config.occurrence_alias:
                return {"sentence_occurrences-old-v1": {}}
            return {"sentence_semantic_catalog-hnsw-v1": {}}

        def exists(self, index):
            return True

        def delete(self, index):
            self.deleted.append(index)

    def __init__(self):
        self.indices = self.Indices()


class ClientThatMustNotRun:
    def mget(self, **request):
        raise AssertionError("Duplicate input should fail before OpenSearch")


class ExistingOccurrenceClient:
    def mget(self, **request):
        assert request["body"]["ids"] == ["1"]
        return {
            "docs": [
                {
                    "_id": "1",
                    "found": True,
                    "_source": {
                        "applicationId": "A1",
                        "tspId": "T1",
                        "sectionName": "Affidavit",
                        "analysisGroup": "Asylee",
                        "sentenceKey": "key-1",
                        "isTracer": False,
                        "isFormLanguage": False,
                    },
                }
            ]
        }


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


def test_occurrence_mapping_has_requested_sentence_fields():
    properties = occurrence_index_definition(config)["mappings"]["properties"]
    assert set(properties) == {
        "applicationId",
        "tspId",
        "sectionName",
        "globalId",
        "sentIdLocal",
        "sentenceContent",
        "isTracer",
        "isFormLanguage",
        "sentenceKey",
        "sourceType",
        "createdAt",
        "updatedAt",
        "analysisGroup",
    }
    assert properties["globalId"] == {"type": "long"}


def test_catalog_mapping_uses_trained_ivf_model():
    definition = catalog_index_definition(config)
    vector = definition["mappings"]["properties"]["sentenceContentVector"]
    assert vector == {
        "type": "knn_vector",
        "model_id": "test-ivf-model",
    }
    assert definition["settings"]["index.default_pipeline"] == (
        "sentence_bedrock_embedding_pipeline"
    )


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


def test_reset_deletes_configured_indexes_and_existing_alias_targets():
    client = AliasIndexClient()
    store = OpenSearchStore(config, client)

    result = store.delete_indices()

    assert result["deletedIndexes"] == sorted(
        {
            config.occurrence_index,
            config.catalog_index,
            "sentence_occurrences-old-v1",
            "sentence_semantic_catalog-hnsw-v1",
        }
    )
    assert client.indices.deleted == result["deletedIndexes"]


def test_catalog_retry_keeps_only_temporary_failed_documents():
    store = OpenSearchStore(config, EmptySearchClient())
    actions = [
        {"_id": "key-1", "_source": {"sentenceKey": "key-1"}},
        {"_id": "key-2", "_source": {"sentenceKey": "key-2"}},
    ]
    error = BulkIndexError(
        "one temporary failure",
        [{"index": {"_id": "key-2", "status": 500}}],
    )

    retry_actions = store._retryable_catalog_actions(error, actions)

    assert retry_actions == [actions[1]]


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


def test_duplicate_global_id_cannot_have_different_sentence_keys():
    store = OpenSearchStore(config, ClientThatMustNotRun())
    first = _occurrence_identity("key-1")
    second = _occurrence_identity("key-2")

    with pytest.raises(ValueError, match="different sentence data"):
        store.validate_occurrence_identity([first, second])


def test_numeric_global_id_uses_string_document_id_for_mget():
    store = OpenSearchStore(config, ExistingOccurrenceClient())

    store.validate_occurrence_identity([_occurrence_identity("key-1")])


def _occurrence_identity(sentence_key):
    return {
        "globalId": 1,
        "applicationId": "A1",
        "tspId": "T1",
        "sectionName": "Affidavit",
        "analysisGroup": "Asylee",
        "sentenceKey": sentence_key,
        "isTracer": False,
        "isFormLanguage": False,
    }
