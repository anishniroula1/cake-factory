from unittest.mock import Mock

import pytest
from opensearchpy.exceptions import OpenSearchException

from doc_delete_job.opensearch.errors import PermanentDataError
from doc_delete_job.opensearch.opensearch_store.sentence_catalog_open_search_store import SentenceCatalogOpenSearchStore
from doc_delete_job.opensearch.opensearch_store.sentence_occurrence_open_search_store import (
    SentenceOccurrenceOpenSearchStore,
)


def test_occurrence_store_loads_deletes_and_checks_remaining_keys():
    client = Mock()
    client.search.return_value = {
        "hits": {
            "hits": [{"_id": "10", "_source": {"sentenceKey": "key-a", "analysisGroup": "Asylee"}}]
        }
    }
    client.delete_by_query.return_value = {"deleted": 1, "failures": []}

    def msearch(**values):
        queries = values["body"][1::2]
        responses = []
        for query in queries:
            sentence_key = query["query"]["bool"]["filter"][0]["term"]["sentenceKey"]
            hits = [{"_id": "other-occurrence"}] if sentence_key == "key-b" else []
            responses.append({"hits": {"hits": hits}})
        return {"responses": responses}

    client.msearch.side_effect = msearch
    store = SentenceOccurrenceOpenSearchStore(client)

    occurrence_records = store.load_document_occurrences(20)
    deleted_occurrence_count = store.delete_occurrences_by_tsp_id(20)
    shared_sentence_keys = store.find_sentence_keys_used_by_other_documents(["key-a", "key-b"], "Asylee", 20)

    assert occurrence_records == [{"sentence_key": "key-a", "analysis_group": "Asylee"}]
    assert (deleted_occurrence_count, shared_sentence_keys) == (1, {"key-b"})
    assert client.search.call_args.kwargs["body"]["_source"] == ["sentenceKey", "analysisGroup"]
    assert client.delete_by_query.call_args.kwargs["body"] == {"query": {"term": {"tspId": 20}}}
    reference_queries = [
        query
        for msearch_call in client.msearch.call_args_list
        for query in msearch_call.kwargs["body"][1::2]
    ]
    assert {query["query"]["bool"]["filter"][0]["term"]["sentenceKey"] for query in reference_queries} == {
        "key-a",
        "key-b",
    }
    assert all(
        query["query"]["bool"]["filter"][1] == {"term": {"analysisGroup": "Asylee"}}
        and query["query"]["bool"]["must_not"] == [{"term": {"tspId": 20}}]
        for query in reference_queries
    )


def test_catalog_store_selects_group_and_reports_bulk_error():
    client = Mock()
    client.bulk.return_value = {"errors": False, "items": []}
    store = SentenceCatalogOpenSearchStore(client)

    assert store.delete_catalog_documents_by_sentence_key("Refugee", ["key-a", "key-a"]) == 1
    assert client.bulk.call_args.kwargs["body"][0]["delete"]["_index"] == "sentence_semantic_catalog_refugee"

    with pytest.raises(PermanentDataError):
        store.select_catalog_alias("Unknown")

    client.bulk.return_value = {
        "errors": True,
        "items": [{"delete": {"_id": "key-a", "status": 500, "error": {"type": "failure"}}}],
    }
    with pytest.raises(OpenSearchException, match="1 item"):
        store.delete_catalog_documents_by_sentence_key("Asylee", ["key-a"])


@pytest.mark.parametrize(
    "delete_response, message",
    [
        ({"deleted": 0, "failures": [{"reason": "failure"}]}, "shard_failures=1"),
        ({"deleted": 0, "version_conflicts": 1}, "version_conflicts=1"),
        ({"deleted": 0, "timed_out": True}, "timed_out=True"),
    ],
)
def test_occurrence_delete_by_query_error_is_reported(delete_response, message):
    client = Mock()
    client.delete_by_query.return_value = delete_response

    with pytest.raises(OpenSearchException, match=message):
        SentenceOccurrenceOpenSearchStore(client).delete_occurrences_by_tsp_id(20)


def test_reference_msearch_caps_parallel_requests_at_thirty():
    client = Mock()

    def msearch(**values):
        return {"responses": [{"hits": {"hits": []}} for query in values["body"][1::2]]}

    client.msearch.side_effect = msearch
    sentence_keys = [f"key-{index}" for index in range(31)]

    shared_keys = SentenceOccurrenceOpenSearchStore(client).find_sentence_keys_used_by_other_documents(
        sentence_keys, "Refugee", 8205
    )

    assert shared_keys == set()
    assert client.msearch.call_count == 30
    searched_keys = {
        query["query"]["bool"]["filter"][0]["term"]["sentenceKey"]
        for msearch_call in client.msearch.call_args_list
        for query in msearch_call.kwargs["body"][1::2]
    }
    assert searched_keys == set(sentence_keys)


@pytest.mark.parametrize(
    "response, message",
    [
        ({"responses": []}, "0 response"),
        ({"responses": [{"error": {"type": "failure"}}]}, "msearch failed"),
    ],
)
def test_reference_msearch_errors_are_reported(response, message):
    client = Mock()
    client.msearch.return_value = response

    with pytest.raises(OpenSearchException, match=message):
        SentenceOccurrenceOpenSearchStore(client).find_sentence_keys_used_by_other_documents(
            ["key-a"], "Asylee", 20
        )
