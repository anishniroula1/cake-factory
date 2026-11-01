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
    client.search.side_effect = [
        {
            "hits": {
                "hits": [{"_id": "10", "_source": {"globalId": 10, "sentenceKey": "key-a", "analysisGroup": "Asylee"}}]
            }
        },
        {"aggregations": {"remaining_keys": {"buckets": [{"key": {"sentenceKey": "key-b"}}]}}},
    ]
    client.bulk.return_value = {"errors": False, "items": []}
    store = SentenceOccurrenceOpenSearchStore(client)

    occurrence_records = store.load_document_occurrences(20)
    deleted_occurrence_count = store.delete_occurrences_by_global_id([10])
    shared_sentence_keys = store.find_sentence_keys_used_by_other_documents(["key-a", "key-b"], "Asylee", 20)

    assert (occurrence_records[0]["global_id"], deleted_occurrence_count, shared_sentence_keys) == (10, 1, {"key-b"})
    assert client.bulk.call_args.kwargs["refresh"] == "wait_for"
    reference_query = client.search.call_args_list[1].kwargs["body"]["query"]["bool"]
    assert reference_query["must_not"] == [{"term": {"tspId": 20}}]


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


def test_occurrence_bulk_error_is_reported():
    client = Mock()
    client.bulk.return_value = {
        "errors": True,
        "items": [{"delete": {"_id": "10", "status": 500, "error": {"type": "failure"}}}],
    }

    with pytest.raises(OpenSearchException, match="1 item"):
        SentenceOccurrenceOpenSearchStore(client).delete_occurrences_by_global_id([10])
