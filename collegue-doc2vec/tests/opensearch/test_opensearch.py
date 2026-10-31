import pytest
from opensearchpy import TransportError

from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper

BULK_PATH = "doc2vec_completed.opensearch.opensearch_helper.helpers.bulk"


class ClientFake:
    def __init__(self, found=None):
        self.found = found or set()
        self.refreshed_indices = []
        self.indices = self

    def mget(self, index, body, params):
        return {
            "docs": [
                {"_id": value, "found": value in self.found} for value in body["ids"]
            ]
        }

    def refresh(self, index):
        self.refreshed_indices.append(index)


def no_wait(value):
    pass


def test_existing_ids_and_bulk_pipeline_selection(
    monkeypatch,
    service_config,
    custom_logger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append((actions, arguments))
        return len(actions), []

    monkeypatch.setattr(BULK_PATH, bulk)
    store = OpenSearchHelper(ClientFake({"A"}), custom_logger)
    store.find_existing_document_ids("catalog", ["A", "B", "A"])
    documents = [{"_id": "A", "value": 1}]
    store.index_documents("catalog", documents, needs_embedding=False)
    store.index_documents("catalog", documents, needs_embedding=True)

    assert [call[1].get("pipeline") for call in calls] == ["_none", None]


def test_empty_opensearch_operations_are_skipped(
    service_config,
    custom_logger,
):
    store = OpenSearchHelper(ClientFake(), custom_logger)

    store.find_existing_document_ids("catalog", [])
    store.index_documents("catalog", [], needs_embedding=False)


def test_refresh_makes_supplied_aliases_searchable(
    service_config,
    custom_logger,
):
    client = ClientFake()
    store = OpenSearchHelper(client, custom_logger)

    store.refresh_aliases(["catalog-asylee", "occurrence"])

    store.refresh_aliases(["training"])
    store.refresh_aliases([])

    assert client.refreshed_indices == ["catalog-asylee,occurrence", "training"]


def test_bulk_retries_only_temporary_failed_documents(
    monkeypatch,
    service_config,
    custom_logger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append(actions)
        if len(calls) == 1:
            return 0, [{"index": {"_id": "B", "status": 500}}]
        return len(actions), []

    monkeypatch.setattr(BULK_PATH, bulk)
    monkeypatch.setattr(
        "doc2vec_completed.opensearch.opensearch_helper.time.sleep",
        no_wait,
    )
    store = OpenSearchHelper(ClientFake(), custom_logger)
    documents = [{"_id": "A"}, {"_id": "B"}]
    store.index_documents("catalog", documents, needs_embedding=False)
    assert [[action["_id"] for action in call] for call in calls] == [
        ["A", "B"],
        ["B"],
    ]


def test_bulk_retries_the_pending_batch_after_a_transport_failure(
    monkeypatch,
    service_config,
    custom_logger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append(actions)
        if len(calls) == 1:
            raise TransportError(503, "temporary")
        return len(actions), []

    monkeypatch.setattr(BULK_PATH, bulk)
    monkeypatch.setattr(
        "doc2vec_completed.opensearch.opensearch_helper.time.sleep",
        no_wait,
    )
    store = OpenSearchHelper(ClientFake(), custom_logger)
    store.index_documents("catalog", [{"_id": "A"}], False)
    assert len(calls) == 2


def test_existing_id_lookup_returns_temporary_and_permanent_failures(
    monkeypatch,
    service_config,
    custom_logger,
):
    class RetryLookupClient(ClientFake):
        def __init__(self):
            super().__init__({"A"})
            self.attempts = 0

        def mget(self, index, body, params):
            self.attempts += 1
            if self.attempts == 1:
                raise TransportError(503, "temporary")
            return super().mget(index, body, params)

    retry_client = RetryLookupClient()
    store = OpenSearchHelper(retry_client, custom_logger)

    with pytest.raises(TransportError):
        store.find_existing_document_ids("catalog", ["A"])

    class FailedLookupClient(ClientFake):
        def mget(self, index, body, params):
            raise TransportError(401, "unauthorized")

    failed_store = OpenSearchHelper(
        FailedLookupClient(),
        custom_logger,
    )
    with pytest.raises(PermanentDataError):
        failed_store.find_existing_document_ids("catalog", ["A"])


def test_permanent_opensearch_errors_are_not_retried(
    monkeypatch,
    service_config,
    custom_logger,
):
    def bad_mapping(client, actions, **arguments):
        return 0, [{"index": {"_id": "A", "status": 400, "error": "bad mapping"}}]

    monkeypatch.setattr(BULK_PATH, bad_mapping)
    store = OpenSearchHelper(ClientFake(), custom_logger)
    with pytest.raises(PermanentDataError):
        store.index_documents("catalog", [{"_id": "A"}], False)

    def unauthorized(client, actions, **arguments):
        raise TransportError(401, "unauthorized")

    monkeypatch.setattr(BULK_PATH, unauthorized)
    with pytest.raises(PermanentDataError):
        store.index_documents("catalog", [{"_id": "A"}], False)


def test_vector_validation_and_retryable_status(service_config, custom_logger):
    store = OpenSearchHelper(ClientFake(), custom_logger)
    store.validate_vector([0.1, 0.2])
    with pytest.raises(PermanentDataError):
        store.validate_vector([0.1])
    store._is_retryable(TransportError(424, "retry"))
    store._is_retryable(TransportError(400, "stop"))

