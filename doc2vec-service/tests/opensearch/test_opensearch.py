import pytest
from opensearchpy import OpenSearchException, TransportError

from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper


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

    monkeypatch.setattr("doc2vec_completed.opensearch.opensearch.helpers.bulk", bulk)
    helper = OpenSearchHelper(service_config, ClientFake({"A"}), custom_logger)
    assert helper.existing_ids("catalog", ["A", "B", "A"]) == {"A"}

    documents = [{"_id": "A", "value": 1}]
    assert helper.bulk_index("catalog", documents, needs_embedding=False) == 1
    assert calls[0][1]["pipeline"] == "_none"
    assert "_id" not in calls[0][0][0]["_source"]

    helper.bulk_index("catalog", documents, needs_embedding=True)
    assert "pipeline" not in calls[1][1]


def test_empty_opensearch_operations_are_skipped(
    service_config,
    custom_logger,
):
    helper = OpenSearchHelper(service_config, ClientFake(), custom_logger)

    assert helper.existing_ids("catalog", []) == set()
    assert helper.bulk_index("catalog", [], needs_embedding=False) == 0


def test_refresh_makes_catalog_and_occurrences_searchable(
    service_config,
    custom_logger,
):
    client = ClientFake()
    helper = OpenSearchHelper(service_config, client, custom_logger)

    helper.refresh_indices("catalog-asylee")

    assert client.refreshed_indices == ["catalog-asylee,occurrence"]
    assert any(
        "Newly indexed sentences are now available" in message
        for message in custom_logger.info_messages
    )


def test_bulk_reports_temporary_failed_documents_to_the_caller(
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

    monkeypatch.setattr("doc2vec_completed.opensearch.opensearch.helpers.bulk", bulk)
    helper = OpenSearchHelper(service_config, ClientFake(), custom_logger)
    documents = [{"_id": "A"}, {"_id": "B"}]
    with pytest.raises(OpenSearchException):
        helper.bulk_index("catalog", documents, needs_embedding=False)
    assert len(calls) == 1


def test_bulk_reports_transport_failure_to_the_caller(
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

    monkeypatch.setattr("doc2vec_completed.opensearch.opensearch.helpers.bulk", bulk)
    helper = OpenSearchHelper(service_config, ClientFake(), custom_logger)
    with pytest.raises(TransportError):
        helper.bulk_index("catalog", [{"_id": "A"}], False)
    assert len(calls) == 1


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
    helper = OpenSearchHelper(service_config, retry_client, custom_logger)

    with pytest.raises(TransportError):
        helper.existing_ids("catalog", ["A"])
    assert retry_client.attempts == 1

    class FailedLookupClient(ClientFake):
        def mget(self, index, body, params):
            raise TransportError(401, "unauthorized")

    failed_helper = OpenSearchHelper(
        service_config,
        FailedLookupClient(),
        custom_logger,
    )
    with pytest.raises(PermanentDataError):
        failed_helper.existing_ids("catalog", ["A"])


def test_permanent_opensearch_errors_are_not_retried(
    monkeypatch,
    service_config,
    custom_logger,
):
    def bad_mapping(client, actions, **arguments):
        return 0, [{"index": {"_id": "A", "status": 400, "error": "bad mapping"}}]

    monkeypatch.setattr(
        "doc2vec_completed.opensearch.opensearch.helpers.bulk", bad_mapping
    )
    helper = OpenSearchHelper(service_config, ClientFake(), custom_logger)
    with pytest.raises(PermanentDataError):
        helper.bulk_index("catalog", [{"_id": "A"}], False)

    def unauthorized(client, actions, **arguments):
        raise TransportError(401, "unauthorized")

    monkeypatch.setattr(
        "doc2vec_completed.opensearch.opensearch.helpers.bulk", unauthorized
    )
    with pytest.raises(PermanentDataError):
        helper.bulk_index("catalog", [{"_id": "A"}], False)


def test_vector_validation_and_retryable_status(service_config, custom_logger):
    helper = OpenSearchHelper(service_config, ClientFake(), custom_logger)
    helper.validate_vector([0.1, 0.2])
    with pytest.raises(PermanentDataError):
        helper.validate_vector([0.1])
    assert helper._is_retryable(TransportError(424, "retry"))
    assert not helper._is_retryable(TransportError(400, "stop"))
