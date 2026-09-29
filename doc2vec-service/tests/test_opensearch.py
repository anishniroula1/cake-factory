import pytest
from opensearchpy import TransportError

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.opensearch import OpenSearchHelper


class ClientFake:
    def __init__(self, found=None):
        self.found = found or set()

    def mget(self, index, body, params):
        return {
            "docs": [
                {"_id": value, "found": value in self.found}
                for value in body["ids"]
            ]
        }


def noWait(value):
    pass


def test_existing_ids_and_bulk_pipeline_selection(
    monkeypatch,
    serviceConfig,
    customLogger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append((actions, arguments))
        return len(actions), []

    monkeypatch.setattr("doc2vec_service.opensearch.helpers.bulk", bulk)
    helper = OpenSearchHelper(serviceConfig, ClientFake({"A"}), customLogger)
    assert helper.existingIds("catalog", ["A", "B", "A"]) == {"A"}

    documents = [{"_id": "A", "value": 1}]
    assert helper.bulkIndex("catalog", documents, needsEmbedding=False) == 1
    assert calls[0][1]["pipeline"] == "_none"
    assert "_id" not in calls[0][0][0]["_source"]

    helper.bulkIndex("catalog", documents, needsEmbedding=True)
    assert "pipeline" not in calls[1][1]


def test_bulk_retries_only_temporary_failed_documents(
    monkeypatch,
    serviceConfig,
    customLogger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append(actions)
        if len(calls) == 1:
            return 0, [{"index": {"_id": "B", "status": 500}}]
        return len(actions), []

    monkeypatch.setattr("doc2vec_service.opensearch.helpers.bulk", bulk)
    monkeypatch.setattr("doc2vec_service.opensearch.time.sleep", noWait)
    helper = OpenSearchHelper(serviceConfig, ClientFake(), customLogger)
    documents = [{"_id": "A"}, {"_id": "B"}]
    assert helper.bulkIndex("catalog", documents, needsEmbedding=False) == 2
    assert [action["_id"] for action in calls[1]] == ["B"]


def test_bulk_retries_transport_failure(
    monkeypatch,
    serviceConfig,
    customLogger,
):
    calls = []

    def bulk(client, actions, **arguments):
        calls.append(actions)
        if len(calls) == 1:
            raise TransportError(503, "temporary")
        return len(actions), []

    monkeypatch.setattr("doc2vec_service.opensearch.helpers.bulk", bulk)
    monkeypatch.setattr("doc2vec_service.opensearch.time.sleep", noWait)
    helper = OpenSearchHelper(serviceConfig, ClientFake(), customLogger)
    assert helper.bulkIndex("catalog", [{"_id": "A"}], False) == 1


def test_permanent_opensearch_errors_are_not_retried(
    monkeypatch,
    serviceConfig,
    customLogger,
):
    def badMapping(client, actions, **arguments):
        return 0, [
            {"index": {"_id": "A", "status": 400, "error": "bad mapping"}}
        ]

    monkeypatch.setattr("doc2vec_service.opensearch.helpers.bulk", badMapping)
    helper = OpenSearchHelper(serviceConfig, ClientFake(), customLogger)
    with pytest.raises(PermanentDataError):
        helper.bulkIndex("catalog", [{"_id": "A"}], False)

    def unauthorized(client, actions, **arguments):
        raise TransportError(401, "unauthorized")

    monkeypatch.setattr("doc2vec_service.opensearch.helpers.bulk", unauthorized)
    with pytest.raises(PermanentDataError):
        helper.bulkIndex("catalog", [{"_id": "A"}], False)


def test_vector_validation_and_retryable_status(serviceConfig, customLogger):
    helper = OpenSearchHelper(serviceConfig, ClientFake(), customLogger)
    helper.validateVector([0.1, 0.2])
    with pytest.raises(PermanentDataError):
        helper.validateVector([0.1])
    assert helper._isRetryable(TransportError(424, "retry"))
    assert not helper._isRetryable(TransportError(400, "stop"))
