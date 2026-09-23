import pytest
from opensearchpy.helpers.errors import BulkIndexError

from sentence_migration import opensearch_store
from sentence_migration.opensearch_store import OpenSearchMigrationStore


class FakeIndices:
    def __init__(self, existing):
        self.existing = existing

    def exists(self, index):
        return index in self.existing


class FakeClient:
    def __init__(self, existing):
        self.indices = FakeIndices(existing)


def test_migration_refuses_missing_index_instead_of_auto_creating(config_stub):
    store = OpenSearchMigrationStore(config_stub, FakeClient({"occurrences-v1"}))

    with pytest.raises(ValueError, match="never creates indexes.*catalog-v1"):
        store.require_existing_indices("occurrences-v1", "catalog-v1")


def test_catalog_bulk_uses_requested_index_and_existing_vector_without_pipeline(
    config_stub,
    monkeypatch,
):
    calls = []

    def fake_bulk(client, actions, **kwargs):
        calls.append((client, actions, kwargs))

    monkeypatch.setattr(opensearch_store.helpers, "bulk", fake_bulk)
    client = FakeClient({"catalog-v1"})
    store = OpenSearchMigrationStore(config_stub, client)

    count = store.bulk_index_catalog(
        "catalog-v1",
        [
            {
                "sentenceKey": "abc",
                "sentenceContent": "Sentence",
                "sentenceContentVector": [0.1, 0.2],
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
            }
        ],
    )

    assert count == 1
    assert calls[0][1][0]["_index"] == "catalog-v1"
    assert calls[0][1][0]["_id"] == "abc"
    assert calls[0][1][0]["_source"]["sentenceContentVector"] == [0.1, 0.2]
    assert "pipeline" not in calls[0][2]


@pytest.mark.parametrize("status", [408, 424, 429, 500, 502, 503, 504])
def test_bulk_retries_only_configured_temporary_statuses(config_stub, status):
    store = OpenSearchMigrationStore(config_stub, FakeClient(set()))
    actions = [
        {"_id": "1", "_source": {"globalId": 1}},
        {"_id": "2", "_source": {"globalId": 2}},
    ]
    error = BulkIndexError(
        "temporary failure",
        [{"index": {"_id": "2", "status": status}}],
    )

    assert store._retryable_actions(error, actions) == [actions[1]]


def test_bulk_does_not_retry_an_unconfigured_status(config_stub):
    store = OpenSearchMigrationStore(config_stub, FakeClient(set()))
    actions = [{"_id": "1", "_source": {"globalId": 1}}]
    error = BulkIndexError(
        "permanent failure",
        [{"index": {"_id": "1", "status": 400}}],
    )

    assert store._retryable_actions(error, actions) is None
