import pytest

from sentence_migration.models import ReadOnlyMethod
from sentence_migration.readonly_policy import validate_read_only_request


@pytest.mark.parametrize(
    ("method", "endpoint"),
    [
        (ReadOnlyMethod.GET, "/_cluster/health"),
        (ReadOnlyMethod.GET, "/_cat/indices"),
        (ReadOnlyMethod.GET, "/_cluster/allocation/explain"),
        (ReadOnlyMethod.GET, "/_remote/info"),
        (ReadOnlyMethod.GET, "/sentence-occurrences/_mapping"),
        (ReadOnlyMethod.GET, "/sentence-occurrences/_doc/12"),
        (ReadOnlyMethod.POST, "/sentence-occurrences/_search"),
        (ReadOnlyMethod.GET, "/_search"),
        (ReadOnlyMethod.POST, "/_msearch"),
        (ReadOnlyMethod.POST, "/sentence-occurrences/_count"),
        (ReadOnlyMethod.POST, "/sentence-occurrences/_analyze"),
    ],
)
def test_read_only_policy_allows_inspection_and_search(method, endpoint):
    assert validate_read_only_request(method, endpoint) == endpoint


def test_read_only_policy_preserves_case_sensitive_document_id():
    endpoint = "/sentence-occurrences/_doc/CaseSensitive-ID"

    assert validate_read_only_request(ReadOnlyMethod.GET, endpoint) == endpoint


@pytest.mark.parametrize(
    ("method", "endpoint"),
    [
        (ReadOnlyMethod.POST, "/sentence-occurrences/_delete_by_query"),
        (ReadOnlyMethod.POST, "/_bulk"),
        (ReadOnlyMethod.POST, "/sentence-occurrences/_settings"),
        (ReadOnlyMethod.POST, "/_aliases"),
        (ReadOnlyMethod.GET, "https://attacker.example/_search"),
        (ReadOnlyMethod.GET, "/_cluster/reroute"),
        (ReadOnlyMethod.GET, "/sentence-occurrences/_refresh"),
    ],
)
def test_read_only_policy_blocks_writes_and_unapproved_paths(method, endpoint):
    with pytest.raises(ValueError):
        validate_read_only_request(method, endpoint)
