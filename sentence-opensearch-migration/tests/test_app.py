from fastapi.testclient import TestClient

from sentence_migration.config import (
    BULK_RETRY_ATTEMPTS,
    BULK_RETRY_SECONDS,
    BULK_RETRY_STATUSES,
    MIGRATION_WORKERS,
    config,
)
from sentence_migration.main import app


def test_health_does_not_check_opensearch():
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json()["opensearchChecked"] is False


def test_configuration_options_returns_environment_choices():
    response = TestClient(app).get("/configuration/options")

    assert response.status_code == 200
    assert response.json() == {
        "iamRoles": list(config.opensearch_iam_roles),
        "occurrenceAliases": list(config.occurrence_aliases),
        "catalogAliases": list(config.catalog_aliases),
        "migrationWorkers": MIGRATION_WORKERS,
        "bulkRetryAttempts": BULK_RETRY_ATTEMPTS,
        "bulkRetrySeconds": BULK_RETRY_SECONDS,
        "openSearchBulkRetryStatuses": list(BULK_RETRY_STATUSES),
    }


def test_required_endpoints_are_in_openapi_with_dropdown_values():
    schema = app.openapi()
    paths = schema["paths"]

    assert "/configuration/options" in paths
    assert "/seed/sentences" in paths
    assert "/opensearch/ping" in paths
    assert "/opensearch/search" in paths
    assert "/opensearch/read-only-request" in paths

    role_property = schema["components"]["schemas"]["OpenSearchRoleSelection"][
        "properties"
    ]["iamRole"]
    assert role_property["enum"] == list(config.opensearch_iam_roles)
    seed_properties = schema["components"]["schemas"]["SeedSentencesRequest"][
        "properties"
    ]
    assert seed_properties["occurrenceAlias"]["enum"] == list(config.occurrence_aliases)
    assert seed_properties["catalogAlias"]["enum"] == list(config.catalog_aliases)
