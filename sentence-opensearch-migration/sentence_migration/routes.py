import boto3
from fastapi import APIRouter, HTTPException
from opensearchpy.exceptions import OpenSearchException

from sentence_migration.config import (
    BULK_RETRY_ATTEMPTS,
    BULK_RETRY_SECONDS,
    BULK_RETRY_STATUSES,
    MIGRATION_WORKERS,
    OPENSEARCH_PORT,
    OPENSEARCH_SERVICE,
    config,
)
from sentence_migration.external_api import ExternalSentenceApi
from sentence_migration.migration_service import SentenceMigrationService
from sentence_migration.models import (
    OpenSearchRoleSelection,
    OpenSearchSearchRequest,
    ReadOnlyOpenSearchRequest,
    SeedSentencesRequest,
)
from sentence_migration.open_search_client import OpenSearchClient
from sentence_migration.opensearch_store import OpenSearchMigrationStore
from sentence_migration.readonly_policy import validate_read_only_request
from sentence_migration.s3_csv import S3TspIdReader

router = APIRouter()


def _client_for_role(iam_role: str):
    return OpenSearchClient(config, iam_role).create_client()


def _service_error(error: Exception):
    if isinstance(error, ValueError):
        return HTTPException(status_code=400, detail=str(error))
    return HTTPException(status_code=503, detail=str(error))


@router.get("/health", tags=["System"])
def health():
    """Report API health without constructing or pinging an OpenSearch client."""

    return {
        "status": "ok",
        "opensearchChecked": False,
        "message": "Use POST /opensearch/ping to test an environment-approved role.",
    }


@router.get("/configuration/options", tags=["System"])
def configuration_options():
    """Return the same role and alias choices shown by Swagger dropdowns."""

    return {
        "iamRoles": list(config.opensearch_iam_roles),
        "occurrenceAliases": list(config.occurrence_aliases),
        "catalogAliases": list(config.catalog_aliases),
        "migrationWorkers": MIGRATION_WORKERS,
        "bulkRetryAttempts": BULK_RETRY_ATTEMPTS,
        "bulkRetrySeconds": BULK_RETRY_SECONDS,
        "openSearchBulkRetryStatuses": list(BULK_RETRY_STATUSES),
    }


@router.post("/opensearch/ping", tags=["OpenSearch"])
def ping_opensearch(request: OpenSearchRoleSelection):
    """Ping AWS OpenSearch using an environment-approved IAM role."""

    try:
        client = _client_for_role(request.iamRole)
        reachable = bool(client.ping())
        return {
            "reachable": reachable,
            "host": config.normalized_opensearch_host,
            "port": OPENSEARCH_PORT,
            "service": OPENSEARCH_SERVICE,
            "iamRole": request.iamRole,
        }
    except Exception as error:
        raise _service_error(error) from error


@router.post("/seed/sentences", tags=["Migration"])
def seed_sentences(request: SeedSentencesRequest):
    """Run one paginated external-API migration worker per S3-listed TSP ID."""

    try:
        config.validate_migration()
        client = _client_for_role(request.iamRole)
        # S3 deliberately uses the application's ambient credentials. It does
        # not use or assume the selected OpenSearch IAM role.
        s3_client = boto3.Session(region_name=config.aws_region).client("s3")
        tsp_reader = S3TspIdReader(s3_client, config.s3_bucket)
        store = OpenSearchMigrationStore(config, client)
        with ExternalSentenceApi(config) as external_api:
            service = SentenceMigrationService(
                config,
                tsp_reader,
                external_api,
                store,
            )
            return service.migrate(
                request.s3File,
                request.occurrenceAlias,
                request.catalogAlias,
            )
    except (ValueError, RuntimeError, OpenSearchException) as error:
        raise _service_error(error) from error
    except Exception as error:
        raise _service_error(error) from error


@router.post("/opensearch/search", tags=["OpenSearch"])
def search_opensearch(request: OpenSearchSearchRequest):
    """Execute an OpenSearch query against an environment-approved alias."""

    try:
        client = _client_for_role(request.iamRole)
        return client.search(index=request.alias, body=request.body)
    except (ValueError, RuntimeError, OpenSearchException) as error:
        raise _service_error(error) from error
    except Exception as error:
        raise _service_error(error) from error


@router.post("/opensearch/read-only-request", tags=["OpenSearch"])
def read_only_opensearch(request: ReadOnlyOpenSearchRequest):
    """Call only explicitly allowed read-only OpenSearch API paths."""

    try:
        endpoint = validate_read_only_request(request.method, request.endpoint)
        client = _client_for_role(request.iamRole)
        response = client.transport.perform_request(
            request.method.value,
            endpoint,
            params=request.params,
            body=request.body,
        )
        return {
            "method": request.method.value,
            "endpoint": endpoint,
            "response": response,
        }
    except (ValueError, RuntimeError, OpenSearchException) as error:
        raise _service_error(error) from error
    except Exception as error:
        raise _service_error(error) from error
