from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from opensearchpy.exceptions import OpenSearchException

from sentence_search.components import opensearch_store
from sentence_search.config import config

router = APIRouter()


class IndexSelection(StrEnum):
    occurrences = config.occurrence_alias
    asylee_catalog = config.asylee_catalog_alias
    refugee_catalog = config.refugee_catalog_alias


@router.get("/health", tags=["System"])
def health():
    """Check OpenSearch without calling Titan."""

    try:
        opensearch_ready = opensearch_store.client.ping()
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "ok" if opensearch_ready else "down",
        "opensearch": config.opensearch_host,
        "occurrenceAlias": config.occurrence_alias,
        "asyleeCatalogAlias": config.asylee_catalog_alias,
        "refugeeCatalogAlias": config.refugee_catalog_alias,
        "ivfNprobesEnabled": config.ivf_nprobes_enabled,
        "ivfNprobes": (
            config.ivf_nprobes if config.ivf_nprobes_enabled else None
        ),
        "modelInvoked": False,
    }


@router.get("/stats", tags=["System"])
def stats():
    """Show occurrence and sentence catalog counts."""

    try:
        return opensearch_store.stats()
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.get("/index-documents", tags=["System"])
def index_documents(
    index: Annotated[
        IndexSelection,
        Query(description="Choose the occurrence or sentence catalog alias."),
    ],
    count: Annotated[int, Query(ge=1, le=100)] = 10,
):
    """Preview unfiltered documents, including catalog embeddings."""

    try:
        return {
            "index": index.value,
            **opensearch_store.preview(index.value, count),
        }
    except OpenSearchException as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
