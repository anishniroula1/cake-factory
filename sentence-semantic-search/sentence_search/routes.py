from enum import StrEnum
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from opensearchpy.exceptions import OpenSearchException

from sentence_search.components import (
    deletion_service,
    opensearch_store,
    seed_service,
    sentence_service,
    tracer_seed_service,
)
from sentence_search.config import config
from sentence_search.models import SentenceOccurrence

router = APIRouter()
PROJECT_ROOT = Path(__file__).resolve().parent.parent


class IndexSelection(StrEnum):
    occurrences = config.occurrence_alias
    semantic_catalog = config.catalog_alias
    tracer_catalog = config.tracer_alias


@router.get("/health", tags=["System"])
def health():
    """Check OpenSearch without calling Titan."""

    try:
        opensearch_ready = opensearch_store.client.ping()
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "ok" if opensearch_ready else "down",
        "opensearch": (f"{config.opensearch_host}:{config.opensearch_port}"),
        "occurrenceAlias": config.occurrence_alias,
        "catalogAlias": config.catalog_alias,
        "tracerAlias": config.tracer_alias,
        "ivfNprobes": config.ivf_nprobes,
        "modelInvoked": False,
    }


@router.get("/stats", tags=["System"])
def stats():
    """Show occurrence, sentence catalog, and tracer catalog counts."""

    try:
        return opensearch_store.stats()
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.get("/index-documents", tags=["System"])
def index_documents(
    index: Annotated[
        IndexSelection,
        Query(description="Choose an occurrence, sentence catalog, or tracer alias."),
    ],
    count: Annotated[int, Query(ge=1, le=100)] = 10,
):
    """Preview unfiltered documents, including either catalog embedding."""

    try:
        return {
            "index": index.value,
            **opensearch_store.preview(index.value, count),
        }
    except OpenSearchException as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.delete("/admin/storage", tags=["Admin"])
def delete_storage(
    confirm: Annotated[
        bool,
        Query(description="Must be true to delete all project indexes."),
    ] = False,
):
    """Delete indexes currently connected to the three configured aliases."""

    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Set confirm=true to delete all sentence-search data.",
        )
    try:
        deleted = opensearch_store.delete_indices()
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "message": "OpenSearch sentence data was deleted",
        **deleted,
    }


@router.post("/admin/seed", tags=["Admin"])
def seed(
    csvPath: Annotated[
        str,
        Query(
            min_length=1,
            description="Absolute CSV path or a path relative to this project.",
        ),
    ] = "data/seed.csv",
):
    """Load the CSV in its existing order into both OpenSearch indexes."""

    path = Path(csvPath).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        return seed_service.seed(
            path,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.post("/admin/tracer-sentences/seed", tags=["Admin"])
async def seed_tracer_sentences(
    csvFile: Annotated[
        UploadFile,
        File(
            description=(
                "UTF-8 CSV containing tracer_id, sentence_content, "
                "analysis_group, is_non_matcher, and is_form_language."
            )
        ),
    ],
):
    """Upload tracer sentences and let OpenSearch create Titan embeddings."""

    try:
        result = tracer_seed_service.seed(await csvFile.read())
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "fileName": csvFile.filename,
        **result,
    }


@router.post("/sentences", tags=["Sentences"])
def add_sentence(sentence: SentenceOccurrence):
    """Add one occurrence and create its catalog vector when needed."""

    try:
        return sentence_service.add_sentence(sentence)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.delete("/applications/{applicationId}", tags=["Admin"])
def delete_application(
    applicationId: str,
    confirm: Annotated[
        bool,
        Query(description="Must be true to delete the application sentences."),
    ] = False,
):
    """Delete one application's occurrences and unused catalog vectors."""

    if not confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to delete.")
    try:
        return deletion_service.delete_application(applicationId)
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.delete(
    "/documents/{tspId}",
    tags=["Admin"],
)
def delete_tsp_document(
    tspId: str,
    applicationId: Annotated[
        str | None,
        Query(description="Optional safety filter when a TSP ID is not unique."),
    ] = None,
    confirm: Annotated[
        bool,
        Query(description="Must be true to delete the TSP document sentences."),
    ] = False,
):
    """Delete one TSP document and its unused catalog vectors."""

    if not confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to delete.")
    try:
        return deletion_service.delete_tsp(tspId, applicationId)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
