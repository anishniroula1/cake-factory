from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from opensearchpy.exceptions import OpenSearchException
from starlette.background import BackgroundTask

from sentence_search.components import match_csv_export_service
from sentence_search.config import DEFAULT_MATCH_THRESHOLD

router = APIRouter()


def delete_temporary_csv(path: Path) -> None:
    """Remove the server-side temporary file after the download finishes."""

    path.unlink(missing_ok=True)


def csv_response(export) -> FileResponse:
    """Return one generated CSV and delete its temporary copy afterward."""

    return FileResponse(
        path=export.path,
        media_type="text/csv; charset=utf-8",
        filename=export.file_name,
        headers={"X-Match-Count": str(export.match_count)},
        background=BackgroundTask(delete_temporary_csv, export.path),
    )


@router.get(
    "/documents/{tspId}/sentences/matches.csv",
    tags=["CSV exports"],
    response_class=FileResponse,
)
def tsp_match_csv(
    tspId: str,
    analysisGroup: Annotated[str, Query(min_length=1)] = "Asylee",
    threshold: Annotated[int, Query(ge=1, le=100)] = DEFAULT_MATCH_THRESHOLD,
):
    """Download every eligible sentence match for one TSP document."""

    try:
        export = match_csv_export_service.create_tsp_csv(
            tspId,
            analysisGroup,
            threshold,
        )
        return csv_response(export)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError, OSError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.get(
    "/applications/{applicationId}/sentences/matches.csv",
    tags=["CSV exports"],
    response_class=FileResponse,
)
def application_match_csv(
    applicationId: str,
    analysisGroup: Annotated[str, Query(min_length=1)] = "Asylee",
    threshold: Annotated[int, Query(ge=1, le=100)] = DEFAULT_MATCH_THRESHOLD,
):
    """Download every eligible sentence match for one application."""

    try:
        export = match_csv_export_service.create_application_csv(
            applicationId,
            analysisGroup,
            threshold,
        )
        return csv_response(export)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError, OSError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
