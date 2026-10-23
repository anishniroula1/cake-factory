from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from opensearchpy.exceptions import OpenSearchException

from sentence_search.components import (
    all_sentence_summary_service,
    sentence_summary_service,
)
from sentence_search.config import DEFAULT_MATCH_THRESHOLD

router = APIRouter()


@router.get(
    "/applications/{applicationId}/sentences/semantic-summary",
    tags=["Sentence summaries"],
)
def application_semantic_summary(
    applicationId: str,
    analysisGroup: Annotated[str, Query(min_length=1)] = "Asylee",
    threshold: Annotated[int, Query(ge=1, le=100)] = DEFAULT_MATCH_THRESHOLD,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 100,
    nextToken: str | None = None,
):
    """Return one page and match counts for only that page."""

    try:
        return sentence_summary_service.application_summary(
            applicationId,
            analysisGroup,
            threshold,
            pageSize,
            nextToken,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.get(
    "/applications/{applicationId}/sentences/semantic-summary-all",
    tags=["Sentence summaries"],
)
def all_application_semantic_summary(
    applicationId: str,
    analysisGroup: Annotated[str, Query(min_length=1)] = "Asylee",
    threshold: Annotated[int, Query(ge=1, le=100)] = DEFAULT_MATCH_THRESHOLD,
):
    """Return summaries and totals for every eligible application sentence."""

    try:
        return all_sentence_summary_service.application_summary(
            applicationId,
            analysisGroup,
            threshold,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (OpenSearchException, RuntimeError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
