from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from bucket_demo.database import getSession, initializeDatabase
from bucket_demo.init_service import generateFakeListRequest, generateFakeRequest
from bucket_demo.list_sql_function_service import (
    insertListMatchesWithSqlFunction,
)
from bucket_demo.schemas import InitRequest, ListMatchRequest, MatchRequest
from bucket_demo.sql_function_service import insertMatchesWithSqlFunction

router = APIRouter()
DatabaseSession = Annotated[Session, Depends(getSession)]


def databaseFailure(error):
    """Return a short database error without a long internal stack trace."""

    return HTTPException(
        status_code=503,
        detail=f"PostgreSQL request failed: {type(error).__name__}",
    )


@router.get("/health", tags=["1. Health"])
def health(session: DatabaseSession):
    """Check PostgreSQL only when this endpoint is called manually."""

    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        raise databaseFailure(error) from error

    return {"status": "healthy"}


@router.post("/init", tags=["2. Generate fake request"])
def initialize(request: InitRequest, session: DatabaseSession):
    """Create the schema and return fake keys for either insert endpoint."""

    try:
        initializeDatabase()

        if request.clearExisting:
            session.execute(
                text(
                    """
                    TRUNCATE TABLE
                        test_matches.sentence_key_matches,
                        test_matches.titan_matches,
                        test_matches.sentence_matches,
                        test_matches.titan_key_matches,
                        test_matches.titan_key_match_buckets
                    """
                )
            )
            session.commit()

        fakeRequest = generateFakeRequest(request)
        fakeListRequest = generateFakeListRequest(request)
    except SQLAlchemyError as error:
        session.rollback()
        raise databaseFailure(error) from error
    except Exception as error:
        session.rollback()
        raise databaseFailure(error) from error

    return {
        "status": "initialized",
        "tableWasCleared": request.clearExisting,
        "message": (
            "Copy the matching request into its insert endpoint. "
            "The fake keys have not been inserted yet."
        ),
        "insertRequest": fakeRequest,
        "listInsertRequest": fakeListRequest,
    }


@router.post("/matches/sql-function", tags=["3. PostgreSQL function"])
def sqlFunctionInsert(
    request: MatchRequest,
    session: DatabaseSession,
):
    """Insert bidirectional bucket matches using the PostgreSQL function."""

    try:
        return insertMatchesWithSqlFunction(session, request)
    except SQLAlchemyError as error:
        session.rollback()
        raise databaseFailure(error) from error


@router.post(
    "/list-matches/sql-function",
    tags=["4. JSONB list PostgreSQL function"],
)
def listSqlFunctionInsert(
    request: ListMatchRequest,
    session: DatabaseSession,
):
    """Save bidirectional lists and generate verbatim IDs in PostgreSQL."""

    try:
        return insertListMatchesWithSqlFunction(session, request)
    except SQLAlchemyError as error:
        session.rollback()
        raise databaseFailure(error) from error
