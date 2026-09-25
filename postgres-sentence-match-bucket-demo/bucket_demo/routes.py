from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from bucket_demo.database import getSession
from bucket_demo.database import initializeDatabase
from bucket_demo.init_service import generateFakeRequest
from bucket_demo.python_service import insertMatchesWithPython
from bucket_demo.schemas import InitRequest
from bucket_demo.schemas import MatchRequest
from bucket_demo.sql_function_service import insertMatchesWithSqlFunction


router = APIRouter()


def databaseFailure(error):
    """Return a short database error without a long internal stack trace."""

    return HTTPException(
        status_code=503,
        detail=f"PostgreSQL request failed: {type(error).__name__}",
    )


@router.get("/health", tags=["1. Health"])
def health(session: Session = Depends(getSession)):
    """Check PostgreSQL only when this endpoint is called manually."""

    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        raise databaseFailure(error) from error

    return {"status": "healthy"}


@router.post("/init", tags=["2. Generate fake request"])
def initialize(request: InitRequest, session: Session = Depends(getSession)):
    """Create the schema and return fake keys for either insert endpoint."""

    try:
        initializeDatabase()

        if request.clearExisting:
            session.execute(text("TRUNCATE TABLE sentence_key_matches"))
            session.commit()

        fakeRequest = generateFakeRequest(request)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
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
            "Copy insertRequest into either insert endpoint. "
            "The fake keys have not been inserted yet."
        ),
        "insertRequest": fakeRequest,
    }


@router.post("/matches/sql-function", tags=["3. PostgreSQL function"])
def sqlFunctionInsert(
    request: MatchRequest,
    session: Session = Depends(getSession),
):
    """Insert bidirectional bucket matches using the PostgreSQL function."""

    try:
        return insertMatchesWithSqlFunction(session, request)
    except ValueError as error:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(error)) from error
    except SQLAlchemyError as error:
        session.rollback()
        raise databaseFailure(error) from error


@router.post("/matches/python", tags=["4. Python and SQLAlchemy"])
def pythonInsert(
    request: MatchRequest,
    session: Session = Depends(getSession),
):
    """Insert bidirectional bucket matches using Python and bind parameters."""

    try:
        return insertMatchesWithPython(session, request)
    except ValueError as error:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(error)) from error
    except SQLAlchemyError as error:
        session.rollback()
        raise databaseFailure(error) from error
