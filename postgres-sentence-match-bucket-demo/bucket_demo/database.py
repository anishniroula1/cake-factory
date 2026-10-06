from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bucket_demo.config import getSettings

settings = getSettings()

engine = create_engine(
    settings.databaseUrl,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
)

SCHEMA_DIRECTORY = (
    Path(__file__).resolve().parent.parent
    / "db"
    / "init"
)


def initializeDatabase():
    """Create the demo tables and PostgreSQL functions when missing."""

    rawConnection = engine.raw_connection()

    try:
        with rawConnection.cursor() as cursor:
            # Run every numbered SQL file in order so each POC owns its schema.
            for schemaFile in sorted(SCHEMA_DIRECTORY.glob("*.sql")):
                schemaSql = schemaFile.read_text(encoding="utf-8")
                cursor.execute(schemaSql, prepare=False)

        rawConnection.commit()
    except Exception:
        rawConnection.rollback()
        raise
    finally:
        rawConnection.close()


def getSession():
    """Provide one SQLAlchemy session to a FastAPI request."""

    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()
