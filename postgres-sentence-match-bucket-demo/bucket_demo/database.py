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

SCHEMA_FILE = (
    Path(__file__).resolve().parent.parent
    / "db"
    / "init"
    / "001_sentence_match_buckets.sql"
)


def initializeDatabase():
    """Create the demo tables and PostgreSQL functions when missing."""

    schemaSql = SCHEMA_FILE.read_text(encoding="utf-8")
    rawConnection = engine.raw_connection()

    try:
        with rawConnection.cursor() as cursor:
            # prepare=False lets psycopg execute the complete SQL script.
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
