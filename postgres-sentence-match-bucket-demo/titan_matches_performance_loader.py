"""Create a fully connected group through the bidirectional SQL function."""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy import ARRAY, URL, Integer, bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

load_dotenv()


# Every generated global ID finishes with exactly this many matches.
# A value of 3,000 creates 3,001 global IDs because an ID cannot match itself.
MATCHES_PER_GLOBAL_ID = 1_000
MATCH_BATCH_SIZE = 100
WORKER_COUNT = 1
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 2
EXACT_MATCH_EVERY = 20
PROGRESS_LOG_EVERY_SOURCES = 100
LOG_EACH_BATCH = False

# The script owns this continuous ID range for repeatable performance tests.
GLOBAL_ID_START = 90_000_002
TSP_ID_START = 100_001
A_NUMBER_START = 700_000_001
SENTENCES_PER_TSP = 20

DELETE_EXISTING_TEST_DATA = True
DELETE_TEST_DATA_AFTER_RUN = False


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
logger = logging.getLogger("titan-matches-performance")


class DatabaseConnection:
    """Create the shared SQLAlchemy connection pool used by all workers."""

    def __init__(
        self,
        pg_db_user: str,
        pg_db_password: str,
        pg_db_host: str,
        pg_db_database: str,
        pg_db_port: int,
        pool_size: int,
    ) -> None:
        connection_string = URL.create(
            "postgresql+psycopg",
            username=pg_db_user,
            password=pg_db_password,
            host=pg_db_host,
            port=pg_db_port,
            database=pg_db_database,
        )
        self.engine = create_engine(
            connection_string,
            pool_size=pool_size,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=1_800,
            connect_args={"application_name": "titan_matches_performance"},
        )

    def close(self) -> None:
        """Close every pooled database connection."""

        self.engine.dispose()


@dataclass(frozen=True)
class SentenceIdentity:
    """Hold the database identity values stored in one match object."""

    global_id: int
    tsp_id: int
    a_number: int


@dataclass(frozen=True)
class SourceJob:
    """Describe one source and the higher IDs it still needs to match."""

    source_number: int
    identity_count: int


@dataclass(frozen=True)
class SourceResult:
    """Report unique relationships saved for one source."""

    source_number: int
    relationship_count: int
    batch_count: int
    elapsed_seconds: float


FUNCTION_NAME = "test_matches.upsert_bidirectional_titan_matches"
TABLE_NAME = "test_matches.titan_matches"

UPSERT_MATCHES = text(
    f"""
    SELECT {FUNCTION_NAME}(
        :source_global_id,
        :source_tsp_id,
        :source_a_number,
        :matches
    )
    """
).bindparams(bindparam("matches", type_=JSONB))

DELETE_TEST_ROWS = text(
    f"""
    DELETE FROM {TABLE_NAME}
    WHERE global_id = ANY(:global_ids)
    """
).bindparams(bindparam("global_ids", type_=ARRAY(Integer)))

def create_database_connection() -> DatabaseConnection:
    """Read local database values and create the shared connection pool."""

    effective_workers = min(WORKER_COUNT, MATCHES_PER_GLOBAL_ID)
    return DatabaseConnection(
        pg_db_user=os.getenv("POSTGRES_USER", "sentence_user"),
        pg_db_password=os.getenv("POSTGRES_PASSWORD", "sentence_password"),
        pg_db_host=os.getenv("POSTGRES_HOST", "localhost"),
        pg_db_database=os.getenv("POSTGRES_DB", "sentence_matches"),
        pg_db_port=int(os.getenv("POSTGRES_PORT", "5434")),
        pool_size=max(1, effective_workers),
    )


def sentence_identity(sentence_number: int) -> SentenceIdentity:
    """Return stable identity values for one generated global ID."""

    document_number = sentence_number // SENTENCES_PER_TSP
    return SentenceIdentity(
        global_id=GLOBAL_ID_START + sentence_number,
        tsp_id=TSP_ID_START + document_number,
        a_number=A_NUMBER_START + document_number,
    )


def create_source_jobs(identity_count: int) -> list[SourceJob]:
    """Create one job per source except the last reverse-only row."""

    return [
        SourceJob(
            source_number=source_number,
            identity_count=identity_count,
        )
        for source_number in range(identity_count - 1)
    ]


def spread_source_jobs(
    jobs: list[SourceJob],
    worker_count: int,
) -> list[SourceJob]:
    """Start workers in different ID areas to reduce lock waiting."""

    partition_size = (len(jobs) + worker_count - 1) // worker_count
    spread_jobs = []
    for offset in range(partition_size):
        for partition_number in range(worker_count):
            job_number = partition_number * partition_size + offset
            if job_number < len(jobs):
                spread_jobs.append(jobs[job_number])
    return spread_jobs


def pair_distance(source_number: int, target_number: int) -> float:
    """Return one stable distance that is reused in both directions."""

    pair_number = source_number * target_number + source_number + target_number
    if pair_number % EXACT_MATCH_EVERY == 0:
        return 0.0
    return ((pair_number % 30) + 1) / 100


def create_match_batch(
    source_number: int,
    target_start: int,
    target_end: int,
) -> list:
    """Create one JSONB batch of higher target IDs for a source."""

    matches = []
    for target_number in range(target_start, target_end):
        target = sentence_identity(target_number)
        matches.append(
            {
                "global_id": target.global_id,
                "tsp_id": target.tsp_id,
                "a_number": target.a_number,
                "distance": pair_distance(source_number, target_number),
            }
        )
    return matches


def call_match_function(
    database: DatabaseConnection,
    source: SentenceIdentity,
    matches: list,
) -> int:
    """Call the SQL function once and commit one relationship batch."""

    parameters = {
        "source_global_id": source.global_id,
        "source_tsp_id": source.tsp_id,
        "source_a_number": source.a_number,
        "matches": matches,
    }
    with database.engine.begin() as connection:
        return int(connection.execute(UPSERT_MATCHES, parameters).scalar_one())


def save_batch_with_retry(
    database: DatabaseConnection,
    source: SentenceIdentity,
    matches: list,
    batch_number: int,
    total_batches: int,
) -> int:
    """Retry one failed SQL function call before stopping its worker."""

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        started_at = time.perf_counter()
        try:
            saved_count = call_match_function(database, source, matches)
            if LOG_EACH_BATCH:
                logger.info(
                    "Source %s batch %s/%s saved %s relationships in %.3f "
                    "seconds.",
                    source.global_id,
                    batch_number,
                    total_batches,
                    saved_count,
                    time.perf_counter() - started_at,
                )
            return saved_count
        except Exception as error:
            logger.error(
                "Source %s batch %s/%s attempt %s/%s failed: %s",
                source.global_id,
                batch_number,
                total_batches,
                attempt,
                RETRY_ATTEMPTS,
                str(error)[:500],
            )
            if attempt == RETRY_ATTEMPTS:
                raise
            time.sleep(RETRY_WAIT_SECONDS)

    raise RuntimeError("The retry loop ended without returning or raising")


def process_source(
    database: DatabaseConnection,
    job: SourceJob,
) -> SourceResult:
    """Save every higher-ID pair for one source in ordered batches."""

    started_at = time.perf_counter()
    source = sentence_identity(job.source_number)
    first_target = job.source_number + 1
    relationship_count = job.identity_count - first_target
    total_batches = (
        relationship_count + MATCH_BATCH_SIZE - 1
    ) // MATCH_BATCH_SIZE
    saved_count = 0

    for batch_number, target_start in enumerate(
        range(first_target, job.identity_count, MATCH_BATCH_SIZE),
        start=1,
    ):
        target_end = min(target_start + MATCH_BATCH_SIZE, job.identity_count)
        matches = create_match_batch(
            job.source_number,
            target_start,
            target_end,
        )
        saved_count += save_batch_with_retry(
            database,
            source,
            matches,
            batch_number,
            total_batches,
        )

    return SourceResult(
        source_number=job.source_number,
        relationship_count=saved_count,
        batch_count=total_batches,
        elapsed_seconds=time.perf_counter() - started_at,
    )


def test_global_ids(identity_count: int) -> list[int]:
    """Return the exact continuous global-ID range owned by this test."""

    return list(range(GLOBAL_ID_START, GLOBAL_ID_START + identity_count))


def delete_test_data(
    database: DatabaseConnection,
    identity_count: int,
) -> None:
    """Delete only the generated global IDs owned by this script."""

    global_ids = test_global_ids(identity_count)
    started_at = time.perf_counter()
    with database.engine.begin() as connection:
        result = connection.execute(
            DELETE_TEST_ROWS,
            {"global_ids": global_ids},
        )
    logger.info(
        "Deleted %s old test rows in %.3f seconds.",
        result.rowcount,
        time.perf_counter() - started_at,
    )


def main() -> None:
    """Build the complete bidirectional group and report its performance."""

    identity_count = MATCHES_PER_GLOBAL_ID + 1
    unique_relationship_count = identity_count * MATCHES_PER_GLOBAL_ID // 2
    stored_match_count = identity_count * MATCHES_PER_GLOBAL_ID
    worker_count = min(WORKER_COUNT, MATCHES_PER_GLOBAL_ID)
    jobs = spread_source_jobs(
        create_source_jobs(identity_count),
        worker_count,
    )
    database = create_database_connection()
    started_at = time.perf_counter()

    try:
        if DELETE_EXISTING_TEST_DATA:
            delete_test_data(database, identity_count)

        logger.info(
            "Starting a complete group of %s global IDs. Every row will have "
            "%s matches.",
            identity_count,
            MATCHES_PER_GLOBAL_ID,
        )
        logger.info(
            "The SQL function will create %s unique relationships and %s "
            "stored bidirectional match objects using %s workers.",
            unique_relationship_count,
            stored_match_count,
            worker_count,
        )

        completed_sources = 0
        completed_relationships = 0
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="titan-match-worker",
        ) as executor:
            futures = {
                executor.submit(process_source, database, job): job
                for job in jobs
            }
            for future in as_completed(futures):
                result = future.result()
                completed_sources += 1
                completed_relationships += result.relationship_count
                if (
                    completed_sources % PROGRESS_LOG_EVERY_SOURCES == 0
                    or completed_sources == len(jobs)
                ):
                    elapsed = time.perf_counter() - started_at
                    logger.info(
                        "Completed %s/%s source rows and %s/%s unique "
                        "relationships in %.3f seconds.",
                        completed_sources,
                        len(jobs),
                        completed_relationships,
                        unique_relationship_count,
                        elapsed,
                    )

        elapsed = time.perf_counter() - started_at
        logger.info(
            "Performance test completed in %.3f seconds at %.1f unique "
            "relationships/second.",
            elapsed,
            unique_relationship_count / elapsed,
        )

        if DELETE_TEST_DATA_AFTER_RUN:
            delete_test_data(database, identity_count)
    finally:
        database.close()


if __name__ == "__main__":
    main()
