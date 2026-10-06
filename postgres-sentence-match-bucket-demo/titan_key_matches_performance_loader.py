"""Load cross-application matches through the titan_key_matches SQL function."""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy import ARRAY, URL, Integer, bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

load_dotenv()


# With 100 IDs per application, 3,000 matches creates 31 applications and
# 3,100 IDs. Every ID matches all 3,000 IDs from the other 30 applications.
MATCHES_PER_GLOBAL_ID = 3000
GLOBAL_IDS_PER_APPLICATION = 100
MATCH_BATCH_SIZE = 500
WORKER_COUNT = 2
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 2
EXACT_MATCH_EVERY = 20
PROGRESS_LOG_EVERY_SOURCES = 100
LOG_EACH_BATCH = False

# This range is separate from the other performance loaders.
GLOBAL_ID_START = 97_000_001
TSP_ID_START = 700_001
APPLICATION_ID_START = 800_001
GLOBAL_IDS_PER_TSP = 20

DELETE_EXISTING_TEST_DATA = True
DELETE_TEST_DATA_AFTER_RUN = False


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
logger = logging.getLogger("titan-key-matches-performance")


class DatabaseConnection:
    """Create the SQLAlchemy connection pool shared by worker threads."""

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
            connect_args={"application_name": "titan_key_matches_test"},
        )

    def close(self) -> None:
        """Close every pooled database connection."""

        self.engine.dispose()


@dataclass(frozen=True)
class SentenceIdentity:
    """Hold the database identity values for one generated sentence."""

    global_id: int
    tsp_id: int
    application_id: int


@dataclass(frozen=True)
class SourceJob:
    """Describe one source and the higher IDs it still needs to match."""

    source_number: int
    identity_count: int


FUNCTION_NAME = "test_matches.upsert_bidirectional_titan_key_matches"
TABLE_NAME = "test_matches.titan_key_matches"

UPSERT_MATCHES = text(
    f"""
    SELECT {FUNCTION_NAME}(
        :source_global_id,
        :source_tsp_id,
        :source_application_id,
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


def application_count() -> int:
    """Return enough applications to give every ID the requested matches."""

    return MATCHES_PER_GLOBAL_ID // GLOBAL_IDS_PER_APPLICATION + 1


def sentence_identity(sentence_number: int) -> SentenceIdentity:
    """Return stable identity values for one generated sentence."""

    application_number = sentence_number // GLOBAL_IDS_PER_APPLICATION
    document_within_application = (
        sentence_number % GLOBAL_IDS_PER_APPLICATION
    ) // GLOBAL_IDS_PER_TSP
    documents_per_application = (
        GLOBAL_IDS_PER_APPLICATION // GLOBAL_IDS_PER_TSP
    )
    document_number = (
        application_number * documents_per_application
        + document_within_application
    )
    return SentenceIdentity(
        global_id=GLOBAL_ID_START + sentence_number,
        tsp_id=TSP_ID_START + document_number,
        application_id=APPLICATION_ID_START + application_number,
    )


def create_source_jobs(identity_count: int) -> list[SourceJob]:
    """Create one job per source except the final reverse-only row."""

    return [
        SourceJob(source_number=source_number, identity_count=identity_count)
        for source_number in range(identity_count - 1)
    ]


def spread_source_jobs(
    jobs: list[SourceJob],
    worker_count: int,
) -> list[SourceJob]:
    """Start workers in separate ID areas to reduce lock contention."""

    partition_size = (len(jobs) + worker_count - 1) // worker_count
    spread_jobs = []
    for offset in range(partition_size):
        for partition_number in range(worker_count):
            job_number = partition_number * partition_size + offset
            if job_number < len(jobs):
                spread_jobs.append(jobs[job_number])
    return spread_jobs


def pair_distance(source_number: int, target_number: int) -> float:
    """Return one stable distance that is saved in both directions."""

    pair_number = source_number * target_number + source_number + target_number
    if pair_number % EXACT_MATCH_EVERY == 0:
        return 0.0
    return ((pair_number % 30) + 1) / 100


def eligible_target_numbers(job: SourceJob) -> list[int]:
    """Return higher target IDs that belong to another application."""

    source_application_id = sentence_identity(job.source_number).application_id
    return [
        target_number
        for target_number in range(job.source_number + 1, job.identity_count)
        if sentence_identity(target_number).application_id
        != source_application_id
    ]


def create_match_batch(
    source_number: int,
    target_numbers: list[int],
) -> dict:
    """Create rich function input while keeping stored values small."""

    matches = {}
    for target_number in target_numbers:
        target = sentence_identity(target_number)
        matches[str(target.global_id)] = {
            "tsp_id": target.tsp_id,
            "application_id": target.application_id,
            "distance": pair_distance(source_number, target_number),
        }
    return matches


def call_match_function(
    database: DatabaseConnection,
    source: SentenceIdentity,
    matches: dict,
) -> int:
    """Call the SQL function and commit one cross-application batch."""

    parameters = {
        "source_global_id": source.global_id,
        "source_tsp_id": source.tsp_id,
        "source_application_id": source.application_id,
        "matches": matches,
    }
    with database.engine.begin() as connection:
        return int(connection.execute(UPSERT_MATCHES, parameters).scalar_one())


def save_batch_with_retry(
    database: DatabaseConnection,
    source: SentenceIdentity,
    matches: dict,
    batch_number: int,
    total_batches: int,
) -> int:
    """Retry one failed SQL-function call before stopping the worker."""

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
) -> int:
    """Save every higher-ID cross-application pair for one source."""

    source = sentence_identity(job.source_number)
    target_numbers = eligible_target_numbers(job)
    total_batches = (
        len(target_numbers) + MATCH_BATCH_SIZE - 1
    ) // MATCH_BATCH_SIZE
    saved_count = 0

    for batch_number, batch_start in enumerate(
        range(0, len(target_numbers), MATCH_BATCH_SIZE),
        start=1,
    ):
        target_batch = target_numbers[
            batch_start : batch_start + MATCH_BATCH_SIZE
        ]
        matches = create_match_batch(job.source_number, target_batch)
        saved_count += save_batch_with_retry(
            database,
            source,
            matches,
            batch_number,
            total_batches,
        )

    # The final IDs can receive every relationship in reverse. Make sure an
    # owner with no higher eligible target is still created when necessary.
    if not target_numbers:
        saved_count += save_batch_with_retry(
            database,
            source,
            {},
            1,
            1,
        )
    return saved_count


def test_global_ids(identity_count: int) -> list[int]:
    """Return the continuous global-ID range owned by this loader."""

    return list(range(GLOBAL_ID_START, GLOBAL_ID_START + identity_count))


def delete_test_data(
    database: DatabaseConnection,
    identity_count: int,
) -> None:
    """Delete only the generated IDs owned by this loader."""

    started_at = time.perf_counter()
    with database.engine.begin() as connection:
        result = connection.execute(
            DELETE_TEST_ROWS,
            {"global_ids": test_global_ids(identity_count)},
        )
    logger.info(
        "Deleted %s old titan_key_matches rows in %.3f seconds.",
        result.rowcount,
        time.perf_counter() - started_at,
    )


def main() -> None:
    """Insert a complete cross-application match group."""
    total_applications = application_count()
    identity_count = total_applications * GLOBAL_IDS_PER_APPLICATION
    total_pair_count = identity_count * (identity_count - 1) // 2
    same_application_pair_count = total_applications * (
        GLOBAL_IDS_PER_APPLICATION * (GLOBAL_IDS_PER_APPLICATION - 1) // 2
    )
    unique_relationship_count = total_pair_count - same_application_pair_count
    stored_match_count = unique_relationship_count * 2
    worker_count = min(WORKER_COUNT, identity_count - 1)
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
            "Starting %s IDs across %s applications with %s IDs per "
            "application.",
            identity_count,
            total_applications,
            GLOBAL_IDS_PER_APPLICATION,
        )
        logger.info(
            "Every row will contain %s matches from other applications and "
            "zero matches from its own application.",
            MATCHES_PER_GLOBAL_ID,
        )
        logger.info(
            "The SQL function will create %s unique relationships and %s "
            "stored bidirectional values using %s workers.",
            unique_relationship_count,
            stored_match_count,
            worker_count,
        )

        completed_sources = 0
        completed_relationships = 0
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="titan-key-match-worker",
        ) as executor:
            futures = {
                executor.submit(process_source, database, job): job
                for job in jobs
            }
            for future in as_completed(futures):
                completed_relationships += future.result()
                completed_sources += 1
                if (
                    completed_sources % PROGRESS_LOG_EVERY_SOURCES == 0
                    or completed_sources == len(jobs)
                ):
                    logger.info(
                        "Completed %s/%s sources and %s/%s unique "
                        "relationships in %.3f seconds.",
                        completed_sources,
                        len(jobs),
                        completed_relationships,
                        unique_relationship_count,
                        time.perf_counter() - started_at,
                    )

        elapsed = time.perf_counter() - started_at
        logger.info(
            "titan_key_matches test completed in %.3f seconds at %.1f "
            "unique relationships/second.",
            elapsed,
            unique_relationship_count / elapsed,
        )

        if DELETE_TEST_DATA_AFTER_RUN:
            delete_test_data(database, identity_count)
    finally:
        database.close()


if __name__ == "__main__":
    main()
