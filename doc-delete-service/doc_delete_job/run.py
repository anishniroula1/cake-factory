import argparse
import os
import signal
import sys
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import newrelic.agent

from doc_delete_job.opensearch.main import run_document_delete_job
from doc_delete_job.tasks import (
    embeddings_table_cleanup,
    index_removal,
    matches_table_cleanup,
    pause_or_unpause_service,
    select_a_numbers_for_tsp_ids,
    select_deletion_batch,
    select_global_ids_for_tsp_ids,
    send_to_state_machine_sqs,
    sentences_vector_table_cleanup,
    similar_cases_cleanup,
)
from doc_delete_job.utils.utils import create_default_sqs_client, get_config_params
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger
from ta_utils.vault.VaultSecrets import VaultSecrets

newrelic.agent.initialize()
newrelic.agent.register_application(timeout=10)
logger = CustomLogger("doc-delete-job")

IN_FLIGHT_STATES = [
    "DOC_DOC2VEC_COMPLETE",
    "DOC_FILTERING_COMPLETE",
    "DOC_FAISS_COMPLETE",
    "DOC_SIMILARITY_COMPLETE",
    "DOC_CASE_COMPLETE",
]


class JobTerminated(BaseException):
    """
    Raised from the SIGTERM handler (K8s activeDeadlineSeconds / pod eviction) .
    Subclasses BaseException so generic `except Exception` blocks don't swallow it
    and the rollback / unpause paths still run.
    """


def _handle_sigterm(signum, frame):
    raise JobTerminated(f"Received signal {signum}")


class TimeBudget:
    """
    Wall-clock budget for one run. Must stay below both the cronjob's
    activeDeadlineSeconds and the Vault dynamic DB credential lease TTL.
    """

    def __init__(self, seconds: int, clock=time.monotonic):
        self._clock = clock
        self._start = clock()
        self.seconds = seconds

    def elapsed(self) -> float:
        return self._clock() - self._start

    def remaining(self) -> float:
        return self.seconds - self.elapsed()

    def can_afford(self, seconds: float) -> bool:
        return self.remaining() >= seconds


@contextmanager
def log_duration(stage: str):
    start = time.monotonic()
    try:
        yield
    finally:
        logger.info(f"[timing] {stage} took {time.monotonic() - start:.1f}s")


def str2bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).strip().lower() in ("true", "1", "yes", "y"):
        return True
    if str(value).strip().lower() in ("false", "0", "no", "n"):
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean, got {value!r}")


def get_args():
    doc_delete_params = get_config_params()
    secrets = VaultSecrets()
    parser = argparse.ArgumentParser(description="manage faiss task args")
    db_group = parser.add_argument_group("DataBase Connection")
    db_group.add_argument(
        "--user", default=secrets.get("pgUser")
    )  # Default was "p2_user"
    db_group.add_argument(
        "--host", default=secrets.get("pgHost")
    )  # Default was "localhost"
    db_group.add_argument(
        "--port", type=int, default=secrets.get("pgPort")
    )  # Default was 5432
    db_group.add_argument(
        "--password", default=secrets.get("pgPassword")
    )  # Default was 1234
    db_group.add_argument(
        "--database", default=secrets.get("pgDatabase")
    )  # Default was "p2_database"
    parser.add_argument(
        "--valid_analysis_groups", default=doc_delete_params["valid_analysis_groups"]
    )
    parser.add_argument(
        "--wait_time_to_check_db_status",
        type=int,
        default=doc_delete_params["wait_time_to_check_db_status"],
    )
    parser.add_argument(
        "--max_retries", type=int, default=doc_delete_params["max_retries"]
    )
    parser.add_argument(
        "--sentence_matches_batch_size",
        type=int,
        default=doc_delete_params["sentence_matches_batch_size"],
    )
    parser.add_argument(
        "--force_start", type=str2bool, default=doc_delete_params["force_start"]
    )
    parser.add_argument(
        "--doc_batch_size", type=int, default=doc_delete_params.get("doc_batch_size", 200)
    )
    parser.add_argument(
        "--time_budget_seconds", type=int, default=doc_delete_params.get("time_budget_seconds", 3000)
    )
    parser.add_argument(
        "--min_seconds_per_batch", type=int, default=doc_delete_params.get("min_seconds_per_batch", 900)
    )
    parser.add_argument(
        "--max_batches_per_run", type=int, default=doc_delete_params.get("max_batches_per_run", 0)
    )
    parser.add_argument(
        "--table_delete_batch_size", type=int, default=doc_delete_params.get("table_delete_batch_size", 10)
    )
    return parser.parse_args()


def get_db(args: Namespace):
    return DatabaseConnection(
        pg_db_user=args.user,
        pg_db_host=args.host,
        pg_db_port=args.port,
        pg_db_password=args.password,
        pg_db_database=args.database,
        dynamic_credentials_file=os.environ.get("VAULT_YAML_PATH")
    )


def wait_until(condition, description: str, args: Namespace, budget: TimeBudget, reserve: float) -> bool:
    """
    Polls condition up to args.max_retries extra times, sleeping
    wait_time_to_check_db_status minutes between checks. Gives up early if
    sleeping would leave less than reserve seconds for the actual deletion.
    """
    retries = args.max_retries
    sleep_seconds = args.wait_time_to_check_db_status * 60
    while True:
        if condition():
            return True
        if retries <= 0:
            logger.info(f"Reached max retries waiting for: {description}")
            return False
        if not budget.can_afford(sleep_seconds + reserve):
            logger.info(
                f"Not enough time left in this run to keep waiting for: {description} "
                f"({budget.remaining():.0f}s remaining)"
            )
            return False
        logger.info(
            f"Waiting {args.wait_time_to_check_db_status} minutes for: {description}"
        )
        retries -= 1
        time.sleep(sleep_seconds)


def delete_batch_in_db(db: DatabaseConnection, global_ids, a_numbers, args: Namespace):
    with db.create_session() as session:
        try:
            if global_ids:
                with log_duration("matches cleanup"):
                    matches_table_cleanup(session, global_ids)
                with log_duration("embeddings cleanup"):
                    embeddings_table_cleanup(
                        session, global_ids, batch_size=args.table_delete_batch_size
                    )
                with log_duration("sentence vector cleanup"):
                    sentences_vector_table_cleanup(
                        session, global_ids, batch_size=args.table_delete_batch_size
                    )
            else:
                logger.info("Didn't find any global_ids to delete")
            if a_numbers:
                with log_duration("Similar cases cleanup"):
                    similar_cases_cleanup(session, a_numbers)
            with log_duration("commit"):
                session.commit()
        except BaseException:
            logger.error("Batch failed, rolling back all DB changes for this batch")
            session.rollback()
            raise


def delete_batch(db: DatabaseConnection, args: Namespace, analysis_group: str, tsp_ids, sqs_client):
    logger.info(f"Starting batch of {len(tsp_ids)} documents for {analysis_group}")
    with db.create_session() as session:
        global_ids = select_global_ids_for_tsp_ids(session, tsp_ids)
        a_numbers = select_a_numbers_for_tsp_ids(session, tsp_ids)
    logger.info(f"Pulled {len(global_ids)} global_ids and {len(a_numbers)} a_numbers")

    if global_ids:
        with log_duration("faiss index removal"):
            index_removal(analysis_group, global_ids)

    delete_batch_in_db(db, global_ids, a_numbers, args)

    with log_duration("send to state machine sqs"):
        send_to_state_machine_sqs(tsp_ids, sqs_client)


def delete_batch_with_enabled_flows(
    db: DatabaseConnection,
    args: Namespace,
    analysis_group: str,
    tsp_ids,
    sqs_client,
):
    """Run the existing and OpenSearch deletion flows for one document batch.

    When the OpenSearch toggle is disabled, the existing deletion runs exactly
    as it did before. When enabled, each flow gets its own worker thread. The
    method waits for both threads before the caller loads the next batch.
    """

    opensearch_flow_enabled = (
        os.getenv("ENABLED_OPENSEARCH_FLOW", "False").lower() == "true"
    )
    if not opensearch_flow_enabled:
        delete_batch(db, args, analysis_group, tsp_ids, sqs_client)
        return

    logger.info(
        "OpenSearch deletion is enabled. Starting the existing deletion flow "
        "and OpenSearch deletion flow in separate threads."
    )
    with ThreadPoolExecutor(
        max_workers=2,
        thread_name_prefix="document-delete-flow",
    ) as executor:
        existing_flow = executor.submit(
            delete_batch,
            db,
            args,
            analysis_group,
            tsp_ids,
            sqs_client,
        )
        opensearch_flow = executor.submit(run_document_delete_job, list(tsp_ids))

        # result() waits for each flow and brings a worker failure back to this
        # batch loop instead of silently losing the error inside the thread.
        existing_flow.result()
        opensearch_flow.result()

    logger.info(
        "The existing deletion flow and OpenSearch deletion flow completed for "
        "the current batch."
    )


def process_analysis_group(db: DatabaseConnection, args: Namespace,
                           analysis_group: str, budget: TimeBudget, sqs_client):
    logger.info(f"Starting Document Delete Job for {analysis_group}")
    with db.create_session() as session:
        batch = select_deletion_batch(session, analysis_group, args.doc_batch_size)
    if not batch:
        logger.info("No documents have been marked for deletion.")
        return
    logger.info(f"First batch has {len(batch)} documents marked for deletion")

    models_idle = wait_until(
        lambda: db.get_tsp_ids_in_states(
            states=["READY_FOR_MODEL_RUN"], analysis_group=analysis_group
        )
        == 0,
        "no documents in READY_FOR_MODEL_RUN",
        args,
        budget,
        reserve=args.min_seconds_per_batch,
    )
    if not models_idle:
        logger.info(
            f"Reached max retries for {analysis_group}. New documents kept coming in processes did not complete."
        )
        return
    logger.info("No documents in READY_FOR_MODEL_RUN, pausing DOC2VEC")
    pause_or_unpause_service(db, service="DOC2VEC", pause=True)
    try:
        if not args.force_start:
            nothing_in_flight = wait_until(
                lambda: db.get_tsp_ids_in_states(
                    IN_FLIGHT_STATES, analysis_group=analysis_group
                )
                == 0,
                "document currently being processed",
                args,
                budget,
                reserve=args.min_seconds_per_batch,
            )
            if not nothing_in_flight:
                logger.info(
                    f"Documents are still being processed for {analysis_group}. Unpausing doc2vec and skipping."
                )
                # TODO: configure alert
                return

        batch_estimate = args.min_seconds_per_batch
        batches_done = 0
        while batch:
            if not budget.can_afford(batch_estimate):
                logger.info(
                    f"Stopping {analysis_group}: {budget.remaining():.0f}s, left, "
                    f"a batch is estimated to take {batch_estimate:.0f}s. "
                    "Remaining documents will be picked up by the next run."
                )
                break
            started = time.monotonic()
            delete_batch_with_enabled_flows(
                db,
                args,
                analysis_group,
                batch,
                sqs_client,
            )
            duration = time.monotonic() - started
            batches_done += 1
            batch_estimate = max(batch_estimate, duration)
            logger.info(
                f"Batch {batches_done} for {analysis_group} done in {duration:.1f}s"
            )
            if args.max_batches_per_run and batches_done >= args.max_batches_per_run:
                break
            with db.create_session() as session:
                batch = select_deletion_batch(
                    session, analysis_group, args.doc_batch_size, after_tsp_id=batch[-1]
                )
    finally:
        try:
            pause_or_unpause_service(db, service="DOC2VEC", pause=False)
        # Unpausing is cleanup and must not hide the original batch failure.
        except Exception as e:  # noqa: BLE001
            logger.error(f"Failed to unpause DOC2VEC for {analysis_group}: {e}")


@newrelic.agent.background_task()
def main(db: DatabaseConnection, args, budget: TimeBudget = None):
    if budget is None:
        budget = TimeBudget(getattr(args, "time_budget_seconds", 3000))
    manage_faiss_params = get_config_params()
    valid_analysis_groups = manage_faiss_params["valid_analysis_groups"]
    sqs_client = create_default_sqs_client(
        os.environ.get("AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT")
    )
    for analysis_group in valid_analysis_groups:
        if not budget.can_afford(args.min_seconds_per_batch):
            logger.info(
                f"Not enough time left to start {analysis_group}, it will run next time."
            )
            continue
        try:
            process_analysis_group(db, args, analysis_group, budget, sqs_client)
        except Exception as e:
            logger.info(f"Error when cleaning up stuff {e}")
            raise


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    args = get_args()
    budget = TimeBudget(args.time_budget_seconds)
    db = get_db(args)
    try:
        main(db, args, budget)
    except JobTerminated as e:
        logger.error(f"Job terminated before finishing: {e}")
        sys.exit(143)
