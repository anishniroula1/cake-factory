import os
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

from doc2vec_completed.opensearch import constants
from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()


def read_integer(name: str, default: int, minimum: int = 1) -> int:
    """Read and validate an integer environment value.

    Input: name="DATABASE_POOL_SIZE", default=20, minimum=1
    Output: the configured integer, or 20 when the value is missing.

    Raises:
        ValueError: If the value is not an integer or is below the minimum.
    """

    try:
        value = int(os.getenv(name, str(default)).strip())
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


@dataclass(frozen=True)
class Config:
    # Database
    pg_db_user: str
    pg_db_password: str
    pg_db_host: str
    pg_db_database: str
    pg_db_port: int
    database_pool_size: int
    dynamic_credential_file: str
    database_max_overflow: int
    environment: str

    # OpenSearch
    aws_region: str
    opensearch_iam_access_role: str
    opensearch_host: str
    opensearch_port: int
    opensearch_service: str
    occurrence_alias: str
    asylee_catalog_alias: str
    refugee_catalog_alias: str
    training_alias: str
    tracer_alias: str
    opensearch_batch_size: int
    opensearch_bulk_retry_count: int

    # SQS
    doc2vec_queue_url: str
    sentence_similarity_backfill_queue_url: str
    state_machine_queue_url: str | None
    sqs_wait_seconds: int
    sqs_visibility_seconds: int
    aws_endpoint_url: str

    # Processing
    live_database_batch_size: int
    worker_batch_size: int
    thread_worker_count: int
    train_model_worker_count: int
    retry_seconds: int
    vector_dimension: int
    titan_embedding_type: int

    @classmethod
    def from_environment(cls) -> Self:
        """Build the worker configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            # Read the PostgreSQL login from Vault and pool sizes from env.
            pg_db_user=str(secrets.get("pgUser") or secrets.get("pgDynamicUser") or ""),
            pg_db_password=str(
                secrets.get("pgPassword") or secrets.get("pgDynamicPassword") or ""
            ),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            database_pool_size=constants.DATABASE_POOL_SIZE,
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            database_max_overflow=constants.DATABASE_MAX_OVERFLOW,
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            # Read values needed to connect to the correct OpenSearch indexes.
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            opensearch_iam_access_role=os.getenv(
                "OPENSEARCH_IAM_ACCESS_ROLE",
                "",
            ).strip(),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearch_port=constants.OPENSEARCH_PORT,
            opensearch_service=constants.OPENSEARCH_SERVICE,
            occurrence_alias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS",
                constants.SENTENCE_OCCURRENCE_ALIAS,
            ).strip(),
            asylee_catalog_alias=os.getenv(
                "OPENSEARCH_ASYLEE_CATALOG_ALIAS",
                constants.ASYLEE_CATALOG_ALIAS,
            ).strip(),
            refugee_catalog_alias=os.getenv(
                "OPENSEARCH_REFUGEE_CATALOG_ALIAS",
                constants.REFUGEE_CATALOG_ALIAS,
            ).strip(),
            training_alias=os.getenv(
                "OPENSEARCH_TRAINING_ALIAS",
                constants.TRAINING_ALIAS,
            ).strip(),
            tracer_alias=os.getenv(
                "OPENSEARCH_TRACER_FORM_LANGUAGE_ALIAS",
                constants.TRACER_ALIAS,
            ).strip(),
            opensearch_batch_size=constants.OPENSEARCH_BATCH_SIZE,
            opensearch_bulk_retry_count=constants.OPENSEARCH_BULK_RETRY_COUNT,
            # Read the source and next-step SQS queue values.
            doc2vec_queue_url=str(secrets.get("awsReceiveSqsQueueEndpoint") or ""),
            sentence_similarity_backfill_queue_url=os.getenv(
                "SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL",
                "",
            ).strip(),
            state_machine_queue_url=(
                str(secrets.get("awsSqsSendEventQueueEndpoint") or "") or None
            ),
            sqs_wait_seconds=read_integer(
                "AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH",
                constants.SQS_WAIT_SECONDS,
            ),
            sqs_visibility_seconds=read_integer(
                "AWS_SQS_VISIBILITY_TIMEOUT",
                constants.SQS_VISIBILITY_SECONDS,
            ),
            aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL", "").strip(),
            # Use fixed batch, worker, retry, and Titan settings.
            live_database_batch_size=constants.LIVE_DATABASE_BATCH_SIZE,
            worker_batch_size=constants.WORKER_BATCH_SIZE,
            thread_worker_count=constants.THREAD_WORKER_COUNT,
            train_model_worker_count=constants.TRAIN_MODEL_WORKER_COUNT,
            retry_seconds=constants.RETRY_SECONDS,
            vector_dimension=constants.VECTOR_DIMENSION,
            titan_embedding_type=constants.TITAN_EMBEDDING_TYPE,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate settings required by PostgreSQL, OpenSearch, and SQS."""

        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.doc2vec_queue_url:
            raise ValueError("Doc2Vec SQS queue URL is required")
        if not self.state_machine_queue_url:
            raise ValueError("State Machine SQS queue URL is required")
        if not self.asylee_catalog_alias:
            raise ValueError("OPENSEARCH_ASYLEE_CATALOG_ALIAS is required")
        if not self.refugee_catalog_alias:
            raise ValueError("OPENSEARCH_REFUGEE_CATALOG_ALIAS is required")
        if not self.training_alias:
            raise ValueError("OPENSEARCH_TRAINING_ALIAS is required")
        if not self.tracer_alias:
            raise ValueError("OPENSEARCH_TRACER_FORM_LANGUAGE_ALIAS is required")
        if self.thread_worker_count > 100:
            raise ValueError("THREAD_WORKER_COUNT cannot be greater than 100")
        if self.train_model_worker_count > 100:
            raise ValueError("TRAIN_MODEL_WORKER_COUNT cannot be greater than 100")
        if self.vector_dimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for Titan embeddings")
        if not 1 <= self.sqs_wait_seconds <= 20:
            raise ValueError(
                "AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH must be between 1 and 20"
            )
