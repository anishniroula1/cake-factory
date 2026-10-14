import os
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()

# These values are the same in every environment, so they do not need env values.
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
OPENSEARCH_BATCH_SIZE = 1_000
OPENSEARCH_BULK_RETRY_COUNT = 15  # Retries after the original bulk request.
WORKER_BATCH_SIZE = 30
THREAD_WORKER_COUNT = 100
RETRY_SECONDS = 5
TITAN_EMBEDDING_TYPE = 2


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

    # OpenSearch
    aws_region: str
    opensearch_iam_access_role: str
    opensearch_host: str
    opensearch_port: int
    opensearch_service: str
    occurrence_alias: str
    asylee_catalog_alias: str
    refugee_catalog_alias: str
    opensearch_batch_size: int
    opensearch_bulk_retry_count: int

    # SQS
    doc2vec_queue_url: str
    sentence_similarity_backfill_queue_url: str
    state_machine_queue_url: str | None
    sqs_wait_seconds: int
    sqs_visibility_seconds: int
    aws_endpoint_url: str

    # Historical processing
    worker_batch_size: int
    thread_worker_count: int
    retry_seconds: int
    vector_dimension: int
    titan_embedding_type: int

    @classmethod
    def from_environment(cls) -> Self:
        """Build the worker configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            # Read the PostgreSQL login from Vault and pool sizes from env.
            pg_db_user=str(secrets.get("pgUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            database_pool_size=read_integer("DATABASE_POOL_SIZE", 20),
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            database_max_overflow=read_integer(
                "DATABASE_MAX_OVERFLOW",
                10,
                minimum=0,
            ),
            # Read values needed to connect to the correct OpenSearch indexes.
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            opensearch_iam_access_role=os.getenv(
                "OPENSEARCH_IAM_ACCESS_ROLE",
                "",
            ).strip(),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearch_port=OPENSEARCH_PORT,
            opensearch_service=OPENSEARCH_SERVICE,
            occurrence_alias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS", "sentence_occurrences"
            ).strip(),
            asylee_catalog_alias=os.getenv(
                "OPENSEARCH_ASYLEE_CATALOG_ALIAS",
                "sentence_semantic_catalog_asylee",
            ).strip(),
            refugee_catalog_alias=os.getenv(
                "OPENSEARCH_REFUGEE_CATALOG_ALIAS",
                "sentence_semantic_catalog_refugee",
            ).strip(),
            opensearch_batch_size=OPENSEARCH_BATCH_SIZE,
            opensearch_bulk_retry_count=OPENSEARCH_BULK_RETRY_COUNT,
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
                10,
            ),
            sqs_visibility_seconds=read_integer(
                "AWS_SQS_VISIBILITY_TIMEOUT",
                7_200,
            ),
            aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL", "").strip(),
            # Use the fixed batch, worker, retry, and Titan settings above.
            worker_batch_size=WORKER_BATCH_SIZE,
            thread_worker_count=THREAD_WORKER_COUNT,
            retry_seconds=RETRY_SECONDS,
            vector_dimension=read_integer("VECTOR_DIMENSION", 512),
            titan_embedding_type=TITAN_EMBEDDING_TYPE,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate settings required by PostgreSQL, OpenSearch, and SQS."""

        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.doc2vec_queue_url:
            raise ValueError("Doc2Vec SQS queue URL is required")
        if not self.asylee_catalog_alias:
            raise ValueError("OPENSEARCH_ASYLEE_CATALOG_ALIAS is required")
        if not self.refugee_catalog_alias:
            raise ValueError("OPENSEARCH_REFUGEE_CATALOG_ALIAS is required")
        if self.thread_worker_count > 100:
            raise ValueError("THREAD_WORKER_COUNT cannot be greater than 100")
        if self.vector_dimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for Titan embeddings")
        if not 1 <= self.sqs_wait_seconds <= 20:
            raise ValueError(
                "AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH must be between 1 and 20"
            )
