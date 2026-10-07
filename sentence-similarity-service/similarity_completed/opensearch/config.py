import os
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

from similarity_completed.opensearch.VaultSecrets import VaultSecrets

load_dotenv()


# These processing limits are shared by real-time and historical deployments.
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
SOURCE_SENTENCE_BATCH_SIZE = 30
THREAD_WORKER_COUNT = 30
TITAN_MATCH_WRITE_BATCH_SIZE = 500  # Matches sent to PostgreSQL per function call.
OPENSEARCH_TERMS_BATCH_SIZE = 1_000  # Keys or hits handled per search request.
DATABASE_CLASSIFICATION_BATCH_SIZE = 1_000  # Sentence rows updated per SQL call.
RETRY_SECONDS = 5
VECTOR_DIMENSION = 512


def read_integer(name: str, default: int, minimum: int = 1) -> int:
    """Read one whole number and reject a value below the safe minimum."""

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
    database_max_overflow: int
    dynamic_credential_file: str

    # OpenSearch
    aws_region: str
    aws_endpoint_url: str
    opensearch_iam_access_role: str
    opensearch_host: str
    opensearch_port: int
    opensearch_service: str
    occurrence_alias: str
    asylee_catalog_alias: str
    refugee_catalog_alias: str
    tracer_alias: str
    vector_dimension: int
    match_threshold: int
    tracer_threshold: int
    ivf_nprobes: int

    # SQS
    sentence_similarity_queue_url: str
    api_transform_queue_url: str
    sqs_wait_seconds: int
    sqs_visibility_seconds: int

    # Processing
    source_sentence_batch_size: int
    thread_worker_count: int
    titan_match_write_batch_size: int
    opensearch_terms_batch_size: int
    database_classification_batch_size: int
    retry_seconds: int

    @classmethod
    def from_environment(cls) -> Self:
        """Build the worker configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            # Database credentials
            pg_db_user=str(secrets.get("pgUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            database_pool_size=read_integer("DATABASE_POOL_SIZE", 30),
            database_max_overflow=read_integer(
                "DATABASE_MAX_OVERFLOW",
                10,
                minimum=0,
            ),
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            # OpenSearch resources
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL", "").strip(),
            opensearch_iam_access_role=os.getenv(
                "OPENSEARCH_IAM_ACCESS_ROLE",
                "",
            ).strip(),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearch_port=OPENSEARCH_PORT,
            opensearch_service=OPENSEARCH_SERVICE,
            occurrence_alias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS",
                "sentence_occurrences",
            ).strip(),
            asylee_catalog_alias=os.getenv(
                "OPENSEARCH_ASYLEE_CATALOG_ALIAS",
                "sentence_semantic_catalog_asylee",
            ).strip(),
            refugee_catalog_alias=os.getenv(
                "OPENSEARCH_REFUGEE_CATALOG_ALIAS",
                "sentence_semantic_catalog_refugee",
            ).strip(),
            tracer_alias=os.getenv(
                "OPENSEARCH_TRACER_ALIAS",
                "tracer_form_language_catalog",
            ).strip(),
            vector_dimension=VECTOR_DIMENSION,
            match_threshold=read_integer("MATCH_THRESHOLD", 90),
            tracer_threshold=read_integer("TRACER_THRESHOLD", 90),
            ivf_nprobes=read_integer("IVF_NPROBES", 16),
            # SQS resources
            sentence_similarity_queue_url=os.getenv(
                "SENTENCE_SIMILARITY_QUEUE_URL",
                "",
            ).strip(),
            api_transform_queue_url=os.getenv(
                "API_TRANSFORM_QUEUE_URL",
                "",
            ).strip(),
            sqs_wait_seconds=read_integer(
                "AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH",
                10,
            ),
            sqs_visibility_seconds=read_integer(
                "AWS_SQS_VISIBILITY_TIMEOUT",
                7_200,
            ),
            # Processing limits
            source_sentence_batch_size=SOURCE_SENTENCE_BATCH_SIZE,
            thread_worker_count=THREAD_WORKER_COUNT,
            titan_match_write_batch_size=TITAN_MATCH_WRITE_BATCH_SIZE,
            opensearch_terms_batch_size=OPENSEARCH_TERMS_BATCH_SIZE,
            database_classification_batch_size=(
                DATABASE_CLASSIFICATION_BATCH_SIZE
            ),
            retry_seconds=RETRY_SECONDS,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Stop startup when a required resource or safe limit is missing."""

        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.sentence_similarity_queue_url:
            raise ValueError("SENTENCE_SIMILARITY_QUEUE_URL is required")
        if not self.api_transform_queue_url:
            raise ValueError("API_TRANSFORM_QUEUE_URL is required")
        if not self.asylee_catalog_alias or not self.refugee_catalog_alias:
            raise ValueError("both sentence catalog aliases are required")
        if not self.occurrence_alias:
            raise ValueError("OPENSEARCH_OCCURRENCE_ALIAS is required")
        if self.thread_worker_count > 30:
            raise ValueError("THREAD_WORKER_COUNT cannot be greater than 30")
        if self.vector_dimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for Titan embeddings")
        if not 1 <= self.match_threshold <= 100:
            raise ValueError("MATCH_THRESHOLD must be between 1 and 100")
        if not 1 <= self.tracer_threshold <= 100:
            raise ValueError("TRACER_THRESHOLD must be between 1 and 100")
        if not 1 <= self.sqs_wait_seconds <= 20:
            raise ValueError(
                "AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH must be between 1 and 20"
            )
