import os
from dataclasses import dataclass
from typing import Self
from urllib.parse import urlparse

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()


# These values are the same in every environment. Keep them here instead of
# adding environment variables that every deployment would set to the same value.
DATABASE_POOL_SIZE = 30
DATABASE_MAX_OVERFLOW = 10
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
OCCURRENCE_ALIAS = "sentence_occurrences"
ASYLEE_CATALOG_ALIAS = "sentence_semantic_catalog_asylee"
REFUGEE_CATALOG_ALIAS = "sentence_semantic_catalog_refugee"
TRACER_ALIAS = "tracer_form_language_catalog"
SOURCE_SENTENCE_BATCH_SIZE = 30
THREAD_WORKER_COUNT = 30
TITAN_MATCH_WRITE_BATCH_SIZE = 500  # Matches sent to PostgreSQL per function call.
OPENSEARCH_TERMS_BATCH_SIZE = 1_000  # Keys or hits handled per search request.
DATABASE_CLASSIFICATION_BATCH_SIZE = 1_000  # Sentence rows updated per SQL call.
RETRY_SECONDS = 5
VECTOR_DIMENSION = 512
MATCH_THRESHOLD = 76
TRACER_THRESHOLD = 70
IVF_NPROBES = 64
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 7_200


def _aws_hostname(value: str) -> str:
    """Check the OpenSearch address and return its host name."""

    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("OPENSEARCH_HOST must be an HTTPS Amazon OpenSearch hostname")
    return parsed.hostname


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
    aws_local_endpoint_url: str
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
    receive_sqs_endpoint: str
    sent_sqs_endpoint: str
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
            database_pool_size=DATABASE_POOL_SIZE,
            database_max_overflow=DATABASE_MAX_OVERFLOW,
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            # OpenSearch resources
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            opensearch_iam_access_role=os.getenv("OPENSEARCH_IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=_aws_hostname(os.getenv("OPENSEARCH_HOST", "").strip()),
            opensearch_port=OPENSEARCH_PORT,
            opensearch_service=OPENSEARCH_SERVICE,
            occurrence_alias=OCCURRENCE_ALIAS,
            asylee_catalog_alias=ASYLEE_CATALOG_ALIAS,
            refugee_catalog_alias=REFUGEE_CATALOG_ALIAS,
            tracer_alias=TRACER_ALIAS,
            vector_dimension=VECTOR_DIMENSION,
            match_threshold=MATCH_THRESHOLD,
            tracer_threshold=TRACER_THRESHOLD,
            ivf_nprobes=IVF_NPROBES,
            # SQS resources
            receive_sqs_endpoint=os.getenv("AWS_RECEIVE_SQS_ENDPOINT", "").strip(),
            sent_sqs_endpoint=os.getenv("AWS_SQS_SENT_EVENT_QUEUE_ENDPOINT", "").strip(),
            sqs_wait_seconds=SQS_WAIT_SECONDS,
            sqs_visibility_seconds=SQS_VISIBILITY_SECONDS,
            # Processing limits
            source_sentence_batch_size=SOURCE_SENTENCE_BATCH_SIZE,
            thread_worker_count=THREAD_WORKER_COUNT,
            titan_match_write_batch_size=TITAN_MATCH_WRITE_BATCH_SIZE,
            opensearch_terms_batch_size=OPENSEARCH_TERMS_BATCH_SIZE,
            database_classification_batch_size=DATABASE_CLASSIFICATION_BATCH_SIZE,
            retry_seconds=RETRY_SECONDS,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate values loaded from Vault or the environment."""

        if not self.pg_db_user:
            raise ValueError("pgUser is required in Vault")
        if not self.pg_db_password:
            raise ValueError("pgPassword is required in Vault")
        if not self.pg_db_host:
            raise ValueError("pgHost is required in Vault")
        if not self.pg_db_database:
            raise ValueError("pgDatabase is required in Vault")
        if not self.pg_db_port:
            raise ValueError("pgPort is required in Vault")
        if self.opensearch_iam_access_role and not self.opensearch_iam_access_role.startswith("arn:"):
            raise ValueError("OPENSEARCH_IAM_ACCESS_ROLE must be an ARN")
        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.receive_sqs_endpoint:
            raise ValueError("AWS_RECEIVE_SQS_ENDPOINT is required")
        if not self.sent_sqs_endpoint:
            raise ValueError("AWS_SQS_SENT_EVENT_QUEUE_ENDPOINT is required")
