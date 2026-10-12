import os
import re
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()

# Database
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 10

# SQS
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 7200

# Payload and Kafka
SENTENCE_MATCH_LIMIT = 3000  # Maximum matching sentences returned for one source sentence.
UNCOMPRESSED_MAX_EVENT_BYTES = 4718592
COMPRESSED_MAX_EVENT_BYTES = 18155776
KAFKA_BUFFER_LIMIT_BYTES = 450000000
KAFKA_TOPIC = "rfs.ata.data_migration.api.events"
KAFKA_MESSAGE_MAX_BYTES = 100 * 1024 * 1024
KAFKA_AUDIT_RETENTION_DAYS = 60

# Retry
RETRY_SECONDS = 5
MAX_ATTEMPTS = 3


def read_boolean(name: str, default: bool) -> bool:
    """Read an environment toggle written as true or false."""

    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


def dlq_url(queue_url: str) -> str:
    """Build the existing DLQ URL from the normal API Transform queue URL."""

    return re.sub(r"(asylee|refugee)-([^/]+\.fifo)", r"\1-dlq-\2", queue_url)


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
    environment: str

    # SQS
    aws_region: str
    aws_local_endpoint_url: str
    receive_sqs_endpoint: str
    dlq_sqs_endpoint: str
    sent_sqs_endpoint: str
    sqs_wait_seconds: int
    sqs_visibility_seconds: int

    # Kafka
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_environment: str
    kafka_server_ca_cert: str
    ssl_cert_file: str
    ssl_key_file: str
    kafka_compression: str
    kafka_topic: str
    verbose_kafka_logging: bool

    # Transformation
    analysis_group: str
    reduce_kafka_size: bool
    sentence_match_limit: int
    uncompressed_max_event_bytes: int
    compressed_max_event_bytes: int
    kafka_buffer_limit_bytes: int
    kafka_audit_retention_days: int
    retry_seconds: int
    max_attempts: int

    @classmethod
    def from_environment(cls) -> Self:
        """Build configuration from Vault secrets and deployment values."""

        secrets = VaultSecrets()
        source_queue = os.getenv("AWS_RECEIVE_SQS_QUEUE_ENDPOINT", "").strip()
        config = cls(
            pg_db_user=str(secrets.get("pgUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            database_pool_size=DATABASE_POOL_SIZE,
            database_max_overflow=DATABASE_MAX_OVERFLOW,
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            receive_sqs_endpoint=source_queue,
            dlq_sqs_endpoint=os.getenv("AWS_DLQ_SQS_QUEUE_ENDPOINT", dlq_url(source_queue)).strip(),
            sent_sqs_endpoint=os.getenv("AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT", "").strip(),
            sqs_wait_seconds=SQS_WAIT_SECONDS,
            sqs_visibility_seconds=SQS_VISIBILITY_SECONDS,
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip(),
            kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL").strip(),
            kafka_environment=os.getenv("KAFKA_ENV", "").strip(),
            kafka_server_ca_cert=os.getenv("KAFKA_SERVER_CACERT", "").strip(),
            ssl_cert_file=os.getenv("SSL_CERTFILE", "").strip(),
            ssl_key_file=os.getenv("SSL_KEYFILE", "").strip(),
            kafka_compression=os.getenv("KAFKA_COMPRESSION", "lz4").strip(),
            kafka_topic=KAFKA_TOPIC,
            verbose_kafka_logging=read_boolean("VERBOSE_KAFKA_LOGGING", False),
            analysis_group=os.getenv("ANALYSIS_GROUP", "").strip().capitalize(),
            reduce_kafka_size=read_boolean("REDUCE_KAFKA_SIZE", False),
            sentence_match_limit=SENTENCE_MATCH_LIMIT,
            uncompressed_max_event_bytes=UNCOMPRESSED_MAX_EVENT_BYTES,
            compressed_max_event_bytes=COMPRESSED_MAX_EVENT_BYTES,
            kafka_buffer_limit_bytes=KAFKA_BUFFER_LIMIT_BYTES,
            kafka_audit_retention_days=KAFKA_AUDIT_RETENTION_DAYS,
            retry_seconds=RETRY_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Stop startup when a required deployment value is missing."""

        required = {
            "pgUser": self.pg_db_user,
            "pgPassword": self.pg_db_password,
            "pgHost": self.pg_db_host,
            "pgDatabase": self.pg_db_database,
            "AWS_RECEIVE_SQS_QUEUE_ENDPOINT": self.receive_sqs_endpoint,
            "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": self.sent_sqs_endpoint,
            "KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
            "KAFKA_ENV": self.kafka_environment,
            "ANALYSIS_GROUP": self.analysis_group,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if self.environment != "local" and not self.dynamic_credential_file:
            raise ValueError("VAULT_YAML_PATH is required outside the local environment")
        if self.kafka_security_protocol == "SSL" and not all(
            (self.kafka_server_ca_cert, self.ssl_cert_file, self.ssl_key_file)
        ):
            raise ValueError("Kafka SSL certificate values are required when KAFKA_SECURITY_PROTOCOL is SSL")
