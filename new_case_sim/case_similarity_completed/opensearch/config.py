import os
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()


def read_boolean(name: str, default: bool) -> bool:
    """Read an environment toggle written as true or false.

    Input: name="SEND_TAH_KAFKA_EVENTS", default=False
    Output: True when the environment value is "true"; otherwise False.

    Raises:
        ValueError: If the configured value is not true or false.
    """

    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


@dataclass(frozen=True)
class Config:
    # Database
    pg_db_user: str
    pg_db_password: str
    pg_db_host: str
    pg_db_database: str
    pg_db_port: int
    dynamic_credential_file: str
    environment: str

    # SQS
    aws_region: str
    aws_local_endpoint_url: str
    receive_sqs_endpoint: str
    sent_sqs_endpoint: str

    # Kafka
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_environment: str
    kafka_server_ca_cert: str
    ssl_cert_file: str
    ssl_key_file: str
    application_url: str
    send_tah_kafka_events: bool

    @classmethod
    def from_environment(cls) -> Self:
        """Build the worker configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            pg_db_user=str(secrets.get("pgUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            receive_sqs_endpoint=os.getenv("AWS_RECEIVE_SQS_QUEUE_ENDPOINT", "").strip(),
            sent_sqs_endpoint=os.getenv("AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT", "").strip(),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip(),
            kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL").strip(),
            kafka_environment=os.getenv("KAFKA_ENV", "").strip(),
            kafka_server_ca_cert=os.getenv("KAFKA_SERVER_CACERT", "").strip(),
            ssl_cert_file=os.getenv("SSL_CERTFILE", "").strip(),
            ssl_key_file=os.getenv("SSL_KEYFILE", "").strip(),
            application_url=os.getenv("APPLICATION_URL", "").strip(),
            send_tah_kafka_events=read_boolean("SEND_TAH_KAFKA_EVENTS", False),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate settings required by PostgreSQL, SQS, and optional Kafka publishing."""

        required_values = {
            "pgUser": self.pg_db_user,
            "pgPassword": self.pg_db_password,
            "pgHost": self.pg_db_host,
            "pgDatabase": self.pg_db_database,
            "pgPort": self.pg_db_port,
            "AWS_RECEIVE_SQS_QUEUE_ENDPOINT": self.receive_sqs_endpoint,
            "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": self.sent_sqs_endpoint,
        }
        if self.send_tah_kafka_events:
            required_values.update(
                {"KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers, "KAFKA_ENV": self.kafka_environment}
            )
        missing = [name for name, value in required_values.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if self.environment != "local" and not self.dynamic_credential_file:
            raise ValueError("VAULT_YAML_PATH is required outside the local environment")
        if (
            self.send_tah_kafka_events
            and self.kafka_security_protocol == "SSL"
            and not all((self.kafka_server_ca_cert, self.ssl_cert_file, self.ssl_key_file))
        ):
            raise ValueError("Kafka SSL certificate values are required when KAFKA_SECURITY_PROTOCOL is SSL")
