import os
from dataclasses import dataclass
from typing import Self
from urllib.parse import urlparse

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()


def _aws_hostname(value: str) -> str:
    """Normalize an Amazon OpenSearch endpoint to the host expected by the client.

    Input: value="https://search-sentences.us-east-1.es.amazonaws.com"
    Output: "search-sentences.us-east-1.es.amazonaws.com"

    The OpenSearch client requires a host name without the HTTPS scheme. An
    empty input returns an empty string so normal configuration validation can
    report the missing host.

    Raises:
        ValueError: If the endpoint is not a valid HTTPS host.
    """

    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("OPENSEARCH_HOST must be an HTTPS Amazon OpenSearch hostname")
    return parsed.hostname


@dataclass(frozen=True)
class Config:
    # Vault and database
    pg_db_user: str
    pg_db_password: str
    pg_db_host: str
    pg_db_database: str
    pg_db_port: int
    dynamic_credential_file: str
    environment: str

    # AWS and OpenSearch environment
    aws_region: str
    aws_local_endpoint_url: str
    opensearch_iam_access_role: str
    opensearch_host: str

    # SQS environment
    receive_sqs_endpoint: str
    send_sqs_endpoint: str

    @classmethod
    def from_environment(cls) -> Self:
        """Build the worker configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            # Database credentials
            pg_db_user=str(secrets.get("pgUser") or secrets.get("pgDynamicUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or secrets.get("pgDynamicPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            # OpenSearch resources
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            opensearch_iam_access_role=os.getenv("OPENSEARCH_IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=_aws_hostname(os.getenv("OPENSEARCH_HOST", "").strip()),
            # SQS resources
            receive_sqs_endpoint=os.getenv("AWS_RECEIVE_SQS_ENDPOINT", "").strip(),
            send_sqs_endpoint=os.getenv("AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT", "").strip(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate settings required by PostgreSQL, OpenSearch, and SQS."""

        required_values = {
            "pgUser": self.pg_db_user,
            "pgPassword": self.pg_db_password,
            "pgHost": self.pg_db_host,
            "pgDatabase": self.pg_db_database,
            "pgPort": self.pg_db_port,
            "OPENSEARCH_HOST": self.opensearch_host,
            "AWS_RECEIVE_SQS_ENDPOINT": self.receive_sqs_endpoint,
            "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": self.send_sqs_endpoint,
        }
        missing = [name for name, value in required_values.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if self.environment != "local" and not self.dynamic_credential_file:
            raise ValueError("VAULT_YAML_PATH is required outside the local environment")
        if self.opensearch_iam_access_role and not self.opensearch_iam_access_role.startswith("arn:"):
            raise ValueError("OPENSEARCH_IAM_ACCESS_ROLE must be an ARN")
