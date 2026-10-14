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
    sent_sqs_endpoint: str

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
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            # OpenSearch resources
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            opensearch_iam_access_role=os.getenv("OPENSEARCH_IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=_aws_hostname(os.getenv("OPENSEARCH_HOST", "").strip()),
            # SQS resources
            receive_sqs_endpoint=os.getenv("AWS_RECEIVE_SQS_ENDPOINT", "").strip(),
            sent_sqs_endpoint=os.getenv("AWS_SQS_SENT_EVENT_QUEUE_ENDPOINT", "").strip(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate settings required by PostgreSQL, OpenSearch, and SQS.

        Input: the current Config instance, such as pg_db_port=5432 and
        opensearch_host="search-sentences.us-east-1.es.amazonaws.com".

        Validation runs during configuration creation so invalid deployment
        values fail before the SQS processing loop starts.

        Raises:
            ValueError: If a required setting is missing or invalid.
        """

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
        if self.environment != "local" and not self.dynamic_credential_file:
            raise ValueError("VAULT_YAML_PATH is required outside the local environment")
        if self.opensearch_iam_access_role and not self.opensearch_iam_access_role.startswith("arn:"):
            raise ValueError("OPENSEARCH_IAM_ACCESS_ROLE must be an ARN")
        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.receive_sqs_endpoint:
            raise ValueError("AWS_RECEIVE_SQS_ENDPOINT is required")
        if not self.sent_sqs_endpoint:
            raise ValueError("AWS_SQS_SENT_EVENT_QUEUE_ENDPOINT is required")
