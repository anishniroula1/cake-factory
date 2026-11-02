import os
from dataclasses import dataclass
from typing import Self
from urllib.parse import urlparse

from dotenv import load_dotenv

from ta_utils.vault.VaultSecrets import VaultSecrets

load_dotenv()


def _read_boolean(name: str, default: bool) -> bool:
    """Read a true or false environment toggle.

    Input: name="DATABASE_DELETE_ENABLED", default=False
    Output: True when the environment value is "true".
    """

    value = os.getenv(name)
    if value is None:
        return default
    normalized_value = value.strip().lower()
    if normalized_value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return normalized_value == "true"


def _aws_hostname(value: str) -> str:
    """Return the hostname portion of an HTTPS Amazon OpenSearch endpoint.

    Input: value="https://search-sentences.us-east-1.es.amazonaws.com"
    Output: "search-sentences.us-east-1.es.amazonaws.com"
    """

    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("OPENSEARCH_HOST must be an HTTPS Amazon OpenSearch hostname")
    return parsed.hostname


@dataclass(frozen=True)
class Config:
    """Hold the environment and Vault values needed by document deletion."""

    # Database
    pg_db_user: str
    pg_db_password: str
    pg_db_host: str
    pg_db_database: str
    pg_db_port: int
    dynamic_credential_file: str
    environment: str

    # AWS and OpenSearch
    aws_region: str
    aws_local_endpoint_url: str
    opensearch_iam_access_role: str
    opensearch_host: str

    # Case Similarity Backfill SQS
    case_similarity_backfill_queue_url: str

    # Flow control
    database_delete_enabled: bool

    @classmethod
    def from_environment(cls) -> Self:
        """Build and validate configuration from Vault and environment values."""

        secrets = VaultSecrets()
        config = cls(
            pg_db_user=str(secrets.get("pgUser") or secrets.get("pgDynamicUser") or ""),
            pg_db_password=str(secrets.get("pgPassword") or secrets.get("pgDynamicPassword") or ""),
            pg_db_host=str(secrets.get("pgHost") or ""),
            pg_db_database=str(secrets.get("pgDatabase") or ""),
            pg_db_port=int(secrets.get("pgPort") or 5432),
            dynamic_credential_file=os.getenv("VAULT_YAML_PATH", "").strip(),
            environment=(os.getenv("ENVIRONMENT", "local").strip().lower() or "local"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip(),
            aws_local_endpoint_url=os.getenv("AWS_LOCAL_ENDPOINT_URL", "").strip(),
            opensearch_iam_access_role=os.getenv("OPENSEARCH_IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=_aws_hostname(os.getenv("OPENSEARCH_HOST", "").strip()),
            case_similarity_backfill_queue_url=os.getenv(
                "CASE_SIMILARITY_BACKFILL_QUEUE_URL", ""
            ).strip(),
            database_delete_enabled=_read_boolean("DATABASE_DELETE_ENABLED", False),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Reject configuration that cannot connect to PostgreSQL, OpenSearch, or SQS."""

        required_values = {
            "pgUser": self.pg_db_user,
            "pgPassword": self.pg_db_password,
            "pgHost": self.pg_db_host,
            "pgDatabase": self.pg_db_database,
            "pgPort": self.pg_db_port,
            "OPENSEARCH_HOST": self.opensearch_host,
            "OPENSEARCH_IAM_ACCESS_ROLE": self.opensearch_iam_access_role,
            "CASE_SIMILARITY_BACKFILL_QUEUE_URL": self.case_similarity_backfill_queue_url,
        }
        missing = [name for name, value in required_values.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if self.environment != "local" and not self.dynamic_credential_file:
            raise ValueError("VAULT_YAML_PATH is required outside the local environment")
        if self.opensearch_iam_access_role and not self.opensearch_iam_access_role.startswith("arn:"):
            raise ValueError("OPENSEARCH_IAM_ACCESS_ROLE must be an ARN")

