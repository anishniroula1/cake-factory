import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from dotenv import load_dotenv

load_dotenv()

ROLE_ARN_PATTERN = re.compile(r"^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:role/.+$")
ALIAS_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")

# Fixed application behavior. Change these values in code, not in .env.
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
MIGRATION_WORKERS = 100
BULK_RETRY_ATTEMPTS = 15
BULK_RETRY_SECONDS = 1
BULK_RETRY_STATUSES = (408, 424, 429, 500, 502, 503, 504)
EXTERNAL_API_RETRY_ATTEMPTS = 10
EXTERNAL_API_RETRY_SECONDS = 2
EXTERNAL_API_TIMEOUT_SECONDS = 60
EXTERNAL_API_KEY_HEADER = "x-api-key"
SOURCE_API_FIRST_PAGE = 1
SENTENCE_SOURCE_ENDPOINT = "/sentences"
SENTENCE_KEY_UPDATE_ENDPOINT = "/sentences/sentence-keys"
DOCUMENT_STATE_ENDPOINT = "/documents/state"
DOCUMENT_COMPLETED_STATE = "OPENSEARCH_MIGRATED"


def _list(name: str) -> tuple[str, ...]:
    raw_value = os.getenv(name, "").strip()
    return tuple(
        dict.fromkeys(item.strip() for item in raw_value.split(",") if item.strip())
    )


@dataclass(frozen=True)
class Config:
    aws_region: str
    aws_role_external_id: str
    s3_bucket: str
    opensearch_iam_roles: tuple[str, ...]
    occurrence_aliases: tuple[str, ...]
    catalog_aliases: tuple[str, ...]
    opensearch_host: str
    external_api_base_url: str
    external_api_key: str

    @classmethod
    def from_environment(cls):
        return cls(
            aws_region=os.getenv("AWS_REGION", "us-east-1").strip(),
            aws_role_external_id=os.getenv("AWS_ROLE_EXTERNAL_ID", "").strip(),
            s3_bucket=os.getenv("S3_BUCKET", "").strip(),
            opensearch_iam_roles=_list("OPENSEARCH_IAM_ROLES"),
            occurrence_aliases=_list("OPENSEARCH_OCCURRENCE_ALIASES"),
            catalog_aliases=_list("OPENSEARCH_CATALOG_ALIASES"),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            external_api_base_url=os.getenv("EXTERNAL_API_BASE_URL", "").strip(),
            external_api_key=os.getenv("EXTERNAL_API_KEY", "").strip(),
        )

    @property
    def normalized_opensearch_host(self) -> str:
        value = self.opensearch_host.strip()
        parsed = urlsplit(value if "://" in value else f"https://{value}")
        if not parsed.hostname or parsed.path not in {"", "/"}:
            raise ValueError("OPENSEARCH_HOST must be a hostname without a path")
        return parsed.hostname

    @property
    def normalized_external_api_base_url(self) -> str:
        parsed = urlsplit(self.external_api_base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "EXTERNAL_API_BASE_URL must be a complete HTTP(S) URL without "
                "a query string or fragment"
            )
        return self.external_api_base_url.rstrip("/")

    @property
    def all_aliases(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.occurrence_aliases, *self.catalog_aliases)))

    def validate_s3(self):
        if not self.aws_region:
            raise ValueError("AWS_REGION is required")
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET is required")

    def validate_opensearch(self):
        if not self.aws_region:
            raise ValueError("AWS_REGION is required")
        _ = self.normalized_opensearch_host
        if not self.opensearch_iam_roles:
            raise ValueError("OPENSEARCH_IAM_ROLES must contain at least one role ARN")
        invalid_roles = [
            role
            for role in self.opensearch_iam_roles
            if not ROLE_ARN_PATTERN.fullmatch(role)
        ]
        if invalid_roles:
            raise ValueError(
                f"Invalid IAM role in OPENSEARCH_IAM_ROLES: {invalid_roles[0]}"
            )

    def validate_migration(self):
        self.validate_s3()
        self.validate_opensearch()
        if not self.occurrence_aliases:
            raise ValueError("OPENSEARCH_OCCURRENCE_ALIASES cannot be empty")
        if not self.catalog_aliases:
            raise ValueError("OPENSEARCH_CATALOG_ALIASES cannot be empty")
        for alias in self.all_aliases:
            if alias in {".", ".."} or not ALIAS_PATTERN.fullmatch(alias):
                raise ValueError(f"Invalid OpenSearch alias in environment: {alias}")
        _ = self.normalized_external_api_base_url
        if not self.external_api_key:
            raise ValueError("EXTERNAL_API_KEY is required")

    def resolve_iam_role(self, selection: str | None) -> str:
        return self._resolve_choice(
            selection,
            self.opensearch_iam_roles,
            "iamRole",
            "OPENSEARCH_IAM_ROLES",
        )

    def resolve_occurrence_alias(self, selection: str | None) -> str:
        return self._resolve_choice(
            selection,
            self.occurrence_aliases,
            "occurrenceAlias",
            "OPENSEARCH_OCCURRENCE_ALIASES",
        )

    def resolve_catalog_alias(self, selection: str | None) -> str:
        return self._resolve_choice(
            selection,
            self.catalog_aliases,
            "catalogAlias",
            "OPENSEARCH_CATALOG_ALIASES",
        )

    def resolve_alias(self, selection: str | None) -> str:
        return self._resolve_choice(
            selection,
            self.all_aliases,
            "alias",
            "OpenSearch alias environment lists",
        )

    @staticmethod
    def _resolve_choice(
        selection: str | None,
        choices: tuple[str, ...],
        field_name: str,
        environment_name: str,
    ) -> str:
        if not choices:
            raise ValueError(f"{environment_name} must contain at least one value")
        selected = selection or choices[0]
        if selected not in choices:
            raise ValueError(
                f"{field_name} must be one of the values configured in {environment_name}"
            )
        if field_name.lower().endswith("alias") and (
            selected in {".", ".."} or not ALIAS_PATTERN.fullmatch(selected)
        ):
            raise ValueError(
                f"Invalid OpenSearch alias in {environment_name}: {selected}"
            )
        return selected


config = Config.from_environment()
