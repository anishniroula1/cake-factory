import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MATCH_THRESHOLD = 90


def _integer(name: str, default: int, minimum: int = 1) -> int:
    """Read a positive whole number from the environment."""

    raw_value = os.getenv(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _boolean(name: str, default: bool) -> bool:
    """Read a true or false environment setting."""

    raw_value = os.getenv(name, str(default)).strip().lower()
    if raw_value in {"true", "1", "yes"}:
        return True
    if raw_value in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True)
class Config:
    aws_region: str
    iam_access_role: str
    opensearch_host: str
    ivf_nprobes_enabled: bool
    ivf_nprobes: int
    occurrence_alias: str
    asylee_catalog_alias: str
    refugee_catalog_alias: str

    @classmethod
    def from_environment(cls):
        """Build the application settings from environment variables."""

        config = cls(
            aws_region=os.getenv("AWS_REGION", "us-east-1").strip(),
            iam_access_role=os.getenv("IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            ivf_nprobes_enabled=_boolean(
                "OPENSEARCH_IVF_NPROBES_ENABLED",
                False,
            ),
            ivf_nprobes=_integer("OPENSEARCH_IVF_NPROBES", 64),
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
        )
        config.validate()
        return config

    def validate(self):
        """Stop early when settings would create an invalid application."""

        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        aliases = {
            self.occurrence_alias,
            self.asylee_catalog_alias,
            self.refugee_catalog_alias,
        }
        if "" in aliases:
            raise ValueError("Every OpenSearch alias is required")
        if len(aliases) != 3:
            raise ValueError("Every OpenSearch alias must be different")

    def catalog_alias_for(self, analysis_group: str) -> str:
        """Select the catalog that belongs to the requested analysis group."""

        normalized_group = analysis_group.strip().lower()
        if normalized_group == "asylee":
            return self.asylee_catalog_alias
        if normalized_group == "refugee":
            return self.refugee_catalog_alias
        raise ValueError("analysisGroup must be Asylee or Refugee")


config = Config.from_environment()
