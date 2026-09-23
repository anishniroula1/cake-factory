from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sentence_migration.config import config
from sentence_migration.text import sentence_key

OPENSEARCH_LONG_MIN = -(2**63)
OPENSEARCH_LONG_MAX = (2**63) - 1


def utc_now() -> datetime:
    return datetime.now(UTC)


class ApiModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )


class SentenceOccurrence(ApiModel):
    applicationId: str
    tspId: str
    sectionName: str
    globalId: int
    sentIdLocal: int
    sentenceContent: str
    isTracer: bool = False
    isFormLanguage: bool = False
    sentenceKey: str | None = None
    sourceType: str
    createdAt: datetime | None = None
    updatedAt: datetime | None = None
    analysisGroup: str

    @model_validator(mode="after")
    def prepare_record(self):
        if not self.sentenceContent:
            raise ValueError("sentenceContent cannot be empty")
        if not OPENSEARCH_LONG_MIN <= self.globalId <= OPENSEARCH_LONG_MAX:
            raise ValueError("globalId must fit in an OpenSearch signed 64-bit long")
        if self.sentIdLocal < 0:
            raise ValueError("sentIdLocal cannot be negative")
        self.sentenceKey = sentence_key(self.sentenceContent)
        current_time = utc_now()
        self.createdAt = self.createdAt or current_time
        self.updatedAt = self.updatedAt or current_time
        return self


class SourceSentence(SentenceOccurrence):
    """External sentence record, including its existing embedding vector."""

    sentenceContentVector: list[float] = Field(min_length=1)


class OpenSearchRoleSelection(ApiModel):
    iamRole: str | None = Field(
        default=None,
        description=(
            "Optional OpenSearch IAM role selection. The first configured role is "
            "used when omitted. Values come only from OPENSEARCH_IAM_ROLES."
        ),
        json_schema_extra={"enum": list(config.opensearch_iam_roles)},
    )

    @model_validator(mode="after")
    def resolve_role(self):
        self.iamRole = config.resolve_iam_role(self.iamRole)
        return self


class SeedSentencesRequest(OpenSearchRoleSelection):
    s3File: str = Field(min_length=1, max_length=1024)
    occurrenceAlias: str | None = Field(
        default=None,
        description="Existing occurrence alias selected from the environment list.",
        json_schema_extra={"enum": list(config.occurrence_aliases)},
    )
    catalogAlias: str | None = Field(
        default=None,
        description="Existing semantic catalog alias selected from the environment list.",
        json_schema_extra={"enum": list(config.catalog_aliases)},
    )

    @model_validator(mode="after")
    def validate_seed_request(self):
        self.occurrenceAlias = config.resolve_occurrence_alias(self.occurrenceAlias)
        self.catalogAlias = config.resolve_catalog_alias(self.catalogAlias)
        if self.occurrenceAlias == self.catalogAlias:
            raise ValueError("occurrenceAlias and catalogAlias must differ")
        if self.s3File.startswith("/") or "\x00" in self.s3File:
            raise ValueError("s3File must be a relative S3 object key")
        return self


class OpenSearchSearchRequest(OpenSearchRoleSelection):
    alias: str | None = Field(
        default=None,
        description="OpenSearch alias selected from the configured alias lists.",
        json_schema_extra={"enum": list(config.all_aliases)},
    )
    body: dict[str, Any]

    @model_validator(mode="after")
    def resolve_search_alias(self):
        self.alias = config.resolve_alias(self.alias)
        return self


class ReadOnlyMethod(StrEnum):
    GET = "GET"
    HEAD = "HEAD"
    POST = "POST"


class ReadOnlyOpenSearchRequest(OpenSearchRoleSelection):
    method: ReadOnlyMethod = ReadOnlyMethod.GET
    endpoint: str = Field(min_length=1, max_length=2048)
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)
    body: dict[str, Any] | list[Any] | str | None = None
