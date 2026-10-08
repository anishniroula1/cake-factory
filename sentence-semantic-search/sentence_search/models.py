from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, model_validator

from sentence_search.text import sentence_key

OPENSEARCH_LONG_MIN = -(2**63)
OPENSEARCH_LONG_MAX = (2**63) - 1
OPENSEARCH_INTEGER_MIN = -(2**31)
OPENSEARCH_INTEGER_MAX = (2**31) - 1


def utc_now() -> datetime:
    """Return the current UTC time."""

    return datetime.now(UTC)


class SentenceOccurrence(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

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
        """Validate the sentence and fill values controlled by the API."""

        if not self.sentenceContent:
            raise ValueError("sentenceContent cannot be empty")
        if not OPENSEARCH_LONG_MIN <= self.globalId <= OPENSEARCH_LONG_MAX:
            raise ValueError("globalId must fit in an OpenSearch signed 64-bit long")
        if self.sentIdLocal < 0:
            raise ValueError("sentIdLocal cannot be negative")

        generated_key = sentence_key(self.sentenceContent)
        if self.sentenceKey and self.sentenceKey != generated_key:
            raise ValueError(
                "sentenceKey does not match sentenceContent; omit it and "
                "the application will generate it"
            )
        self.sentenceKey = generated_key
        current_time = utc_now()
        self.createdAt = self.createdAt or current_time
        self.updatedAt = self.updatedAt or current_time
        return self


class TracerSentence(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tracerId: int
    sentenceContent: str
    analysisGroup: str
    isNonMatcher: bool
    isFormLanguage: bool
    createdAt: datetime | None = None
    updatedAt: datetime | None = None

    @model_validator(mode="after")
    def validate_record(self):
        """Validate the tracer values stored in the strict OpenSearch mapping."""

        if not OPENSEARCH_INTEGER_MIN <= self.tracerId <= OPENSEARCH_INTEGER_MAX:
            raise ValueError("tracerId must fit in an OpenSearch signed 32-bit integer")
        if not self.sentenceContent:
            raise ValueError("sentenceContent cannot be empty")
        if not self.analysisGroup:
            raise ValueError("analysisGroup cannot be empty")
        return self
