from dataclasses import dataclass, field
from typing import TypedDict

TspId = int | str


class OccurrenceRecord(TypedDict):
    sentence_key: str
    analysis_group: str


@dataclass(frozen=True)
class DocumentDeletionContext:
    """Sentence global IDs required for embedding deletion."""

    global_ids: list[int]


@dataclass(frozen=True)
class TitanMatchDeletionResult:
    """Affected owners and deletion counts reported by the Titan function."""

    affected_application_ids: list[int]
    affected_tsp_ids: list[int]
    has_deleted_document: bool
    deleted_match_bucket_count: int


@dataclass(frozen=True)
class OpenSearchDeletionResult:
    """Counts produced by the OpenSearch deletion flow."""

    occurrence_count: int
    catalog_count: int


@dataclass(frozen=True)
class DatabaseDeletionResult:
    """Affected owners and counts produced by PostgreSQL deletion."""

    affected_application_ids: list[int] = field(default_factory=list)
    affected_tsp_ids: list[int] = field(default_factory=list)
    has_deleted_document: bool = False
    deleted_match_bucket_count: int = 0
    deleted_embedding_count: int = 0
    deleted_sentence_count: int = 0


class DeleteResult(TypedDict):
    """Affected owners and deletion counts returned for one document."""

    tsp_id: TspId
    affected_application_ids: list[int]
    affected_tsp_ids: list[int]
    has_deleted_document: bool
    occurrence_count: int
    catalog_count: int
    deleted_match_bucket_count: int
    deleted_embedding_count: int
    deleted_sentence_count: int


class BulkDeleteAction(TypedDict):
    delete: dict[str, str]

