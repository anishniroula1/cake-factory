from dataclasses import dataclass
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
    """Result returned after deleting one document's Titan matches."""

    has_deleted_document: bool
    deleted_match_bucket_count: int


@dataclass(frozen=True)
class OpenSearchDeletionResult:
    """Counts produced by the OpenSearch deletion flow."""

    occurrence_count: int
    catalog_count: int


@dataclass(frozen=True)
class DatabaseDeletionResult:
    """Counts produced by PostgreSQL deletion."""

    has_deleted_document: bool = False
    deleted_match_bucket_count: int = 0
    deleted_embedding_count: int = 0
    deleted_sentence_count: int = 0


class DeleteResult(TypedDict):
    """Deletion counts returned for one document."""

    tsp_id: TspId
    has_deleted_document: bool
    occurrence_count: int
    catalog_count: int
    deleted_match_bucket_count: int
    deleted_embedding_count: int
    deleted_sentence_count: int


class BulkDeleteAction(TypedDict):
    delete: dict[str, str]

