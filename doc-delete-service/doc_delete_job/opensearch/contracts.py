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
    """Titan deletion counts reported by the document-deletion flow."""

    deleted_match_bucket_count: int
    updated_reverse_bucket_count: int


@dataclass(frozen=True)
class OpenSearchDeletionResult:
    """Counts produced by the OpenSearch deletion flow."""

    occurrence_count: int
    catalog_count: int
    shared_catalog_count: int


@dataclass(frozen=True)
class DatabaseDeletionResult:
    """Counts produced by the optional PostgreSQL deletion flow."""

    deleted_match_bucket_count: int = 0
    updated_reverse_bucket_count: int = 0
    deleted_embedding_count: int = 0
    deleted_sentence_count: int = 0


class DeleteResult(TypedDict):
    tsp_id: TspId
    occurrence_count: int
    catalog_count: int
    shared_catalog_count: int
    deleted_match_bucket_count: int
    updated_reverse_bucket_count: int
    deleted_embedding_count: int
    deleted_sentence_count: int
    database_delete_enabled: bool
    next_message_sent: bool


class BulkDeleteAction(TypedDict):
    delete: dict[str, str]

