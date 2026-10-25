from dataclasses import dataclass
from typing import NotRequired, TypedDict

TspId = int | str


class SqsMessage(TypedDict, total=False):
    tsp_id: TspId
    source: str
    sent_by: str
    reason: str
    MessageId: str
    ReceiptHandle: str
    message_receive_count: int


class NextQueueMessage(TypedDict):
    source: str
    tsp_id: str
    sent_by: str


class RawSqsMessage(TypedDict, total=False):
    Body: str
    MessageId: str
    ReceiptHandle: str
    Attributes: dict[str, str]


class OccurrenceRecord(TypedDict):
    global_id: int
    sentence_key: str
    analysis_group: str


@dataclass(frozen=True)
class DocumentDeletionContext:
    """Sentence-vector identifiers that remain available during deletion."""

    tsp_id: TspId
    analysis_group: str | None
    global_ids: list[int]
    sentence_keys: list[str]


@dataclass(frozen=True)
class TitanMatchDeletionResult:
    """Counts returned by the bidirectional Titan match deletion function."""

    source_global_id_count: int
    deleted_match_bucket_count: int
    updated_reverse_bucket_count: int
    deleted_empty_bucket_count: int


@dataclass(frozen=True)
class OpenSearchDeletionResult:
    """Counts and identifiers produced by the OpenSearch deletion flow."""

    occurrence_count: int
    catalog_count: int
    shared_catalog_count: int
    global_ids: list[int]


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


class BulkItemError(TypedDict, total=False):
    status: int
    error: object
    _id: str


class BulkDeleteResponse(TypedDict, total=False):
    errors: bool
    items: list[dict[str, BulkItemError]]
    took: NotRequired[int]

