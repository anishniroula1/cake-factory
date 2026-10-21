from typing import NotRequired, TypedDict

TspId = int | str


class RawSqsAttributes(TypedDict, total=False):
    ApproximateReceiveCount: str


class RawSqsMessage(TypedDict, total=False):
    Body: str
    MessageId: str
    ReceiptHandle: str
    Attributes: RawSqsAttributes


class SqsMessage(TypedDict, total=False):
    tsp_id: TspId
    reason: str
    MessageId: str | None
    ReceiptHandle: str | None
    message_receive_count: int


class NextQueueMessage(TypedDict):
    tsp_id: TspId


class Classification(TypedDict):
    is_non_matcher: bool
    is_tracer: bool
    is_form_language: bool


class CatalogRecord(TypedDict):
    sentence_key: str
    word_count: int
    vector: list[float]
    classification: Classification | None


class TargetOccurrence(TypedDict):
    global_id: int
    tsp_id: int
    application_id: int
    sentence_key: str
    distance: float


class MatchTarget(TypedDict):
    tsp_id: int
    application_id: int
    distance: float


class SentenceResult(TypedDict):
    is_non_matcher: bool
    is_tracer: bool
    is_form_language: bool
    saved_relationships: int


class ProcessResult(TypedDict):
    tsp_id: TspId
    source_count: int
    processed_count: int
    non_matcher_count: int
    tracer_count: int
    form_language_count: int
    matched_sentence_count: int
    saved_relationship_count: int


class CatalogSearchHit(TypedDict):
    sentence_key: str
    distance: float
    classification: NotRequired[Classification]
