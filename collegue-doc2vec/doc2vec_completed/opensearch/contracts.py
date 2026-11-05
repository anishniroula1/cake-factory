from typing import Literal, NotRequired, TypedDict

type TspId = int | str


class RawSqsAttributes(TypedDict, total=False):
    """Delivery attributes returned by SQS."""

    ApproximateReceiveCount: str


class RawSqsMessage(TypedDict, total=False):
    """Raw boto3 SQS message before JSON decoding."""

    Body: str
    MessageId: str
    ReceiptHandle: str
    Attributes: RawSqsAttributes


class SqsMessage(TypedDict, total=False):
    """Decoded document message and its delivery metadata."""

    reason: str
    tsp_id: TspId | list[TspId]
    tracer_sentence_id: int | str
    MessageId: str | None
    ReceiptHandle: str | None
    message_receive_count: int


class HistoricalNextQueueMessage(TypedDict):
    """Message sent to Sentence Similarity after indexing completes."""

    source: Literal["P2"]
    tsp_id: TspId
    sent_by: Literal["doc2vec"]


class LiveNextQueueMessage(TypedDict):
    """State-machine message sent after one live document is processed."""

    source: Literal["P2"]
    tsp_id: TspId
    sent_by: Literal["doc2vec"]
    success: bool
    reason: str


type NextQueueMessage = HistoricalNextQueueMessage | LiveNextQueueMessage


class HistoricalResult(TypedDict):
    """Counts returned after one historical document finishes indexing."""

    reason: Literal["HISTORICAL"]
    tsp_id: TspId
    source_count: int
    indexed_count: int


class HistoricalBatchResult(TypedDict):
    """Combined counts after every historical document in one message finishes."""

    reason: Literal["HISTORICAL"]
    tsp_ids: list[TspId]
    document_count: int
    source_count: int
    indexed_count: int


class LiveResult(TypedDict):
    """Counts returned after one incoming document finishes processing."""

    tsp_id: TspId
    source_count: int
    indexed_count: int
    success: bool
    reason: str


class TrainModelResult(TypedDict):
    """Counts returned after one document's training sentences are indexed."""

    reason: Literal["TRAIN_MODEL"]
    tsp_id: TspId
    source_count: int
    unique_count: int
    indexed_count: int


class TracerResult(TypedDict):
    """Counts returned after requested tracer references are indexed."""

    reason: Literal["TRACER"]
    source_count: int
    indexed_count: int


class CatalogDocument(TypedDict):
    """Unique sentence document written to an analysis-group catalog."""

    _id: str
    sentenceKey: str
    sentenceContent: str
    wordCount: int
    createdAt: str
    updatedAt: str
    isNonMatcher: NotRequired[bool]
    isTracer: NotRequired[bool]
    isFormLanguage: NotRequired[bool]
    sentenceContentVector: NotRequired[list[float]]


class Classification(TypedDict):
    """Classification shared by a catalog key and its sentence occurrences."""

    is_non_matcher: bool
    is_tracer: bool
    is_form_language: bool


class CatalogRecord(TypedDict):
    """Catalog data needed to classify one unique sentence key."""

    sentence_key: str
    vector: list[float]
    classification: Classification | None


class CatalogClassificationDocument(TypedDict):
    """Partial catalog update containing only reusable classification fields."""

    _id: str
    isNonMatcher: bool
    isTracer: bool
    isFormLanguage: bool
    updatedAt: str


class OccurrenceDocument(TypedDict):
    """Sentence occurrence document written for one global ID."""

    _id: int
    globalId: int
    applicationId: int
    tspId: int
    sectionName: str
    sentIdLocal: int
    sentenceKey: str
    sourceType: str
    analysisGroup: str
    isCurrent: bool
    createdAt: str
    updatedAt: str


class TrainingDocument(TypedDict):
    """Unique sentence document written to the IVF model training index."""

    _id: str
    sentenceKey: str
    sentenceContent: str
    createdAt: str
    updatedAt: str
    sentenceContentVector: NotRequired[list[float]]


class TracerDocument(TypedDict):
    """Reference sentence written to the tracer and form-language catalog."""

    _id: int
    tracerId: int
    sentenceContent: str
    analysisGroup: str
    isNonMatcher: bool
    isFormLanguage: bool
    createdAt: str
    updatedAt: str
    sentenceContentVector: NotRequired[list[float]]


type OpenSearchDocument = (
    CatalogDocument
    | CatalogClassificationDocument
    | OccurrenceDocument
    | TrainingDocument
    | TracerDocument
)

