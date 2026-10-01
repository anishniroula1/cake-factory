from typing import Literal, NotRequired, Protocol, TypedDict

type TspId = int | str


class RawSqsAttributes(TypedDict, total=False):
    ApproximateReceiveCount: str


class RawSqsMessage(TypedDict, total=False):
    Body: str
    MessageId: str
    ReceiptHandle: str
    Attributes: RawSqsAttributes


class SqsMessage(TypedDict, total=False):
    reason: str
    tsp_id: TspId
    MessageId: str | None
    ReceiptHandle: str | None
    message_receive_count: int


class NextQueueMessage(TypedDict):
    reason: Literal["HISTORICAL"]
    tsp_id: TspId


class HistoricalResult(TypedDict):
    reason: Literal["HISTORICAL"]
    tsp_id: TspId
    source_count: int
    indexed_count: int


class CatalogDocument(TypedDict):
    _id: str
    sentenceKey: str
    sentenceContent: str
    createdAt: str
    updatedAt: str
    sentenceContentVector: NotRequired[list[float]]


class OccurrenceDocument(TypedDict):
    _id: int
    globalId: int
    applicationId: str
    tspId: str
    sectionName: str
    sentIdLocal: int
    sentenceContent: str
    sentenceKey: str
    sourceType: str
    analysisGroup: str
    isNonMatcher: bool
    isTracer: bool
    isFormLanguage: bool
    createdAt: str
    updatedAt: str


type OpenSearchDocument = CatalogDocument | OccurrenceDocument


class Logger(Protocol):
    def debug(self, message: str) -> None: ...

    def info(self, message: str) -> None: ...

    def warning(self, message: str) -> None: ...

    def error(self, message: str) -> None: ...

    def exception(self, message: str) -> None: ...
