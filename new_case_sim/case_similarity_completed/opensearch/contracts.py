from dataclasses import dataclass
from typing import TypedDict

TspId = int | str


class SqsMessage(TypedDict, total=False):
    """Source SQS message and its delivery metadata."""

    tsp_id: TspId
    MessageId: str
    ReceiptHandle: str
    message_receive_count: int


class NextQueueMessage(TypedDict):
    """Message sent after Case Similarity finishes a document."""

    source: str
    tsp_id: TspId


@dataclass(frozen=True)
class DocumentContext:
    """Values that identify the document and its case."""

    tsp_id: TspId
    application_id: int
    a_number: int
    analysis_group: str


@dataclass(frozen=True)
class CaseSummary:
    """Counts returned after one application summary is refreshed."""

    application_id: int
    analysis_group: str
    similar_case_count: int
    tah_case_count: int


@dataclass(frozen=True)
class KafkaEvent:
    """One TAH change ready to publish."""

    receipt_number: str
    application_id: int
    a_number: int
    analysis_group: str
    is_tah: bool


@dataclass(frozen=True)
class SkippedKafkaEvent:
    """Explain why an affected application did not produce a Kafka event."""

    application_id: int
    receipt_number: str | None
    reason: str
    current_is_tah: bool
    published_is_tah: bool | None


@dataclass(frozen=True)
class KafkaEventSelection:
    """Hold the Kafka events to publish and the candidates that were skipped."""

    events: list[KafkaEvent]
    skipped_events: list[SkippedKafkaEvent]


@dataclass(frozen=True)
class KafkaDelivery:
    """Kafka delivery values saved after the broker accepts an event."""

    timestamp: str
    partition: int
    offset: int


class ProcessResult(TypedDict):
    """Counts returned after one document completes Case Similarity."""

    tsp_id: TspId
    source_application_id: int
    rebuilt_application_count: int
    similar_case_count: int
    tah_case_count: int
    kafka_event_count: int
