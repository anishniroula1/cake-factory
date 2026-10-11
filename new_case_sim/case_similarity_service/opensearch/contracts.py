from dataclasses import dataclass
from typing import TypedDict

TspId = int | str


class SqsMessage(TypedDict, total=False):
    tsp_id: TspId
    MessageId: str
    ReceiptHandle: str
    message_receive_count: int


class NextQueueMessage(TypedDict):
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
    """Complete Case Similarity result for one application."""

    application_id: int
    analysis_group: str
    similar_application_ids: list[int]
    similar_case_ids: list[int]
    tah_case_ids: list[int]

    @property
    def similar_case_count(self) -> int:
        """Return the number of applications that pass the similar-case rule."""

        return len(self.similar_case_ids)

    @property
    def tah_case_count(self) -> int:
        """Return the number of applications that pass the TAH rule."""

        return len(self.tah_case_ids)

    @property
    def is_tah(self) -> bool:
        """Return true when at least one target application passes the TAH rule."""

        return bool(self.tah_case_ids)


@dataclass(frozen=True)
class KafkaEvent:
    """One TAH change ready to publish."""

    receipt_number: str
    application_id: int
    a_number: int
    analysis_group: str
    is_tah: bool


@dataclass(frozen=True)
class KafkaDelivery:
    """Kafka delivery values saved after the broker accepts an event."""

    timestamp: str
    partition: int
    offset: int


class ProcessResult(TypedDict):
    tsp_id: TspId
    source_application_id: int
    rebuilt_application_count: int
    similar_case_count: int
    tah_case_count: int
    kafka_event_count: int
