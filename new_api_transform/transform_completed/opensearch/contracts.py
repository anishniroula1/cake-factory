from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

type TspId = int | str
type SqsMessage = dict[str, Any]


@dataclass(frozen=True)
class ApplicationReference:
    """Identify one application selected by an incoming document."""

    application_id: int
    a_number: int
    analysis_group: str


@dataclass(frozen=True)
class KafkaDelivery:
    """Keep broker details returned after a successful publication."""

    application_id: int
    a_number: int
    analysis_group: str
    kafka_value: str
    timestamp: datetime
    partition: int
    offset: int


class TransformResult(TypedDict):
    """Counts returned after one API Transform processing cycle."""

    source_message_count: int
    application_count: int
    kafka_event_count: int
