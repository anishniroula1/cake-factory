from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from case_similarity_service.opensearch.contracts import KafkaDelivery, KafkaEvent

# Build Kafka events only when an application has a new TAH value.
# Example: the saved value is false and the current value is true, so one event
# is returned. If both values are true, nothing is returned.
LOAD_CHANGED_TAH_EVENTS = text(
    """
    -- One application can have many document rows. Keep one copy of its values.
    WITH application_values AS (
        SELECT DISTINCT
            input.application_id,
            input.a_number,
            input.analysis_group,
            -- No summary row means the application currently has no match and
            -- therefore has a false TAH value.
            COALESCE(summary.is_tah, FALSE) AS is_tah
        FROM tsp.text_similarity_input AS input
        LEFT JOIN tsp.titan_similar_cases AS summary
            ON summary.application_id = input.application_id
        WHERE input.application_id = ANY(:application_ids)
    ),
    -- Use a_number to find the receipt number used as the Kafka message key.
    candidates AS (
        SELECT DISTINCT
            registry.receipt_number,
            application_values.application_id,
            application_values.a_number,
            application_values.analysis_group,
            application_values.is_tah
        FROM application_values
        JOIN landing.global_document_registry AS registry
            ON registry.a_number = application_values.a_number
    ),
    -- Skip a receipt number when it points to more than one application because
    -- the service would not know which application's TAH value to publish.
    valid_receipts AS (
        SELECT receipt_number
        FROM candidates
        GROUP BY receipt_number
        HAVING COUNT(DISTINCT application_id) = 1
    )
    SELECT
        candidates.receipt_number,
        candidates.application_id,
        candidates.a_number,
        candidates.analysis_group,
        candidates.is_tah
    FROM candidates
    JOIN valid_receipts USING (receipt_number)
    -- This lookup row represents the last message confirmed by Kafka.
    LEFT JOIN validation.kafka_ta_hit_lookup AS published
        ON published.receipt_number = candidates.receipt_number
       AND published.kafka_environment = :kafka_environment
    -- Return the first value or a changed value. Ignore an unchanged value.
    WHERE published.receipt_number IS NULL
       OR published.is_tah IS DISTINCT FROM candidates.is_tah
    ORDER BY candidates.application_id, candidates.receipt_number
    """
)

# Save Kafka's delivery result. A later run reads this value and avoids sending
# the same TAH value again.
SAVE_KAFKA_DELIVERY = text(
    """
    INSERT INTO validation.kafka_ta_hit_lookup (
        receipt_number,
        kafka_environment,
        is_tah,
        kafka_ts,
        kafka_partition,
        kafka_offset
    )
    VALUES (
        :receipt_number,
        :kafka_environment,
        :is_tah,
        :kafka_ts,
        :kafka_partition,
        :kafka_offset
    )
    -- Replace the old delivery details after Kafka confirms a newer value.
    ON CONFLICT (receipt_number, kafka_environment)
    DO UPDATE SET
        is_tah = EXCLUDED.is_tah,
        kafka_ts = EXCLUDED.kafka_ts,
        kafka_partition = EXCLUDED.kafka_partition,
        kafka_offset = EXCLUDED.kafka_offset
    """
)


class KafkaEventDBStore:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        """Keep the database reads and writes used by TAH Kafka publishing."""

        self.session_factory = session_factory

    def load_changed_tah_events(self, application_ids: list[int], kafka_environment: str) -> list[KafkaEvent]:
        """Return only applications whose TAH value needs a Kafka event.

        Input: application_ids=[100, 200], kafka_environment="prod".
        Output: KafkaEvent objects for new or changed values. An unchanged value
        is not included.
        """

        if not application_ids:
            return []

        # Remove duplicate IDs before sending them to PostgreSQL. For example,
        # [100, 100, 200] becomes [100, 200].
        unique_application_ids = sorted(set(application_ids))
        with self.session_factory() as session:
            rows = list(
                session.execute(
                    LOAD_CHANGED_TAH_EVENTS,
                    {"application_ids": unique_application_ids, "kafka_environment": kafka_environment},
                ).mappings()
            )

            # Keep SQLAlchemy rows inside this method and return simple objects
            # that the Kafka processor can use after the session closes.
            return [
                KafkaEvent(
                    receipt_number=str(row["receipt_number"]),
                    application_id=int(row["application_id"]),
                    a_number=int(row["a_number"]),
                    analysis_group=str(row["analysis_group"]),
                    is_tah=bool(row["is_tah"]),
                )
                for row in rows
            ]

    def save_kafka_delivery(self, event: KafkaEvent, delivery: KafkaDelivery, kafka_environment: str) -> None:
        """Save one Kafka-confirmed TAH value and its broker location.

        Input: a TAH event plus Kafka timestamp, partition, and offset.
        Result: the lookup row is inserted or replaced for this receipt number
        and environment.
        """

        with self.session_factory() as session:
            # The row is saved only after Kafka confirms delivery. Failed sends
            # therefore remain eligible for another publishing attempt.
            session.execute(
                SAVE_KAFKA_DELIVERY,
                {
                    "receipt_number": event.receipt_number,
                    "kafka_environment": kafka_environment,
                    "is_tah": event.is_tah,
                    "kafka_ts": delivery.timestamp,
                    "kafka_partition": delivery.partition,
                    "kafka_offset": delivery.offset,
                },
            )
            session.commit()
