import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from transform_completed.opensearch.contracts import KafkaDelivery

# Record when one application's API event was last confirmed by Kafka.
#
# Input: {"application_id": 50}
# Table used: api.transform_status
# Output: one record whose last_transmission is the current database time.
#
# The first successful delivery inserts the record. A later delivery updates
# the same application instead of creating a duplicate record.
SAVE_TRANSFORM_STATUS = text(
    """
    INSERT INTO api.transform_status (application_id, last_transmission)
    VALUES (:application_id, NOW())
    ON CONFLICT (application_id)
    DO UPDATE SET last_transmission = EXCLUDED.last_transmission
    """
)

# Save the Kafka payload and the broker location returned by its delivery
# callback.
#
# Input example:
#   {
#       "application_id": 50,
#       "a_number": 900001,
#       "analysis_group": "Asylee",
#       "kafka_partition": 2,
#       "kafka_offset": 500,
#       "kafka_environment": "prod"
#   }
#
# Table used: api.kafka_events
# Output: one audit record for the confirmed Kafka event.
SAVE_KAFKA_EVENT = text(
    """
    INSERT INTO api.kafka_events (
        analysis_group,
        a_number,
        application_id,
        kafka_value,
        kafka_ts,
        kafka_partition,
        kafka_offset,
        kafka_environment
    ) VALUES (
        :analysis_group,
        :a_number,
        :application_id,
        :kafka_value,
        :kafka_ts,
        :kafka_partition,
        :kafka_offset,
        :kafka_environment
    )
    """
)

# Remove Kafka audit records older than the configured retention window.
#
# Input: {"oldest_timestamp": "2026-08-21T00:00:00+00:00"}
# Table used: api.kafka_events
# Output: audit records before the timestamp are deleted.
DELETE_OLD_KAFKA_EVENTS = text("DELETE FROM api.kafka_events WHERE kafka_ts < :oldest_timestamp")

# Save one API Transform failure for operational investigation.
#
# Input: {"message": "API Transform failed", "exception": "missing application"}
# Table used: landing.etlerror
# Output: one error record with the current database time.
#
# The legacy batch IDs use -1 because this worker processes one SQS document
# rather than a batch file.
SAVE_ETL_ERROR = text(
    """
    INSERT INTO landing.etlerror (
        batch_file_response_id,
        batch_file_document_id,
        message,
        exception,
        load_date
    ) VALUES (-1, -1, :message, :exception, NOW())
    """
)


class TransmissionDBStore:
    """Persist Kafka delivery audits, transform status, and ETL errors."""

    def __init__(self, session_factory: Callable[[], Session], kafka_environment: str) -> None:
        self.session_factory = session_factory
        self.kafka_environment = kafka_environment

    def save_confirmed_kafka_deliveries(self, confirmed_deliveries: list[KafkaDelivery], retention_days: int) -> None:
        """Write audit and transform-status records after Kafka confirms delivery.

        Input: deliveries=[KafkaDelivery(application_id=50, partition=2,
        offset=500, ...)], retention_days=60

        Audit records older than the retention window are removed in the same
        transaction.
        """

        if not confirmed_deliveries:
            return
        oldest_timestamp = datetime.now(UTC) - timedelta(days=retention_days)
        with self.session_factory() as session:
            session.execute(DELETE_OLD_KAFKA_EVENTS, {"oldest_timestamp": oldest_timestamp})
            for confirmed_delivery in confirmed_deliveries:
                delivery_parameters = {
                    "analysis_group": confirmed_delivery.analysis_group,
                    "a_number": confirmed_delivery.a_number,
                    "application_id": confirmed_delivery.application_id,
                    "kafka_value": confirmed_delivery.kafka_value,
                    "kafka_ts": confirmed_delivery.timestamp,
                    "kafka_partition": confirmed_delivery.partition,
                    "kafka_offset": confirmed_delivery.offset,
                    "kafka_environment": self.kafka_environment,
                }
                session.execute(SAVE_KAFKA_EVENT, delivery_parameters)
                session.execute(SAVE_TRANSFORM_STATUS, {"application_id": confirmed_delivery.application_id})
            session.commit()

    def save_transform_error(self, message: str, error: Exception) -> None:
        """Save one transformation failure using a separate transaction.

        Input: message="Failed to transform application 50",
        error=ValueError("missing applicant data")
        """

        with self.session_factory() as session:
            session.execute(SAVE_ETL_ERROR, {"message": message, "exception": str(error)[:4000]})
            session.commit()

    @staticmethod
    def build_kafka_audit_value(application_event: dict, verbose_logging: bool) -> str:
        """Create an audit copy without changing the Kafka event.

        Input: event containing 100 sentences, verbose_logging=False
        Output: JSON audit text where payload.sentences is 100 instead of the
        complete sentence list.
        """

        audit_event = json.loads(json.dumps(application_event))
        audit_payload = json.loads(audit_event["after"]["payload"])
        if not verbose_logging:
            audit_payload["sentences"] = len(audit_payload.get("sentences", []))
        audit_event["after"]["payload"] = audit_payload
        return json.dumps(audit_event)
