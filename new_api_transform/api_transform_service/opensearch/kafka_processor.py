import json
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from api_transform_service.opensearch import constants
from api_transform_service.opensearch.config import Config
from api_transform_service.opensearch.contracts import KafkaDelivery
from api_transform_service.opensearch.db_store.transmission_db_store import TransmissionDBStore
from api_transform_service.opensearch.errors import KafkaPublishError
from ta_utils.logger.CustomLogger import CustomLogger


class KafkaProcessor:
    """Publish API events to Kafka and save broker-confirmed audit records."""

    def __init__(
        self,
        config: Config,
        producer: Producer,
        transmission_db_store: TransmissionDBStore,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.producer = producer
        self.transmission_db_store = transmission_db_store
        self.logger = custom_logger

    def publish(self, events: list[dict]) -> int:
        """Publish a group of application events and require every acknowledgement.

        Input: a list containing one or more API events.
        Output: the number of events acknowledged by Kafka. The inner payload is
        stored as JSON text because existing consumers expect that format.

        Raises: KafkaPublishError when Kafka rejects an event or does not confirm every delivery.
        """

        deliveries: list[KafkaDelivery] = []
        delivery_errors: list[str] = []
        try:
            for event in events:
                kafka_event = self._serialize_payload(event)
                kafka_value = json.dumps(kafka_event, default=str)
                self.producer.produce(
                    topic=constants.KAFKA_TOPIC,
                    key=str(event["after"]["application_id"]),
                    value=kafka_value,
                    on_delivery=self._delivery_callback(event, kafka_value, deliveries, delivery_errors),
                )
                self.producer.poll(0)
            pending = self.producer.flush()
        except (BufferError, KafkaException) as error:
            raise KafkaPublishError(str(error)) from error

        if delivery_errors:
            raise KafkaPublishError("; ".join(delivery_errors[:5]))
        if pending:
            raise KafkaPublishError(f"Kafka did not deliver {pending} queued message(s)")
        if len(deliveries) != len(events):
            raise KafkaPublishError(f"Kafka confirmed {len(deliveries)} of {len(events)} event(s)")

        self.transmission_db_store.save_deliveries(deliveries, constants.KAFKA_AUDIT_RETENTION_DAYS)
        self.logger.info(f"Published and confirmed {len(deliveries)} API event(s) to Kafka.")
        return len(deliveries)

    @staticmethod
    def _serialize_payload(event: dict) -> dict:
        """Copy an event and convert its inner payload into JSON text.

        Input: an event whose after.payload value is a dictionary.
        Output: a copy whose after.payload value is a JSON string.
        """

        kafka_event = json.loads(json.dumps(event, default=str))
        kafka_event["after"]["payload"] = json.dumps(kafka_event["after"]["payload"])
        return kafka_event

    def _delivery_callback(self, event: dict, kafka_value: str, deliveries: list[KafkaDelivery], errors: list[str]):
        """Create the callback that records one event's Kafka delivery result.

        Input: one source event, its serialized value, and shared success and error lists.
        Output: a callback that adds either a KafkaDelivery audit record or an error message.
        """

        def callback(error: KafkaError | None, message: Message) -> None:
            # Kafka calls this function later. nonlocal lists let the outer
            # publish method inspect every success and failure after flush().
            if error is not None:
                errors.append(str(error))
                return
            timestamp_milliseconds = message.timestamp()[1]
            timestamp = (
                datetime.fromtimestamp(timestamp_milliseconds / 1000, UTC)
                if timestamp_milliseconds and timestamp_milliseconds > 0
                else datetime.now(UTC)
            )
            audit_value = self.transmission_db_store.audit_value(
                json.loads(kafka_value), self.config.verbose_kafka_logging
            )
            deliveries.append(
                KafkaDelivery(
                    application_id=int(event["after"]["application_id"]),
                    a_number=int(event["after"]["a_number"]),
                    analysis_group=str(event["after"]["analysis_group"]),
                    kafka_value=audit_value,
                    timestamp=timestamp,
                    partition=message.partition(),
                    offset=message.offset(),
                )
            )

        return callback
