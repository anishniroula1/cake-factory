import json
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from ta_utils.logger.CustomLogger import CustomLogger
from transform_completed.opensearch import constants
from transform_completed.opensearch.config import Config
from transform_completed.opensearch.contracts import KafkaDelivery
from transform_completed.opensearch.db_store.transmission_db_store import TransmissionDBStore
from transform_completed.opensearch.errors import KafkaPublishError


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

    def publish_application_events(self, application_events: list[dict]) -> int:
        """Publish a group of application events and require every acknowledgement.

        Input: a list containing one or more API events.
        Output: the number of events acknowledged by Kafka. The inner payload is
        stored as JSON text because existing consumers expect that format.

        Raises: KafkaPublishError when Kafka rejects an event or does not confirm every delivery.
        """

        if not application_events:
            self.logger.info("No API events were available to send to Kafka.")
            return 0

        deliveries: list[KafkaDelivery] = []
        delivery_errors: list[str] = []
        self.logger.info(f"Sending {len(application_events)} API event(s) to Kafka. topic={constants.KAFKA_TOPIC}.")
        try:
            for application_event in application_events:
                kafka_event = self._serialize_payload_for_kafka(application_event)
                kafka_value = json.dumps(kafka_event, default=str)
                self.producer.produce(
                    topic=constants.KAFKA_TOPIC,
                    key=str(application_event["after"]["application_id"]),
                    value=kafka_value,
                    on_delivery=self._create_delivery_callback(
                        application_event, kafka_value, deliveries, delivery_errors
                    ),
                )
                self.producer.poll(0)
            pending = self.producer.flush()
        except (BufferError, KafkaException) as error:
            raise KafkaPublishError(str(error)) from error

        if delivery_errors:
            raise KafkaPublishError("; ".join(delivery_errors[:5]))
        if pending:
            raise KafkaPublishError(f"Kafka did not deliver {pending} queued message(s)")
        if len(deliveries) != len(application_events):
            raise KafkaPublishError(f"Kafka confirmed {len(deliveries)} of {len(application_events)} event(s)")

        self.logger.info(
            f"Kafka confirmed {len(deliveries)} API event(s). Saving delivery audits to api.kafka_events and "
            "api.transform_status."
        )
        self.transmission_db_store.save_confirmed_kafka_deliveries(deliveries, constants.KAFKA_AUDIT_RETENTION_DAYS)
        self.logger.info(
            f"Saved {len(deliveries)} Kafka delivery audit record(s) to api.kafka_events and "
            "api.transform_status."
        )
        return len(deliveries)

    @staticmethod
    def _serialize_payload_for_kafka(application_event: dict) -> dict:
        """Copy an event and convert its inner payload into JSON text.

        Input: an event whose after.payload value is a dictionary.
        Output: a copy whose after.payload value is a JSON string.
        """

        kafka_event = json.loads(json.dumps(application_event, default=str))
        kafka_event["after"]["payload"] = json.dumps(kafka_event["after"]["payload"])
        return kafka_event

    def _create_delivery_callback(
        self,
        application_event: dict,
        kafka_value: str,
        confirmed_deliveries: list[KafkaDelivery],
        delivery_errors: list[str],
    ):
        """Create the callback that records one event's Kafka delivery result.

        Input: one source event, its serialized value, and shared success and error lists.
        Output: a callback that adds either a KafkaDelivery audit record or an error message.
        """

        def record_delivery_result(error: KafkaError | None, message: Message) -> None:
            # Kafka calls this function later. nonlocal lists let the outer
            # publish method inspect every success and failure after flush().
            if error is not None:
                delivery_errors.append(str(error))
                return
            timestamp_milliseconds = message.timestamp()[1]
            timestamp = (
                datetime.fromtimestamp(timestamp_milliseconds / 1000, UTC)
                if timestamp_milliseconds and timestamp_milliseconds > 0
                else datetime.now(UTC)
            )
            audit_value = self.transmission_db_store.build_kafka_audit_value(
                json.loads(kafka_value), self.config.verbose_kafka_logging
            )
            application_id = int(application_event["after"]["application_id"])
            analysis_group = str(application_event["after"]["analysis_group"])
            partition = message.partition()
            offset = message.offset()
            confirmed_deliveries.append(
                KafkaDelivery(
                    application_id=application_id,
                    a_number=int(application_event["after"]["a_number"]),
                    analysis_group=analysis_group,
                    kafka_value=audit_value,
                    timestamp=timestamp,
                    partition=partition,
                    offset=offset,
                )
            )
            self.logger.info(
                f"Kafka delivery confirmed. topic={constants.KAFKA_TOPIC}, partition={partition}, offset={offset}."
            )

        return record_delivery_result
