import json
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from case_similarity_service.opensearch import constants
from case_similarity_service.opensearch.config import Config
from case_similarity_service.opensearch.contracts import KafkaDelivery, KafkaEvent
from case_similarity_service.opensearch.db_store.kafka_event_db_store import KafkaEventDBStore
from case_similarity_service.opensearch.errors import KafkaPublishError
from case_similarity_service.opensearch.retry import retry_operation
from ta_utils.logger.CustomLogger import CustomLogger


class KafkaProcessor:
    """Publish changed TAH values and save confirmed Kafka delivery details."""

    def __init__(
        self,
        config: Config,
        kafka_producer: Producer | None,
        kafka_event_db_store: KafkaEventDBStore,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.kafka_producer = kafka_producer
        self.kafka_event_db_store = kafka_event_db_store
        self.logger = custom_logger

    def publish_changed_tah_events(self, application_ids: list[int]) -> int:
        """Publish only TAH values that are new or different from the last
        confirmed delivery.

        Input: application_ids=[10, 20]
        Output: 1 when application 10 changed from false to true and application
        20 remained false.

        Historical processing returns 0 without connecting to Kafka when TAH
        publishing is disabled.
        """

        # Historical processing can update case summaries without publishing TAH
        # messages. In that deployment the toggle is false and this method stops here.
        if not self.config.send_tah_kafka_events:
            self.logger.info("TAH Kafka publishing is disabled for this deployment.")
            return 0
        if self.kafka_producer is None:
            raise KafkaPublishError("TAH Kafka publishing is enabled, but the producer is unavailable")

        # The lookup table removes unchanged TAH values before Kafka work starts.
        events = retry_operation(
            "Load changed TAH events",
            self.kafka_event_db_store.load_changed_tah_events,
            application_ids,
            self.config.kafka_environment,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.MAX_ATTEMPTS,
            custom_logger=self.logger,
        )

        # Publish and confirm one event at a time. A temporary failure retries only
        # the event that failed instead of reloading all application summaries.
        for event in events:
            retry_operation(
                f"Publish TAH event for application_id={event.application_id}",
                self._publish_event,
                event,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.MAX_ATTEMPTS,
                custom_logger=self.logger,
            )
        if events:
            self.logger.info(f"Published {len(events)} changed TAH event(s) to Kafka.")
        else:
            self.logger.info("No changed TAH value needed a Kafka event.")
        return len(events)

    def _publish_event(self, event: KafkaEvent) -> None:
        """Publish one TAH event and save its confirmed broker location.

        Input: KafkaEvent(receipt_number="A123456789", application_id=100,
        a_number=900001, analysis_group="Asylee", is_tah=True)

        Kafka receives is_tah, application_url, primary_a_number, and
        analysis_group. The confirmed timestamp, partition, and offset are then
        saved in kafka_ta_hit_lookup.
        """

        # The receipt number is used as the Kafka key below. The payload contains
        # the values read by the Global consumer.
        payload = {
            "is_tah": event.is_tah,
            "application_url": f"{self.config.application_url or constants.DEFAULT_APPLICATION_URL}"
            f"{event.application_id}",
            "primary_a_number": str(event.a_number),
            "analysis_group": event.analysis_group,
        }
        delivery: KafkaDelivery | None = None
        delivery_error: str | None = None

        def delivery_callback(error: KafkaError | None, message: Message) -> None:
            """Capture Kafka's delivery result for the event being published.

            Input: error=None and a message with partition=2 and offset=500, or
            a KafkaError when delivery fails.
            """

            # Update the values created by _publish_event() so the code after
            # flush() can read Kafka's confirmed delivery or failure.
            nonlocal delivery, delivery_error
            if error is not None:
                delivery_error = str(error)
                return
            delivery = KafkaDelivery(
                timestamp=self._delivery_timestamp(message), partition=message.partition(), offset=message.offset()
            )

        try:
            # produce() queues the message. poll() serves callbacks, and flush()
            # waits for Kafka to confirm whether the queued message was delivered.
            self.kafka_producer.produce(
                topic=constants.TAH_TOPIC,
                key=event.receipt_number,
                value=json.dumps(payload),
                on_delivery=delivery_callback,
            )
            self.kafka_producer.poll(0)
            pending_message_count = self.kafka_producer.flush()
        except (BufferError, KafkaException) as error:
            raise KafkaPublishError(str(error)) from error

        if delivery_error:
            raise KafkaPublishError(delivery_error)
        if pending_message_count:
            raise KafkaPublishError(f"Kafka did not deliver {pending_message_count} queued message(s)")
        if delivery is None:
            raise KafkaPublishError("Kafka did not return a delivery acknowledgement")

        # Save the lookup row only after Kafka confirms the message.
        self.kafka_event_db_store.save_kafka_delivery(event, delivery, self.config.kafka_environment)

    @staticmethod
    def _delivery_timestamp(message: Message) -> str:
        """Return Kafka's timestamp as UTC text.

        Input: message timestamp=1760123456000 milliseconds
        Output: the equivalent ISO-8601 UTC timestamp.

        The current UTC time is returned when Kafka does not provide a timestamp.
        """

        timestamp_milliseconds = message.timestamp()[1]
        if timestamp_milliseconds and timestamp_milliseconds > 0:
            return datetime.fromtimestamp(timestamp_milliseconds / 1000, UTC).isoformat()
        return datetime.now(UTC).isoformat()
