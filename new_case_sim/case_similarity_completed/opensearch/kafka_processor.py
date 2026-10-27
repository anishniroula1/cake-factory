import json
from collections import defaultdict
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.config import Config
from case_similarity_completed.opensearch.contracts import KafkaDelivery, KafkaEvent, SkippedKafkaEvent
from case_similarity_completed.opensearch.db_store.kafka_event_db_store import KafkaEventDBStore
from case_similarity_completed.opensearch.errors import KafkaPublishError
from case_similarity_completed.opensearch.retry import retry_operation, safe_error_summary
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

        # PostgreSQL returns both the events to publish and an explanation for
        # every application that cannot or does not need to publish an event.
        tah_event_candidates = retry_operation(
            "Load TAH Kafka event selection",
            self.kafka_event_db_store.load_tah_event_candidates,
            application_ids,
            self.config.kafka_environment,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.MAX_ATTEMPTS,
            custom_logger=self.logger,
        )
        self._log_skipped_kafka_events(tah_event_candidates.skipped_events)

        # Publish and confirm one event at a time. A temporary failure retries only
        # the event that failed instead of reloading all application summaries.
        for tah_event in tah_event_candidates.events:
            retry_operation(
                "Publish one TAH event to Kafka",
                self._publish_tah_event,
                tah_event,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.MAX_ATTEMPTS,
                custom_logger=self.logger,
            )
        if tah_event_candidates.events:
            self.logger.info(f"Published {len(tah_event_candidates.events)} new or changed TAH event(s) to Kafka.")
        else:
            self.logger.info("No TAH Kafka event was published. The selection reason is logged above.")
        return len(tah_event_candidates.events)

    def _log_skipped_kafka_events(self, skipped_events: list[SkippedKafkaEvent]) -> None:
        """Log grouped reasons explaining why Kafka candidates were skipped.

        Input: two unchanged applications and one application missing a GDR
        receipt. Output: two compact messages containing only the count and
        reason. Candidate values can be inspected with the diagnostic SQL.
        """

        event_count_by_reason = defaultdict(int)
        for skipped_event in skipped_events:
            event_count_by_reason[skipped_event.reason] += 1

        reason_messages = {
            "MISSING_TEXT_SIMILARITY_INPUT": "no text_similarity_input row was found",
            "MISSING_GDR_RECEIPT": "the a_number did not resolve to a GDR receipt number",
            "RECEIPT_HAS_MULTIPLE_APPLICATIONS": "the receipt number belongs to multiple affected applications",
            "UNCHANGED_TAH_VALUE": "the current TAH value already matches the last Kafka-confirmed value",
        }
        warning_reasons = {"MISSING_TEXT_SIMILARITY_INPUT", "MISSING_GDR_RECEIPT", "RECEIPT_HAS_MULTIPLE_APPLICATIONS"}

        for reason in sorted(event_count_by_reason):
            message = (
                f"Skipped {event_count_by_reason[reason]} TAH Kafka candidate(s) because "
                f"{reason_messages.get(reason, reason.lower())}."
            )
            if reason in warning_reasons:
                self.logger.warning(message)
            else:
                self.logger.info(message)

    def _publish_tah_event(self, tah_event: KafkaEvent) -> None:
        """Publish one TAH event and save its confirmed broker location.

        Input: KafkaEvent(receipt_number="A123456789", application_id=100,
        a_number=900001, analysis_group="Asylee", is_tah=True)

        Kafka receives is_tah, application_url, primary_a_number, and
        analysis_group. The confirmed timestamp, partition, and offset are then
        saved in kafka_ta_hit_lookup.
        """

        # The receipt number is used as the Kafka key below. The payload contains
        # the values read by the Global consumer.
        tah_event_payload = {
            "is_tah": tah_event.is_tah,
            "application_url": f"{self.config.application_url or constants.DEFAULT_APPLICATION_URL}"
            f"{tah_event.application_id}",
            "primary_a_number": str(tah_event.a_number),
            "analysis_group": tah_event.analysis_group,
        }
        kafka_delivery: KafkaDelivery | None = None
        kafka_delivery_error: str | None = None

        self.logger.info(f"Sending one TAH event to Kafka. topic={constants.TAH_TOPIC}.")

        def delivery_callback(error: KafkaError | None, message: Message) -> None:
            """Capture Kafka's delivery result for the event being published.

            Input: error=None and a message with partition=2 and offset=500, or
            a KafkaError when delivery fails.
            """

            # Update the values created by _publish_tah_event() so the code after
            # flush() can read Kafka's confirmed delivery or failure.
            nonlocal kafka_delivery, kafka_delivery_error
            if error is not None:
                kafka_delivery_error = safe_error_summary(error)
                return
            partition = message.partition()
            offset = message.offset()
            kafka_delivery = KafkaDelivery(
                timestamp=self._get_kafka_delivery_timestamp(message),
                partition=partition,
                offset=offset,
            )
            self.logger.info(
                f"Kafka delivery confirmed. topic={constants.TAH_TOPIC}, "
                f"partition={partition}, offset={offset}."
            )

        try:
            # produce() queues the message. poll() serves callbacks, and flush()
            # waits for Kafka to confirm whether the queued message was delivered.
            self.kafka_producer.produce(
                topic=constants.TAH_TOPIC,
                key=tah_event.receipt_number,
                value=json.dumps(tah_event_payload),
                on_delivery=delivery_callback,
            )
            self.kafka_producer.poll(0)
            pending_message_count = self.kafka_producer.flush()
        except (BufferError, KafkaException) as error:
            raise KafkaPublishError(safe_error_summary(error)) from error

        if kafka_delivery_error:
            raise KafkaPublishError(kafka_delivery_error)
        if pending_message_count:
            raise KafkaPublishError(f"Kafka did not deliver {pending_message_count} queued message(s)")
        if kafka_delivery is None:
            raise KafkaPublishError("Kafka did not return a delivery acknowledgement")

        # Save the lookup row only after Kafka confirms the message.
        self.kafka_event_db_store.save_confirmed_kafka_delivery(
            tah_event, kafka_delivery, self.config.kafka_environment
        )

    @staticmethod
    def _get_kafka_delivery_timestamp(message: Message) -> str:
        """Return Kafka's timestamp as UTC text.

        Input: message timestamp=1760123456000 milliseconds
        Output: the equivalent ISO-8601 UTC timestamp.

        The current UTC time is returned when Kafka does not provide a timestamp.
        """

        timestamp_milliseconds = message.timestamp()[1]
        if timestamp_milliseconds and timestamp_milliseconds > 0:
            return datetime.fromtimestamp(timestamp_milliseconds / 1000, UTC).isoformat()
        return datetime.now(UTC).isoformat()
