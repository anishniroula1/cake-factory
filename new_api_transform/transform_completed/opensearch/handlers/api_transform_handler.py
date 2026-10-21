from time import perf_counter

from ta_utils.logger.CustomLogger import CustomLogger
from transform_completed.opensearch import constants
from transform_completed.opensearch.config import Config
from transform_completed.opensearch.contracts import ApplicationReference, TransformResult, TspId
from transform_completed.opensearch.db_store import (
    ApplicantDBStore,
    ApplicationDBStore,
    ContentDBStore,
    TransmissionDBStore,
)
from transform_completed.opensearch.errors import ApiTransformError, PermanentDataError
from transform_completed.opensearch.kafka_processor import KafkaProcessor
from transform_completed.opensearch.payload_builder import PayloadBuilder
from transform_completed.opensearch.retry import retry_operation


class ApiTransformHandler:
    """Build and publish the API payload for the application affected by one document."""

    def __init__(
        self,
        config: Config,
        application_db_store: ApplicationDBStore,
        applicant_db_store: ApplicantDBStore,
        content_db_store: ContentDBStore,
        transmission_db_store: TransmissionDBStore,
        payload_builder: PayloadBuilder,
        kafka_processor: KafkaProcessor,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.application_db_store = application_db_store
        self.applicant_db_store = applicant_db_store
        self.content_db_store = content_db_store
        self.transmission_db_store = transmission_db_store
        self.payload_builder = payload_builder
        self.kafka_processor = kafka_processor
        self.logger = custom_logger

    def process(self, tsp_id: TspId, historical: bool = False) -> TransformResult:
        """Rebuild and publish the application affected by one document.

        Input: tsp_id=10 and historical=True for a historical SQS message.
        Output: counts for that source message, its application, and Kafka events.
        """

        started_at = perf_counter()
        try:
            reference: ApplicationReference | None = retry_operation(
                "Load the application for API Transform",
                self.application_db_store.load_application_reference,
                tsp_id,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if reference is None:
                raise PermanentDataError(f"No current application was found for tsp_id={tsp_id}")

            published_count = self._process_application(reference, historical)
            self.logger.info(
                f"API Transform completed one document, one application, and {published_count} Kafka event(s)."
            )
            self.logger.info(f"API Transform took {perf_counter() - started_at:.2f} seconds to complete.")
            return {"source_message_count": 1, "application_count": 1, "kafka_event_count": published_count}
        except ApiTransformError as error:
            self.transmission_db_store.save_etl_error("API Transform failed", error)
            raise

    def _process_application(self, reference: ApplicationReference, historical: bool) -> int:
        """Load all API sections, build events, and publish one affected application.

        Input: a reference with application_id=100 and historical=True.
        Output: the number of application events confirmed by Kafka.
        """

        application_ids = [reference.application_id]
        records = retry_operation(
            "Load application records",
            self.application_db_store.load_application_records,
            [reference],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        applicants = retry_operation(
            "Load applicant details",
            self.applicant_db_store.load_applicant_details,
            application_ids,
            self.config.reduce_kafka_size,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        documents = retry_operation(
            "Load application documents",
            self.content_db_store.load_documents,
            application_ids,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        sentences = retry_operation(
            "Load Titan sentence matches",
            self.content_db_store.load_sentences,
            application_ids,
            self.config.reduce_kafka_size,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        templates = (
            retry_operation(
                "Load public templates",
                self.content_db_store.load_public_templates,
                application_ids,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if reference.analysis_group == "Asylee"
            else {}
        )

        application_id = reference.application_id
        record = records.get(application_id)
        if record is None:
            raise PermanentDataError(f"Application record is missing for application_id={application_id}")
        events = self.payload_builder.build_application_events(
            record,
            applicants.get(application_id),
            documents.get(application_id, []),
            sentences.get(application_id, []),
            templates.get(application_id),
            historical,
        )
        return self._publish_with_buffer_limit(events)

    def _publish_with_buffer_limit(self, events: list[dict]) -> int:
        """Publish before buffered event bytes approach PostgreSQL and worker limits.

        Input: application events produced by PayloadBuilder.
        Output: the total number of events confirmed by Kafka. Events are sent
        before their combined size passes the configured 450 MB safety limit.
        """

        published_count = 0
        buffered_size = 0
        buffered_events = []
        for event in events:
            event_size = self.payload_builder.serialized_size(event)
            if buffered_events and buffered_size + event_size > constants.KAFKA_BUFFER_LIMIT_BYTES:
                published_count += retry_operation(
                    "Publish API events to Kafka",
                    self.kafka_processor.publish,
                    buffered_events,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                buffered_events = []
                buffered_size = 0
            buffered_events.append(event)
            buffered_size += event_size
        if buffered_events:
            published_count += retry_operation(
                "Publish API events to Kafka",
                self.kafka_processor.publish,
                buffered_events,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        return published_count
