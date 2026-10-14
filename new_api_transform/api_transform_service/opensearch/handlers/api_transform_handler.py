from api_transform_service.opensearch.config import Config
from api_transform_service.opensearch.contracts import TransformResult, TspId
from api_transform_service.opensearch.db_store import (
    ApplicantDBStore,
    ApplicationDBStore,
    ContentDBStore,
    TransmissionDBStore,
)
from api_transform_service.opensearch.errors import ApiTransformError, PermanentDataError
from api_transform_service.opensearch.kafka_processor import KafkaProcessor
from api_transform_service.opensearch.payload_builder import PayloadBuilder
from api_transform_service.opensearch.retry import retry_operation
from ta_utils.logger.CustomLogger import CustomLogger


class ApiTransformHandler:
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
        """Connect database reads, payload building, and Kafka publication."""

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

        try:
            reference = self._retry(
                "Load the application for API Transform", self.application_db_store.load_application_reference, tsp_id
            )
            if reference is None:
                raise PermanentDataError(f"No current application was found for tsp_id={tsp_id}")

            published_count = self._process_application(reference, historical)
            self.logger.info(
                f"API Transform completed one document, one application, and {published_count} Kafka event(s)."
            )
            return {"source_message_count": 1, "application_count": 1, "kafka_event_count": published_count}
        except ApiTransformError as error:
            self.transmission_db_store.save_etl_error("API Transform failed", error)
            raise

    def _process_application(self, reference, historical: bool) -> int:
        """Load every related section and publish one affected application."""

        application_ids = [reference.application_id]
        records = self._retry(
            "Load application records", self.application_db_store.load_application_records, [reference]
        )
        applicants = self._retry(
            "Load applicant details",
            self.applicant_db_store.load_applicant_details,
            application_ids,
            self.config.reduce_kafka_size,
        )
        documents = self._retry("Load application documents", self.content_db_store.load_documents, application_ids)
        sentences = self._retry(
            "Load Titan sentence matches",
            self.content_db_store.load_sentences,
            application_ids,
            self.config.reduce_kafka_size,
        )
        templates = (
            self._retry("Load public templates", self.content_db_store.load_public_templates, application_ids)
            if self.config.analysis_group == "Asylee"
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

        This keeps the current 450 MB safety boundary even when one application
        is split into several Kafka events.
        """

        published_count = 0
        buffered_size = 0
        buffered_events = []
        for event in events:
            event_size = self.payload_builder.serialized_size(event)
            if buffered_events and buffered_size + event_size > self.config.kafka_buffer_limit_bytes:
                published_count += self._retry(
                    "Publish API events to Kafka", self.kafka_processor.publish, buffered_events
                )
                buffered_events = []
                buffered_size = 0
            buffered_events.append(event)
            buffered_size += event_size
        if buffered_events:
            published_count += self._retry("Publish API events to Kafka", self.kafka_processor.publish, buffered_events)
        return published_count

    def _retry(self, label, operation, *arguments):
        """Apply the service retry settings to one isolated operation."""

        return retry_operation(
            label,
            operation,
            *arguments,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.max_attempts,
            custom_logger=self.logger,
        )
