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
from transform_completed.opensearch.retry import retry_operation, safe_error_summary


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

    def process_document(self, tsp_id: TspId) -> TransformResult:
        """Rebuild and publish the application affected by one document.

        Input: tsp_id=10
        Output: counts for that source message, its application, and Kafka events.
        """

        started_at = perf_counter()
        try:
            self.logger.info("Loading the application reference from tsp.text_similarity_input.")
            application_reference: ApplicationReference | None = retry_operation(
                "Load the application for API Transform",
                self.application_db_store.load_application_reference,
                tsp_id,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if application_reference is None:
                raise PermanentDataError(f"No current application was found for tsp_id={tsp_id}")
            application_id = application_reference.application_id
            analysis_group = application_reference.analysis_group
            self.logger.info(
                "Loaded the application reference from tsp.text_similarity_input. "
                f"application_id={application_id}, analysis_group={analysis_group}."
            )

            published_event_count = self._build_and_publish_application_events(application_reference)
            self.logger.info(
                f"API Transform completed one document, one application, and {published_event_count} Kafka event(s)."
            )
            self.logger.info(f"API Transform took {perf_counter() - started_at:.2f} seconds to complete.")
            return {"source_message_count": 1, "application_count": 1, "kafka_event_count": published_event_count}
        except ApiTransformError as error:
            try:
                retry_operation(
                    "Save the API Transform failure record",
                    self.transmission_db_store.save_transform_error,
                    "API Transform failed",
                    error,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
            except ApiTransformError as audit_error:
                self.logger.error(
                    "The API Transform failure record could not be saved: "
                    f"{safe_error_summary(audit_error)}"
                )
            raise

    def _build_and_publish_application_events(self, application_reference: ApplicationReference) -> int:
        """Load all API sections, build events, and publish one affected application.

        Input: a reference with application_id=100.
        Output: the number of application events confirmed by Kafka.
        """

        application_id = application_reference.application_id
        application_ids = [application_id]
        self.logger.info("Loading the application record from landing.application_id_lookup.")
        application_payload_records = retry_operation(
            "Load application records",
            self.application_db_store.load_application_payload_records,
            [application_reference],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Loaded {len(application_payload_records)} application record(s) from landing.application_id_lookup."
        )

        self.logger.info("Loading applicant details from biodata.applicant and biodata.address.")
        applicant_payloads = retry_operation(
            "Load applicant details",
            self.applicant_db_store.load_applicant_details,
            application_ids,
            self.config.reduce_kafka_size,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"biodata.applicant and biodata.address query completed. Record found: "
            f"{application_id in applicant_payloads}."
        )

        self.logger.info("Loading application documents from tsp.text_similarity_input and tsp.titan_matches.")
        document_payload_records = retry_operation(
            "Load application documents",
            self.content_db_store.load_document_payload_records,
            application_ids,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        application_documents = document_payload_records.get(application_id, [])
        self.logger.info(
            f"Loaded {len(application_documents)} application document(s) from "
            "tsp.text_similarity_input and tsp.titan_matches."
        )

        self.logger.info("Loading Titan sentence matches from tsp.sentences_vector and tsp.titan_matches.")
        sentence_payload_records = retry_operation(
            "Load Titan sentence matches",
            self.content_db_store.load_sentence_payload_records,
            application_ids,
            self.config.reduce_kafka_size,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        application_sentences = sentence_payload_records.get(application_id, [])
        self.logger.info(
            f"Loaded {len(application_sentences)} Titan sentence record(s) from "
            "tsp.sentences_vector and tsp.titan_matches."
        )

        public_template_payloads = self._load_asylee_public_template(application_reference)

        application_payload_record = application_payload_records.get(application_id)
        if application_payload_record is None:
            raise PermanentDataError(f"Application record is missing for application_id={application_id}")
        application_events = self.payload_builder.build_application_events(
            application_payload_record,
            applicant_payloads.get(application_id),
            document_payload_records.get(application_id, []),
            sentence_payload_records.get(application_id, []),
            public_template_payloads.get(application_id),
        )
        self.logger.info(f"Built {len(application_events)} Kafka event(s).")
        return self._publish_events_in_size_limited_batches(application_events)

    def _load_asylee_public_template(self, application_reference: ApplicationReference) -> dict[int, dict]:
        """Load public-template data only when the application is Asylee.

        Input: application_reference with application_id=100 and analysis_group="Asylee"
        Output: {100: {...}} when a template exists, otherwise {}.

        Refugee applications return an empty dictionary without querying the
        public-template tables.
        """

        if application_reference.analysis_group != "Asylee":
            return {}

        application_id = application_reference.application_id
        self.logger.info(
            "Loading the public template from tsp.text_similarity_input and landing.template_registry."
        )
        public_template_payloads = retry_operation(
            "Load public templates",
            self.content_db_store.load_public_template_payload_records,
            [application_id],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            "tsp.text_similarity_input and landing.template_registry query completed. "
            f"Record found: {application_id in public_template_payloads}."
        )
        return public_template_payloads

    def _publish_events_in_size_limited_batches(self, application_events: list[dict]) -> int:
        """Publish before buffered event bytes approach PostgreSQL and worker limits.

        Input: application events produced by PayloadBuilder.
        Output: the total number of events confirmed by Kafka. Events are sent
        before their combined size passes the configured 450 MB safety limit.
        """

        published_event_count = 0
        buffered_event_size = 0
        buffered_application_events = []
        for application_event in application_events:
            event_size = self.payload_builder.calculate_serialized_size(application_event)
            if buffered_application_events and (buffered_event_size + event_size > constants.KAFKA_BUFFER_LIMIT_BYTES):
                published_event_count += retry_operation(
                    "Publish API events to Kafka",
                    self.kafka_processor.publish_application_events,
                    buffered_application_events,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                buffered_application_events = []
                buffered_event_size = 0
            buffered_application_events.append(application_event)
            buffered_event_size += event_size
        if buffered_application_events:
            published_event_count += retry_operation(
                "Publish API events to Kafka",
                self.kafka_processor.publish_application_events,
                buffered_application_events,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        return published_event_count
