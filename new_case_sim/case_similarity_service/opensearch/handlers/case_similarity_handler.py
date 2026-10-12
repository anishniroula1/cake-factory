from case_similarity_service.opensearch.config import Config
from case_similarity_service.opensearch.contracts import ProcessResult, TspId
from case_similarity_service.opensearch.db_store.case_similarity_db_store import CaseSimilarityDBStore
from case_similarity_service.opensearch.errors import QueueSendError
from case_similarity_service.opensearch.kafka_processor import KafkaProcessor
from case_similarity_service.opensearch.retry import retry_operation
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class CaseSimilarityHandler:
    def __init__(
        self,
        config: Config,
        case_similarity_db_store: CaseSimilarityDBStore,
        kafka_processor: KafkaProcessor,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Connect the database, Kafka, and SQS steps used for one tsp_id."""

        self.config = config
        self.case_similarity_db_store = case_similarity_db_store
        self.kafka_processor = kafka_processor
        self.aws_sqs = aws_sqs
        self.logger = custom_logger

    def process(self, tsp_id: TspId) -> ProcessResult:
        """Process the case connected to one tsp_id.

        Input: tsp_id=100 from the SQS message.
        Output: counts showing how many application summaries, similar cases,
        TAH cases, and Kafka events were handled.

        The database function refreshes the source application and any connected
        applications. Kafka receives only TAH values that are new or changed.
        """

        # First find the application and analysis group that own this document.
        context = retry_operation(
            "Load Case Similarity document values",
            self.case_similarity_db_store.load_document_context,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.max_attempts,
            custom_logger=self.logger,
        )

        # Recalculate all case-level lists made from the Titan sentence matches.
        summaries = retry_operation(
            "Refresh Titan case summaries",
            self.case_similarity_db_store.refresh_case_summaries,
            context.application_id,
            context.analysis_group,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.max_attempts,
            custom_logger=self.logger,
        )

        # The returned list contains the source application and every connected
        # application changed by the bidirectional sentence relationships.
        affected_ids = [summary.application_id for summary in summaries]
        self.logger.info(f"Refreshed Titan case summaries for {len(affected_ids)} affected application(s).")

        # Use the source row for the counts returned by this message. Other rows
        # belong to applications that were refreshed because they match the source.
        source_summary = next(summary for summary in summaries if summary.application_id == context.application_id)
        kafka_event_count = self.kafka_processor.publish_changed_tah_events(affected_ids)
        self._send_next_message(tsp_id)

        self.logger.info(
            f"Case Similarity completed {len(summaries)} application summary rebuild(s), "
            f"{source_summary.similar_case_count} similar case(s), {source_summary.tah_case_count} TAH case(s), "
            f"and {kafka_event_count} Kafka event(s)."
        )
        return {
            "tsp_id": tsp_id,
            "source_application_id": context.application_id,
            "rebuilt_application_count": len(summaries),
            "similar_case_count": source_summary.similar_case_count,
            "tah_case_count": source_summary.tah_case_count,
            "kafka_event_count": kafka_event_count,
        }

    def _send_next_message(self, tsp_id: TspId) -> None:
        """Send the processed tsp_id to the configured next SQS queue.

        Input: tsp_id=100.
        Message: {"source": "P2", "tsp_id": "100", "sent_by": "case-similarity"}.
        """

        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.sent_sqs_endpoint,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "case-similarity"},
            wait_time=self.config.retry_seconds,
            retries=self.config.max_attempts - 1,
        )
        if not sent:
            raise QueueSendError("The next-step SQS message could not be sent")
