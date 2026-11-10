from time import perf_counter

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.config import Config
from case_similarity_completed.opensearch.contracts import DocumentContext, ProcessResult, TspId
from case_similarity_completed.opensearch.db_store.case_similarity_db_store import CaseSimilarityDBStore
from case_similarity_completed.opensearch.errors import QueueSendError
from case_similarity_completed.opensearch.kafka_processor import KafkaProcessor
from case_similarity_completed.opensearch.retry import retry_operation
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class CaseSimilarityHandler:
    """Coordinate summary rebuilding, TAH publishing, and downstream SQS delivery."""

    def __init__(
        self,
        config: Config,
        case_similarity_db_store: CaseSimilarityDBStore,
        kafka_processor: KafkaProcessor,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.case_similarity_db_store = case_similarity_db_store
        self.kafka_processor = kafka_processor
        self.aws_sqs = aws_sqs
        self.logger = custom_logger

    def process_case_similarity(self, tsp_id: TspId) -> ProcessResult:
        """Process one document through Case Similarity.

        Input: tsp_id=100
        Output: the source application's similar-case, TAH, and Kafka counts.

        The next SQS message is sent only after the database and optional Kafka
        work completes.
        """

        document_context = retry_operation(
            "Load Case Similarity document values",
            self.case_similarity_db_store.load_case_similarity_document_context,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.MAX_ATTEMPTS,
            custom_logger=self.logger,
        )
        process_result = self._refresh_case_similarity_application(document_context)
        self._send_case_similarity_result_to_next_queue(tsp_id)
        return process_result

    def _refresh_case_similarity_application(self, document_context: DocumentContext) -> ProcessResult:
        """Refresh one application and publish its changed TAH values.

        Input: DocumentContext(tsp_id=100, application_id=20,
        analysis_group="Asylee")
        Output: the refreshed source counts and Kafka event count for application 20.
        """

        started_at = perf_counter()
        application_id = document_context.application_id
        analysis_group = document_context.analysis_group
        self.logger.info(
            "Loaded the document context from text_similarity_input. "
            f"application_id={application_id}, analysis_group={analysis_group}."
        )

        # Rebuild the source lists and mirror this one relationship on targets.
        summary_refresh_started_at = perf_counter()
        refreshed_case_summaries = retry_operation(
            "Refresh Titan case summaries",
            self.case_similarity_db_store.refresh_bidirectional_case_summaries,
            application_id,
            analysis_group,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.MAX_ATTEMPTS,
            custom_logger=self.logger,
        )
        self.logger.info(
            "Titan case summary refresh completed in "
            f"{perf_counter() - summary_refresh_started_at:.2f} seconds."
        )

        # The returned list contains the rebuilt source and target rows where
        # only the source application's membership was changed.
        affected_application_ids = [case_summary.application_id for case_summary in refreshed_case_summaries]
        self.logger.info(f"Updated Titan case summaries for {len(affected_application_ids)} affected application(s).")

        # Use the source row for the counts returned by this message. Other rows
        # belong to applications where the source relationship was mirrored.
        source_case_summary = next(
            case_summary
            for case_summary in refreshed_case_summaries
            if case_summary.application_id == application_id
        )
        kafka_event_count = self.kafka_processor.publish_changed_tah_events(affected_application_ids)

        self.logger.info(
            f"Case Similarity completed {len(refreshed_case_summaries)} application summary update(s), "
            f"{source_case_summary.similar_case_count} similar case(s), "
            f"{source_case_summary.tah_case_count} TAH case(s), "
            f"and {kafka_event_count} Kafka event(s)."
        )
        self.logger.info(f"Case Similarity took {perf_counter() - started_at:.2f} seconds to complete.")
        return {
            "tsp_id": document_context.tsp_id,
            "source_application_id": application_id,
            "rebuilt_application_count": len(refreshed_case_summaries),
            "similar_case_count": source_case_summary.similar_case_count,
            "tah_case_count": source_case_summary.tah_case_count,
            "kafka_event_count": kafka_event_count,
        }

    def _send_case_similarity_result_to_next_queue(self, tsp_id: TspId) -> None:
        """Send one completed document to API Transform.

        Input: tsp_id=100

        Raises:
            QueueSendError: When SQS does not accept the message after retries.
        """

        message_sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.sent_sqs_endpoint,
            message_body={
                "source": "P2",
                "tsp_id": str(tsp_id),
                "sent_by": "case-similarity",
            },
            wait_time=constants.RETRY_SECONDS,
            retries=constants.MAX_ATTEMPTS - 1,
            is_fifo=self.config.sent_sqs_endpoint.endswith(".fifo"),
        )
        if not message_sent:
            raise QueueSendError("API Transform SQS send failed")
