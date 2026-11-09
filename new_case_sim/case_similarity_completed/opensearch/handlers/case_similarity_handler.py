from time import perf_counter

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.config import Config
from case_similarity_completed.opensearch.contracts import DocumentContext, ProcessResult, TspId
from case_similarity_completed.opensearch.db_store.case_similarity_db_store import CaseSimilarityDBStore
from case_similarity_completed.opensearch.errors import CaseSimilarityError
from case_similarity_completed.opensearch.kafka_processor import KafkaProcessor
from case_similarity_completed.opensearch.retry import retry_operation, safe_error_summary
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

    def process_case_similarity_batch(self, tsp_ids: list[TspId]) -> list[bool]:
        """Refresh unique applications and forward every successful document.

        Input: tsp_ids=[100, 101, 200], where 100 and 101 belong to application
        10 and 200 belongs to application 20.
        Output: [True, True, True] when both applications were refreshed and all
        three document messages were accepted by the next SQS queue.

        The output position matches the input position. A False value leaves
        only that source SQS message available for retry.
        """

        if not tsp_ids:
            return []

        document_contexts = retry_operation(
            "Load Case Similarity document values",
            self.case_similarity_db_store.load_case_similarity_document_contexts,
            tsp_ids,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.MAX_ATTEMPTS,
            custom_logger=self.logger,
        )
        context_by_tsp_id = {str(context.tsp_id): context for context in document_contexts}
        positions_by_application: dict[tuple[int, str], list[int]] = {}
        context_by_application: dict[tuple[int, str], DocumentContext] = {}

        for position, tsp_id in enumerate(tsp_ids):
            document_context = context_by_tsp_id.get(str(tsp_id))
            if document_context is None:
                self.logger.error(
                    f"No text_similarity_input row exists for tsp_id={tsp_id}."
                )
                continue
            application_key = (
                document_context.application_id,
                document_context.analysis_group,
            )
            positions_by_application.setdefault(application_key, []).append(position)
            context_by_application[application_key] = document_context

        unique_application_count = len({application_id for application_id, _ in positions_by_application})
        self.logger.info(
            f"Collected {len(tsp_ids)} SQS message(s) representing "
            f"{unique_application_count} unique application_id(s)."
        )

        positions_ready_for_next_queue: list[int] = []
        for application_key, message_positions in positions_by_application.items():
            document_context = context_by_application[application_key]
            try:
                self._refresh_case_similarity_application(document_context)
                positions_ready_for_next_queue.extend(message_positions)
            except CaseSimilarityError as error:
                self.logger.error(
                    "Case Similarity could not complete "
                    f"application_id={document_context.application_id}: "
                    f"{safe_error_summary(error)}"
                )

        processing_results = self._send_case_similarity_results_to_next_queue(
            tsp_ids,
            positions_ready_for_next_queue,
        )
        self.logger.info(
            f"Case Similarity batch completed {sum(processing_results)} of "
            f"{len(tsp_ids)} source message(s)."
        )
        return processing_results

    def _refresh_case_similarity_application(self, document_context: DocumentContext) -> ProcessResult:
        """Refresh one unique application and publish its changed TAH values.

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

    def _send_case_similarity_results_to_next_queue(
        self,
        tsp_ids: list[TspId],
        message_positions: list[int],
    ) -> list[bool]:
        """Send one downstream message for every successfully refreshed document.

        Input: tsp_ids=[100, 101, 200], message_positions=[0, 1]
        Output: [True, True, False] when documents 100 and 101 are accepted.

        Messages use the SQS batch API in groups of ten, but each tsp_id remains
        a separate message for API Transform.
        """

        processing_results = [False] * len(tsp_ids)
        for offset in range(0, len(message_positions), constants.SQS_SEND_BATCH_SIZE):
            batch_positions = message_positions[offset : offset + constants.SQS_SEND_BATCH_SIZE]
            message_batch = [
                {
                    "source": "P2",
                    "tsp_id": str(tsp_ids[position]),
                    "sent_by": "case-similarity",
                }
                for position in batch_positions
            ]
            send_result = self.aws_sqs.send_messages_in_batch(
                queue_url=self.config.sent_sqs_endpoint,
                batch=message_batch,
                is_fifo=self.config.sent_sqs_endpoint.endswith(".fifo"),
                message_group_id=None,
                wait_time=constants.RETRY_SECONDS,
                retries=constants.MAX_ATTEMPTS - 1,
            )
            if send_result.get("status") != "SUCCEEDED":
                self.logger.error(
                    f"API Transform SQS send failed for {len(batch_positions)} document message(s)."
                )
                continue
            for position in batch_positions:
                processing_results[position] = True

        return processing_results
