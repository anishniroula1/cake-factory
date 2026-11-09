import time

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.contracts import SqsMessage, TspId
from case_similarity_completed.opensearch.dependencies import Dependencies
from case_similarity_completed.opensearch.errors import CaseSimilarityError
from case_similarity_completed.opensearch.retry import safe_error_summary


class SqsProcessor:
    """Receive document messages and coordinate their Case Similarity processing."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Continuously collect and process up to 100 SQS messages together."""

        while True:
            first_messages = self.dependencies.aws_sqs.listen_sqs_message()
            if not first_messages:
                continue
            message_batch = self._collect_message_batch(first_messages)
            self._process_case_similarity_messages(message_batch)

    def _collect_message_batch(self, first_messages: list[SqsMessage]) -> list[SqsMessage]:
        """Collect visible messages for 15 seconds, up to the 100-message limit.

        Input: the first SQS receive returns 10 messages.
        Output: up to 100 messages. A backlog of at least 100 is collected
        immediately; a smaller backlog gets 15 seconds to receive more work.
        """

        message_batch = list(first_messages)
        visible_message_count = self.dependencies.aws_sqs.get_approximate_message_count()
        if len(message_batch) + visible_message_count < constants.SQS_MESSAGE_BATCH_SIZE:
            time.sleep(constants.SQS_BATCH_WAIT_SECONDS)
            visible_message_count = self.dependencies.aws_sqs.get_approximate_message_count()

        # The shared SQS client receives at most ten messages per call. Stop
        # before a call could take this batch above 100; remaining messages stay
        # visible and will be collected by the next batch.
        while (
            visible_message_count > 0
            and len(message_batch) <= constants.SQS_MESSAGE_BATCH_SIZE - constants.SQS_RECEIVE_BATCH_SIZE
        ):
            received_messages = self.dependencies.aws_sqs.listen_sqs_message()
            if not received_messages:
                break
            message_batch.extend(received_messages)
            visible_message_count = max(0, visible_message_count - len(received_messages))

        self.dependencies.logger.info(
            f"Collected {len(message_batch)} Case Similarity SQS message(s) for processing."
        )
        return message_batch

    def _process_case_similarity_message(self, message: SqsMessage) -> None:
        """Process one message through the same path used by a collected batch."""

        self._process_case_similarity_messages([message])

    def _process_case_similarity_messages(self, messages: list[SqsMessage]) -> None:
        """Process a message batch and delete only individually successful work.

        Input: two messages for application 10 and one for application 20.
        Output: application 10 is refreshed once, application 20 is refreshed
        once, and each successful source message is deleted separately.
        """

        valid_messages: list[SqsMessage] = []
        tsp_ids: list[TspId] = []
        for message in messages:
            tsp_id = self._read_tsp_id_from_message(message)
            if tsp_id is None:
                self._leave_case_similarity_message_for_sqs(
                    "The SQS message is missing the required tsp_id."
                )
                continue
            valid_messages.append(message)
            tsp_ids.append(tsp_id)

        if not valid_messages:
            return

        try:
            self.dependencies.logger.set_generic_message(
                f"message_count={len(valid_messages)}: "
            )
            processing_results = self.dependencies.case_similarity_handler.process_case_similarity_batch(tsp_ids)
            deleted_message_count = 0
            for message, processing_succeeded in zip(valid_messages, processing_results, strict=True):
                if not processing_succeeded:
                    continue
                if self.dependencies.aws_sqs.delete_messages(message):
                    deleted_message_count += 1
                else:
                    self.dependencies.logger.error(
                        "Processing completed, but one source message deletion failed."
                    )
            self.dependencies.logger.info(
                f"Deleted {deleted_message_count} of {len(valid_messages)} source SQS message(s)."
            )
        except CaseSimilarityError as error:
            self.dependencies.logger.error(
                "Case Similarity batch stopped because of "
                f"{safe_error_summary(error)}"
            )
        finally:
            self.dependencies.logger.set_generic_message(None)

    def _leave_case_similarity_message_for_sqs(self, reason: str) -> None:
        """Log why a source message remains available for SQS redelivery.

        Input: reason="The document context is unavailable."
        """

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    def _read_tsp_id_from_message(self, message: SqsMessage) -> TspId | None:
        """Return tsp_id without changing the type supplied by upstream.

        Input: message={"tsp_id": 100}
        Output: 100

        A missing or empty tsp_id returns None.
        """

        tsp_id = message.get("tsp_id")
        return None if tsp_id is None or tsp_id == "" else tsp_id
