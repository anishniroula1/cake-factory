from case_similarity_completed.opensearch.contracts import SqsMessage, TspId
from case_similarity_completed.opensearch.dependencies import Dependencies
from case_similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError


class SqsProcessor:
    """Receive document messages and coordinate their Case Similarity processing."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Continuously receive and process one SQS document message at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, message: SqsMessage) -> None:
        """Process one document and delete its message only after completion.
        Failures remain available for the configured SQS retry and DLQ policy.

        Input: message={"tsp_id": 100, "ReceiptHandle": "receipt-value"}
        """

        try:
            tsp_id = self._read_tsp_id(message)
            if tsp_id is None:
                self._leave_message_for_sqs("The SQS message is missing the required tsp_id.")
                return
            self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
            self.dependencies.logger.info("Starting a Case Similarity document message.")
            result = self.dependencies.case_similarity_handler.process(tsp_id)
            if result is None:
                self._leave_message_for_sqs("Case Similarity did not complete the message.")
                return
        except (OperationFailedError, PermanentDataError, QueueSendError) as error:
            self.dependencies.logger.error(
                f"Case Similarity stopped because of {type(error).__name__}: {str(error)[:500]}"
            )
            return

        if not self.dependencies.aws_sqs.delete_messages(message):
            self.dependencies.logger.error("Processing completed, but source message deletion failed.")
            return
        self.dependencies.logger.info("Case Similarity completed and deleted the source SQS message.")

    def _leave_message_for_sqs(self, reason: str) -> None:
        """Log why a source message remains available for SQS redelivery.

        Input: reason="The document context is unavailable."
        """

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId | None:
        """Return tsp_id without changing the type supplied by upstream.

        Input: message={"tsp_id": 100}
        Output: 100

        A missing or empty tsp_id returns None.
        """

        tsp_id = message.get("tsp_id")
        return None if tsp_id is None or tsp_id == "" else tsp_id
