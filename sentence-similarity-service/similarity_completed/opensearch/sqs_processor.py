from similarity_completed.opensearch.contracts import SqsMessage, TspId
from similarity_completed.opensearch.dependencies import Dependencies
from similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError


class SqsProcessor:
    """Receive document messages and coordinate their sentence-similarity processing."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Continuously receive and process one SQS document message at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, message: SqsMessage) -> None:
        """Process a document message and delete it after successful completion.
        Failures are logged without deleting the message so SQS can redeliver it
        and eventually route it to the DLQ.

        Input: message={"tsp_id": 8205, "ReceiptHandle": "receipt-value"}
        """

        try:
            tsp_id = self._read_tsp_id(message)
            if tsp_id is None:
                self._leave_message_for_sqs("The SQS message is missing the required tsp_id.")
                return
            self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
            self.dependencies.logger.info("Starting an SQS document message.")
            result = self.dependencies.sentence_similarity_handler.process(tsp_id)
            if result is None:
                self._leave_message_for_sqs("Sentence Similarity did not complete the message.")
                return
        except (OperationFailedError, PermanentDataError, QueueSendError) as error:
            self.dependencies.logger.error(
                f"Sentence Similarity stopped because of {type(error).__name__}: {str(error)[:500]}"
            )
            return

        if not self.dependencies.aws_sqs.delete_messages(message):
            self.dependencies.logger.error("Processing completed, but source message deletion failed.")
            return
        self.dependencies.logger.info("Sentence Similarity completed and deleted the source SQS message.")

    def _leave_message_for_sqs(self, reason: str) -> None:
        """Log the reason a message remains available for redelivery.

        Input: reason="Catalog vector data is unavailable."
        """

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId | None:
        """Extract the document identifier from an SQS message.

        Input: message={"tsp_id": 8205}
        Output: 8205

        The original integer or string value is preserved. A missing or empty
        tsp_id returns None.
        """

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id
