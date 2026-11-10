from case_similarity_completed.opensearch.contracts import SqsMessage, TspId
from case_similarity_completed.opensearch.dependencies import Dependencies
from case_similarity_completed.opensearch.errors import CaseSimilarityError
from case_similarity_completed.opensearch.retry import safe_error_summary


class SqsProcessor:
    """Receive and process one Case Similarity document at a time."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Keep listening for SQS messages and process each message separately."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_case_similarity_message(message)

    def _process_case_similarity_message(self, message: SqsMessage) -> None:
        """Process one document and delete its source message after success.

        Input: message={"tsp_id": 100, "ReceiptHandle": "receipt-local"}

        A failed document is not deleted. SQS can retry it and send it to the
        configured DLQ after the receive limit is reached.
        """

        tsp_id = self._read_tsp_id_from_message(message)
        if tsp_id is None:
            self._leave_case_similarity_message_for_sqs(
                "The SQS message is missing the required tsp_id."
            )
            return

        self.dependencies.logger.info("Processing one Case Similarity SQS message.")
        try:
            self.dependencies.case_similarity_handler.process_case_similarity(tsp_id)
        except CaseSimilarityError as error:
            self.dependencies.logger.error(
                "Case Similarity processing stopped because of "
                f"{safe_error_summary(error)}"
            )
            return

        if self.dependencies.aws_sqs.delete_messages(message):
            self.dependencies.logger.info("Deleted the completed source SQS message.")
        else:
            self.dependencies.logger.error(
                "Processing completed, but the source SQS message deletion failed."
            )

    def _leave_case_similarity_message_for_sqs(self, reason: str) -> None:
        """Log why a source message remains available for SQS redelivery.

        Input: reason="The SQS message is missing the required tsp_id."
        """

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its "
            "configured retry and DLQ flow."
        )

    def _read_tsp_id_from_message(self, message: SqsMessage) -> TspId | None:
        """Return tsp_id without changing the type supplied by upstream.

        Input: message={"tsp_id": 100}
        Output: 100

        A missing or empty tsp_id returns None.
        """

        tsp_id = message.get("tsp_id")
        return None if tsp_id is None or tsp_id == "" else tsp_id
