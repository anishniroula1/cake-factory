from similarity_completed.opensearch.contracts import SqsMessage, TspId
from similarity_completed.opensearch.dependencies import Dependencies
from similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError


class SqsProcessor:
    def __init__(self, dependencies: Dependencies) -> None:
        """Keep the dependencies used to process one SQS message."""

        self.dependencies = dependencies

    def run(self) -> None:
        """Read and process one message at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, message: SqsMessage) -> None:
        """Process and delete one message only after every step succeeds."""

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
        """Log the one standard message used for normal SQS retry handling."""

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId | None:
        """Return tsp_id exactly as supplied, or None when it is missing."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id
