from doc2vec_completed.opensearch.contracts import SqsMessage, TspId
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.errors import CriticalProcessingError


class SqsProcessor:
    def __init__(self, dependencies: Dependencies) -> None:
        """Save the service objects used for every SQS message."""

        self.dependencies = dependencies

    def run(self) -> None:
        """Keep reading SQS and process each message one at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, raw_message: SqsMessage) -> None:
        """Process one document message and delete it only after success."""

        if raw_message.get("reason") != "HISTORICAL":
            self.dependencies.logger.error(
                "Cannot process the SQS message because its reason is missing "
                "or not supported."
            )
            return

        tsp_id = self._read_tsp_id(raw_message)
        if tsp_id is None:
            self._leave_message_for_sqs(
                "The SQS message is missing the required tsp_id."
            )
            return

        self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
        try:
            self.dependencies.logger.info("Starting an SQS document message.")
            result = self.dependencies.historical_handler.process(tsp_id)
            if result is None:
                self._leave_message_for_sqs(
                    "Sentence processing did not complete."
                )
                return
        except CriticalProcessingError as error:
            self.dependencies.logger.error(
                f"Sentence processing stopped because of "
                f"{type(error).__name__}: {str(error)[:500]}"
            )
            return

        if not self.dependencies.aws_sqs.delete_messages(raw_message):
            self.dependencies.logger.error(
                "Processing completed, but source message deletion failed."
            )
            return
        self.dependencies.logger.info(
            "Processing completed and deleted the source SQS message."
        )

    def _leave_message_for_sqs(self, reason: str) -> None:
        """Log why the message stays in SQS for another delivery."""

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its "
            "configured retry and DLQ flow."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId | None:
        """Return the message's tsp_id, or None when it is missing or empty."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id
