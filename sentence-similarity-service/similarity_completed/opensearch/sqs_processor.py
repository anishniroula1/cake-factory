from similarity_completed.opensearch.contracts import SqsMessage, TspId
from similarity_completed.opensearch.dependencies import Dependencies
from similarity_completed.opensearch.errors import PermanentMessageError


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
            self.dependencies.logger.info("Starting an SQS document message.")
            self.dependencies.sentence_similarity_handler.process(tsp_id)
        except PermanentMessageError as error:
            self.dependencies.logger.error(
                f"Invalid Sentence Similarity message: {str(error)[:500]}"
            )
            return
        except Exception as error:  # noqa: BLE001
            self.dependencies.logger.error(
                "Sentence Similarity failed and the message was not deleted. "
                f"Cause: {type(error).__name__}: {str(error)[:500]}"
            )
            return

        if not self.dependencies.aws_sqs.delete_messages(message):
            self.dependencies.logger.warning(
                "Processing completed, but the source SQS message was not deleted."
            )
            return
        self.dependencies.logger.info(
            "Sentence Similarity completed and deleted the source SQS message."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId:
        """Return tsp_id exactly as supplied, including a string value."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            raise PermanentMessageError("the required tsp_id is missing")
        return tsp_id
