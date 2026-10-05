from doc2vec_completed.opensearch.contracts import SqsMessage, TspId
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.errors import PermanentMessageError


class SqsProcessor:
    def __init__(self, dependencies: Dependencies) -> None:
        """Keep the dependencies used to process one SQS message."""

        self.dependencies = dependencies

    def run(self) -> None:
        """Read SQS and process one message at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, raw_message: SqsMessage) -> None:
        """Validate, process, and delete one successful SQS message."""

        if raw_message.get("reason") != "HISTORICAL":
            self.dependencies.logger.warning(
                "Cannot process the SQS message without reason=HISTORICAL. "
                "Please provide the reason for your message."
            )
            return

        try:
            tsp_id = self._read_tsp_id(raw_message)
            message_id = raw_message.get("MessageId")
            self.dependencies.logger.info(
                f"Starting historical SQS message {message_id}."
            )
            self.dependencies.historical_handler.process(tsp_id)
        except PermanentMessageError as error:
            self.dependencies.logger.error(
                f"Invalid historical SQS message: {str(error)[:500]}"
            )
            return
        # The SQS boundary keeps any failed message on the queue for retry.
        except Exception as error:  # noqa: BLE001
            self.dependencies.logger.error(
                "Historical SQS message failed and was not deleted. "
                f"Cause: {type(error).__name__}: {str(error)[:500]}"
            )
            return

        self.dependencies.logger.info(
            f"Historical SQS message {message_id} finished processing. "
            "Deleting it from the source queue."
        )
        if not self.dependencies.aws_sqs.delete_messages(raw_message):
            self.dependencies.logger.warning(
                f"SQS message_id={raw_message.get('MessageId')} was not deleted"
            )
            return
        self.dependencies.logger.info(
            f"Historical SQS message {message_id} completed and was deleted."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId:
        """Return tsp_id exactly as supplied, including a string value."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None:
            raise PermanentMessageError("the required document identifier is missing")
        return tsp_id
