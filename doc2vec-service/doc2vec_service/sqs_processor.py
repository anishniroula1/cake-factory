from doc2vec_service.errors import PermanentMessageError


class SqsProcessor:
    def __init__(self, dependencies):
        """Keep the dependencies used to process one SQS message."""

        self.dependencies = dependencies

    def run(self):
        """Read SQS and process one message at a time."""

        while True:
            for message in self.dependencies.awsSqs.listen_sqs_message():
                self._processMessage(message)

    def _processMessage(self, rawMessage: dict):
        """Validate, process, and delete one successful SQS message."""

        if rawMessage.get("reason") != "HISTORICAL":
            self.dependencies.logger.warning(
                "Cannot process the SQS message without reason=HISTORICAL. "
                "Please provide the reason for your message."
            )
            return

        try:
            tsp_id = self._read_tsp_id(rawMessage)
            self.dependencies.historicalHandler.process(tsp_id)
        except PermanentMessageError as error:
            self.dependencies.logger.error(
                f"Invalid SQS message: {str(error)[:500]}"
            )
            return
        except Exception:
            self.dependencies.logger.exception(
                "Doc2Vec message failed and was not deleted"
            )
            return

        if not self.dependencies.awsSqs.delete_messages(rawMessage):
            self.dependencies.logger.warning(
                f"SQS message_id={rawMessage.get('MessageId')} was not deleted"
            )

    def _read_tsp_id(self, message: dict):
        """Return tsp_id exactly as supplied, including a string value."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None:
            raise PermanentMessageError("tsp_id is required")
        return tsp_id
