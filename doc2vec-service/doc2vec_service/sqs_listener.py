import json
import time

from botocore.exceptions import BotoCoreError, ClientError

from doc2vec_service.errors import PermanentMessageError


class SqsListener:
    def __init__(
        self,
        config,
        sqsClient,
        historicalHandler,
        customLogger,
    ):
        """Keep the queue and the historical handler."""

        self.config = config
        self.sqsClient = sqsClient
        self.historicalHandler = historicalHandler
        self.logger = customLogger

    def run(self):
        """Listen to SQS and process one message at a time."""

        while True:
            try:
                response = self.sqsClient.receive_message(
                    QueueUrl=self.config.sourceQueueUrl,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=self.config.sqsWaitSeconds,
                    VisibilityTimeout=self.config.sqsVisibilitySeconds,
                )
            except (BotoCoreError, ClientError) as error:
                self.logger.warning(
                    f"SQS receive failed: {str(error)[:500]}"
                )
                time.sleep(self.config.retrySeconds)
                continue

            for rawMessage in response.get("Messages", []):
                self._processMessage(rawMessage)

    def _processMessage(self, rawMessage: dict):
        """Parse, dispatch, and delete one successful SQS message."""

        try:
            message = self._parseMessage(rawMessage["Body"])
            self.historicalHandler.process(message["tspId"])
        except PermanentMessageError as error:
            self.logger.error(f"Invalid SQS message: {str(error)[:500]}")
            return
        except Exception:
            self.logger.exception(
                "Doc2Vec message failed and was not deleted"
            )
            return

        try:
            self.sqsClient.delete_message(
                QueueUrl=self.config.sourceQueueUrl,
                ReceiptHandle=rawMessage["ReceiptHandle"],
            )
        except (BotoCoreError, ClientError) as error:
            self.logger.warning(f"SQS delete failed: {str(error)[:500]}")

    def _parseMessage(self, body: str) -> dict:
        """Read the required tsp_id from one historical JSON message."""

        try:
            message = json.loads(body)
        except json.JSONDecodeError as error:
            raise PermanentMessageError("SQS body must be valid JSON") from error
        if not isinstance(message, dict):
            raise PermanentMessageError("SQS body must be a JSON object")

        rawTspId = message.get("tsp_id", message.get("tspId"))
        if rawTspId is None:
            raise PermanentMessageError("tsp_id is required")
        try:
            tspId = int(rawTspId)
        except (TypeError, ValueError) as error:
            raise PermanentMessageError("tsp_id must be a number") from error

        return {"tspId": tspId}
