import json
import logging
import time

from botocore.exceptions import BotoCoreError, ClientError

from sentence_similarity.errors import PermanentMessageError

logger = logging.getLogger(__name__)


class SqsListener:
    def __init__(self, config, sqsClient, handler):
        """Keep the source queue and Sentence Similarity handler."""

        self.config = config
        self.sqsClient = sqsClient
        self.handler = handler

    def run(self):
        """Listen to SQS and process one tsp_id message at a time."""

        while True:
            try:
                response = self.sqsClient.receive_message(
                    QueueUrl=self.config.sourceQueueUrl,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=self.config.sqsWaitSeconds,
                    VisibilityTimeout=self.config.sqsVisibilitySeconds,
                )
            except (BotoCoreError, ClientError) as error:
                logger.warning("SQS receive failed: %s", str(error)[:500])
                time.sleep(self.config.retrySeconds)
                continue

            for rawMessage in response.get("Messages", []):
                self._processMessage(rawMessage)

    def _processMessage(self, rawMessage: dict):
        """Process and delete one successful SQS message."""

        try:
            tspId = self._tspId(rawMessage["Body"])
            self.handler.process(tspId)
        except PermanentMessageError as error:
            logger.error("Invalid SQS message: %s", str(error)[:500])
            return
        except Exception:
            logger.exception("Sentence Similarity failed and message was not deleted")
            return

        try:
            self.sqsClient.delete_message(
                QueueUrl=self.config.sourceQueueUrl,
                ReceiptHandle=rawMessage["ReceiptHandle"],
            )
        except (BotoCoreError, ClientError) as error:
            logger.warning("SQS delete failed: %s", str(error)[:500])

    def _tspId(self, body: str) -> int:
        """Read tsp_id from one JSON message body."""

        try:
            message = json.loads(body)
        except json.JSONDecodeError as error:
            raise PermanentMessageError("SQS body must be valid JSON") from error
        if not isinstance(message, dict):
            raise PermanentMessageError("SQS body must be a JSON object")
        value = message.get("tsp_id", message.get("tspId"))
        try:
            tspId = int(value)
        except (TypeError, ValueError) as error:
            raise PermanentMessageError("tsp_id must be a number") from error
        if tspId < 1:
            raise PermanentMessageError("tsp_id must be greater than zero")
        return tspId
