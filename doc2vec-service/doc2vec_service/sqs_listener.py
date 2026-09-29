import json
import logging
import time

from botocore.exceptions import BotoCoreError, ClientError

from doc2vec_service.errors import PermanentMessageError

logger = logging.getLogger(__name__)


class SqsListener:
    def __init__(
        self,
        config,
        sqsClient,
        trainHandler,
        tracerHandler,
        historicalHandler,
        liveHandler,
    ):
        """Keep the queue and the four easy-to-follow reason handlers."""

        self.config = config
        self.sqsClient = sqsClient
        self.trainHandler = trainHandler
        self.tracerHandler = tracerHandler
        self.historicalHandler = historicalHandler
        self.liveHandler = liveHandler

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
                logger.warning("SQS receive failed: %s", str(error)[:500])
                time.sleep(self.config.retrySeconds)
                continue

            for rawMessage in response.get("Messages", []):
                self._processMessage(rawMessage)

    def _processMessage(self, rawMessage: dict):
        """Parse, dispatch, and delete one successful SQS message."""

        try:
            message = self._parseMessage(rawMessage["Body"])
            self._dispatch(message)
        except PermanentMessageError as error:
            logger.error("Invalid SQS message: %s", str(error)[:500])
            return
        except Exception:
            logger.exception("Doc2Vec message failed and was not deleted")
            return

        try:
            self.sqsClient.delete_message(
                QueueUrl=self.config.sourceQueueUrl,
                ReceiptHandle=rawMessage["ReceiptHandle"],
            )
        except (BotoCoreError, ClientError) as error:
            logger.warning("SQS delete failed: %s", str(error)[:500])

    def _parseMessage(self, body: str) -> dict:
        """Read the optional reason and tsp_id from a JSON body."""

        try:
            message = json.loads(body)
        except json.JSONDecodeError as error:
            raise PermanentMessageError("SQS body must be valid JSON") from error
        if not isinstance(message, dict):
            raise PermanentMessageError("SQS body must be a JSON object")

        reason = str(message.get("reason", "LIVE")).strip().upper()
        if reason == "TRAIN_MODEL":
            reason = "TRAIN"

        rawTspId = message.get("tsp_id", message.get("tspId"))
        tspId = None
        if rawTspId is not None:
            try:
                tspId = int(rawTspId)
            except (TypeError, ValueError) as error:
                raise PermanentMessageError("tsp_id must be a number") from error
        if reason != "TRACER" and tspId is None:
            raise PermanentMessageError("tsp_id is required")

        return {
            "reason": reason,
            "tspId": tspId,
        }

    def _dispatch(self, message: dict):
        """Send the message to the handler matching its reason."""

        if message["reason"] == "TRACER":
            return self.tracerHandler.process()
        if message["reason"] == "TRAIN":
            return self.trainHandler.process(message["tspId"])
        if message["reason"] == "HISTORICAL":
            return self.historicalHandler.process(message["tspId"])
        return self.liveHandler.process(message["tspId"])
