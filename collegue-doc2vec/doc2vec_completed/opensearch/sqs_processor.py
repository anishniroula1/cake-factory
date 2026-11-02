import json

from doc2vec_completed.opensearch.contracts import SqsMessage, TspId
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.errors import (
    CriticalProcessingError,
    PermanentMessageError,
)
from doc2vec_completed.opensearch.retry import safe_error_summary


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

        try:
            if raw_message.get("reason") == "HISTORICAL":
                tsp_ids = self._read_tsp_ids(raw_message)
                self.dependencies.logger.info(
                    f"Received {len(tsp_ids)} historical tsp_id(s) to process."
                )
                result = self.dependencies.historical_handler.process(tsp_ids)
            elif raw_message.get("reason") == "TRAIN_MODEL":
                tsp_id = self._read_tsp_id(raw_message)
                self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}: ")
                result = self.dependencies.train_model_handler.process(tsp_id)
            elif raw_message.get("reason") == "TRACER":
                tracer_id = self._read_tracer_id(raw_message)
                self.dependencies.logger.set_generic_message(
                    f"tracer_id={tracer_id}: "
                )
                result = self.dependencies.tracer_handler.process(tracer_id)
            else:
                tsp_id = self._read_tsp_id(raw_message)
                self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}: ")
                result = self.dependencies.live_handler.process(tsp_id)

            if result is None:
                flow_name = raw_message.get("reason") or "LIVE"
                self.dependencies.logger.error(
                    f"{flow_name} processing did not complete."
                )
                return

            if not self.dependencies.aws_sqs.delete_messages(raw_message):
                self.dependencies.logger.warning(
                    f"SQS message_id={raw_message.get('MessageId')} was not deleted"
                )
                return
            self.dependencies.logger.info(
                "Processing completed and deleted the source SQS message."
            )
        except PermanentMessageError as error:
            self.dependencies.logger.error(
                f"Invalid SQS message: {safe_error_summary(error)}"
            )
            return
        # Expected processing failures keep the message available for the queue
        # retry and DLQ policy. Unexpected code defects still stop the process.
        except CriticalProcessingError as error:
            self.dependencies.logger.error(
                f"SQS message processing failed: {safe_error_summary(error)}"
            )
            return
        finally:
            self.dependencies.logger.set_generic_message(None)

    @staticmethod
    def _read_tsp_id(message: SqsMessage) -> TspId:
        """Return tsp_id exactly as supplied, including a string value."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            raise PermanentMessageError("tsp_id is required")
        return tsp_id

    @classmethod
    def _read_tsp_ids(cls, message: SqsMessage) -> list[TspId]:
        """Return historical IDs as one list.

        Input: {"tsp_id": 1040} or {"tsp_ids": "[1040, 1041]"}
        Output: [1040, 1041]
        """

        raw_tsp_ids = message.get("tsp_ids")
        if raw_tsp_ids is None:
            return [cls._read_tsp_id(message)]

        try:
            tsp_ids = json.loads(raw_tsp_ids) if isinstance(raw_tsp_ids, str) else raw_tsp_ids
        except json.JSONDecodeError as error:
            raise PermanentMessageError("tsp_ids must be a valid JSON list") from error

        valid_ids = isinstance(tsp_ids, list) and tsp_ids and all(
            not isinstance(value, bool)
            and isinstance(value, (int, str))
            and (not isinstance(value, str) or bool(value.strip()))
            for value in tsp_ids
        )
        if not valid_ids:
            raise PermanentMessageError("tsp_ids must be a non-empty list of strings or numbers")
        return list(dict.fromkeys(tsp_ids))

    @staticmethod
    def _read_tracer_id(message: SqsMessage) -> int:
        """Convert the message tracer_sentence_id into the Tracer primary ID.

        Input: message={"tracer_sentence_id": "10"}
        Output: tracer_id=10
        """

        value = message.get("tracer_sentence_id")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value)
        raise PermanentMessageError("tracer_sentence_id must be a number")

