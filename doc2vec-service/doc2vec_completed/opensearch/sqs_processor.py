from doc2vec_completed.opensearch.contracts import SqsMessage, TspId
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.errors import CriticalProcessingError


class SqsProcessor:
    """Route live and backfill messages to the matching Doc2Vec handler."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Continuously receive and process one SQS document message at a time."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_message(message)

    def _process_message(self, raw_message: SqsMessage) -> None:
        """Route one supported message and delete it only after the handler succeeds.

        Input: raw_message={"reason": "HISTORICAL", "tsp_id": 1040,
        "ReceiptHandle": "receipt-value"}
        """

        reason = str(raw_message.get("reason") or "").strip().upper()
        if reason and reason not in {"HISTORICAL", "TRAIN_MODEL", "TRACER"}:
            self.dependencies.logger.error(
                f"Cannot process the SQS message because reason={reason!r} is "
                "not supported."
            )
            return

        tsp_id = None
        tracer_sentence_ids = None
        if reason == "TRACER":
            tracer_sentence_ids = self._read_tracer_sentence_ids(raw_message)
            if tracer_sentence_ids is None:
                self._leave_message_for_sqs(
                    "The TRACER message requires tracer_sentence_ids."
                )
                return
            context = f"reason={reason}"
        else:
            tsp_id = self._read_tsp_id(raw_message)
            if tsp_id is None:
                self._leave_message_for_sqs(
                    f"The {reason} message is missing the required tsp_id."
                )
                return
            context = f"tsp_id={tsp_id}"

        self.dependencies.logger.set_generic_message(context)
        try:
            self.dependencies.logger.info("Starting an SQS document message.")
            if reason == "HISTORICAL":
                result = self.dependencies.historical_handler.process(tsp_id)
            elif reason == "TRAIN_MODEL":
                result = self.dependencies.train_model_handler.process(tsp_id)
            elif reason == "TRACER":
                result = self.dependencies.tracer_handler.process(tracer_sentence_ids)
            else:
                # The established live message contract has no reason field.
                result = self.dependencies.live_handler.process(tsp_id)
            if result is None:
                flow_name = reason or "LIVE"
                self._leave_message_for_sqs(f"{flow_name} processing did not complete.")
                return

            if not self.dependencies.aws_sqs.delete_messages(raw_message):
                self.dependencies.logger.error(
                    "Processing completed, but source message deletion failed."
                )
                return
            self.dependencies.logger.info(
                "Processing completed and deleted the source SQS message."
            )
        except CriticalProcessingError as error:
            self.dependencies.logger.error(
                f"Sentence processing stopped because of "
                f"{type(error).__name__}: {str(error)[:500]}"
            )
            return
        finally:
            self.dependencies.logger.set_generic_message(None)

    def _leave_message_for_sqs(self, reason: str) -> None:
        """Log the reason a message remains available for redelivery.

        Input: reason="Sentence processing did not complete."
        """

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its "
            "configured retry and DLQ flow."
        )

    def _read_tsp_id(self, message: SqsMessage) -> TspId | None:
        """Extract tsp_id without changing the type supplied by upstream.

        Input: message={"tsp_id": 1040}
        Output: 1040

        A missing or empty tsp_id returns None.
        """

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id

    @staticmethod
    def _read_tracer_sentence_ids(message: SqsMessage) -> list[int] | None:
        """Validate and deduplicate tracer_sentence_ids from a TRACER message.

        Input: message={"tracer_sentence_ids": [10, 20, 10]}
        Output: [10, 20]
        """

        values = message.get("tracer_sentence_ids")
        if not isinstance(values, list) or not values:
            return None
        if any(
            isinstance(value, bool) or not isinstance(value, int) for value in values
        ):
            return None
        return list(dict.fromkeys(values))
