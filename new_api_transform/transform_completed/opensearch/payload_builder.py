import json
import math
import re

from transform_completed.opensearch import constants
from transform_completed.opensearch.config import Config
from transform_completed.opensearch.errors import PermanentDataError


class PayloadBuilder:
    """Build the existing API Kafka contract without adding the NER section."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def build_application_events(
        self,
        application_record: dict,
        applicant_details: dict | None,
        documents: list[dict],
        sentences: list[dict],
        public_template: dict | None,
        historical: bool = False,
    ) -> list[dict]:
        """Build one full event or sentence chunks when the payload is large.

        Input: one application record plus its related sections.
        Output: normally one create event; an oversized sentence list returns one
        create event followed by update events. NER is not part of this flow.
        """

        application_id = int(application_record["application_id"])
        a_number = int(application_record["a_number"])
        analysis_group = str(application_record["analysis_group"])
        application_event = {
            "pipeline": analysis_group,
            "a_number": a_number,
            "application_id": application_id,
            "analysis_group": analysis_group,
            "historical": historical,
            "payload": {
                "a_records": self._prepare_application_payload_fields(application_record),
                "documents": documents,
                "applicant_details": applicant_details,
                "sentences": sentences,
            },
        }
        if analysis_group == "Asylee":
            application_event["payload"]["public_templates"] = public_template

        complete_application_event = {"after": application_event, "op": "c"}
        maximum_event_size = (
            constants.UNCOMPRESSED_MAX_EVENT_BYTES
            if self.config.kafka_compression == "none"
            else constants.COMPRESSED_MAX_EVENT_BYTES
        )
        if self.calculate_serialized_size(complete_application_event) < maximum_event_size:
            return [complete_application_event]
        return self._split_large_sentence_payload(complete_application_event, maximum_event_size)

    def _prepare_application_payload_fields(self, application_record: dict) -> dict:
        """Remove repeated identifiers and normalize a Refugee receipt number.

        Input: a record containing application_id, a_number, analysis_group, and receipt_number="ABC123".
        Output: the remaining API fields with receipt_number="ABC-00000123" for a Refugee record.
        """

        application_payload_fields = dict(application_record)
        if application_payload_fields.get("analysis_group") == "Refugee" and application_payload_fields.get(
            "receipt_number"
        ):
            application_payload_fields["receipt_number"] = self._normalize_receipt_number(
                str(application_payload_fields["receipt_number"])
            )
        for name in ("application_id", "a_number", "analysis_group"):
            application_payload_fields.pop(name, None)
        return application_payload_fields

    def _split_large_sentence_payload(self, complete_application_event: dict, maximum_event_size: int) -> list[dict]:
        """Split only sentences and keep application metadata in the first event.

        Input: one oversized event and its maximum allowed byte size.
        Output: a create event for the first sentence chunk followed by update
        events for later chunks, matching the current consumer contract.
        """

        sentence_payloads = complete_application_event["after"]["payload"].get("sentences", [])
        if not sentence_payloads:
            raise PermanentDataError("An oversized API event has no sentences available for splitting")
        chunk_count = max(1, math.ceil(self.calculate_serialized_size(complete_application_event) / maximum_event_size))
        chunk_size = max(1, math.ceil(len(sentence_payloads) / chunk_count))
        sentence_chunks = [
            sentence_payloads[index : index + chunk_size] for index in range(0, len(sentence_payloads), chunk_size)
        ]

        split_application_events = []
        for index, sentence_chunk in enumerate(sentence_chunks):
            if index == 0:
                first_application_event = json.loads(json.dumps(complete_application_event, default=str))
                first_application_event["after"]["payload"]["sentences"] = sentence_chunk
                split_application_events.append(first_application_event)
                continue
            split_application_events.append(
                {
                    "after": {
                        **{
                            key: value for key, value in complete_application_event["after"].items() if key != "payload"
                        },
                        "payload": {
                            "a_records": complete_application_event["after"]["payload"]["a_records"],
                            "sentences": sentence_chunk,
                        },
                    },
                    "op": "u",
                }
            )
        return split_application_events

    @staticmethod
    def calculate_serialized_size(application_event: dict) -> int:
        """Return the UTF-8 byte size Kafka receives.

        Input: event={"op": "c"}
        Output: the number of bytes in its JSON representation.
        """

        return len(json.dumps(application_event, default=str).encode("utf-8"))

    @staticmethod
    def _normalize_receipt_number(receipt_number: str) -> str:
        """Format a Refugee receipt as PREFIX-########.

        Input: receipt_number="ABC123"
        Output: "ABC-00000123"
        """

        if "-" in receipt_number:
            prefix, number = receipt_number.split("-", 1)
        else:
            first_digit = re.search(r"\d", receipt_number)
            if first_digit is None:
                return receipt_number
            prefix = receipt_number[: first_digit.start()]
            number = receipt_number[first_digit.start() :]
        return f"{prefix}-{number.zfill(8)}"
