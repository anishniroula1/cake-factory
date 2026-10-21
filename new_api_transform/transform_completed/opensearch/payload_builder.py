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
        event = {
            "pipeline": analysis_group,
            "a_number": a_number,
            "application_id": application_id,
            "analysis_group": analysis_group,
            "historical": historical,
            "payload": {
                "a_records": self._clean_application_record(application_record),
                "documents": documents,
                "applicant_details": applicant_details,
                "sentences": sentences,
            },
        }
        if analysis_group == "Asylee":
            event["payload"]["public_templates"] = public_template

        full_event = {"after": event, "op": "c"}
        maximum_size = (
            constants.UNCOMPRESSED_MAX_EVENT_BYTES
            if self.config.kafka_compression == "none"
            else constants.COMPRESSED_MAX_EVENT_BYTES
        )
        if self.serialized_size(full_event) < maximum_size:
            return [full_event]
        return self._split_sentences(full_event, maximum_size)

    def _clean_application_record(self, record: dict) -> dict:
        """Remove repeated identifiers and normalize a Refugee receipt number.

        Input: a record containing application_id, a_number, analysis_group, and receipt_number="ABC123".
        Output: the remaining API fields with receipt_number="ABC-00000123" for a Refugee record.
        """

        cleaned = dict(record)
        if cleaned.get("analysis_group") == "Refugee" and cleaned.get("receipt_number"):
            cleaned["receipt_number"] = self._normalize_receipt_number(str(cleaned["receipt_number"]))
        for name in ("application_id", "a_number", "analysis_group"):
            cleaned.pop(name, None)
        return cleaned

    def _split_sentences(self, full_event: dict, maximum_size: int) -> list[dict]:
        """Split only sentences and keep application metadata in the first event.

        Input: one oversized event and its maximum allowed byte size.
        Output: a create event for the first sentence chunk followed by update
        events for later chunks, matching the current consumer contract.
        """

        sentences = full_event["after"]["payload"].get("sentences", [])
        if not sentences:
            raise PermanentDataError("An oversized API event has no sentences available for splitting")
        chunk_count = max(1, math.ceil(self.serialized_size(full_event) / maximum_size))
        chunk_size = max(1, math.ceil(len(sentences) / chunk_count))
        chunks = [sentences[index : index + chunk_size] for index in range(0, len(sentences), chunk_size)]

        split_events = []
        for index, sentence_chunk in enumerate(chunks):
            if index == 0:
                first_event = json.loads(json.dumps(full_event, default=str))
                first_event["after"]["payload"]["sentences"] = sentence_chunk
                split_events.append(first_event)
                continue
            split_events.append(
                {
                    "after": {
                        **{key: value for key, value in full_event["after"].items() if key != "payload"},
                        "payload": {
                            "a_records": full_event["after"]["payload"]["a_records"],
                            "sentences": sentence_chunk,
                        },
                    },
                    "op": "u",
                }
            )
        return split_events

    @staticmethod
    def serialized_size(event: dict) -> int:
        """Return the UTF-8 byte size Kafka receives.

        Input: event={"op": "c"}
        Output: the number of bytes in its JSON representation.
        """

        return len(json.dumps(event, default=str).encode("utf-8"))

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
