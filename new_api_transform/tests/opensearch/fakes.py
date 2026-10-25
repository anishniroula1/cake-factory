from datetime import UTC, datetime

from transform_completed.opensearch.contracts import ApplicationReference
from transform_completed.opensearch.errors import OperationFailedError


class LoggerFake:
    def __init__(self) -> None:
        self.messages = []
        self.generic_message = None

    def debug(self, message):
        self.messages.append(("debug", message))

    def info(self, message):
        self.messages.append(("info", message))

    def warning(self, message):
        self.messages.append(("warning", message))

    def error(self, message):
        self.messages.append(("error", message))

    def set_generic_message(self, message):
        self.generic_message = message


class SqsFake:
    def __init__(self, receive_batches=None, send_status="SUCCEED", delete_status="SUCCEED") -> None:
        self.receive_batches = list(receive_batches or [])
        self.send_status = send_status
        self.delete_status = delete_status
        self.sent = []
        self.deleted = []

    def listen_sqs_message(self):
        return self.receive_batches.pop(0) if self.receive_batches else []

    def send_sqs_message(self, send_queue_url, message_body):
        self.sent.append((send_queue_url, message_body))
        return self.send_status == "SUCCEED"

    def delete_messages(self, message):
        self.deleted.append(message)
        return self.delete_status == "SUCCEED"


class HandlerFake:
    def __init__(self, fail=False) -> None:
        self.calls = []
        self.historical_values = []
        self.fail = fail

    def process_document(self, tsp_id, historical=False):
        self.calls.append(tsp_id)
        self.historical_values.append(historical)
        if self.fail:
            raise OperationFailedError("database unavailable")
        return {"source_message_count": 1, "application_count": 1, "kafka_event_count": 1}


class DependenciesFake:
    def __init__(self, config, source, sent, handler, logger, dlq=None) -> None:
        self.config = config
        self.aws_sqs = source
        self.dlq_sqs = dlq
        self.sent_sqs = sent
        self.api_transform_handler = handler
        self.logger = logger


class ApplicationStoreFake:
    def load_application_reference(self, tsp_id):
        return ApplicationReference(100, 900001, "Asylee")

    def load_application_payload_records(self, application_references):
        return {
            100: {
                "application_id": 100,
                "a_number": 900001,
                "analysis_group": "Asylee",
                "receipt_number": "ABC123",
                "similarity_found": True,
                "similar_cases_count": 1,
                "similar_case_ids": [800002],
                "similar_case_application_ids": [200],
                "ta_hit": False,
            }
        }


class ApplicantStoreFake:
    def load_applicant_details(self, application_ids, reduce_kafka_size):
        return {100: {"first_name": "Ada", "ocr_name_fault": False}}


class ContentStoreFake:
    def load_document_payload_records(self, application_ids):
        return {100: [{"tsp_id": 10, "section_name": "affidavit", "match_count": 1}]}

    def load_sentence_payload_records(self, application_ids, reduce_kafka_size):
        return {
            100: [
                {
                    "global_id": 1,
                    "local_id": 1,
                    "tsp_id": 10,
                    "section": "affidavit",
                    "sentence": "A complete sentence.",
                    "matches_count": 1,
                    "matches": [{"global_id": 20, "distance": 0.1}],
                    "is_template": False,
                }
            ]
        }

    def load_public_template_payload_records(self, application_ids):
        return {100: {"a_number": 900001, "url": "https://template.local"}}


class TransmissionStoreFake:
    def __init__(self) -> None:
        self.errors = []
        self.deliveries = []

    def save_confirmed_kafka_deliveries(self, confirmed_deliveries, retention_days):
        self.deliveries.extend(confirmed_deliveries)

    def save_transform_error(self, message, error):
        self.errors.append((message, error))

    @staticmethod
    def build_kafka_audit_value(application_event, verbose_logging):
        import json

        return json.dumps(application_event)


class KafkaProcessorFake:
    def __init__(self) -> None:
        self.events = []

    def publish_application_events(self, application_events):
        self.events.extend(application_events)
        return len(application_events)


class KafkaMessageFake:
    def timestamp(self):
        return 1, int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)

    def partition(self):
        return 2

    def offset(self):
        return 10


class KafkaProducerFake:
    def __init__(self, delivery_error=None, pending=0) -> None:
        self.delivery_error = delivery_error
        self.pending = pending
        self.published = []

    def produce(self, topic, key, value, on_delivery):
        self.published.append((topic, key, value))
        on_delivery(self.delivery_error, KafkaMessageFake())

    def poll(self, timeout):
        return 0

    def flush(self):
        return self.pending


class ResultFake:
    def __init__(self, rows=None) -> None:
        self.rows = rows or []

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def one_or_none(self):
        return self.rows[0] if self.rows else None


class SessionFake:
    def __init__(self, rows=None) -> None:
        self.rows = rows or []
        self.executions = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False

    def execute(self, statement, parameters=None):
        self.executions.append((statement, parameters))
        return ResultFake(self.rows)

    def commit(self):
        self.commits += 1


class SessionFactoryFake:
    def __init__(self, sessions):
        self.sessions = list(sessions)

    def __call__(self):
        return self.sessions.pop(0)
