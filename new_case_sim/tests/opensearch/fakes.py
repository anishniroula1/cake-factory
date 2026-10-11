from case_similarity_service.opensearch.contracts import CaseSummary, DocumentContext, KafkaEvent
from case_similarity_service.opensearch.errors import OperationFailedError


class LoggerFake:
    def __init__(self) -> None:
        self.messages = []
        self.generic_message = None

    def debug(self, message: str) -> None:
        self.messages.append(("debug", message))

    def info(self, message: str) -> None:
        self.messages.append(("info", message))

    def warning(self, message: str) -> None:
        self.messages.append(("warning", message))

    def error(self, message: str) -> None:
        self.messages.append(("error", message))

    def set_generic_message(self, message: str | None) -> None:
        self.generic_message = message


class AwsSqsFake:
    def __init__(
        self,
        messages: list[dict] | None = None,
        send_result: bool = True,
        delete_result: bool = True,
        **init_values,
    ) -> None:
        self.messages = messages or []
        self.send_result = send_result
        self.delete_result = delete_result
        self.init_values = init_values
        self.sent = []
        self.deleted = []

    def listen_sqs_message(self) -> list[dict]:
        return self.messages

    def send_sqs_message(self, **values) -> bool:
        self.sent.append(values)
        return self.send_result

    def delete_messages(self, message: dict) -> bool:
        self.deleted.append(message)
        return self.delete_result


class CaseSimilarityDBStoreFake:
    def __init__(self) -> None:
        self.context = DocumentContext(10, 100, 900001, "Asylee")
        self.summaries = [
            # Application 100 has one sentence pair with 200, two with 300,
            # and three with 400. This represents all three case-match levels.
            CaseSummary(100, "Asylee", [200, 300, 400], [300, 400], [400]),
            CaseSummary(200, "Asylee", [100], [], []),
            CaseSummary(300, "Asylee", [100], [100], []),
            CaseSummary(400, "Asylee", [100], [100], [100]),
        ]
        self.refreshed = []

    def load_document_context(self, tsp_id):
        return self.context

    def refresh_case_summaries(self, source_application_id, analysis_group):
        self.refreshed.append((source_application_id, analysis_group))
        return self.summaries


class HandlerFake:
    def __init__(self, failure: bool = False) -> None:
        self.calls = []
        self.failure = failure

    def process(self, tsp_id):
        self.calls.append(tsp_id)
        if self.failure:
            raise OperationFailedError("temporary database failure")
        return {"tsp_id": tsp_id}


class DependenciesFake:
    def __init__(self, aws_sqs: AwsSqsFake, handler: HandlerFake, logger: LoggerFake) -> None:
        self.aws_sqs = aws_sqs
        self.case_similarity_handler = handler
        self.logger = logger


class KafkaEventDBStoreFake:
    def __init__(self, events: list[KafkaEvent] | None = None) -> None:
        self.events = events if events is not None else [KafkaEvent("ABC123", 100, 900001, "Asylee", True)]
        self.deliveries = []

    def load_changed_tah_events(self, application_ids, kafka_environment):
        return self.events

    def save_kafka_delivery(self, event, delivery, kafka_environment):
        self.deliveries.append((event, delivery, kafka_environment))


class KafkaProducerFake:
    def __init__(
        self,
        config: dict | None = None,
        *,
        delivery_error: Exception | None = None,
        pending_message_count: int = 0,
        produce_error: Exception | None = None,
        acknowledge: bool = True,
    ) -> None:
        self.config = config or {}
        self.delivery_error = delivery_error
        self.pending_message_count = pending_message_count
        self.produce_error = produce_error
        self.acknowledge = acknowledge
        self.published = []

    def produce(self, topic, key, value, on_delivery):
        if self.produce_error:
            raise self.produce_error
        self.published.append((topic, key, value))
        if self.acknowledge:
            on_delivery(self.delivery_error, KafkaMessageFake())

    def poll(self, timeout):
        return 0

    def flush(self):
        return self.pending_message_count


class KafkaMessageFake:
    def __init__(self, timestamp_milliseconds: int = 1791633600000) -> None:
        self.timestamp_milliseconds = timestamp_milliseconds

    def timestamp(self):
        return 1, self.timestamp_milliseconds

    def partition(self):
        return 1

    def offset(self):
        return 20


class ResultFake:
    def __init__(self, rows=None, scalar=None) -> None:
        self.rows = rows or []
        self.scalar = scalar

    def mappings(self):
        return self

    def scalars(self):
        return iter(self.rows)

    def all(self):
        return self.rows

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def scalar_one_or_none(self):
        return self.scalar

    def __iter__(self):
        return iter(self.rows)


class SessionFake:
    def __init__(self, execute_result=None, execute_router=None) -> None:
        self.execute_result = execute_result or ResultFake()
        self.execute_router = execute_router
        self.executions = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False

    def execute(self, statement, parameters=None):
        self.executions.append((statement, parameters))
        if self.execute_router:
            return self.execute_router(statement, parameters)
        return self.execute_result

    def commit(self):
        self.commits += 1


class SessionFactoryFake:
    def __init__(self, sessions: list[SessionFake]) -> None:
        self.sessions = sessions

    def __call__(self):
        return self.sessions.pop(0)


class ProcessorFake:
    run_count = 0

    def __init__(self, dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        ProcessorFake.run_count += 1


class MainDependenciesFake:
    def __init__(self) -> None:
        self.logger = LoggerFake()
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class EngineFake:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


class DatabaseConnectionFake:
    def __init__(self, **arguments) -> None:
        self.arguments = arguments
        self.engine = EngineFake()


class VaultSecretsFake:
    def __init__(self) -> None:
        self.values = {
            "pgUser": "user",
            "pgPassword": "password",
            "pgHost": "database-local",
            "pgDatabase": "case-similarity-local",
            "pgPort": "5432",
        }

    def get(self, name: str, default=None):
        return self.values.get(name, default)
