from threading import Lock

from sqlalchemy.exc import SQLAlchemyError

from doc2vec_completed.opensearch.errors import PermanentDataError


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def all(self):
        return list(self.rows)


class SessionFake:
    def __init__(
        self,
        rows=None,
        scalar_value=0,
        document=None,
        fail_flush_number=None,
        fail_execute=False,
    ):
        self.rows = rows or []
        self.scalar_value = scalar_value
        self.document = document
        self.executed = []
        self.scalar_statements = []
        self.scalars_statements = []
        self.commits = 0
        self.flushes = 0
        self.rollbacks = 0
        self.added = []
        self.next_global_id = 1
        self.fail_flush_number = fail_flush_number
        self.fail_execute = fail_execute

    def __enter__(self):
        return self

    def __exit__(self, *values):
        pass

    def scalar(self, statement):
        self.scalar_statements.append(statement)
        return self.scalar_value

    def scalars(self, statement):
        self.scalars_statements.append(statement)
        return ScalarRows(self.rows)

    def get(self, model, key):
        return self.document

    def execute(self, statement, parameters=None):
        if self.fail_execute:
            raise SQLAlchemyError("database execute failed")
        self.executed.append((statement, parameters))

    def add_all(self, rows):
        for row in rows:
            if row.global_id is None:
                row.global_id = self.next_global_id
                self.next_global_id += 1
        self.added.extend(rows)

    def flush(self):
        self.flushes += 1
        if self.flushes == self.fail_flush_number:
            raise SQLAlchemyError("database flush failed")

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class SessionSequence:
    def __init__(self, sessions):
        self.sessions = sessions
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            return self.sessions.pop(0)


class OpenSearchFake:
    def __init__(self, existing=None, vector_dimension=2):
        self.existing = existing or set()
        self.vector_dimension = vector_dimension
        self.existing_id_calls = []
        self.bulk_calls = []
        self.refresh_alias_calls = []

    def find_existing_document_ids(self, alias, document_ids):
        self.existing_id_calls.append((alias, document_ids))
        return set(document_ids) & self.existing

    def index_documents(self, alias, documents, needs_embedding):
        self.bulk_calls.append((alias, documents, needs_embedding))
        return len(documents)

    def validate_vector(self, vector):
        if len(vector) != self.vector_dimension:
            raise PermanentDataError("wrong vector size")

    def refresh_aliases(self, aliases):
        self.refresh_alias_calls.append(aliases)


class SqsFake:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.sent = []
        self.deleted = []
        self.send_result = True
        self.delete_result = True

    def listen_sqs_message(self):
        messages = self.messages
        self.messages = []
        return messages

    def send_sqs_message(self, **values):
        self.sent.append(values)
        return self.send_result

    def send_batch_messages(self, send_queue_url, batch_messages, **values):
        self.sent.append(
            {
                "send_queue_url": send_queue_url,
                "batch_messages": batch_messages,
                **values,
            }
        )
        return {"status": "SUCCEED" if self.send_result else "FAILED"}

    def delete_messages(self, message):
        self.deleted.append(message)
        return self.delete_result


class LoggerFake:
    def __init__(self):
        self.debug_messages = []
        self.info_messages = []
        self.warning_messages = []
        self.error_messages = []
        self.generic_message = None
        self.raise_fatal_errors = True

    def set_generic_message(self, message):
        self.generic_message = message

    def debug(self, message):
        self.debug_messages.append(message)

    def info(self, message):
        self.info_messages.append(message)

    def warning(self, message):
        self.warning_messages.append(message)

    def error(self, message):
        self.error_messages.append(message)
        # A success test should fail right away when it receives this error log.
        if self.raise_fatal_errors and message.startswith(
            "Historical SQS message failed"
        ):
            raise AssertionError(message)

