from threading import Lock


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def all(self):
        return self.rows


class SessionFake:
    def __init__(self, rows=None, scalar_value=0, document=None):
        self.rows = rows or []
        self.scalar_value = scalar_value
        self.document = document
        self.executed = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *values):
        pass

    def scalar(self, statement):
        return self.scalar_value

    def scalars(self, statement):
        return ScalarRows(self.rows)

    def get(self, model, key):
        return self.document

    def execute(self, statement, parameters=None):
        self.executed.append((statement, parameters))

    def commit(self):
        self.commits += 1


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

    def existing_ids(self, alias, document_ids):
        self.existing_id_calls.append((alias, document_ids))
        return set(document_ids) & self.existing

    def bulk_index(self, alias, documents, needs_embedding):
        self.bulk_calls.append((alias, documents, needs_embedding))
        return len(documents)

    def validate_vector(self, vector):
        if len(vector) != self.vector_dimension:
            raise ValueError("wrong vector size")


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

    def delete_messages(self, message):
        self.deleted.append(message)
        return self.delete_result


class LoggerFake:
    def __init__(self):
        self.debug_messages = []
        self.info_messages = []
        self.warning_messages = []
        self.error_messages = []
        self.exception_messages = []

    def debug(self, message):
        self.debug_messages.append(message)

    def info(self, message):
        self.info_messages.append(message)

    def warning(self, message):
        self.warning_messages.append(message)

    def error(self, message):
        self.error_messages.append(message)

    def exception(self, message):
        self.exception_messages.append(message)
