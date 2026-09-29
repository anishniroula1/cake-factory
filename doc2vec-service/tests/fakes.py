from threading import Lock


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def all(self):
        return self.rows


class SessionFake:
    def __init__(self, rows=None, scalarValue=0, document=None):
        self.rows = rows or []
        self.scalarValue = scalarValue
        self.document = document
        self.executed = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *values):
        pass

    def scalar(self, statement):
        return self.scalarValue

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
    def __init__(self, existing=None, vectorDimension=2):
        self.existing = existing or set()
        self.vectorDimension = vectorDimension
        self.bulkCalls = []

    def existingIds(self, alias, documentIds):
        return set(documentIds) & self.existing

    def bulkIndex(self, alias, documents, needsEmbedding):
        self.bulkCalls.append((alias, documents, needsEmbedding))
        return len(documents)

    def validateVector(self, vector):
        if len(vector) != self.vectorDimension:
            raise ValueError("wrong vector size")


class SqsFake:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.sent = []
        self.deleted = []
        self.sendResult = True
        self.deleteResult = True

    def listen_sqs_message(self):
        messages = self.messages
        self.messages = []
        return messages

    def send_sqs_message(self, **values):
        self.sent.append(values)
        return self.sendResult

    def delete_messages(self, message):
        self.deleted.append(message)
        return self.deleteResult


class LoggerFake:
    def __init__(self):
        self.debugMessages = []
        self.infoMessages = []
        self.warningMessages = []
        self.errorMessages = []
        self.exceptionMessages = []

    def debug(self, message):
        self.debugMessages.append(message)

    def info(self, message):
        self.infoMessages.append(message)

    def warning(self, message):
        self.warningMessages.append(message)

    def error(self, message):
        self.errorMessages.append(message)

    def exception(self, message):
        self.exceptionMessages.append(message)
