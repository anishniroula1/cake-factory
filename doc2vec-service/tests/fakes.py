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
    def __init__(self):
        self.sent = []
        self.deleted = []
        self.visibility = []

    def send_message(self, **values):
        self.sent.append(values)

    def delete_message(self, **values):
        self.deleted.append(values)

    def change_message_visibility(self, **values):
        self.visibility.append(values)

