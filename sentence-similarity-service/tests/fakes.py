from threading import Lock


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class ExecuteResult:
    def __init__(self, value=0):
        self.value = value

    def scalar_one(self):
        return self.value


class SessionFake:
    def __init__(self, rows=None, scalarValue=0, executeValue=0):
        self.rows = rows or []
        self.scalarValue = scalarValue
        self.executeValue = executeValue
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

    def execute(self, statement, parameters=None):
        self.executed.append((statement, parameters))
        return ExecuteResult(self.executeValue)

    def commit(self):
        self.commits += 1


class SessionSequence:
    def __init__(self, sessions):
        self.sessions = sessions
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            return self.sessions.pop(0)


class SqsFake:
    def __init__(self):
        self.sent = []
        self.deleted = []

    def send_message(self, **values):
        self.sent.append(values)

    def delete_message(self, **values):
        self.deleted.append(values)


class OpenSearchHandlerFake:
    def __init__(self, sentenceKey, nonMatcher=False):
        self.sentenceKey = sentenceKey
        self.nonMatcher = nonMatcher
        self.updated = []
        self.catalogCalls = 0

    def catalogVectors(self, sentenceKeys):
        return {key: [0.1, 0.2] for key in sentenceKeys}

    def tracerClassification(self, vector, analysisGroup):
        return {
            "is_non_matcher": self.nonMatcher,
            "is_tracer": self.nonMatcher,
            "is_form_language": False,
        }

    def updateOccurrence(self, globalId, classification):
        self.updated.append((globalId, classification))

    def catalogMatches(self, sourceKey, vector):
        self.catalogCalls += 1
        return {"b" * 64: 0.1}
