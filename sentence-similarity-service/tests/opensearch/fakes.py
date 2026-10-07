from threading import Lock


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class ExecuteResult:
    def __init__(self, value=0, rows=None):
        self.value = value
        self.rows = rows or []

    def scalar_one(self):
        return self.value

    def __iter__(self):
        return iter(self.rows)


class SessionFake:
    def __init__(
        self,
        *,
        rows=None,
        scalar_value=0,
        execute_value=0,
        execute_rows=None,
    ):
        self.rows = rows or []
        self.scalar_value = scalar_value
        self.execute_value = execute_value
        self.execute_rows = execute_rows or []
        self.executed = []
        self.scalar_statements = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *values):
        return None

    def scalar(self, statement):
        self.scalar_statements.append(statement)
        return self.scalar_value

    def scalars(self, statement):
        return ScalarRows(self.rows)

    def execute(self, statement, parameters=None):
        self.executed.append((statement, parameters))
        return ExecuteResult(self.execute_value, self.execute_rows)

    def commit(self):
        self.commits += 1


class SessionSequence:
    def __init__(self, sessions):
        self.sessions = sessions
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            return self.sessions.pop(0)


class LoggerFake:
    def __init__(self):
        self.messages = []

    def debug(self, message):
        self.messages.append(("debug", message))

    def info(self, message):
        self.messages.append(("info", message))

    def warning(self, message):
        self.messages.append(("warning", message))

    def error(self, message):
        self.messages.append(("error", message))


class SqsFake:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.sent = []
        self.deleted = []
        self.polls = 0

    def listen_sqs_message(self):
        self.polls += 1
        return self.messages

    def send_sqs_message(self, **values):
        self.sent.append(values)
        return True

    def delete_messages(self, message):
        self.deleted.append(message)
        return True


class OpenSearchFake:
    def __init__(self, source_key, *, non_matcher=False):
        self.source_key = source_key
        self.non_matcher = non_matcher
        self.saved_classifications = []

    def catalog_alias(self, analysis_group):
        return f"catalog-{analysis_group.casefold()}"

    def catalog_records(self, catalog_alias, sentence_keys):
        classification = {
            "is_non_matcher": self.non_matcher,
            "closest_non_matcher": 501 if self.non_matcher else None,
            "closest_nn_score": 0.95 if self.non_matcher else None,
            "is_tracer": False,
            "is_form_language": False,
        }
        return {
            key: {
                "sentence_key": key,
                "vector": [0.1, 0.2],
                "classification": classification,
            }
            for key in sentence_keys
        }

    def tracer_classification(self, vector, analysis_group):
        return {
            "is_non_matcher": self.non_matcher,
            "closest_non_matcher": 501 if self.non_matcher else None,
            "closest_nn_score": 0.95 if self.non_matcher else None,
            "is_tracer": False,
            "is_form_language": False,
        }

    def save_catalog_classification(
        self,
        catalog_alias,
        sentence_key,
        classification,
    ):
        self.saved_classifications.append(
            (catalog_alias, sentence_key, classification)
        )

    def catalog_matches(self, catalog_alias, source_key, vector):
        return {source_key: 0.0}

    def target_occurrences(
        self,
        key_distances,
        analysis_group,
        source_application_id,
    ):
        return [
            {
                "global_id": 200,
                "tsp_id": 20,
                "application_id": 2,
                "sentence_key": self.source_key,
                "distance": key_distances[self.source_key],
            }
        ]
