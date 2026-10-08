import base64
import struct
from threading import Lock
from unittest.mock import MagicMock, create_autospec

from opensearchpy import OpenSearch
from sqlalchemy.engine import Result, ScalarResult
from sqlalchemy.orm import Session


def session_mock(*, rows=None, scalar_value=0, execute_value=0, execute_rows=None):
    """Return a SQLAlchemy Session mock with realistic result objects."""

    session = MagicMock(spec=Session)
    session.__enter__.return_value = session
    session.__exit__.return_value = None
    session.scalar.return_value = scalar_value

    scalar_result = MagicMock(spec=ScalarResult)
    scalar_result.__iter__.side_effect = lambda: iter(rows or [])
    session.scalars.return_value = scalar_result

    execute_result = MagicMock(spec=Result)
    execute_result.scalar_one.return_value = execute_value
    execute_result.__iter__.side_effect = lambda: iter(execute_rows or [])
    session.execute.return_value = execute_result
    return session


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


def _encoded_vector(values):
    """Encode vectors exactly like OpenSearch binary doc values."""

    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode()


class OpenSearchResponseRouter:
    """Return OpenSearch-shaped JSON while the real stores build each query."""

    def __init__(self, source_key, *, saved_classification=None, tracer_result=None, missing_catalog_keys=None):
        self.source_key = source_key
        self.saved_classification = saved_classification or {
            "is_non_matcher": False,
            "is_tracer": False,
            "is_form_language": False,
        }
        self.tracer_result = tracer_result
        self.missing_catalog_keys = set(missing_catalog_keys or [])
        self.saved_classifications = []
        self.semantic_search_count = 0

    def search(self, **values):
        """Route a real store request to its matching unit-test response."""

        body = values["body"]
        index = values["index"]
        query = body["query"]

        if "ids" in query:
            return self._catalog_records(query["ids"]["values"])
        if "knn" in query:
            self.semantic_search_count += 1
            return self._catalog_matches()
        if index == "tracer":
            return self._tracer_matches()
        if index == "occurrence":
            return self._occurrences(body)
        raise AssertionError(f"Unexpected OpenSearch request for index {index}: {body}")

    def update(self, **values):
        """Remember catalog classification updates made by the real store."""

        self.saved_classifications.append(values)
        return {"result": "updated"}

    def _catalog_records(self, sentence_keys):
        hits = []
        for sentence_key in sentence_keys:
            if sentence_key in self.missing_catalog_keys:
                continue
            classification = self.saved_classification
            hits.append(
                {
                    "_id": sentence_key,
                    "_source": {
                        "sentenceKey": sentence_key,
                        "isNonMatcher": classification["is_non_matcher"],
                        "isTracer": classification["is_tracer"],
                        "isFormLanguage": classification["is_form_language"],
                    },
                    "fields": {"sentenceContentVector": [_encoded_vector([0.1, 0.2])]},
                }
            )
        return {"hits": {"hits": hits}}

    def _catalog_matches(self):
        return {
            "aggregations": {
                "matches": {
                    "buckets": [
                        {"sample": {"hits": {"hits": [{"_score": 1.0, "_source": {"sentenceKey": self.source_key}}]}}}
                    ]
                }
            }
        }

    def _tracer_matches(self):
        if self.tracer_result is None:
            return {"hits": {"hits": []}}
        return {
            "hits": {
                "hits": [
                    {
                        "_score": 0.95,
                        "_source": {
                            "isNonMatcher": self.tracer_result["is_non_matcher"],
                            "isFormLanguage": self.tracer_result["is_form_language"],
                        },
                    }
                ]
            }
        }

    def _occurrences(self, body):
        sentence_keys = next(
            item["terms"]["sentenceKey"] for item in body["query"]["bool"]["filter"] if "terms" in item
        )
        hits = [
            {
                "_source": {"globalId": position, "tspId": 20, "applicationId": 2, "sentenceKey": sentence_key},
                "sort": [position],
            }
            for position, sentence_key in enumerate(sentence_keys, start=200)
        ]
        return {"hits": {"hits": hits}}


def opensearch_mock(source_key, *, saved_classification=None, tracer_result=None, missing_catalog_keys=None):
    """Mock only the OpenSearch client; production stores still run."""

    router = OpenSearchResponseRouter(
        source_key,
        saved_classification=saved_classification,
        tracer_result=tracer_result,
        missing_catalog_keys=missing_catalog_keys,
    )
    client = create_autospec(OpenSearch, instance=True)
    client.search.side_effect = router.search
    client.update.side_effect = router.update
    return client, router
