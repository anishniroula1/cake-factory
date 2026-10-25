from doc_delete_completed.opensearch.contracts import DocumentDeletionContext, TitanMatchDeletionResult


class LoggerFake:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.generic_message = None

    def set_generic_message(self, message) -> None:
        self.generic_message = message

    def info(self, message: str) -> None:
        self.messages.append(("info", message))

    def warning(self, message: str) -> None:
        self.messages.append(("warning", message))

    def error(self, message: str) -> None:
        self.messages.append(("error", message))


class OccurrenceStoreFake:
    def __init__(self, records=None, referenced=None, events=None) -> None:
        self.records = records or []
        self.referenced = set(referenced or [])
        self.events = events
        self.deleted: list[str] = []
        self.reference_calls: list[tuple[list[str], str, object]] = []

    def load_occurrences_by_tsp_id(self, tsp_id):
        return self.records

    def delete_occurrences(self, global_ids):
        if self.events is not None:
            self.events.append("delete occurrences")
        self.deleted.extend(global_ids)
        return len(global_ids)

    def find_referenced_sentence_keys(self, sentence_keys, analysis_group, excluded_tsp_id):
        self.reference_calls.append((sentence_keys, analysis_group, excluded_tsp_id))
        return self.referenced


class CatalogStoreFake:
    def __init__(self, events=None) -> None:
        self.events = events
        self.deleted: list[tuple[str, list[str]]] = []

    def delete_sentence_keys(self, analysis_group, sentence_keys):
        if self.events is not None:
            self.events.append("delete catalog")
        self.deleted.append((analysis_group, sentence_keys))
        return len(sentence_keys)


class SentenceVectorDBStoreFake:
    def __init__(self) -> None:
        self.context = DocumentDeletionContext(10, "Asylee", [1, 2], ["key-a", "key-b"])
        self.loaded = []
        self.deleted = []

    def load_document_from_sentence_vector(self, tsp_id):
        self.loaded.append(tsp_id)
        return self.context

    def delete_document_sentences(self, tsp_id):
        self.deleted.append(tsp_id)
        return 2


class EmbeddingsDBStoreFake:
    def __init__(self) -> None:
        self.deleted = []

    def delete_embeddings(self, global_ids):
        self.deleted.extend(global_ids)
        return len(global_ids)


class TitanMatchesDBStoreFake:
    def __init__(self) -> None:
        self.deleted = []

    def delete_document_matches(self, tsp_id):
        self.deleted.append(tsp_id)
        return TitanMatchDeletionResult(2, 3, 4, 1)


class AwsSqsFake:
    def __init__(self, messages=None, send_result=True, delete_result=True) -> None:
        self.messages = messages or []
        self.send_result = send_result
        self.delete_result = delete_result
        self.sent = []
        self.deleted = []

    def listen_sqs_message(self):
        return self.messages

    def send_sqs_message(self, **values):
        self.sent.append(values)
        return self.send_result

    def delete_messages(self, message):
        self.deleted.append(message)
        return self.delete_result


class HandlerFake:
    def __init__(self, failure=None) -> None:
        self.failure = failure
        self.calls = []

    def process(self, tsp_id):
        self.calls.append(tsp_id)
        if self.failure:
            raise self.failure
        return {"next_message_sent": True}


class DependenciesFake:
    def __init__(self, aws_sqs, handler, logger) -> None:
        self.aws_sqs = aws_sqs
        self.document_delete_handler = handler
        self.logger = logger

