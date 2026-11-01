from doc_delete_job.opensearch.contracts import DocumentDeletionContext, TitanMatchDeletionResult


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
    def __init__(self, occurrence_records=None, shared_sentence_keys=None, events=None) -> None:
        self.occurrence_records = occurrence_records or []
        self.shared_sentence_keys = set(shared_sentence_keys or [])
        self.events = events
        self.deleted: list[str] = []
        self.reference_calls: list[tuple[list[str], str, object]] = []

    def load_document_occurrences(self, tsp_id):
        return self.occurrence_records

    def delete_occurrences_by_global_id(self, global_ids):
        if self.events is not None:
            self.events.append("delete occurrences")
        self.deleted.extend(global_ids)
        return len(global_ids)

    def find_sentence_keys_used_by_other_documents(self, sentence_keys, analysis_group, excluded_tsp_id):
        self.reference_calls.append((sentence_keys, analysis_group, excluded_tsp_id))
        return self.shared_sentence_keys


class CatalogStoreFake:
    def __init__(self, events=None) -> None:
        self.events = events
        self.deleted: list[tuple[str, list[str]]] = []

    def delete_catalog_documents_by_sentence_key(self, analysis_group, sentence_keys):
        if self.events is not None:
            self.events.append("delete catalog")
        self.deleted.append((analysis_group, sentence_keys))
        return len(sentence_keys)


class SentenceVectorDBStoreFake:
    def __init__(self) -> None:
        self.context = DocumentDeletionContext(10, "Asylee", [1, 2], ["key-a", "key-b"])
        self.loaded = []
        self.deleted = []

    def load_document_sentence_context(self, tsp_id):
        self.loaded.append(tsp_id)
        return self.context

    def delete_document_sentence_records(self, tsp_id):
        self.deleted.append(tsp_id)
        return 2


class EmbeddingsDBStoreFake:
    def __init__(self) -> None:
        self.deleted = []

    def delete_sentence_embeddings(self, global_ids):
        self.deleted.extend(global_ids)
        return len(global_ids)


class TitanMatchesDBStoreFake:
    def __init__(self) -> None:
        self.deleted = []

    def delete_bidirectional_document_matches(self, tsp_id):
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

    def process_document_deletion(self, tsp_id):
        self.calls.append(tsp_id)
        if self.failure:
            raise self.failure
        return {"next_message_sent": True}


class TextSimilarityInputDBStoreFake:
    def __init__(self, pages=None) -> None:
        self.pages = list(pages or [[]])
        self.calls = []

    def load_ready_for_deletion_tsp_ids(self, after_tsp_id, batch_size):
        self.calls.append((after_tsp_id, batch_size))
        return self.pages.pop(0)
