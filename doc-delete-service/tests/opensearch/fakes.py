from types import SimpleNamespace


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

    def delete_occurrences_by_tsp_id(self, tsp_id):
        if self.events is not None:
            self.events.append("delete occurrences")
        self.deleted.append(tsp_id)
        return len(self.occurrence_records)

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
    def __init__(self, events=None) -> None:
        self.context = SimpleNamespace(global_ids=[1, 2])
        self.loaded = []
        self.deleted = []
        self.events = events

    def load_document_sentence_context(self, tsp_id):
        self.loaded.append(tsp_id)
        return self.context

    def delete_document_sentence_records(self, tsp_id):
        if self.events is not None:
            self.events.append("delete sentence vectors")
        self.deleted.append(tsp_id)
        return 2


class EmbeddingsDBStoreFake:
    def __init__(self, events=None) -> None:
        self.deleted = []
        self.events = events

    def delete_sentence_embeddings(self, global_ids):
        if self.events is not None:
            self.events.append("delete embeddings")
        self.deleted.extend(global_ids)
        return len(global_ids)


class TitanMatchesDBStoreFake:
    def __init__(self, events=None) -> None:
        self.deleted = []
        self.events = events

    def delete_bidirectional_document_matches(self, tsp_id):
        if self.events is not None:
            self.events.append("delete Titan matches")
        self.deleted.append(tsp_id)
        return SimpleNamespace(
            affected_application_ids=[100, 200],
            affected_tsp_ids=[10, 20],
            has_deleted_document=True,
            deleted_match_bucket_count=3,
        )


class AwsSqsFake:
    def __init__(self, messages=None, send_result=True, delete_result=True, events=None) -> None:
        self.messages = messages or []
        self.send_result = send_result
        self.delete_result = delete_result
        self.sent = []
        self.deleted = []
        self.events = events

    def listen_sqs_message(self):
        return self.messages

    def send_sqs_message(self, **values):
        if self.events is not None:
            self.events.append("send SQS")
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
        return {
            "tsp_id": tsp_id,
            "affected_application_ids": [],
            "affected_tsp_ids": [],
            "has_deleted_document": False,
            "occurrence_count": 0,
            "catalog_count": 0,
            "deleted_match_bucket_count": 0,
            "deleted_embedding_count": 0,
            "deleted_sentence_count": 0,
        }


class TextSimilarityInputDBStoreFake:
    def __init__(self, pages=None, events=None, updated_count=1) -> None:
        self.pages = list(pages or [[]])
        self.calls = []
        self.state_updates = []
        self.events = events
        self.updated_count = updated_count

    def load_ready_for_deletion_tsp_ids(self, after_tsp_id, batch_size):
        self.calls.append((after_tsp_id, batch_size))
        return self.pages.pop(0)

    def update_document_state(self, tsp_id, document_state):
        if self.events is not None:
            self.events.append(f"state={document_state}")
        self.state_updates.append((tsp_id, document_state))
        return self.updated_count
