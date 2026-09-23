from threading import Lock, current_thread
from types import SimpleNamespace

import pytest

from sentence_migration.external_api import SentencePage
from sentence_migration.migration_service import SentenceMigrationService


def _row(global_id: int, tsp_id: str, text: str):
    vector = [0.1, 0.2] if text == "Same sentence" else [0.3, 0.4]
    return {
        "applicationId": f"A-{tsp_id}",
        "tspId": tsp_id,
        "sectionName": "Statement",
        "globalId": global_id,
        "sentIdLocal": global_id,
        "sentenceContent": text,
        "sentenceContentVector": vector,
        "isTracer": False,
        "isFormLanguage": False,
        "sourceType": "document",
        "analysisGroup": "Asylee",
    }


class FakeReader:
    def tsp_ids(self, object_key):
        assert object_key == "ids.csv"
        yield "T1"
        yield "T2"


class FakeExternalApi:
    def __init__(self, events):
        self.events = events
        self.key_updates = []
        self.state_updates = []
        self.fetch_calls = []
        self.fetch_threads = []
        self.pages = {
            ("T1", 1): SentencePage(
                [_row(1, "T1", "Same sentence")],
                None,
            ),
            ("T2", 1): SentencePage(
                [_row(2, "T2", "Same sentence")],
                "cursor-2",
            ),
            ("T2", "cursor-2"): SentencePage(
                [_row(3, "T2", "Different sentence")],
                None,
            ),
        }

    def fetch_sentence_page(self, tsp_id, page):
        self.fetch_threads.append(current_thread().name)
        self.fetch_calls.append((tsp_id, page))
        return self.pages[(tsp_id, page)]

    def update_sentence_keys(self, tsp_id, records):
        self.key_updates.append((tsp_id, [record.globalId for record in records]))

    def update_document_state(self, tsp_id):
        thread = current_thread().name
        assert (thread, "refresh") in self.events
        self.events.append((thread, "state"))
        self.state_updates.append(tsp_id)


class FakeOpenSearch:
    def __init__(self, events):
        self.catalog_keys = set()
        self.catalog_documents = []
        self.occurrences = []
        self.required = None
        self.events = events
        self.lock = Lock()

    def require_existing_indices(self, occurrence_alias, catalog_alias):
        self.required = (occurrence_alias, catalog_alias)

    def existing_catalog_keys(self, catalog_alias, keys):
        with self.lock:
            return set(keys) & self.catalog_keys

    def bulk_index_catalog(self, catalog_alias, documents):
        with self.lock:
            self.catalog_keys.update(item["sentenceKey"] for item in documents)
            self.catalog_documents.extend(documents)
        return len(documents)

    def bulk_index_occurrences(self, occurrence_alias, documents):
        required = {item["sentenceKey"] for item in documents}
        with self.lock:
            assert required <= self.catalog_keys
            assert all("sentenceContentVector" not in item for item in documents)
            self.occurrences.extend(item["globalId"] for item in documents)
        return len(documents)

    def refresh_indices(self, occurrence_alias, catalog_alias):
        self.events.append((current_thread().name, "refresh"))


def test_each_worker_pages_one_tsp_and_marks_document_after_refresh():
    config = SimpleNamespace(s3_bucket="bucket")
    events = []
    external_api = FakeExternalApi(events)
    opensearch = FakeOpenSearch(events)
    service = SentenceMigrationService(
        config,
        FakeReader(),
        external_api,
        opensearch,
    )

    result = service.migrate("ids.csv", "occurrences", "catalog")

    assert opensearch.required == ("occurrences", "catalog")
    assert sorted(opensearch.occurrences) == [1, 2, 3]
    assert len(opensearch.catalog_keys) == 2
    assert all(
        document["sentenceContentVector"] for document in opensearch.catalog_documents
    )
    assert sorted(external_api.key_updates) == [
        ("T1", [1]),
        ("T2", [2]),
        ("T2", [3]),
    ]
    assert sorted(external_api.state_updates) == ["T1", "T2"]
    assert set(external_api.fetch_calls) == {
        ("T1", 1),
        ("T2", 1),
        ("T2", "cursor-2"),
    }
    assert all(name.startswith("tsp-migration") for name in external_api.fetch_threads)
    assert result["tspIdsRead"] == 2
    assert result["pagesRead"] == 3
    assert result["sentencesRead"] == 3
    assert result["occurrencesIndexed"] == 3
    assert result["catalogDocumentsIndexed"] == 2
    assert result["catalogDocumentsReused"] == 1
    assert result["sentenceKeysUpdated"] == 3
    assert result["documentsCompleted"] == 2
    assert result["completedWorkers"] == 2
    assert result["workers"] == 100


def test_failed_page_key_update_never_marks_document_complete():
    class SingleReader:
        def tsp_ids(self, object_key):
            yield "T1"

    class FailingExternalApi(FakeExternalApi):
        def update_sentence_keys(self, tsp_id, records):
            raise RuntimeError("update API unavailable")

    config = SimpleNamespace(s3_bucket="bucket")
    events = []
    external_api = FailingExternalApi(events)
    opensearch = FakeOpenSearch(events)
    service = SentenceMigrationService(
        config,
        SingleReader(),
        external_api,
        opensearch,
    )

    with pytest.raises(RuntimeError, match="update API unavailable"):
        service.migrate("ids.csv", "occurrences", "catalog")

    assert external_api.state_updates == []
    assert not any(event[1] == "refresh" for event in events)
