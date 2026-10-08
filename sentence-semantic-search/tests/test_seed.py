from threading import Barrier, Lock, current_thread
from types import SimpleNamespace

import pytest

from sentence_search.seed_service import (
    SeedService,
    _canonical_headers,
    csv_batches,
)


def test_legacy_headers_are_corrected():
    assert _canonical_headers(
        ["application_Id", "local_globa_id", "senetence_content"]
    ) == ["applicationId", "sentIdLocal", "sentenceContent"]


def test_sample_csv_loads_without_sorting(tmp_path):
    csv_file = tmp_path / "sentences.csv"
    csv_file.write_text(
        "applicationId,tspId,sectionName,globalId,sentIdLocal,"
        "sentenceContent,isTracer,isFormLanguage,sentenceKey,"
        "sourceType,createdAt,updatedAt,analysisGroup\n"
        "A1,T1,Statement,9,9,Last sentence,false,false,,document,,,G1\n"
        "A1,T1,Statement,2,2,Earlier ID,false,false,,document,,,G1\n",
        encoding="utf-8",
    )

    records = []
    for batch in csv_batches(csv_file, 1):
        records.extend(batch)

    assert [record.globalId for record in records] == [9, 2]


def test_csv_rejects_non_numeric_global_id_with_row_number(tmp_path):
    csv_file = tmp_path / "bad-global-id.csv"
    csv_file.write_text(
        "applicationId,tspId,sectionName,globalId,sentIdLocal,"
        "sentenceContent,isTracer,isFormLanguage,sentenceKey,"
        "sourceType,createdAt,updatedAt,analysisGroup\n"
        "A1,T1,Statement,SENT-9,9,Sentence,false,false,,document,,,G1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid CSV row 2"):
        list(csv_batches(csv_file, 1))


class FakeParallelOpenSearchStore:
    def __init__(self, workers):
        self.catalog_keys = set()
        self.occurrence_ids = []
        self.occurrence_thread_names = []
        self.refresh_calls = 0
        self._workers = workers
        self._first_wave = Barrier(workers)
        self._catalog_calls = 0
        self._active_catalog_calls = 0
        self.max_active_catalog_calls = 0
        self._lock = Lock()

    def existing_catalog_keys(self, keys):
        return set(keys) & self.catalog_keys

    def bulk_index_catalog(self, sources):
        with self._lock:
            self._catalog_calls += 1
            call_number = self._catalog_calls
            self._active_catalog_calls += 1
            self.max_active_catalog_calls = max(
                self.max_active_catalog_calls,
                self._active_catalog_calls,
            )
        if call_number <= self._workers:
            self._first_wave.wait(timeout=2)
        with self._lock:
            self.catalog_keys.update(source["sentenceKey"] for source in sources)
            self._active_catalog_calls -= 1
        return len(sources)

    def bulk_index_occurrences(self, sources):
        required_keys = {source["sentenceKey"] for source in sources}
        assert required_keys <= self.catalog_keys
        with self._lock:
            self.occurrence_ids.extend(source["globalId"] for source in sources)
            self.occurrence_thread_names.append(current_thread().name)
        return len(sources)

    def refresh_indices(self):
        self.refresh_calls += 1


def test_seed_runs_bounded_catalog_workers_without_matching(tmp_path):
    workers = 3
    csv_file = tmp_path / "parallel-sentences.csv"
    rows = [
        "A1,T1,Statement,1,1,Sentence one,false,false,,document,,,Asylee",
        "A1,T1,Statement,2,2,Sentence two,false,false,,document,,,Asylee",
        "A1,T1,Statement,3,3,Sentence three,false,false,,document,,,Asylee",
        "A1,T1,Statement,4,4,Sentence four,false,false,,document,,,Asylee",
        "A1,T1,Statement,5,5,Sentence five,false,false,,document,,,Asylee",
        "A1,T1,Statement,6,6,Sentence six,false,false,,document,,,Asylee",
    ]
    csv_file.write_text(
        "applicationId,tspId,sectionName,globalId,sentIdLocal,"
        "sentenceContent,isTracer,isFormLanguage,sentenceKey,"
        "sourceType,createdAt,updatedAt,analysisGroup\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )
    opensearch = FakeParallelOpenSearchStore(workers)
    service = SeedService(
        SimpleNamespace(seed_batch_size=2, seed_workers=workers),
        opensearch,
    )

    result = service.seed(csv_file)

    assert sorted(opensearch.occurrence_ids) == list(range(1, 7))
    assert all(
        name.startswith("sentence-seed") for name in opensearch.occurrence_thread_names
    )
    assert opensearch.max_active_catalog_calls == workers
    assert opensearch.refresh_calls == 1
    assert result["completedBatches"] == 3
    assert result["completedWaves"] == 1
    assert result["seedBatchSize"] == 2
    assert result["seedWorkers"] == workers
    assert result["matchProcessing"] == "searchTime"
    assert "matchesCalculated" not in result


def test_repeated_sentence_waits_for_the_worker_creating_its_vector(tmp_path):
    workers = 2
    csv_file = tmp_path / "repeated-sentence.csv"
    csv_file.write_text(
        "applicationId,tspId,sectionName,globalId,sentIdLocal,"
        "sentenceContent,isTracer,isFormLanguage,sentenceKey,"
        "sourceType,createdAt,updatedAt,analysisGroup\n"
        "A1,T1,Statement,1,1,Same sentence,false,false,,document,,,Asylee\n"
        "A2,T2,Statement,2,1,Same sentence,false,false,,document,,,Asylee\n",
        encoding="utf-8",
    )
    opensearch = FakeParallelOpenSearchStore(workers)
    service = SeedService(
        SimpleNamespace(seed_batch_size=1, seed_workers=workers),
        opensearch,
    )

    result = service.seed(csv_file)

    assert opensearch.occurrence_ids == [1, 2]
    assert result["catalogDocumentsIndexed"] == 1
    assert result["occurrencesIndexed"] == 2
