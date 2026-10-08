from types import SimpleNamespace

import pytest

from sentence_search.tracer_seed_service import TracerSeedService


class FakeTracerOpenSearchStore:
    def __init__(self):
        self.batches = []
        self.refresh_calls = 0

    def bulk_index_tracer_sentences(self, documents):
        self.batches.append(documents)
        return len(documents)

    def refresh_tracer_sentences(self):
        self.refresh_calls += 1


def test_tracer_csv_indexes_only_mapping_fields_in_batches():
    opensearch = FakeTracerOpenSearchStore()
    service = TracerSeedService(
        SimpleNamespace(seed_batch_size=2, tracer_alias="tracer-catalog"),
        opensearch,
    )
    csv_bytes = (
        b"tracer_id,sentence_content,analysis_group,is_non_matcher,"
        b"is_form_language,created_at,unused_column\n"
        b"10,First tracer,Asylee,false,false,2026-01-01T00:00:00Z,ignored\n"
        b"11,Form sentence,Asylee,false,true,,ignored\n"
        b"12,Do not match,Refugee,1,0,,ignored\n"
    )

    result = service.seed(csv_bytes)

    assert [len(batch) for batch in opensearch.batches] == [2, 1]
    assert opensearch.batches[0][0] == {
        "tracerId": 10,
        "sentenceContent": "First tracer",
        "analysisGroup": "Asylee",
        "isNonMatcher": False,
        "isFormLanguage": False,
        "createdAt": "2026-01-01T00:00:00Z",
    }
    assert result == {
        "recordsRead": 3,
        "tracerDocumentsIndexed": 3,
        "completedBatches": 2,
        "batchSize": 2,
        "alias": "tracer-catalog",
        "embeddingGeneration": "OpenSearch ingest pipeline",
    }
    assert opensearch.refresh_calls == 1


def test_tracer_csv_reports_the_bad_row_number():
    opensearch = FakeTracerOpenSearchStore()
    service = TracerSeedService(
        SimpleNamespace(seed_batch_size=100, tracer_alias="tracer-catalog"),
        opensearch,
    )
    csv_bytes = (
        b"tracer_id,sentence_content,analysis_group,is_non_matcher,"
        b"is_form_language\n"
        b"10,First tracer,Asylee,not-a-boolean,false\n"
    )

    with pytest.raises(ValueError, match="Invalid tracer CSV row 2"):
        service.seed(csv_bytes)

    assert opensearch.batches == []
    assert opensearch.refresh_calls == 0
