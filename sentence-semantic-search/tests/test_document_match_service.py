from sentence_search.document_match_service import DocumentMatchService


class FakeSentenceSummaryService:
    def __init__(self, scores):
        self.scores = scores

    def matching_key_scores(self, source_keys, analysis_group, threshold):
        assert source_keys == ["source-key"]
        assert analysis_group == "Asylee"
        assert threshold == 90
        return self.scores


class FakeDocumentMatchStore:
    def __init__(self, source_sentences, count_rows, target_sentences):
        self.source_sentences = source_sentences
        self.count_rows = count_rows
        self.target_sentences = target_sentences

    def tsp_occurrence_page(
        self,
        tsp_id,
        analysis_group,
        page_size,
        after_global_id,
    ):
        assert tsp_id == "101"
        assert analysis_group == "Asylee"
        assert after_global_id is None
        return {
            "sentences": self.source_sentences,
            "nextAfterGlobalId": None,
        }

    def occurrence_counts_by_tsp_and_key(
        self,
        sentence_keys,
        excluded_tsp_id,
        analysis_group,
    ):
        assert set(sentence_keys) == {"source-key", "target-key"}
        assert excluded_tsp_id == "101"
        assert analysis_group == "Asylee"
        return self.count_rows

    def matching_occurrences_for_documents(
        self,
        sentence_keys,
        tsp_ids,
        excluded_tsp_id,
        analysis_group,
    ):
        assert set(sentence_keys) == {"source-key", "target-key"}
        assert tsp_ids == ["201", "202"]
        assert excluded_tsp_id == "101"
        assert analysis_group == "Asylee"
        return self.target_sentences


def sentence(application_id, tsp_id, global_id, sentence_key, text):
    return {
        "applicationId": application_id,
        "tspId": tsp_id,
        "sectionName": "Affidavit",
        "globalId": global_id,
        "sentIdLocal": global_id,
        "sentenceContent": text,
        "sentenceKey": sentence_key,
        "sourceType": "document",
        "analysisGroup": "Asylee",
    }


def test_document_match_keeps_target_documents_separate():
    source = sentence(1, 101, 10, "source-key", "Source sentence")
    targets = [
        sentence(2, 201, 20, "target-key", "Similar target"),
        sentence(2, 201, 21, "source-key", "Exact target"),
        sentence(2, 202, 22, "target-key", "Other document target"),
    ]
    count_rows = [
        {
            "tspId": "201",
            "applicationId": "2",
            "sentenceKey": "target-key",
            "occurrenceCount": 1,
        },
        {
            "tspId": "201",
            "applicationId": "2",
            "sentenceKey": "source-key",
            "occurrenceCount": 1,
        },
        {
            "tspId": "202",
            "applicationId": "2",
            "sentenceKey": "target-key",
            "occurrenceCount": 1,
        },
    ]
    store = FakeDocumentMatchStore([source], count_rows, targets)
    summary = FakeSentenceSummaryService(
        {"source-key": {"source-key": 100.0, "target-key": 94.0}}
    )

    result = DocumentMatchService(store, summary).compare("101", "Asylee", 90)

    assert result["totalMatchingDocuments"] == 2
    assert result["totalMatchCount"] == 3
    assert [item["tspId"] for item in result["documents"]] == ["201", "202"]
    assert result["documents"][0]["applicationId"] == "2"
    assert result["documents"][0]["exactMatchCount"] == 1
    assert [
        target["matchPercentage"]
        for target in result["documents"][0]["matches"][0]["targets"]
    ] == [100.0, 94.0]
