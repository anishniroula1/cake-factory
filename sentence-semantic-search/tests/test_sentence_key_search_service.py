import pytest

from sentence_search.sentence_key_search_service import SentenceKeySearchService

SOURCE_KEY = "a" * 64
SIMILAR_KEY = "b" * 64
LOW_SCORE_KEY = "c" * 64


class FakeOpenSearchStore:
    def __init__(self):
        self.search_keys = None
        self.source_is_eligible = True

    def eligible_sentence_exists(
        self,
        sentence_key,
        analysis_group,
    ):
        assert sentence_key == SOURCE_KEY
        assert analysis_group == "Asylee"
        return self.source_is_eligible

    def catalog_vector(self, sentence_key):
        assert sentence_key == SOURCE_KEY
        return [1.0, 0.0]

    def catalog_matches(self, sentence_key, vector, threshold):
        assert sentence_key == SOURCE_KEY
        assert vector == [1.0, 0.0]
        return [
            {
                "sentenceKey": SOURCE_KEY,
                "similarityPercentage": 100.0,
            },
            {
                "sentenceKey": SIMILAR_KEY,
                "similarityPercentage": 92.0,
            },
            {
                "sentenceKey": LOW_SCORE_KEY,
                "similarityPercentage": 83.0,
            },
        ]

    def matching_occurrences(
        self,
        sentence_keys,
        excluded_application_id,
        analysis_group,
    ):
        self.search_keys = sentence_keys
        assert excluded_application_id == "A1"
        assert analysis_group == "Asylee"
        occurrences = [
            {"globalId": 11, "sentenceKey": SIMILAR_KEY},
            {"globalId": 10, "sentenceKey": SOURCE_KEY},
            {"globalId": 12, "sentenceKey": LOW_SCORE_KEY},
        ]
        return [
            occurrence
            for occurrence in occurrences
            if occurrence["sentenceKey"] in sentence_keys
        ]


def test_sentence_key_search_runs_live_and_applies_threshold():
    opensearch = FakeOpenSearchStore()
    service = SentenceKeySearchService(opensearch)

    result = service.search("A1", SOURCE_KEY, "Asylee", 90)

    assert opensearch.search_keys == [SOURCE_KEY, SIMILAR_KEY]
    assert result["exactMatchCount"] == 1
    assert result["similarMatchCount"] == 1
    assert result["totalMatches"] == 2
    assert result["matches"][0]["matchType"] == "exact"
    assert result["matches"][0]["matchPercentage"] == 100.0
    assert result["matches"][1]["matchType"] == "similar"
    assert result["matches"][1]["matchPercentage"] == 92.0
    assert [match["globalId"] for match in result["matches"]] == [10, 11]
    assert result["thresholdPercentage"] == 90
    assert result["matchingCountScope"] == "allResults"
    assert result["directMatchingKeyCount"] == 1
    assert "pagination" not in result
    assert result["neuralSearchUsed"] is False


def test_higher_query_threshold_removes_lower_score_matches():
    opensearch = FakeOpenSearchStore()
    service = SentenceKeySearchService(opensearch)

    result = service.search("A1", SOURCE_KEY, "Asylee", 95)

    assert opensearch.search_keys == [SOURCE_KEY]
    assert result["directMatchingKeyCount"] == 0
    assert result["similarMatchCount"] == 0


def test_lower_query_threshold_returns_more_live_matches():
    opensearch = FakeOpenSearchStore()
    service = SentenceKeySearchService(opensearch)

    result = service.search("A1", SOURCE_KEY, "Asylee", 70)

    assert opensearch.search_keys == [SOURCE_KEY, SIMILAR_KEY, LOW_SCORE_KEY]
    assert result["directMatchingKeyCount"] == 2
    assert result["similarMatchCount"] == 2
    assert {match["matchPercentage"] for match in result["matches"]} == {
        100.0,
        92.0,
        83.0,
    }


def test_sentence_key_search_rejects_an_ineligible_source_sentence():
    opensearch = FakeOpenSearchStore()
    opensearch.source_is_eligible = False
    service = SentenceKeySearchService(opensearch)

    with pytest.raises(ValueError, match="not an eligible sentence"):
        service.search("A1", SOURCE_KEY, "Asylee", 90)
