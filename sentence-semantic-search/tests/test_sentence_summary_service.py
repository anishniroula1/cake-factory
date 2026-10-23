from types import SimpleNamespace

from sentence_search.opensearch_store import sentence_eligibility_filters
from sentence_search.sentence_summary_service import SentenceSummaryService


class FakeOpenSearchStore:
    def __init__(self):
        self.config = SimpleNamespace(
            ivf_nprobes_enabled=True,
            ivf_nprobes=16,
        )
        self.msearch_calls = 0

    def application_occurrence_page(
        self,
        application_id,
        analysis_group,
        page_size,
        after_global_id,
        include_total,
    ):
        assert application_id == "A1"
        assert analysis_group == "Asylee"
        assert page_size == 100
        assert after_global_id is None
        assert include_total is True
        return {
            "sentences": [
                {
                    "globalId": 1,
                    "sectionName": "Affidavit",
                    "sentenceKey": "key-a",
                    "sentenceContent": "First sentence",
                },
                {
                    "globalId": 2,
                    "sectionName": "B1",
                    "sentenceKey": "key-b",
                    "sentenceContent": "Second sentence",
                },
            ],
            "nextAfterGlobalId": None,
            "totalSentences": 2,
        }

    def occurrence_counts_by_key(
        self,
        sentence_keys,
        excluded_application_id,
        analysis_group,
    ):
        assert excluded_application_id == "A1"
        assert analysis_group == "Asylee"
        all_counts = {"key-a": 2, "key-b": 1, "key-c": 4, "key-d": 5}
        return {key: all_counts[key] for key in sentence_keys}

    def catalog_vectors(self, sentence_keys, analysis_group):
        assert analysis_group == "Asylee"
        return {key: [1.0, 0.0] for key in sentence_keys}

    def multi_search_catalog(self, bodies, analysis_group):
        assert analysis_group == "Asylee"
        self.msearch_calls += 1
        responses = []
        for body in bodies:
            bool_query = body["query"]["bool"]
            assert bool_query["filter"] == sentence_eligibility_filters()
            source_key = bool_query["should"][0]["term"]["sentenceKey"]
            minimum_score = bool_query["should"][1]["knn"][
                "sentenceContentVector"
            ]["min_score"]
            hits = [self._bucket(source_key, 1.0)]
            if source_key == "key-a":
                hits.append(self._bucket("key-c", 0.96))
                if minimum_score <= 0.915:
                    hits.append(self._bucket("key-d", 0.915))
            responses.append(
                {
                    "aggregations": {
                        "matches": {
                            "buckets": hits,
                        }
                    }
                }
            )
        return responses

    @staticmethod
    def _bucket(sentence_key, score):
        return {
            "sample": {
                "hits": {
                    "hits": [
                        {
                            "_score": score,
                            "_source": {"sentenceKey": sentence_key},
                        }
                    ]
                }
            }
        }


def test_summary_calculates_counts_for_only_the_requested_page():
    opensearch = FakeOpenSearchStore()
    service = SentenceSummaryService(opensearch)

    result = service.application_summary("A1", "Asylee", 90, 100, None)

    assert opensearch.msearch_calls == 1
    assert result["summaryScope"] == "currentPage"
    assert result["totalSentences"] == 2
    assert result["totalMatching"] == 7
    assert result["sectionMatches"] == [
        {"sectionName": "Affidavit", "matchingCount": 6},
        {"sectionName": "B1", "matchingCount": 1},
    ]
    assert result["sentences"][0]["exactMatchCount"] == 2
    assert result["sentences"][0]["similarMatchCount"] == 4
    assert "pageTotalMatches" not in result
    assert result["neuralSearchUsed"] is False


def test_lower_threshold_adds_matches_to_the_current_page_count():
    service = SentenceSummaryService(FakeOpenSearchStore())

    result = service.application_summary("A1", "Asylee", 70, 100, None)

    assert result["thresholdPercentage"] == 70
    assert result["totalMatching"] == 12
    assert result["sectionMatches"] == [
        {"sectionName": "Affidavit", "matchingCount": 11},
        {"sectionName": "B1", "matchingCount": 1},
    ]
    assert result["sentences"][0]["similarMatchCount"] == 9


def test_matching_key_details_keep_raw_score_and_distance():
    service = SentenceSummaryService(FakeOpenSearchStore())

    details = service.matching_key_details(["key-a"], "Asylee", 90)

    assert details["key-a"]["key-a"]["matchType"] == "exact"
    assert details["key-a"]["key-c"] == {
        "matchPercentage": 92.0,
        "cosineDistance": 0.08,
        "openSearchScore": 0.96,
        "matchType": "similar",
    }
