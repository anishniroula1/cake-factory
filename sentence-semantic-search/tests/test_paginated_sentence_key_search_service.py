from sentence_search.paginated_sentence_key_search_service import (
    PaginatedSentenceKeySearchService,
)
from sentence_search.sentence_key_search_service import SentenceKeySearchService

SOURCE_KEY = "a" * 64
SIMILAR_KEY_B = "b" * 64
SIMILAR_KEY_C = "c" * 64
SIMILAR_KEY_D = "d" * 64


class FakeOpenSearchStore:
    def __init__(self):
        self.catalog_page_requests = []
        self.occurrence_requests = []

    def eligible_sentence_exists(
        self,
        sentence_key,
        analysis_group,
    ):
        return (
            sentence_key == SOURCE_KEY and analysis_group == "Asylee"
        )

    def catalog_vector(self, sentence_key, analysis_group):
        assert sentence_key == SOURCE_KEY
        assert analysis_group == "Asylee"
        return [1.0, 0.0]

    def catalog_match_page(
        self,
        sentence_key,
        vector,
        threshold,
        page_size,
        after_key,
        analysis_group,
    ):
        assert sentence_key == SOURCE_KEY
        assert vector == [1.0, 0.0]
        assert threshold == 90
        assert analysis_group == "Asylee"
        self.catalog_page_requests.append(
            {"pageSize": page_size, "afterKey": after_key}
        )
        if after_key is None:
            return {
                "matches": [
                    self._candidate(SOURCE_KEY, 100.0, "exact"),
                    self._candidate(SIMILAR_KEY_B, 94.0, "similar"),
                ],
                "nextAfterKey": {"sentenceKey": SIMILAR_KEY_B},
            }
        if after_key == {"sentenceKey": SIMILAR_KEY_B}:
            return {
                "matches": [
                    self._candidate(SIMILAR_KEY_C, 96.0, "similar"),
                ],
                "nextAfterKey": {"sentenceKey": SIMILAR_KEY_C},
            }
        return {
            "matches": [
                self._candidate(SIMILAR_KEY_D, 91.0, "similar"),
            ],
            "nextAfterKey": None,
        }

    def matching_occurrences(
        self,
        sentence_keys,
        excluded_application_id,
        analysis_group,
    ):
        assert excluded_application_id == "A1"
        assert analysis_group == "Asylee"
        self.occurrence_requests.append(sentence_keys)
        occurrences = {
            SOURCE_KEY: [],
            SIMILAR_KEY_B: [
                {"globalId": 10, "sentenceKey": SIMILAR_KEY_B},
                {"globalId": 11, "sentenceKey": SIMILAR_KEY_B},
            ],
            SIMILAR_KEY_C: [
                {"globalId": 12, "sentenceKey": SIMILAR_KEY_C},
            ],
            SIMILAR_KEY_D: [
                {"globalId": 13, "sentenceKey": SIMILAR_KEY_D},
            ],
        }
        return [
            occurrence
            for key in sentence_keys
            for occurrence in occurrences[key]
        ]

    @staticmethod
    def _candidate(sentence_key, percentage, match_type):
        return {
            "sentenceKey": sentence_key,
            "sentenceContent": f"Sentence for {sentence_key[0]}",
            "similarityPercentage": percentage,
            "matchType": match_type,
        }


def test_paginated_sentence_search_pages_catalog_and_returns_all_occurrences():
    opensearch = FakeOpenSearchStore()
    shared_search = SentenceKeySearchService(opensearch)
    service = PaginatedSentenceKeySearchService(opensearch, shared_search)

    first = service.search("A1", SOURCE_KEY, "Asylee", 90, 2, None)
    second = service.search(
        "A1",
        SOURCE_KEY,
        "Asylee",
        90,
        2,
        first["nextToken"],
    )

    # The exact catalog key had no occurrence outside A1, so the service
    # continued scanning until it filled the first page with eligible keys.
    assert opensearch.catalog_page_requests == [
        {"pageSize": 2, "afterKey": None},
        {"pageSize": 1, "afterKey": {"sentenceKey": SIMILAR_KEY_B}},
        {"pageSize": 2, "afterKey": {"sentenceKey": SIMILAR_KEY_C}},
    ]
    assert first["matchingCountScope"] == "currentCatalogPageOccurrences"
    assert first["returnedCatalogMatches"] == 2
    assert first["totalMatches"] == 3
    assert first["returnedMatches"] == 3
    assert [match["globalId"] for match in first["matches"]] == [12, 10, 11]
    assert [match["matchPercentage"] for match in first["matches"]] == [
        96.0,
        94.0,
        94.0,
    ]
    assert all("occurrences" not in match for match in first["matches"])
    assert all("occurrenceCount" not in match for match in first["matches"])
    assert first["pagination"]["hasNextPage"] is True

    assert second["totalMatches"] == 1
    assert second["returnedCatalogMatches"] == 1
    assert second["matches"][0]["sentenceKey"] == SIMILAR_KEY_D
    assert second["pagination"]["page"] == 2
    assert second["pagination"]["hasNextPage"] is False
