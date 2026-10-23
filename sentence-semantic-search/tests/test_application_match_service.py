from sentence_search.application_match_service import ApplicationMatchService


class FakeSentenceSummaryService:
    def __init__(self, scores):
        self.scores = scores

    def matching_key_scores(self, source_keys, analysis_group, threshold):
        assert sorted(source_keys) == sorted(self.scores)
        assert analysis_group == "Asylee"
        return self.scores


class FakeApplicationMatchStore:
    def __init__(self, source_sentences, count_rows, target_sentences):
        self.source_sentences = source_sentences
        self.count_rows = count_rows
        self.target_sentences = target_sentences
        self.selected_application_ids = None

    def application_occurrence_page(
        self,
        application_id,
        analysis_group,
        page_size,
        after_global_id,
        include_total,
    ):
        assert application_id == "A1234"
        assert analysis_group == "Asylee"
        assert after_global_id is None
        assert include_total is False
        return {
            "sentences": self.source_sentences,
            "nextAfterGlobalId": None,
            "totalSentences": None,
        }

    def occurrence_counts_by_application_and_key(
        self,
        sentence_keys,
        excluded_application_id,
        analysis_group,
    ):
        assert excluded_application_id == "A1234"
        assert analysis_group == "Asylee"
        assert {
            row["sentenceKey"] for row in self.count_rows
        }.issubset(set(sentence_keys))
        return self.count_rows

    def matching_occurrences_for_applications(
        self,
        sentence_keys,
        application_ids,
        excluded_application_id,
        analysis_group,
    ):
        self.selected_application_ids = application_ids
        selected = set(application_ids)
        selected_keys = set(sentence_keys)
        return [
            sentence
            for sentence in self.target_sentences
            if str(sentence["applicationId"]) in selected
            and sentence["sentenceKey"] in selected_keys
        ]


def _sentence(application_id, global_id, sentence_key, text):
    return {
        "applicationId": application_id,
        "tspId": f"T-{application_id}",
        "sectionName": "Affidavit",
        "globalId": global_id,
        "sentIdLocal": global_id,
        "sentenceContent": text,
        "sentenceKey": sentence_key,
        "sourceType": "document",
        "analysisGroup": "Asylee",
    }


def test_application_match_count_is_every_source_target_pair():
    source_key = "source-key"
    similar_key = "similar-key"
    source = _sentence("A1234", 20, source_key, "Source sentence")
    targets = [
        _sentence("A456", 1, similar_key, "Target one"),
        _sentence("A456", 40, similar_key, "Target forty"),
        _sentence("A456", 6, similar_key, "Target six"),
        _sentence("A789", 80, source_key, "Source sentence"),
    ]
    count_rows = [
        {
            "applicationId": "A456",
            "sentenceKey": similar_key,
            "occurrenceCount": 3,
        },
        {
            "applicationId": "A789",
            "sentenceKey": source_key,
            "occurrenceCount": 1,
        },
    ]
    store = FakeApplicationMatchStore([source], count_rows, targets)
    summary = FakeSentenceSummaryService(
        {source_key: {source_key: 100.0, similar_key: 94.0}}
    )
    service = ApplicationMatchService(store, summary)

    result = service.compare("A1234", "Asylee", 90)

    assert result["totalMatchingApplications"] == 2
    assert result["totalMatchCount"] == 4
    assert result["applications"][0]["applicationId"] == "A456"
    assert result["applications"][0]["matchCount"] == 3
    assert result["applications"][0]["similarMatchCount"] == 3
    source_match = result["applications"][0]["matches"][0]
    assert source_match["targetCount"] == 3
    assert [target["target"]["globalId"] for target in source_match["targets"]] == [
        1,
        6,
        40,
    ]
    assert result["applications"][1]["applicationId"] == "A789"
    assert result["applications"][1]["exactMatchCount"] == 1


def test_numeric_occurrence_application_id_joins_to_aggregation_count():
    source_key = "source-key"
    target_key = "target-key"
    source = _sentence("A1234", 20, source_key, "Source sentence")
    targets = [
        _sentence(456, 1, target_key, "Similar target sentence"),
        _sentence(456, 2, source_key, "Exact target sentence"),
    ]
    count_rows = [
        {
            "applicationId": "456",
            "sentenceKey": target_key,
            "occurrenceCount": 1,
        },
        {
            "applicationId": "456",
            "sentenceKey": source_key,
            "occurrenceCount": 1,
        },
    ]
    store = FakeApplicationMatchStore([source], count_rows, targets)
    summary = FakeSentenceSummaryService(
        {source_key: {source_key: 100.0, target_key: 94.0}}
    )

    result = ApplicationMatchService(store, summary).compare(
        "A1234",
        "Asylee",
        90,
    )

    application = result["applications"][0]
    assert application["matchCount"] == 2
    assert [
        target["matchPercentage"]
        for target in application["matches"][0]["targets"]
    ] == [
        100.0,
        94.0,
    ]


def test_application_match_returns_every_ranked_application():
    source_key = "source-key"
    target_key = "target-key"
    source = _sentence("A1234", 20, source_key, "Source sentence")
    count_rows = []
    targets = []
    next_global_id = 100
    for position in range(1, 12):
        application_id = f"A{position:02d}"
        occurrence_count = 12 - position
        count_rows.append(
            {
                "applicationId": application_id,
                "sentenceKey": target_key,
                "occurrenceCount": occurrence_count,
            }
        )
        for local_id in range(occurrence_count):
            targets.append(
                _sentence(
                    application_id,
                    next_global_id,
                    target_key,
                    f"Target {position}-{local_id}",
                )
            )
            next_global_id += 1

    store = FakeApplicationMatchStore([source], count_rows, targets)
    summary = FakeSentenceSummaryService(
        {source_key: {source_key: 100.0, target_key: 95.0}}
    )
    service = ApplicationMatchService(store, summary)

    result = service.compare("A1234", "Asylee", 90)

    assert [item["applicationId"] for item in result["applications"]] == [
        "A01",
        "A02",
        "A03",
        "A04",
        "A05",
        "A06",
        "A07",
        "A08",
        "A09",
        "A10",
        "A11",
    ]
    assert result["returnedApplications"] == 11
    assert "pagination" not in result
    assert "nextToken" not in result
