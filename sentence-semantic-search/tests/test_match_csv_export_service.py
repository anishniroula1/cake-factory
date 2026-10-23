import csv

from sentence_search.match_csv_export_service import CSV_FIELDS, MatchCsvExportService

SOURCE_KEY = "a" * 64
SIMILAR_KEY = "b" * 64


def sentence(application_id, tsp_id, global_id, sentence_key, text):
    return {
        "applicationId": application_id,
        "tspId": tsp_id,
        "sectionName": "Affidavit",
        "globalId": global_id,
        "sentIdLocal": global_id,
        "sentenceContent": text,
        "sentenceKey": sentence_key,
        "sourceType": "affidavit",
        "analysisGroup": "Asylee",
        "wordCount": 7,
        "isNonMatcher": False,
        "isTracer": False,
        "isFormLanguage": False,
    }


class FakeSummaryService:
    def matching_key_details(self, source_keys, analysis_group, threshold):
        assert source_keys == [SOURCE_KEY]
        assert analysis_group == "Asylee"
        assert threshold == 90
        return {
            SOURCE_KEY: {
                SOURCE_KEY: {
                    "matchPercentage": 100.0,
                    "cosineDistance": 0.0,
                    "openSearchScore": 1.0,
                    "matchType": "exact",
                },
                SIMILAR_KEY: {
                    "matchPercentage": 92.0,
                    "cosineDistance": 0.08,
                    "openSearchScore": 0.96,
                    "matchType": "similar",
                },
            }
        }


class FakeOpenSearchStore:
    def __init__(self):
        self.source = sentence("101", "501", 1, SOURCE_KEY, "Source sentence")
        self.exclusions = []

    def application_occurrence_page(
        self,
        application_id,
        analysis_group,
        page_size,
        after_global_id,
        include_total,
    ):
        assert application_id == "101"
        assert analysis_group == "Asylee"
        assert after_global_id is None
        assert include_total is False
        return {"sentences": [self.source], "nextAfterGlobalId": None}

    def tsp_occurrence_page(
        self,
        tsp_id,
        analysis_group,
        page_size,
        after_global_id,
    ):
        assert tsp_id == "501"
        assert analysis_group == "Asylee"
        assert after_global_id is None
        return {"sentences": [self.source], "nextAfterGlobalId": None}

    def matching_occurrence_page(
        self,
        sentence_keys,
        excluded_application_id,
        analysis_group,
        page_size,
        after_global_id,
        include_total,
        excluded_tsp_id=None,
    ):
        assert sentence_keys == [SOURCE_KEY, SIMILAR_KEY]
        assert analysis_group == "Asylee"
        assert include_total is False
        self.exclusions.append((excluded_application_id, excluded_tsp_id))
        if after_global_id is None:
            return {
                "matches": [
                    sentence(
                        "303",
                        "701",
                        3,
                        SIMILAR_KEY,
                        "Similar sentence",
                    ),
                ],
                "nextAfterGlobalId": 3,
                "totalMatches": None,
            }
        assert after_global_id == 3
        return {
            "matches": [
                sentence("202", "601", 2, SOURCE_KEY, "Source sentence"),
            ],
            "nextAfterGlobalId": None,
            "totalMatches": None,
        }


def read_rows(export):
    try:
        with export.path.open(encoding="utf-8", newline="") as csv_file:
            return list(csv.DictReader(csv_file))
    finally:
        export.path.unlink(missing_ok=True)


def test_application_export_contains_every_match_and_score_field():
    opensearch = FakeOpenSearchStore()
    service = MatchCsvExportService(opensearch, FakeSummaryService())

    export = service.create_application_csv("101", "Asylee", 90)
    rows = read_rows(export)

    assert export.file_name == "application_id-101.csv"
    assert export.match_count == 2
    assert list(rows[0]) == CSV_FIELDS
    assert [row["match_type"] for row in rows] == ["exact", "similar"]
    assert rows[1]["match_percentage"] == "92.0"
    assert rows[1]["distance"] == "0.08"
    assert rows[1]["opensearch_score"] == "0.96"
    assert rows[1]["source_global_id"] == "1"
    assert rows[1]["target_global_id"] == "3"
    assert opensearch.exclusions == [("101", None), ("101", None)]


def test_tsp_export_uses_the_requested_download_name():
    opensearch = FakeOpenSearchStore()
    service = MatchCsvExportService(opensearch, FakeSummaryService())

    export = service.create_tsp_csv("501", "Asylee", 90)
    rows = read_rows(export)

    assert export.file_name == "tsp_id-501.csv"
    assert len(rows) == 2
    assert opensearch.exclusions == [(None, "501"), (None, "501")]
