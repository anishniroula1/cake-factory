import pytest
from pydantic import ValidationError

from sentence_search.main import app
from sentence_search.models import SentenceOccurrence, TracerSentence
from sentence_search.text import sentence_key


def sentence_values():
    return {
        "applicationId": "A0001",
        "tspId": "TSP-1",
        "sectionName": "Written Statement",
        "globalId": 1,
        "sentIdLocal": 1,
        "sentenceContent": "The United States Government issued a notice.",
        "isTracer": False,
        "isFormLanguage": False,
        "sentenceKey": None,
        "sourceType": "document",
        "createdAt": None,
        "updatedAt": None,
        "analysisGroup": "group-1",
    }


def test_sentence_generates_key_and_dates():
    sentence = SentenceOccurrence.model_validate(sentence_values())
    assert sentence.sentenceKey == sentence_key(sentence.sentenceContent)
    assert sentence.createdAt is not None
    assert sentence.updatedAt is not None
    assert sentence.globalId == 1
    assert "sentIdLocal" in sentence.model_dump()
    assert "local_global_id" not in sentence.model_dump()


def test_numeric_string_global_id_is_saved_as_an_integer():
    values = sentence_values()
    values["globalId"] = "123"

    sentence = SentenceOccurrence.model_validate(values)

    assert sentence.globalId == 123


def test_global_id_must_fit_in_opensearch_long():
    values = sentence_values()
    values["globalId"] = 2**63

    with pytest.raises(ValidationError, match="signed 64-bit long"):
        SentenceOccurrence.model_validate(values)


def test_wrong_supplied_key_is_rejected():
    values = sentence_values()
    values["sentenceKey"] = "wrong"
    with pytest.raises(ValidationError):
        SentenceOccurrence.model_validate(values)


def test_tracer_sentence_uses_numeric_id_and_has_no_embedding_input():
    tracer = TracerSentence.model_validate(
        {
            "tracerId": "42",
            "sentenceContent": "A known tracer sentence.",
            "analysisGroup": "Asylee",
            "isNonMatcher": False,
            "isFormLanguage": False,
        }
    )

    assert tracer.tracerId == 42
    assert "sentenceContentVector" not in tracer.model_dump()


def test_seed_swagger_uses_csv_path_without_index_reset():
    operation = app.openapi()["paths"]["/admin/seed"]["post"]
    parameters = {item["name"]: item for item in operation["parameters"]}

    assert "requestBody" not in operation
    assert set(parameters) == {"csvPath"}
    assert parameters["csvPath"]["schema"]["default"] == "data/seed.csv"


def test_tracer_seed_swagger_accepts_a_csv_file():
    operation = app.openapi()["paths"]["/admin/tracer-sentences/seed"]["post"]
    content = operation["requestBody"]["content"]

    assert "multipart/form-data" in content
    assert operation["requestBody"]["required"] is True


def test_api_does_not_create_opensearch_indexes():
    paths = app.openapi()["paths"]

    assert "/admin/init" not in paths
    assert "/admin/storage" in paths


def test_sentence_key_search_swagger_has_no_public_pagination():
    operation = app.openapi()["paths"][
        "/applications/{applicationId}/sentences/semantic-search"
    ]["get"]
    parameters = {item["name"] for item in operation["parameters"]}

    assert parameters == {
        "applicationId",
        "sentenceKey",
        "analysisGroup",
        "threshold",
    }
    threshold = next(
        item for item in operation["parameters"] if item["name"] == "threshold"
    )
    assert threshold["schema"]["default"] == 90
    assert threshold["schema"]["minimum"] == 1


def test_semantic_summary_swagger_accepts_any_threshold_percentage():
    operation = app.openapi()["paths"][
        "/applications/{applicationId}/sentences/semantic-summary"
    ]["get"]
    parameters = {item["name"]: item for item in operation["parameters"]}

    assert parameters["threshold"]["schema"]["default"] == 90
    assert parameters["threshold"]["schema"]["minimum"] == 1
    assert parameters["threshold"]["schema"]["maximum"] == 100
    assert parameters["pageSize"]["schema"]["maximum"] == 100


def test_all_summary_has_no_page_inputs():
    operation = app.openapi()["paths"][
        "/applications/{applicationId}/sentences/semantic-summary-all"
    ]["get"]
    parameters = {item["name"] for item in operation["parameters"]}

    assert parameters == {
        "applicationId",
        "analysisGroup",
        "threshold",
    }


def test_paginated_sentence_search_has_page_inputs():
    operation = app.openapi()["paths"][
        "/applications/{applicationId}/sentences/semantic-search-paginated"
    ]["get"]
    parameters = {item["name"] for item in operation["parameters"]}

    assert parameters == {
        "applicationId",
        "sentenceKey",
        "analysisGroup",
        "threshold",
        "pageSize",
        "nextToken",
    }


def test_application_matches_has_fixed_ten_application_pages():
    operation = app.openapi()["paths"][
        "/applications/{applicationId}/sentences/application-matches"
    ]["get"]
    parameters = {item["name"] for item in operation["parameters"]}

    assert parameters == {
        "applicationId",
        "analysisGroup",
        "threshold",
        "nextToken",
    }
