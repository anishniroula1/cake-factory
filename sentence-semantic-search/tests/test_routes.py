from sentence_search.main import app


def test_api_does_not_create_opensearch_indexes():
    paths = app.openapi()["paths"]

    assert "/admin/init" not in paths
    assert "/admin/seed" not in paths
    assert "/admin/storage" not in paths
    assert all(
        method not in path_details
        for path_details in paths.values()
        for method in {"post", "put", "patch", "delete"}
    )


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


def test_tsp_matches_has_fixed_ten_application_pages():
    operation = app.openapi()["paths"][
        "/documents/{tspId}/sentences/application-matches"
    ]["get"]
    parameters = {item["name"] for item in operation["parameters"]}

    assert parameters == {
        "tspId",
        "analysisGroup",
        "threshold",
        "nextToken",
    }


def test_csv_download_endpoints_take_analysis_group_and_threshold():
    paths = app.openapi()["paths"]
    expected = {
        "/documents/{tspId}/sentences/matches.csv": "tspId",
        "/applications/{applicationId}/sentences/matches.csv": "applicationId",
    }

    for path, identifier in expected.items():
        parameters = {
            item["name"]: item for item in paths[path]["get"]["parameters"]
        }
        assert set(parameters) == {identifier, "analysisGroup", "threshold"}
        assert parameters["threshold"]["schema"]["default"] == 90
