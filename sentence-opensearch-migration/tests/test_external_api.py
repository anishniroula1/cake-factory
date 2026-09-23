import json
from types import SimpleNamespace

import httpx
import pytest

from sentence_migration import external_api as external_api_module
from sentence_migration.external_api import ExternalSentenceApi, occurrence_from_api


def _config(**overrides):
    values = {
        "normalized_external_api_base_url": "https://api.example.test",
        "external_api_key": "secret-key",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_fetch_retries_failures_and_reads_next_page(monkeypatch):
    monkeypatch.setattr(external_api_module, "EXTERNAL_API_RETRY_ATTEMPTS", 3)
    monkeypatch.setattr(external_api_module, "EXTERNAL_API_RETRY_SECONDS", 0)
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(503, request=request)
        return httpx.Response(
            200,
            json={
                "data": {
                    "items": [{"globalId": 1}],
                    "nextPage": "cursor-2",
                }
            },
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    api = ExternalSentenceApi(_config(), client)

    page = api.fetch_sentence_page("TSP/1", 4)

    assert page.items == [{"globalId": 1}]
    assert page.next_page == "cursor-2"
    assert len(calls) == 3
    assert calls[-1].url.path == "/sentences"
    assert calls[-1].url.params["tspId"] == "TSP/1"
    assert calls[-1].url.params["page"] == "4"
    assert "pageSize" not in calls[-1].url.params
    assert calls[-1].headers["x-api-key"] == "secret-key"


def test_key_and_document_updates_send_expected_payloads():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204, request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    api = ExternalSentenceApi(_config(), client)
    record = occurrence_from_api(
        {
            "applicationId": "A1",
            "sectionName": "Statement",
            "globalId": 99,
            "sentIdLocal": 4,
            "sentenceContent": "Database sentence",
            "sentenceContentVector": [0.1, 0.2, 0.3],
            "isTracer": False,
            "isFormLanguage": False,
            "sourceType": "document",
            "analysisGroup": "Asylee",
        },
        "TSP/1",
    )

    api.update_sentence_keys("TSP/1", [record])
    api.update_document_state("TSP/1")

    assert requests[0].url.path == "/sentences/sentence-keys"
    assert json.loads(requests[0].content) == {
        "tspId": "TSP/1",
        "sentences": [{"globalId": 99, "sentenceKey": record.sentenceKey}],
    }
    assert requests[1].url.path == "/documents/state"
    assert json.loads(requests[1].content) == {
        "tspId": "TSP/1",
        "state": "OPENSEARCH_MIGRATED",
    }


def test_empty_204_source_page_stops_without_json_parsing():
    def handler(request):
        return httpx.Response(204, request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    api = ExternalSentenceApi(_config(), client)

    page = api.fetch_sentence_page("T1", 1)

    assert page.items == []
    assert page.next_page is None


def test_source_sentence_rejects_non_camel_case_fields():
    with pytest.raises(ValueError, match="must use camelCase.*global_id"):
        occurrence_from_api(
            {
                "applicationId": "A1",
                "sectionName": "Statement",
                "global_id": 99,
            },
            "T1",
        )


def test_source_sentence_requires_existing_vector():
    with pytest.raises(ValueError, match="sentenceContentVector"):
        occurrence_from_api(
            {
                "applicationId": "A1",
                "sectionName": "Statement",
                "globalId": 99,
                "sentIdLocal": 4,
                "sentenceContent": "Database sentence",
                "sourceType": "document",
                "analysisGroup": "Asylee",
            },
            "T1",
        )


def test_source_response_rejects_non_camel_case_envelope_fields(monkeypatch):
    monkeypatch.setattr(external_api_module, "EXTERNAL_API_RETRY_ATTEMPTS", 1)
    request = httpx.Request("GET", "https://api.example.test/sentences")

    def handler(_request):
        return httpx.Response(
            200,
            json={"sentence_items": []},
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    api = ExternalSentenceApi(_config(), client)

    with pytest.raises(RuntimeError, match="camelCase.*sentence_items"):
        api.fetch_sentence_page("T1", 1)
