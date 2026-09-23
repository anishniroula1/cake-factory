import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass

import httpx

from sentence_migration.config import (
    DOCUMENT_COMPLETED_STATE,
    DOCUMENT_STATE_ENDPOINT,
    EXTERNAL_API_KEY_HEADER,
    EXTERNAL_API_RETRY_ATTEMPTS,
    EXTERNAL_API_RETRY_SECONDS,
    EXTERNAL_API_TIMEOUT_SECONDS,
    SENTENCE_KEY_UPDATE_ENDPOINT,
    SENTENCE_SOURCE_ENDPOINT,
    Config,
)
from sentence_migration.models import SentenceOccurrence, SourceSentence

logger = logging.getLogger(__name__)

CAMEL_CASE_KEY = re.compile(r"^[a-z][A-Za-z0-9]*$")
SOURCE_SENTENCE_FIELDS = frozenset(SourceSentence.model_fields)
ITEM_KEYS = ("items", "sentences", "results", "content", "data")
RETRYABLE_API_ERRORS = (httpx.HTTPError, ValueError, TypeError, RuntimeError)


@dataclass(frozen=True)
class SentencePage:
    items: list[dict]
    next_page: int | str | None


def require_camel_case_keys(value: Mapping, label: str) -> None:
    invalid_keys = [
        str(key)
        for key in value
        if not isinstance(key, str) or not CAMEL_CASE_KEY.fullmatch(key)
    ]
    if invalid_keys:
        raise ValueError(
            f"{label} fields must use camelCase; invalid field(s): "
            + ", ".join(sorted(invalid_keys))
        )


def occurrence_from_api(row: Mapping, expected_tsp_id: str) -> SourceSentence:
    """Convert one external API row to the semantic-search occurrence shape."""

    require_camel_case_keys(row, "Source API sentence")
    values = {field: row[field] for field in SOURCE_SENTENCE_FIELDS if field in row}
    values.setdefault("tspId", expected_tsp_id)
    values["sentenceKey"] = None
    if str(values["tspId"]) != expected_tsp_id:
        raise ValueError(
            f"Source API returned tspId {values['tspId']!r} while processing "
            f"{expected_tsp_id!r}"
        )
    try:
        return SourceSentence.model_validate(values)
    except ValueError as error:
        global_id = row.get("globalId", "unknown")
        raise ValueError(
            f"Invalid source API sentence for globalId {global_id}: {error}"
        ) from error


class ExternalSentenceApi:
    def __init__(self, config: Config, client: httpx.Client | None = None):
        self.config = config
        self._owns_client = client is None
        if client is None:
            self.client = httpx.Client(
                headers=self._headers(),
                timeout=EXTERNAL_API_TIMEOUT_SECONDS,
            )
        else:
            self.client = client
            self.client.headers.update(self._headers())

    def close(self):
        if self._owns_client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def fetch_sentence_page(
        self,
        tsp_id: str,
        page: int | str,
    ) -> SentencePage:
        """Read and validate one paginated sentence response."""

        url = self._url(SENTENCE_SOURCE_ENDPOINT)
        params = {"tspId": tsp_id, "page": page}

        def request_page():
            response = self.client.get(url, params=params)
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return SentencePage(items=[], next_page=None)
            return self._extract_page(response.json())

        return self._retry(
            f"fetch sentences for TSP {tsp_id}, page {page}", request_page
        )

    def update_sentence_keys(
        self,
        tsp_id: str,
        records: list[SentenceOccurrence],
    ) -> None:
        """Save one successfully indexed page's global ID/key pairs."""

        url = self._url(SENTENCE_KEY_UPDATE_ENDPOINT)
        payload = {
            "tspId": tsp_id,
            "sentences": [
                {
                    "globalId": record.globalId,
                    "sentenceKey": record.sentenceKey,
                }
                for record in records
            ],
        }

        def update_keys():
            response = self.client.post(url, json=payload)
            response.raise_for_status()

        self._retry(f"update sentence keys for TSP {tsp_id}", update_keys)

    def update_document_state(self, tsp_id: str) -> None:
        """Mark one fully migrated TSP document complete."""

        url = self._url(DOCUMENT_STATE_ENDPOINT)
        payload = {
            "tspId": tsp_id,
            "state": DOCUMENT_COMPLETED_STATE,
        }

        def update_state():
            response = self.client.post(url, json=payload)
            response.raise_for_status()

        self._retry(f"update document state for TSP {tsp_id}", update_state)

    def _retry(self, label: str, operation):
        last_error = None
        for attempt in range(1, EXTERNAL_API_RETRY_ATTEMPTS + 1):
            try:
                return operation()
            except RETRYABLE_API_ERRORS as error:
                last_error = error
                if attempt == EXTERNAL_API_RETRY_ATTEMPTS:
                    break
                logger.warning(
                    "External API call failed (%s), attempt %s/%s; retrying in %s "
                    "seconds: %s",
                    label,
                    attempt,
                    EXTERNAL_API_RETRY_ATTEMPTS,
                    EXTERNAL_API_RETRY_SECONDS,
                    error,
                )
                time.sleep(EXTERNAL_API_RETRY_SECONDS)
        raise RuntimeError(
            f"External API call failed after "
            f"{EXTERNAL_API_RETRY_ATTEMPTS} attempts: {label}: {last_error}"
        ) from last_error

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json"}
        if self.config.external_api_key:
            headers[EXTERNAL_API_KEY_HEADER] = self.config.external_api_key
        return headers

    def _url(self, endpoint: str) -> str:
        return f"{self.config.normalized_external_api_base_url}{endpoint}"

    @classmethod
    def _extract_page(cls, payload) -> SentencePage:
        if payload is None or payload == {}:
            return SentencePage(items=[], next_page=None)
        if isinstance(payload, list):
            items = payload
            next_page = None
        elif isinstance(payload, dict):
            require_camel_case_keys(payload, "Source API response")
            items = None
            next_page = cls._next_page(payload.get("nextPage"))
            for key in ITEM_KEYS:
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    items = candidate
                    break
                if isinstance(candidate, dict):
                    try:
                        nested_page = cls._extract_page(candidate)
                    except (TypeError, ValueError):
                        continue
                    items = nested_page.items
                    if next_page is None:
                        next_page = nested_page.next_page
                    break
            if items is None:
                raise ValueError(
                    "Source API response must be a list or contain an items, "
                    "sentences, results, content, or data list"
                )
        else:
            raise TypeError("Source API response must be a JSON list or object")

        if not all(isinstance(item, dict) for item in items):
            raise ValueError("Every source API sentence must be a JSON object")
        return SentencePage(items=items, next_page=next_page)

    @staticmethod
    def _next_page(value) -> int | str | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError("Source API nextPage must be a string, integer, or null")
        if isinstance(value, str) and not value.strip():
            raise ValueError("Source API nextPage cannot be blank")
        return value
