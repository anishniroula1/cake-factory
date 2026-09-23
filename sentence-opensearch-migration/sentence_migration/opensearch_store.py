import logging
import threading
import time

from opensearchpy import TransportError, helpers
from opensearchpy.helpers.errors import BulkIndexError

from sentence_migration.config import (
    BULK_RETRY_ATTEMPTS,
    BULK_RETRY_SECONDS,
    BULK_RETRY_STATUSES,
    Config,
)

logger = logging.getLogger(__name__)


class OpenSearchMigrationStore:
    def __init__(self, config: Config, client):
        self.config = config
        self.client = client
        self._catalog_backoff_lock = threading.Lock()
        self._catalog_cooldown_until = 0.0

    def require_existing_indices(self, occurrence_index: str, catalog_index: str):
        """Fail before bulk indexing so auto-index creation cannot occur."""

        missing = [
            index
            for index in (occurrence_index, catalog_index)
            if not self.client.indices.exists(index=index)
        ]
        if missing:
            raise ValueError(
                "OpenSearch index or alias does not exist; this app never creates "
                "indexes: " + ", ".join(missing)
            )

    def existing_catalog_keys(self, catalog_index: str, keys: list[str]) -> set[str]:
        existing = set()
        for offset in range(0, len(keys), 1_000):
            batch = keys[offset : offset + 1_000]
            response = self.client.mget(
                index=catalog_index,
                body={"ids": batch},
                params={"_source": "false"},
            )
            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        return existing

    def bulk_index_catalog(self, catalog_index: str, documents: list[dict]) -> int:
        actions = [
            {
                "_op_type": "index",
                "_index": catalog_index,
                "_id": document["sentenceKey"],
                "_source": document,
            }
            for document in documents
        ]
        self._bulk_with_retry(
            actions,
            "catalog",
            shared_catalog_cooldown=True,
        )
        return len(actions)

    def bulk_index_occurrences(
        self,
        occurrence_index: str,
        documents: list[dict],
    ) -> int:
        actions = [
            {
                "_op_type": "index",
                "_index": occurrence_index,
                "_id": str(document["globalId"]),
                "_source": document,
            }
            for document in documents
        ]
        self._bulk_with_retry(actions, "occurrence")
        return len(actions)

    def refresh_indices(self, occurrence_index: str, catalog_index: str):
        self.client.indices.refresh(index=f"{catalog_index},{occurrence_index}")

    def _bulk_with_retry(
        self,
        actions: list[dict],
        label: str,
        shared_catalog_cooldown: bool = False,
    ):
        if not actions:
            return
        pending_actions = actions
        last_error = None
        for attempt in range(1, BULK_RETRY_ATTEMPTS + 1):
            if shared_catalog_cooldown:
                self._wait_for_catalog_cooldown()
            try:
                helpers.bulk(
                    self.client,
                    pending_actions,
                    raise_on_error=True,
                )
                return
            except (BulkIndexError, TransportError) as error:
                last_error = error
                retry_actions = self._retryable_actions(error, pending_actions)
                if retry_actions is None:
                    raise RuntimeError(
                        f"Non-retryable OpenSearch {label} bulk failure: {error}"
                    ) from error
                if attempt == BULK_RETRY_ATTEMPTS:
                    break
                pending_actions = retry_actions
                logger.warning(
                    "OpenSearch %s batch attempt %s/%s failed; retrying %s "
                    "document(s) after %s seconds",
                    label,
                    attempt,
                    BULK_RETRY_ATTEMPTS,
                    len(pending_actions),
                    BULK_RETRY_SECONDS,
                )
                if shared_catalog_cooldown:
                    self._start_catalog_cooldown()
                else:
                    time.sleep(BULK_RETRY_SECONDS)

        raise RuntimeError(
            f"OpenSearch {label} bulk failed after "
            f"{BULK_RETRY_ATTEMPTS} attempts: {last_error}"
        ) from last_error

    def _start_catalog_cooldown(self):
        with self._catalog_backoff_lock:
            self._catalog_cooldown_until = max(
                self._catalog_cooldown_until,
                time.monotonic() + BULK_RETRY_SECONDS,
            )

    def _wait_for_catalog_cooldown(self):
        while True:
            with self._catalog_backoff_lock:
                remaining = self._catalog_cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(remaining)

    def _retryable_actions(self, error, actions: list[dict]) -> list[dict] | None:
        if isinstance(error, TransportError):
            status = getattr(error, "status_code", None)
            if status == "N/A":
                return actions
            try:
                status = int(status)
            except (TypeError, ValueError):
                return None
            return actions if status in BULK_RETRY_STATUSES else None

        failed_ids = set()
        for item in error.errors:
            operation = next(iter(item.values()))
            try:
                status = int(operation.get("status"))
            except (TypeError, ValueError):
                return None
            failed_id = operation.get("_id")
            if status not in BULK_RETRY_STATUSES or failed_id is None:
                return None
            failed_ids.add(str(failed_id))
        retry_actions = [
            action for action in actions if str(action.get("_id")) in failed_ids
        ]
        retry_ids = {str(action.get("_id")) for action in retry_actions}
        return retry_actions if failed_ids and retry_ids == failed_ids else None
