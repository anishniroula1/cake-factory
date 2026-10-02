import threading
import time

from opensearchpy import OpenSearch, TransportError, helpers

from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import OpenSearchDocument
from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.errors import PermanentDataError

# These failures are normally temporary problems from OpenSearch, Lasso,
# Bedrock, or a gateway in front of them.
RETRYABLE_STATUS_CODES = {408, 424, 425, 429, 500, 502, 503, 504}


class OpenSearchHelper:
    def __init__(
        self,
        config: Config,
        client: OpenSearch,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep the small amount of OpenSearch code shared by all handlers."""

        self.config = config
        self.client = client
        self.logger = custom_logger
        self.cooldown_lock = threading.Lock()
        self.embedding_cooldown_until = 0.0

    def existing_ids(self, alias: str, document_ids: list[str]) -> set[str]:
        """Return document IDs that already exist in an OpenSearch alias."""

        existing: set[str] = set()
        unique_ids = list(dict.fromkeys(document_ids))
        if not unique_ids:
            self.logger.info(
                f"Skipped the OpenSearch lookup in {alias} because no document "
                "IDs were provided."
            )
            return existing

        self.logger.info(
            f"Checking {len(unique_ids)} unique document ID(s) in OpenSearch "
            f"alias {alias}."
        )
        for offset in range(0, len(unique_ids), self.config.opensearch_batch_size):
            batch = unique_ids[offset : offset + self.config.opensearch_batch_size]
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = self.client.mget(
                        index=alias,
                        body={"ids": batch},
                        params={"_source": "false"},
                    )
                    break
                except TransportError as error:
                    if not self._is_retryable(error):
                        message = (
                            f"OpenSearch lookup in alias {alias} failed permanently: "
                            f"{str(error)[:500]}"
                        )
                        self.logger.error(message)
                        raise PermanentDataError(message) from error
                    self.logger.warning(
                        f"OpenSearch lookup in alias {alias} failed on attempt "
                        f"{attempt} for {len(batch)} document ID(s). Retrying in "
                        f"{self.config.retry_seconds} seconds."
                    )
                    time.sleep(self.config.retry_seconds)

            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        self.logger.info(
            f"OpenSearch lookup in alias {alias} completed. Found "
            f"{len(existing)} existing document(s)."
        )
        return existing

    def bulk_index(
        self,
        alias: str,
        documents: list[OpenSearchDocument],
        needs_embedding: bool,
    ) -> int:
        """Index documents and keep retrying temporary failed items.

        Every document must contain an internal `_id`. The helper removes that
        value from `_source` before sending the document to OpenSearch.
        """

        if not documents:
            self.logger.info(
                f"Skipped the OpenSearch bulk write to {alias} because there "
                "were no documents to index."
            )
            return 0

        vector_source = (
            "the Bedrock embedding pipeline"
            if needs_embedding
            else "vectors already stored in the documents"
        )
        self.logger.info(
            f"Starting an OpenSearch bulk write of {len(documents)} document(s) "
            f"to {alias} using {vector_source}."
        )
        pending = documents
        attempt = 0
        while pending:
            attempt += 1
            if needs_embedding:
                self._wait_for_embedding_cooldown()

            actions = []
            for document in pending:
                actions.append(
                    {
                        "_op_type": "index",
                        "_index": alias,
                        "_id": str(document["_id"]),
                        "_source": {
                            key: value
                            for key, value in document.items()
                            if key != "_id"
                        },
                    }
                )

            bulk_arguments = {
                "chunk_size": self.config.opensearch_batch_size,
                "raise_on_error": False,
                "raise_on_exception": True,
            }
            if not needs_embedding:
                # This prevents an index default pipeline from replacing an
                # existing vector or calling Bedrock unnecessarily.
                bulk_arguments["pipeline"] = "_none"

            try:
                _, failures = helpers.bulk(
                    self.client,
                    actions,
                    **bulk_arguments,
                )
            except TransportError as error:
                if not self._is_retryable(error):
                    message = (
                        f"OpenSearch bulk write to {alias} failed permanently: "
                        f"{str(error)[:500]}"
                    )
                    self.logger.error(message)
                    raise PermanentDataError(message) from error
                self._pause_before_retry(alias, attempt, len(pending), needs_embedding)
                continue

            if not failures:
                self.logger.info(
                    f"OpenSearch bulk write to {alias} completed. Indexed "
                    f"{len(documents)} document(s)."
                )
                return len(documents)

            try:
                pending = self._retryable_documents(failures, pending)
            except PermanentDataError as error:
                self.logger.error(
                    f"OpenSearch bulk write to {alias} failed permanently: "
                    f"{str(error)[:500]}"
                )
                raise
            self._pause_before_retry(alias, attempt, len(pending), needs_embedding)

        return len(documents)

    def validate_vector(self, vector: list[float]) -> None:
        """Reject a vector that does not match the 512-dimension mapping."""

        if len(vector) != self.config.vector_dimension:
            message = (
                f"embedding has {len(vector)} values; "
                f"expected {self.config.vector_dimension}"
            )
            self.logger.error(f"OpenSearch vector validation failed: {message}")
            raise PermanentDataError(message)

    def _retryable_documents(
        self,
        failures: list,
        documents: list[OpenSearchDocument],
    ) -> list[OpenSearchDocument]:
        """Return only temporary failed documents from a bulk response."""

        failed_ids = set()
        for failure in failures:
            operation = next(iter(failure.values()))
            status = int(operation.get("status", 0))
            document_id = operation.get("_id")
            if status not in RETRYABLE_STATUS_CODES or document_id is None:
                reason = operation.get("error", operation)
                raise PermanentDataError(
                    f"OpenSearch rejected document {document_id}: "
                    f"{str(reason)[:500]}"
                )
            failed_ids.add(str(document_id))
        return [
            document for document in documents if str(document["_id"]) in failed_ids
        ]

    def _pause_before_retry(
        self,
        alias: str,
        attempt: int,
        failed_count: int,
        needs_embedding: bool,
    ) -> None:
        """Log one short error and apply the correct retry pause."""

        request_type = "embedding" if needs_embedding else "bulk"
        self.logger.warning(
            f"OpenSearch {request_type} write to {alias} failed on attempt "
            f"{attempt} for {failed_count} document(s). Retrying in "
            f"{self.config.retry_seconds} seconds."
        )
        if needs_embedding:
            with self.cooldown_lock:
                self.embedding_cooldown_until = max(
                    self.embedding_cooldown_until,
                    time.monotonic() + self.config.retry_seconds,
                )
            return
        time.sleep(self.config.retry_seconds)

    def _wait_for_embedding_cooldown(self) -> None:
        """Wait when another worker reports Lasso or Bedrock throttling."""

        while True:
            with self.cooldown_lock:
                remaining = self.embedding_cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            self.logger.info(
                "Waiting "
                f"{remaining:.1f} second(s) before the next OpenSearch embedding "
                "write because an earlier request was throttled."
            )
            time.sleep(remaining)

    @staticmethod
    def _is_retryable(error: TransportError) -> bool:
        """Return true for network errors and known temporary status codes."""

        status = getattr(error, "status_code", None)
        if status in {None, "N/A"}:
            return True
        try:
            return int(status) in RETRYABLE_STATUS_CODES
        except (TypeError, ValueError):
            return False
