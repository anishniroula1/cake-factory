import threading
import time

from opensearchpy import OpenSearch, OpenSearchException, TransportError, helpers

from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import OpenSearchDocument
from doc2vec_completed.opensearch.errors import PermanentDataError
from ta_utils.logger.CustomLogger import CustomLogger

# These status codes usually mean the service needs a short pause and another try.
RETRYABLE_STATUS_CODES = {408, 424, 425, 429, 500, 502, 503, 504}


class OpenSearchHelper:
    def __init__(
        self,
        config: Config,
        client: OpenSearch,
        custom_logger: CustomLogger,
    ) -> None:
        """Save the OpenSearch client, settings, and logger."""

        self.config = config
        self.client = client
        self.logger = custom_logger
        self.cooldown_lock = threading.Lock()
        self.embedding_cooldown_until = 0.0

    def existing_ids(self, alias: str, document_ids: list[str]) -> set[str]:
        """Return the requested IDs that already exist in an alias.

        Input: document_ids=["A", "B"] and only A exists.
        Output: {"A"}.

        A temporary error goes back to the retry code. A bad request stops
        immediately because waiting will not fix permissions or a bad mapping.
        """

        existing: set[str] = set()
        unique_ids = list(dict.fromkeys(document_ids))
        if not unique_ids:
            return existing

        for offset in range(0, len(unique_ids), self.config.opensearch_batch_size):
            batch = unique_ids[offset : offset + self.config.opensearch_batch_size]
            try:
                response = self.client.mget(
                    index=alias,
                    body={"ids": batch},
                    params={"_source": "false"},
                )
            except TransportError as error:
                if self._is_retryable(error):
                    raise
                raise PermanentDataError(
                    f"OpenSearch lookup in alias {alias} was rejected: "
                    f"{str(error)[:500]}"
                ) from error

            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        return existing

    def bulk_index(
        self,
        alias: str,
        documents: list[OpenSearchDocument],
        needs_embedding: bool,
    ) -> int:
        """Try one OpenSearch bulk write.

        Input: documents with an `_id` field.
        Output: the number written when the request succeeds.

        `_id` is used as the OpenSearch document ID and is removed from the
        saved body. The caller handles the wait and retry when this call fails.
        """

        if not documents:
            return 0

        if needs_embedding:
            self._wait_for_embedding_cooldown()

        actions = [
            {
                "_op_type": "index",
                "_index": alias,
                "_id": str(document["_id"]),
                "_source": {key: value for key, value in document.items() if key != "_id"},
            }
            for document in documents
        ]
        bulk_arguments = {
            "chunk_size": self.config.opensearch_batch_size,
            "raise_on_error": False,
            "raise_on_exception": True,
        }
        if not needs_embedding:
            # This document already has its vector. Skip the default pipeline so
            # Bedrock does not create and charge for the same vector again.
            bulk_arguments["pipeline"] = "_none"

        try:
            _, failures = helpers.bulk(self.client, actions, **bulk_arguments)
        except TransportError as error:
            if not self._is_retryable(error):
                raise PermanentDataError(
                    f"OpenSearch bulk write to {alias} was rejected: "
                    f"{str(error)[:500]}"
                ) from error
            if needs_embedding:
                self._start_embedding_cooldown()
            raise

        if failures:
            retryable_documents = self._retryable_documents(failures, documents)
            if needs_embedding:
                self._start_embedding_cooldown()
            raise OpenSearchException(
                f"OpenSearch temporarily rejected {len(retryable_documents)} "
                f"document(s) in alias {alias}"
            )
        return len(documents)

    def validate_vector(self, vector: list[float]) -> None:
        """Stop when a saved vector has the wrong number of values.

        Example: a 1,024-value vector cannot go into a 512-value field.
        """

        if len(vector) != self.config.vector_dimension:
            raise PermanentDataError(
                f"embedding has {len(vector)} values; expected "
                f"{self.config.vector_dimension}"
            )

    def refresh_indices(self, catalog_alias: str) -> None:
        """Make the new catalog and occurrence documents searchable.

        Input: catalog_alias="sentence_semantic_catalog_asylee".
        This runs once after all batches because refreshing every batch is slow.
        """

        indices = f"{catalog_alias},{self.config.occurrence_alias}"
        self.client.indices.refresh(index=indices)
        self.logger.info(
            "OpenSearch refresh completed. Newly indexed sentences are now "
            "available to Sentence Similarity."
        )

    def _retryable_documents(
        self,
        failures: list,
        documents: list[OpenSearchDocument],
    ) -> list[OpenSearchDocument]:
        """Return failed documents that are allowed to be tried again.

        Example: A succeeds and B returns 503. Output contains only B.
        A status such as 400 stops immediately because another try will not help.
        The caller uses this result to count failures, then retries the full batch.
        """

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

    def _start_embedding_cooldown(self) -> None:
        """Set one shared wait time after Lasso or Bedrock asks us to slow down."""

        with self.cooldown_lock:
            self.embedding_cooldown_until = max(
                self.embedding_cooldown_until,
                time.monotonic() + self.config.retry_seconds,
            )

    def _wait_for_embedding_cooldown(self) -> None:
        """Wait until the shared Lasso or Bedrock pause has finished."""

        while True:
            with self.cooldown_lock:
                remaining = self.embedding_cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            self.logger.debug(
                "Waiting "
                f"{remaining:.1f} second(s) before the next OpenSearch embedding "
                "write because an earlier request was throttled."
            )
            time.sleep(remaining)

    @staticmethod
    def _is_retryable(error: TransportError) -> bool:
        """Return True when waiting and trying the request again may work."""

        status = getattr(error, "status_code", None)
        if status in {None, "N/A"}:
            return True
        try:
            return int(status) in RETRYABLE_STATUS_CODES
        except (TypeError, ValueError):
            return False
