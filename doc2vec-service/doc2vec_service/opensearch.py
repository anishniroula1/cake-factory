import threading
import time

from opensearchpy import TransportError, helpers

from doc2vec_service.errors import PermanentDataError

# These failures are normally temporary problems from OpenSearch, Lasso,
# Bedrock, or a gateway in front of them.
RETRYABLE_STATUS_CODES = {408, 424, 425, 429, 500, 502, 503, 504}


class OpenSearchHelper:
    def __init__(self, config, client, customLogger):
        """Keep the small amount of OpenSearch code shared by all handlers."""

        self.config = config
        self.client = client
        self.logger = customLogger
        self.cooldownLock = threading.Lock()
        self.embeddingCooldownUntil = 0.0

    def existingIds(self, alias: str, documentIds: list) -> set:
        """Return document IDs that already exist in an OpenSearch alias."""

        existing = set()
        uniqueIds = list(dict.fromkeys(documentIds))
        for offset in range(0, len(uniqueIds), self.config.opensearchBatchSize):
            batch = uniqueIds[offset : offset + self.config.opensearchBatchSize]
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
                    if not self._isRetryable(error):
                        raise PermanentDataError(
                            f"OpenSearch could not read {alias}: {error}"
                        ) from error
                    self.logger.warning(
                        f"OpenSearch {alias} read attempt {attempt} failed; "
                        f"retrying in {self.config.retrySeconds} seconds"
                    )
                    time.sleep(self.config.retrySeconds)

            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        return existing

    def bulkIndex(self, alias: str, documents: list, needsEmbedding: bool) -> int:
        """Index documents and keep retrying temporary failed items.

        Every document must contain an internal `_id`. The helper removes that
        value from `_source` before sending the document to OpenSearch.
        """

        if not documents:
            return 0

        pending = documents
        attempt = 0
        while pending:
            attempt += 1
            if needsEmbedding:
                self._waitForEmbeddingCooldown()

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

            bulkArguments = {
                "chunk_size": self.config.opensearchBatchSize,
                "raise_on_error": False,
                "raise_on_exception": True,
            }
            if not needsEmbedding:
                # This prevents an index default pipeline from replacing an
                # existing vector or calling Bedrock unnecessarily.
                bulkArguments["pipeline"] = "_none"

            try:
                _, failures = helpers.bulk(
                    self.client,
                    actions,
                    **bulkArguments,
                )
            except TransportError as error:
                if not self._isRetryable(error):
                    raise PermanentDataError(
                        f"OpenSearch could not write {alias}: {error}"
                    ) from error
                self._pauseBeforeRetry(alias, attempt, len(pending), needsEmbedding)
                continue

            if not failures:
                return len(documents)

            pending = self._retryableDocuments(failures, pending)
            self._pauseBeforeRetry(alias, attempt, len(pending), needsEmbedding)

        return len(documents)

    def validateVector(self, vector: list):
        """Reject a vector that does not match the 512-dimension mapping."""

        if len(vector) != self.config.vectorDimension:
            raise PermanentDataError(
                f"embedding has {len(vector)} values; "
                f"expected {self.config.vectorDimension}"
            )

    def _retryableDocuments(self, failures: list, documents: list) -> list:
        """Return only temporary failed documents from a bulk response."""

        failedIds = set()
        for failure in failures:
            operation = next(iter(failure.values()))
            status = int(operation.get("status", 0))
            documentId = operation.get("_id")
            if status not in RETRYABLE_STATUS_CODES or documentId is None:
                reason = operation.get("error", operation)
                raise PermanentDataError(
                    f"OpenSearch rejected document {documentId}: {reason}"
                )
            failedIds.add(str(documentId))
        return [
            document
            for document in documents
            if str(document["_id"]) in failedIds
        ]

    def _pauseBeforeRetry(
        self,
        alias: str,
        attempt: int,
        failedCount: int,
        needsEmbedding: bool,
    ):
        """Log one short error and apply the correct retry pause."""

        self.logger.warning(
            f"OpenSearch {alias} attempt {attempt} failed for "
            f"{failedCount} document(s); retrying in "
            f"{self.config.retrySeconds} seconds"
        )
        if needsEmbedding:
            with self.cooldownLock:
                self.embeddingCooldownUntil = max(
                    self.embeddingCooldownUntil,
                    time.monotonic() + self.config.retrySeconds,
                )
            return
        time.sleep(self.config.retrySeconds)

    def _waitForEmbeddingCooldown(self):
        """Wait when another worker reports Lasso or Bedrock throttling."""

        while True:
            with self.cooldownLock:
                remaining = self.embeddingCooldownUntil - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(remaining)

    @staticmethod
    def _isRetryable(error: TransportError) -> bool:
        """Return true for network errors and known temporary status codes."""

        status = getattr(error, "status_code", None)
        if status in {None, "N/A"}:
            return True
        try:
            return int(status) in RETRYABLE_STATUS_CODES
        except (TypeError, ValueError):
            return False
