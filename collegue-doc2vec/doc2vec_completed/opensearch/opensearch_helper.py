import threading
import time

from opensearchpy import OpenSearch, OpenSearchException, TransportError, helpers

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import OpenSearchDocument
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.retry import safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_STATUS_CODES = {408, 424, 425, 429, 500, 502, 503, 504}


class OpenSearchHelper:
    """Provide shared bulk, retry, cooldown, validation, and refresh operations."""

    def __init__(
        self,
        client: OpenSearch,
        custom_logger: CustomLogger,
    ) -> None:
        self.client = client
        self.logger = custom_logger
        self.cooldown_lock = threading.Lock()
        self.embedding_cooldown_until = 0.0

    def find_existing_document_ids(
        self,
        alias: str,
        document_ids: list[str],
    ) -> set[str]:
        """Return requested document IDs that already exist in one alias.

        Input: alias="sentence_semantic_catalog_asylee",
        document_ids=["A", "B"], where only A exists.
        Output: {"A"}
        """

        existing: set[str] = set()
        unique_ids = list(dict.fromkeys(document_ids))
        if not unique_ids:
            return existing

        for offset in range(0, len(unique_ids), constants.OPENSEARCH_BATCH_SIZE):
            batch = unique_ids[offset : offset + constants.OPENSEARCH_BATCH_SIZE]
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
                    f"{safe_error_summary(error)}"
                ) from error

            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        return existing

    def index_documents(
        self,
        alias: str,
        documents: list[OpenSearchDocument],
        needs_embedding: bool,
    ) -> int:
        """Write one document batch and optionally run the embedding pipeline.

        Input: alias="sentence_semantic_catalog_asylee", documents=[30 records],
        needs_embedding=True
        Output: 30 after every document is accepted.

        Documents with saved vectors bypass the default pipeline so Bedrock
        does not create and charge for the same vectors again.
        """

        if not documents:
            return 0

        bulk_arguments = {
            "chunk_size": constants.OPENSEARCH_BATCH_SIZE,
            "raise_on_error": False,
            "raise_on_exception": True,
        }
        if not needs_embedding:
            bulk_arguments["pipeline"] = "_none"

        pending_documents = documents
        maximum_attempts = constants.OPENSEARCH_BULK_RETRY_COUNT + 1

        for attempt in range(1, maximum_attempts + 1):
            if needs_embedding:
                self._wait_for_embedding_cooldown()

            actions = [
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
                for document in pending_documents
            ]

            try:
                _, failures = helpers.bulk(
                    self.client,
                    actions,
                    **bulk_arguments,
                )
            except TransportError as error:
                if not self._is_retryable(error):
                    raise PermanentDataError(
                        f"OpenSearch bulk write to {alias} was rejected: "
                        f"{safe_error_summary(error)}"
                    ) from error
                self._prepare_bulk_retry(
                    alias,
                    attempt,
                    maximum_attempts,
                    len(pending_documents),
                    needs_embedding,
                )
                continue

            if not failures:
                if attempt > 1:
                    self.logger.info(
                        f"OpenSearch bulk write to {alias} succeeded on attempt "
                        f"{attempt}."
                    )
                return len(documents)

            # Keep only documents rejected by a temporary status. Documents
            # already accepted by OpenSearch must not call Bedrock again.
            pending_documents = self._retryable_documents(
                failures,
                pending_documents,
            )
            self._prepare_bulk_retry(
                alias,
                attempt,
                maximum_attempts,
                len(pending_documents),
                needs_embedding,
            )

        raise OpenSearchException(
            f"OpenSearch bulk write to {alias} failed after {maximum_attempts} "
            "attempts"
        )

    def validate_vector(self, vector: list[float]) -> None:
        """Validate a saved vector against the configured dimension.

        Input: vector=[512 float values]

        Raises:
            PermanentDataError: If the vector length differs from 512.
        """

        if len(vector) != constants.VECTOR_DIMENSION:
            raise PermanentDataError(
                f"embedding has {len(vector)} values; expected "
                f"{constants.VECTOR_DIMENSION}"
            )

    def refresh_aliases(self, aliases: list[str]) -> None:
        """Refresh supplied aliases once after their bulk writes finish.

        Input: aliases=["sentence_semantic_catalog_asylee", "sentence_occurrences"]

        One final refresh makes all supplied aliases searchable before the next
        service receives its SQS message.
        """

        if not aliases:
            return
        self.client.indices.refresh(index=",".join(aliases))
        self.logger.info(f"OpenSearch refresh completed for {len(aliases)} alias(es).")

    def _retryable_documents(
        self,
        failures: list,
        documents: list[OpenSearchDocument],
    ) -> list[OpenSearchDocument]:
        """Return failed documents when every failure can be retried.

        Input: document A succeeds and document B returns status 503.
        Output: [document B]

        A permanent status such as 400 raises an error immediately because
        another request cannot fix a rejected mapping or request.
        """

        failed_ids = set()
        for failure in failures:
            operation = next(iter(failure.values()))
            status = int(operation.get("status", 0))
            document_id = operation.get("_id")
            if status not in RETRYABLE_STATUS_CODES or document_id is None:
                error_details = operation.get("error")
                error_type = (
                    error_details.get("type", "unknown")
                    if isinstance(error_details, dict)
                    else type(error_details).__name__
                )
                raise PermanentDataError(
                    "OpenSearch permanently rejected a document. "
                    f"status_code={status}, error_type={error_type}."
                )
            failed_ids.add(str(document_id))
        return [
            document for document in documents if str(document["_id"]) in failed_ids
        ]

    def _start_embedding_cooldown(self) -> None:
        """Start a shared pause after the embedding service throttles a write."""

        with self.cooldown_lock:
            self.embedding_cooldown_until = max(
                self.embedding_cooldown_until,
                time.monotonic() + constants.RETRY_SECONDS,
            )

    def _prepare_bulk_retry(
        self,
        alias: str,
        attempt: int,
        maximum_attempts: int,
        failed_document_count: int,
        needs_embedding: bool,
    ) -> None:
        """Pause before retrying only the documents that failed a bulk write.

        Input: attempt=1, maximum_attempts=16, failed_document_count=2
        Output: waits five seconds before the second request. Attempt 16 raises
        an error because all configured requests have been used.
        """

        if attempt >= maximum_attempts:
            raise OpenSearchException(
                f"OpenSearch bulk write to {alias} failed after "
                f"{maximum_attempts} attempts for {failed_document_count} "
                "document(s)"
            )

        self.logger.warning(
            f"OpenSearch bulk write to {alias} attempt {attempt} temporarily "
            f"failed for {failed_document_count} document(s); retrying in "
            f"{constants.RETRY_SECONDS} seconds."
        )
        if needs_embedding:
            self._start_embedding_cooldown()
            return
        time.sleep(constants.RETRY_SECONDS)

    def _wait_for_embedding_cooldown(self) -> None:
        """Wait until the shared embedding-service pause expires."""

        while True:
            with self.cooldown_lock:
                remaining = self.embedding_cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            self.logger.debug(
                f"Waiting {remaining:.1f} second(s) before the next OpenSearch "
                "embedding write because an earlier request was throttled."
            )
            time.sleep(remaining)

    @staticmethod
    def _is_retryable(error: TransportError) -> bool:
        """Return whether a transport error is safe to retry.

        Input: TransportError with status_code=503
        Output: True
        """

        status = getattr(error, "status_code", None)
        if status in {None, "N/A"}:
            return True
        try:
            return int(status) in RETRYABLE_STATUS_CODES
        except (TypeError, ValueError):
            return False

