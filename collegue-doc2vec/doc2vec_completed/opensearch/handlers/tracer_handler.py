import json
from array import array
from collections.abc import Callable
from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import TracerDocument, TracerResult
from doc2vec_completed.opensearch.db_store import TracerDBStore
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    TracerOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from ta_db_connection.atatables import TracerSentence
from ta_utils.logger.CustomLogger import CustomLogger


class TracerHandler:
    """Load tracer reference records and save them in the tracer catalog."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        opensearch_helper: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.tracer_opensearch_store = TracerOpenSearchStore(
            opensearch_helper,
        )
        self.logger = custom_logger
        self.tracer_db_store = TracerDBStore(session_factory)

    def process(self, tracer_id: int) -> TracerResult:
        """Index one tracer, form-language, or non-matcher reference.

        Input: tracer_id=10
        Output: {"reason": "TRACER", "source_count": 1, "indexed_count": 1}
        """

        started_at = perf_counter()
        tracer_record = retry_operation(
            "Load tracer record",
            self.tracer_db_store.retrieve_tracer_from_tracer_id,
            tracer_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        if tracer_record is None:
            raise PermanentDataError(f"Tracer record is missing for ID {tracer_id}")

        document = self._build_tracer_document(tracer_record)
        if "sentenceContentVector" in document:
            indexed_count = retry_operation(
                "Write tracer references with saved vectors",
                self.tracer_opensearch_store.index_saved_vectors,
                [document],
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=1,
                custom_logger=self.logger,
            )
        else:
            indexed_count = retry_operation(
                "Create Bedrock vectors for tracer references",
                self.tracer_opensearch_store.index_with_bedrock,
                [document],
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=1,
                custom_logger=self.logger,
            )

        self.logger.info(
            f"Tracer processing completed. Indexed count: {indexed_count}."
        )
        self.logger.info(
            f"Tracer processing took {perf_counter() - started_at:.2f} seconds to complete."
        )
        return {
            "reason": "TRACER",
            "source_count": 1,
            "indexed_count": indexed_count,
        }

    def _build_tracer_document(
        self,
        tracer_record: TracerSentence,
    ) -> TracerDocument:
        """Build a strict tracer document and omit its vector when one is not saved.

        Input: one TracerSentence database record.
        Output: a tracer catalog document ready for direct or pipeline indexing.
        """

        timestamp = datetime.now(UTC)
        document: TracerDocument = {
            "_id": tracer_record.tracer_id,
            "tracerId": tracer_record.tracer_id,
            "sentenceContent": tracer_record.tracer_content,
            "analysisGroup": tracer_record.analysis_group,
            "isNonMatcher": tracer_record.is_non_matcher,
            "isFormLanguage": tracer_record.is_form_language,
            "createdAt": timestamp.isoformat(),
            "updatedAt": timestamp.isoformat(),
        }
        try:
            vector = self._read_saved_vector(tracer_record.titan_embedding_list)
        except PermanentDataError as error:
            # A malformed legacy vector should not keep retrying the same SQS
            # message. Omit it so the OpenSearch pipeline creates a fresh one.
            self.logger.warning(
                "Saved tracer embedding is invalid and will be regenerated "
                f"through Bedrock. Cause: {str(error)[:500]}"
            )
            vector = None
        if vector is not None:
            document["sentenceContentVector"] = vector
        return document

    def _read_saved_vector(self, value) -> list[float] | None:
        """Convert a saved tracer embedding into a normal float list.

        Input: a list, NumPy-like value, JSON bytes, or raw float bytes.
        Output: a list containing the configured 512 vector values, or None
        when the tracer record has no saved embedding.
        """

        if value is None:
            return None
        if hasattr(value, "tolist"):
            value = value.tolist()
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, bytes):
            try:
                value = json.loads(value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                value = self._decode_raw_float_vector(value)
        try:
            vector = [float(item) for item in value]
        except (TypeError, ValueError) as error:
            raise PermanentDataError(
                "saved tracer embedding is not a list of numbers"
            ) from error
        self.tracer_opensearch_store.validate_vector(vector)
        return vector

    def _decode_raw_float_vector(self, value: bytes) -> list[float]:
        """Decode one raw 512-dimension tracer embedding.

        Input: bytes containing 512 packed 32-bit or 64-bit float values.
        Output: a normal Python list containing the same 512 float values.
        """

        for type_code in ("f", "d"):
            item_size = array(type_code).itemsize
            if len(value) != constants.VECTOR_DIMENSION * item_size:
                continue
            decoded = array(type_code)
            decoded.frombytes(value)
            return list(decoded)
        raise PermanentDataError(
            "saved tracer embedding bytes do not contain a 512-dimension vector"
        )

