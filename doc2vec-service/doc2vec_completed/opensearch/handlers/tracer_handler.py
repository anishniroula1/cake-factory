import json
from array import array
from collections.abc import Callable
from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import TracerDocument, TracerResult
from doc2vec_completed.opensearch.db_store import TracerSentenceDBStore
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    TracerOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from ta_db_connection.atables import TracerSentence
from ta_utils.logger.CustomLogger import CustomLogger


class TracerHandler:
    """Load tracer reference records and save them in the tracer catalog."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_helper: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.tracer_open_search_store = TracerOpenSearchStore(
            config,
            opensearch_helper,
        )
        self.logger = custom_logger
        self.tracer_sentence_db_store = TracerSentenceDBStore(session_factory)

    def process(self, tracer_sentence_ids: list[int]) -> TracerResult | None:
        """Index the requested tracer, form-language, and non-matcher references.

        Input: tracer_sentence_ids=[10, 20]
        Output: {"reason": "TRACER", "source_count": 2, "indexed_count": 2}
        """

        started_at = perf_counter()
        unique_ids = list(dict.fromkeys(tracer_sentence_ids))
        if not unique_ids:
            self.logger.error("No tracer_sentence_ids were supplied for processing.")
            return None

        indexed_count = 0
        loaded_ids: set[int] = set()
        for id_batch in batches(unique_ids, constants.OPENSEARCH_BATCH_SIZE):
            tracer_sentence_records = retry_operation(
                "Load tracer sentence batch",
                self.tracer_sentence_db_store.load_tracer_sentences_by_ids,
                id_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            loaded_ids.update(
                tracer_sentence_record.tracer_sentence_id
                for tracer_sentence_record in tracer_sentence_records
            )
            documents = [
                self._build_tracer_document(tracer_sentence_record)
                for tracer_sentence_record in tracer_sentence_records
            ]
            saved_vectors = [
                document
                for document in documents
                if "sentenceContentVector" in document
            ]
            bedrock_vectors = [
                document
                for document in documents
                if "sentenceContentVector" not in document
            ]
            indexed_count += retry_operation(
                "Write tracer references with saved vectors",
                self.tracer_open_search_store.index_saved_vectors,
                saved_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
                custom_logger=self.logger,
            )
            indexed_count += retry_operation(
                "Create Bedrock vectors for tracer references",
                self.tracer_open_search_store.index_with_bedrock,
                bedrock_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
                custom_logger=self.logger,
            )

        missing_ids = sorted(set(unique_ids) - loaded_ids)
        if missing_ids:
            raise PermanentDataError(
                f"tracer_sentence records are missing for IDs {missing_ids}"
            )

        retry_operation(
            "Refresh the tracer catalog",
            self.tracer_open_search_store.refresh,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Tracer processing completed. Source count: {len(unique_ids)}. "
            f"Indexed count: {indexed_count}."
        )
        self.logger.info(
            f"Tracer processing took {perf_counter() - started_at:.2f} seconds to complete."
        )
        return {
            "reason": "TRACER",
            "source_count": len(unique_ids),
            "indexed_count": indexed_count,
        }

    def _build_tracer_document(
        self,
        tracer_sentence_record: TracerSentence,
    ) -> TracerDocument:
        """Build a strict tracer document and omit its vector when one is not saved.

        Input: one TracerSentence database record.
        Output: a tracer catalog document ready for direct or pipeline indexing.
        """

        timestamp = (
            getattr(tracer_sentence_record, "last_modified", None)
            or datetime.now(UTC)
        )
        document: TracerDocument = {
            "_id": tracer_sentence_record.tracer_sentence_id,
            "tracerId": tracer_sentence_record.tracer_sentence_id,
            "sentenceContent": tracer_sentence_record.sentence_content,
            "analysisGroup": tracer_sentence_record.analysis_group,
            "isNonMatcher": tracer_sentence_record.is_non_matcher,
            "isFormLanguage": tracer_sentence_record.is_form_language,
            "createdAt": timestamp.isoformat(),
            "updatedAt": timestamp.isoformat(),
        }
        vector = self._read_saved_vector(tracer_sentence_record.embedding)
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
        self.tracer_open_search_store.validate_vector(vector)
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
