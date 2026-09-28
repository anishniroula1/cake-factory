import logging
import struct

from sqlalchemy import func, select

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.models import TracerSentence
from doc2vec_service.retry import retryOperation

logger = logging.getLogger(__name__)


class TracerHandler:
    def __init__(self, config, sessionFactory, opensearch):
        """Keep everything required for a TRACER message."""

        self.config = config
        self.sessionFactory = sessionFactory
        self.opensearch = opensearch

    def process(self) -> dict:
        """Read every tracer sentence and copy its saved vector to OpenSearch."""

        total = retryOperation(
            "count tracer sentences",
            self._count,
            waitSeconds=self.config.retrySeconds,
        )
        logger.info("TRACER contains %s sentence(s)", total)

        indexed = 0
        afterId = 0
        while True:
            rows = retryOperation(
                "load tracer sentences",
                self._loadPage,
                afterId,
                waitSeconds=self.config.retrySeconds,
            )
            if not rows:
                break

            documents = []
            for row in rows:
                documents.append(
                    {
                        "_id": row.tracer_sentence_id,
                        "sentenceContent": row.sentence_content,
                        "sentenceContentVector": self._decodeVector(row.embedding),
                        "isNonMatcher": row.is_non_matcher,
                        "isTracer": row.is_tracer,
                        "isFormLanguage": row.is_form_language,
                        "analysisGroup": row.analysis_group,
                    }
                )

            indexed += self.opensearch.bulkIndex(
                self.config.tracerAlias,
                documents,
                needsEmbedding=False,
            )
            afterId = rows[-1].tracer_sentence_id
            logger.info("TRACER processed through tracer_sentence_id=%s", afterId)

        return {"reason": "TRACER", "sourceCount": total, "indexedCount": indexed}

    def _count(self) -> int:
        """Count tracer and form-language rows before processing."""

        with self.sessionFactory() as session:
            return int(
                session.scalar(select(func.count()).select_from(TracerSentence))
                or 0
            )

    def _loadPage(self, afterId: int) -> list:
        """Read the next 1,000 tracer rows in primary-key order."""

        with self.sessionFactory() as session:
            statement = (
                select(TracerSentence)
                .where(TracerSentence.tracer_sentence_id > afterId)
                .order_by(TracerSentence.tracer_sentence_id)
                .limit(self.config.databaseBatchSize)
            )
            return list(session.scalars(statement))

    def _decodeVector(self, value: bytes) -> list:
        """Convert one saved Titan BYTEA value into 512 float values."""

        if not isinstance(value, bytes):
            raise PermanentDataError("tracer embedding must be stored as bytes")
        expectedBytes = self.config.vectorDimension * 4
        if len(value) != expectedBytes:
            raise PermanentDataError(
                f"tracer embedding has {len(value)} bytes; expected {expectedBytes}"
            )
        vector = list(struct.unpack(f"<{self.config.vectorDimension}f", value))
        self.opensearch.validateVector(vector)
        return vector
