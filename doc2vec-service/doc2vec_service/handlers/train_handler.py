import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update

from doc2vec_service.batching import batches
from doc2vec_service.models import Embedding, SentenceVector
from doc2vec_service.retry import retryOperation
from doc2vec_service.text import sentenceKey

logger = logging.getLogger(__name__)


class TrainHandler:
    def __init__(self, config, sessionFactory, opensearch):
        """Keep everything required for a TRAIN message."""

        self.config = config
        self.sessionFactory = sessionFactory
        self.opensearch = opensearch

    def process(self, tspId: int) -> dict:
        """Create keys and add one document's sentences to the training index."""

        total = retryOperation(
            f"count TRAIN tsp_id {tspId}",
            self._count,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        logger.info("TRAIN tsp_id=%s contains %s sentence(s)", tspId, total)

        indexed = 0
        afterGlobalId = 0
        with ThreadPoolExecutor(
            max_workers=self.config.workerCount,
            thread_name_prefix="train-sentence",
        ) as executor:
            while True:
                rows = retryOperation(
                    f"load TRAIN tsp_id {tspId}",
                    self._loadPage,
                    tspId,
                    afterGlobalId,
                    waitSeconds=self.config.retrySeconds,
                )
                if not rows:
                    break

                retryOperation(
                    f"save TRAIN tsp_id {tspId} sentence keys",
                    self._saveKeys,
                    rows,
                    waitSeconds=self.config.retrySeconds,
                )
                workerBatches = list(batches(rows, self.config.workerBatchSize))
                indexed += sum(executor.map(self._runWorker, workerBatches))
                afterGlobalId = rows[-1].global_id
                logger.info(
                    "TRAIN tsp_id=%s processed through global_id=%s",
                    tspId,
                    afterGlobalId,
                )

        return {
            "reason": "TRAIN",
            "tspId": tspId,
            "sourceCount": total,
            "indexedCount": indexed,
        }

    def _count(self, tspId: int) -> int:
        """Count current sentence rows for the document."""

        with self.sessionFactory() as session:
            statement = select(func.count()).select_from(SentenceVector).where(
                SentenceVector.tsp_id == tspId,
                SentenceVector.is_current.is_(True),
            )
            return int(session.scalar(statement) or 0)

    def _loadPage(self, tspId: int, afterGlobalId: int) -> list:
        """Read the next 1,000 sentences using fast global-ID paging."""

        with self.sessionFactory() as session:
            statement = (
                select(SentenceVector)
                .where(
                    SentenceVector.tsp_id == tspId,
                    SentenceVector.is_current.is_(True),
                    SentenceVector.global_id > afterGlobalId,
                )
                .order_by(SentenceVector.global_id)
                .limit(self.config.databaseBatchSize)
            )
            return list(session.scalars(statement))

    def _saveKeys(self, rows: list):
        """Generate and update all sentence keys from one database page."""

        if not rows:
            return
        currentTime = datetime.now(UTC)
        updates = []
        for row in rows:
            row.sentence_key = sentenceKey(row.sentence_content)
            row.updated_at = currentTime
            updates.append(
                {
                    "recordGlobalId": row.global_id,
                    "recordSentenceKey": row.sentence_key,
                    "recordUpdatedAt": currentTime,
                }
            )

        table = SentenceVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("recordGlobalId"))
            .values(
                sentence_key=bindparam("recordSentenceKey"),
                updated_at=bindparam("recordUpdatedAt"),
            )
        )
        with self.sessionFactory() as session:
            session.execute(statement, updates)
            session.commit()

    def _runWorker(self, rows: list) -> int:
        """Retry one 30-sentence worker until it finishes."""

        label = f"TRAIN global_id {rows[0].global_id}-{rows[-1].global_id}"
        return retryOperation(
            label,
            self._processWorker,
            rows,
            waitSeconds=self.config.retrySeconds,
        )

    def _processWorker(self, rows: list) -> int:
        """Reuse saved vectors and call Bedrock only when a vector is missing."""

        embeddings = self._loadEmbeddings([row.global_id for row in rows])
        existingKeys = self.opensearch.existingIds(
            self.config.trainingAlias,
            [row.sentence_key for row in rows],
        )

        withVectors = []
        needVectors = []
        seen = set()
        for row in rows:
            if row.sentence_key in existingKeys or row.sentence_key in seen:
                continue
            seen.add(row.sentence_key)
            document = {
                "_id": row.sentence_key,
                "sentenceKey": row.sentence_key,
                "sentenceContent": row.sentence_content,
                "createdAt": row.created_at.isoformat(),
                "updatedAt": row.updated_at.isoformat(),
            }
            vector = embeddings.get(row.global_id)
            if vector is None:
                needVectors.append(document)
                continue
            self.opensearch.validateVector(vector)
            document["sentenceContentVector"] = vector
            withVectors.append(document)

        indexed = self.opensearch.bulkIndex(
            self.config.trainingAlias,
            withVectors,
            needsEmbedding=False,
        )
        indexed += self.opensearch.bulkIndex(
            self.config.trainingAlias,
            needVectors,
            needsEmbedding=True,
        )
        return indexed

    def _loadEmbeddings(self, globalIds: list) -> dict:
        """Read existing Titan vectors for the worker's global IDs."""

        with self.sessionFactory() as session:
            statement = select(Embedding).where(
                Embedding.global_id.in_(globalIds),
                Embedding.embedding_type == self.config.titanEmbeddingType,
            )
            return {
                row.global_id: list(row.embedding)
                for row in session.scalars(statement)
                if row.embedding
            }
