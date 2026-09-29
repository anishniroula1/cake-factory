import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update

from doc2vec_service.batching import batches
from doc2vec_service.errors import PermanentDataError
from doc2vec_service.models import Embedding, SentenceVector
from doc2vec_service.retry import retryOperation
from doc2vec_service.text import sentenceKey

logger = logging.getLogger(__name__)


class HistoricalHandler:
    def __init__(self, config, sessionFactory, opensearch, sqsClient):
        """Keep everything required for one HISTORICAL message."""

        self.config = config
        self.sessionFactory = sessionFactory
        self.opensearch = opensearch
        self.sqsClient = sqsClient

    def process(self, tspId: int) -> dict:
        """Index already-split historical sentences and send the next message."""

        total = retryOperation(
            f"count HISTORICAL tsp_id {tspId}",
            self._count,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        if total == 0:
            raise PermanentDataError(
                f"sentences_vector has no current rows for tsp_id {tspId}"
            )

        logger.info("HISTORICAL tsp_id=%s contains %s sentence(s)", tspId, total)
        indexed = 0
        afterGlobalId = 0

        with ThreadPoolExecutor(
            max_workers=self.config.historicalWorkerCount,
            thread_name_prefix="historical-sentence",
        ) as executor:
            while True:
                rows = retryOperation(
                    f"load HISTORICAL tsp_id {tspId}",
                    self._loadPage,
                    tspId,
                    afterGlobalId,
                    waitSeconds=self.config.retrySeconds,
                )
                if not rows:
                    break

                # Historical rows already contain split sentences. Only create
                # the stable sentence keys that the two OpenSearch indexes use.
                retryOperation(
                    f"save HISTORICAL tsp_id {tspId} sentence keys",
                    self._saveKeys,
                    rows,
                    waitSeconds=self.config.retrySeconds,
                )

                workerBatches = list(batches(rows, self.config.workerBatchSize))
                indexed += sum(executor.map(self._runWorker, workerBatches))
                afterGlobalId = rows[-1].global_id
                logger.info(
                    "HISTORICAL tsp_id=%s processed through global_id=%s",
                    tspId,
                    afterGlobalId,
                )

        # Publish only after every historical sentence is safely indexed. The
        # listener deletes the source message after this method returns.
        retryOperation(
            f"publish HISTORICAL tsp_id {tspId}",
            self._publish,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )

        return {
            "reason": "HISTORICAL",
            "tspId": tspId,
            "sourceCount": total,
            "indexedCount": indexed,
        }

    def _count(self, tspId: int) -> int:
        """Count current historical sentence rows for one document."""

        with self.sessionFactory() as session:
            statement = select(func.count()).select_from(SentenceVector).where(
                SentenceVector.tsp_id == tspId,
                SentenceVector.is_current.is_(True),
            )
            return int(session.scalar(statement) or 0)

    def _loadPage(self, tspId: int, afterGlobalId: int) -> list:
        """Read the next historical page in stable global-ID order."""

        pageSize = (
            self.config.workerBatchSize * self.config.historicalWorkerCount
        )
        with self.sessionFactory() as session:
            statement = (
                select(SentenceVector)
                .where(
                    SentenceVector.tsp_id == tspId,
                    SentenceVector.is_current.is_(True),
                    SentenceVector.global_id > afterGlobalId,
                )
                .order_by(SentenceVector.global_id)
                .limit(pageSize)
            )
            return list(session.scalars(statement))

    def _saveKeys(self, rows: list):
        """Create sentence keys and update one historical database page."""

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
        """Retry one historical worker batch up to the configured limit."""

        label = f"HISTORICAL global_id {rows[0].global_id}-{rows[-1].global_id}"
        return retryOperation(
            label,
            self._processWorker,
            rows,
            waitSeconds=self.config.retrySeconds,
        )

    def _processWorker(self, rows: list) -> int:
        """Write missing catalog entries and every historical occurrence."""

        embeddings = self._loadEmbeddings([row.global_id for row in rows])
        existingKeys = self.opensearch.existingIds(
            self.config.catalogAlias,
            [row.sentence_key for row in rows],
        )

        withVectors = []
        needVectors = []
        seenKeys = set()
        for row in rows:
            if row.sentence_key in existingKeys or row.sentence_key in seenKeys:
                continue
            seenKeys.add(row.sentence_key)
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

        # Documents with a saved Titan vector bypass the embedding pipeline.
        # Only documents without a saved vector call the Bedrock pipeline.
        self.opensearch.bulkIndex(
            self.config.catalogAlias,
            withVectors,
            needsEmbedding=False,
        )
        self.opensearch.bulkIndex(
            self.config.catalogAlias,
            needVectors,
            needsEmbedding=True,
        )

        occurrences = []
        for row in rows:
            occurrences.append(
                {
                    "_id": row.global_id,
                    "globalId": row.global_id,
                    "applicationId": row.application_id,
                    "tspId": str(row.tsp_id),
                    "sectionName": row.section_name,
                    "sentIdLocal": row.sent_id_local,
                    "sentenceContent": row.sentence_content,
                    "sentenceKey": row.sentence_key,
                    "sourceType": row.source_type,
                    "analysisGroup": row.analysis_group,
                    "isNonMatcher": row.is_non_matcher,
                    "isTracer": row.is_tracer,
                    "isFormLanguage": row.is_form_language,
                    "createdAt": row.created_at.isoformat(),
                    "updatedAt": row.updated_at.isoformat(),
                }
            )
        self.opensearch.bulkIndex(
            self.config.occurrenceAlias,
            occurrences,
            needsEmbedding=False,
        )
        return len(rows)

    def _loadEmbeddings(self, globalIds: list) -> dict:
        """Read saved Titan type-2 vectors for one worker batch."""

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

    def _publish(self, tspId: int):
        """Send the completed historical document to Sentence Similarity."""

        if not self.config.nextQueueUrl:
            return

        body = json.dumps({"tsp_id": tspId, "reason": "HISTORICAL"})
        parameters = {
            "QueueUrl": self.config.nextQueueUrl,
            "MessageBody": body,
        }
        if self.config.nextQueueUrl.endswith(".fifo"):
            parameters["MessageGroupId"] = str(tspId)
            parameters["MessageDeduplicationId"] = (
                f"doc2vec-historical-{tspId}-"
                f"{uuid.uuid5(uuid.NAMESPACE_URL, body)}"
            )[:128]
        self.sqsClient.send_message(**parameters)
