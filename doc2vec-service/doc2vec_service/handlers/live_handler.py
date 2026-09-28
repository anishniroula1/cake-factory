import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from doc2vec_service.batching import batches
from doc2vec_service.errors import PermanentDataError
from doc2vec_service.models import Embedding, SentenceVector, TextSimilarityInput
from doc2vec_service.retry import retryOperation
from doc2vec_service.text import sentenceKey, splitSentences

logger = logging.getLogger(__name__)


class LiveHandler:
    def __init__(self, config, sessionFactory, opensearch, sqsClient):
        """Keep everything required for a normal live Doc2Vec message."""

        self.config = config
        self.sessionFactory = sessionFactory
        self.opensearch = opensearch
        self.sqsClient = sqsClient

    def process(self, tspId: int) -> dict:
        """Split, save, index, and forward one incoming document."""

        document = retryOperation(
            f"load LIVE tsp_id {tspId}",
            self._loadDocument,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        if document is None:
            raise PermanentDataError(
                f"tsp.text_similarity_input has no tsp_id {tspId}"
            )

        # Replace splitSentences with the production sentence splitter when
        # this code is moved into the existing Doc2Vec service.
        sentences = splitSentences(document.content)
        if not sentences:
            raise PermanentDataError(f"tsp_id {tspId} has no sentence content")

        # Save PostgreSQL first. Each 1,000-row batch commits, so a long
        # document does not keep one large database transaction open.
        for sentenceBatch in batches(sentences, self.config.databaseBatchSize):
            retryOperation(
                f"save LIVE tsp_id {tspId} sentences",
                self._saveSentences,
                document,
                sentenceBatch,
                waitSeconds=self.config.retrySeconds,
            )

        indexed = self._indexSavedSentences(tspId)

        # Publish before the source message is deleted. This favors a harmless
        # duplicate over losing the handoff to Sentence Similarity.
        retryOperation(
            f"publish LIVE tsp_id {tspId}",
            self._publish,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        retryOperation(
            f"update LIVE tsp_id {tspId} state",
            self._updateState,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )

        return {
            "reason": "LIVE",
            "tspId": tspId,
            "sourceCount": len(sentences),
            "indexedCount": indexed,
        }

    def _loadDocument(self, tspId: int):
        """Load source text and document metadata from the TSI table."""

        with self.sessionFactory() as session:
            return session.get(TextSimilarityInput, tspId)

    def _saveSentences(self, document, sentences: list):
        """Upsert one sentence batch and commit its generated global IDs."""

        currentTime = datetime.now(UTC)
        values = []
        for sentence in sentences:
            values.append(
                {
                    "application_id": document.application_id,
                    "a_number": document.a_number,
                    "tsp_id": document.tsp_id,
                    "section_name": sentence.sectionName,
                    "is_non_matcher": False,
                    "is_tracer": False,
                    "is_form_language": False,
                    "sent_id_local": sentence.sentIdLocal,
                    "sentence_content": sentence.sentenceContent,
                    "source_type": document.source_type,
                    "analysis_group": document.analysis_group,
                    "is_current": True,
                    "sentence_key": sentenceKey(sentence.sentenceContent),
                    "created_at": currentTime,
                    "updated_at": currentTime,
                }
            )

        baseInsert = insert(SentenceVector).values(values)
        statement = baseInsert.on_conflict_do_update(
            index_elements=["tsp_id", "sent_id_local"],
            index_where=SentenceVector.is_current.is_(True),
            set_={
                "sentence_content": baseInsert.excluded.sentence_content,
                "sentence_key": baseInsert.excluded.sentence_key,
                "section_name": baseInsert.excluded.section_name,
                "updated_at": currentTime,
            },
        )
        with self.sessionFactory() as session:
            session.execute(statement)
            session.commit()

    def _indexSavedSentences(self, tspId: int) -> int:
        """Read saved rows and index them with 16 workers."""

        indexed = 0
        afterGlobalId = 0
        with ThreadPoolExecutor(
            max_workers=self.config.workerCount,
            thread_name_prefix="live-sentence",
        ) as executor:
            while True:
                rows = retryOperation(
                    f"load saved LIVE tsp_id {tspId}",
                    self._loadSentencePage,
                    tspId,
                    afterGlobalId,
                    waitSeconds=self.config.retrySeconds,
                )
                if not rows:
                    break
                workerBatches = list(batches(rows, self.config.workerBatchSize))
                indexed += sum(executor.map(self._runWorker, workerBatches))
                afterGlobalId = rows[-1].global_id
        return indexed

    def _loadSentencePage(self, tspId: int, afterGlobalId: int) -> list:
        """Read the next saved sentence page in global-ID order."""

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

    def _runWorker(self, rows: list) -> int:
        """Retry one 30-sentence OpenSearch worker until it succeeds."""

        label = f"LIVE global_id {rows[0].global_id}-{rows[-1].global_id}"
        return retryOperation(
            label,
            self._processWorker,
            rows,
            waitSeconds=self.config.retrySeconds,
        )

    def _processWorker(self, rows: list) -> int:
        """Write missing catalog entries and then every occurrence."""

        embeddings = self._loadEmbeddings([row.global_id for row in rows])
        existingKeys = self.opensearch.existingIds(
            self.config.catalogAlias,
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
        """Read saved Titan vectors for one worker batch."""

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
        """Send the completed document to Sentence Similarity."""

        if not self.config.nextQueueUrl:
            return
        body = json.dumps({"tsp_id": tspId, "reason": "LIVE"})
        parameters = {
            "QueueUrl": self.config.nextQueueUrl,
            "MessageBody": body,
        }
        if self.config.nextQueueUrl.endswith(".fifo"):
            parameters["MessageGroupId"] = str(tspId)
            parameters["MessageDeduplicationId"] = (
                f"doc2vec-{tspId}-{uuid.uuid5(uuid.NAMESPACE_URL, body)}"
            )[:128]
        self.sqsClient.send_message(**parameters)

    def _updateState(self, tspId: int):
        """Move the completed document to the Sentence Similarity state."""

        with self.sessionFactory() as session:
            statement = (
                update(TextSimilarityInput)
                .where(TextSimilarityInput.tsp_id == tspId)
                .values(document_state=self.config.nextDocumentState)
            )
            session.execute(statement)
            session.commit()
