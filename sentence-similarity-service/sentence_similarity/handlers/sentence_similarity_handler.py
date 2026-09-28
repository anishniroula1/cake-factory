import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, text, update
from sqlalchemy.dialects.postgresql import JSONB

from sentence_similarity.errors import PermanentDataError
from sentence_similarity.models import SentenceVector, TextSimilarityInput
from sentence_similarity.retry import retryOperation

logger = logging.getLogger(__name__)

# PostgreSQL calculates buckets, locks affected rows in a stable order, and
# writes both directions. The worker only supplies one source key and a JSONB
# object containing matching keys and cosine distances.
SAVE_MATCHES = text(
    """
    SELECT upsert_bidirectional_sentence_key_matches(
        :analysisGroup,
        :sourceSentenceKey,
        :matches
    )
    """
).bindparams(bindparam("matches", type_=JSONB))

TERMS_BATCH_SIZE = 1_000  # Keys checked in one PostgreSQL IN query.


class SentenceSimilarityHandler:
    def __init__(self, config, sessionFactory, opensearch, sqsClient):
        """Keep everything required to process one tsp_id."""

        self.config = config
        self.sessionFactory = sessionFactory
        self.opensearch = opensearch
        self.sqsClient = sqsClient

    def process(self, tspId: int) -> dict:
        """Classify and match every current sentence for one tsp_id."""

        total = retryOperation(
            f"count tsp_id {tspId} sentences",
            self._countSentences,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        logger.info("tsp_id=%s contains %s current sentence(s)", tspId, total)

        processed = 0
        nonMatchers = 0
        savedRelationships = 0
        afterGlobalId = 0

        with ThreadPoolExecutor(
            max_workers=self.config.workerCount,
            thread_name_prefix="sentence-similarity",
        ) as executor:
            while True:
                rows = retryOperation(
                    f"load tsp_id {tspId} sentence page",
                    self._loadPage,
                    tspId,
                    afterGlobalId,
                    waitSeconds=self.config.retrySeconds,
                )
                if not rows:
                    break

                vectors = retryOperation(
                    f"load tsp_id {tspId} catalog vectors",
                    self.opensearch.catalogVectors,
                    [row.sentence_key for row in rows],
                    waitSeconds=self.config.retrySeconds,
                )
                missingKeys = sorted(
                    {
                        row.sentence_key
                        for row in rows
                        if row.sentence_key not in vectors
                    }
                )
                if missingKeys:
                    raise PermanentDataError(
                        f"catalog vector is missing for {missingKeys[0]}"
                    )

                jobs = []
                for row in rows:
                    jobs.append(
                        executor.submit(
                            self._runWorker,
                            row,
                            vectors[row.sentence_key],
                        )
                    )
                for job in jobs:
                    result = job.result()
                    processed += 1
                    nonMatchers += int(result["isNonMatcher"])
                    savedRelationships += result["savedRelationships"]

                afterGlobalId = rows[-1].global_id
                logger.info(
                    "tsp_id=%s processed through global_id=%s",
                    tspId,
                    afterGlobalId,
                )

        retryOperation(
            f"update tsp_id {tspId} state",
            self._updateDocumentState,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        retryOperation(
            f"publish tsp_id {tspId}",
            self._publish,
            tspId,
            waitSeconds=self.config.retrySeconds,
        )
        return {
            "tspId": tspId,
            "sourceCount": total,
            "processedCount": processed,
            "nonMatcherCount": nonMatchers,
            "savedRelationshipCount": savedRelationships,
        }

    def _runWorker(self, row, vector: list) -> dict:
        """Retry one complete sentence flow up to three times."""

        return retryOperation(
            f"global_id {row.global_id}",
            self._processSentence,
            row,
            vector,
            waitSeconds=self.config.retrySeconds,
        )

    def _processSentence(self, row, vector: list) -> dict:
        """Check tracer rules first and save eligible semantic matches."""

        classification = self.opensearch.tracerClassification(
            vector,
            row.analysis_group,
        )
        self._saveClassification(row.global_id, classification)
        self.opensearch.updateOccurrence(row.global_id, classification)

        if classification["is_non_matcher"]:
            return {"isNonMatcher": True, "savedRelationships": 0}

        matches = self.opensearch.catalogMatches(row.sentence_key, vector)
        eligibleKeys = self._eligibleCandidateKeys(row, list(matches))
        eligibleMatches = {
            key: distance
            for key, distance in matches.items()
            if key in eligibleKeys
        }
        saved = self._saveMatches(
            row.analysis_group,
            row.sentence_key,
            eligibleMatches,
        )
        return {"isNonMatcher": False, "savedRelationships": saved}

    def _countSentences(self, tspId: int) -> int:
        """Count current source sentences for the provided document."""

        with self.sessionFactory() as session:
            statement = select(func.count()).select_from(SentenceVector).where(
                SentenceVector.tsp_id == tspId,
                SentenceVector.is_current.is_(True),
            )
            return int(session.scalar(statement) or 0)

    def _loadPage(self, tspId: int, afterGlobalId: int) -> list:
        """Read the next 100 source rows without loading sentence content."""

        with self.sessionFactory() as session:
            statement = (
                select(SentenceVector)
                .where(
                    SentenceVector.tsp_id == tspId,
                    SentenceVector.is_current.is_(True),
                    SentenceVector.global_id > afterGlobalId,
                )
                .order_by(SentenceVector.global_id)
                .limit(self.config.sourcePageSize)
            )
            return list(session.scalars(statement))

    def _saveClassification(self, globalId: int, classification: dict):
        """Save one tracer decision to sentences_vector by global_id."""

        with self.sessionFactory() as session:
            statement = (
                update(SentenceVector)
                .where(SentenceVector.global_id == globalId)
                .values(
                    is_non_matcher=classification["is_non_matcher"],
                    is_tracer=classification["is_tracer"],
                    is_form_language=classification["is_form_language"],
                    updated_at=datetime.now(UTC),
                )
            )
            session.execute(statement)
            session.commit()

    def _eligibleCandidateKeys(self, sourceRow, candidateKeys: list) -> set:
        """Keep eligible keys found outside the source application."""

        eligible = set()
        uniqueKeys = list(dict.fromkeys(candidateKeys))
        for offset in range(0, len(uniqueKeys), TERMS_BATCH_SIZE):
            batch = uniqueKeys[offset : offset + TERMS_BATCH_SIZE]
            with self.sessionFactory() as session:
                statement = (
                    select(SentenceVector.sentence_key)
                    .where(
                        SentenceVector.sentence_key.in_(batch),
                        SentenceVector.analysis_group == sourceRow.analysis_group,
                        SentenceVector.application_id != sourceRow.application_id,
                        SentenceVector.is_current.is_(True),
                        SentenceVector.is_non_matcher.is_(False),
                        SentenceVector.is_tracer.is_(False),
                        SentenceVector.is_form_language.is_(False),
                    )
                    .distinct()
                )
                eligible.update(session.scalars(statement))
        return eligible

    def _saveMatches(
        self,
        analysisGroup: str,
        sourceSentenceKey: str,
        matches: dict,
    ) -> int:
        """Save matches in smaller commits through the bucket SQL function."""

        if not matches:
            return 0

        saved = 0
        items = list(matches.items())
        for offset in range(0, len(items), self.config.matchWriteBatchSize):
            matchBatch = dict(
                items[offset : offset + self.config.matchWriteBatchSize]
            )
            with self.sessionFactory() as session:
                processed = session.execute(
                    SAVE_MATCHES,
                    {
                        "analysisGroup": analysisGroup,
                        "sourceSentenceKey": sourceSentenceKey,
                        "matches": matchBatch,
                    },
                ).scalar_one()
                session.commit()
                saved += int(processed)
        return saved

    def _updateDocumentState(self, tspId: int):
        """Move the completed document to the API Transform state."""

        with self.sessionFactory() as session:
            statement = (
                update(TextSimilarityInput)
                .where(TextSimilarityInput.tsp_id == tspId)
                .values(document_state=self.config.nextDocumentState)
            )
            session.execute(statement)
            session.commit()

    def _publish(self, tspId: int):
        """Send the completed tsp_id to the next SQS queue."""

        body = json.dumps({"tsp_id": tspId})
        parameters = {
            "QueueUrl": self.config.nextQueueUrl,
            "MessageBody": body,
        }
        if self.config.nextQueueUrl.endswith(".fifo"):
            parameters["MessageGroupId"] = str(tspId)
            parameters["MessageDeduplicationId"] = (
                f"sentence-similarity-{tspId}-"
                f"{uuid.uuid5(uuid.NAMESPACE_URL, body)}"
            )[:128]
        self.sqsClient.send_message(**parameters)
