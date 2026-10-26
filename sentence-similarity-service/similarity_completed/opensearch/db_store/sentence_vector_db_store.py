from collections.abc import Callable

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, joinedload, load_only

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import Classification, TspId
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


class SentenceVectorDBStore:
    """Provide database operations for sentence-vector records and classifications."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def count_document_sentences(self, tsp_id: TspId) -> int:
        """Count all sentence rows that belong to one document.

        Input: tsp_id=8205
        Output: 30, which means the document contains 30 sentence rows.
        """

        with self.session_factory() as session:
            statement = select(func.count()).select_from(SentencesVector).where(SentencesVector.tsp_id == tsp_id)
            return int(session.scalar(statement) or 0)

    def load_document_sentence_batch(self, tsp_id: TspId, after_global_id: int) -> list[SentencesVector]:
        """Load the next ordered page of sentences for a document. The global_id
        cursor limits memory use and keeps every page in deterministic order.

        Input: tsp_id=8205, after_global_id=39234
        Output: ordered SentencesVector rows beginning after global ID 39234,
        such as a page containing global IDs 39235 through 39264.

        Pass after_global_id=0 for the first page. The related
        TextSimilarityInput record is loaded with each sentence row.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    load_only(SentencesVector.global_id, SentencesVector.tsp_id, SentencesVector.sentence_key),
                    joinedload(SentencesVector.tsp).load_only(
                        TextSimilarityInput.tsp_id,
                        TextSimilarityInput.application_id,
                        TextSimilarityInput.analysis_group,
                    ),
                )
                .where(SentencesVector.tsp_id == tsp_id, SentencesVector.global_id > after_global_id)
                .order_by(SentencesVector.global_id)
                .limit(constants.SOURCE_SENTENCE_BATCH_SIZE)
            )
            return list(session.scalars(statement))

    def update_sentence_classification_by_global_ids(
        self, global_ids: list[int], classification: Classification
    ) -> None:
        """Persist one classification on the specified sentence rows. Rows are
        locked in global_id order and updated in batches to reduce deadlock risk.

        Input: global_ids=[39205, 39206], classification={"is_non_matcher": True,
        "is_tracer": False, "is_form_language": False}

        Both sentence rows receive the same classification flags and an updated
        last_modified timestamp.
        """

        if not global_ids:
            return

        # Every worker locks the same rows from the smallest global_id to the
        # largest. FOR NO KEY UPDATE keeps classification writers ordered but
        # stays compatible with foreign-key checks made by titan_matches.
        ordered_global_ids = sorted(set(global_ids))
        batch_size = constants.DATABASE_CLASSIFICATION_BATCH_SIZE
        for offset in range(0, len(ordered_global_ids), batch_size):
            global_id_batch = ordered_global_ids[offset : offset + batch_size]
            with self.session_factory() as session, session.begin():
                locked_global_ids = list(
                    session.scalars(
                        select(SentencesVector.global_id)
                        .where(SentencesVector.global_id.in_(global_id_batch))
                        .order_by(SentencesVector.global_id)
                        .with_for_update(key_share=True)
                    )
                )
                if not locked_global_ids:
                    continue

                statement = (
                    update(SentencesVector)
                    .where(SentencesVector.global_id.in_(locked_global_ids))
                    .values(
                        is_non_matcher=classification["is_non_matcher"],
                        is_tracer=classification["is_tracer"],
                        is_form_language=classification["is_form_language"],
                        last_modified=func.now(),
                    )
                )
                session.execute(statement)
