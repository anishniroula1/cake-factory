from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, load_only

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import TspId
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


class SentenceVectorDBStore:
    """Load sentence-vector rows used by the similarity eligibility check."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_document_sentence_batch(
        self, tsp_id: TspId, after_global_id: int
    ) -> list[SentencesVector]:
        """Load the next unfiltered sentence page in global-ID order.

        Input: tsp_id=8205, after_global_id=39234
        Output: ordered SentencesVector rows beginning after global ID 39234,
        such as a page containing global IDs 39235 through 39264.

        Word count and all classification flags are returned so the handler can
        report why rows are skipped before starting any worker threads.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.tsp_id,
                        SentencesVector.sentence_key,
                        SentencesVector.word_count,
                        SentencesVector.is_non_matcher,
                        SentencesVector.is_tracer,
                        SentencesVector.is_form_language,
                    ),
                    joinedload(SentencesVector.tsp).load_only(
                        TextSimilarityInput.tsp_id,
                        TextSimilarityInput.application_id,
                        TextSimilarityInput.analysis_group,
                    ),
                )
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                )
                .order_by(SentencesVector.global_id)
                .limit(constants.DATABASE_SENTENCE_LOAD_BATCH_SIZE)
            )
            return list(session.scalars(statement))
