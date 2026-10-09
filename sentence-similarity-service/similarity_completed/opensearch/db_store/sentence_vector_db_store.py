from collections.abc import Callable

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, joinedload, load_only

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import Classification, TspId
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


class SentenceVectorDBStore:
    def __init__(self, config: Config, session_factory: Callable[[], Session]) -> None:
        """Keep database operations for the sentences_vector table."""

        self.config = config
        self.session_factory = session_factory

    def count_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count sentence rows belonging to one document.

        Input: tsp_id=100
        Output: 30
        """

        with self.session_factory() as session:
            statement = select(func.count()).select_from(SentencesVector).where(SentencesVector.tsp_id == tsp_id)
            return int(session.scalar(statement) or 0)

    def load_sentence_batch_by_tsp_id(self, tsp_id: TspId, after_global_id: int) -> list[SentencesVector]:
        """Load the next sentence batch and its shared document values.

        Input: tsp_id=100 and after_global_id=500
        Output: up to 30 sentences with global_id greater than 500.
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
                .limit(self.config.source_sentence_batch_size)
            )
            return list(session.scalars(statement))

    def update_classification_by_global_ids(self, global_ids: list[int], classification: Classification) -> None:
        """Save one tracer decision to the matching sentence rows.

        Input: global_ids=[100, 200] and one tracer classification.
        Output: both sentences_vector rows contain the same decision.
        """

        if not global_ids:
            return

        batch_size = self.config.database_classification_batch_size
        for offset in range(0, len(global_ids), batch_size):
            id_batch = global_ids[offset : offset + batch_size]
            with self.session_factory() as session:
                statement = (
                    update(SentencesVector)
                    .where(SentencesVector.global_id.in_(id_batch))
                    .values(
                        is_non_matcher=classification["is_non_matcher"],
                        is_tracer=classification["is_tracer"],
                        is_form_language=classification["is_form_language"],
                        last_modified=func.now(),
                    )
                )
                session.execute(statement)
                session.commit()
