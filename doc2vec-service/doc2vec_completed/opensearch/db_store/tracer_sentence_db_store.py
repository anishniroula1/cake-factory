from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from ta_db_connection.atables import TracerSentence


class TracerSentenceDBStore:
    """Read tracer, form-language, and non-matcher reference sentences."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_tracer_sentences_by_ids(
        self,
        tracer_sentence_ids: list[int],
    ) -> list[TracerSentence]:
        """Load requested tracer rows in tracer ID order.

        Input: tracer_sentence_ids=[10, 20]
        Output: matching TracerSentence rows ordered as 10, then 20.
        """

        if not tracer_sentence_ids:
            return []

        with self.session_factory() as session:
            statement = (
                select(TracerSentence)
                .where(TracerSentence.tracer_sentence_id.in_(tracer_sentence_ids))
                .order_by(TracerSentence.tracer_sentence_id)
            )
            return list(session.scalars(statement))
