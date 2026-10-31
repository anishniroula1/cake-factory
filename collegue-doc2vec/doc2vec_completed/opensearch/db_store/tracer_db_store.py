from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from ta_db_connection.atatables import TracerSentence


class TracerDBStore:
    """Provide database operations for Tracer records."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def retrieve_tracer_from_tracer_id(
        self,
        tracer_id: int,
    ) -> TracerSentence | None:
        """Retrieve one TracerSentence record by its primary-key tracer_id.

        Input: tracer_id=10
        Output: the matching Tracer record, or None when absent.
        """

        with self.session_factory() as session:
            statement = (
                select(TracerSentence)
                .options(
                    load_only(
                        TracerSentence.tracer_id,
                        TracerSentence.tracer_content,
                        TracerSentence.titan_embedding_list,
                        TracerSentence.analysis_group,
                        TracerSentence.is_non_matcher,
                        TracerSentence.is_form_language,
                    )
                )
                .where(TracerSentence.tracer_id == tracer_id)
            )
            records = session.scalars(statement).all()
            return records[0] if records else None
