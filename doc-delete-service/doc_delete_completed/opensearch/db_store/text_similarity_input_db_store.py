from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ta_db_connection.atatables import TextSimilarityInput


class TextSimilarityInputDBStore:
    """Load documents that the state machine marked ready for deletion."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self.session_factory = session_factory

    def load_ready_for_deletion_tsp_ids(self, after_tsp_id: int | None, batch_size: int) -> list[int]:
        """Load one ordered page of documents waiting for deletion.

        Input: after_tsp_id=100, batch_size=200
        Output: up to 200 tsp_id values greater than 100.

        The ordered page lets one scheduled run continue through the table
        without loading every deletion record into memory at once.
        """

        with self.session_factory() as session:
            statement = (
                select(TextSimilarityInput.tsp_id)
                .where(TextSimilarityInput.document_state == "READY_FOR_DELETION")
                .order_by(TextSimilarityInput.tsp_id)
                .limit(batch_size)
            )
            if after_tsp_id is not None:
                statement = statement.where(TextSimilarityInput.tsp_id > after_tsp_id)
            return list(session.scalars(statement))
