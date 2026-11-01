from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker

from doc_delete_job.opensearch import constants
from doc_delete_job.opensearch.contracts import TspId
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
                .where(TextSimilarityInput.document_state == constants.READY_FOR_DELETION_DOCUMENT_STATE)
                .order_by(TextSimilarityInput.tsp_id)
                .limit(batch_size)
            )
            if after_tsp_id is not None:
                statement = statement.where(TextSimilarityInput.tsp_id > after_tsp_id)
            return list(session.scalars(statement))

    def update_document_state(self, tsp_id: TspId, document_state: str) -> int:
        """Update the retained text_similarity_input row for one document.

        Input: tsp_id=8205, document_state="DOC_DELETED"
        Output: 1 when the document row was updated.
        """

        with self.session_factory() as session:
            update_result = session.execute(
                update(TextSimilarityInput)
                .where(TextSimilarityInput.tsp_id == tsp_id)
                .values(document_state=document_state)
            )
            session.commit()
            return int(update_result.rowcount or 0)
