from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

from doc_delete_job.opensearch.contracts import TspId
from ta_db_connection.atatables import TextSimilarityInput


class TextSimilarityInputDBStore:
    """Update the retained text_similarity_input record after deletion."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self.session_factory = session_factory

    def update_document_state(self, tsp_id: TspId, document_state: str) -> int:
        """Save the completed deletion state for one document.

        Input: tsp_id=8205, document_state="DOC_DELETED"
        Output: 1 when one text_similarity_input record was updated.

        This method does not read or validate the document's current state.
        """

        with self.session_factory() as session:
            update_result = session.execute(
                update(TextSimilarityInput)
                .where(TextSimilarityInput.tsp_id == tsp_id)
                .values(document_state=document_state)
            )
            session.commit()
            return int(update_result.rowcount or 0)
