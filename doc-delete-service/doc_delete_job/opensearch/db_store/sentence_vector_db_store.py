from collections.abc import Callable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from doc_delete_job.opensearch.contracts import DocumentDeletionContext, TspId
from ta_db_connection.atatables import SentencesVector


class SentenceVectorDBStore:
    """Read and delete sentence rows owned by one document."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_document_sentence_context(self, tsp_id: TspId) -> DocumentDeletionContext:
        """Load the sentence global IDs required for embedding deletion.

        Input: tsp_id=8205
        Output: the document's global IDs.
        """

        with self.session_factory() as session:
            sentence_vector_records = session.execute(
                select(SentencesVector.global_id)
                .where(SentencesVector.tsp_id == tsp_id)
                .order_by(SentencesVector.global_id)
            ).all()
            return DocumentDeletionContext(
                global_ids=[
                    int(sentence_vector_record.global_id) for sentence_vector_record in sentence_vector_records
                ]
            )

    def delete_document_sentence_records(self, tsp_id: TspId) -> int:
        """Delete every sentences_vector row owned by one tsp_id."""

        with self.session_factory() as session:
            deleted_count = int(
                session.execute(delete(SentencesVector).where(SentencesVector.tsp_id == tsp_id)).rowcount or 0
            )
            session.commit()
            return deleted_count
