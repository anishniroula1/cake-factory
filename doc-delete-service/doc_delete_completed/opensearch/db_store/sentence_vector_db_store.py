from collections.abc import Callable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from doc_delete_completed.opensearch.contracts import DocumentDeletionContext, TspId
from ta_db_connection.atatables import SentencesVector


class SentenceVectorDBStore:
    """Read and delete sentence rows owned by one document."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_document_from_sentence_vector(self, tsp_id: TspId) -> DocumentDeletionContext:
        """Load the sentence identifiers required by the deletion flow.

        Input: tsp_id=8205
        Output: the document's analysis group, global IDs, and sentence keys.
        """

        with self.session_factory() as session:
            rows = session.execute(
                select(
                    SentencesVector.global_id,
                    SentencesVector.sentence_key,
                    SentencesVector.analysis_group,
                )
                .where(SentencesVector.tsp_id == tsp_id)
                .order_by(SentencesVector.global_id)
            ).all()
            return DocumentDeletionContext(
                tsp_id=tsp_id,
                analysis_group=str(rows[0].analysis_group) if rows else None,
                global_ids=[int(row.global_id) for row in rows],
                sentence_keys=[str(row.sentence_key) for row in rows if row.sentence_key],
            )

    def delete_document_sentences(self, tsp_id: TspId) -> int:
        """Delete every sentences_vector row owned by one tsp_id."""

        with self.session_factory() as session:
            deleted_count = int(
                session.execute(delete(SentencesVector).where(SentencesVector.tsp_id == tsp_id)).rowcount or 0
            )
            session.commit()
            return deleted_count
