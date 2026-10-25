from collections.abc import Callable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from doc_delete_completed.opensearch.contracts import DocumentDeletionContext, TspId
from ta_db_connection.atatables import SentencesVector


class SentenceVectorDBStore:
    """Read and delete sentence rows owned by one document."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_document_sentence_context(self, tsp_id: TspId) -> DocumentDeletionContext:
        """Load the sentence identifiers required by the deletion flow.

        Input: tsp_id=8205
        Output: the document's analysis group, global IDs, and sentence keys.
        """

        with self.session_factory() as session:
            sentence_vector_records = session.execute(
                select(SentencesVector.global_id, SentencesVector.sentence_key, SentencesVector.analysis_group)
                .where(SentencesVector.tsp_id == tsp_id)
                .order_by(SentencesVector.global_id)
            ).all()
            return DocumentDeletionContext(
                tsp_id=tsp_id,
                analysis_group=(str(sentence_vector_records[0].analysis_group) if sentence_vector_records else None),
                global_ids=[
                    int(sentence_vector_record.global_id) for sentence_vector_record in sentence_vector_records
                ],
                sentence_keys=[
                    str(sentence_vector_record.sentence_key)
                    for sentence_vector_record in sentence_vector_records
                    if sentence_vector_record.sentence_key
                ],
            )

    def delete_document_sentence_records(self, tsp_id: TspId) -> int:
        """Delete every sentences_vector row owned by one tsp_id."""

        with self.session_factory() as session:
            deleted_count = int(
                session.execute(delete(SentencesVector).where(SentencesVector.tsp_id == tsp_id)).rowcount or 0
            )
            session.commit()
            return deleted_count
