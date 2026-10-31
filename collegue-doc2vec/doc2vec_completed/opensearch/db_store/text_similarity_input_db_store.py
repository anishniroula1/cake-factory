from collections.abc import Callable

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch.contracts import TspId
from ta_db_connection.atatables import TextSimilarityInput


class TextSimilarityInputDBStore:
    """Provide database operations for text-similarity-input records."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def retrieve_document_from_tsp_id(
        self,
        tsp_id: TspId,
    ) -> TextSimilarityInput | None:
        """Retrieve one TextSimilarityInput record by its primary-key tsp_id.

        Input: tsp_id=1040
        Output: the matching TextSimilarityInput record, or None when absent.
        """

        with self.session_factory() as session:
            return session.get(TextSimilarityInput, tsp_id)

