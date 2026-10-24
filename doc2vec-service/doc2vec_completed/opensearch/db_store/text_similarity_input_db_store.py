from collections.abc import Callable

from sqlalchemy.orm import Session, load_only

from doc2vec_completed.opensearch.contracts import TspId
from ta_db_connection.atables import TextSimilarityInput


class TextSimilarityInputDBStore:
    """Load the document fields required by live Doc2Vec processing."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_document(self, tsp_id: TspId) -> TextSimilarityInput | None:
        """Load only the document fields required by live processing.

        Input: tsp_id=1040
        Output: the matching TextSimilarityInput row, or None when it is absent.
        """

        with self.session_factory() as session:
            return session.get(
                TextSimilarityInput,
                tsp_id,
                options=[
                    load_only(
                        TextSimilarityInput.tsp_id,
                        TextSimilarityInput.application_id,
                        TextSimilarityInput.a_number,
                        TextSimilarityInput.content,
                        TextSimilarityInput.source_type,
                        TextSimilarityInput.section_name,
                        TextSimilarityInput.analysis_group,
                        TextSimilarityInput.is_current,
                    )
                ],
            )
