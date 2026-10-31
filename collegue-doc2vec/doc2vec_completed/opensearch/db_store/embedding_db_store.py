from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from ta_db_connection.atatables import Embeddings


class EmbeddingDBStore:
    """Read saved Titan embeddings that can be reused during indexing."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_titan_embeddings_by_global_ids(
        self,
        global_ids: list[int],
        embedding_type: int,
    ) -> dict[int, list[float]]:
        """Load saved Titan vectors for a group of sentence global IDs.

        Input: global_ids=[10, 20], embedding_type=2
        Output: {10: [512 vector values]} when only global ID 10 has a saved
        Titan embedding of the requested type.
        """

        if not global_ids:
            return {}

        with self.session_factory() as session:
            statement = select(Embeddings).where(
                Embeddings.global_id.in_(global_ids),
                Embeddings.embedding_type == embedding_type,
            )
            return {
                embedding_record.global_id: list(embedding_record.embeddings)
                for embedding_record in session.scalars(statement)
                if embedding_record.embeddings
            }

