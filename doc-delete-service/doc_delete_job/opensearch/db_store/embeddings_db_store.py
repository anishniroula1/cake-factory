from collections.abc import Callable

from sqlalchemy import delete
from sqlalchemy.orm import Session

from ta_db_connection.atatables import Embeddings


class EmbeddingsDBStore:
    """Delete saved embeddings that belong to removed sentence global IDs."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def delete_sentence_embeddings(self, global_ids: list[int]) -> int:
        """Delete embeddings for the supplied sentence global IDs.

        Input: global_ids=[100, 101]
        Output: number of deleted embedding rows.
        """

        if not global_ids:
            return 0
        with self.session_factory() as session:
            deleted_count = int(
                session.execute(delete(Embeddings).where(Embeddings.global_id.in_(global_ids))).rowcount or 0
            )
            session.commit()
            return deleted_count
