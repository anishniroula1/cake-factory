from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import MatchTarget
from similarity_completed.opensearch.models import SentencesVector

# PostgreSQL selects one of 16 buckets, creates reverse owner rows, and writes
# both relationship directions inside the same transaction.
SAVE_MATCHES = text(
    """
    SELECT tsp.upsert_bidirectional_titan_matches(
        :source_global_id,
        :source_tsp_id,
        :source_application_id,
        :matches
    )
    """
).bindparams(bindparam("matches", type_=JSONB))


class TitanMatchesDBStore:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
    ) -> None:
        """Keep database operations for the titan_matches table."""

        self.config = config
        self.session_factory = session_factory

    def save_bidirectional_matches(
        self,
        source: SentencesVector,
        matches: dict[str, MatchTarget],
    ) -> int:
        """Save compact bidirectional matches through the bucket function.

        Input: source global_id=100 and matches={200: {distance: 0.08}}.
        Output: number of relationships processed by PostgreSQL.
        """

        # No database row is needed when no relationship was found.
        if not matches:
            return 0

        items = list(matches.items())
        match_batches = [
            dict(
                items[
                    offset : offset + self.config.titan_match_write_batch_size
                ]
            )
            for offset in range(
                0,
                len(items),
                self.config.titan_match_write_batch_size,
            )
        ]
        saved = 0
        for match_batch in match_batches:
            with self.session_factory() as session:
                processed = session.execute(
                    SAVE_MATCHES,
                    {
                        "source_global_id": source.global_id,
                        "source_tsp_id": source.tsp_id,
                        "source_application_id": source.tsp.application_id,
                        "matches": match_batch,
                    },
                ).scalar_one()
                session.commit()
                saved += int(processed)
        return saved
