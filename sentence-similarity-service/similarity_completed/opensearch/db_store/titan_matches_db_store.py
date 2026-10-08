from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import MatchTarget
from similarity_completed.opensearch.retry import retry_operation
from ta_db_connection.atatables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger

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
    def __init__(self, config: Config, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        """Keep database operations for the titan_matches table."""

        self.config = config
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_matches(self, source: SentencesVector, matches: dict[str, MatchTarget]) -> int:
        """Save compact bidirectional matches through the bucket function.

        Input: source global_id=100 and matches={200: {distance: 0.08}}.
        Output: number of relationships processed by PostgreSQL.
        """

        # No database row is needed when no relationship was found.
        if not matches:
            return 0

        items = list(matches.items())
        match_batches = [
            dict(items[offset : offset + self.config.titan_match_write_batch_size])
            for offset in range(0, len(items), self.config.titan_match_write_batch_size)
        ]
        saved = 0
        for batch_number, match_batch in enumerate(match_batches, start=1):
            saved += retry_operation(
                f"Save Titan match batch {batch_number} for source global ID {source.global_id}",
                self._save_match_batch,
                source,
                match_batch,
                wait_seconds=self.config.retry_seconds,
                custom_logger=self.logger,
            )
        return saved

    def _save_match_batch(self, source: SentencesVector, match_batch: dict[str, MatchTarget]) -> int:
        """Write one match batch so only this batch is repeated after failure."""

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
            return int(processed)
