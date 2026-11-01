from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import MatchTarget
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
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
    """Persist bidirectional sentence relationships in Titan match buckets."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_sentence_matches(
        self, source_sentence: SentencesVector, eligible_target_occurrences: dict[str, MatchTarget]
    ) -> int:
        """Persist bidirectional relationships through the PostgreSQL bucket
        function. Large results use bounded writes, and each call stores both
        relationship directions.

        Input: source_sentence.global_id=39205, eligible_target_occurrences={"50001": {"tsp_id": 9001,
        "application_id": 200, "distance": 0.08}}
        Output: 1, the number of target relationships processed.

        The output counts the supplied targets. PostgreSQL also writes the
        reverse 50001-to-39205 relationship without increasing this count.
        """

        # No database row is needed when no relationship was found.
        if not eligible_target_occurrences:
            return 0

        match_items = list(eligible_target_occurrences.items())
        match_batches = [
            dict(match_items[offset : offset + constants.TITAN_MATCH_WRITE_BATCH_SIZE])
            for offset in range(0, len(match_items), constants.TITAN_MATCH_WRITE_BATCH_SIZE)
        ]
        saved_relationship_count = 0
        for batch_number, match_batch in enumerate(match_batches, start=1):
            saved_relationship_count += retry_operation(
                f"Save Titan match batch {batch_number} for source global_id={source_sentence.global_id}",
                self._save_bidirectional_match_batch,
                source_sentence,
                match_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        return saved_relationship_count

    def _save_bidirectional_match_batch(
        self, source_sentence: SentencesVector, match_batch: dict[str, MatchTarget]
    ) -> int:
        """Persist one relationship batch in a dedicated transaction. This is
        the retry boundary for transient PostgreSQL errors such as deadlocks.

        Input: source_sentence.global_id=39205 and a match_batch containing 500 targets.
        Output: 500, the count reported by the PostgreSQL function.
        """

        try:
            with self.session_factory() as session:
                processed_relationship_count = session.execute(
                    SAVE_MATCHES,
                    {
                        "source_global_id": source_sentence.global_id,
                        "source_tsp_id": source_sentence.tsp_id,
                        "source_application_id": source_sentence.tsp.application_id,
                        "matches": match_batch,
                    },
                ).scalar_one()
                session.commit()
                return int(processed_relationship_count)
        except IntegrityError as error:
            # A constraint failure is a data problem, not a transient database
            # outage. Keep PostgreSQL's missing-key/constraint explanation, but
            # do not retain SQLAlchemy's statement and matches parameter in the
            # displayed exception chain.
            raise PermanentDataError(
                f"Titan match write rejected for source global_id={source_sentence.global_id}: "
                f"{safe_error_summary(error)}"
            ) from None
