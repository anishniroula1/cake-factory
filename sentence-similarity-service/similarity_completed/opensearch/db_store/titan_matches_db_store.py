from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import MatchTarget, TitanMatchRelationship
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger

# The existing PostgreSQL function accepts one source sentence and up to 1000
# target matches. It writes both relationship directions in one transaction.
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

    def save_bidirectional_match_relationships(
        self, match_relationships: list[TitanMatchRelationship]
    ) -> int:
        """Save collected relationships in batches of 1000 after searching ends.

        Example: relationships 100 -> 200 and 100 -> 232 are grouped under
        source 100 and sent as one p_matches object containing targets 200 and
        232. The method returns 2; PostgreSQL also saves 200 -> 100 and
        232 -> 100.
        """

        if not match_relationships:
            return 0

        relationships_by_source: dict[tuple[int, int, int], dict[str, MatchTarget]] = {}
        for relationship in match_relationships:
            source_identity = (
                relationship["source_global_id"],
                relationship["source_tsp_id"],
                relationship["source_application_id"],
            )
            relationships_by_source.setdefault(source_identity, {})[
                str(relationship["target_global_id"])
            ] = {
                "tsp_id": relationship["target_tsp_id"],
                "application_id": relationship["target_application_id"],
                "distance": relationship["distance"],
            }

        saved_relationship_count = 0
        for source_identity in sorted(relationships_by_source):
            source_global_id, source_tsp_id, source_application_id = source_identity
            match_items = list(relationships_by_source[source_identity].items())
            match_batches = [
                dict(match_items[offset : offset + constants.TITAN_MATCH_WRITE_BATCH_SIZE])
                for offset in range(0, len(match_items), constants.TITAN_MATCH_WRITE_BATCH_SIZE)
            ]
            for batch_number, match_batch in enumerate(match_batches, start=1):
                saved_relationship_count += retry_operation(
                    f"Save Titan match batch {batch_number} of {len(match_batches)} "
                    f"for source global_id={source_global_id}",
                    self._save_bidirectional_match_batch,
                    source_global_id,
                    source_tsp_id,
                    source_application_id,
                    match_batch,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
        return saved_relationship_count

    def _save_bidirectional_match_batch(
        self,
        source_global_id: int,
        source_tsp_id: int,
        source_application_id: int,
        match_batch: dict[str, MatchTarget],
    ) -> int:
        """Commit one source sentence's bounded match batch.

        Input: one source identity and up to 1000 target matches.
        Output: the number of eligible relationships handled by PostgreSQL.
        """

        try:
            with self.session_factory() as session:
                processed_relationship_count = session.execute(
                    SAVE_MATCHES,
                    {
                        "source_global_id": source_global_id,
                        "source_tsp_id": source_tsp_id,
                        "source_application_id": source_application_id,
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
                f"Titan match write rejected for source global_id={source_global_id}: "
                f"{safe_error_summary(error)}"
            ) from None
