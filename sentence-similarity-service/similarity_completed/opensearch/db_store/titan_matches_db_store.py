from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import TitanMatchRelationship
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger

UPSERT_BIDIRECTIONAL_TITAN_MATCH_BATCH = text(
    """
    SELECT tsp.upsert_bidirectional_titan_match_batch(
        :relationships
    )
    """
).bindparams(bindparam("relationships", type_=JSONB))


class TitanMatchesDBStore:
    """Persist all bidirectional sentence relationships with one set-based function call."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_match_relationships(
        self, match_relationships: list[TitanMatchRelationship]
    ) -> int:
        """Send the complete relationship list to PostgreSQL after searching.

        Input: relationships such as source 100 -> targets 200 and 201.
        PostgreSQL groups the complete list by owner and bucket, then saves
        100 -> 200, 100 -> 201, 200 -> 100, and 201 -> 100 together.
        """

        if not match_relationships:
            return 0

        return retry_operation(
            "Save all Titan relationships",
            self._call_bidirectional_match_batch_function,
            match_relationships,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

    def _call_bidirectional_match_batch_function(
        self,
        match_relationships: list[TitanMatchRelationship],
    ) -> int:
        """Commit one JSONB relationship list through the batch SQL function."""

        try:
            with self.session_factory() as session:
                processed_match_count = session.execute(
                    UPSERT_BIDIRECTIONAL_TITAN_MATCH_BATCH,
                    {"relationships": match_relationships},
                ).scalar_one()
                session.commit()
                return int(processed_match_count)
        except IntegrityError as error:
            # Do not include the SQL statement or the complete relationship
            # list in the error that reaches application logs.
            raise PermanentDataError(
                f"Titan matches were rejected: {safe_error_summary(error)}"
            ) from None
