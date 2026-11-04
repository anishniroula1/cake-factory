from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import MatchTarget, TitanMatchRelationship
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger

UPSERT_BIDIRECTIONAL_TITAN_MATCHES = text(
    """
    SELECT tsp.upsert_bidirectional_titan_matches(
        :source_global_id,
        :source_tsp_id,
        :source_application_id,
        :matches
    )
    """
).bindparams(bindparam("matches", type_=JSONB))


class SourceSentenceMatches(TypedDict):
    """Hold one source sentence and the target matches sent to PostgreSQL."""

    tsp_id: int
    application_id: int
    matches: dict[str, MatchTarget]


class TitanMatchesDBStore:
    """Persist bidirectional sentence relationships through the PostgreSQL function."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_match_relationships(
        self, match_relationships: list[TitanMatchRelationship]
    ) -> int:
        """Save all collected matches after OpenSearch processing is complete.

        Relationships are grouped by source global ID because the PostgreSQL
        function accepts one source sentence and its target matches per call.
        Five workers save different source sentences concurrently.

        Example: source 100 matches targets 200 and 201. One function call
        receives both targets and also creates 200 -> 100 and 201 -> 100.
        """

        source_sentence_matches = self._group_matches_by_source_sentence(match_relationships)
        if not source_sentence_matches:
            return 0

        ordered_source_matches = sorted(source_sentence_matches.items())
        with ThreadPoolExecutor(
            max_workers=constants.TITAN_MATCH_WRITE_WORKER_COUNT,
            thread_name_prefix="titan-match-write",
        ) as executor:
            saved_counts = executor.map(
                lambda source_and_matches: self._save_source_sentence_matches(*source_and_matches),
                ordered_source_matches,
            )
            return sum(saved_counts)

    @staticmethod
    def _group_matches_by_source_sentence(
        match_relationships: list[TitanMatchRelationship],
    ) -> dict[int, SourceSentenceMatches]:
        """Group targets under the source global ID expected by the SQL function.

        If the same source-target pair appears more than once, its final
        distance and target information replace the earlier value.
        """

        grouped_matches: dict[int, SourceSentenceMatches] = {}
        for relationship in match_relationships:
            source_global_id = relationship["source_global_id"]
            source_matches = grouped_matches.setdefault(
                source_global_id,
                {
                    "tsp_id": relationship["source_tsp_id"],
                    "application_id": relationship["source_application_id"],
                    "matches": {},
                },
            )
            source_matches["matches"][str(relationship["target_global_id"])] = {
                "tsp_id": relationship["target_tsp_id"],
                "application_id": relationship["target_application_id"],
                "distance": relationship["distance"],
            }
        return grouped_matches

    def _save_source_sentence_matches(
        self,
        source_global_id: int,
        source_sentence_matches: SourceSentenceMatches,
    ) -> int:
        """Save one source sentence's matches in groups of at most 1,000.

        Calls for the same source stay in order. This prevents two workers from
        writing separate chunks for the same source sentence at the same time.
        """

        match_items = list(source_sentence_matches["matches"].items())
        saved_match_count = 0
        for offset in range(0, len(match_items), constants.TITAN_MATCH_WRITE_BATCH_SIZE):
            match_batch = dict(match_items[offset : offset + constants.TITAN_MATCH_WRITE_BATCH_SIZE])
            batch_number = (offset // constants.TITAN_MATCH_WRITE_BATCH_SIZE) + 1
            saved_match_count += retry_operation(
                f"Save Titan matches for source global_id={source_global_id}, batch {batch_number}",
                self._call_bidirectional_match_function,
                source_global_id,
                source_sentence_matches["tsp_id"],
                source_sentence_matches["application_id"],
                match_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        return saved_match_count

    def _call_bidirectional_match_function(
        self,
        source_global_id: int,
        source_tsp_id: int,
        source_application_id: int,
        matches: dict[str, MatchTarget],
    ) -> int:
        """Call PostgreSQL for one source sentence and commit both directions."""

        try:
            with self.session_factory() as session:
                processed_match_count = session.execute(
                    UPSERT_BIDIRECTIONAL_TITAN_MATCHES,
                    {
                        "source_global_id": source_global_id,
                        "source_tsp_id": source_tsp_id,
                        "source_application_id": source_application_id,
                        "matches": matches,
                    },
                ).scalar_one()
                session.commit()
                return int(processed_match_count)
        except IntegrityError as error:
            # Do not include the SQL statement or the complete matches object
            # in the error that reaches application logs.
            raise PermanentDataError(
                "Titan matches were rejected for "
                f"source global_id={source_global_id}: {safe_error_summary(error)}"
            ) from None
