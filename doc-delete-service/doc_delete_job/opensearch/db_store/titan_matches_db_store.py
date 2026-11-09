from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from doc_delete_job.opensearch.contracts import TitanMatchDeletionResult, TspId

# This calls the PostgreSQL function installed by DELETE_TITAN_MATCHES.sql.
# The function finds every titan_matches row owned by the supplied tsp_id and
# removes those sentence relationships in both directions.
#
# Example:
#   tsp_id 10 owns global IDs 100 and 101.
#   Global ID 100 contains {"200": 0.10} in its matches object.
#   Because matching is bidirectional, global ID 200 contains
#   {"100": 0.10} in one of its own bucket rows.
#
# Calling the function with tsp_id=10 does the following:
#   1. Locks the affected bucket rows so another worker cannot modify them
#      during this deletion.
#   2. Removes keys "100" and "101" from other sentences' matches objects.
#   3. Deletes reverse bucket rows that become empty after those keys are
#      removed.
#   4. Deletes every Titan bucket row owned by tsp_id 10.
#
# The function returns only the values used by this service:
#   has_deleted_document:       true when the requested tsp_id was present.
#   deleted_match_bucket_count: bucket rows owned by the deleted document.
#
# It does not delete embeddings, or sentences_vector.
# Their cleanup is intentionally handled by their own stores.
DELETE_TITAN_MATCHES = text("SELECT * FROM tsp.delete_titan_matches_for_tsp(:tsp_id)")


class TitanMatchesDBStore:
    """Run the database function that removes bidirectional Titan matches."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def delete_bidirectional_document_matches(self, tsp_id: TspId) -> TitanMatchDeletionResult:
        """Delete source buckets and reverse references for one document.

        Input: tsp_id=8205
        Output: whether the document existed in titan_matches and how many
        source bucket rows were deleted.
        """

        with self.session_factory() as session:
            deletion_counts_record = session.execute(DELETE_TITAN_MATCHES, {"tsp_id": tsp_id}).one()
            session.commit()
            return TitanMatchDeletionResult(
                has_deleted_document=bool(deletion_counts_record.has_deleted_document),
                deleted_match_bucket_count=int(deletion_counts_record.deleted_match_bucket_count),
            )
