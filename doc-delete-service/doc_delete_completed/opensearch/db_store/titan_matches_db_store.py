from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from doc_delete_completed.opensearch.contracts import TitanMatchDeletionResult, TspId

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
# The function returns one row with four counts:
#   source_global_id_count:       source global IDs found for the tsp_id.
#   deleted_match_bucket_count:   bucket rows owned by the deleted document.
#   updated_reverse_bucket_count: other bucket rows with reverse keys removed.
#   deleted_empty_bucket_count:   reverse bucket rows deleted after becoming {}.
#
# It does not delete embeddings, or sentences_vector.
# Their cleanup is intentionally handled by their own stores.
DELETE_TITAN_MATCHES = text("SELECT * FROM tsp.delete_titan_matches_for_tsp(:tsp_id)")


class TitanMatchesDBStore:
    """Run the database function that removes bidirectional Titan matches."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def delete_document_matches(self, tsp_id: TspId) -> TitanMatchDeletionResult:
        """Delete source buckets and reverse references for one document.

        Input: tsp_id=8205
        Output: counts returned by tsp.delete_titan_matches_for_tsp.
        """

        with self.session_factory() as session:
            row = session.execute(DELETE_TITAN_MATCHES, {"tsp_id": tsp_id}).one()
            session.commit()
            return TitanMatchDeletionResult(
                source_global_id_count=int(row.source_global_id_count),
                deleted_match_bucket_count=int(row.deleted_match_bucket_count),
                updated_reverse_bucket_count=int(row.updated_reverse_bucket_count),
                deleted_empty_bucket_count=int(row.deleted_empty_bucket_count),
            )
