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

# One statement saves up to 1000 relationships from different source sentences.
# It creates both directions, groups changes by bucket, and merges JSONB without
# removing matches that are already stored in the bucket.
SAVE_MATCH_RELATIONSHIPS = text(
    """
    WITH raw_relationships AS MATERIALIZED (
        SELECT
            (item.value ->> 'source_global_id')::INTEGER AS source_global_id,
            (item.value ->> 'source_tsp_id')::INTEGER AS source_tsp_id,
            (item.value ->> 'source_application_id')::INTEGER AS source_application_id,
            (item.value ->> 'target_global_id')::INTEGER AS target_global_id,
            (item.value ->> 'target_tsp_id')::INTEGER AS target_tsp_id,
            (item.value ->> 'target_application_id')::INTEGER AS target_application_id,
            (item.value ->> 'distance')::DOUBLE PRECISION AS distance,
            item.position AS relationship_position
        FROM jsonb_array_elements(
            COALESCE(:relationships, '[]'::JSONB)
        ) WITH ORDINALITY AS item(value, position)
    ),
    source_relationships AS MATERIALIZED (
        /* Keep the final value if the same source-target pair appears twice. */
        SELECT DISTINCT ON (source_global_id, target_global_id)
            source_global_id,
            source_tsp_id,
            source_application_id,
            target_global_id,
            target_tsp_id,
            target_application_id,
            distance,
            relationship_position
        FROM raw_relationships
        WHERE source_global_id <> target_global_id
          AND source_application_id <> target_application_id
        ORDER BY
            source_global_id,
            target_global_id,
            relationship_position DESC
    ),
    both_directions AS MATERIALIZED (
        /* Forward relationship: source global ID owns the target match. */
        SELECT
            source_global_id AS owner_global_id,
            source_tsp_id AS owner_tsp_id,
            source_application_id AS owner_application_id,
            target_global_id AS matching_global_id,
            distance,
            relationship_position
        FROM source_relationships

        UNION ALL

        /* Reverse relationship: target global ID owns the source match. */
        SELECT
            target_global_id AS owner_global_id,
            target_tsp_id AS owner_tsp_id,
            target_application_id AS owner_application_id,
            source_global_id AS matching_global_id,
            distance,
            relationship_position
        FROM source_relationships
    ),
    unique_relationships AS MATERIALIZED (
        /* A pair can arrive in both directions. Keep its final supplied value. */
        SELECT DISTINCT ON (owner_global_id, matching_global_id)
            owner_global_id,
            owner_tsp_id,
            owner_application_id,
            matching_global_id,
            distance
        FROM both_directions
        ORDER BY
            owner_global_id,
            matching_global_id,
            relationship_position DESC
    ),
    lock_ids AS MATERIALIZED (
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    'titan_matches',
                    owner_global_id,
                    tsp.titan_match_bucket(matching_global_id)
                ),
                0
            ) AS lock_id
        FROM unique_relationships
    ),
    acquired_locks AS MATERIALIZED (
        /* Every writer acquires bucket locks in the same order. */
        SELECT pg_advisory_xact_lock(lock_id) AS acquired
        FROM lock_ids
        ORDER BY lock_id
    ),
    lock_guard AS MATERIALIZED (
        /* Referencing this count guarantees all locks run before the insert. */
        SELECT COUNT(*) AS lock_count
        FROM acquired_locks
    ),
    bucket_updates AS MATERIALIZED (
        /* Make one JSONB patch for each owner and match bucket. */
        SELECT
            owner_global_id,
            owner_tsp_id,
            owner_application_id,
            tsp.titan_match_bucket(matching_global_id) AS bucket_number,
            jsonb_object_agg(
                matching_global_id::TEXT,
                to_jsonb(distance)
                ORDER BY matching_global_id
            ) AS matches
        FROM unique_relationships
        GROUP BY
            owner_global_id,
            owner_tsp_id,
            owner_application_id,
            tsp.titan_match_bucket(matching_global_id)
    ),
    saved_buckets AS (
        INSERT INTO tsp.titan_matches (
            global_id,
            tsp_id,
            application_id,
            match_bucket_number,
            matches
        )
        SELECT
            bucket.owner_global_id,
            bucket.owner_tsp_id,
            bucket.owner_application_id,
            bucket.bucket_number,
            bucket.matches
        FROM bucket_updates AS bucket
        CROSS JOIN lock_guard
        ORDER BY bucket.owner_global_id, bucket.bucket_number
        ON CONFLICT (global_id, match_bucket_number)
        DO UPDATE SET
            tsp_id = EXCLUDED.tsp_id,
            application_id = EXCLUDED.application_id,
            matches = titan_matches.matches || EXCLUDED.matches,
            last_modified = NOW()
        RETURNING 1
    )
    SELECT (SELECT COUNT(*) FROM source_relationships)::INTEGER
    FROM (SELECT COUNT(*) FROM saved_buckets) AS completed_write
    """
).bindparams(bindparam("relationships", type_=JSONB))


class TitanMatchesDBStore:
    """Persist bidirectional sentence relationships in Titan match buckets."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_match_relationships(
        self, match_relationships: list[TitanMatchRelationship]
    ) -> int:
        """Save collected relationships in batches of 1000 after searching ends.

        Example: relationships 100 -> 200 and 101 -> 200 are sent together.
        PostgreSQL also saves 200 -> 100 and 200 -> 101. The method returns 2.
        """

        if not match_relationships:
            return 0

        # Deduplicate before batching. The final value replaces an earlier
        # value for the same source-target pair, matching JSONB merge behavior.
        unique_relationships: dict[tuple[int, int], TitanMatchRelationship] = {}
        for relationship in match_relationships:
            relationship_identity = (
                relationship["source_global_id"],
                relationship["target_global_id"],
            )
            unique_relationships[relationship_identity] = relationship

        relationships = list(unique_relationships.values())
        relationship_batches = [
            relationships[offset : offset + constants.TITAN_MATCH_WRITE_BATCH_SIZE]
            for offset in range(0, len(relationships), constants.TITAN_MATCH_WRITE_BATCH_SIZE)
        ]
        saved_relationship_count = 0
        for batch_number, relationship_batch in enumerate(relationship_batches, start=1):
            saved_relationship_count += retry_operation(
                f"Save Titan relationship batch {batch_number} of {len(relationship_batches)}",
                self._save_bidirectional_match_batch,
                relationship_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        return saved_relationship_count

    def _save_bidirectional_match_batch(
        self,
        relationship_batch: list[TitanMatchRelationship],
    ) -> int:
        """Commit up to 1000 relationships from any source sentences.

        Input: source 100 -> target 200 and source 101 -> target 200.
        Output: the number of eligible relationships handled by PostgreSQL.
        """

        try:
            with self.session_factory() as session:
                processed_relationship_count = session.execute(
                    SAVE_MATCH_RELATIONSHIPS,
                    {"relationships": relationship_batch},
                ).scalar_one()
                session.commit()
                return int(processed_relationship_count)
        except IntegrityError as error:
            # A constraint failure is a data problem, not a transient database
            # outage. Keep PostgreSQL's missing-key/constraint explanation, but
            # do not retain SQLAlchemy's statement and matches parameter in the
            # displayed exception chain.
            raise PermanentDataError(
                "Titan relationship batch was rejected: "
                f"{safe_error_summary(error)}"
            ) from None
