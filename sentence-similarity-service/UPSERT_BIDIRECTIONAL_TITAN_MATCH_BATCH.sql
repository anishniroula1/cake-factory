/*
 * Save a complete document's sentence relationships with one database call.
 *
 * Python sends a JSONB list instead of calling PostgreSQL once for every
 * source sentence. For example:
 *
 *   [
 *     {
 *       "source_global_id": 100,
 *       "source_tsp_id": 10,
 *       "source_application_id": 1,
 *       "target_global_id": 200,
 *       "target_tsp_id": 20,
 *       "target_application_id": 2,
 *       "distance": 0.10
 *     },
 *     {
 *       "source_global_id": 100,
 *       "source_tsp_id": 10,
 *       "source_application_id": 1,
 *       "target_global_id": 201,
 *       "target_tsp_id": 21,
 *       "target_application_id": 3,
 *       "distance": 0.04
 *     }
 *   ]
 *
 * PostgreSQL creates both directions, groups them by owner and bucket, and
 * updates each affected bucket once. Existing JSONB keys are preserved. If a
 * supplied key already exists, its latest distance replaces the old distance.
 *
 * The INSERT reads bucket patches in one stable order. ON CONFLICT protects
 * each row while it is merged, so this batch function does not acquire one
 * advisory lock per bucket. A large document can affect thousands of buckets,
 * and holding thousands of advisory locks in one transaction could exhaust
 * PostgreSQL's shared lock table.
 */
CREATE OR REPLACE FUNCTION tsp.upsert_bidirectional_titan_match_batch(
    p_relationships JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
AS $$
DECLARE
    processed_match_count INTEGER;
BEGIN
    WITH raw_relationships AS MATERIALIZED (
        /* Turn every JSONB list item into typed PostgreSQL columns. */
        SELECT
            (item.value ->> 'source_global_id')::INTEGER
                AS source_global_id,
            (item.value ->> 'source_tsp_id')::INTEGER
                AS source_tsp_id,
            (item.value ->> 'source_application_id')::INTEGER
                AS source_application_id,
            (item.value ->> 'target_global_id')::INTEGER
                AS target_global_id,
            (item.value ->> 'target_tsp_id')::INTEGER
                AS target_tsp_id,
            (item.value ->> 'target_application_id')::INTEGER
                AS target_application_id,
            (item.value ->> 'distance')::DOUBLE PRECISION
                AS distance,
            item.position AS relationship_position
        FROM jsonb_array_elements(
            COALESCE(p_relationships, '[]'::JSONB)
        ) WITH ORDINALITY AS item(value, position)
    ),
    source_relationships AS MATERIALIZED (
        /*
         * Remove self-matches and same-application matches. If the same pair
         * appears more than once, keep the final value from the input list.
         */
        SELECT DISTINCT ON (
            source_global_id,
            target_global_id
        )
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
        /* Forward copy: source global ID owns the target global ID. */
        SELECT
            source_global_id AS owner_global_id,
            source_tsp_id AS owner_tsp_id,
            source_application_id AS owner_application_id,
            target_global_id AS matching_global_id,
            distance,
            relationship_position
        FROM source_relationships

        UNION ALL

        /* Reverse copy: target global ID owns the source global ID. */
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
        /* A pair can arrive in both directions. Keep its final distance. */
        SELECT DISTINCT ON (
            owner_global_id,
            matching_global_id
        )
            owner_global_id,
            owner_tsp_id,
            owner_application_id,
            matching_global_id,
            distance,
            relationship_position
        FROM both_directions
        ORDER BY
            owner_global_id,
            matching_global_id,
            relationship_position DESC
    ),
    bucket_patches AS MATERIALIZED (
        /*
         * Build one JSONB change for each owner and bucket. This is the main
         * performance improvement: one bucket is merged once even when many
         * source sentences contributed matches to it.
         */
        SELECT
            owner_global_id,
            (array_agg(
                owner_tsp_id
                ORDER BY relationship_position DESC
            ))[1] AS owner_tsp_id,
            (array_agg(
                owner_application_id
                ORDER BY relationship_position DESC
            ))[1] AS owner_application_id,
            tsp.titan_match_bucket(matching_global_id)
                AS match_bucket_number,
            jsonb_object_agg(
                matching_global_id::TEXT,
                to_jsonb(distance)
                ORDER BY matching_global_id
            ) AS matches
        FROM unique_relationships
        GROUP BY
            owner_global_id,
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
            owner_global_id,
            owner_tsp_id,
            owner_application_id,
            match_bucket_number,
            matches
        FROM bucket_patches
        /* Every concurrent writer touches rows in the same order. */
        ORDER BY owner_global_id, match_bucket_number
        ON CONFLICT (global_id, match_bucket_number)
        DO UPDATE SET
            matches = titan_matches.matches || EXCLUDED.matches,
            last_modified = NOW()
        /* Do not rewrite a bucket when every supplied distance is unchanged. */
        WHERE titan_matches.matches IS DISTINCT FROM
              titan_matches.matches || EXCLUDED.matches
        RETURNING 1
    )
    SELECT
        (SELECT COUNT(*) FROM source_relationships)::INTEGER
    INTO processed_match_count
    /* Reading this count guarantees the data-changing CTE is completed. */
    FROM (
        SELECT COUNT(*) AS saved_bucket_count
        FROM saved_buckets
    ) AS completed_write;

    /* Reverse copies are not counted again. */
    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION tsp.upsert_bidirectional_titan_match_batch(JSONB)
IS
'Saves a JSONB list of sentence relationships in both directions with one set-based bucket merge.';


/*
 * Runnable batch example:
 *
 * SELECT tsp.upsert_bidirectional_titan_match_batch(
 *     '[
 *       {
 *         "source_global_id": 100,
 *         "source_tsp_id": 10,
 *         "source_application_id": 1,
 *         "target_global_id": 200,
 *         "target_tsp_id": 20,
 *         "target_application_id": 2,
 *         "distance": 0.10
 *       },
 *       {
 *         "source_global_id": 100,
 *         "source_tsp_id": 10,
 *         "source_application_id": 1,
 *         "target_global_id": 201,
 *         "target_tsp_id": 21,
 *         "target_application_id": 3,
 *         "distance": 0.04
 *       }
 *     ]'::JSONB
 * );
 *
 * Expected return value: 2
 */
