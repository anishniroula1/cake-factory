/*
 * Save sentence matches in both directions.
 *
 * The Sentence Similarity service calls this function for one source sentence.
 * Python can send up to 1000 target matches in p_matches per call.
 *
 * Example input:
 *
 *   source global_id:      100
 *   source tsp_id:         10
 *   source application_id: 1
 *
 *   matches:
 *   {
 *     "200": {"tsp_id": 20, "application_id": 2, "distance": 0.10},
 *     "232": {"tsp_id": 30, "application_id": 3, "distance": 0.0}
 *   }
 *
 * The function saves these relationships:
 *
 *   100 -> 200 at distance 0.10
 *   100 -> 232 at distance 0.0
 *   200 -> 100 at distance 0.10
 *   232 -> 100 at distance 0.0
 *
 * Required existing database objects:
 *
 *   tsp.titan_matches
 *   tsp.titan_match_bucket(INTEGER)
 *   tsp.titan_match_object_size(JSONB)
 */
CREATE OR REPLACE FUNCTION tsp.upsert_bidirectional_titan_matches(
    p_source_global_id INTEGER,
    p_source_tsp_id INTEGER,
    p_source_application_id INTEGER,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
AS $$
DECLARE
    eligible_targets JSONB;
    processed_match_count INTEGER;
    locked_row_id BIGINT;
BEGIN
    /*
     * Remove self-matches and matches from the source application.
     * OpenSearch already excludes them, but PostgreSQL checks again before
     * saving the relationships.
     */
    SELECT COALESCE(
        jsonb_object_agg(
            target.match_key,
            jsonb_build_object(
                'tsp_id',
                (target.match_value ->> 'tsp_id')::INTEGER,
                'application_id',
                (target.match_value ->> 'application_id')::INTEGER,
                'distance',
                (target.match_value ->> 'distance')::DOUBLE PRECISION
            )
        ),
        '{}'::JSONB
    )
    INTO eligible_targets
    FROM jsonb_each(COALESCE(p_matches, '{}'::JSONB))
        AS target(match_key, match_value)
    WHERE target.match_key::INTEGER <> p_source_global_id
      AND (target.match_value ->> 'application_id')::INTEGER
          <> p_source_application_id;

    processed_match_count :=
        tsp.titan_match_object_size(eligible_targets);

    /* Do not create an empty row when no eligible relationship exists. */
    IF processed_match_count = 0 THEN
        RETURN 0;
    END IF;

    /*
     * Lock every forward and reverse bucket in the same order. This prevents
     * two workers from overwriting each other and reduces deadlock risk.
     */
    FOR locked_row_id IN
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    'titan_matches',
                    affected.owner_global_id,
                    affected.bucket_number
                ),
                0
            ) AS lock_id
        FROM (
            /* Forward rows owned by the source sentence. */
            SELECT
                p_source_global_id AS owner_global_id,
                tsp.titan_match_bucket(target.match_key::INTEGER)
                    AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)

            UNION

            /* Reverse rows owned by each target sentence. */
            SELECT
                target.match_key::INTEGER AS owner_global_id,
                tsp.titan_match_bucket(p_source_global_id)
                    AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)
        ) AS affected
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /* Save source global_id -> target global_id relationships. */
    INSERT INTO tsp.titan_matches (
        global_id,
        tsp_id,
        application_id,
        match_bucket_number,
        matches
    )
    SELECT
        p_source_global_id,
        p_source_tsp_id,
        p_source_application_id,
        tsp.titan_match_bucket(target.match_key::INTEGER),
        jsonb_object_agg(
            target.match_key,
            target.match_value -> 'distance'
        )
    FROM jsonb_each(eligible_targets)
        AS target(match_key, match_value)
    GROUP BY tsp.titan_match_bucket(target.match_key::INTEGER)
    ORDER BY tsp.titan_match_bucket(target.match_key::INTEGER)
    ON CONFLICT (global_id, match_bucket_number)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /* Save target global_id -> source global_id relationships. */
    INSERT INTO tsp.titan_matches (
        global_id,
        tsp_id,
        application_id,
        match_bucket_number,
        matches
    )
    SELECT
        target.match_key::INTEGER,
        (target.match_value ->> 'tsp_id')::INTEGER,
        (target.match_value ->> 'application_id')::INTEGER,
        tsp.titan_match_bucket(p_source_global_id),
        jsonb_build_object(
            p_source_global_id::TEXT,
            (target.match_value ->> 'distance')::DOUBLE PRECISION
        )
    FROM jsonb_each(eligible_targets)
        AS target(match_key, match_value)
    ORDER BY target.match_key::INTEGER
    ON CONFLICT (global_id, match_bucket_number)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /* Reverse copies are not counted again. */
    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION tsp.upsert_bidirectional_titan_matches(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Saves cross-application Titan matches in both directions using 16 buckets.';


/*
 * Runnable example:
 *
 * SELECT tsp.upsert_bidirectional_titan_matches(
 *     100,
 *     10,
 *     1,
 *     '{
 *       "200": {"tsp_id": 20, "application_id": 2, "distance": 0.10},
 *       "232": {"tsp_id": 30, "application_id": 3, "distance": 0.0}
 *     }'::JSONB
 * );
 *
 * Expected return value: 2
 */
