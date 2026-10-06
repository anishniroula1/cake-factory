CREATE SCHEMA IF NOT EXISTS test_matches;


/*
 * Count the global-ID keys in the stored match object.
 *
 * Input:  {"10": 0.0, "40": 0.12}
 * Output: 2
 */
CREATE OR REPLACE FUNCTION test_matches.titan_key_match_count(
    match_values JSONB
)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = test_matches, public
AS $$
    SELECT count(*)::INTEGER
    FROM jsonb_object_keys(match_values);
$$;


/*
 * Return the global-ID keys whose distance is exactly 0.0.
 *
 * Input:  {"10": 0.0, "40": 0.12}
 * Output: {10}
 */
CREATE OR REPLACE FUNCTION test_matches.titan_key_match_verbatim_ids(
    match_values JSONB
)
RETURNS INTEGER[]
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = test_matches, public
AS $$
    SELECT COALESCE(
        array_agg(exact_match.global_id ORDER BY exact_match.global_id),
        ARRAY[]::INTEGER[]
    )
    FROM (
        SELECT match_key::INTEGER AS global_id
        FROM jsonb_each(match_values) AS item(match_key, distance_value)
        WHERE (distance_value #>> '{}')::DOUBLE PRECISION = 0.0
    ) AS exact_match;
$$;


/*
 * Store only target global ID and distance in matches.
 *
 * Example:
 * {
 *   "10": 0.0,
 *   "40": 0.12
 * }
 *
 * tsp_id and application_id describe the owner row. They are not repeated
 * inside every saved match value.
 */
CREATE TABLE IF NOT EXISTS test_matches.titan_key_matches (
    global_id INTEGER PRIMARY KEY,
    tsp_id INTEGER NOT NULL,
    application_id INTEGER NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,

    matches_count INTEGER GENERATED ALWAYS AS (
        test_matches.titan_key_match_count(matches)
    ) STORED NOT NULL,

    verbatim INTEGER[] GENERATED ALWAYS AS (
        test_matches.titan_key_match_verbatim_ids(matches)
    ) STORED NOT NULL,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_titan_key_matches_json_object
        CHECK (jsonb_typeof(matches) = 'object')
);


CREATE INDEX IF NOT EXISTS ix_titan_key_matches_tsp_id
ON test_matches.titan_key_matches (tsp_id);


CREATE INDEX IF NOT EXISTS ix_titan_key_matches_application_id
ON test_matches.titan_key_matches (application_id);


/*
 * Save eligible relationships in both directions.
 *
 * p_matches contains the target details needed to create reverse owner rows:
 * {
 *   "10": {
 *     "tsp_id": 1010,
 *     "application_id": 5010,
 *     "distance": 0.0
 *   }
 * }
 *
 * The table stores only:
 * Row 1:  {"10": 0.0}
 * Row 10: {"1": 0.0}
 *
 * A target is ignored when it has the same application_id as the source.
 * This prevents sentences from the same application from matching each other.
 */
CREATE OR REPLACE FUNCTION test_matches.upsert_bidirectional_titan_key_matches(
    p_source_global_id INTEGER,
    p_source_tsp_id INTEGER,
    p_source_application_id INTEGER,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
SET search_path = test_matches, public
AS $$
DECLARE
    eligible_targets JSONB;
    forward_matches JSONB;
    processed_match_count INTEGER;
    locked_owner_id BIGINT;
BEGIN
    /*
     * Keep one value per target global ID and remove both self-matches and
     * matches belonging to the source application.
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

    /* Reduce the rich input to the small global-ID-to-distance format. */
    SELECT COALESCE(
        jsonb_object_agg(
            target.match_key,
            target.match_value -> 'distance'
        ),
        '{}'::JSONB
    )
    INTO forward_matches
    FROM jsonb_each(eligible_targets)
        AS target(match_key, match_value);

    processed_match_count := titan_key_match_count(forward_matches);

    /*
     * Lock every affected owner in the same order across concurrent callers.
     * The table name is included so these locks do not conflict with another
     * bidirectional match table that happens to use the same global IDs.
     */
    FOR locked_owner_id IN
        SELECT DISTINCT
            hashtextextended(
                'titan_key_matches:' || owner.owner_global_id::TEXT,
                0
            ) AS lock_id
        FROM (
            SELECT p_source_global_id AS owner_global_id

            UNION

            SELECT target.match_key::INTEGER
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)
        ) AS owner
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_owner_id);
    END LOOP;

    /* Always create or refresh the source row, even with no eligible match. */
    INSERT INTO test_matches.titan_key_matches (
        global_id,
        tsp_id,
        application_id,
        matches
    )
    VALUES (
        p_source_global_id,
        p_source_tsp_id,
        p_source_application_id,
        forward_matches
    )
    ON CONFLICT (global_id)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_key_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /*
     * Create each target owner when missing and save the source global ID as
     * its reverse key. Only the numeric distance is stored in matches.
     */
    INSERT INTO test_matches.titan_key_matches (
        global_id,
        tsp_id,
        application_id,
        matches
    )
    SELECT
        target.match_key::INTEGER,
        (target.match_value ->> 'tsp_id')::INTEGER,
        (target.match_value ->> 'application_id')::INTEGER,
        jsonb_build_object(
            p_source_global_id::TEXT,
            (target.match_value ->> 'distance')::DOUBLE PRECISION
        )
    FROM jsonb_each(eligible_targets)
        AS target(match_key, match_value)
    ORDER BY target.match_key::INTEGER
    ON CONFLICT (global_id)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_key_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION test_matches.titan_key_match_count(JSONB)
IS
'Counts the global-ID keys in a titan_key_matches JSONB object.';


COMMENT ON FUNCTION test_matches.titan_key_match_verbatim_ids(JSONB)
IS
'Builds the stored integer[] of exact-match global-ID keys.';


COMMENT ON FUNCTION test_matches.upsert_bidirectional_titan_key_matches(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Creates bidirectional cross-application matches while storing only global ID and distance.';
