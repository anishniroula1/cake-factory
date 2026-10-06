CREATE SCHEMA IF NOT EXISTS test_matches;


/*
 * Place a matching global ID into one of 32 stable buckets.
 *
 * The owner global ID does not choose the bucket. The matching global ID
 * chooses it, so the same relationship always updates the same bucket row.
 *
 * Input:  global ID 97
 * Output: bucket 1 because 97 % 32 = 1
 */
CREATE OR REPLACE FUNCTION test_matches.titan_global_id_match_bucket(
    matching_global_id INTEGER
)
RETURNS SMALLINT
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = test_matches, public
AS $$
    SELECT mod(abs(matching_global_id::BIGINT), 32)::SMALLINT;
$$;


/* Count the global-ID keys stored in one bucket. */
CREATE OR REPLACE FUNCTION test_matches.titan_bucket_match_count(
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


/* Return the exact-match global IDs stored in one bucket. */
CREATE OR REPLACE FUNCTION test_matches.titan_bucket_verbatim_ids(
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
 * One owner can have up to 32 rows instead of one continuously growing row.
 *
 * Example:
 * global_id | match_bucket_number | matches
 * ----------+---------------------+---------------------------
 * 1         | 10                  | {"10": 0.0, "42": 0.12}
 * 1         | 11                  | {"11": 0.2, "43": 0.09}
 */
CREATE TABLE IF NOT EXISTS test_matches.titan_key_match_buckets (
    global_id INTEGER NOT NULL,
    tsp_id INTEGER NOT NULL,
    application_id INTEGER NOT NULL,
    match_bucket_number SMALLINT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,

    matches_count INTEGER GENERATED ALWAYS AS (
        test_matches.titan_bucket_match_count(matches)
    ) STORED NOT NULL,

    verbatim INTEGER[] GENERATED ALWAYS AS (
        test_matches.titan_bucket_verbatim_ids(matches)
    ) STORED NOT NULL,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (global_id, match_bucket_number),

    CONSTRAINT ck_titan_key_match_buckets_number
        CHECK (match_bucket_number BETWEEN 0 AND 31),

    CONSTRAINT ck_titan_key_match_buckets_json_object
        CHECK (jsonb_typeof(matches) = 'object')
);


CREATE INDEX IF NOT EXISTS ix_titan_key_match_buckets_tsp_id
ON test_matches.titan_key_match_buckets (tsp_id, global_id);


CREATE INDEX IF NOT EXISTS ix_titan_key_match_buckets_application_id
ON test_matches.titan_key_match_buckets (application_id, global_id);


/*
 * Save cross-application relationships in both directions.
 *
 * The caller sends target identity details so missing reverse rows can be
 * created:
 * {
 *   "10": {
 *     "tsp_id": 1010,
 *     "application_id": 5010,
 *     "distance": 0.0
 *   }
 * }
 *
 * Only {"10": 0.0} is stored. Targets from the source application and the
 * source global ID itself are removed before anything is written.
 */
CREATE OR REPLACE FUNCTION test_matches.upsert_bidirectional_titan_key_match_buckets(
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
    processed_match_count INTEGER;
    locked_row_id BIGINT;
BEGIN
    /* Keep only targets belonging to another application. */
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

    processed_match_count := titan_bucket_match_count(eligible_targets);

    /*
     * Lock only the bucket rows affected by this call. Different buckets for
     * the same owner can therefore be updated by different workers.
     */
    FOR locked_row_id IN
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    'titan_key_match_buckets',
                    affected.owner_global_id,
                    affected.bucket_number
                ),
                0
            ) AS lock_id
        FROM (
            /* Forward bucket rows owned by the source. */
            SELECT
                p_source_global_id AS owner_global_id,
                titan_global_id_match_bucket(target.match_key::INTEGER)
                    AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)

            UNION

            /* Reverse bucket rows owned by each target. */
            SELECT
                target.match_key::INTEGER AS owner_global_id,
                titan_global_id_match_bucket(p_source_global_id)
                    AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)
        ) AS affected
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /*
     * Group the source's targets into bucket patches. At most 32 source rows
     * are rewritten even when the input contains thousands of matches.
     */
    INSERT INTO test_matches.titan_key_match_buckets (
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
        titan_global_id_match_bucket(target.match_key::INTEGER),
        jsonb_object_agg(
            target.match_key,
            target.match_value -> 'distance'
        )
    FROM jsonb_each(eligible_targets)
        AS target(match_key, match_value)
    GROUP BY titan_global_id_match_bucket(target.match_key::INTEGER)
    ORDER BY titan_global_id_match_bucket(target.match_key::INTEGER)
    ON CONFLICT (global_id, match_bucket_number)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_key_match_buckets.matches || EXCLUDED.matches,
        last_modified = NOW();

    /*
     * Save the reverse relationship in the bucket selected by the source
     * global ID. Only the numeric distance is stored in the JSONB object.
     */
    INSERT INTO test_matches.titan_key_match_buckets (
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
        titan_global_id_match_bucket(p_source_global_id),
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
        matches = titan_key_match_buckets.matches || EXCLUDED.matches,
        last_modified = NOW();

    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION test_matches.titan_global_id_match_bucket(INTEGER)
IS
'Returns the stable 0-31 bucket selected by a matching global ID.';


COMMENT ON FUNCTION test_matches.upsert_bidirectional_titan_key_match_buckets(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Creates bidirectional cross-application matches in small JSONB bucket rows.';
