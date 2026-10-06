CREATE SCHEMA IF NOT EXISTS test_matches;


/*
 * Count the keys in a JSONB match object.
 *
 * Input:
 * {
 *   "10": {"tsp_id": 100, "a_number": 200, "distance": 0.0},
 *   "40": {"tsp_id": 400, "a_number": 500, "distance": 0.12}
 * }
 *
 * Output: 2
 */
CREATE OR REPLACE FUNCTION test_matches.sentence_match_object_count(
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
 * Input:
 * {
 *   "10": {"tsp_id": 100, "a_number": 200, "distance": 0.0},
 *   "40": {"tsp_id": 400, "a_number": 500, "distance": 0.12}
 * }
 *
 * Output: {10}
 */
CREATE OR REPLACE FUNCTION test_matches.sentence_match_object_verbatim_ids(
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
        array_agg(match_item.global_id ORDER BY match_item.global_id),
        ARRAY[]::INTEGER[]
    )
    FROM (
        SELECT match_key::INTEGER AS global_id
        FROM jsonb_each(match_values) AS item(match_key, match_value)
        WHERE (match_value ->> 'distance')::DOUBLE PRECISION = 0.0
    ) AS match_item;
$$;


/*
 * Store matches as an object keyed by global ID.
 *
 * Incoming values replace saved values for the same global-ID key. This means
 * distance, tsp_id, and a_number are updated without creating duplicates.
 * PostgreSQL recalculates match_count and verbatim after every change.
 */
CREATE TABLE IF NOT EXISTS test_matches.sentence_matches (
    global_id INTEGER PRIMARY KEY,
    tsp_id INTEGER NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,

    match_count INTEGER GENERATED ALWAYS AS (
        test_matches.sentence_match_object_count(matches)
    ) STORED NOT NULL,

    verbatim INTEGER[] GENERATED ALWAYS AS (
        test_matches.sentence_match_object_verbatim_ids(matches)
    ) STORED NOT NULL,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_sentence_matches_json_object
        CHECK (jsonb_typeof(matches) = 'object')
);


CREATE INDEX IF NOT EXISTS ix_sentence_matches_tsp_id
ON test_matches.sentence_matches (tsp_id);


/*
 * Save key-value matches in both directions.
 *
 * Source 1 with target 10 creates or updates:
 *
 * Row 1:
 * {
 *   "10": {"tsp_id": 1010, "a_number": 654321, "distance": 0.10}
 * }
 *
 * Row 10:
 * {
 *   "1": {"tsp_id": 1001, "a_number": 123456, "distance": 0.10}
 * }
 *
 * JSONB object merge uses the incoming value when a key already exists. A
 * later distance, tsp_id, or a_number therefore replaces the earlier value.
 */
CREATE OR REPLACE FUNCTION test_matches.upsert_bidirectional_sentence_matches(
    p_source_global_id INTEGER,
    p_source_tsp_id INTEGER,
    p_source_a_number INTEGER,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
SET search_path = test_matches, public
AS $$
DECLARE
    normalized_matches JSONB;
    processed_match_count INTEGER;
    locked_owner_id BIGINT;
    source_match_value JSONB;
BEGIN
    /*
     * Remove a self-match. JSONB objects already guarantee one value per key,
     * so no duplicate scan, DISTINCT, or sort is required.
     */
    normalized_matches :=
        COALESCE(p_matches, '{}'::JSONB)
        - p_source_global_id::TEXT;

    processed_match_count :=
        sentence_match_object_count(normalized_matches);

    source_match_value := jsonb_build_object(
        'tsp_id', p_source_tsp_id,
        'a_number', p_source_a_number
    );

    /* Lock all affected owners in stable global-ID order. */
    FOR locked_owner_id IN
        SELECT DISTINCT owner_global_id::BIGINT
        FROM (
            SELECT p_source_global_id AS owner_global_id

            UNION

            SELECT match_key::INTEGER
            FROM jsonb_object_keys(normalized_matches) AS item(match_key)
        ) AS owners
        ORDER BY owner_global_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_owner_id);
    END LOOP;

    /*
     * Save the forward values. The || operator adds new keys and completely
     * replaces the value of an existing key with the incoming value.
     */
    INSERT INTO test_matches.sentence_matches (
        global_id,
        tsp_id,
        matches
    )
    VALUES (
        p_source_global_id,
        p_source_tsp_id,
        normalized_matches
    )
    ON CONFLICT (global_id)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        matches = sentence_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /*
     * Create or update every reverse owner. Each target gets the source global
     * ID as its key and receives the same distance in the reverse direction.
     */
    INSERT INTO test_matches.sentence_matches (
        global_id,
        tsp_id,
        matches
    )
    SELECT
        match_key::INTEGER,
        (match_value ->> 'tsp_id')::INTEGER,
        jsonb_build_object(
            p_source_global_id::TEXT,
            source_match_value || jsonb_build_object(
                'distance',
                (match_value ->> 'distance')::DOUBLE PRECISION
            )
        )
    FROM jsonb_each(normalized_matches)
        AS item(match_key, match_value)
    ORDER BY match_key::INTEGER
    ON CONFLICT (global_id)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        matches = sentence_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION test_matches.sentence_match_object_count(JSONB)
IS
'Counts the global-ID keys in a sentence match JSONB object.';

COMMENT ON FUNCTION test_matches.sentence_match_object_verbatim_ids(JSONB)
IS
'Builds the stored integer[] of exact-match global-ID keys.';

COMMENT ON FUNCTION test_matches.upsert_bidirectional_sentence_matches(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Creates or updates bidirectional sentence matches stored by global-ID key.';
