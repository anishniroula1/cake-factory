CREATE SCHEMA IF NOT EXISTS test_matches;


/*
 * Return the unique global IDs whose distance is exactly 0.0.
 *
 * Input:
 * [{"global_id": 10, "distance": 0.0},
 *  {"global_id": 40, "distance": 0.12}]
 *
 * Output: {10}
 */
CREATE OR REPLACE FUNCTION test_matches.sentence_match_verbatim_ids(
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
        SELECT DISTINCT
            (match_item ->> 'global_id')::INTEGER AS global_id
        FROM jsonb_array_elements(match_values) AS item(match_item)
        WHERE (match_item ->> 'distance')::DOUBLE PRECISION = 0.0
    ) AS exact_match;
$$;


/* Merge by global_id and let the incoming object replace the saved object. */
CREATE OR REPLACE FUNCTION test_matches.sentence_match_merge_lists(
    existing_matches JSONB,
    incoming_matches JSONB
)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = test_matches, public
AS $$
    WITH combined_matches AS (
        SELECT
            saved_item AS match_item,
            0 AS incoming_priority
        FROM jsonb_array_elements(existing_matches) AS saved(saved_item)

        UNION ALL

        SELECT
            incoming_item AS match_item,
            1 AS incoming_priority
        FROM jsonb_array_elements(incoming_matches)
            AS incoming(incoming_item)
    ),
    latest_matches AS (
        SELECT DISTINCT ON (
            (match_item ->> 'global_id')::INTEGER
        )
            (match_item ->> 'global_id')::INTEGER AS match_global_id,
            match_item
        FROM combined_matches
        ORDER BY
            (match_item ->> 'global_id')::INTEGER,
            incoming_priority DESC
    )
    SELECT COALESCE(
        jsonb_agg(match_item ORDER BY match_global_id),
        '[]'::JSONB
    )
    FROM latest_matches;
$$;


/*
 * This is a new POC table. The caller writes the owner global ID, TSP ID,
 * and matches.
 * PostgreSQL maintains match_count, verbatim, and last_modified.
 */
CREATE TABLE IF NOT EXISTS test_matches.titan_matches (
    global_id INTEGER PRIMARY KEY,
    tsp_id INTEGER NOT NULL,
    matches JSONB NOT NULL DEFAULT '[]'::JSONB,

    match_count INTEGER GENERATED ALWAYS AS (
        jsonb_array_length(matches)
    ) STORED NOT NULL,

    verbatim INTEGER[] GENERATED ALWAYS AS (
        test_matches.sentence_match_verbatim_ids(matches)
    ) STORED NOT NULL,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_titan_matches_json_array
        CHECK (jsonb_typeof(matches) = 'array')
);


/*
 * Save relationships in both directions.
 *
 * Input source 1 with target 10 creates or updates:
 * - Row 1 with target 10 inside matches.
 * - Row 10 with source 1 inside matches.
 *
 * A missing owner row is inserted with its global_id and tsp_id.
 * An existing owner row keeps those two identity values and updates only
 * matches and last_modified. PostgreSQL regenerates match_count and verbatim.
 * An empty p_matches array still creates the source row.
 */
CREATE OR REPLACE FUNCTION test_matches.upsert_bidirectional_titan_matches(
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
    source_match JSONB;
BEGIN
    /* Remove self-matches, remove duplicates, and keep only saved fields. */
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'global_id', unique_match.global_id,
                'tsp_id', unique_match.tsp_id,
                'a_number', unique_match.a_number,
                'distance', unique_match.distance
            )
            ORDER BY unique_match.global_id
        ),
        '[]'::JSONB
    )
    INTO normalized_matches
    FROM (
        SELECT DISTINCT ON (
            (match_item ->> 'global_id')::INTEGER
        )
            (match_item ->> 'global_id')::INTEGER AS global_id,
            (match_item ->> 'tsp_id')::INTEGER AS tsp_id,
            (match_item ->> 'a_number')::INTEGER AS a_number,
            (match_item ->> 'distance')::DOUBLE PRECISION AS distance
        FROM jsonb_array_elements(
            COALESCE(p_matches, '[]'::JSONB)
        ) AS item(match_item)
        WHERE (match_item ->> 'global_id')::INTEGER
            <> p_source_global_id
        ORDER BY (match_item ->> 'global_id')::INTEGER
    ) AS unique_match;

    processed_match_count := jsonb_array_length(normalized_matches);

    source_match := jsonb_build_object(
        'global_id', p_source_global_id,
        'tsp_id', p_source_tsp_id,
        'a_number', p_source_a_number
    );

    /* Lock the source and target owners in the same order across workers. */
    FOR locked_owner_id IN
        SELECT DISTINCT owner_global_id::BIGINT
        FROM (
            SELECT p_source_global_id AS owner_global_id

            UNION

            SELECT (match_item ->> 'global_id')::INTEGER
            FROM jsonb_array_elements(normalized_matches)
                AS item(match_item)
        ) AS owners
        ORDER BY owner_global_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_owner_id);
    END LOOP;

    /* Always create the source row, even when it has no matches. */
    INSERT INTO test_matches.titan_matches (
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
        matches = sentence_match_merge_lists(
            titan_matches.matches,
            EXCLUDED.matches
        ),
        last_modified = NOW();

    /* Create missing target rows and add the reverse source relationship. */
    INSERT INTO test_matches.titan_matches (
        global_id,
        tsp_id,
        matches
    )
    SELECT
        (match_item ->> 'global_id')::INTEGER,
        (match_item ->> 'tsp_id')::INTEGER,
        jsonb_build_array(
            source_match || jsonb_build_object(
                'distance',
                (match_item ->> 'distance')::DOUBLE PRECISION
            )
        )
    FROM jsonb_array_elements(normalized_matches) AS item(match_item)
    ORDER BY (match_item ->> 'global_id')::INTEGER
    ON CONFLICT (global_id)
    DO UPDATE SET
        matches = sentence_match_merge_lists(
            titan_matches.matches,
            EXCLUDED.matches
        ),
        last_modified = NOW();

    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION test_matches.sentence_match_verbatim_ids(JSONB)
IS
'Builds the stored integer[] of exact-match global IDs from matches JSONB.';

COMMENT ON FUNCTION test_matches.upsert_bidirectional_titan_matches(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Creates missing owners and saves bidirectional matches without replacing owner IDs.';
