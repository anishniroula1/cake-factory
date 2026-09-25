/*
 * This POC keeps the existing production shape where each owner row stores a
 * JSONB list. Example: owner 1 stores [{"global_id": 10, "distance": 0.1}].
 * The function adds both 1-to-10 and 10-to-1 without reading the lists into
 * Python first.
 */


/* Merge by global_id and let the incoming object replace the saved object.
 * Example: saved B has distance 0.20 and incoming B has distance 0.10.
 * The result contains one B with distance 0.10, not two copies of B. */
CREATE OR REPLACE FUNCTION sentence_match_merge_lists(
    existing_matches JSONB,
    incoming_matches JSONB
)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    WITH combined_matches AS (
        /* Saved values have lower priority than incoming values. */
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
        /* Keep one object per global_id. Incoming values win. */
        SELECT DISTINCT ON (
            (match_item ->> 'global_id')::BIGINT
        )
            (match_item ->> 'global_id')::BIGINT AS match_global_id,
            match_item
        FROM combined_matches
        ORDER BY
            (match_item ->> 'global_id')::BIGINT,
            incoming_priority DESC
    )
    SELECT COALESCE(
        jsonb_agg(match_item ORDER BY match_global_id),
        '[]'::JSONB
    )
    FROM latest_matches;
$$;


/* One row belongs to one global ID and analysis group.
 * matches stays a JSONB list to copy the current production design. */
/* Keep an existing POC database usable after the table name change. */
DO $$
BEGIN
    IF to_regclass('matches') IS NULL
       AND to_regclass('sentence_match_lists') IS NOT NULL THEN
        ALTER TABLE sentence_match_lists RENAME TO matches;
    END IF;
END;
$$;


CREATE TABLE IF NOT EXISTS matches (
    analysis_group TEXT NOT NULL,
    global_id BIGINT NOT NULL,
    tsp_id BIGINT NOT NULL,
    matches JSONB NOT NULL DEFAULT '[]'::JSONB,

    match_count INTEGER GENERATED ALWAYS AS (
        jsonb_array_length(matches)
    ) STORED,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_matches
        PRIMARY KEY (
            analysis_group,
            global_id
        ),

    CONSTRAINT ck_matches_json_array
        CHECK (
            jsonb_typeof(matches) = 'array'
        )
);


/*
 * Save a JSONB-list relationship in both directions.
 *
 * Example: source 1 matches 10 at 0.1.
 * - Row 1 appends global ID 10.
 * - Row 10 appends global ID 1 with the same distance.
 *
 * An existing global ID gets its latest object and distance instead of a
 * duplicate. All changes commit or roll back together, and transaction
 * advisory locks protect concurrent function calls.
 */
CREATE OR REPLACE FUNCTION upsert_bidirectional_sentence_match_lists(
    p_analysis_group TEXT,
    p_source_global_id BIGINT,
    p_source_tsp_id BIGINT,
    p_source_application_id TEXT,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
STRICT
AS $$
DECLARE
    forward_matches JSONB;
    processed_match_count INTEGER;
    locked_owner_id BIGINT;
    source_match JSONB;
BEGIN
    /* Remove a source-to-source match and duplicate incoming global IDs. */
    SELECT COALESCE(
        jsonb_agg(match_item ORDER BY match_global_id),
        '[]'::JSONB
    )
    INTO forward_matches
    FROM (
        SELECT DISTINCT ON (
            (match_item ->> 'global_id')::BIGINT
        )
            (match_item ->> 'global_id')::BIGINT AS match_global_id,
            match_item
        FROM jsonb_array_elements(p_matches) AS item(match_item)
        WHERE (match_item ->> 'global_id')::BIGINT
            <> p_source_global_id
        ORDER BY
            (match_item ->> 'global_id')::BIGINT
    ) AS unique_matches;

    processed_match_count := jsonb_array_length(forward_matches);

    /* Example: source 1 with only global ID 1 leaves nothing to save. */
    IF processed_match_count = 0 THEN
        RETURN 0;
    END IF;

    /* Build the object that every target row will store for the source. */
    source_match := jsonb_build_object(
        'global_id', p_source_global_id,
        'tsp_id', p_source_tsp_id,
        'application_id', p_source_application_id
    );

    /* Lock the source and target owner rows in the same numeric order. */
    FOR locked_owner_id IN
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    p_analysis_group,
                    owner_global_id::TEXT
                ),
                0
            ) AS lock_id
        FROM (
            SELECT p_source_global_id AS owner_global_id

            UNION

            SELECT (match_item ->> 'global_id')::BIGINT
            FROM jsonb_array_elements(forward_matches)
                AS item(match_item)
        ) AS owners
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_owner_id);
    END LOOP;

    /* Insert the source row or upsert its target objects by global ID. */
    INSERT INTO matches (
        analysis_group,
        global_id,
        tsp_id,
        matches
    )
    VALUES (
        p_analysis_group,
        p_source_global_id,
        p_source_tsp_id,
        forward_matches
    )
    ON CONFLICT (
        analysis_group,
        global_id
    )
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        matches = sentence_match_merge_lists(
            matches.matches,
            EXCLUDED.matches
        ),
        updated_at = NOW();

    /* Insert every target row or upsert the reverse source object. */
    INSERT INTO matches (
        analysis_group,
        global_id,
        tsp_id,
        matches
    )
    SELECT
        p_analysis_group,
        (match_item ->> 'global_id')::BIGINT,
        (match_item ->> 'tsp_id')::BIGINT,
        jsonb_build_array(
            source_match || jsonb_build_object(
                'distance',
                (match_item ->> 'distance')::DOUBLE PRECISION
            )
        )
    FROM jsonb_array_elements(forward_matches) AS item(match_item)
    ORDER BY (match_item ->> 'global_id')::BIGINT
    ON CONFLICT (
        analysis_group,
        global_id
    )
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        matches = sentence_match_merge_lists(
            matches.matches,
            EXCLUDED.matches
        ),
        updated_at = NOW();

    RETURN processed_match_count;
END;
$$;


/* Database documentation only; it does not run with the function. */
COMMENT ON FUNCTION upsert_bidirectional_sentence_match_lists(
    TEXT,
    BIGINT,
    BIGINT,
    TEXT,
    JSONB
)
IS
'Saves bidirectional sentence matches in production-style JSONB lists.';
