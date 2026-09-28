/* Return the same bucket for the same lowercase SHA-256 sentence key. */
CREATE OR REPLACE FUNCTION sentence_match_bucket(sentence_key_value TEXT)
RETURNS SMALLINT
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT (
        get_byte(decode(sentence_key_value, 'hex'), 0) % 32
    )::SMALLINT;
$$;


/* Count the matching keys stored inside one JSONB object. */
CREATE OR REPLACE FUNCTION sentence_match_jsonb_object_size(jsonb_value JSONB)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT COUNT(*)::INTEGER
    FROM jsonb_object_keys(jsonb_value);
$$;


/* One sentence key can use up to 32 smaller match rows. */
CREATE TABLE IF NOT EXISTS sentence_key_matches (
    analysis_group TEXT NOT NULL,
    sentence_key TEXT NOT NULL,
    match_bucket_number SMALLINT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,
    match_key_count INTEGER GENERATED ALWAYS AS (
        sentence_match_jsonb_object_size(matches)
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (analysis_group, sentence_key, match_bucket_number),
    CHECK (sentence_key ~ '^[0-9a-f]{64}$'),
    CHECK (match_bucket_number BETWEEN 0 AND 31),
    CHECK (jsonb_typeof(matches) = 'object')
);


/*
 * These two indexes support the source-page and candidate-key queries used by
 * this worker. Run them outside a Flyway transaction because CONCURRENTLY
 * avoids blocking normal writes on the large sentences_vector table.
 */
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_sentences_vector_current_tsp_global
ON sentences_vector (tsp_id, global_id)
WHERE is_current = TRUE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_sentences_vector_eligible_group_key_app
ON sentences_vector (analysis_group, sentence_key, application_id)
WHERE is_current = TRUE
  AND is_non_matcher = FALSE
  AND is_tracer = FALSE
  AND is_form_language = FALSE;


/*
 * Save source-to-target and target-to-source relationships together.
 * Advisory transaction locks are calculated from the affected bucket rows
 * and acquired in the same order by every worker to reduce deadlocks.
 */
CREATE OR REPLACE FUNCTION upsert_bidirectional_sentence_key_matches(
    p_analysis_group TEXT,
    p_source_sentence_key TEXT,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
STRICT
AS $$
DECLARE
    processed_match_count INTEGER;
    source_key_bucket SMALLINT;
    locked_row_id BIGINT;
BEGIN
    processed_match_count := sentence_match_jsonb_object_size(p_matches);

    /* A source key is not allowed to match itself. */
    IF p_matches ? p_source_sentence_key THEN
        processed_match_count := processed_match_count - 1;
    END IF;

    IF processed_match_count = 0 THEN
        RETURN 0;
    END IF;

    source_key_bucket := sentence_match_bucket(p_source_sentence_key);

    /* Lock only the forward and reverse bucket rows this call will change. */
    FOR locked_row_id IN
        SELECT lock_rows.lock_id
        FROM (
            SELECT DISTINCT
                hashtextextended(
                    concat_ws(
                        ':',
                        p_analysis_group,
                        identities.owner_sentence_key,
                        identities.match_bucket_number::TEXT
                    ),
                    0
                ) AS lock_id
            FROM (
                SELECT
                    p_source_sentence_key AS owner_sentence_key,
                    sentence_match_bucket(item.match_key)
                        AS match_bucket_number
                FROM jsonb_each(p_matches)
                    AS item(match_key, distance_value)
                WHERE item.match_key <> p_source_sentence_key

                UNION

                SELECT
                    item.match_key AS owner_sentence_key,
                    source_key_bucket AS match_bucket_number
                FROM jsonb_each(p_matches)
                    AS item(match_key, distance_value)
                WHERE item.match_key <> p_source_sentence_key
            ) AS identities
        ) AS lock_rows
        ORDER BY lock_rows.lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /* Group forward matches by target bucket and merge each JSONB patch. */
    WITH incoming_matches AS (
        SELECT
            item.match_key,
            (item.distance_value #>> '{}')::DOUBLE PRECISION AS distance
        FROM jsonb_each(p_matches)
            AS item(match_key, distance_value)
        WHERE item.match_key <> p_source_sentence_key
    ),
    bucket_patches AS (
        SELECT
            sentence_match_bucket(match_key) AS match_bucket_number,
            jsonb_object_agg(match_key, distance) AS matches
        FROM incoming_matches
        GROUP BY sentence_match_bucket(match_key)
    )
    INSERT INTO sentence_key_matches (
        analysis_group,
        sentence_key,
        match_bucket_number,
        matches
    )
    SELECT
        p_analysis_group,
        p_source_sentence_key,
        bucket_patches.match_bucket_number,
        bucket_patches.matches
    FROM bucket_patches
    ORDER BY bucket_patches.match_bucket_number
    ON CONFLICT (analysis_group, sentence_key, match_bucket_number)
    DO UPDATE SET
        matches = sentence_key_matches.matches || EXCLUDED.matches,
        updated_at = NOW();

    /* Save the reverse relationship under every matching sentence key. */
    WITH incoming_matches AS (
        SELECT
            item.match_key,
            (item.distance_value #>> '{}')::DOUBLE PRECISION AS distance
        FROM jsonb_each(p_matches)
            AS item(match_key, distance_value)
        WHERE item.match_key <> p_source_sentence_key
    )
    INSERT INTO sentence_key_matches (
        analysis_group,
        sentence_key,
        match_bucket_number,
        matches
    )
    SELECT
        p_analysis_group,
        incoming_matches.match_key,
        source_key_bucket,
        jsonb_build_object(
            p_source_sentence_key,
            incoming_matches.distance
        )
    FROM incoming_matches
    ORDER BY incoming_matches.match_key
    ON CONFLICT (analysis_group, sentence_key, match_bucket_number)
    DO UPDATE SET
        matches = sentence_key_matches.matches || EXCLUDED.matches,
        updated_at = NOW();

    RETURN processed_match_count;
END;
$$;
