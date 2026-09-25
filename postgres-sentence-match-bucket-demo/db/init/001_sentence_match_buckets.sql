/*
 * This file stores sentence-key matches in both directions.
 *
 * Example: source A matches B at 0.1 and C at 0.5.
 * - A stores {"B": 0.1, "C": 0.5} in buckets chosen from B and C.
 * - B stores {"A": 0.1} in the bucket chosen from A.
 * - C stores {"A": 0.5} in the bucket chosen from A.
 *
 * Each sentence can use up to 32 rows instead of keeping every match in one
 * very large JSONB value. The function locks, writes, and commits both match
 * directions together, so another worker cannot leave only one side saved.
 */


/*
 * Convert one lowercase SHA-256 sentence key into a stable bucket.
 * The returned bucket is always between 0 and 31.
 * Example: a key starting with "ab" becomes 171, and 171 % 32 is bucket 11.
 */
CREATE OR REPLACE FUNCTION sentence_match_bucket(
    sentence_key_value TEXT
)
RETURNS SMALLINT
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT (
        get_byte(
            decode(sentence_key_value, 'hex'),
            0
        ) % 32
    )::SMALLINT;
$$;


/*
 * Count the properties inside one JSONB object.
 * Example: {"B": 0.1, "C": 0.5} returns 2.
 * PostgreSQL has jsonb_array_length, but it does not have a built-in
 * jsonb_object_length function. This small immutable helper can be used by
 * the generated match_key_count column.
 */
CREATE OR REPLACE FUNCTION sentence_match_jsonb_object_size(
    jsonb_value JSONB
)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT COUNT(*)::INTEGER
    FROM jsonb_object_keys(jsonb_value);
$$;


/*
 * Store one sentence key's matching keys across up to 32 JSONB buckets.
 * Empty bucket rows are never created.
 * Example row: ("Asylee", "A", 11, {"B": 0.1, "C": 0.5}).
 */
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

    CONSTRAINT pk_sentence_key_matches
        PRIMARY KEY (
            analysis_group,
            sentence_key,
            match_bucket_number
        ),

    CONSTRAINT ck_sentence_key_matches_bucket_range
        CHECK (
            match_bucket_number BETWEEN 0 AND 31
        ),

    CONSTRAINT ck_sentence_key_matches_json_object
        CHECK (
            jsonb_typeof(matches) = 'object'
        )
);


/*
 * Save a source sentence key's matches in both directions.
 *
 * Example input: source A with {"B": 0.1, "C": 0.5}.
 * The function saves A-to-B, A-to-C, B-to-A, and C-to-A.
 * All writes use the same transaction. Any failure rolls everything back.
 *
 * The caller validates the group, SHA-256 keys, JSON shape, and distances.
 * This function owns bucket calculation, stable locking, and database writes.
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
    /*
     * Count accepted relationships and ignore a possible self-match.
     */
    processed_match_count := sentence_match_jsonb_object_size(p_matches);

    /* Example: source A inside {"A": 0.0, "B": 0.1} is a self-match.
     * Subtract A because the INSERT queries below only save A-to-B. */
    IF p_matches ? p_source_sentence_key THEN
        processed_match_count := processed_match_count - 1;
    END IF;

    /* Example: source A with only {"A": 0.0} leaves zero valid matches.
     * Return now because there are no A-to-other relationships to save. */
    IF processed_match_count = 0 THEN
        RETURN 0;
    END IF;

    /*
     * Reverse rows store the source key inside bucket(source key).
     * Example: if A belongs to bucket 7, both B-to-A and C-to-A use bucket 7.
     */
    source_key_bucket :=
        sentence_match_bucket(p_source_sentence_key);

    /*
     * Lock every affected bucket identity in the same order.
     * Example identity: analysis group Asylee, owner A, bucket 11.
     * This prevents A-to-B and B-to-A workers from locking rows backwards.
     */
    FOR locked_row_id IN
        /* Return each numeric lock ID in a stable order. */
        SELECT lock_rows.lock_id
        FROM (
            /* The same bucket identity only needs one lock. */
            SELECT DISTINCT
                /* Convert "Asylee:A:11" into one numeric advisory-lock ID. */
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
                /* Example forward identity: owner A and bucket(B). */
                SELECT
                    p_source_sentence_key AS owner_sentence_key,
                    sentence_match_bucket(item.match_key)
                        AS match_bucket_number
                FROM jsonb_each(p_matches)
                    AS item(match_key, distance_value)
                WHERE item.match_key <> p_source_sentence_key

                UNION

                /* Example reverse identity: owner B and bucket(A). */
                SELECT
                    item.match_key AS owner_sentence_key,
                    source_key_bucket AS match_bucket_number
                FROM jsonb_each(p_matches)
                    AS item(match_key, distance_value)
                WHERE item.match_key <> p_source_sentence_key
            ) AS identities
        ) AS lock_rows
        /* Every worker uses this order, which reduces deadlocks. */
        ORDER BY lock_rows.lock_id
    LOOP
        /* Wait here if another transaction currently owns this lock ID. */
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /*
     * Example: source A has {"B": 0.1, "C": 0.5}. Turn B and C into rows,
     * group them by bucket, and save one A row for each bucket being used.
     * If A's bucket row exists, || adds new keys and updates old distances.
     */
    WITH incoming_matches AS (
        /* Turn {"B": 0.1, "C": 0.5} into one row for B and one for C. */
        SELECT
            item.match_key,
            /* Read the JSON number as text, then convert it to a number. */
            (item.distance_value #>> '{}')::DOUBLE PRECISION AS distance
        FROM jsonb_each(p_matches)
            AS item(match_key, distance_value)
        /* Do not save source A as a match for itself. */
        WHERE item.match_key <> p_source_sentence_key
    ),
    bucket_patches AS (
        /* Group keys by bucket and rebuild one JSONB object per bucket. */
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
    /* Save every grouped bucket under the source sentence key. */
    SELECT
        p_analysis_group,
        p_source_sentence_key,
        bucket_patches.match_bucket_number,
        bucket_patches.matches
    FROM bucket_patches
    ORDER BY bucket_patches.match_bucket_number
    ON CONFLICT (
        analysis_group,
        sentence_key,
        match_bucket_number
    )
    /* Merge when the bucket exists. The incoming distance wins when the
     * same matching key already has an older distance. */
    DO UPDATE SET
        matches =
            sentence_key_matches.matches
            || EXCLUDED.matches,
        updated_at = NOW();

    /*
     * Example: after saving A-to-B at 0.1, save B-to-A at the same 0.1.
     * Each matching key owns its reverse row, and bucket(A) chooses the row.
     */
    WITH incoming_matches AS (
        /* Turn {"B": 0.1, "C": 0.5} into one row for B and one for C. */
        SELECT
            item.match_key,
            /* Read the JSON number as text, then convert it to a number. */
            (item.distance_value #>> '{}')::DOUBLE PRECISION AS distance
        FROM jsonb_each(p_matches)
            AS item(match_key, distance_value)
        /* Do not create an A-to-A reverse relationship. */
        WHERE item.match_key <> p_source_sentence_key
    )
    INSERT INTO sentence_key_matches (
        analysis_group,
        sentence_key,
        match_bucket_number,
        matches
    )
    /* B and C become row owners. Both rows use bucket(A). */
    SELECT
        p_analysis_group,
        incoming_matches.match_key,
        source_key_bucket,
        /* Example for owner B: build {"A": 0.1}. */
        jsonb_build_object(
            p_source_sentence_key,
            incoming_matches.distance
        )
    FROM incoming_matches
    ORDER BY incoming_matches.match_key
    ON CONFLICT (
        analysis_group,
        sentence_key,
        match_bucket_number
    )
    /* Merge when the reverse bucket exists. The incoming distance wins when
     * the source key already has an older distance. */
    DO UPDATE SET
        matches =
            sentence_key_matches.matches
            || EXCLUDED.matches,
        updated_at = NOW();

    /* Return how many non-self relationships were saved in both directions. */
    RETURN processed_match_count;
END;
$$;


/* Add a description that can be viewed in psql with
 * This is documentation only and does not run with the function. */
COMMENT ON FUNCTION upsert_bidirectional_sentence_key_matches(
    TEXT,
    TEXT,
    JSONB
)
IS
'Saves sentence-key matches in both directions using 32 deterministic JSONB buckets.';
