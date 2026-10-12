/*
 * Create the table that stores one summary for each application.
 *
 * Example:
 *   Application 100 has 1 matching sentence with application 200.
 *   Application 100 has 2 matching sentences with application 300.
 *   Application 100 has 3 matching sentences with application 400.
 *
 * The row for application 100 will contain:
 *   similar_application_ids = [200, 300, 400] because all have at least 1 match
 *   similar_case_ids        = [300, 400] because both have at least 2 matches
 *   tah_case_ids            = [400] because it has at least 3 matches
 *
 * PostgreSQL counts the IDs in the lists and sets is_tah automatically.
 */
DROP TABLE IF EXISTS tsp.titan_similar_cases2;

CREATE TABLE tsp.titan_similar_cases2 (
    application_id INTEGER PRIMARY KEY,
    analysis_group VARCHAR(30) NOT NULL,

    similar_application_ids JSONB NOT NULL DEFAULT '[]'::JSONB,
    similar_case_ids JSONB NOT NULL DEFAULT '[]'::JSONB,
    similar_case_count INTEGER GENERATED ALWAYS AS (
        jsonb_array_length(similar_case_ids)
    ) STORED,

    tah_case_ids JSONB NOT NULL DEFAULT '[]'::JSONB,
    tah_case_count INTEGER GENERATED ALWAYS AS (
        jsonb_array_length(tah_case_ids)
    ) STORED,
    is_tah BOOLEAN GENERATED ALWAYS AS (
        jsonb_array_length(tah_case_ids) > 0
    ) STORED,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_titan_similar_application_ids_array
        CHECK (jsonb_typeof(similar_application_ids) = 'array'),
    CONSTRAINT ck_titan_similar_case_ids_array
        CHECK (jsonb_typeof(similar_case_ids) = 'array'),
    CONSTRAINT ck_titan_tah_case_ids_array
        CHECK (jsonb_typeof(tah_case_ids) = 'array')
);

/* Find applications in one analysis group without reading the whole table. */
CREATE INDEX ix_titan_similar_cases2_analysis_group
    ON tsp.titan_similar_cases2 (analysis_group, application_id);

/*
 * Rebuild the summary for one source application and its connected applications.
 *
 * Example: application 100 currently matches 200 and previously matched 300.
 * The function refreshes 100, 200, and 300. Application 300 is included so
 * its old relationship with 100 can be removed.
 *
 * The function does this:
 *   1. Finds the applications that need an update.
 *   2. Locks them in number order so two workers do not update them together.
 *   3. Counts matching sentence pairs between the applications.
 *   4. Replaces the three saved lists with the newest lists.
 *   5. Returns the updated rows to the Python service.
 *
 * This function does not commit. The Python service commits the transaction.
 */
CREATE OR REPLACE FUNCTION tsp.refresh_titan_similar_cases2(
    p_source_application_id INTEGER,
    p_analysis_group VARCHAR(30),
    p_similar_case_threshold INTEGER DEFAULT 2,
    p_tah_case_threshold INTEGER DEFAULT 3
)
RETURNS TABLE (
    refreshed_application_id INTEGER,
    refreshed_similar_application_ids JSONB,
    refreshed_similar_case_ids JSONB,
    refreshed_similar_case_count INTEGER,
    refreshed_tah_case_ids JSONB,
    refreshed_tah_case_count INTEGER,
    refreshed_is_tah BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    affected_application_ids INTEGER[];
    affected_application_id INTEGER;
BEGIN
    /*
     * Make one list of every application that needs a new summary.
     *
     * Example:
     *   Source application: 100
     *   Current matches: [200, 300]
     *   Previous saved matches: [300, 400]
     *   Final affected list: [100, 200, 300, 400]
     *
     * Application 400 is still included even though it no longer matches.
     * Its old connection to application 100 must be removed.
     */
    -- Put the application IDs into one sorted list. UNION removes duplicates.
    SELECT ARRAY_AGG(candidate.application_id ORDER BY candidate.application_id)
    INTO affected_application_ids
    FROM (
        -- Start with the application that is being processed.
        -- Example: processing application 100 adds 100 to the list.
        SELECT p_source_application_id AS application_id

        UNION

        -- Add applications that currently match the source application.
        SELECT target_owner.application_id
        FROM tsp.titan_matches AS source_match
        -- Example: {"5001": 0.10, "5002": 0.20} becomes global IDs 5001 and 5002.
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches) AS target(target_global_id)
        JOIN (
            -- One sentence can have several bucket rows. Keep one application
            -- owner for each global ID.
            SELECT DISTINCT global_id, application_id
            FROM tsp.titan_matches
        ) AS target_owner
            -- The JSON key is text. Convert it to a number and find its application.
            ON target_owner.global_id = target.target_global_id::INTEGER
        -- Read matches only from the source application.
        WHERE source_match.application_id = p_source_application_id
          -- Do not add the source application as its own match.
          AND target_owner.application_id <> p_source_application_id

        UNION

        -- Add applications from the old summary so removed matches can be cleared.
        SELECT previous_target.application_id::INTEGER
        FROM tsp.titan_similar_cases2 AS previous_summary
        -- Example: [200, 300] becomes one row for 200 and one row for 300.
        CROSS JOIN LATERAL jsonb_array_elements_text(
            previous_summary.similar_application_ids
        ) AS previous_target(application_id)
        WHERE previous_summary.application_id = p_source_application_id
    ) AS candidate;

    /*
     * Lock each affected application in number order.
     *
     * Example: two workers both need applications 100 and 200. Both workers
     * ask for 100 first and 200 second. One worker waits instead of both workers
     * blocking each other. This lock also works when a summary row is new.
     */
    FOREACH affected_application_id IN ARRAY affected_application_ids
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended(
                'titan_similar_cases2:' || affected_application_id::TEXT,
                0
            )
        );
    END LOOP;

    /* Lock summary rows that already exist until this transaction finishes. */
    PERFORM summary.application_id
    FROM tsp.titan_similar_cases2 AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id
    FOR UPDATE;

    WITH affected AS (
        /* Change [100, 200] into two rows so the next queries can use the IDs. */
        SELECT UNNEST(affected_application_ids) AS application_id
    ),
    eligible_applications AS (
        /* Use only applications from the requested group, such as Asylee. */
        SELECT DISTINCT input.application_id
        FROM tsp.text_similarity_input AS input
        WHERE input.analysis_group = p_analysis_group
    ),
    target_owners AS (
        /* Connect each target global ID to the application that owns it. */
        SELECT DISTINCT match_owner.global_id, match_owner.application_id
        FROM tsp.titan_matches AS match_owner
        JOIN eligible_applications AS eligible
            ON eligible.application_id = match_owner.application_id
    ),
    sentence_pairs AS (
        /*
         * Make one row for each unique matching sentence pair.
         *
         * Example: source global ID 10 matches target global IDs 50 and 60.
         * This creates (10, 50) and (10, 60). DISTINCT removes the same pair
         * if it appears again in another bucket row.
         */
        SELECT DISTINCT
            affected.application_id AS source_application_id,
            source_match.global_id AS source_global_id,
            target.target_global_id::INTEGER AS target_global_id
        FROM affected
        JOIN tsp.titan_matches AS source_match
            ON source_match.application_id = affected.application_id
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches) AS target(target_global_id)
    ),
    application_match_counts AS (
        /*
         * Count how many sentence pairs connect each pair of applications.
         *
         * Example: application 100 has sentence pairs (10, 50), (11, 51), and
         * (12, 52) with application 200. Its count for application 200 is 3.
         */
        SELECT
            sentence_pairs.source_application_id,
            target_owners.application_id AS target_application_id,
            COUNT(*)::INTEGER AS sentence_match_count
        FROM sentence_pairs
        JOIN target_owners
            ON target_owners.global_id = sentence_pairs.target_global_id
        WHERE target_owners.application_id <> sentence_pairs.source_application_id
        GROUP BY
            sentence_pairs.source_application_id,
            target_owners.application_id
    ),
    rebuilt_summaries AS (
        /*
         * Build the three new lists for every affected application.
         *
         * Example with limits 2 and 3:
         *   App 200 has 1 sentence match: add it only to similar_application_ids.
         *   App 300 has 2 sentence matches: also add it to similar_case_ids.
         *   App 400 has 3 sentence matches: also add it to tah_case_ids.
         *
         * When an application has no matches, COALESCE saves [] instead of null.
         */
        SELECT
            affected.application_id,
            COALESCE(
                jsonb_agg(
                    counts.target_application_id
                    ORDER BY counts.target_application_id
                ) FILTER (
                    WHERE counts.sentence_match_count >= 1
                ),
                '[]'::JSONB
            ) AS similar_application_ids,
            COALESCE(
                jsonb_agg(
                    counts.target_application_id
                    ORDER BY counts.target_application_id
                ) FILTER (
                    WHERE counts.sentence_match_count >= p_similar_case_threshold
                ),
                '[]'::JSONB
            ) AS similar_case_ids,
            COALESCE(
                jsonb_agg(
                    counts.target_application_id
                    ORDER BY counts.target_application_id
                ) FILTER (
                    WHERE counts.sentence_match_count >= p_tah_case_threshold
                ),
                '[]'::JSONB
            ) AS tah_case_ids
        FROM affected
        LEFT JOIN application_match_counts AS counts
            ON counts.source_application_id = affected.application_id
        GROUP BY affected.application_id
    )
    /* Save a new summary row or replace all lists in an existing summary row. */
    INSERT INTO tsp.titan_similar_cases2 (
        application_id,
        analysis_group,
        similar_application_ids,
        similar_case_ids,
        tah_case_ids,
        last_modified
    )
    SELECT
        rebuilt.application_id,
        p_analysis_group,
        rebuilt.similar_application_ids,
        rebuilt.similar_case_ids,
        rebuilt.tah_case_ids,
        NOW()
    FROM rebuilt_summaries AS rebuilt
    /* Use the primary-key column so this upsert does not depend on PostgreSQL's generated constraint name. */
    ON CONFLICT (application_id)
    DO UPDATE SET
        analysis_group = EXCLUDED.analysis_group,
        similar_application_ids = EXCLUDED.similar_application_ids,
        similar_case_ids = EXCLUDED.similar_case_ids,
        tah_case_ids = EXCLUDED.tah_case_ids,
        last_modified = NOW();

    /* Return the updated rows. PostgreSQL has already calculated counts and is_tah. */
    RETURN QUERY
    SELECT
        summary.application_id,
        summary.similar_application_ids,
        summary.similar_case_ids,
        summary.similar_case_count,
        summary.tah_case_ids,
        summary.tah_case_count,
        summary.is_tah
    FROM tsp.titan_similar_cases2 AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id;
END;
$$;

COMMENT ON FUNCTION tsp.refresh_titan_similar_cases2(INTEGER, VARCHAR, INTEGER, INTEGER)
IS 'Updates the case-match lists for one application and every application connected to it.';
