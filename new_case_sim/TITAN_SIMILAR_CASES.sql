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
DROP TABLE IF EXISTS tsp.titan_similar_cases;

CREATE TABLE tsp.titan_similar_cases (
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
CREATE INDEX ix_titan_similar_cases_analysis_group
    ON tsp.titan_similar_cases (analysis_group, application_id);

/*
 * Refresh one source application and mirror only that relationship on its targets.
 *
 * Example: application 10 matches applications 20, 30, and 40.
 *
 *   Application 10 is rebuilt from all Titan matches owned by application 10.
 *   Application 20 only adds or removes application 10 from its saved lists.
 *   Application 30 only adds or removes application 10 from its saved lists.
 *   Application 40 only adds or removes application 10 from its saved lists.
 *
 * The function does not read every Titan match owned by applications 20, 30,
 * and 40. Their other saved relationships stay unchanged. This follows the old
 * Case Similarity behavior and avoids rebuilding a large connected network.
 *
 * This function does not commit. The Python service commits the transaction.
 */
CREATE OR REPLACE FUNCTION tsp.refresh_titan_similar_cases(
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
     * Make one list containing the source and every target row that needs a patch.
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
        SELECT DISTINCT target_owner.application_id
        FROM tsp.titan_matches AS source_match
        -- Example: {"5001": 0.10, "5002": 0.20} becomes global IDs 5001 and 5002.
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches) AS target(target_global_id)
        -- global_id is the first part of the Titan match primary key, so this
        -- lookup reads only the target rows instead of scanning the whole table.
        JOIN tsp.titan_matches AS target_owner
            ON target_owner.global_id = target.target_global_id::INTEGER
        JOIN tsp.text_similarity_input AS target_input
            ON target_input.application_id = target_owner.application_id
           AND target_input.analysis_group = p_analysis_group
        -- Read matches only from the source application.
        WHERE source_match.application_id = p_source_application_id
          -- Do not add the source application as its own match.
          AND target_owner.application_id <> p_source_application_id

        UNION

        -- Add applications from the old summary so removed matches can be cleared.
        SELECT previous_target.application_id::INTEGER
        FROM tsp.titan_similar_cases AS previous_summary
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
                'titan_similar_cases:' || affected_application_id::TEXT,
                0
            )
        );
    END LOOP;

    /* Lock summary rows that already exist until this transaction finishes. */
    PERFORM summary.application_id
    FROM tsp.titan_similar_cases AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id
    FOR UPDATE;

    WITH source_sentence_pairs AS (
        /*
         * Read only sentence pairs owned by the source application.
         *
         * Example: source global ID 10 matches target global IDs 50 and 60.
         * This creates (10, 50) and (10, 60). DISTINCT removes the same pair
         * if it appears again in another bucket row.
         */
        SELECT DISTINCT
            source_match.global_id AS source_global_id,
            target.target_global_id::INTEGER AS target_global_id
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches) AS target(target_global_id)
        WHERE source_match.application_id = p_source_application_id
    ),
    target_owners AS (
        /*
         * Find the application that owns each target sentence.
         *
         * The join starts with the small source_sentence_pairs result. It does
         * not build an owner list for every global ID in Titan matches.
         */
        SELECT DISTINCT target_match.global_id, target_match.application_id
        FROM source_sentence_pairs AS sentence_pair
        JOIN tsp.titan_matches AS target_match
            ON target_match.global_id = sentence_pair.target_global_id
        JOIN tsp.text_similarity_input AS target_input
            ON target_input.application_id = target_match.application_id
           AND target_input.analysis_group = p_analysis_group
        WHERE target_match.application_id <> p_source_application_id
    ),
    source_match_counts AS (
        /*
         * Count how many sentence pairs connect the source to each target.
         *
         * Example: application 100 has sentence pairs (10, 50), (11, 51), and
         * (12, 52) with application 200. Its count for application 200 is 3.
         */
        SELECT
            target_owners.application_id AS target_application_id,
            COUNT(*)::INTEGER AS sentence_match_count
        FROM source_sentence_pairs AS sentence_pair
        JOIN target_owners
            ON target_owners.global_id = sentence_pair.target_global_id
        GROUP BY target_owners.application_id
    ),
    rebuilt_source_summary AS (
        /*
         * Fully rebuild only the source application's three lists.
         *
         * Example with limits 2 and 3:
         *   App 200 has 1 sentence match: add it only to similar_application_ids.
         *   App 300 has 2 sentence matches: also add it to similar_case_ids.
         *   App 400 has 3 sentence matches: also add it to tah_case_ids.
         *
         * When an application has no matches, COALESCE saves [] instead of null.
         */
        SELECT
            p_source_application_id AS application_id,
            p_analysis_group AS analysis_group,
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= 1
                ),
                '[]'::JSONB
            ) AS similar_application_ids,
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= p_similar_case_threshold
                ),
                '[]'::JSONB
            ) AS similar_case_ids,
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= p_tah_case_threshold
                ),
                '[]'::JSONB
            ) AS tah_case_ids
        FROM (SELECT 1) AS one_source
        LEFT JOIN source_match_counts ON TRUE
    ),
    target_rows AS (
        /*
         * Load the existing target summaries and the source-to-target count.
         *
         * A target can be here because it matches now or because it matched
         * before and application 10 must be removed from its old lists.
         */
        SELECT
            affected.application_id,
            existing.application_id IS NOT NULL AS summary_exists,
            COALESCE(existing.similar_application_ids, '[]'::JSONB)
                AS similar_application_ids,
            COALESCE(existing.similar_case_ids, '[]'::JSONB)
                AS similar_case_ids,
            COALESCE(existing.tah_case_ids, '[]'::JSONB)
                AS tah_case_ids,
            source_match_counts.sentence_match_count
        FROM UNNEST(affected_application_ids) AS affected(application_id)
        LEFT JOIN tsp.titan_similar_cases AS existing
            ON existing.application_id = affected.application_id
        LEFT JOIN source_match_counts
            ON source_match_counts.target_application_id = affected.application_id
        WHERE affected.application_id <> p_source_application_id
    ),
    patched_target_summaries AS (
        /*
         * Change only the source application's membership in each target row.
         *
         * Example: app 20 already has [5, 8, 30]. When app 10 now has two
         * sentence matches with app 20, the result is [5, 8, 10, 30]. The
         * relationships to 5, 8, and 30 are not recalculated or removed.
         * An invalid self-reference, such as app 20 containing 20, is removed.
         */
        SELECT
            target_row.application_id,
            p_analysis_group AS analysis_group,
            COALESCE(
                (
                    SELECT jsonb_agg(candidate.application_id ORDER BY candidate.application_id)
                    FROM (
                        SELECT saved.application_id::INTEGER
                        FROM jsonb_array_elements_text(
                            target_row.similar_application_ids
                        ) AS saved(application_id)
                        WHERE saved.application_id::INTEGER <> p_source_application_id
                          AND saved.application_id::INTEGER <> target_row.application_id

                        UNION

                        SELECT p_source_application_id
                        WHERE target_row.sentence_match_count >= 1
                    ) AS candidate
                ),
                '[]'::JSONB
            ) AS similar_application_ids,
            COALESCE(
                (
                    SELECT jsonb_agg(candidate.application_id ORDER BY candidate.application_id)
                    FROM (
                        SELECT saved.application_id::INTEGER
                        FROM jsonb_array_elements_text(
                            target_row.similar_case_ids
                        ) AS saved(application_id)
                        WHERE saved.application_id::INTEGER <> p_source_application_id
                          AND saved.application_id::INTEGER <> target_row.application_id

                        UNION

                        SELECT p_source_application_id
                        WHERE target_row.sentence_match_count >= p_similar_case_threshold
                    ) AS candidate
                ),
                '[]'::JSONB
            ) AS similar_case_ids,
            COALESCE(
                (
                    SELECT jsonb_agg(candidate.application_id ORDER BY candidate.application_id)
                    FROM (
                        SELECT saved.application_id::INTEGER
                        FROM jsonb_array_elements_text(
                            target_row.tah_case_ids
                        ) AS saved(application_id)
                        WHERE saved.application_id::INTEGER <> p_source_application_id
                          AND saved.application_id::INTEGER <> target_row.application_id

                        UNION

                        SELECT p_source_application_id
                        WHERE target_row.sentence_match_count >= p_tah_case_threshold
                    ) AS candidate
                ),
                '[]'::JSONB
            ) AS tah_case_ids
        FROM target_rows AS target_row
        -- Do not create an empty row only because an old source list named it.
        WHERE target_row.summary_exists
           OR target_row.sentence_match_count IS NOT NULL
    ),
    summaries_to_save AS (
        SELECT * FROM rebuilt_source_summary

        UNION ALL

        SELECT * FROM patched_target_summaries
    )
    /*
     * Replace the source lists. For targets, these values already contain their
     * old relationships plus or minus only the source application.
     */
    INSERT INTO tsp.titan_similar_cases (
        application_id,
        analysis_group,
        similar_application_ids,
        similar_case_ids,
        tah_case_ids,
        last_modified
    )
    SELECT
        summary.application_id,
        summary.analysis_group,
        summary.similar_application_ids,
        summary.similar_case_ids,
        summary.tah_case_ids,
        NOW()
    FROM summaries_to_save AS summary
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
    FROM tsp.titan_similar_cases AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id;
END;
$$;

COMMENT ON FUNCTION tsp.refresh_titan_similar_cases(INTEGER, VARCHAR, INTEGER, INTEGER)
IS 'Rebuilds one source application and mirrors only that relationship on connected applications.';
