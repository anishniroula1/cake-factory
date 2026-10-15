/*
 * Delete the old clone because its A-number-based structure does not match the new application-ID-based Titan flow.
 * A-number is PII, so the new table does not copy it or store it unnecessarily; it can be retrieved using application_id when needed.
 * Run this DROP only in the one-time Flyway migration because running it again would delete saved Titan case summaries.
 */
DROP TABLE IF EXISTS tsp.titan_similar_cases;
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
 * An application with no matches does not have a row in this table.
 */

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
 * Recalculate the case summary for one application and update the other side
 * of each relationship.
 *
 * Running example used in the comments below:
 *   Application 10 has 1 matching sentence with application 20.
 *   Application 10 has 2 matching sentences with application 30.
 *   Application 10 has 3 matching sentences with application 40.
 *   Application 10 used to match application 50, but that match is now gone.
 *
 * With the default limits, application 10 gets:
 *   similar_application_ids = [20, 30, 40] because each has at least 1 match
 *   similar_case_ids        = [30, 40] because each has at least 2 matches
 *   tah_case_ids            = [40] because it has at least 3 matches
 *
 * Every relationship is saved in both directions. For example, application 10
 * contains application 50, and application 50 also contains application 10.
 * If application 50's matching document or sentences are deleted, the current
 * Titan matches no longer connect applications 10 and 50. This refresh removes
 * 50 from application 10 and removes 10 from application 50, including their
 * similar-case and TAH lists when previously present. Application 50 may still
 * match applications 60 and 70, so those unrelated relationships are not removed.
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
    /* Every application whose saved row may change during this refresh. */
    affected_application_ids INTEGER[];
    /* One ID from the list above, used while rows are locked in order. */
    affected_application_id INTEGER;
BEGIN
    /*
     * STEP 1: Find every application row that may need to change.
     *
     * In the running example, current matches are [20, 30, 40] and the old
     * saved list also contains 50. The result is [10, 20, 30, 40, 50]. We keep
     * 50 in this list so its old reverse relationship to 10 can be removed.
     */
    -- ARRAY_AGG makes one sorted array. UNION removes duplicate IDs.
    SELECT ARRAY_AGG(candidate.application_id ORDER BY candidate.application_id)
    INTO affected_application_ids
    FROM (
        -- Always include the source. In the example, this adds application 10.
        SELECT p_source_application_id AS application_id

        UNION

        -- Add every application that currently has a sentence matching the source.
        SELECT DISTINCT target_owner.application_id
        FROM tsp.titan_matches AS source_match
        -- jsonb_object_keys changes {"5001": 0.10, "5002": 0.20} into IDs 5001 and 5002.
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches) AS target(target_global_id)
        -- Use each target global ID to find the application that owns that sentence.
        JOIN tsp.titan_matches AS target_owner
            ON target_owner.global_id = target.target_global_id::INTEGER
        JOIN tsp.text_similarity_input AS target_input
            ON target_input.application_id = target_owner.application_id
           AND target_input.analysis_group = p_analysis_group
        -- Read sentence matches owned by the source application only.
        WHERE source_match.application_id = p_source_application_id
          -- An application must never be listed as matching itself.
          AND target_owner.application_id <> p_source_application_id

        UNION

        -- Include old matches so relationships that disappeared can be removed.
        SELECT previous_target.application_id::INTEGER
        FROM tsp.titan_similar_cases AS previous_summary
        -- jsonb_array_elements_text changes [30, 40, 50] into three SQL rows.
        CROSS JOIN LATERAL jsonb_array_elements_text(
            previous_summary.similar_application_ids
        ) AS previous_target(application_id)
        WHERE previous_summary.application_id = p_source_application_id
    ) AS candidate;

    /*
     * STEP 2: Lock affected applications in the same order every time.
     *
     * Example: two workers both need applications 10 and 20. Both lock 10 first
     * and 20 second. One worker waits for the other instead of creating a
     * deadlock. Advisory locks also work when a summary row does not exist yet.
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

    /*
     * STEP 3: Lock summary rows that already exist.
     *
     * The rows stay locked until the transaction finishes. Other workers can
     * still read them, but they must wait before changing the same rows.
     */
    PERFORM summary.application_id
    FROM tsp.titan_similar_cases AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id
    FOR UPDATE;

    WITH source_sentence_pairs AS (
        /*
         * STEP 4: Expand the source application's JSONB matches into sentence pairs.
         *
         * Example: source global ID 10 matches target global IDs 50 and 60.
         * This produces two rows: (10, 50) and (10, 60). DISTINCT keeps only
         * one copy if the same pair appears more than once.
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
         * STEP 5: Find which application owns each matching target sentence.
         *
         * Example: target global IDs 50 and 60 belong to application 20, so
         * both sentence pairs count toward application 10 matching application
         * 20. Targets outside the requested analysis group are ignored.
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
         * STEP 6: Count matching sentence pairs for each target application.
         *
         * Example: application 10 has pairs (100, 500), (101, 501), and
         * (102, 502) with application 40. Application 40 gets a count of 3.
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
         * STEP 7: Build all three lists for the source application.
         *
         * Running example with limits 2 and 3:
         *   App 20 has 1 match, so it goes in similar_application_ids only.
         *   App 30 has 2 matches, so it also goes in similar_case_ids.
         *   App 40 has 3 matches, so it also goes in tah_case_ids.
         *
         * HAVING stops the function from creating an empty source row when no
         * matches exist. The case and TAH lists are allowed to be empty.
         */
        SELECT
            p_source_application_id AS application_id,
            p_analysis_group AS analysis_group,
            /* List every application with at least one matching sentence. */
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= 1
                ),
                '[]'::JSONB
            ) AS similar_application_ids,
            /* List applications that meet the similar-case limit, normally 2. */
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= p_similar_case_threshold
                ),
                '[]'::JSONB
            ) AS similar_case_ids,
            /* List applications that meet the TAH limit, normally 3. */
            COALESCE(
                jsonb_agg(
                    source_match_counts.target_application_id
                    ORDER BY source_match_counts.target_application_id
                ) FILTER (
                    WHERE source_match_counts.sentence_match_count >= p_tah_case_threshold
                ),
                '[]'::JSONB
            ) AS tah_case_ids
        FROM source_match_counts
        HAVING COUNT(*) > 0
    ),
    target_rows AS (
        /*
         * STEP 8: Load each target's saved lists and its new count with the source.
         *
         * A target appears here when it matches now or matched before. In the
         * running example, app 50 has no new count, but it is loaded so app 10
         * can be removed from app 50's old lists.
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
         * STEP 9: Update only the source ID inside each target application's lists.
         *
         * Example: app 30 already has [5, 8]. It now has two sentence matches
         * with app 10, so its similar_case_ids become [5, 8, 10]. IDs 5 and 8
         * stay unchanged. If app 10 no longer qualifies, only 10 is removed.
         */
        SELECT
            target_row.application_id,
            p_analysis_group AS analysis_group,
            /* Add/remove the source in the target's one-or-more-match list. */
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
            /* Add/remove the source in the target's similar-case list. */
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
            /* Add/remove the source in the target's TAH list. */
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
        -- Keep an existing target or a target that has a current sentence match.
        WHERE target_row.summary_exists
           OR target_row.sentence_match_count IS NOT NULL
    ),
    summaries_to_save AS (
        /* STEP 10: Put the rebuilt source and updated target rows into one list. */
        SELECT * FROM rebuilt_source_summary

        UNION ALL

        SELECT * FROM patched_target_summaries
    ),
    summaries_with_matches AS (
        /*
         * STEP 11: Keep only summaries that still contain a relationship.
         *
         * Example: if app 50 contained only app 10 and that match disappeared,
         * all three lists are empty and app 50 is not saved again.
         */
        SELECT *
        FROM summaries_to_save
        WHERE jsonb_array_length(similar_application_ids) > 0
           OR jsonb_array_length(similar_case_ids) > 0
           OR jsonb_array_length(tah_case_ids) > 0
    ),
    deleted_empty_summaries AS (
        /*
         * STEP 12: Delete an old row after its last relationship disappears.
         *
         * In the running example, app 50 is deleted if app 10 was its only
         * relationship. We do not keep a database row with three empty lists.
         */
        DELETE FROM tsp.titan_similar_cases AS existing
        WHERE existing.application_id = ANY(affected_application_ids)
          AND NOT EXISTS (
              SELECT 1
              FROM summaries_with_matches AS saved
              WHERE saved.application_id = existing.application_id
          )
        RETURNING existing.application_id
    )
    /*
     * STEP 13: Insert new summaries or replace existing summaries.
     *
     * The source row contains fully rebuilt lists. Each target row contains its
     * old relationships with only the source ID added or removed.
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
    FROM summaries_with_matches AS summary
    /* application_id is the primary key, so an existing row is updated. */
    ON CONFLICT (application_id)
    DO UPDATE SET
        analysis_group = EXCLUDED.analysis_group,
        similar_application_ids = EXCLUDED.similar_application_ids,
        similar_case_ids = EXCLUDED.similar_case_ids,
        tah_case_ids = EXCLUDED.tah_case_ids,
        last_modified = NOW();

    /*
     * STEP 14: Return the final value for every application that was checked.
     *
     * A deleted row is returned with empty lists, zero counts, and is_tah=false.
     * This lets the service publish a TAH change from true to false when needed.
     */
    RETURN QUERY
    SELECT
        affected.application_id,
        COALESCE(summary.similar_application_ids, '[]'::JSONB),
        COALESCE(summary.similar_case_ids, '[]'::JSONB),
        COALESCE(summary.similar_case_count, 0),
        COALESCE(summary.tah_case_ids, '[]'::JSONB),
        COALESCE(summary.tah_case_count, 0),
        COALESCE(summary.is_tah, FALSE)
    FROM UNNEST(affected_application_ids) AS affected(application_id)
    LEFT JOIN tsp.titan_similar_cases AS summary
        ON summary.application_id = affected.application_id
    ORDER BY affected.application_id;
END;
$$;

COMMENT ON FUNCTION tsp.refresh_titan_similar_cases(INTEGER, VARCHAR, INTEGER, INTEGER)
IS 'Rebuilds one source application and mirrors only that relationship on connected applications.';
