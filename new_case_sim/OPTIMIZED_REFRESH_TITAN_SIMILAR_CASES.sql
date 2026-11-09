/*
 * Create the case summary for one application.
 *
 * Sentence Similarity has already saved sentence matches in the 16 buckets of
 * tsp.titan_matches. This function reads those matches, counts how many unique
 * sentence pairs connect the source application to each other application, and
 * saves the result in tsp.titan_similar_cases.
 *
 * The default rules are:
 *
 *   1 or more sentence pairs = add the application to similar_application_ids
 *   2 or more sentence pairs = also add it to similar_case_ids
 *   3 or more sentence pairs = also add it to tah_case_ids
 *
 * An application with 3 pairs is in all three lists. The two input thresholds
 * can change the similar-case and TAH limits.
 *
 * The match is saved on both sides. For example, if application 10 matches
 * application 20, application 10 stores 20 and application 20 stores 10. If
 * the match changes or disappears later, this function fixes both sides.
 *
 * The source row is rebuilt from all current matches. Other application rows
 * are changed only when the source application must be added to or removed
 * from one of their lists. Their unrelated matches are left alone. A row is
 * deleted when all three lists become empty.
 *
 * PostgreSQL automatically calculates similar_case_count, tah_case_count, and
 * is_tah from the three lists. This function does not commit. Python commits or
 * rolls back the transaction.
 *
 * Inputs:
 *
 *   p_source_application_id    application to process
 *   p_analysis_group           group to match, such as Asylee or Refugee
 *   p_similar_case_threshold   number of pairs required for a similar case
 *   p_tah_case_threshold       number of pairs required for a TAH case
 *
 * Output:
 *
 *   The source application is always returned. Another application is returned
 *   only when one of its saved lists needed to change. Each returned row has
 *   the application ID, similar-case count, and TAH-case count needed by the
 *   Python service. The number of returned rows is not the number of matches.
 */
CREATE OR REPLACE FUNCTION tsp.refresh_titan_similar_cases(
    p_source_application_id INTEGER,
    p_analysis_group VARCHAR(30),
    p_similar_case_threshold INTEGER DEFAULT 2,
    p_tah_case_threshold INTEGER DEFAULT 3
)
RETURNS TABLE (
    refreshed_application_id INTEGER,
    refreshed_similar_case_count INTEGER,
    refreshed_tah_case_count INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    /*
     * These two arrays work together. Values in the same position belong
     * together.
     *
     * Example:
     *   current_target_application_ids = [20, 30]
     *   current_sentence_match_counts  = [3, 1]
     *
     * Application 20 has 3 sentence pairs, and application 30 has 1. Later,
     * UNNEST turns these arrays back into the rows (20, 3) and (30, 1).
     */
    current_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];
    current_sentence_match_counts INTEGER[] := ARRAY[]::INTEGER[];

    /* Old targets are kept so relationships that disappeared can be removed. */
    previous_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];

    /* Only target rows with a changed list need to be saved. */
    changed_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];
    affected_application_ids INTEGER[] := ARRAY[]::INTEGER[];
BEGIN
    /*
     * STEP 1: Read the source application's 16 buckets and count unique
     * sentence pairs for each target application.
     *
     * Example:
     *   (source 101, target 201)
     *   (source 102, target 202)
     *   (source 102, target 203)
     *
     * If target sentences 201, 202, and 203 belong to application 20, the count
     * for application 20 is 3. With the default rules, it is a TAH case.
     */
    WITH source_sentence_pairs AS (
        SELECT DISTINCT
            source_match.global_id AS source_global_id,
            target.target_global_id::INTEGER AS target_global_id
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
            AS target(target_global_id)
        WHERE source_match.application_id = p_source_application_id
    ),
    current_counts AS (
        SELECT
            target_owner.application_id AS target_application_id,
            COUNT(*)::INTEGER AS sentence_match_count
        FROM source_sentence_pairs AS sentence_pair
        /*
         * One target sentence can have several bucket rows. All of those rows
         * have the same application_id, so reading one row is enough.
         */
        JOIN LATERAL (
            SELECT target_match.application_id
            FROM tsp.titan_matches AS target_match
            WHERE target_match.global_id = sentence_pair.target_global_id
            ORDER BY target_match.match_bucket_number
            LIMIT 1
        ) AS target_owner ON TRUE
        WHERE target_owner.application_id <> p_source_application_id
          /*
           * Keep targets from the requested analysis group. EXISTS checks that
           * the application belongs to the group without creating extra rows.
           */
          AND EXISTS (
              SELECT 1
              FROM tsp.text_similarity_input AS target_input
              WHERE target_input.application_id = target_owner.application_id
                AND target_input.analysis_group = p_analysis_group
          )
        GROUP BY target_owner.application_id
    )
    /*
     * Put the application IDs and their counts into the two arrays above.
     * Both arrays use the same application ID order, so the values stay paired.
     *
     * Example current_counts rows:
     *
     *   target_application_id | sentence_match_count
     *   ----------------------+---------------------
     *   20                    | 3
     *   30                    | 1
     *
     * Stored result:
     *
     *   current_target_application_ids = [20, 30]
     *   current_sentence_match_counts  = [3, 1]
     *
     * ARRAY_AGG returns NULL when there are no targets. COALESCE changes NULL
     * to an empty array so the later steps can continue safely.
     */
    SELECT
        COALESCE(
            ARRAY_AGG(target_application_id ORDER BY target_application_id),
            ARRAY[]::INTEGER[]
        ),
        COALESCE(
            ARRAY_AGG(sentence_match_count ORDER BY target_application_id),
            ARRAY[]::INTEGER[]
        )
    INTO current_target_application_ids, current_sentence_match_counts
    FROM current_counts;

    /*
     * STEP 2: Load every application previously saved for the source.
     *
     * A match that disappeared will not be in the Step 1 result. We still need
     * the old application ID so Step 3 can remove the old reverse match.
     *
     * Example:
     *   current targets from Step 1 = [20, 30]
     *   previously saved targets   = [20, 50]
     *
     * Application 50 is still checked because its old reverse match must be
     * removed.
     *
     * Normally, IDs in the case and TAH lists are also in the similar
     * application list. Reading all three lists also handles older rows where
     * those lists do not agree. UNION removes duplicate IDs.
     */
    SELECT COALESCE(
        ARRAY_AGG(previous_target.application_id ORDER BY previous_target.application_id),
        ARRAY[]::INTEGER[]
    )
    INTO previous_target_application_ids
    FROM (
        SELECT saved.application_id::INTEGER
        FROM tsp.titan_similar_cases AS previous_summary
        CROSS JOIN LATERAL jsonb_array_elements_text(
            previous_summary.similar_application_ids
        ) AS saved(application_id)
        WHERE previous_summary.application_id = p_source_application_id

        UNION

        SELECT saved.application_id::INTEGER
        FROM tsp.titan_similar_cases AS previous_summary
        CROSS JOIN LATERAL jsonb_array_elements_text(
            previous_summary.similar_case_ids
        ) AS saved(application_id)
        WHERE previous_summary.application_id = p_source_application_id

        UNION

        SELECT saved.application_id::INTEGER
        FROM tsp.titan_similar_cases AS previous_summary
        CROSS JOIN LATERAL jsonb_array_elements_text(
            previous_summary.tah_case_ids
        ) AS saved(application_id)
        WHERE previous_summary.application_id = p_source_application_id
    ) AS previous_target;

    /*
     * STEP 3: Find the other application rows that need an update.
     *
     * First, current_counts changes the Step 1 arrays back into rows such as:
     *
     *   target application 20, sentence count 3
     *
     * candidate_targets then combines the current targets from titan_matches
     * with the old targets from titan_similar_cases. This lets the function
     * handle both new matches and matches that disappeared.
     *
     * For each target, the function decides whether the source should be in
     * each reverse list:
     *
     *   count >= 1                         -> similar application list
     *   count >= p_similar_case_threshold -> similar case list
     *   count >= p_tah_case_threshold     -> TAH list
     *
     * It compares those answers with the saved lists. If any answer is
     * different, that target application needs an update. IS DISTINCT FROM is
     * used so the comparison also works when a saved row does not exist.
     *
     * Examples with default thresholds:
     *
     *   count 5 -> 6: no list changes, so no update is needed;
     *   count 2 -> 3: add the source to the target's TAH list;
     *   count 3 -> 2: remove it from TAH but keep it as a similar case;
     *   count 1 -> 0: remove it from all three lists;
     *   new count 1: create or update the reverse row and add it to the similar
     *                application list.
     */
    WITH current_counts AS (
        SELECT *
        FROM UNNEST(
            current_target_application_ids,
            current_sentence_match_counts
        ) AS current_count(target_application_id, sentence_match_count)
    ),
    candidate_targets AS (
        SELECT UNNEST(current_target_application_ids) AS application_id

        UNION

        SELECT UNNEST(previous_target_application_ids) AS application_id
    )
    SELECT COALESCE(
        ARRAY_AGG(candidate.application_id ORDER BY candidate.application_id),
        ARRAY[]::INTEGER[]
    )
    INTO changed_target_application_ids
    FROM candidate_targets AS candidate
    LEFT JOIN current_counts AS current_count
        ON current_count.target_application_id = candidate.application_id
    LEFT JOIN tsp.titan_similar_cases AS target_summary
        ON target_summary.application_id = candidate.application_id
    WHERE candidate.application_id <> p_source_application_id
      AND (
          (COALESCE(current_count.sentence_match_count, 0) >= 1)
              IS DISTINCT FROM
              (
                  COALESCE(
                      target_summary.similar_application_ids,
                      '[]'::JSONB
                  ) @> jsonb_build_array(p_source_application_id)
              )
          OR
          (
              COALESCE(current_count.sentence_match_count, 0)
                  >= p_similar_case_threshold
          )
              IS DISTINCT FROM
              (
                  COALESCE(
                      target_summary.similar_case_ids,
                      '[]'::JSONB
                  ) @> jsonb_build_array(p_source_application_id)
              )
          OR
          (
              COALESCE(current_count.sentence_match_count, 0)
                  >= p_tah_case_threshold
          )
              IS DISTINCT FROM
              (
                  COALESCE(
                      target_summary.tah_case_ids,
                      '[]'::JSONB
                  ) @> jsonb_build_array(p_source_application_id)
              )
      );

    /*
     * Always include the source because its full summary is rebuilt. Include
     * another application only when Step 3 found a list that must change.
     *
     * Example: source 10 changed its relationship with applications 30 and 50.
     * The result is [10, 30, 50]. Unchanged application 20 is not updated.
     */
    SELECT ARRAY_AGG(candidate.application_id ORDER BY candidate.application_id)
    INTO affected_application_ids
    FROM (
        SELECT p_source_application_id AS application_id

        UNION

        SELECT UNNEST(changed_target_application_ids) AS application_id
    ) AS candidate;

    /*
     * STEP 4: Lock the application rows that will be changed.
     *
     * Two messages can finish at the same time and try to update the same
     * summary. These locks make one transaction wait for the other. The IDs are
     * locked in number order to reduce deadlocks. PostgreSQL releases the locks
     * when Python commits or rolls back.
     *
     * Unchanged applications are not locked.
     */
    /* Lock by application ID, even when the table row does not exist yet. */
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            'titan_similar_cases:' || affected.application_id::TEXT,
            0
        )
    )
    FROM UNNEST(affected_application_ids) AS affected(application_id)
    ORDER BY affected.application_id;

    /* Lock existing rows so their lists cannot change during Step 5. */
    PERFORM summary.application_id
    FROM tsp.titan_similar_cases AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id
    FOR UPDATE;

    /*
     * STEP 5: Build and save the final summary rows.
     *
     * Rebuild the complete source row from its current matches. For each changed
     * target row, change only the source application's ID. Keep every unrelated
     * application ID already stored in that target row.
     */
    /* Change the two Step 1 arrays back into application/count rows. */
    WITH current_counts AS (
        SELECT *
        FROM UNNEST(
            current_target_application_ids,
            current_sentence_match_counts
        ) AS current_count(target_application_id, sentence_match_count)
    ),
    /*
     * Build all three lists for the source application.
     *
     * Example counts: application 20=1, application 30=2, application 40=3.
     * The source receives:
     *
     *   similar_application_ids = [20, 30, 40]
     *   similar_case_ids        = [30, 40]
     *   tah_case_ids            = [40]
     *
     * Do not create a source row when there are no sentence matches.
     */
    rebuilt_source_summary AS (
        SELECT
            p_source_application_id AS application_id,
            p_analysis_group AS analysis_group,
            COALESCE(
                jsonb_agg(
                    target_application_id ORDER BY target_application_id
                ) FILTER (WHERE sentence_match_count >= 1),
                '[]'::JSONB
            ) AS similar_application_ids,
            COALESCE(
                jsonb_agg(
                    target_application_id ORDER BY target_application_id
                ) FILTER (
                    WHERE sentence_match_count >= p_similar_case_threshold
                ),
                '[]'::JSONB
            ) AS similar_case_ids,
            COALESCE(
                jsonb_agg(
                    target_application_id ORDER BY target_application_id
                ) FILTER (
                    WHERE sentence_match_count >= p_tah_case_threshold
                ),
                '[]'::JSONB
            ) AS tah_case_ids
        FROM current_counts
        HAVING COUNT(*) > 0
    ),
    /*
     * Load the saved lists for the target rows selected in Step 3.
     * summary_exists tells us whether a row is already stored.
     * sentence_match_count is NULL when the old match disappeared.
     */
    target_rows AS (
        SELECT
            changed.application_id,
            existing.application_id IS NOT NULL AS summary_exists,
            COALESCE(existing.similar_application_ids, '[]'::JSONB)
                AS similar_application_ids,
            COALESCE(existing.similar_case_ids, '[]'::JSONB)
                AS similar_case_ids,
            COALESCE(existing.tah_case_ids, '[]'::JSONB)
                AS tah_case_ids,
            current_count.sentence_match_count
        FROM UNNEST(changed_target_application_ids)
            AS changed(application_id)
        LEFT JOIN tsp.titan_similar_cases AS existing
            ON existing.application_id = changed.application_id
        LEFT JOIN current_counts AS current_count
            ON current_count.target_application_id = changed.application_id
    ),
    /*
     * Fix each changed target row one list at a time.
     *
     * First remove the source ID and any incorrect self-reference. Add the
     * source ID back only when the current count meets that list's rule. UNION
     * removes duplicates, and ORDER BY keeps the IDs in a clear order.
     *
     * Example: target 20 contains [10, 60, 70]. If source 10 no longer matches,
     * the result is [60, 70]. The matches with 60 and 70 stay unchanged.
     */
    patched_target_summaries AS (
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
        WHERE target_row.summary_exists
           OR target_row.sentence_match_count IS NOT NULL
    ),
    /* Put the source row and changed target rows into one result. */
    summaries_to_save AS (
        SELECT * FROM rebuilt_source_summary

        UNION ALL

        SELECT * FROM patched_target_summaries
    ),
    /* Keep only rows that have at least one application in any list. */
    summaries_with_matches AS (
        SELECT *
        FROM summaries_to_save
        WHERE jsonb_array_length(similar_application_ids) > 0
           OR jsonb_array_length(similar_case_ids) > 0
           OR jsonb_array_length(tah_case_ids) > 0
    ),
    /*
     * Delete a saved row when its new result has no matches. This can happen
     * when the source has no matches or when a target matched only the source.
     */
    deleted_empty_summaries AS (
        DELETE FROM tsp.titan_similar_cases AS existing
        WHERE existing.application_id = ANY(affected_application_ids)
          AND NOT EXISTS (
              SELECT 1
              FROM summaries_with_matches AS saved
              WHERE saved.application_id = existing.application_id
          )
        RETURNING existing.application_id
    )
    /* Create missing rows and update existing rows with the final lists. */
    INSERT INTO tsp.titan_similar_cases AS saved_summary (
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
    ON CONFLICT (application_id)
    DO UPDATE SET
        analysis_group = EXCLUDED.analysis_group,
        similar_application_ids = EXCLUDED.similar_application_ids,
        similar_case_ids = EXCLUDED.similar_case_ids,
        tah_case_ids = EXCLUDED.tah_case_ids,
        last_modified = NOW()
    /*
     * Do not update a row when every value is already the same. This keeps
     * last_modified unchanged and avoids an unnecessary database write.
     */
    WHERE saved_summary.analysis_group IS DISTINCT FROM EXCLUDED.analysis_group
       OR saved_summary.similar_application_ids IS DISTINCT FROM EXCLUDED.similar_application_ids
       OR saved_summary.similar_case_ids IS DISTINCT FROM EXCLUDED.similar_case_ids
       OR saved_summary.tah_case_ids IS DISTINCT FROM EXCLUDED.tah_case_ids;

    /*
     * STEP 6: Return the source and the target rows that needed a list change.
     *
     * Always return the source so Python can read its final counts. Return a
     * target only when Step 3 found a change. If a row was deleted because all
     * lists became empty, return zero for both counts.
     */
    RETURN QUERY
    SELECT
        affected.application_id,
        COALESCE(summary.similar_case_count, 0),
        COALESCE(summary.tah_case_count, 0)
    FROM UNNEST(affected_application_ids) AS affected(application_id)
    LEFT JOIN tsp.titan_similar_cases AS summary
        ON summary.application_id = affected.application_id
    ORDER BY affected.application_id;
END;
$$;

COMMENT ON FUNCTION tsp.refresh_titan_similar_cases(INTEGER, VARCHAR, INTEGER, INTEGER)
IS 'Counts unique sentence pairs and saves similar-application, similar-case, and TAH results in both directions.';
