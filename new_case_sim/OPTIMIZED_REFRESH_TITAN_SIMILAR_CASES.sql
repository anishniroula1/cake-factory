/*
 * Replace only the refresh function. The 16-bucket tsp.titan_matches table and
 * the tsp.titan_similar_cases table do not change.
 *
 * The case rules also stay the same:
 *   1 unique source/target global_id pair  = similar application
 *   2 unique source/target global_id pairs = similar case
 *   3 or more unique pairs                 = TAH case
 *
 * Every category remains bidirectional. If application 10 qualifies against
 * application 20, application 20 receives application 10 in the same list.
 *
 * This version is faster because it expands the source match buckets once and
 * updates only reverse application rows whose 1/2/3 category actually changed.
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
    /* These two arrays are parallel: ID at position 1 uses count at position 1. */
    current_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];
    current_sentence_match_counts INTEGER[] := ARRAY[]::INTEGER[];

    /* Previous targets are needed so a relationship that disappeared is removed. */
    previous_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];

    /* Only targets whose reverse 1/2/3 membership changed are written. */
    changed_target_application_ids INTEGER[] := ARRAY[]::INTEGER[];
    affected_application_ids INTEGER[] := ARRAY[]::INTEGER[];
BEGIN
    /*
     * STEP 1: Expand all 16 source buckets once and count unique sentence pairs
     * per target application.
     *
     * Example:
     *   (source 101, target 201)
     *   (source 102, target 202)
     *   (source 102, target 203)
     *
     * If targets 201, 202, and 203 belong to application 20, its count is 3.
     * Application 20 is therefore a TAH case in both directions.
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
         * A target sentence can own multiple bucket rows, but every bucket has
         * the same application_id. Read one row instead of joining all buckets
         * and removing the duplicates later.
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
           * EXISTS checks the analysis group without multiplying one sentence
           * pair by every text_similarity_input document in the application.
           */
          AND EXISTS (
              SELECT 1
              FROM tsp.text_similarity_input AS target_input
              WHERE target_input.application_id = target_owner.application_id
                AND target_input.analysis_group = p_analysis_group
          )
        GROUP BY target_owner.application_id
    )
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
     * STEP 2: Load old targets from all three source lists. Normally the case
     * and TAH lists are subsets of similar_application_ids. Reading all three
     * also repairs an older row if those lists became inconsistent.
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
     * STEP 3: Find reverse summaries that truly need a change.
     *
     * Count 5 -> 6 remains TAH, so the target row is not rewritten.
     * Count 2 -> 3 enters TAH, so that target row is updated.
     * Count 1 -> 0 removes the source from the target row.
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

    /* The source is always checked. Only changed reverse targets are included. */
    SELECT ARRAY_AGG(candidate.application_id ORDER BY candidate.application_id)
    INTO affected_application_ids
    FROM (
        SELECT p_source_application_id AS application_id

        UNION

        SELECT UNNEST(changed_target_application_ids) AS application_id
    ) AS candidate;

    /*
     * STEP 4: Lock affected summaries in a stable order. Unchanged target rows
     * are not locked, so unrelated document messages can continue processing.
     */
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            'titan_similar_cases:' || affected.application_id::TEXT,
            0
        )
    )
    FROM UNNEST(affected_application_ids) AS affected(application_id)
    ORDER BY affected.application_id;

    PERFORM summary.application_id
    FROM tsp.titan_similar_cases AS summary
    WHERE summary.application_id = ANY(affected_application_ids)
    ORDER BY summary.application_id
    FOR UPDATE;

    /*
     * STEP 5: Rebuild the source and patch only changed reverse targets.
     * Existing relationships that do not involve the source are preserved.
     */
    WITH current_counts AS (
        SELECT *
        FROM UNNEST(
            current_target_application_ids,
            current_sentence_match_counts
        ) AS current_count(target_application_id, sentence_match_count)
    ),
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
    summaries_to_save AS (
        SELECT * FROM rebuilt_source_summary

        UNION ALL

        SELECT * FROM patched_target_summaries
    ),
    summaries_with_matches AS (
        SELECT *
        FROM summaries_to_save
        WHERE jsonb_array_length(similar_application_ids) > 0
           OR jsonb_array_length(similar_case_ids) > 0
           OR jsonb_array_length(tah_case_ids) > 0
    ),
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
    /* Do not create WAL when the rebuilt source lists did not change. */
    WHERE saved_summary.analysis_group IS DISTINCT FROM EXCLUDED.analysis_group
       OR saved_summary.similar_application_ids IS DISTINCT FROM EXCLUDED.similar_application_ids
       OR saved_summary.similar_case_ids IS DISTINCT FROM EXCLUDED.similar_case_ids
       OR saved_summary.tah_case_ids IS DISTINCT FROM EXCLUDED.tah_case_ids;

    /*
     * STEP 6: Return the source and reverse targets that actually changed.
     * A deleted row is returned with empty lists and is_tah=false.
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
IS 'Reads Titan match buckets once and updates bidirectional case summaries only when a 1/2/3 relationship tier changes.';
