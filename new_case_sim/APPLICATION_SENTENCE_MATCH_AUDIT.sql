/*
 * Audit every sentence match between two applications.
 *
 * Set these values when running the queries:
 *   :application_id_a = source application
 *   :application_id_b = target application
 *
 * titan_matches already contains sentence relationships that passed Sentence
 * Similarity's configured threshold. Distance 0.0 is exact. A distance greater
 * than 0.0 is similar.
 *
 * Case rules used below:
 *   1 or more sentence pairs = similar application
 *   2 or more sentence pairs = similar case
 *   3 or more sentence pairs = TAH case
 */


/*
 * Query 1: Show every matching sentence pair from application A to application B.
 *
 * One source sentence matching three target occurrences produces three rows.
 * The total columns repeat on each row so the result also explains the case level.
 */
WITH matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
    WHERE source_match.application_id = :application_id_a
      AND EXISTS (
          /* Keep only target sentences owned by application B. */
          SELECT 1
          FROM tsp.titan_matches AS target_owner
          WHERE target_owner.global_id = target.target_global_id::INTEGER
            AND target_owner.application_id = :application_id_b
      )
),
pair_details AS (
    SELECT
        matching_pairs.source_global_id,
        source_sentence.tsp_id AS source_tsp_id,
        source_sentence.sentence_key AS source_sentence_key,
        source_sentence.sentence_content AS source_sentence_content,
        matching_pairs.target_global_id,
        target_sentence.tsp_id AS target_tsp_id,
        target_sentence.sentence_key AS target_sentence_key,
        target_sentence.sentence_content AS target_sentence_content,
        matching_pairs.distance,
        CASE
            WHEN matching_pairs.distance = 0.0 THEN 'exact'
            ELSE 'similar'
        END AS match_type,
        ROUND(((1.0 - matching_pairs.distance) * 100.0)::NUMERIC, 2)
            AS similarity_percentage
    FROM matching_pairs
    JOIN tsp.sentences_vector AS source_sentence
        ON source_sentence.global_id = matching_pairs.source_global_id
    JOIN tsp.sentences_vector AS target_sentence
        ON target_sentence.global_id = matching_pairs.target_global_id
)
SELECT
    COUNT(*) OVER () AS total_sentence_match_count,
    COUNT(*) FILTER (WHERE pair_details.distance = 0.0) OVER ()
        AS exact_sentence_match_count,
    COUNT(*) FILTER (WHERE pair_details.distance > 0.0) OVER ()
        AS similar_sentence_match_count,
    COUNT(*) OVER () >= 1 AS qualifies_as_similar_application,
    COUNT(*) OVER () >= 2 AS qualifies_as_similar_case,
    COUNT(*) OVER () >= 3 AS qualifies_as_tah_case,
    pair_details.*
FROM pair_details
ORDER BY
    pair_details.source_global_id,
    pair_details.target_global_id;


/*
 * Query 2: Explain the application-level result and compare it with the saved
 * titan_similar_cases rows in both directions.
 *
 * Example with three sentence pairs:
 *   App A should contain App B in all three lists.
 *   App B should contain App A in all three lists.
 *   Each PASS column confirms the calculated value matches the saved value.
 */
WITH parameters AS (
    SELECT
        :application_id_a::INTEGER AS application_id_a,
        :application_id_b::INTEGER AS application_id_b,
        2::INTEGER AS similar_case_threshold,
        3::INTEGER AS tah_case_threshold
),
matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM parameters
    JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.application_id_a
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
    WHERE EXISTS (
        SELECT 1
        FROM tsp.titan_matches AS target_owner
        WHERE target_owner.global_id = target.target_global_id::INTEGER
          AND target_owner.application_id = parameters.application_id_b
    )
),
match_counts AS (
    SELECT
        COUNT(*)::INTEGER AS total_match_count,
        COUNT(*) FILTER (WHERE distance = 0.0)::INTEGER AS exact_match_count,
        COUNT(*) FILTER (WHERE distance > 0.0)::INTEGER AS similar_match_count
    FROM matching_pairs
),
audit AS (
    SELECT
        parameters.*,
        match_counts.*,
        match_counts.total_match_count >= 1
            AS expected_similar_application,
        match_counts.total_match_count >= parameters.similar_case_threshold
            AS expected_similar_case,
        match_counts.total_match_count >= parameters.tah_case_threshold
            AS expected_tah_case,
        COALESCE(
            source_summary.similar_application_ids @>
                jsonb_build_array(parameters.application_id_b),
            FALSE
        ) AS source_saved_similar_application,
        COALESCE(
            source_summary.similar_case_ids @>
                jsonb_build_array(parameters.application_id_b),
            FALSE
        ) AS source_saved_similar_case,
        COALESCE(
            source_summary.tah_case_ids @>
                jsonb_build_array(parameters.application_id_b),
            FALSE
        ) AS source_saved_tah_case,
        COALESCE(
            target_summary.similar_application_ids @>
                jsonb_build_array(parameters.application_id_a),
            FALSE
        ) AS target_saved_similar_application,
        COALESCE(
            target_summary.similar_case_ids @>
                jsonb_build_array(parameters.application_id_a),
            FALSE
        ) AS target_saved_similar_case,
        COALESCE(
            target_summary.tah_case_ids @>
                jsonb_build_array(parameters.application_id_a),
            FALSE
        ) AS target_saved_tah_case,
        source_summary.similar_application_ids AS source_similar_application_ids,
        source_summary.similar_case_ids AS source_similar_case_ids,
        source_summary.tah_case_ids AS source_tah_case_ids,
        target_summary.similar_application_ids AS target_similar_application_ids,
        target_summary.similar_case_ids AS target_similar_case_ids,
        target_summary.tah_case_ids AS target_tah_case_ids
    FROM parameters
    CROSS JOIN match_counts
    LEFT JOIN tsp.titan_similar_cases AS source_summary
        ON source_summary.application_id = parameters.application_id_a
    LEFT JOIN tsp.titan_similar_cases AS target_summary
        ON target_summary.application_id = parameters.application_id_b
)
SELECT
    application_id_a AS source_application_id,
    application_id_b AS target_application_id,
    total_match_count,
    exact_match_count,
    similar_match_count,
    CASE
        WHEN expected_tah_case THEN
            'Three or more sentence pairs: similar application, similar case, and TAH case.'
        WHEN expected_similar_case THEN
            'Two sentence pairs: similar application and similar case, but not a TAH case.'
        WHEN expected_similar_application THEN
            'One sentence pair: similar application only.'
        ELSE
            'No sentence pairs: the applications should not be in any matching list.'
    END AS calculated_reason,
    expected_similar_application,
    expected_similar_case,
    expected_tah_case,
    source_saved_similar_application,
    source_saved_similar_case,
    source_saved_tah_case,
    target_saved_similar_application,
    target_saved_similar_case,
    target_saved_tah_case,
    CASE
        WHEN source_saved_similar_application = expected_similar_application
         AND target_saved_similar_application = expected_similar_application
            THEN 'PASS'
        ELSE 'FAIL'
    END AS similar_application_check,
    CASE
        WHEN source_saved_similar_case = expected_similar_case
         AND target_saved_similar_case = expected_similar_case
            THEN 'PASS'
        ELSE 'FAIL'
    END AS similar_case_check,
    CASE
        WHEN source_saved_tah_case = expected_tah_case
         AND target_saved_tah_case = expected_tah_case
            THEN 'PASS'
        ELSE 'FAIL'
    END AS tah_case_check,
    source_similar_application_ids,
    source_similar_case_ids,
    source_tah_case_ids,
    target_similar_application_ids,
    target_similar_case_ids,
    target_tah_case_ids
FROM audit;
