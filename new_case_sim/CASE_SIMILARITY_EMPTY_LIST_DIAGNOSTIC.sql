/*
 * Use this query when similar_application_ids, similar_case_ids, and
 * tah_case_ids are all empty.
 *
 * Change only source_application_id and analysis_group below, then run the
 * complete file in pgAdmin.
 *
 * The diagnostic_status column explains what happened to each saved match:
 *
 *   ELIGIBLE FOR CASE SUMMARY
 *       The relationship should appear in similar_application_ids.
 *
 *   REVERSE OWNER ROW IS MISSING
 *       The target global ID is inside the source JSONB, but the matching
 *       sentence does not have its own titan_matches row. The bidirectional
 *       write did not finish or the row was created by an older function.
 *
 *   ANALYSIS GROUP DOES NOT MATCH
 *       The target belongs to a different analysis group. Case Similarity
 *       intentionally ignores it.
 *
 *   SAME APPLICATION IS EXCLUDED
 *       The source and target sentences belong to the same application.
 *
 *   NO TITAN MATCH ROWS / NO SAVED RELATIONSHIPS
 *       Sentence Similarity did not save a usable relationship for the source
 *       application.
 */
WITH parameters AS (
    SELECT
        10::INTEGER AS source_application_id,
        'Asylee'::VARCHAR(30) AS analysis_group
),
source_stats AS (
    /* Show whether the source application owns any Titan match data. */
    SELECT
        COUNT(source_match.global_id)::INTEGER AS source_bucket_rows,
        COALESCE(SUM(jsonb_object_length(source_match.matches)), 0)::INTEGER
            AS saved_relationship_count
    FROM parameters
    LEFT JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.source_application_id
),
saved_pairs AS (
    /* Turn each source JSONB entry into one source-to-target sentence pair. */
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM parameters
    JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.source_application_id
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
),
resolved_pairs AS (
    /* Find the application and analysis group that own each target sentence. */
    SELECT
        saved_pairs.*,
        target_owner.application_id AS target_application_id,
        target_owner.tsp_id AS target_tsp_id,
        target_groups.analysis_groups AS target_analysis_groups
    FROM saved_pairs
    LEFT JOIN LATERAL (
        SELECT DISTINCT
            target_match.application_id,
            target_match.tsp_id
        FROM tsp.titan_matches AS target_match
        WHERE target_match.global_id = saved_pairs.target_global_id
    ) AS target_owner ON TRUE
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(DISTINCT target_input.analysis_group ORDER BY target_input.analysis_group)
            AS analysis_groups
        FROM tsp.text_similarity_input AS target_input
        WHERE target_input.application_id = target_owner.application_id
    ) AS target_groups ON TRUE
),
diagnostic_rows AS (
    /* Return one row even when the source has no relationships. */
    SELECT
        resolved_pairs.source_global_id,
        resolved_pairs.source_tsp_id,
        resolved_pairs.target_global_id,
        resolved_pairs.target_application_id,
        resolved_pairs.target_tsp_id,
        resolved_pairs.target_analysis_groups,
        resolved_pairs.distance,
        CASE
            WHEN source_stats.source_bucket_rows = 0
                THEN 'NO TITAN MATCH ROWS FOR SOURCE APPLICATION'
            WHEN source_stats.saved_relationship_count = 0
                THEN 'NO SAVED RELATIONSHIPS FOR SOURCE APPLICATION'
            WHEN resolved_pairs.target_application_id IS NULL
                THEN 'REVERSE OWNER ROW IS MISSING'
            WHEN resolved_pairs.target_application_id = parameters.source_application_id
                THEN 'SAME APPLICATION IS EXCLUDED'
            WHEN resolved_pairs.target_analysis_groups IS NULL
                THEN 'TARGET APPLICATION IS MISSING FROM TEXT_SIMILARITY_INPUT'
            WHEN NOT parameters.analysis_group = ANY(resolved_pairs.target_analysis_groups)
                THEN 'ANALYSIS GROUP DOES NOT MATCH'
            ELSE 'ELIGIBLE FOR CASE SUMMARY'
        END AS diagnostic_status
    FROM parameters
    CROSS JOIN source_stats
    LEFT JOIN resolved_pairs ON TRUE
)
SELECT
    parameters.source_application_id,
    parameters.analysis_group AS requested_analysis_group,
    source_stats.source_bucket_rows,
    source_stats.saved_relationship_count,
    diagnostic_rows.source_global_id,
    diagnostic_rows.source_tsp_id,
    diagnostic_rows.target_global_id,
    diagnostic_rows.target_application_id,
    diagnostic_rows.target_tsp_id,
    diagnostic_rows.target_analysis_groups,
    diagnostic_rows.distance,
    diagnostic_rows.diagnostic_status
FROM parameters
CROSS JOIN source_stats
CROSS JOIN diagnostic_rows
ORDER BY
    diagnostic_rows.diagnostic_status,
    diagnostic_rows.source_global_id,
    diagnostic_rows.target_global_id;
