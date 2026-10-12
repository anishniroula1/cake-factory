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
 *   TITAN MATCH APPLICATION ID DOES NOT MATCH TEXT_SIMILARITY_INPUT
 *       Titan matches exist for the application's tsp_id values, but their
 *       saved application_id is different from text_similarity_input.
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
    /* Find rows using the application_id saved directly in titan_matches. */
    SELECT
        COUNT(source_match.global_id)::INTEGER AS source_bucket_rows,
        COALESCE(SUM(tsp.titan_match_object_size(source_match.matches)), 0)::INTEGER
            AS saved_relationship_count
    FROM parameters
    LEFT JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.source_application_id
),
document_owner_stats AS (
    /*
     * Check the same application through tsp_id and text_similarity_input.
     *
     * Example: text_similarity_input says tsp_id 500 belongs to application 10,
     * but its titan_matches row says application_id 20. source_stats returns 0,
     * while this section finds the row and reports saved_application_ids={20}.
     */
    SELECT
        COUNT(titan_match.global_id)::INTEGER AS rows_found_through_tsp_id,
        ARRAY_REMOVE(
            ARRAY_AGG(DISTINCT titan_match.application_id ORDER BY titan_match.application_id),
            NULL
        ) AS saved_application_ids
    FROM parameters
    LEFT JOIN tsp.text_similarity_input AS source_input
        ON source_input.application_id = parameters.source_application_id
    LEFT JOIN tsp.titan_matches AS titan_match
        ON titan_match.tsp_id = source_input.tsp_id
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
                 AND document_owner_stats.rows_found_through_tsp_id > 0
                THEN 'TITAN MATCH APPLICATION ID DOES NOT MATCH TEXT_SIMILARITY_INPUT'
            WHEN source_stats.source_bucket_rows = 0
                THEN 'NO TITAN MATCH ROWS FOR THIS APPLICATION OR ITS DOCUMENTS'
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
    CROSS JOIN document_owner_stats
    LEFT JOIN resolved_pairs ON TRUE
)
SELECT
    parameters.source_application_id,
    parameters.analysis_group AS requested_analysis_group,
    source_stats.source_bucket_rows,
    source_stats.saved_relationship_count,
    document_owner_stats.rows_found_through_tsp_id,
    document_owner_stats.saved_application_ids,
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
CROSS JOIN document_owner_stats
CROSS JOIN diagnostic_rows
ORDER BY
    diagnostic_rows.diagnostic_status,
    diagnostic_rows.source_global_id,
    diagnostic_rows.target_global_id;
