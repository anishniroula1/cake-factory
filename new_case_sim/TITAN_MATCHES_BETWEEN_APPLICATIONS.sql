/*
 * Show every Titan sentence match between two applications.
 *
 * In the parameters section below, replace 10 and 20 with the two application
 * IDs you want to check. Then run the complete query in pgAdmin.
 *
 * This query reads only tsp.titan_matches. It does not read sentence content
 * or the application summary table.
 *
 * Example:
 *   application_id_a = 10
 *   application_id_b = 20
 *
 * The result shows every global ID from app 10 that matches a global ID owned
 * by app 20. It also checks that app 20 stores the reverse relationship.
 */
WITH parameters AS (
    /* Change only these two numbers. Do not put quotes around them. */
    SELECT
        10::INTEGER AS application_id_a,
        20::INTEGER AS application_id_b
),
forward_pairs AS (
    /* Expand app A's JSONB objects into one row for every matching global ID. */
    SELECT DISTINCT
        source_match.application_id AS source_application_id,
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM parameters
    JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.application_id_a
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
),
application_pairs AS (
    /* Keep only target global IDs that belong to app B. */
    SELECT DISTINCT
        forward_pairs.source_application_id,
        forward_pairs.source_global_id,
        forward_pairs.source_tsp_id,
        target_owner.application_id AS target_application_id,
        forward_pairs.target_global_id,
        target_owner.tsp_id AS target_tsp_id,
        forward_pairs.distance
    FROM parameters
    JOIN forward_pairs ON TRUE
    JOIN tsp.titan_matches AS target_owner
        ON target_owner.global_id = forward_pairs.target_global_id
       AND target_owner.application_id = parameters.application_id_b
),
pairs_with_reverse_distance AS (
    /*
     * Read the reverse distance saved on the target sentence.
     *
     * Example: if 100 -> 200 exists, look for 200 -> 100.
     */
    SELECT
        application_pairs.*,
        (
            SELECT (reverse_match.matches ->>
                application_pairs.source_global_id::TEXT)::DOUBLE PRECISION
            FROM tsp.titan_matches AS reverse_match
            WHERE reverse_match.global_id = application_pairs.target_global_id
              AND reverse_match.application_id = application_pairs.target_application_id
              AND reverse_match.matches ? application_pairs.source_global_id::TEXT
            LIMIT 1
        ) AS reverse_distance
    FROM application_pairs
)
SELECT
    COUNT(*) OVER () AS total_sentence_match_count,
    source_application_id,
    source_global_id,
    source_tsp_id,
    target_application_id,
    target_global_id,
    target_tsp_id,
    distance,
    CASE
        WHEN distance = 0.0 THEN 'exact'
        ELSE 'similar'
    END AS match_type,
    ROUND(((1.0 - distance) * 100.0)::NUMERIC, 2)
        AS similarity_percentage,
    reverse_distance,
    CASE
        WHEN reverse_distance IS NULL THEN 'FAIL - reverse relationship is missing'
        WHEN ABS(distance - reverse_distance) > 0.00000001
            THEN 'FAIL - reverse distance is different'
        ELSE 'PASS'
    END AS bidirectional_check
FROM pairs_with_reverse_distance
ORDER BY
    source_global_id,
    target_global_id;
