/*
 * Change 10 and 20 to the two application IDs, then run this in pgAdmin.
 * The query returns every saved Titan match from application A to application B.
 */
WITH parameters AS (
    SELECT
        10::INTEGER AS application_id_a,
        20::INTEGER AS application_id_b
),
matching_pairs AS (
    SELECT DISTINCT
        parameters.application_id_a AS source_application_id,
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        parameters.application_id_b AS target_application_id,
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
)
SELECT
    COUNT(*) OVER () AS total_match_count,
    matching_pairs.source_application_id,
    matching_pairs.source_global_id,
    matching_pairs.source_tsp_id,
    source_sentence.sentence_content AS source_sentence,
    matching_pairs.target_application_id,
    matching_pairs.target_global_id,
    target_sentence.tsp_id AS target_tsp_id,
    target_sentence.sentence_content AS target_sentence,
    matching_pairs.distance,
    CASE
        WHEN matching_pairs.distance = 0.0 THEN 'exact'
        ELSE 'similar'
    END AS match_type
FROM matching_pairs
JOIN tsp.sentences_vector AS source_sentence
    ON source_sentence.global_id = matching_pairs.source_global_id
JOIN tsp.sentences_vector AS target_sentence
    ON target_sentence.global_id = matching_pairs.target_global_id
ORDER BY
    matching_pairs.source_global_id,
    matching_pairs.target_global_id;
