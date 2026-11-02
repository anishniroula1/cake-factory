/*
 * Delete every Titan sentence relationship owned by one document.
 *
 * Titan matches are bidirectional. If global ID 100 contains 200, global ID
 * 200 also contains 100. Deleting tsp_id 10 must therefore remove:
 *
 *   1. bucket rows owned by tsp_id 10;
 *   2. global IDs from tsp_id 10 inside other sentences' JSONB objects; and
 *   3. reverse bucket rows that become empty after those keys are removed.
 *
 * This function does not delete text_similarity_input, sentences_vector, or
 * embeddings. After this function commits, Python deletes embeddings and
 * sentence rows through their own database stores. text_similarity_input is
 * kept so Case Similarity can refresh the affected application afterward.
 */
CREATE FUNCTION tsp.delete_titan_matches_for_tsp(
    p_tsp_id BIGINT
)
RETURNS TABLE (
    affected_application_ids BIGINT[],
    affected_tsp_ids BIGINT[],
    has_deleted_document BOOLEAN,
    source_global_id_count INTEGER,
    deleted_match_bucket_count INTEGER,
    updated_reverse_bucket_count INTEGER,
    deleted_empty_bucket_count INTEGER
)
LANGUAGE PLPGSQL
AS $$
DECLARE
    deleted_global_ids INTEGER[];
    locked_row_id BIGINT;
    source_id_count INTEGER := 0;
    deleted_bucket_count INTEGER := 0;
    updated_reverse_count INTEGER := 0;
    deleted_empty_count INTEGER := 0;
    affected_applications BIGINT[] := ARRAY[]::BIGINT[];
    affected_documents BIGINT[] := ARRAY[]::BIGINT[];
BEGIN
    /*
     * Stop immediately when this document has no Titan matches.
     *
     * Example: tsp_id 99 is not present in titan_matches. There are no source
     * rows or reverse relationships to remove, so return empty lists and false.
     */
    IF NOT EXISTS (
        SELECT 1
        FROM tsp.titan_matches AS source_match
        WHERE source_match.tsp_id = p_tsp_id
    ) THEN
        RETURN QUERY
        SELECT
            ARRAY[]::BIGINT[],
            ARRAY[]::BIGINT[],
            FALSE,
            0,
            0,
            0,
            0;
        RETURN;
    END IF;

    /*
     * Read the global IDs owned by this document directly from Titan matches.
     *
     * Example: if tsp_id 10 owns global IDs 100 and 101, both IDs are used to
     * delete their bucket rows and remove their keys from reverse matches.
     * sentences_vector is cleaned separately after this function finishes.
     */
    SELECT COALESCE(
        array_agg(source_id.global_id ORDER BY source_id.global_id),
        ARRAY[]::INTEGER[]
    )
    INTO deleted_global_ids
    FROM (
        SELECT DISTINCT match_row.global_id
        FROM tsp.titan_matches AS match_row
        WHERE match_row.tsp_id = p_tsp_id
    ) AS source_id;

    source_id_count := cardinality(deleted_global_ids);
    /*
     * Save every application and document affected by this deletion before
     * changing the match rows.
     *
     * Example: the deleted document belongs to application 10 and matches
     * documents from applications 20 and 30. The returned application list is
     * [10, 20, 30], and affected_tsp_ids contains the source and target tsp IDs.
     * These lists can be used to refresh case-level summaries afterward.
     */
    WITH source_rows AS (
        SELECT global_id, tsp_id, application_id, matches
        FROM tsp.titan_matches
        WHERE tsp_id = p_tsp_id
    ),
    affected_owners AS (
        SELECT source_row.application_id, source_row.tsp_id
        FROM source_rows AS source_row

        UNION

        SELECT target_owner.application_id, target_owner.tsp_id
        FROM source_rows AS source_row
        CROSS JOIN LATERAL jsonb_object_keys(source_row.matches)
            AS target(target_global_id)
        JOIN tsp.titan_matches AS target_owner
            ON target_owner.global_id = target.target_global_id::INTEGER
    )
    SELECT
        ARRAY(
            SELECT DISTINCT owner.application_id::BIGINT
            FROM affected_owners AS owner
            ORDER BY owner.application_id::BIGINT
        ),
        ARRAY(
            SELECT DISTINCT owner.tsp_id::BIGINT
            FROM affected_owners AS owner
            ORDER BY owner.tsp_id::BIGINT
        )
    INTO affected_applications, affected_documents;

    /*
     * Use the same advisory-lock identity as the bidirectional upsert function.
     *
     * All 16 possible owner buckets are locked for every deleted global ID.
     * Existing reverse rows are also locked. This makes a concurrent matcher
     * wait until the deletion transaction finishes instead of adding a match
     * while that sentence is being removed.
     *
     * Locks are taken in numeric order to reduce deadlocks. PostgreSQL releases
     * every lock automatically when the outer transaction commits or rolls back.
     */
    FOR locked_row_id IN
        WITH reverse_rows AS (
            SELECT DISTINCT
                target.target_global_id::INTEGER AS owner_global_id,
                tsp.titan_match_bucket(source_match.global_id) AS bucket_number
            FROM tsp.titan_matches AS source_match
            CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
                AS target(target_global_id)
            WHERE source_match.global_id = ANY(deleted_global_ids)
              AND target.target_global_id::INTEGER <> ALL(deleted_global_ids)
        ),
        affected_rows AS (
            /* Lock every possible bucket owned by a sentence being deleted. */
            SELECT
                deleted_global_id AS owner_global_id,
                bucket_number::SMALLINT
            FROM unnest(deleted_global_ids) AS deleted(deleted_global_id)
            CROSS JOIN generate_series(0, 15) AS bucket(bucket_number)

            UNION

            /* Lock the exact target buckets that contain reverse references. */
            SELECT owner_global_id, bucket_number
            FROM reverse_rows
        )
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    'titan_matches',
                    affected_rows.owner_global_id,
                    affected_rows.bucket_number
                ),
                0
            ) AS lock_id
        FROM affected_rows
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /*
     * Remove deleted IDs from their matching sentences.
     *
     * Example before deleting 100 and 101:
     *
     *   owner 200 matches {"100": 0.0, "101": 0.10, "300": 0.20}
     *
     * Example afterward:
     *
     *   owner 200 matches {"300": 0.20}
     *
     * The primary key finds each owner and bucket directly. The function does
     * not scan every JSONB value in titan_matches.
     */
    WITH reverse_removals AS (
        SELECT
            target.target_global_id::INTEGER AS owner_global_id,
            tsp.titan_match_bucket(source_match.global_id) AS bucket_number,
            array_agg(DISTINCT source_match.global_id::TEXT ORDER BY source_match.global_id::TEXT)
                AS removed_keys
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
            AS target(target_global_id)
        WHERE source_match.global_id = ANY(deleted_global_ids)
          AND target.target_global_id::INTEGER <> ALL(deleted_global_ids)
        GROUP BY
            target.target_global_id::INTEGER,
            tsp.titan_match_bucket(source_match.global_id)
    )
    UPDATE tsp.titan_matches AS reverse_match
    SET
        matches = reverse_match.matches - reverse_removals.removed_keys,
        last_modified = NOW()
    FROM reverse_removals
    WHERE reverse_match.global_id = reverse_removals.owner_global_id
      AND reverse_match.match_bucket_number = reverse_removals.bucket_number;

    GET DIAGNOSTICS updated_reverse_count = ROW_COUNT;

    /* Delete only reverse rows touched above when their JSONB object is empty. */
    WITH reverse_rows AS (
        SELECT DISTINCT
            target.target_global_id::INTEGER AS owner_global_id,
            tsp.titan_match_bucket(source_match.global_id) AS bucket_number
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
            AS target(target_global_id)
        WHERE source_match.global_id = ANY(deleted_global_ids)
          AND target.target_global_id::INTEGER <> ALL(deleted_global_ids)
    )
    DELETE FROM tsp.titan_matches AS empty_match
    USING reverse_rows
    WHERE empty_match.global_id = reverse_rows.owner_global_id
      AND empty_match.match_bucket_number = reverse_rows.bucket_number
      AND empty_match.matches = '{}'::JSONB;

    GET DIAGNOSTICS deleted_empty_count = ROW_COUNT;

    /* Remove every bucket owned by the deleted document. */
    DELETE FROM tsp.titan_matches AS source_match
    WHERE source_match.global_id = ANY(deleted_global_ids)
       OR source_match.tsp_id = p_tsp_id;

    GET DIAGNOSTICS deleted_bucket_count = ROW_COUNT;

    RETURN QUERY
    SELECT
        affected_applications,
        affected_documents,
        TRUE,
        source_id_count,
        deleted_bucket_count,
        updated_reverse_count,
        deleted_empty_count;
END;
$$;


COMMENT ON FUNCTION tsp.delete_titan_matches_for_tsp(BIGINT)
IS
'Returns affected application/document IDs and removes one document from bidirectional Titan match buckets.';
