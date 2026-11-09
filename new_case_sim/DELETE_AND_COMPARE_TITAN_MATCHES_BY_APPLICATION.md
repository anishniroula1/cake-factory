# Delete and compare Titan matches by application

This guide removes every `tsp.titan_matches` relationship owned by one
application, including the reverse references stored by other applications.
It also provides a query that summarizes every application currently matched
to the source application.

Use this only for testing. Do not process the same application through
Sentence Similarity while the delete is running. The function changes only
`tsp.titan_matches`; it does not delete `sentences_vector`,
`text_similarity_input`, or `titan_similar_cases` rows.

## 1. Create the delete function

Run this section once in pgAdmin.

```sql
/*
 * Delete all sentence relationships for one application in both directions.
 *
 * Example:
 *
 *   application 10, global ID 100 -> global ID 200 from application 20
 *   application 20, global ID 200 -> global ID 100 from application 10
 *
 * Deleting application 10 removes every bucket owned by application 10 and
 * removes key "100" from application 20's JSONB bucket. Other keys owned by
 * application 20 stay unchanged.
 */
CREATE OR REPLACE FUNCTION tsp.delete_titan_matches_for_application(
    p_application_id INTEGER
)
RETURNS TABLE (
    affected_application_ids INTEGER[],
    source_global_id_count INTEGER,
    deleted_source_bucket_count INTEGER,
    updated_reverse_bucket_count INTEGER,
    deleted_empty_reverse_bucket_count INTEGER
)
LANGUAGE PLPGSQL
AS $$
DECLARE
    source_global_ids INTEGER[];
    affected_applications INTEGER[] := ARRAY[]::INTEGER[];
    locked_row_id BIGINT;
    source_id_count INTEGER := 0;
    deleted_source_count INTEGER := 0;
    updated_reverse_count INTEGER := 0;
    deleted_empty_reverse_count INTEGER := 0;
BEGIN
    /* Find every sentence global ID owned by the requested application. */
    SELECT COALESCE(
        ARRAY_AGG(source_id.global_id ORDER BY source_id.global_id),
        ARRAY[]::INTEGER[]
    )
    INTO source_global_ids
    FROM (
        SELECT DISTINCT source_match.global_id
        FROM tsp.titan_matches AS source_match
        WHERE source_match.application_id = p_application_id
    ) AS source_id;

    source_id_count := CARDINALITY(source_global_ids);

    /* Nothing is stored for this application, so there is nothing to delete. */
    IF source_id_count = 0 THEN
        RETURN QUERY
        SELECT
            ARRAY[]::INTEGER[],
            0,
            0,
            0,
            0;
        RETURN;
    END IF;

    /*
     * Save the application IDs affected by the delete.
     *
     * Example: application 10 matches applications 20 and 30. The returned
     * list is [10, 20, 30], which identifies the summaries that may need to be
     * refreshed after the test.
     */
    WITH target_applications AS (
        SELECT DISTINCT target_owner.application_id
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL JSONB_OBJECT_KEYS(source_match.matches)
            AS target(target_global_id)
        CROSS JOIN LATERAL (
            SELECT candidate.application_id
            FROM tsp.titan_matches AS candidate
            WHERE candidate.global_id = target.target_global_id::INTEGER
            ORDER BY candidate.match_bucket_number
            LIMIT 1
        ) AS target_owner
        WHERE source_match.application_id = p_application_id
    )
    SELECT ARRAY(
        SELECT application_id
        FROM (
            SELECT p_application_id AS application_id
            UNION
            SELECT target_application.application_id
            FROM target_applications AS target_application
        ) AS affected
        ORDER BY application_id
    )
    INTO affected_applications;

    /*
     * Use the same bucket lock identity as the bidirectional upsert function.
     * PostgreSQL releases these locks when the caller commits or rolls back.
     */
    FOR locked_row_id IN
        WITH reverse_rows AS (
            SELECT DISTINCT
                target.target_global_id::INTEGER AS owner_global_id,
                tsp.titan_match_bucket(source_match.global_id) AS bucket_number
            FROM tsp.titan_matches AS source_match
            CROSS JOIN LATERAL JSONB_OBJECT_KEYS(source_match.matches)
                AS target(target_global_id)
            WHERE source_match.application_id = p_application_id
              AND target.target_global_id::INTEGER <> ALL(source_global_ids)
        ),
        rows_to_lock AS (
            /* Lock all buckets that can be owned by a deleted sentence. */
            SELECT
                source_global_id AS owner_global_id,
                bucket_number::SMALLINT AS bucket_number
            FROM UNNEST(source_global_ids) AS source(source_global_id)
            CROSS JOIN GENERATE_SERIES(0, 15) AS bucket(bucket_number)

            UNION

            /* Lock target buckets that contain the reverse relationships. */
            SELECT owner_global_id, bucket_number
            FROM reverse_rows
        )
        SELECT DISTINCT
            HASHTEXTEXTENDED(
                CONCAT_WS(
                    ':',
                    'titan_matches',
                    rows_to_lock.owner_global_id,
                    rows_to_lock.bucket_number
                ),
                0
            ) AS lock_id
        FROM rows_to_lock
        ORDER BY lock_id
    LOOP
        PERFORM PG_ADVISORY_XACT_LOCK(locked_row_id);
    END LOOP;

    /*
     * Remove source keys from the matching rows owned by other applications.
     *
     * Before: owner 200 has {"100": 0.0, "300": 0.12}
     * Delete: application 10 owns global ID 100
     * After:  owner 200 has {"300": 0.12}
     */
    WITH reverse_removals AS (
        SELECT
            target.target_global_id::INTEGER AS owner_global_id,
            tsp.titan_match_bucket(source_match.global_id) AS bucket_number,
            ARRAY_AGG(
                DISTINCT source_match.global_id::TEXT
                ORDER BY source_match.global_id::TEXT
            ) AS removed_keys
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL JSONB_OBJECT_KEYS(source_match.matches)
            AS target(target_global_id)
        WHERE source_match.application_id = p_application_id
          AND target.target_global_id::INTEGER <> ALL(source_global_ids)
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

    /* Delete only the reverse bucket rows touched above when they are empty. */
    WITH touched_reverse_rows AS (
        SELECT DISTINCT
            target.target_global_id::INTEGER AS owner_global_id,
            tsp.titan_match_bucket(source_match.global_id) AS bucket_number
        FROM tsp.titan_matches AS source_match
        CROSS JOIN LATERAL JSONB_OBJECT_KEYS(source_match.matches)
            AS target(target_global_id)
        WHERE source_match.application_id = p_application_id
          AND target.target_global_id::INTEGER <> ALL(source_global_ids)
    )
    DELETE FROM tsp.titan_matches AS empty_reverse_match
    USING touched_reverse_rows
    WHERE empty_reverse_match.global_id = touched_reverse_rows.owner_global_id
      AND empty_reverse_match.match_bucket_number = touched_reverse_rows.bucket_number
      AND empty_reverse_match.matches = '{}'::JSONB;

    GET DIAGNOSTICS deleted_empty_reverse_count = ROW_COUNT;

    /* Delete every bucket row owned by the requested application. */
    DELETE FROM tsp.titan_matches AS source_match
    WHERE source_match.application_id = p_application_id;

    GET DIAGNOSTICS deleted_source_count = ROW_COUNT;

    RETURN QUERY
    SELECT
        affected_applications,
        source_id_count,
        deleted_source_count,
        updated_reverse_count,
        deleted_empty_reverse_count;
END;
$$;

COMMENT ON FUNCTION tsp.delete_titan_matches_for_application(INTEGER)
IS 'Deletes one application from Titan match buckets and removes its reverse sentence relationships.';
```

## 2. Show every application matched to one application

Change `10` to the source `application_id`. Run this query before deletion and
save the result. Run it again after Sentence Similarity rebuilds the source
application; the two result sets can then be compared.

`relationship_count` counts unique source-to-target sentence pairs. The three
case columns follow the Case Similarity rules: at least one relationship is a
similar application, at least two is a similar case, and at least three is a
TAH case.

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
sentence_relationships AS (
    SELECT DISTINCT
        source_match.application_id AS source_application_id,
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        target_owner.application_id AS target_application_id,
        target.target_global_id::INTEGER AS target_global_id,
        target_owner.tsp_id AS target_tsp_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM parameters
    JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = parameters.source_application_id
    CROSS JOIN LATERAL JSONB_EACH_TEXT(source_match.matches)
        AS target(target_global_id, distance)
    CROSS JOIN LATERAL (
        SELECT candidate.application_id, candidate.tsp_id
        FROM tsp.titan_matches AS candidate
        WHERE candidate.global_id = target.target_global_id::INTEGER
        ORDER BY candidate.match_bucket_number
        LIMIT 1
    ) AS target_owner
    WHERE target_owner.application_id <> source_match.application_id
)
SELECT
    relationship.source_application_id,
    relationship.target_application_id,
    COUNT(*)::INTEGER AS relationship_count,
    COUNT(DISTINCT relationship.source_global_id)::INTEGER
        AS source_sentence_count,
    COUNT(DISTINCT relationship.target_global_id)::INTEGER
        AS target_sentence_count,
    COUNT(*) FILTER (WHERE relationship.distance = 0.0)::INTEGER
        AS exact_relationship_count,
    COUNT(*) FILTER (WHERE relationship.distance <> 0.0)::INTEGER
        AS similar_relationship_count,
    ARRAY_AGG(DISTINCT relationship.source_tsp_id ORDER BY relationship.source_tsp_id)
        AS source_tsp_ids,
    ARRAY_AGG(DISTINCT relationship.target_tsp_id ORDER BY relationship.target_tsp_id)
        AS target_tsp_ids,
    TRUE AS is_similar_application,
    COUNT(*) >= 2 AS is_similar_case,
    COUNT(*) >= 3 AS is_tah_case
FROM sentence_relationships AS relationship
GROUP BY
    relationship.source_application_id,
    relationship.target_application_id
ORDER BY
    relationship_count DESC,
    relationship.target_application_id;
```

Example output:

| source_application_id | target_application_id | relationship_count | source_sentence_count | target_sentence_count | is_similar_application | is_similar_case | is_tah_case |
|---:|---:|---:|---:|---:|:---:|:---:|:---:|
| 10 | 20 | 5 | 3 | 4 | true | true | true |
| 10 | 30 | 2 | 2 | 2 | true | true | false |
| 10 | 40 | 1 | 1 | 1 | true | false | false |

## 3. Delete the application relationships

Change `10` to the application being tested.

```sql
BEGIN;

SELECT *
FROM tsp.delete_titan_matches_for_application(10);

COMMIT;
```

Example function result:

| affected_application_ids | source_global_id_count | deleted_source_bucket_count | updated_reverse_bucket_count | deleted_empty_reverse_bucket_count |
|---|---:|---:|---:|---:|
| {10,20,30,40} | 12 | 30 | 18 | 7 |

The counts are bucket-row counts, except `source_global_id_count`. One sentence
can own more than one bucket, so deleting 12 source global IDs may delete more
than 12 source bucket rows.

## 4. Confirm that both directions were removed

Change `10` below. The expected result is `0` for both counts.

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS deleted_application_id
),
application_global_ids AS (
    SELECT sentence.global_id
    FROM parameters
    JOIN tsp.text_similarity_input AS input
        ON input.application_id = parameters.deleted_application_id
    JOIN tsp.sentences_vector AS sentence
        ON sentence.tsp_id = input.tsp_id
)
SELECT
    (
        SELECT COUNT(*)
        FROM tsp.titan_matches AS source_match
        WHERE source_match.application_id = parameters.deleted_application_id
    ) AS remaining_source_bucket_count,
    (
        SELECT COUNT(*)
        FROM tsp.titan_matches AS reverse_match
        CROSS JOIN LATERAL JSONB_OBJECT_KEYS(reverse_match.matches)
            AS target(target_global_id)
        WHERE target.target_global_id::INTEGER IN (
            SELECT global_id
            FROM application_global_ids
        )
    ) AS remaining_reverse_relationship_count
FROM parameters;
```

## 5. Compare the rebuilt result

After deletion:

1. Reprocess every `tsp_id` belonging to the application through Sentence
   Similarity so `tsp.titan_matches` is rebuilt.
2. Reprocess the application through Case Similarity if the
   `titan_similar_cases` summary also needs to be refreshed.
3. Run the query from section 2 again and compare it with the saved result.

The application-level rows should be the same when the source sentences,
catalog data, thresholds, and OpenSearch results have not changed.
