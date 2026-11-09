# Delete and compare Titan matches by application

These are normal SQL queries that can be copied into pgAdmin. They do not
create a stored function.

The first query shows every application matched to one source application. The
second query deletes that application's `tsp.titan_matches` rows and removes
the reverse relationships from the other applications.

## 1. Show every application matched to the source application

Change `10` to the source `application_id`. Save this result before deleting
anything. After rebuilding the matches, run the same query and compare the new
result with the saved result.

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
        /* One sentence can own several buckets. One row is enough here. */
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
    ARRAY_AGG(
        DISTINCT relationship.source_tsp_id
        ORDER BY relationship.source_tsp_id
    ) AS source_tsp_ids,
    ARRAY_AGG(
        DISTINCT relationship.target_tsp_id
        ORDER BY relationship.target_tsp_id
    ) AS target_tsp_ids,
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

The Case Similarity rules are:

- One or more sentence relationships: similar application.
- Two or more sentence relationships: similar case.
- Three or more sentence relationships: TAH case.

## 2. Delete all relationships for one application

Change `10` to the `application_id`, then run the complete script in pgAdmin.

This script:

1. Finds every source `global_id` owned by the application.
2. Finds the reverse bucket and JSONB key stored by each target sentence.
3. Locks the affected buckets in a consistent order.
4. Removes the source keys from the target applications.
5. Deletes reverse buckets that became empty.
6. Deletes every source bucket owned by the application.
7. Confirms that both directions are gone.

```sql
BEGIN;

/*
 * Keep the application ID in one temporary row so it is changed in one place.
 * Replace 10 with the application being tested.
 */
CREATE TEMP TABLE delete_application_parameter
ON COMMIT DROP
AS
SELECT 10::INTEGER AS application_id;

/*
 * Save the source global IDs before deleting anything.
 *
 * Example: application 10 owns global IDs 100 and 101, so this temporary
 * table contains two rows: 100 and 101.
 */
CREATE TEMP TABLE delete_application_source_ids
ON COMMIT DROP
AS
SELECT DISTINCT source_match.global_id
FROM tsp.titan_matches AS source_match
JOIN delete_application_parameter AS parameter
    ON parameter.application_id = source_match.application_id;

/*
 * Prepare the keys that must be removed from the reverse rows.
 *
 * Example: global ID 100 matches global ID 200. The reverse relationship is
 * stored under owner 200 in bucket titan_match_bucket(100). The value
 * removed_keys contains {"100"}.
 */
CREATE TEMP TABLE delete_application_reverse_keys
ON COMMIT DROP
AS
SELECT
    target.target_global_id::INTEGER AS owner_global_id,
    tsp.titan_match_bucket(source_match.global_id) AS match_bucket_number,
    ARRAY_AGG(
        DISTINCT source_match.global_id::TEXT
        ORDER BY source_match.global_id::TEXT
    ) AS removed_keys
FROM tsp.titan_matches AS source_match
JOIN delete_application_parameter AS parameter
    ON parameter.application_id = source_match.application_id
CROSS JOIN LATERAL JSONB_OBJECT_KEYS(source_match.matches)
    AS target(target_global_id)
WHERE NOT EXISTS (
    /* Same-application rows will be deleted completely in the final delete. */
    SELECT 1
    FROM delete_application_source_ids AS source_id
    WHERE source_id.global_id = target.target_global_id::INTEGER
)
GROUP BY
    target.target_global_id::INTEGER,
    tsp.titan_match_bucket(source_match.global_id);

/*
 * Take the same advisory bucket locks used by the bidirectional upsert.
 * PostgreSQL releases these locks when this transaction commits or rolls back.
 */
SELECT PG_ADVISORY_XACT_LOCK(affected.lock_id)
FROM (
    SELECT DISTINCT
        HASHTEXTEXTENDED(
            CONCAT_WS(
                ':',
                'titan_matches',
                bucket_identity.owner_global_id,
                bucket_identity.match_bucket_number
            ),
            0
        ) AS lock_id
    FROM (
        /* Lock all 16 possible buckets owned by each source sentence. */
        SELECT
            source_id.global_id AS owner_global_id,
            bucket_number::SMALLINT AS match_bucket_number
        FROM delete_application_source_ids AS source_id
        CROSS JOIN GENERATE_SERIES(0, 15) AS bucket(bucket_number)

        UNION

        /* Lock every reverse bucket that will be changed. */
        SELECT owner_global_id, match_bucket_number
        FROM delete_application_reverse_keys
    ) AS bucket_identity
) AS affected
ORDER BY affected.lock_id;

/*
 * Remove the application global IDs from the other applications' JSONB.
 *
 * Before: owner 200 has {"100": 0.0, "300": 0.12}
 * Delete: application 10 owns global ID 100
 * After:  owner 200 has {"300": 0.12}
 */
UPDATE tsp.titan_matches AS reverse_match
SET
    matches = reverse_match.matches - reverse_key.removed_keys,
    last_modified = NOW()
FROM delete_application_reverse_keys AS reverse_key
WHERE reverse_match.global_id = reverse_key.owner_global_id
  AND reverse_match.match_bucket_number = reverse_key.match_bucket_number;

/* Remove only affected reverse bucket rows that are now empty. */
DELETE FROM tsp.titan_matches AS empty_reverse_match
USING delete_application_reverse_keys AS reverse_key
WHERE empty_reverse_match.global_id = reverse_key.owner_global_id
  AND empty_reverse_match.match_bucket_number = reverse_key.match_bucket_number
  AND empty_reverse_match.matches = '{}'::JSONB;

/* Delete all bucket rows owned by the source application. */
DELETE FROM tsp.titan_matches AS source_match
USING delete_application_parameter AS parameter
WHERE source_match.application_id = parameter.application_id;

/*
 * Both values must be zero. This check uses the saved source global IDs, so it
 * can still find an incorrect reverse reference after source rows are deleted.
 */
SELECT
    (
        SELECT COUNT(*)
        FROM tsp.titan_matches AS source_match
        JOIN delete_application_parameter AS parameter
            ON parameter.application_id = source_match.application_id
    ) AS remaining_source_bucket_count,
    (
        SELECT COUNT(*)
        FROM tsp.titan_matches AS reverse_match
        CROSS JOIN LATERAL JSONB_OBJECT_KEYS(reverse_match.matches)
            AS target(target_global_id)
        JOIN delete_application_source_ids AS source_id
            ON source_id.global_id = target.target_global_id::INTEGER
    ) AS remaining_reverse_relationship_count;

COMMIT;
```

Expected verification output:

| remaining_source_bucket_count | remaining_reverse_relationship_count |
|---:|---:|
| 0 | 0 |

This deletes only `tsp.titan_matches`. It does not delete rows from
`sentences_vector`, `text_similarity_input`, or `titan_similar_cases`.

## 3. Rebuild and compare the result

1. Reprocess every `tsp_id` belonging to the application through Sentence
   Similarity. This rebuilds its source and reverse `titan_matches` rows.
2. Reprocess the application through Case Similarity if its
   `titan_similar_cases` summary also needs to be refreshed.
3. Run the query from section 1 again.
4. Compare the new output with the result saved before deletion.

The results should be the same when the sentences, OpenSearch indexes,
thresholds, and classification values have not changed.
