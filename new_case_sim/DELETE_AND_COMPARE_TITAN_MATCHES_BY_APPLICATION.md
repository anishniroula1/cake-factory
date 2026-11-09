# Delete and compare Titan case summaries by application

These are normal SQL queries for pgAdmin. They do not create a stored
function, and they do not change `tsp.titan_matches`.

The first query shows every application saved in one source application's
summary. The second query deletes that source row from
`tsp.titan_similar_cases` and removes its reverse references from the other
application summaries.

## 1. Show all saved matches for one application

Change `10` to the source `application_id`. Save this output before deleting
the summary. After Case Similarity rebuilds it, run the query again and compare
the two results.

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
source_summary AS (
    SELECT summary.*
    FROM tsp.titan_similar_cases AS summary
    JOIN parameters
        ON parameters.source_application_id = summary.application_id
),
target_application_ids AS (
    /* Read all three lists in case an older row has inconsistent subsets. */
    SELECT saved.application_id::INTEGER AS target_application_id
    FROM source_summary
    CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(
        source_summary.similar_application_ids
    ) AS saved(application_id)

    UNION

    SELECT saved.application_id::INTEGER
    FROM source_summary
    CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(
        source_summary.similar_case_ids
    ) AS saved(application_id)

    UNION

    SELECT saved.application_id::INTEGER
    FROM source_summary
    CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(
        source_summary.tah_case_ids
    ) AS saved(application_id)
)
SELECT
    parameters.source_application_id,
    target.target_application_id,
    source_summary.similar_application_ids
        @> JSONB_BUILD_ARRAY(target.target_application_id)
        AS is_similar_application,
    source_summary.similar_case_ids
        @> JSONB_BUILD_ARRAY(target.target_application_id)
        AS is_similar_case,
    source_summary.tah_case_ids
        @> JSONB_BUILD_ARRAY(target.target_application_id)
        AS is_tah_case,
    COALESCE(
        reverse_summary.similar_application_ids
            @> JSONB_BUILD_ARRAY(parameters.source_application_id),
        FALSE
    ) AS reverse_similar_application_saved,
    COALESCE(
        reverse_summary.similar_case_ids
            @> JSONB_BUILD_ARRAY(parameters.source_application_id),
        FALSE
    ) AS reverse_similar_case_saved,
    COALESCE(
        reverse_summary.tah_case_ids
            @> JSONB_BUILD_ARRAY(parameters.source_application_id),
        FALSE
    ) AS reverse_tah_case_saved
FROM parameters
JOIN source_summary ON TRUE
JOIN target_application_ids AS target ON TRUE
LEFT JOIN tsp.titan_similar_cases AS reverse_summary
    ON reverse_summary.application_id = target.target_application_id
ORDER BY target.target_application_id;
```

Example output:

| source_application_id | target_application_id | is_similar_application | is_similar_case | is_tah_case | reverse_similar_application_saved | reverse_similar_case_saved | reverse_tah_case_saved |
|---:|---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 10 | 20 | true | false | false | true | false | false |
| 10 | 30 | true | true | false | true | true | false |
| 10 | 40 | true | true | true | true | true | true |

The first three booleans show the source lists. The last three show whether the
same category was saved in the reverse direction.

## 2. Delete the source summary and its reverse references

Change `10` to the `application_id`, then run the complete script in pgAdmin.
This script changes only `tsp.titan_similar_cases`.

```sql
BEGIN;

/* Replace 10 with the application being tested. */
CREATE TEMP TABLE delete_case_summary_parameter
ON COMMIT DROP
AS
SELECT 10::INTEGER AS application_id;

/* Save only the reverse rows that contain this application ID. */
CREATE TEMP TABLE delete_case_summary_reverse_ids
ON COMMIT DROP
AS
SELECT summary.application_id
FROM tsp.titan_similar_cases AS summary
CROSS JOIN delete_case_summary_parameter AS parameter
WHERE summary.application_id <> parameter.application_id
  AND (
      summary.similar_application_ids
          @> JSONB_BUILD_ARRAY(parameter.application_id)
      OR summary.similar_case_ids
          @> JSONB_BUILD_ARRAY(parameter.application_id)
      OR summary.tah_case_ids
          @> JSONB_BUILD_ARRAY(parameter.application_id)
  );

/*
 * Lock the source row and every row containing its reverse relationship.
 * Sorting by application_id gives concurrent transactions a consistent order.
 */
SELECT summary.application_id
FROM tsp.titan_similar_cases AS summary
CROSS JOIN delete_case_summary_parameter AS parameter
WHERE summary.application_id = parameter.application_id
   OR summary.application_id IN (
       SELECT reverse_id.application_id
       FROM delete_case_summary_reverse_ids AS reverse_id
   )
ORDER BY summary.application_id
FOR UPDATE OF summary;

/*
 * Remove the source application ID from all reverse lists.
 *
 * Example before deleting application 10:
 *   application 20 has similar_application_ids = [10, 30]
 *
 * Example afterward:
 *   application 20 has similar_application_ids = [30]
 *
 * Application 20's unrelated relationship with application 30 stays saved.
 */
UPDATE tsp.titan_similar_cases AS reverse_summary
SET
    similar_application_ids = (
        SELECT COALESCE(
            JSONB_AGG(
                item.application_id
                ORDER BY (item.application_id #>> '{}')::INTEGER
            ),
            '[]'::JSONB
        )
        FROM JSONB_ARRAY_ELEMENTS(reverse_summary.similar_application_ids)
            AS item(application_id)
        WHERE item.application_id
            <> TO_JSONB(parameter.application_id)
    ),
    similar_case_ids = (
        SELECT COALESCE(
            JSONB_AGG(
                item.application_id
                ORDER BY (item.application_id #>> '{}')::INTEGER
            ),
            '[]'::JSONB
        )
        FROM JSONB_ARRAY_ELEMENTS(reverse_summary.similar_case_ids)
            AS item(application_id)
        WHERE item.application_id
            <> TO_JSONB(parameter.application_id)
    ),
    tah_case_ids = (
        SELECT COALESCE(
            JSONB_AGG(
                item.application_id
                ORDER BY (item.application_id #>> '{}')::INTEGER
            ),
            '[]'::JSONB
        )
        FROM JSONB_ARRAY_ELEMENTS(reverse_summary.tah_case_ids)
            AS item(application_id)
        WHERE item.application_id
            <> TO_JSONB(parameter.application_id)
    ),
    last_modified = NOW()
FROM
    delete_case_summary_parameter AS parameter,
    delete_case_summary_reverse_ids AS reverse_id
WHERE reverse_summary.application_id = reverse_id.application_id;

/*
 * A summary with no remaining relationships is no longer needed.
 * Keep rows that still match any unrelated application.
 */
DELETE FROM tsp.titan_similar_cases AS empty_reverse_summary
USING delete_case_summary_reverse_ids AS reverse_id
WHERE empty_reverse_summary.application_id = reverse_id.application_id
  AND empty_reverse_summary.similar_application_ids = '[]'::JSONB
  AND empty_reverse_summary.similar_case_ids = '[]'::JSONB
  AND empty_reverse_summary.tah_case_ids = '[]'::JSONB;

/* Delete the source application's own summary row. */
DELETE FROM tsp.titan_similar_cases AS source_summary
USING delete_case_summary_parameter AS parameter
WHERE source_summary.application_id = parameter.application_id;

/* Both values must be zero before the transaction is committed. */
SELECT
    (
        SELECT COUNT(*)
        FROM tsp.titan_similar_cases AS source_summary
        JOIN delete_case_summary_parameter AS parameter
            ON parameter.application_id = source_summary.application_id
    ) AS remaining_source_summary_count,
    (
        SELECT COUNT(*)
        FROM tsp.titan_similar_cases AS reverse_summary
        CROSS JOIN delete_case_summary_parameter AS parameter
        WHERE reverse_summary.similar_application_ids
                  @> JSONB_BUILD_ARRAY(parameter.application_id)
           OR reverse_summary.similar_case_ids
                  @> JSONB_BUILD_ARRAY(parameter.application_id)
           OR reverse_summary.tah_case_ids
                  @> JSONB_BUILD_ARRAY(parameter.application_id)
    ) AS remaining_reverse_reference_count;

COMMIT;
```

Expected verification output:

| remaining_source_summary_count | remaining_reverse_reference_count |
|---:|---:|
| 0 | 0 |

The generated `similar_case_count`, `tah_case_count`, and `is_tah` columns are
automatically recalculated when their JSONB lists change.

## 3. Rebuild and compare

1. Keep `tsp.titan_matches` unchanged.
2. Send one of the application's `tsp_id` values to Case Similarity.
3. Case Similarity reads the existing Titan sentence matches and rebuilds the
   source and reverse rows in `tsp.titan_similar_cases`.
4. Run the query from section 1 again and compare it with the saved result.

The results should be the same because the underlying `tsp.titan_matches`
relationships were not deleted or changed.
