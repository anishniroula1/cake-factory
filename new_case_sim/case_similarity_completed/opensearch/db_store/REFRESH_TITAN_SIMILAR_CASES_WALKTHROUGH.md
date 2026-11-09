# Refresh Titan Similar Cases: Step-by-step Walkthrough

This guide shows what `tsp.refresh_titan_similar_cases` does without requiring
someone to understand the full function at once. Run each query separately in
the same pgAdmin Query Tool window.

This walkthrough follows the optimized function. It expands the source match
buckets once, checks the target analysis group without multiplying rows, and
updates only reverse applications whose 1/2/3 relationship level changed. The
function output therefore contains the source application plus changed reverse
applications, not every application that was checked.

## Start with a useful application

First, find an existing row that has values in all three relationship lists.
Using a populated row makes it easier to see how the function counts matches,
builds each list, and updates the reverse relationships.

```sql
SELECT
    application_id,
    analysis_group,
    similar_application_ids,
    similar_case_ids,
    tah_case_ids
FROM tsp.titan_similar_cases
WHERE jsonb_array_length(similar_application_ids) > 0
  AND jsonb_array_length(similar_case_ids) > 0
  AND jsonb_array_length(tah_case_ids) > 0
ORDER BY application_id
LIMIT 10;
```

Example output:

| application_id | analysis_group | similar_application_ids | similar_case_ids | tah_case_ids |
|---:|---|---|---|---|
| 10 | Asylee | `[20, 30, 40, 50]` | `[30, 40, 50]` | `[40, 50]` |

Choose one result and use its `application_id` and `analysis_group` in every
query below. For example, if the selected row has application ID 725 and group
`Asylee`, replace `10` with `725` and keep `Asylee` as the analysis group.

The examples use:

- Source application: `10`
- Analysis group: `Asylee`
- Similar-case limit: `2` matching sentence pairs
- TAH limit: `3` matching sentence pairs

Replace application `10` and `Asylee` with real lower-environment values.

## Expected example

Assume application 10 currently has:

| Target application | Matching sentence pairs | Result |
|---:|---:|---|
| 20 | 1 | Similar application |
| 30 | 2 | Similar application and similar case |
| 40 | 3 | Similar application, similar case, and TAH case |

The final source row should contain:

```text
similar_application_ids = [20, 30, 40]
similar_case_ids        = [30, 40]
tah_case_ids            = [40]
```

The relationship is bidirectional. Application 20 also stores application 10
in `similar_application_ids`, application 30 stores 10 in its first two lists,
and application 40 stores 10 in all three lists.

## 1. Check the source application's Titan match buckets

This shows the sentence rows and JSONB buckets used by the function.

```sql
SELECT
    global_id,
    tsp_id,
    application_id,
    match_bucket_number,
    matches
FROM tsp.titan_matches
WHERE application_id = 10
ORDER BY global_id, match_bucket_number;
```

Example output:

| global_id | tsp_id | application_id | match_bucket_number | matches |
|---:|---:|---:|---:|---|
| 1001 | 101 | 10 | 0 | `{"5001": 0.10, "5002": 0.20}` |
| 1002 | 101 | 10 | 0 | `{"5003": 0.12, "5004": 0.18}` |
| 1003 | 102 | 10 | 0 | `{"5005": 0.15, "5006": 0.20}` |

This means source sentence 1001 matches target sentences 5001 and 5002. The
number is cosine distance, so the smaller number is the closer match.

## 2. Expand JSONB into source and target sentence pairs

The function changes JSONB keys into normal SQL rows so they can be counted.

```sql
WITH source_sentence_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    WHERE source_match.application_id = 10
)
SELECT *
FROM source_sentence_pairs
ORDER BY source_global_id, target_global_id;
```

Example result:

| source_global_id | target_global_id |
|---:|---:|
| 1001 | 5001 |
| 1001 | 5002 |
| 1002 | 5003 |
| 1002 | 5004 |
| 1003 | 5005 |
| 1003 | 5006 |

`DISTINCT` prevents the same sentence pair from being counted twice.

## 3. Find the application that owns each target sentence

The target global ID tells us which sentence matched. This query finds which
application owns that target sentence and keeps only the requested analysis
group.

```sql
WITH source_sentence_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    WHERE source_match.application_id = 10
)
SELECT
    sentence_pair.source_global_id,
    sentence_pair.target_global_id,
    target_owner.application_id AS target_application_id
FROM source_sentence_pairs AS sentence_pair
JOIN LATERAL (
    SELECT target_match.application_id
    FROM tsp.titan_matches AS target_match
    WHERE target_match.global_id = sentence_pair.target_global_id
    ORDER BY target_match.match_bucket_number
    LIMIT 1
) AS target_owner ON TRUE
WHERE target_owner.application_id <> 10
  AND EXISTS (
      SELECT 1
      FROM tsp.text_similarity_input AS target_input
      WHERE target_input.application_id = target_owner.application_id
        AND target_input.analysis_group = 'Asylee'
  )
ORDER BY target_application_id, source_global_id, target_global_id;
```

Example output:

| source_global_id | target_global_id | target_application_id |
|---:|---:|---:|
| 1001 | 5001 | 20 |
| 1001 | 5002 | 30 |
| 1002 | 5003 | 30 |
| 1002 | 5004 | 40 |
| 1003 | 5005 | 40 |
| 1003 | 5006 | 40 |

The source application is excluded because an application must not match itself.
Only one target bucket row is read because every bucket owned by a global ID has
the same `application_id`. `EXISTS` checks the analysis group without repeating
the sentence pair for every document owned by the target application.

## 4. Count sentence pairs for each target application

This is the count used to decide which of the three lists receives the target.

```sql
WITH source_sentence_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    WHERE source_match.application_id = 10
)
SELECT
    target_owner.application_id AS target_application_id,
    COUNT(*)::INTEGER AS sentence_match_count
FROM source_sentence_pairs AS sentence_pair
JOIN LATERAL (
    SELECT target_match.application_id
    FROM tsp.titan_matches AS target_match
    WHERE target_match.global_id = sentence_pair.target_global_id
    ORDER BY target_match.match_bucket_number
    LIMIT 1
) AS target_owner ON TRUE
WHERE target_owner.application_id <> 10
  AND EXISTS (
      SELECT 1
      FROM tsp.text_similarity_input AS target_input
      WHERE target_input.application_id = target_owner.application_id
        AND target_input.analysis_group = 'Asylee'
  )
GROUP BY target_owner.application_id
ORDER BY sentence_match_count DESC, target_application_id;
```

Example output:

| target_application_id | sentence_match_count |
|---:|---:|
| 40 | 3 |
| 30 | 2 |
| 20 | 1 |

This output matches the expected example at the top of the guide.

## 5. Build the source application's three lists

This query applies the normal limits of 1, 2, and 3 sentence pairs.

```sql
WITH source_sentence_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        target.target_global_id::INTEGER AS target_global_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    WHERE source_match.application_id = 10
),
source_match_counts AS (
    SELECT
        target_owner.application_id AS target_application_id,
        COUNT(*)::INTEGER AS sentence_match_count
    FROM source_sentence_pairs AS sentence_pair
    JOIN LATERAL (
        SELECT target_match.application_id
        FROM tsp.titan_matches AS target_match
        WHERE target_match.global_id = sentence_pair.target_global_id
        ORDER BY target_match.match_bucket_number
        LIMIT 1
    ) AS target_owner ON TRUE
    WHERE target_owner.application_id <> 10
      AND EXISTS (
          SELECT 1
          FROM tsp.text_similarity_input AS target_input
          WHERE target_input.application_id = target_owner.application_id
            AND target_input.analysis_group = 'Asylee'
      )
    GROUP BY target_owner.application_id
)
SELECT
    COALESCE(
        jsonb_agg(target_application_id ORDER BY target_application_id)
            FILTER (WHERE sentence_match_count >= 1),
        '[]'::JSONB
    ) AS similar_application_ids,
    COALESCE(
        jsonb_agg(target_application_id ORDER BY target_application_id)
            FILTER (WHERE sentence_match_count >= 2),
        '[]'::JSONB
    ) AS similar_case_ids,
    COALESCE(
        jsonb_agg(target_application_id ORDER BY target_application_id)
            FILTER (WHERE sentence_match_count >= 3),
        '[]'::JSONB
    ) AS tah_case_ids
FROM source_match_counts;
```

Example output:

| similar_application_ids | similar_case_ids | tah_case_ids |
|---|---|---|
| `[20, 30, 40]` | `[30, 40]` | `[40]` |

## 6. Check what is currently saved on both sides

Run this before the function to see the old state. Add or remove IDs from the
`IN` list based on the count query above.

```sql
SELECT
    application_id,
    analysis_group,
    similar_application_ids,
    similar_case_ids,
    similar_case_count,
    tah_case_ids,
    tah_case_count,
    is_tah,
    last_modified
FROM tsp.titan_similar_cases
WHERE application_id IN (10, 20, 30, 40, 50)
ORDER BY application_id;
```

Example output before the refresh:

| application_id | similar_application_ids | similar_case_ids | similar_case_count | tah_case_ids | tah_case_count | is_tah |
|---:|---|---|---:|---|---:|---|
| 10 | `[20, 30, 40, 50]` | `[30, 40, 50]` | 3 | `[40, 50]` | 2 | true |
| 20 | `[10, 60]` | `[60]` | 1 | `[]` | 0 | false |
| 30 | `[10]` | `[10]` | 1 | `[]` | 0 | false |
| 40 | `[10]` | `[10]` | 1 | `[10]` | 1 | true |
| 50 | `[10, 60, 70]` | `[10, 60]` | 2 | `[10]` | 1 | true |

For bidirectional matching, application 10 must appear in the correct lists for
applications 20, 30, and 40. If application 50 no longer matches 10, both sides
must stop listing each other.

## 7. Preview relationships that will be removed

This compares the source application's saved list with its current Titan
matches. It shows stale application IDs that were saved before but no longer
have a current sentence match.

```sql
WITH current_target_applications AS (
    SELECT DISTINCT target_owner.application_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    JOIN LATERAL (
        SELECT target_match.application_id
        FROM tsp.titan_matches AS target_match
        WHERE target_match.global_id = target.target_global_id::INTEGER
        ORDER BY target_match.match_bucket_number
        LIMIT 1
    ) AS target_owner ON TRUE
    WHERE source_match.application_id = 10
      AND target_owner.application_id <> 10
      AND EXISTS (
          SELECT 1
          FROM tsp.text_similarity_input AS target_input
          WHERE target_input.application_id = target_owner.application_id
            AND target_input.analysis_group = 'Asylee'
      )
),
previous_target_applications AS (
    SELECT saved.application_id::INTEGER AS application_id
    FROM tsp.titan_similar_cases AS summary
    CROSS JOIN LATERAL jsonb_array_elements_text(
        summary.similar_application_ids
    ) AS saved(application_id)
    WHERE summary.application_id = 10

    UNION

    SELECT saved.application_id::INTEGER
    FROM tsp.titan_similar_cases AS summary
    CROSS JOIN LATERAL jsonb_array_elements_text(
        summary.similar_case_ids
    ) AS saved(application_id)
    WHERE summary.application_id = 10

    UNION

    SELECT saved.application_id::INTEGER
    FROM tsp.titan_similar_cases AS summary
    CROSS JOIN LATERAL jsonb_array_elements_text(
        summary.tah_case_ids
    ) AS saved(application_id)
    WHERE summary.application_id = 10
)
SELECT previous.application_id AS relationship_to_remove
FROM previous_target_applications AS previous
LEFT JOIN current_target_applications AS current
    ON current.application_id = previous.application_id
WHERE current.application_id IS NULL
ORDER BY previous.application_id;
```

Example output:

| relationship_to_remove |
|---:|
| 50 |

Application 50 is returned because it was previously saved but has no current
sentence match with application 10. The refresh removes 50 from application 10
and removes 10 from application 50. Application 50's relationships with 60 and
70 remain unchanged. All three previous lists are checked so an inconsistent
older row can also be repaired.

## 8. Safely run the complete function

Use a transaction in a lower environment. `ROLLBACK` returns the database to its
original state after the result has been inspected.

Run this complete block together:

```sql
BEGIN;

SELECT *
FROM tsp.refresh_titan_similar_cases(
    p_source_application_id => 10,
    p_analysis_group => 'Asylee',
    p_similar_case_threshold => 2,
    p_tah_case_threshold => 3
);

SELECT
    application_id,
    similar_application_ids,
    similar_case_ids,
    similar_case_count,
    tah_case_ids,
    tah_case_count,
    is_tah,
    last_modified
FROM tsp.titan_similar_cases
WHERE application_id IN (10, 20, 30, 40, 50)
ORDER BY application_id;

ROLLBACK;
```

Example function output before `ROLLBACK`:

| refreshed_application_id | refreshed_similar_case_count | refreshed_tah_case_count |
|---:|---:|---:|
| 10 | 2 | 1 |
| 50 | 1 | 0 |

The function returns application 10 because it is always the source. It returns
application 50 because its reverse relationship changed. Applications 20, 30,
and 40 are not returned because they already stored application 10 in the
correct lists; they were neither locked nor rewritten.

The function returns only the ID and counts used by the Python service. The
second `SELECT` shows the full saved lists for all five rows, including
unchanged applications, so the final table state can be checked before the
transaction is rolled back.

## 9. Save the result when the output is correct

After testing with `ROLLBACK`, run the function again and commit it:

```sql
BEGIN;

SELECT *
FROM tsp.refresh_titan_similar_cases(
    p_source_application_id => 10,
    p_analysis_group => 'Asylee',
    p_similar_case_threshold => 2,
    p_tah_case_threshold => 3
);

COMMIT;
```

The source and changed reverse rows should match the example function output
from the previous section. Unchanged target rows keep their existing values and
`last_modified` timestamp. Unlike `ROLLBACK`, `COMMIT` keeps the changes.

Do not run `COMMIT` while experimenting with sample IDs. Use it only after 
the source application and analysis group have been verified.
