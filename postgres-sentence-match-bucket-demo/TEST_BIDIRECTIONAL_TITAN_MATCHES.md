# Test the Bidirectional Titan Matches Function

Use this SQL to test `upsert_bidirectional_titan_matches` from a SQL editor.

The test uses `BEGIN` and `ROLLBACK`, so the test rows are not permanently
saved.

## Test a New Bidirectional Match

```sql
BEGIN;

-- Sentence 900001 matches:
-- 900010 exactly because its distance is 0.0.
-- 900040 similarly because its distance is 0.12.
SELECT upsert_bidirectional_titan_matches(
    900001, -- Source global_id
    1001,   -- Source tsp_id
    123456, -- Source a_number
    '[
        {
            "global_id": 900010,
            "tsp_id": 1010,
            "a_number": 654321,
            "distance": 0.0
        },
        {
            "global_id": 900040,
            "tsp_id": 1040,
            "a_number": 777777,
            "distance": 0.12
        }
    ]'::JSONB
);

-- View the source row and the two reverse rows.
SELECT
    global_id,
    tsp_id,
    matches,
    match_count,
    verbatim,
    last_modified
FROM titan_matches
WHERE global_id IN (900001, 900010, 900040)
ORDER BY global_id;
```

Expected result:

| global_id | match_count | Matches contain | verbatim |
|---:|---:|---|---|
| 900001 | 2 | 900010 and 900040 | `{900010}` |
| 900010 | 1 | 900001 | `{900001}` |
| 900040 | 1 | 900001 | `{}` |

The function automatically saves both directions:

```text
900001 -> 900010
900010 -> 900001

900001 -> 900040
900040 -> 900001
```

## Test Updating a Distance

Run this before the final `ROLLBACK`.

```sql
-- Change the relationship between 900001 and 900040 from a distance
-- of 0.12 to an exact-match distance of 0.0.
SELECT upsert_bidirectional_titan_matches(
    900001,
    1001,
    123456,
    '[
        {
            "global_id": 900040,
            "tsp_id": 1040,
            "a_number": 777777,
            "distance": 0.0
        }
    ]'::JSONB
);

SELECT
    global_id,
    matches,
    match_count,
    verbatim
FROM titan_matches
WHERE global_id IN (900001, 900040)
ORDER BY global_id;
```

Both directions should now contain distance `0.0`. The `verbatim` values
should be:

```text
900001 -> {900010,900040}
900040 -> {900001}
```

## Test an Empty Match List

An empty list still creates the source row.

```sql
SELECT upsert_bidirectional_titan_matches(
    900099,
    1099,
    999999,
    '[]'::JSONB
);

SELECT
    global_id,
    tsp_id,
    matches,
    match_count,
    verbatim
FROM titan_matches
WHERE global_id = 900099;
```

Expected values:

```text
matches: []
match_count: 0
verbatim: {}
```

## Remove All Test Changes

Run this after checking the results:

```sql
ROLLBACK;
```

## If the Objects Use the tsp Schema

If the function and table were created under the `tsp` schema, include the
schema name:

```sql
SELECT tsp.upsert_bidirectional_titan_matches(
    900001,
    1001,
    123456,
    '[
        {
            "global_id": 900010,
            "tsp_id": 1010,
            "a_number": 654321,
            "distance": 0.0
        }
    ]'::JSONB
);

SELECT *
FROM tsp.titan_matches
WHERE global_id IN (900001, 900010)
ORDER BY global_id;
```
