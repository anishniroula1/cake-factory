# Test Bidirectional Titan Matches

All test objects are under the `test_matches` schema.

## Apply the SQL

```bash
docker compose exec -T postgres psql \
  -U sentence_user \
  -d sentence_matches \
  < db/init/005_titan_key_match_buckets.sql
```

## Call the bucket function

```sql
SELECT test_matches.upsert_bidirectional_titan_key_match_buckets(
    100,
    10,
    1,
    '{
      "200": {
        "tsp_id": 20,
        "application_id": 2,
        "distance": 0.10
      },
      "232": {
        "tsp_id": 22,
        "application_id": 3,
        "distance": 0.0
      }
    }'::JSONB
);
```

The function stores the forward relationships under global ID `100` and the
reverse relationships under global IDs `200` and `232`. Only
`{global_id: distance}` is kept inside `matches`.

## Inspect one owner

```sql
SELECT
    global_id,
    match_bucket_number,
    matches,
    matches_count,
    verbatim
FROM test_matches.titan_key_match_buckets
WHERE global_id = 100
ORDER BY match_bucket_number;
```

Get the total across all buckets:

```sql
SELECT
    global_id,
    SUM(matches_count) AS total_matches
FROM test_matches.titan_key_match_buckets
WHERE global_id = 100
GROUP BY global_id;
```

Targets with the same `application_id` as the source are ignored. Calling the
function again with a new distance replaces the saved value in both directions.
