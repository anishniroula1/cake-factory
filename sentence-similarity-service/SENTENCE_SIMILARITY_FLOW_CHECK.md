# Sentence Similarity Flow Check

## Verified processing flow

| Step | Current behavior | Status |
|---|---|---|
| 1. Check whether OpenSearch is ready | The service checks whether at least one sentence occurrence exists for the received `tsp_id`. If none is found, it waits 30 seconds and checks one more time. | Implemented. This checks the occurrence index, not every OpenSearch index. |
| 2. Load and filter source sentences | PostgreSQL loads every row for the `tsp_id` in pages of 1000 before workers start. The handler reports short and classified rows, then keeps only rows whose `word_count` is at least five and whose three classification flags are false. | Implemented. Doc2Vec owns word counting and classification. |
| 3. Load catalog vectors | Source sentence keys load their vectors from the correct Asylee or Refugee catalog. Processing stops when a required source vector is missing. | Implemented. Sentence Similarity no longer writes classifications. |
| 4. Search the sentence catalog | Each source vector returns every qualifying sentence key at or above the configured threshold. The catalog query also excludes target keys that are short, non-matchers, tracers, or form language. | Implemented. |
| 5. Expand sentence keys through the occurrence index | Matching sentence keys are searched in the occurrence index to obtain target `global_id`, `tsp_id`, and `application_id`. Results stay in the same analysis group and exclude the source application. | Implemented. |
| 6. Prepare bidirectional relationships | Source and target identifiers and distance are collected in memory. Repeated source-to-target results keep the smaller distance. | Implemented. |
| 7. Save `titan_matches` | All relationships are saved after the search workers finish. They are grouped by source global ID and sent sequentially to `tsp.upsert_bidirectional_titan_matches`, with at most 3000 targets per call. Each call commits separately, and PostgreSQL creates both directions without removing existing JSONB keys. | Implemented without database-write threading. |
| 8. Continue the workflow | The next SQS message is sent only after all eligible source sentences finish and the Titan match writes succeed. A document with zero eligible sentences also continues with zero matches. | Implemented. |

## Important result counts

The sentence-catalog log reports the number of matching **sentence keys**, not the number of final occurrence relationships. The source sentence key is also included with distance `0.0`. A search can therefore report one key while producing zero target relationships when that key has no occurrence outside the source application.

The final saved count is determined after sentence-catalog search, occurrence
expansion, source-application exclusion, and duplicate removal.

## PostgreSQL activity query

Run this query while `titan_matches` is being written. It shows active PostgreSQL work and whether a query is waiting on a lock, I/O, or another database event.

```sql
SELECT
    pid,
    NOW() - query_start AS duration,
    wait_event_type,
    wait_event,
    state,
    LEFT(query, 150) AS query
FROM pg_stat_activity
WHERE state <> 'idle'
ORDER BY query_start;
```

Common results:

- `wait_event_type = 'Lock'` means the query is waiting for another transaction or Titan bucket writer.
- `wait_event = 'transactionid'` means another transaction must finish before this query can continue.
- `wait_event_type = 'IO'` means PostgreSQL is waiting for storage, table pages, TOAST data, or WAL writes. WAL is the write-ahead log PostgreSQL records before committing data.
- A long-running active query with no wait event is normally doing CPU work such as JSONB parsing, grouping, bucket calculation, or merging.

