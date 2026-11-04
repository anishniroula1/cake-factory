# Sentence Similarity Flow Check

## Verified processing flow

| Step | Current behavior | Status |
|---|---|---|
| 1. Check whether OpenSearch is ready | The service checks whether at least one sentence occurrence exists for the received `tsp_id`. If none is found, it waits 30 seconds and checks one more time. | Implemented. This checks the occurrence index, not every OpenSearch index. |
| 2. Load source sentences | The service counts and loads `sentences_vector` records for the `tsp_id` in ordered batches. Sentences are processed through a continuously replenished worker pool. | Implemented. |
| 3. Load catalog vectors and classification flags | Source sentence keys are used to load their vectors, word counts, `isNonMatcher`, `isTracer`, and `isFormLanguage` from the correct Asylee or Refugee sentence catalog. Processing stops when a required source catalog record or vector is missing. | Implemented. |
| 4. Search the tracer catalog when classification is unavailable | The vector is searched against the closest 100 tracer references. Every result at or above the 70% threshold contributes to the final non-matcher, tracer, and form-language flags. | Implemented. Any complete saved classification is reused. The tracer catalog is searched only when the classification fields are missing. |
| 5. Save the classification | A newly calculated classification is written to the sentence catalog. The source `sentences_vector` row is also updated by `global_id`. | Implemented. Catalog is updated only when tracer search is performed; `sentences_vector` is updated for each processed source. |
| 6. Decide whether the sentence may be matched | A sentence is excluded from the main catalog search when it is a non-matcher, tracer, form language, or contains fewer than five words. | Implemented. |
| 7. Search the sentence catalog | A source sentence reaches this step only when its three classification flags are false and it contains at least five words. It uses its vector to search the correct analysis-group catalog and returns every sentence key meeting the configured similarity threshold and word-count rule. | Implemented. |
| 8. Expand sentence keys through the occurrence index | Matching sentence keys are searched in the occurrence index to obtain target `global_id`, `tsp_id`, and `application_id`. Results stay in the same analysis group and exclude the source application. | Implemented. |
| 9. Prepare bidirectional relationships | Eligible source and target data are collected as source global ID, source document and application IDs, target global ID, target document and application IDs, and distance. Duplicate A-to-B and B-to-A inputs are collapsed into one relationship before writing. | Implemented. |
| 10. Save `titan_matches` | All relationships are saved after the search workers finish. They are grouped by source global ID and sent sequentially to `tsp.upsert_bidirectional_titan_matches`, with at most 3000 targets per call. Each call commits separately, and PostgreSQL creates both directions without removing existing JSONB keys. | Implemented without database-write threading. |
| 11. Continue the workflow | The next SQS message is sent only after all source sentences finish and the Titan match writes succeed. | Implemented. |

## Important result counts

The sentence-catalog log reports the number of matching **sentence keys**, not the number of final occurrence relationships. The source sentence key is also included with distance `0.0`. A search can therefore report one key while producing zero target relationships when that key has no occurrence outside the source application.

The final saved count is determined after source classification, sentence-catalog search, occurrence expansion, source-application exclusion, and duplicate removal.

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

