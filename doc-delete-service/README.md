# Document Delete OpenSearch Job

This job removes sentence data for a list of `tsp_id` values supplied by the
calling service. It does not query `tsp.text_similarity_input` for work or
check its current status. After enabled database cleanup succeeds, it updates
the retained record to `DOC_DELETED`.

The existing batch job remains the entry point. Set
`OPENSEARCH_FLOW_ENABLED=true` to run the existing deletion flow and the new
OpenSearch deletion flow in separate threads for the same batch. The job waits
for both flows to finish before it loads the next batch. When the toggle is
false, the existing deletion flow runs unchanged.

## Processing flow

1. Receive a list such as `[1040, 1041]` from the calling service.
2. Process each `tsp_id` in the received order.
3. Read every matching record from `sentence_occurrences`.
4. Use parallel multi-search requests, with at most 30 workers, to check each
   sentence key against other `tsp_id` values in the same analysis group.
5. Delete a catalog vector only when no other document uses that key. The
   Asylee and Refugee catalogs are handled independently.
6. Delete all occurrence records for the `tsp_id` with one delete-by-query,
   only after catalog cleanup succeeds. This keeps occurrence records available
   when a later job must retry.
7. Always call `tsp.delete_titan_matches_for_tsp`. It deletes source match
   buckets, removes reverse JSONB references, and deletes reverse buckets that
   become empty. It returns the affected application IDs, affected document
   IDs, and whether the requested document existed in `tsp.titan_matches`. If
   the document is absent, the function returns immediately without scanning
   or changing match rows. This table belongs to the new OpenSearch flow.
8. When `DATABASE_DELETE_ENABLED=true`, read the document's global IDs from
   `tsp.sentences_vector`, then call
   `tsp.embeddings`, delete the document rows from `tsp.sentences_vector`, and
   update `tsp.text_similarity_input.document_state` to `DOC_DELETED`. These
   database changes remain behind the rollout toggle.
9. After every enabled deletion step succeeds, send the completed `tsp_id` to
   the Case Similarity Backfill queue so its application summary can refresh.
10. Continue until every supplied ID is processed.

The job is idempotent. If one document fails with an expected processing,
data, or outbound-queue error, the job records its `tsp_id` and continues with
the remaining list. One final error log contains every failed ID so the calling
service can submit those documents again later. Unexpected programming errors
still stop the run.

`DocumentDeleteHandler` keeps OpenSearch, Titan matches, and legacy database
deletion in separate methods. OpenSearch and Titan cleanup always run.
`tsp.embeddings` and `tsp.sentences_vector` cleanup run only when
`DATABASE_DELETE_ENABLED=true`.

Each PostgreSQL table has its own store file:

- `titan_matches_db_store.py` runs the bidirectional match deletion function.
- `embeddings_db_store.py` deletes embeddings by global ID.
- `sentence_vector_db_store.py` reads and deletes sentence rows by `tsp_id`.

## PostgreSQL migration

Run `DELETE_TITAN_MATCHES.sql` through Flyway before deploying this worker. The
function uses the same 16-bucket advisory-lock identity as the Titan upsert
function, so matching and deletion cannot overwrite one another.

The function expects these existing objects:

- `tsp.titan_matches`
- `tsp.titan_match_bucket(integer)`

The Python cleanup removes `tsp.sentences_vector` after the function removes
the Titan match relationships.

## Local configuration

Copy `.env.example` to `.env` and `example.localVaultSecret.yaml` to
`localVaultSecret.yaml`, then provide the local database and OpenSearch values.

```bash
cp .env.example .env
cp example.localVaultSecret.yaml localVaultSecret.yaml
make localstack
```

Call `run_document_delete_job([1040, 1041])` from the service that supplies the
document list. No `text_similarity_input` status is required.

Configure the required destination with:

```text
CASE_SIMILARITY_BACKFILL_QUEUE_URL=<case-similarity-backfill-queue-url>
```

## Tests

```bash
make ostest
```
