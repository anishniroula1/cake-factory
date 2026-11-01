# Document Delete OpenSearch Job

This scheduled job removes sentence data for documents marked
`READY_FOR_DELETION`. It reads work directly from `tsp.text_similarity_input`
and keeps that table so the next state-machine step can continue the document.

## Processing flow

1. Load ordered `tsp_id` values whose document state is `READY_FOR_DELETION`.
2. Process the records in database pages of 100 documents.
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
   become empty. This table belongs to the new OpenSearch flow.
8. When `DATABASE_DELETE_ENABLED=true`, read the document's global IDs from
   `tsp.sentences_vector`, then call
   `tsp.embeddings`, delete the document rows from `tsp.sentences_vector`, and
   update `tsp.text_similarity_input.document_state` to `DOC_DELETED`.
   These database changes remain behind the rollout toggle.
9. Keep the `tsp.text_similarity_input` row and do not delete a case-summary row.
10. Send the completed `tsp_id` to the next SQS queue so the state-machine flow can continue.
11. Continue until the current run has no more deletion records.

The job is idempotent. If one document fails, the job exits with that document
still marked `READY_FOR_DELETION`. When an outbound SQS send fails after the
state update, the job restores `READY_FOR_DELETION` before stopping. The next
scheduled run can safely repeat the OpenSearch checks, PostgreSQL cleanup, and outbound SQS send. The deployment
should use a CronJob concurrency policy of `Forbid` so two deletion jobs do not
process the same records at the same time.

`DocumentDeleteHandler` keeps OpenSearch, Titan matches, and legacy database
deletion in separate methods. OpenSearch and Titan cleanup always run.
`tsp.embeddings` and `tsp.sentences_vector` cleanup, plus the `DOC_DELETED`
state update, run only when `DATABASE_DELETE_ENABLED=true`.

Each PostgreSQL table has its own store file:

- `titan_matches_db_store.py` runs the bidirectional match deletion function.
- `embeddings_db_store.py` deletes embeddings by global ID.
- `sentence_vector_db_store.py` reads and deletes sentence rows by `tsp_id`.
- `text_similarity_input_db_store.py` loads deletion work and updates the retained document state.

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
make local
```

`make local` watches the Python source folders. Saving a Python code change
stops the current worker and starts it again with the updated code. The Docker
`make run` command keeps the normal production-style behavior without reload.

Set a local `tsp.text_similarity_input` row to `READY_FOR_DELETION`, then run
`make local`. The job processes the matching records and exits when the query
returns no more work.

## Tests

```bash
make test
```
