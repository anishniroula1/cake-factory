# Document Delete OpenSearch Worker

This worker removes the sentence data owned by one `tsp_id`. It processes one
SQS message at a time and keeps `tsp.text_similarity_input` so the next Case
Similarity step can identify and refresh the affected application.

## Processing flow

1. Receive one SQS message containing `tsp_id`.
2. Read every matching record from `sentence_occurrences`.
3. Check each sentence key against occurrences from other `tsp_id` values in
   the same analysis group.
4. Delete a catalog vector only when no other document uses that key. The
   Asylee and Refugee catalogs are handled independently.
5. Bulk-delete the document's occurrence records only after catalog cleanup
   succeeds. This keeps the occurrence records available when catalog cleanup
   fails and SQS needs to retry.
6. When `DATABASE_DELETE_ENABLED=true`, read the document's global IDs from
   `tsp.sentences_vector`, then call
   `tsp.delete_titan_matches_for_tsp`. It deletes source match buckets, removes
   reverse JSONB references, and deletes reverse buckets that become empty.
7. When database deletion is enabled, delete matching rows from
   `tsp.embeddings`, then delete the document rows from
   `tsp.sentences_vector`.
8. Do not delete `tsp.text_similarity_input` or a case-summary row.
9. Send the same `tsp_id` to the next SQS queue so Case Similarity can rebuild
    source and reverse application summaries.
10. Delete the source SQS message only after every step succeeds.

The flow is idempotent. SQS can retry after a partial failure because repeated
OpenSearch deletes, catalog checks, PostgreSQL cleanup, and match deletion are
safe.

`DocumentDeleteHandler` keeps OpenSearch and PostgreSQL deletion in separate
methods. OpenSearch cleanup always runs. PostgreSQL cleanup runs only when
`DATABASE_DELETE_ENABLED=true`. The default is `false`, which keeps database
records during the first production rollout.

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
make local
```

`make local` watches the Python source folders. Saving a Python code change
stops the current worker and starts it again with the updated code. The Docker
`make run` command keeps the normal production-style behavior without reload.

Send the sample local SQS message with:

```bash
make send-message
```

## Tests

```bash
make test
```
