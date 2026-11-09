# Document Delete Job

Document Delete is a scheduled batch job that removes sentence data for
documents marked `READY_FOR_DELETION`. It processes one analysis group at a
time and handles a configured number of `tsp_id` values in each batch.

The job submits each batch to the deletion worker and waits for it to finish
before loading the next batch.

## What the job does

For each analysis group, the job:

1. Loads a batch of `tsp_id` values whose document state is
   `READY_FOR_DELETION`.
2. Waits until documents are no longer waiting for a model run.
3. Pauses Doc2Vec so new documents do not enter the processing flow during
   deletion.
4. Unless `force_start` is enabled, waits for documents already moving through
   downstream services to finish.
5. Submits the batch to the document-deletion worker.
6. Waits for deletion to finish before loading the next batch.
7. Stops when no documents remain, the run reaches its batch limit, or the
   remaining time is too short for another batch.
8. Unpauses Doc2Vec before the job exits.

The batch timer uses the longest completed batch as the estimate for the next
one. Documents left because of the time limit are picked up by the next
scheduled run.

## OpenSearch deletion flow

The OpenSearch flow processes each `tsp_id` in the batch:

1. Loads the document's records from `sentence_occurrences`.
2. Collects the document's unique `sentenceKey` values and analysis group.
3. Checks whether another document in the same analysis group still uses each
   sentence key.
4. Keeps shared vectors in the Asylee or Refugee sentence catalog.
5. Deletes catalog vectors used only by the document being deleted.
6. Deletes all occurrence records for the document.
7. Calls `tsp.delete_titan_matches_for_tsp` to remove Titan relationships in
   both directions.
8. Deletes related rows from `tsp.embeddings` and
   `tsp.sentences_vector`, then updates
   `tsp.text_similarity_input.document_state` to `DOC_DELETED`.
9. Sends the completed `tsp_id` to the Case Similarity Backfill SQS queue.

Catalog vectors are deleted before occurrences. If catalog cleanup fails, the
occurrences remain available so the document can be retried safely.

## Shared sentence-key example

Before deleting Asylee `tsp_id=100`:

```text
tsp_id 100 uses key-a and key-b
tsp_id 200 uses key-b
```

After deletion:

```text
key-a is removed from the Asylee catalog because no other document uses it
key-b remains because tsp_id 200 still uses it
all occurrence records owned by tsp_id 100 are removed
```

The Refugee catalog is separate and is not changed while an Asylee document is
being deleted.

## Titan match deletion

`tsp.delete_titan_matches_for_tsp` removes the document's source bucket rows
and every reverse JSONB relationship pointing to those global IDs. Reverse
bucket rows are deleted when their `matches` object becomes empty.

The function returns:

- whether the requested document existed in `tsp.titan_matches`;
- the number of source bucket rows deleted.

If the requested `tsp_id` is absent, the function returns immediately without
scanning or changing unrelated match rows.

## Case Similarity Backfill message

After one document finishes successfully, the OpenSearch flow sends:

```json
{
  "source": "P2",
  "tsp_id": "1040",
  "sent_by": "doc-delete"
}
```

The message is sent only after the document's deletion steps finish.
Case Similarity can then refresh the affected application summary.

## Failure handling

The deletion worker finishes before the batch loop continues. A critical
failure returns to the main job instead of being silently lost.

An expected failure for one `tsp_id` is logged and does not stop later IDs in
the same batch. The final error log lists every failed ID so it can be
submitted again. OpenSearch, PostgreSQL, and SQS operations use bounded retries
where configured.

The deletion operations are safe to retry. Missing occurrence, catalog, and
Titan match records are treated as already deleted.

## Project layout

```text
doc_delete_job/
  run.py                   Scheduled batch entry point
  opensearch/
    db_store/              PostgreSQL table and Titan function operations
    handlers/              One-document deletion flow
    opensearch_store/      Occurrence and catalog deletion operations
    config.py              Environment and Vault configuration
    constants.py           Fixed OpenSearch, database, and retry values
    dependencies.py        Database, OpenSearch, SQS, and handler construction
    document_delete_job.py Batch-level document processing
tests/opensearch/           OpenSearch-flow unit tests
```

## Local prerequisites

- Access to the PostgreSQL database used for testing
- AWS credentials that can access the configured OpenSearch domain
- Access to the Case Similarity Backfill SQS queue
- The Document Delete configuration used by the scheduled batch job

PostgreSQL and OpenSearch use the addresses and credentials supplied in the
local configuration.

## Configure the job

Create the local environment file:

```bash
cp .env.example .env
```

Set the service values in `.env`:

```text
OPENSEARCH_HOST=search-sentences.us-east-1.es.amazonaws.com
OPENSEARCH_IAM_ACCESS_ROLE=<OpenSearch role ARN>
CASE_SIMILARITY_BACKFILL_QUEUE_URL=<Case Similarity Backfill queue URL>
```

Create the local Vault file:

```bash
cp example.localVaultSecret.yaml localVaultSecret.yaml
```

Add the database values:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
```

The real `.env` and `localVaultSecret.yaml` files are ignored by Git.

## Run the job

The scheduled job entry point is:

```bash
python -m doc_delete_job.run
```

For a direct OpenSearch test, call the flow with a list of document IDs:

```python
from doc_delete_job.opensearch.main import run_document_delete_job

run_document_delete_job([1040, 1041])
```

## Run tests

Run the project checks and OpenSearch tests:

```bash
make ostest
```
