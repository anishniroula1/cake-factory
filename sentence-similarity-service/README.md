# Sentence Similarity Service

Sentence Similarity is an SQS/OpenSearch worker that processes one document
(`tsp_id`) at a time. It finds exact and semantic matches for sentences already
approved by Doc2Vec and saves the relationships in PostgreSQL.

The same application supports real-time and historical processing. Each
deployment can listen to a different SQS queue, but the processing logic stays
the same.

## What the service does

For each SQS message, the service:

1. Reads `tsp_id` from the message.
2. Confirms the document's sentence occurrences are searchable in OpenSearch.
3. Loads every source sentence from `sentences_vector` in database pages of
   1000 before starting worker threads.
4. Reports the total, eligible, short-sentence, non-matcher, tracer, and
   form-language counts. Only eligible rows continue.
5. Uses the document's `analysis_group` to select the Asylee or Refugee catalog.
6. Loads every eligible source vector from that catalog by `sentenceKey`.
   Missing vectors are checked up to three times with a 30-second readiness
   wait. Later attempts request only the keys that are still unavailable.
7. Processes up to 30 eligible source sentences at the same time.
   Progress logs group the submitted work into 30-sentence ranges and show the
   starting and ending `global_id`. A failed worker logs its exact source
   `global_id` without logging sentence content.
8. Searches the sentence catalog for every exact and semantic key at or above
   76%. The catalog query also excludes short, tracer, non-matcher, and
   form-language target keys.
9. Expands those keys through `sentence_occurrences` and excludes occurrences
    from the source application.
10. Saves the remaining relationships in both directions in `tsp.titan_matches`.
11. Sends the completed `tsp_id` to the next SQS queue (Case Similarity Service).
12. Deletes the source message only after all work completes successfully.

If processing fails, the source message is not deleted. The SQS retries three time and eventual delivery to the DLQ.

## Message format

The source queue accepts one document message:

```json
{
  "tsp_id": 8205
}
```

After processing, the next queue receives:

```json
{
  "source": "P2",
  "tsp_id": "8205",
  "sent_by": "sentence-similarity"
}
```

## Doc2Vec prerequisite

Doc2Vec calculates `word_count` from sentence content and classifies every
unique sentence key before sending the document here. It stores the three
classification flags in the sentence catalog and copies them to each
`sentences_vector` occurrence. Sentence Similarity therefore does not repeat
tracer searches or classification writes.

A document with no eligible sentences is still completed and sent to the next
queue with zero relationships. This is a valid result, not a retry condition.

## Sentence matching

Eligible sentences are searched in one of these catalogs alais:

- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`

The semantic search uses the 512-dimension Titan vector, a 76% threshold, and
`nprobes=64`. It searches only catalog targets whose word count is at least five
and whose three classification flags are false. Every matching sentence key is expanded through
`sentence_occurrences`. The occurrence search stays inside the analysis group
and excludes the source `applicationId`.

## Titan match buckets

PostgreSQL stores relationships in `tsp.titan_matches`. Each source
`global_id` can have 16 smaller JSONB bucket rows instead of one very large
row:

```text
bucket = matching global_id % 16
34 % 16 = 2 <---- 2 is the bucket number
```

One bucket can look like this:

```json
{
  "200": 0.1,
  "232": 0.0
}
```

Distance `0.0` is an exact vector match. Smaller distances are better. Python
removes duplicate forward/reverse pairs and groups the remaining matches
by source global ID. The `tsp.upsert_bidirectional_titan_matches` function then
receives at most 3000 targets per call, writes both directions, and calculates
`match_count` and `verbatim`. Calls run sequentially and commit independently so
one completed source batch does not keep its locks while the next batch runs.

The complete SQL and a runnable example are in
`UPSERT_BIDIRECTIONAL_TITAN_MATCHES.sql`.

## Project layout

```text
similarity_completed/opensearch/
  db_store/              Source sentence reads and Titan match writes
  handlers/              One-document processing flow
  opensearch_store/      Occurrence and sentence-catalog OpenSearch operations
  utill/                 Score, vector, and catalog helpers
  config.py              Environment and Vault configuration
  constants.py           Fixed database, OpenSearch, processing, and SQS values
  dependencies.py        Database, OpenSearch, SQS, and handler construction
  sqs_processor.py       Receive, process, and delete SQS messages
tests/opensearch/        OpenSearch-flow unit tests
```

## Local prerequisites

- Access to the PostgreSQL database used for testing
- AWS credentials that can access the configured OpenSearch domain

LocalStack is used only for SQS. OpenSearch and PostgreSQL still use the
addresses and credentials supplied in the local configuration.

## Configure the service

Create the local environment file:

```bash
cp example.env .env
```

Set `OPENSEARCH_HOST` and `OPENSEARCH_IAM_ACCESS_ROLE` in `.env`. The host can
include `https://`; the application removes the scheme before creating the
client. If the current AWS credentials can access OpenSearch directly, the role
ARN can be left empty.

Create the local Vault file:

```bash
cp example.localVaultSecret.yaml localVaultSecret.yaml
```

Add the local database values:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
```

The real `.env` and `localVaultSecret.yaml` files are ignored by Git.

## Run locally — recommended

Running the Python process locally is faster than rebuilding the Docker image
after every code change.

Start LocalStack and create the source and next-service FIFO queues:

```bash
make localstack
```

Start the worker:

```bash
make local
```

In a second terminal, send the sample message from the Makefile:

```bash
make sos
```

In the makefile there is a `opensearch_msg`, change the `tsp_id` as per your test need.


## Run tests

Run only the OpenSearch flow tests:

```bash
make ostest
```

`ostest` runs the tests inside  `tests/opensearch`

```bash
make test
```
