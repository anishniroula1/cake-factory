# Sentence Similarity Service

Sentence Similarity is an SQS/OpenSearch worker that processes one document (`tsp_id`) at
a time. It classifies every sentence, finds exact and semantic matches in
OpenSearch, and saves the relationships in PostgreSQL (`tsp` schema) database.

The same application supports real-time and historical processing. Each
deployment can listen to a different SQS queue, but the processing logic stays
the same.

## What the service does

For each SQS message, the service:

1. Reads `tsp_id` from the message.
2. Confirms the document's sentence occurrences are searchable in OpenSearch.
3. Loads source sentences from `sentences_vector` in pages of 30.
4. Uses the document's `analysis_group` to select the Asylee or Refugee catalog.
5. Loads each source vector from that catalog by `sentenceKey`.
6. Processes up to 30 source sentences at the same time.
7. Checks each vector against the tracer and form-language catalog.
8. Updates the sentence classification in `sentences_vector` table and the sentence catalog.
9. Skips normal matching when a sentence is a tracer, non-matcher, or form language.
10. Searches the sentence catalog for every exact and semantic key at or above 76%.
11. Expands those keys through `sentence_occurrences` and excludes occurrences
    from the source application.
12. Classifies target sentences and removes ineligible targets.
13. Saves the remaining relationships in both directions in `tsp.titan_matches`.
14. Sends the completed `tsp_id` to the next SQS queue (Case Similarity Service).
15. Deletes the source message only after all work completes successfully.

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

## Sentence classification

The tracer search reads the 100 closest references for the same analysis group.
Only results with at least 70% cosine similarity affect the classification.

```text
isNonMatcher=false and isFormLanguage=false -> tracer
isNonMatcher=true                           -> non-matcher
isFormLanguage=true                         -> form language
no qualifying result                        -> eligible for sentence matching
```

### Non matcher logic
```
1. is_non_matcher and not is_form_lanaguage
2. is_non_matcher and is_form_lanaguage
3. non is_non_matcher and is_form_lanaguage
```

All three flags are independent. Different qualifying reference records can
make more than one flag true. A sentence is excluded from normal sentence
matching when any flag is true.

The result is stored on the sentence catalog. Eligible classifications can be
reused by another occurrence with the same `sentenceKey`; excluded results are
checked again against the tracer catalog. Occurrence-level flags are stored in
`sentences_vector`.

## Sentence matching

Eligible sentences are searched in one of these catalogs alais:

- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`

The semantic search uses the 512-dimension Titan vector, a 76% threshold, and
`nprobes=64`. Every matching sentence key is expanded through
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
  db_store/              PostgreSQL reads, classification updates, and match writes
  handlers/              One-document processing flow
  opensearch_store/      Occurrence, catalog, and tracer OpenSearch operations
  utill/                 Score, vector, catalog, and classification helpers
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
