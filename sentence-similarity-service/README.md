# Sentence Similarity Service

Sentence Similarity is an SQS worker that processes one document (`tsp_id`) at
a time. It classifies every sentence, finds exact and semantic matches in
OpenSearch, and saves the relationships in PostgreSQL.

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
8. Updates the sentence classification in PostgreSQL and the sentence catalog.
9. Skips normal matching when a sentence is a tracer, non-matcher, or form language.
10. Searches the sentence catalog for every exact and semantic key at or above 76%.
11. Expands those keys through `sentence_occurrences` and excludes occurrences
    from the source application.
12. Classifies target sentences and removes ineligible targets.
13. Saves the remaining relationships in both directions in `tsp.titan_matches`.
14. Sends the completed `tsp_id` to the next SQS queue.
15. Deletes the source message only after all work completes successfully.

If processing fails, the source message is not deleted. The SQS redrive policy
controls retries and eventual delivery to the DLQ.

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

All three flags are independent. Different qualifying reference records can
make more than one flag true. A sentence is excluded from normal sentence
matching when any flag is true.

The result is stored on the sentence catalog. Eligible classifications can be
reused by another occurrence with the same `sentenceKey`; excluded results are
checked again against the tracer catalog. Occurrence-level flags are stored in
`sentences_vector`.

## Sentence matching

Eligible sentences are searched in one of these catalogs:

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
```

One bucket can look like this:

```json
{
  "200": {
    "tsp_id": 9001,
    "application_id": 300,
    "distance": 0.1
  },
  "232": {
    "tsp_id": 9002,
    "application_id": 301,
    "distance": 0.0
  }
}
```

Distance `0.0` is an exact vector match. Smaller distances are better. The
PostgreSQL function writes both directions in one transaction and calculates
`match_count` and `verbatim`. Buckets keep JSONB updates smaller and reduce
lock contention between sentence workers.

The shared Flyway migration owns the table and the
`tsp.upsert_bidirectional_titan_matches` function.

## Project layout

```text
similarity_completed/opensearch/
  db_store/              PostgreSQL reads, classification updates, and match writes
  handlers/              One-document processing flow
  opensearch_store/      Occurrence, catalog, and tracer OpenSearch operations
  utill/                 Score, vector, catalog, and classification helpers
  config.py              Environment values and processing constants
  dependencies.py        Database, OpenSearch, SQS, and handler construction
  sqs_processor.py       Receive, process, and delete SQS messages
ta_db_connection/        Local copy of the shared database library imports
ta_utils/                Local copy of shared SQS, logging, and Vault imports
tests/opensearch/        OpenSearch-flow unit tests
```

The local `ta_utils` and `ta_db_connection` folders use the same import paths as
the team libraries. They let this project run by itself. When copying the
service into the main application, those local folders can be removed so the
installed libraries provide the imports.

## Local prerequisites

- Python 3.12
- `uv`
- Docker with Docker Compose
- Access to the PostgreSQL database used for testing
- AWS credentials that can access the configured OpenSearch domain

LocalStack is used only for SQS. OpenSearch and PostgreSQL still use the
addresses and credentials supplied in the local configuration.

## Configure the service

Create the local environment file:

```bash
cp .env.example .env
```

Set `OPENSEARCH_HOST` and `OPENSEARCH_IAM_ACCESS_ROLE` in `.env`. The host can
include `https://`; the application removes the scheme before creating the
client. If the current AWS credentials can access OpenSearch directly, the role
ARN can be left empty.

Create the local Vault file:

```bash
cp vault-secrets.local.example.yaml vault-secrets.local.yaml
```

Add the local database values:

```yaml
pgUser: your-database-user
pgPassword: your-database-password
pgHost: your-database-host
pgDatabase: your-database-name
pgPort: 5432
```

The real `.env` and `vault-secrets.local.yaml` files are ignored by Git.

## Run locally — recommended

Running the Python process locally is faster than rebuilding the Docker image
after every code change.

Start LocalStack and create the source and next-service FIFO queues:

```bash
make localstack
```

In a second terminal, start the worker:

```bash
make local
```

In a third terminal, send the sample message from the Makefile:

```bash
make sos
```

The sample uses `tsp_id=8205`. Pass another document ID when needed:

```bash
make sos TSP_ID=12345
```

## Run in Docker

Create `.env` and `vault-secrets.local.yaml` first, then run:

```bash
make run
```

This starts LocalStack, creates both FIFO queues, builds the worker image, and
runs the worker. Follow its logs with:

```bash
make logs
```

Stop the worker and LocalStack with:

```bash
make down
```

## Run tests

Run only the OpenSearch flow tests:

```bash
make ostest
```

`ostest` installs the development dependencies and runs
`tests/opensearch`. These tests use controlled database, OpenSearch, and SQS
boundaries; they do not require the real OpenSearch domain or PostgreSQL
database.

Run lint plus the complete test suite with the configured coverage requirement:

```bash
make test
```

## Make commands

| Command | Purpose |
|---|---|
| `make localstack` | Start LocalStack and create both local FIFO queues. |
| `make local` | Run the worker directly with `uv`; recommended for development. |
| `make sos` | Send the default sample `tsp_id` to the source queue. |
| `make sos TSP_ID=12345` | Send a specific `tsp_id`. |
| `make run` | Build and run the worker and LocalStack in Docker. |
| `make ostest` | Run only `tests/opensearch`. |
| `make test` | Run Ruff and the full test suite with coverage. |
| `make logs` | Follow Docker worker logs. |
| `make down` | Stop Docker services. |

## Main processing settings

The values below are constants in `similarity_completed/opensearch/config.py`:

| Setting | Value |
|---|---:|
| Source sentence page | 30 |
| Sentence worker threads | 30 |
| Titan vector dimensions | 512 |
| Sentence match threshold | 76% |
| Tracer classification threshold | 70% |
| Tracer top K | 100 |
| IVF nprobes | 64 |
| Titan match write batch | 500 |
| Retry wait | 5 seconds |
| Occurrence readiness wait | 30 seconds |
| SQS visibility timeout | 2 hours |

## Common local issues

- **Local SQS connection fails:** run `make localstack` and confirm
  `AWS_LOCAL_ENDPOINT_URL=http://localhost:4566` in `.env`.
- **Vault file is missing:** create `vault-secrets.local.yaml` and keep
  `VAULT_YAML_PATH=./vault-secrets.local.yaml`.
- **OpenSearch returns 403:** confirm the active AWS credentials and the role
  configured by `OPENSEARCH_IAM_ACCESS_ROLE` can access the domain.
- **The document is not processed:** confirm Doc2Vec already indexed the
  document's sentence occurrences and catalog vectors.
