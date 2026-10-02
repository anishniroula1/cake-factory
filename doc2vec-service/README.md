# Doc2Vec Historical Sentence Ingestion

This is a standalone SQS worker for historical sentence backfill. It reads
already-split sentences from PostgreSQL and writes them to the sentence
catalog selected by `analysis_group` and the occurrence alias in AWS
OpenSearch.

It is not an API or FastAPI application. It processes one SQS message at a
time. `historical_handler.py` coordinates pages, worker threads, retries, and
queue handoff. `sentence_batch_processor.py` contains the reusable batch work:
sentence keys, saved embeddings, catalog writes, and occurrence writes.

All OpenSearch flow code lives under `doc2vec_completed/opensearch`. All tests
for this flow live under `tests/opensearch`.

`dependencies.py` creates the configuration, database connection, OpenSearch
helper, SQS helper, and historical handler. The SQS processor receives only
this one dependency object.
`doc2vec_completed.opensearch.main.run_opensearch_flow(custom_logger=...)` can
still receive the team's logger, while `custom_logger.py` remains the
standalone default.

`AwsSQS.py` mirrors the shared SQS library used by the application. It creates
the AWS client from the pod credentials, receives and formats messages, keeps
the receipt handle and receive count, deletes completed messages, and sends the
next FIFO or standard message. The historical code uses only
`listen_sqs_message`, `send_sqs_message`, and `delete_messages`; unused batch
operations are intentionally not included.

`DatabaseConnection.py` mirrors the shared database library's connection-pool
interface. It owns the SQLAlchemy engine, pool size, overflow, recycle time,
pre-ping check, and PostgreSQL `application_name`. `VaultSecrets.py` reads the
mounted YAML file identified by `VAULT_YAML_PATH`. Production can still pass
the real shared database object by calling `run_opensearch_flow` with
`database_connection=shared_connection`.

The Vault YAML file provides these values:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
awsReceiveSqsQueueEndpoint: doc2vec-queue-url
awsSqsSendEventQueueEndpoint: state-machine-queue-url
```

## Message

Input:

```json
{"reason": "HISTORICAL", "tsp_id": 1040}
```

The SQS processor requires `reason=HISTORICAL` and a `tsp_id`. A message without
that reason is logged and is not sent into the historical handler. The worker
keeps `tsp_id` exactly as received, so both a number and a string are valid. It
does not accept the alternate name `tspId` in an SQS message.

The SQS value is used only for the database lookup and next message. The
OpenSearch occurrence uses the numeric `tsp_id` loaded from
`text_similarity_input`.

The main code types are defined in `contracts.py`. The processing flow is:

```text
SqsMessage -> TspId -> list[SentencesVector] -> HistoricalResult
```

OpenSearch catalog and occurrence documents also have separate named types, so
the expected fields are visible before a document is sent for bulk indexing.

## Historical flow

1. Count all `sentences_vector` rows for the `tsp_id`.
2. Read up to 3,000 rows at a time in `global_id` order.
3. Split the page into 30-sentence jobs and run up to 100 workers.
4. Inside each worker, reuse existing sentence keys and generate and save only
   the missing SHA-256 keys.
5. Read the document's one `analysis_group` from `text_similarity_input`,
   select the Asylee or Refugee catalog, and check which sentence keys exist.
6. For a missing key, reuse its saved Titan vector from `embeddings` when
   `embedding_type = 2`. If no vector exists, let the catalog's Bedrock ingest
   pipeline create it.
7. Write every sentence occurrence using `global_id` as its OpenSearch ID.
8. After every page succeeds, send the `tsp_id` and `HISTORICAL` reason to the
   Sentence Similarity queue.
9. The SQS processor deletes the source message only after the handler succeeds.

Documents use stable OpenSearch IDs, so retrying a page updates the same
documents instead of creating duplicates. Temporary operations retry three
times by default. The OpenSearch helper continues retrying temporary Lasso,
Bedrock, gateway, and OpenSearch failures while applying the shared embedding
cooldown.

## Processing logs

The worker writes simple progress logs in processing order. They show when a
message starts, how many sentences were found, when each database page is
loaded, when keys are saved, and when catalog and occurrence writes finish.
Parallel worker messages include the first and last `global_id` in the batch,
so a failed batch is easy to find without logging the `tsp_id`.

Every failed stage writes an error before it is retried. Retry logs show the
attempt number and wait time. A permanent failure says that it will not be
retried, and the source SQS message is left on the queue. A successful flow
logs the source count, indexed count, next-queue send, and source-message
deletion.

The OpenSearch helper also logs every ID lookup and bulk write. Its messages
show the alias, document count, whether saved vectors or the Bedrock pipeline
are used, retry failures, permanent failures, and shared embedding cooldown
waits. Empty lookup and write requests are logged as skipped operations.

## Database tables

The worker uses:

- `sentences_vector`
- `text_similarity_input`, joined by `tsp_id` to read `application_id`,
  `analysis_group`, `section_name`, and `is_current`
- `embeddings`, joined by `global_id`

The historical page query does not load `sentences_vector.embedding_list`.
That legacy column is not used by this flow and may contain NumPy arrays that
cannot be loaded through SQLAlchemy `MutableList`. Saved Titan vectors are read
only from the separate `embeddings` table.

`a_number` remains in PostgreSQL. The occurrence index stores the numeric
`applicationId` and `tspId`. It reads `applicationId`, `analysisGroup`,
`sectionName`, and `isCurrent` from the related `text_similarity_input` row and
does not store `a_number`.

If production column names differ, update `models.py`.

## OpenSearch aliases

The worker writes to:

- `sentence_occurrences`
- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`

Both catalogs use the same trained 512-dimension Faiss IVF-SQ16 model. Their
default Bedrock pipeline creates a vector when the field is missing. A saved
vector is sent with `pipeline=_none`, which avoids another Bedrock call. The
same sentence key may correctly exist once in each catalog.

The occurrence document stores identity, content, location, `analysisGroup`,
and `isCurrent`. It does not store `isNonMatcher`, `isTracer`,
`isFormLanguage`, or `nonMatcherChecked`. Sentence Similarity stores the
reusable classification on the selected group catalog and the occurrence-level
classification in PostgreSQL.

## Run

Copy `.env.example` to `.env`, set `VAULT_YAML_PATH`, and fill in the AWS,
OpenSearch, and Sentence Similarity queue values.

The OpenSearch client uses the pod service-account credentials to assume
`OPENSEARCH_IAM_ACCESS_ROLE`. The pod role needs permission to call
`sts:AssumeRole`, and the assumed role needs access to the OpenSearch domain.
Do not inject `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or
`AWS_SESSION_TOKEN` through Harness. The assumed-role credentials refresh
automatically during a long historical run.

```bash
make test
make dev
```

`make dev` runs the historical worker in Docker. `make local` runs it without
Docker. There is no HTTP port, health endpoint, or Swagger page.

The main tests start at `main.run_opensearch_flow()` and execute the real SQS
processor, historical handler, and sentence batch processor. Only database
sessions, OpenSearch, and SQS transport are replaced at the external boundary.
The batch processor does not have a duplicate test file; its behavior is
covered by the historical flow test.

## Portable copy

Copy `generate_project.py` to another machine and run:

```bash
python3 generate_project.py /path/to/parent-folder
```

It creates `/path/to/parent-folder/doc2vec-completed` with the complete
historical-only project and a safe `.env` copied from `.env.example`.
