# Doc2Vec Historical Sentence Ingestion

This is a standalone SQS worker for historical sentence backfill. It reads
already-split sentences from PostgreSQL and writes them to the sentence
catalog and occurrence aliases in AWS OpenSearch.

It is not an API or FastAPI application. It processes one SQS message at a
time, and all historical processing logic stays inside
`handlers/historical_handler.py`.

`dependencies.py` creates the configuration, database connection, OpenSearch
helper, SQS helper, and historical handler. The SQS processor receives only
this one dependency object. `main.run(customLogger=...)` can still receive the
team's logger, while `custom_logger.py` remains the standalone default.

`AwsSQS.py` mirrors the shared SQS library used by the application. It creates
the AWS client from the pod credentials, receives and formats messages, keeps
the receipt handle and receive count, deletes completed messages, and sends the
next FIFO or standard message. The historical code uses only
`listen_sqs_message`, `send_sqs_message`, and `delete_messages`; unused batch
operations are intentionally not included.

`DatabaseConnection.py` mirrors the shared database library's connection-pool
interface. It owns the SQLAlchemy engine, pool size, overflow, recycle time,
pre-ping check, and PostgreSQL `application_name`. The standalone project
creates it from `DATABASE_URL`. Production can pass the real shared object with
`main.run(databaseConnection=sharedConnection)`; `main` only requires its
`engine`. This lets the shared library keep responsibility for Vault or other
dynamic credentials.

## Message

Input:

```json
{"reason": "HISTORICAL", "tsp_id": 1040}
```

The SQS processor requires `reason=HISTORICAL` and a `tsp_id`. A message without
that reason is logged and is not sent into the historical handler. The worker
keeps `tsp_id` exactly as received, so both a number and a string are valid. It
does not accept the alternate name `tspId` in an SQS message.

The main code types are defined in `contracts.py`. The processing flow is:

```text
SqsMessage -> TspId -> list[SentenceVector] -> HistoricalResult
```

OpenSearch catalog and occurrence documents also have separate named types, so
the expected fields are visible before a document is sent for bulk indexing.

## Historical flow

1. Count all `sentences_vector` rows for the `tsp_id`.
2. Read up to 3,000 rows at a time in `global_id` order.
3. Generate and save the SHA-256 `sentence_key` for every row.
4. Split the page into 30-sentence jobs and run up to 100 workers.
5. Check which sentence keys already exist in the catalog.
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

## Database tables

The worker uses:

- `sentences_vector`
- `embeddings`, joined by `global_id`

If production column names differ, update `models.py`.

## OpenSearch aliases

The worker writes to:

- `sentence_occurrences`
- `sentence_semantic_catalog`

The catalog uses a 512-dimension `sentenceContentVector`. Its default Bedrock
pipeline creates a vector when the field is missing. A saved vector is sent
with `pipeline=_none`, which avoids another Bedrock call.

## Run

Copy `.env.example` to `.env` and fill in the database, AWS, OpenSearch, and
SQS values.

```bash
make test
make dev
```

`make dev` runs the historical worker in Docker. `make local` runs it without
Docker. There is no HTTP port, health endpoint, or Swagger page.

## Portable copy

Copy `generate_project.py` to another machine and run:

```bash
python3 generate_project.py /path/to/parent-folder
```

It creates `/path/to/parent-folder/doc2vec-service` with the complete
historical-only project and a safe `.env` copied from `.env.example`.
