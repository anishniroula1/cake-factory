# Doc2Vec Sentence Ingestion

This is a standalone SQS application. It reads PostgreSQL with SQLAlchemy and
writes sentence data to the existing AWS OpenSearch aliases. It does not import
code or files from another project.

This is not an API or FastAPI application. It runs one foreground SQS listener
and processes one message at a time.

## Simple code layout

The worker intentionally has one short path:

1. `main.py` creates the database, AWS, OpenSearch, and SQS clients.
2. `sqs_listener.py` reads one SQS message and checks its `reason`.
3. The listener sends the message to exactly one handler:
   - `handlers/tracer_handler.py` owns the complete TRACER flow.
   - `handlers/train_handler.py` owns the complete TRAIN flow.
   - `handlers/live_handler.py` owns the complete LIVE flow.
4. After the handler finishes, the listener deletes the SQS message.

There are no controller, service, repository, processor, or dependency-container
layers. Database queries and flow decisions stay in the handler that uses them.
`opensearch.py` only holds the common bulk-write and retry code needed by all
three handlers.

## Message flows

### TRAIN

Input:

```json
{"reason": "TRAIN", "tsp_id": 1040}
```

1. Count the current `sentences_vector` rows for the `tsp_id`.
2. Read rows 1,000 at a time in `global_id` order.
3. Split each page into 30-sentence jobs and run up to 16 jobs together.
4. Generate and save each SHA-256 `sentence_key`.
5. Read `embedding_type = 2` from `embeddings` by `global_id`.
6. If a saved 512-number vector exists, send it and bypass the default ingest
   pipeline. Otherwise, send the text and let the Bedrock pipeline create it.
7. Index by `sentenceKey`, so a retry or duplicate sentence does not create a
   second training document.

### TRACER

Input:

```json
{"reason": "TRACER"}
```

The service reads all `tracer_sentence` rows in pages of 1,000, decodes the
saved little-endian 512-float Titan `BYTEA`, and bulk writes it to
`tracer_form_language_catalog`. Every document uses the `analysis_group` from
its own database row. No Bedrock call is made for this flow.

### Live document

Input (reason can be omitted because LIVE is the default):

```json
{"reason": "LIVE", "tsp_id": 1040}
```

1. Load `tsp.text_similarity_input` by `tsp_id`.
2. Call `splitSentences`. Replace that small method with the existing
   production sentence splitter.
3. Generate sentence keys and upsert `sentences_vector` 1,000 rows at a time.
   Each batch commits, and the handler reads the saved rows with their generated
   `global_id` values.
4. Read those saved rows in pages. Sixteen workers handle 30 rows each.
5. Add missing `sentenceKey` documents to the semantic catalog, then add every
   `globalId` to the occurrence index.
6. Send the completed `tsp_id` to Sentence Similarity, update document state,
   and only then delete the source SQS message.

## Failure safety

- OpenSearch writes use stable IDs, so sending the same batch again updates the
  same documents.
- The current `(tsp_id, sent_id_local)` unique index makes live database writes
  safe when SQS delivers a message again.
- Temporary database or processing failures are tried up to three times by
  default, with a five-second wait between attempts. A caller can pass a
  different `maxAttempts` value when needed.
- Mapping errors, invalid vectors, missing source documents, and bad SQS
  payloads are permanent. They are not hidden in an endless loop; the message
  remains in SQS and can reach its DLQ.
- SQS uses a two-hour visibility timeout while one message is being processed.
- SQS completion is at-least-once. The downstream consumer must also use
  `tsp_id` as an idempotency key.

## Required database shape

The code maps these existing tables:

- `tsp.text_similarity_input`
- `sentences_vector`
- `embeddings`, joined by `global_id`; Titan rows use `embedding_type = 2`
- `tracer_sentence`; its `embedding` is a 2,048-byte little-endian Titan vector

Live idempotency requires this PostgreSQL index:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS ux_sentences_vector_current_tsp_line
ON sentences_vector (tsp_id, sent_id_local)
WHERE is_current = TRUE;
```

If production column names differ, change only `models.py`; service logic does
not contain raw table names.

## Required OpenSearch behavior

Infrastructure creates the physical indexes, aliases, IVF model, and pipelines.
This service only writes to these aliases:

- `sentence_occurrences`
- `sentence_semantic_catalog`
- `sentence_semantic_training`
- `tracer_form_language_catalog`

The training and semantic catalog mappings use a 512-dimension
`sentenceContentVector`. Their default Bedrock pipeline must create a vector
when the vector field is missing. Existing vectors are sent with
`pipeline=_none`, so the service does not pay to create them again.

## Run

Copy `.env.example` to `.env` and fill the real database, AWS, OpenSearch, and
SQS values.

```bash
make test
make dev
```

`make dev` runs the worker in Docker. `make local` runs the same foreground SQS
worker without Docker. There is no HTTP port, health endpoint, or Swagger page.

## Portable copy

`generate_project.py` contains an encoded copy of this complete project. Copy
that one file to another machine and run:

```bash
python3 generate_project.py /path/to/parent-folder
```

It creates `/path/to/parent-folder/doc2vec-service`, including a safe `.env`
copied from `.env.example`. Fill in the real values before starting the worker.
