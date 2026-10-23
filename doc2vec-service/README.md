# Doc2Vec OpenSearch Ingestion

This standalone SQS worker supports incoming documents, historical sentence
backfill, IVF training data, and tracer or form-language reference data. Each
message is routed to a separate handler.

It is not an API or FastAPI application. It processes one SQS message at a
time. `live_handler.py`, `historical_handler.py`, `train_model_handler.py`, and
`tracer_handler.py` contain the four independent flows.

All OpenSearch flow code lives under `doc2vec_completed/opensearch`. All tests
for this flow live under `tests/opensearch`.

`dependencies.py` creates the configuration, database connection, OpenSearch
store, SQS helper, and all four handlers. The SQS processor receives only this
one dependency object.
`doc2vec_completed.opensearch.main.run_opensearch_flow(custom_logger=...)` can
still receive the team's logger, while `ta_utils/logger/CustomLogger.py`
remains the standalone default.

`ta_utils/aws/sqs/AwsSQS.py` mirrors the shared SQS library used by the
application. It creates the AWS client from the pod credentials, receives and
formats messages, keeps the receipt handle and receive count, deletes completed
messages, and sends the next FIFO or standard message. The worker uses only
`listen_sqs_message`, `send_sqs_message`, and `delete_messages`; unused batch
operations are intentionally not included.

`ta_db_connection/database_connection.py` mirrors the shared database
library's connection-pool interface. `ta_db_connection/atables.py` contains
the table models used by this flow. The connection owns the SQLAlchemy engine,
pool size, overflow, recycle time, pre-ping check, and PostgreSQL
`application_name`. `ta_utils/vault/VaultSecrets.py` reads the mounted YAML
file identified by `VAULT_YAML_PATH`. Production can still pass the real shared
database object by calling `run_opensearch_flow` with
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

Supported messages are:

```json
{"tsp_id": 1040}
```

An incoming/live document has no `reason`. This is the existing live message
contract and routes the message to `LiveHandler`.

```json
{"reason": "HISTORICAL", "tsp_id": 1040}
```

```json
{"reason": "TRAIN_MODEL", "tsp_id": 1040}
```

```json
{"reason": "TRACER", "tracer_sentence_ids": [10, 20, 30]}
```

Historical and training messages keep `tsp_id` exactly as received, so both a
number and a string are valid. The alternate name `tspId` is not accepted.
Tracer IDs must be integers. An unsupported or incomplete message is logged and
is not deleted.

The main code types are defined in `contracts.py`. The four processing flows
are:

```text
SqsMessage -> TspId -> list[SentencesVector] -> HistoricalResult
SqsMessage -> TspId -> list[SentencesVector] -> TrainModelResult
SqsMessage -> list[tracer_sentence_ids] -> list[TracerSentence] -> TracerResult
SqsMessage -> TextSimilarityInput -> list[SentencesVector] -> LiveResult
```

OpenSearch catalog and occurrence documents also have separate named types, so
the expected fields are visible before a document is sent for bulk indexing.

## Code structure

`constants.py` holds fixed batch sizes, worker counts, retry values, index
defaults, and connection defaults. `config.py` is limited to values read from
Vault or the environment and uses those constants as defaults.

Database access is separated under `db_store`:

- `TextSimilarityInputDBStore` loads an incoming document and its text.
- `SentenceVectorDBStore` saves live sentence batches, counts and pages saved
  rows, and saves sentence keys.
- `EmbeddingDBStore` loads saved Titan embeddings by `global_id`.
- `TracerSentenceDBStore` loads tracer reference rows by tracer ID.

OpenSearch access is separated under `opensearch_store`.
`OpenSearchDocumentStore` handles existing-ID lookups, bulk indexing, pipeline
selection, vector validation, refreshes, and temporary embedding cooldowns.
The same low-level store is shared because occurrence, catalog, training, and
tracer ingestion all use the same bulk-write behavior.

Handlers now coordinate the processing steps only. They call the stores for
database and OpenSearch work instead of containing SQL or low-level indexing
code.

## Live document flow

1. Receive the existing live message containing `tsp_id` and no `reason`.
2. Load the `text_similarity_input` row and its document text.
3. Split the text with the same Syntok rules used by the old Doc2Vec flow. The
   original text is saved; temporary tokenizer masks are never stored.
4. Create `sent_id_local` values starting at 1 and create a stable SHA-256
   `sentence_key` for every sentence.
5. Save up to 1000 sentence rows per database commit. A retried message reuses
   `(tsp_id, sent_id_local)` rows instead of inserting the same sentence again.
6. Split the saved rows into 30-sentence worker jobs and index them concurrently.
7. Check the selected Asylee or Refugee catalog before creating a vector. Reuse
   a saved Titan type-2 embedding when available; otherwise let the Bedrock
   pipeline create the vector.
8. Write every sentence to the occurrence index, including sentences with fewer
   than five words. Sentence Similarity applies that matching rule later.
   The catalog `wordCount` preserves Unicode words and counts a consecutive run
   of three or more identical words once. This prevents text such as
   `yes yes yes yes yes` from passing the five-word rule without reducing a
   normal sentence such as `The man said the man escaped`.
9. Refresh the selected catalog and occurrence aliases once after all writes.
10. Send the existing state-machine result and delete the source SQS message
    only after the state-machine message is accepted.

The successful state-machine message is:

```json
{
  "source": "P2",
  "tsp_id": 1040,
  "sent_by": "doc2vec",
  "success": true,
  "reason": ""
}
```

Whitespace-only content, including the pilcrow character, sends the same
message with `success=false` and `reason="no_content"`. It does not create
sentence or OpenSearch rows.

## Model-training flow

1. Count the `sentences_vector` rows for the supplied `tsp_id`.
2. Load 1000 rows at a time in `global_id` order.
3. Exclude sentences marked `is_non_matcher`, `is_tracer`, or
   `is_form_language`. These sentences never enter the IVF training index.
4. Generate SHA-256 sentence keys in memory. The training flow does not update
   `sentences_vector.sentence_key`.
5. Remove duplicate sentence keys across the complete document.
6. Load Titan vectors from `embeddings` where `embedding_type = 2`.
7. Split unique documents into groups of 30 and use up to 16 workers.
8. Skip keys that already exist in `sentence_semantic_training`.
9. Reuse valid saved vectors and let the Bedrock pipeline create only missing
   vectors.
10. Refresh the training alias and delete the source SQS message after all
   batches finish.

The sentence key is the stable OpenSearch document ID. Reprocessing the same
training message therefore updates or skips the same document instead of
creating a duplicate.

## Tracer flow

1. Deduplicate the requested `tracer_sentence_ids`.
2. Load tracer-table rows in batches of 1000.
3. Build the strict tracer document containing `tracerId`, sentence text,
   analysis group, `isNonMatcher`, and `isFormLanguage`.
4. Reuse a valid saved vector when one exists. List, JSON-byte, raw float-byte,
   and NumPy-like saved values are supported.
5. Omit the vector when it is missing so the Bedrock pipeline creates it.
6. Use `tracerId` as the stable OpenSearch document ID, refresh the tracer
   alias, and delete the source message only after every requested row is
   indexed.

The flow stops without deleting the SQS message when a requested tracer ID does
not exist in PostgreSQL.

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
8. After all pages succeed, refresh the selected catalog and occurrence
   alias once, then send the `tsp_id` and `HISTORICAL` reason to the Sentence
   Similarity queue.
9. The SQS processor deletes the source message only after the handler succeeds.

Documents use stable OpenSearch IDs, so retrying a write updates the same
documents instead of creating duplicates. Each OpenSearch bulk write gets the
original request plus 15 retries, with a five-second wait after each failed
attempt. Other temporary database, AWS, OpenSearch lookup, refresh, queue, or
network operations get up to three total attempts. Only the failed operation
is retried; the whole worker batch is not started again. A throttled embedding
request also starts one short cooldown shared by the embedding workers.

## Processing logs

The worker keeps normal logs short. It logs the message start, source count,
page range, one result for each worker batch, page completion, final result,
refresh, and source-message deletion. The worker-batch result shows how many
keys were generated or reused, how many catalog keys already existed, how many
saved vectors were reused, how many vectors Bedrock created, and how many
occurrences were written.

A temporary operation logs a warning before waiting and retrying. If a later
attempt works, one information log confirms the successful attempt. If the
operation exhausts its configured attempts, the SQS boundary writes one final
error and does not delete the source message. The queue then applies its
configured receive retry and DLQ policy. Invalid data follows the same single
final-error path without repeating an operation that waiting cannot repair.

Unexpected runtime or programming errors are not treated as temporary service
failures. They are allowed to surface so New Relic shows the real code defect.
OpenSearch ID lookups, empty writes, and successful low-level writes do not
produce extra logs. Shared embedding cooldown waits use `DEBUG` because many
workers can wait at the same time.

## Database tables

The worker uses:

- `sentences_vector`
- `text_similarity_input`, joined by `tsp_id` to read `application_id`,
  `a_number`, `content`, `source_type`, `analysis_group`, `section_name`, and
  `is_current`
- `embeddings`, joined by `global_id`
- `tracer_sentence`, loaded by `tracer_sentence_id`

The historical page query does not load `sentences_vector.embedding_list`.
That legacy column is not used by this flow and may contain NumPy arrays that
cannot be loaded through SQLAlchemy `MutableList`. Saved Titan vectors are read
only from the separate `embeddings` table.

`a_number` remains in PostgreSQL. The occurrence index stores the numeric
`applicationId` and `tspId`. It reads `applicationId`, `analysisGroup`,
`sectionName`, and `isCurrent` from the related `text_similarity_input` row and
does not store `a_number`.

If production column names differ, update `ta_db_connection/atables.py`.

## OpenSearch aliases

The worker writes to:

- `sentence_occurrences`
- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`
- `sentence_semantic_training`
- `tracer_form_language_catalog`

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

`make dev` runs the worker in Docker. `make local` runs it without Docker.
There is no HTTP port, health endpoint, or Swagger page.

The tests cover SQS routing and the live, historical, training, and tracer
handler behavior. Database sessions, OpenSearch, and SQS transports are
replaced only at their external boundaries.

## Portable copy

Copy `generate_project.py` to another machine and run:

```bash
python3 generate_project.py /path/to/parent-folder
```

It creates `/path/to/parent-folder/doc2vec-completed` with all three handlers
and a safe `.env` copied from `.env.example`.
