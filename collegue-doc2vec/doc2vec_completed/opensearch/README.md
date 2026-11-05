# Doc2Vec OpenSearch Flow

This worker prepares sentence data for the OpenSearch-based similarity flow. It
receives one SQS message at a time and routes the message to the incoming,
historical, tracer, or IVF training handler.

The service is not an API and does not expose HTTP endpoints. PostgreSQL is the
source of document and sentence data. OpenSearch stores unique sentence vectors,
sentence occurrences, tracer references, and IVF training documents.

## Complete message flow

```text
SQS message
  -> validate message fields
  -> select one handler
  -> read PostgreSQL data
  -> write the required OpenSearch index or indexes
  -> refresh the written aliases
  -> send the next SQS event when that flow requires one
  -> delete the source message
```

The source message is deleted only after its required processing succeeds. A
failed message remains in SQS for the queue retry and DLQ policy.

## Supported messages

An incoming document has no `reason`:

```json
{
  "tsp_id": 1040
}
```

Historical sentence backfill:

```json
{
  "reason": "HISTORICAL",
  "tsp_id": "[1040, 1041]"
}
```

IVF training data:

```json
{
  "reason": "TRAIN_MODEL",
  "tsp_id": 1040
}
```

Tracer, form-language, and non-matcher references:

```json
{
  "reason": "TRACER",
  "tracer_sentence_id": 10
}
```

`tsp_id` remains an integer or string exactly as supplied by the upstream
service. Historical `tsp_id` can contain one ID or a JSON array string and is
always converted to a list before processing. `tracer_sentence_id` can be an integer or numeric string and
is converted to an integer before processing. `HISTORICAL`, `TRAIN_MODEL`, and
`TRACER` select their named handlers. Every other message follows the incoming
document flow and must contain `tsp_id`.

## Incoming document flow

The incoming flow supports both a new document and a retry after PostgreSQL was
already completed.

1. Read existing `sentences_vector` records in 500-record pages.
2. If records exist, reuse them instead of splitting and inserting the document
   again. Existing sentence keys are reused and only missing keys are generated.
3. If no sentence records exist, load the required `text_similarity_input`
   fields by primary-key `tsp_id`.
4. For eligible Refugee referral documents, remove RRF sections 1 through 3
   and section 5 while keeping section 4 and section 6.
5. Split the remaining document content with Syntok. Temporary masks improve known
   numbering and abbreviation cases, but the original sentence text is saved.
6. Remove empty sentence values and assign `sent_id_local` starting at 1.
7. Generate a normalized SHA-256 `sentence_key` and a meaningful Unicode word
   count for every sentence.
8. Process the prepared records in 30-sentence catalog jobs with up to 100
   workers before inserting them into `sentences_vector`.
9. Select the Asylee or Refugee catalog using the document's `analysis_group`.
10. Use `mget` to skip sentence keys already in that catalog. Missing keys are
    sent through the catalog's Bedrock embedding pipeline.
11. Read each catalog vector with real-time `mget`. Reuse a complete saved
    classification or search the closest tracer references when classification
    fields are missing.
12. Save new classifications on their catalog keys.
13. Insert the sentences into `sentences_vector` in database batches of 1000.
    The insert already contains the sentence key, calculated word count, and
    all three classification flags. Each batch is flushed for `global_id`, and
    the complete document is committed once.
14. Save an occurrence document for every sentence using its assigned `global_id` as its
    OpenSearch document ID.
15. Refresh the selected catalog and occurrence aliases once after every worker
    finishes.
16. Send the existing state-machine result and delete the source SQS message.

The completion message is:

```json
{
  "source": "P2",
  "tsp_id": 1040,
  "sent_by": "doc2vec",
  "success": true,
  "reason": ""
}
```

No sentence or OpenSearch records are created when the document contains no
usable sentence text. That completion message uses `success=false` and
`reason="no_content"` so the next state-machine step can handle it explicitly.

## Historical flow

Historical documents already have sentence records, so this flow never splits
the original document again. One source SQS message can contain several
document IDs.

1. Convert the scalar or JSON-list `tsp_id` value into a list and process each document in order.
2. Count the document's `sentences_vector` records.
3. Load up to 3000 records per page in `global_id` order.
4. Split each page into 30-sentence jobs and run up to 100 jobs concurrently.
5. Reuse saved sentence keys, generate missing keys, and recalculate
   `sentences_vector.word_count` from the current sentence content.
6. Load existing Titan vectors from `embeddings` using `embedding_type=2`.
7. Check the selected catalog by sentence key. A missing catalog key reuses its
   saved vector when available; otherwise the Bedrock pipeline creates it.
8. Read each catalog vector and classify unfinished keys against the tracer
   catalog. New classifications are cached on their catalog keys.
9. Update each `sentences_vector` row once with its sentence key, recalculated
   word count, and all three classification flags.
10. Save every sentence occurrence.
11. After every document finishes, refresh each affected catalog and the
   occurrence alias once.
12. Use the configured outbound queue URL with the shared SQS client.
    Convert every downstream message to a JSON string and send at most ten
    messages in each SQS batch request.
13. Delete the source message only after every downstream message is accepted.

An individual document failure does not stop the remaining IDs in the source
message. Successful documents are refreshed and sent downstream. Failed
`tsp_id` values are written together in one error log for manual reprocessing,
and the source message is deleted after the successful results are sent.

The historical flow fails safely when the next queue is not configured. It does
not report success and does not allow the source message to be deleted.

## IVF training flow

The training flow creates representative vector data for training the Faiss IVF
model. It does not update `sentences_vector.sentence_key`.

1. Read classification-eligible sentence records in pages of 1000.
2. Exclude records marked `is_non_matcher`, `is_tracer`, or
   `is_form_language`.
3. Calculate the meaningful word count from current sentence text and exclude
   sentences containing fewer than five words.
4. Generate sentence keys in memory and keep one document per unique key.
5. Reuse vectors from `embeddings` when `embedding_type=2`.
6. Check the training index and skip keys already present.
7. Process 30-document jobs with up to 16 workers. Saved vectors bypass the
   embedding pipeline; missing vectors use the Bedrock pipeline.
8. Refresh the training alias when new documents were written.
9. Return success so the SQS processor can delete the source message.

## Tracer reference flow

1. Read one `tracer_sentence_id` from the SQS message.
2. Load that record from `tracer_sentence`.
3. Build one document containing `tracerId`, sentence content, analysis group,
   `isNonMatcher`, and `isFormLanguage`.
4. Reuse a saved embedding when present. List, NumPy-like, JSON-byte, packed
   32-bit float, and packed 64-bit float values are supported.
5. Let the Bedrock pipeline create the vector when the record has no saved
   embedding.
6. Use `tracerId` as the stable OpenSearch document ID.
7. Refresh the tracer alias and return success.

A missing tracer ID is treated as permanent bad input. The message is logged
once and remains available for the configured SQS and DLQ handling.

## OpenSearch documents

The Asylee and Refugee catalogs contain one document per unique sentence key:

```json
{
  "sentenceKey": "64-character-sha256-key",
  "sentenceContent": "The applicant returned home after the interview.",
  "wordCount": 8,
  "isNonMatcher": false,
  "isTracer": false,
  "isFormLanguage": false,
  "sentenceContentVector": [0.01, 0.02],
  "createdAt": "2026-10-30T12:00:00+00:00",
  "updatedAt": "2026-10-30T12:00:00+00:00"
}
```

The occurrence index contains identity and document-location fields. Sentence
content and vectors are intentionally kept in the catalog:

```json
{
  "globalId": 5001,
  "applicationId": 1001,
  "tspId": 1040,
  "sectionName": "affidavit",
  "sentIdLocal": 12,
  "sentenceKey": "64-character-sha256-key",
  "sourceType": "affidavit",
  "analysisGroup": "Asylee",
  "isCurrent": true
}
```

The worker writes these aliases:

- `sentence_occurrences`
- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`
- `sentence_semantic_training`
- `tracer_form_language_catalog`

## Word counting and sentence keys

Sentence keys use normalized Unicode text, lowercase comparison, collapsed
whitespace, and SHA-256. Equivalent casing and spacing therefore produce the
same key.

Word count uses Unicode letters, permits apostrophes and hyphens only inside a
word, and ignores standalone numbers and punctuation. Three or more identical
consecutive words count once. For example:

| Sentence | Word count |
|---|---:|
| `The applicant moved to New York.` | 6 |
| `Yes yes yes yes yes` | 1 |
| `The man said the man escaped` | 6 |
| `La policía me detuvo durante tres días` | 7 |

Short sentences are still stored in PostgreSQL, the occurrence index, and the
selected catalog. Doc2Vec stores the recalculated count in both
`sentences_vector` and the catalog. Sentence Similarity filters the database
query to load only rows with at least five words. The training flow also
excludes shorter sentences from IVF training.

## Transactions, retries, and errors

- New document sentences use one PostgreSQL transaction. Flushes assign IDs;
  only the final commit makes all sentence records permanent.
- Historical key and word-count updates use small independent worker
  transactions. Classification updates use another small transaction after the
  catalog vector is available.
- Normal database calls, OpenSearch lookups and refreshes, and downstream SQS
  sends use up to three total attempts.
- OpenSearch bulk writes use the first request plus 15 retries because the
  embedding pipeline can throttle while creating vectors. A retry resends the
  same batch, and stable document IDs make that operation safe.
- Retryable status codes are `408`, `424`, `425`, `429`, `500`, `502`, `503`,
  and `504`.
- A throttled embedding request starts a short shared cooldown before another
  embedding write begins.
- Permanent mapping, vector, analysis-group, or input errors are not repeated.
- Database errors are logged by safe exception type. SQL values, sentence
  content, A-numbers, credentials, and response bodies are not logged.
- Unexpected programming errors are allowed to surface so the pod and New
  Relic show the real defect.

All OpenSearch IDs are stable. Retrying an index operation updates the same
document instead of creating a duplicate.

Source-message deletion is attempted once after processing succeeds. If that
delete fails, the error is logged and SQS can deliver the source message again.
The stable database and OpenSearch identities make the repeated processing
safe.

## Configuration

PostgreSQL and the source queue value come from Vault:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
awsReceiveSqsQueueEndpoint: doc2vec-queue-url
```

Dynamic database credentials can use `pgDynamicUser` and
`pgDynamicPassword`. Outside the local environment, `VAULT_YAML_PATH` is
required.

Environment values used by this flow are:

```text
ENVIRONMENT=local
VAULT_YAML_PATH=example.localVaultSecret.yaml
AWS_DEFAULT_REGION=us-east-1
AWS_LOCAL_ENDPOINT_URL=http://localhost:4566
OPENSEARCH_HOST=search-domain.us-east-1.es.amazonaws.com
OPENSEARCH_IAM_ACCESS_ROLE=arn:aws:iam::123456789012:role/opensearch-role
AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT=https://sqs.example.com/next.fifo
```

The shared `OpenSearchClient` uses refreshable assumed-role credentials when an
IAM role is supplied. Long-running SQS workers therefore do not keep an expired
one-hour session token.

## Project layout

```text
doc2vec_completed/opensearch/
  db_store/                 PostgreSQL reads and writes by table responsibility
  handlers/                 Incoming, historical, training, and tracer flows
  opensearch_store/         One store for each OpenSearch index type
  config.py                 Vault and environment configuration
  constants.py              Fixed aliases, batch sizes, workers, and retry values
  contracts.py              SQS, result, and OpenSearch document types
  dependencies.py           Shared database, SQS, OpenSearch, and handler setup
  opensearch_helper.py      Bulk writes, mget, cooldown, refresh, and validation
  rrf_filterer.py           Refugee referral section filtering before splitting
  sentence_batch_processor.py Shared catalog and occurrence batch preparation
  tracer_filtering_service.py Reusable catalog classification orchestration
  sentence_splitter.py      Syntok splitting while preserving original text
  sqs_processor.py          Message validation, routing, and source deletion
  text.py                   Sentence normalization, keys, and word counting
tests/opensearch/            OpenSearch flow tests
```

## Run locally

Running the Python worker locally is faster than rebuilding the application
container after every code change. The configured source and next SQS queues
must already exist.

```bash
make local
```

## Run tests

Run the Doc2Vec OpenSearch tests:

```bash
make ostest
```

