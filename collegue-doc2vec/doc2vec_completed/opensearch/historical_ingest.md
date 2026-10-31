# Doc2Vec Historical Sentence Ingestion

This is a standalone SQS worker for historical sentence backfill. It reads
already-split sentences from PostgreSQL and writes them to the sentence
catalog selected by `analysis_group` and the occurrence alias in AWS
OpenSearch.

It is not an API or FastAPI application. It processes one SQS message at a
time, and all historical processing logic stays inside
`doc2vec_completed/opensearch/handlers/historical_handler.py`.

All OpenSearch flow code lives under `doc2vec_completed/opensearch`. All tests
for this flow live under `tests/opensearch`.

`dependencies.py` creates the configuration, database connection, OpenSearch
helper, SQS helper, and historical handler. The SQS processor receives only
this one dependency object.
`doc2vec_completed.opensearch.main.run_opensearch_flow(custom_logger=...)` can
still receive the team's logger, while `custom_logger.py` remains the
standalone default.

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
SqsMessage -> TspId -> list[SentencesVector] -> HistoricalResult
```

OpenSearch catalog and occurrence documents also have separate named types, so
the expected fields are visible before a document is sent for bulk indexing.

## Historical flow

1. Count all `sentences_vector` rows for the `tsp_id`.
2. Read up to 3,000 rows at a time in `global_id` order.
3. Generate and save the SHA-256 `sentence_key` for every row.
4. Split the page into 30-sentence jobs and run up to 100 workers.
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

## Database tables

### Historical Handler uses
- `sentences_vector`
- `text_similarity_input`, joined by `tsp_id` to read `application_id`,
  `analysis_group`, `section_name`, and `is_current`
- `embeddings`, joined by `global_id`

`a_number` remains in PostgreSQL. The occurrence index stores the numeric
`applicationId` and `tspId`. It reads `applicationId`, `analysisGroup`,
`sectionName`, and `isCurrent` from the related `text_similarity_input` row and
does not store `a_number`.

## OpenSearch aliases

The worker writes to:

- `sentence_semantic_catalog_asylee`
- `sentence_semantic_catalog_refugee`

Both catalogs use the same trained 512-dimension Faiss IVF-SQ16 model. Their
default Bedrock pipeline creates a vector when the field is missing. A saved
vector is sent with `pipeline=_none`, which avoids another Bedrock call. The
same sentence key may correctly exist once in each catalog.

## Run

Copy `example.env` to `.env`, set `VAULT_YAML_PATH`, and fill in the AWS,
OpenSearch, and Sentence Similarity queue values.

Copy `eample.localValutSecret.yaml` to `localValutSecret.yaml` and fill the db credential and local sqs url

### Run in this order after opening Docker
```bash
make localstack
make local
make sos
```

`make localstack` run the localstack and stat the local AWS SQS
`make local` runs the code in local.
open new terminal and run `make sos`: this send the sample sqs message from makefile
`make purge` removes all the sentence from localstack
