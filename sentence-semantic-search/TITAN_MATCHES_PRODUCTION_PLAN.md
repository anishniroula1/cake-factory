# Titan Sentence Matching Production Plan

## Purpose

This document describes the production design implemented by these projects:

| Project | Responsibility |
|---|---|
| `collegue-doc2vec` | Prepare sentences, create or reuse vectors, classify sentences, and write OpenSearch documents. |
| `sentence-similarity-service` | Search eligible sentence vectors and save bidirectional sentence relationships. |
| `new_case_sim` | Convert sentence relationships into bidirectional application summaries and optional TAH events. |
| `new_api_transform` | Build and publish the final Titan application payload. |
| `doc-delete-service` | Remove a document from OpenSearch and PostgreSQL without deleting shared catalog vectors. |

## High-level runtime flow

```text
Document SQS
  -> Doc2Vec
     -> tsp.sentences_vector
     -> sentence_occurrences
     -> Asylee or Refugee sentence catalog
     -> tracer catalog when classification is missing
  -> Sentence Similarity SQS
  -> Sentence Similarity
     -> semantic search in the selected sentence catalog
     -> occurrence expansion in sentence_occurrences
     -> bidirectional tsp.titan_matches
  -> Case Similarity SQS
  -> Case Similarity
     -> bidirectional tsp.titan_similar_cases
     -> optional TAH Kafka event
  -> API Transform SQS
  -> API Transform
     -> application payload with embedding_model="TITAN"
     -> API Kafka topic
     -> next SQS event
```

Document deletion is a separate scheduled path:

```text
Document Delete batch
  -> delete orphaned catalog vectors and document occurrences
  -> delete source and reverse tsp.titan_matches relationships
  -> optionally delete embeddings and sentences_vector rows
  -> mark text_similarity_input as DOC_DELETED
  -> Case Similarity Backfill SQS
  -> refresh affected case summaries
```

## System ownership

| Layer | Owns |
|---|---|
| PostgreSQL | Source documents, sentence occurrences, saved embeddings, sentence relationships, case summaries, Kafka audit state, and document state. |
| OpenSearch | Unique sentence vectors, occurrence lookup, tracer references, and IVF training vectors. |
| SQS | One-document handoff, retries, DLQ redrive, and historical batches entering Doc2Vec. |
| Kafka | TAH change events and transformed Titan application payloads. |

OpenSearch performs vector retrieval. PostgreSQL remains the durable source of
the relationships consumed by Case Similarity and API Transform.

## OpenSearch resources

| Alias | Stored data | Engine | Identity |
|---|---|---|---|
| `sentence_occurrences` | One location record for every sentence occurrence | No vector | `_id = globalId` |
| `sentence_semantic_catalog_asylee` | One vector per unique Asylee sentence key | Faiss IVF-SQ16 | `_id = sentenceKey` |
| `sentence_semantic_catalog_refugee` | One vector per unique Refugee sentence key | Faiss IVF-SQ16 | `_id = sentenceKey` |
| `tracer_form_language_catalog` | Tracer, form-language, and non-matcher references | Lucene HNSW | `_id = tracerId` |
| `sentence_semantic_training` | Representative 512-dimension vectors used to train IVF | Training index | `_id = sentenceKey` |

### Production vector design

- Titan embeddings contain 512 numeric values.
- The Asylee and Refugee catalogs use the same trained Faiss IVF-SQ16 cosine model.
- The physical catalogs are separate because the two analysis groups have very little sentence overlap and must not return cross-group candidates.
- The occurrence-index capacity baseline uses 18 primary shards and one replica.
- Size each group catalog from its own Asylee or Refugee volume. The earlier 18-shard estimate was for a combined catalog near 100 million vectors and should not automatically be applied to both split catalogs.
- The tracer catalog uses one primary shard because it contains only about 4,000 references.
- The training index uses six primary shards.
- IVF search currently uses `nprobes=64`.
- The production IVF design uses `nlist=16384` and a representative training set before the catalogs are created.

### How OpenSearch Faiss sentence similarity works

OpenSearch uses Faiss to find sentences whose Titan vectors are close to one
another. It does not compare sentence text word by word. The complete runtime
path is:

```text
Source sentence
  -> 512-value Titan embedding
  -> Faiss IVF-SQ16 search in the matching analysis-group catalog
  -> matching sentence keys
  -> sentence_occurrences lookup
  -> target global IDs, applications, and documents
  -> bidirectional titan_matches relationships
```

#### 1. Train the IVF model

Before the sentence catalogs are created, representative Titan embeddings are
loaded into `sentence_semantic_training`. Faiss uses those vectors to divide
the vector space into `nlist=16384` clusters. Sentences with similar vector
meaning are expected to land in nearby clusters.

This training happens once for the model. The Asylee and Refugee catalogs are
separate, but both can use the same trained model because their vectors have
the same 512-dimension Titan format.

#### 2. Store unique sentence vectors

Doc2Vec writes one catalog document per unique sentence key. Faiss assigns the
vector to its nearest IVF cluster, and SQ16 reduces the vector storage needed
inside the search index. Repeated sentence occurrences do not create repeated
catalog vectors; their document locations are stored separately in
`sentence_occurrences`.

#### 3. Search only nearby clusters

Sentence Similarity loads a source sentence's saved 512-value vector and
selects the Asylee or Refugee catalog. Faiss probes the nearest 64 clusters
(`nprobes=64`) instead of scanning every vector in the catalog. It then returns
the candidates that satisfy the configured score and catalog filters.

This is an approximate nearest-neighbor search. Increasing `nprobes` examines
more clusters and may improve recall, but it also increases search time and
OpenSearch work. Decreasing it is faster but can miss more valid candidates.

#### 4. Apply sentence eligibility and the similarity threshold

Source and target sentences must contain at least five words. Non-matchers,
tracers, and form-language sentences are excluded. Separate catalogs prevent
Asylee sentences from matching Refugee sentences.

OpenSearch converts cosine similarity into a positive `_score`:

```text
OpenSearch score = (1 + cosine similarity) / 2
cosine similarity = (2 * OpenSearch score) - 1
saved distance = 1 - cosine similarity
```

For example, a result with `_score=0.95` has cosine similarity `0.90`, or a 90%
match. The saved distance is `0.10`. The current 76% match threshold is sent to
OpenSearch as minimum score `0.88` and produces a maximum accepted distance of
`0.24`. A smaller distance is a better match, and distance `0.0` is exact.

#### 5. Expand a unique sentence into real occurrences

Faiss returns catalog sentence keys, not the final database relationships.
Sentence Similarity searches `sentence_occurrences` for those keys to obtain
the target `globalId`, `applicationId`, and `tspId`. It excludes occurrences
owned by the source application, then PostgreSQL saves each accepted
relationship in both directions in `tsp.titan_matches`.

Example: source global ID `100` finds sentence key `abc` at 90% similarity.
That key occurs as global IDs `200` and `300` in two other applications. The
service saves `100 -> 200`, `200 -> 100`, `100 -> 300`, and `300 -> 100`, each
with distance `0.10`.

### Occurrence document

`sentence_occurrences` stores the fields needed to expand a matching sentence
key into document occurrences:

```json
{
  "globalId": 1001,
  "applicationId": 10,
  "tspId": 2001,
  "sectionName": "affidavit",
  "sentIdLocal": 1,
  "sentenceKey": "64-character-sha256-key",
  "sourceType": "affidavit",
  "analysisGroup": "Asylee",
  "isCurrent": true,
  "createdAt": "timestamp",
  "updatedAt": "timestamp"
}
```

The occurrence index does not store sentence text or classification flags.
Those values remain in `tsp.sentences_vector` and the group catalog.

### Sentence catalog document

Each group catalog stores one document per unique sentence key:

```json
{
  "sentenceKey": "64-character-sha256-key",
  "sentenceContent": "The applicant worked at the hospital for five years.",
  "wordCount": 9,
  "sentenceContentVector": ["512 Titan values"],
  "isNonMatcher": false,
  "isTracer": false,
  "isFormLanguage": false,
  "createdAt": "timestamp",
  "updatedAt": "timestamp"
}
```

The text is required by the Bedrock ingest pipeline when OpenSearch must create
an embedding. The catalog does not store `analysisGroup`; the selected physical
catalog already establishes the group.

### Why occurrence and catalog indexes are separate

If the same sentence appears in 500 documents, OpenSearch stores one catalog
vector and 500 occurrence documents. The catalog avoids duplicate vector
storage, while the occurrence index preserves each `globalId`, `applicationId`,
and `tspId` needed by downstream services.

### Tracer classification

The tracer catalog stores `tracerId`, text, a 512-dimension vector,
`analysisGroup`, `isNonMatcher`, and `isFormLanguage`. It has no `isTracer`
field. A qualifying tracer reference with both stored flags set to false means
`is_tracer=true` for the sentence being classified.

Doc2Vec reads the nearest 100 tracer references from the same analysis group
and accepts every result at or above 70% cosine similarity. Multiple references
may cause more than one classification flag to become true.

## 1. Doc2Vec

### Message routing

| Message | Flow |
|---|---|
| `tsp_id` without a recognized reason | Incoming document flow |
| `reason=HISTORICAL` | Existing sentence backfill; `tsp_id` may contain one ID or a JSON list |
| `reason=TRAIN_MODEL` | IVF training-data flow |
| `reason=TRACER` with `tracer_sentence_id` | Tracer catalog flow |

Doc2Vec receives one SQS message at a time. A historical message can contain
many document IDs; completed document events are sent to the next queue in SQS
batches of at most ten.

### Incoming document flow

1. Load existing `sentences_vector` rows in 500-row pages. A retry reuses saved rows instead of splitting and inserting again.
2. When no rows exist, load the document from `tsp.text_similarity_input`.
3. Apply RRF section filtering to eligible Refugee referral documents.
4. Split the content with Syntok and clean empty values.
5. Calculate a normalized SHA-256 `sentence_key` and Unicode-aware word count from the sentence text.
6. Check the selected group catalog for existing keys.
7. Create missing catalog documents. OpenSearch creates the vector when no reusable vector is supplied.
8. Reuse complete catalog classification or search the tracer catalog when classification fields are missing.
9. Insert new `sentences_vector` rows in batches of 1,000. Flush each batch to obtain `global_id`; commit the complete document once.
10. Write one occurrence document for every saved sentence.
11. Refresh the selected catalog and occurrence aliases once after all writes finish.
12. Send `{source, tsp_id, sent_by}` to Sentence Similarity and delete the source SQS message.

### Historical flow

Historical sentences are already split. For each document, Doc2Vec:

1. Loads a page of up to 3,000 existing sentence rows.
2. Splits the page into 30-sentence worker jobs and runs up to 100 workers.
3. Reuses an existing sentence key or calculates a missing key.
4. Calculates the word count from `sentence_content`; it does not trust the legacy value.
5. Checks the selected catalog and creates missing unique documents.
6. Reuses `embedding_type=2` from `tsp.embeddings` when it is a valid 512-value vector; otherwise the ingest pipeline creates a replacement.
7. Reuses or calculates tracer classification.
8. Updates each sentence row once with key, word count, and classification.
9. Writes every occurrence record.

After every document in the received historical message finishes, Doc2Vec
refreshes the affected catalog aliases and `sentence_occurrences`, sends one
downstream message per successful document, logs failed IDs, and deletes the
source message only after the batch-level work succeeds.

### Training and tracer flows

- Training excludes sentences with fewer than five words and sentences already marked as non-matcher, tracer, or form language.
- Training reuses valid Titan embeddings and creates only missing vectors.
- The tracer handler writes the supplied tracer record to the HNSW reference catalog.
- Training and tracer writes do not refresh the runtime sentence indexes.

### Doc2Vec operating values

| Setting | Value |
|---|---:|
| OpenSearch bulk size | 1,000 documents |
| Bulk retry count | 30 retries after the original request |
| Historical worker job | 30 sentences |
| Historical worker count | 100 |
| Training worker count | 16 |
| Normal retry wait | 5 seconds |
| SQS visibility timeout | 1 hour |

Bulk retry statuses are `408`, `424`, `425`, `429`, `500`, `502`, `503`, and
`504`. Only failed retryable bulk documents are submitted again.

## 2. Sentence Similarity

Sentence Similarity no longer performs tracer classification. Doc2Vec has
already saved all three flags in the catalog and `sentences_vector`.

### Processing steps

1. Receive one `tsp_id` from SQS.
2. Check that at least one occurrence is searchable. If not, wait 30 seconds and check once more.
3. Load all document sentence rows from PostgreSQL in pages of 1,000.
4. Exclude sources with fewer than five words or any true classification flag.
5. Select the Asylee or Refugee catalog from the document's analysis group.
6. Load all source catalog vectors. A missing catalog record raises a retryable readiness error.
7. Use `retry_operation` for at most five catalog-load attempts with five seconds between attempts.
8. Group source work in 30-sentence progress batches and process with up to 60 worker threads.
9. Search the selected catalog at 76% cosine similarity and page every qualifying key.
10. Expand the keys through `sentence_occurrences`, restricted to the same analysis group and excluding the source application.
11. Collect all relationships, group them by source global ID, and save them after sentence searches finish.
12. Send the completed document to Case Similarity and delete the source message.

### Search rules

| Rule | Value |
|---|---:|
| Minimum source and target word count | 5 |
| Match similarity | 76% |
| OpenSearch minimum score | 0.88 |
| IVF `nprobes` | 64 |
| Catalog and occurrence page size | 1,000 |
| Source progress batch | 30 sentences |
| Worker threads | 60 |
| Source SQS visibility timeout | 2 hours |

The vector query excludes target catalog records where `isNonMatcher`,
`isTracer`, or `isFormLanguage` is true. The source application is excluded
during occurrence expansion, so a case never matches itself.

OpenSearch score conversion is:

```text
cosine_similarity = (2 * OpenSearch score) - 1
distance = 1 - cosine_similarity

score 1.00 -> similarity 100% -> distance 0.00
score 0.95 -> similarity  90% -> distance 0.10
score 0.88 -> similarity  76% -> distance 0.24
```

### `tsp.titan_matches`

One owner sentence can have up to 16 bucket rows:

| Column | Purpose |
|---|---|
| `global_id` | Sentence occurrence that owns the match object |
| `tsp_id` | Owner document |
| `application_id` | Owner application |
| `match_bucket_number` | `abs(target_global_id) % 16` |
| `matches` | JSONB object `{target_global_id: distance}` |
| `match_count` | Generated count of JSONB keys |
| `verbatim` | Generated integer array of target IDs with distance `0.0` |
| `last_modified` | Latest bucket update time |

The primary key is `(global_id, match_bucket_number)`. Indexes on
`(tsp_id, global_id)` and `(application_id, global_id)` support downstream
document and application reads.

Example:

```json
{
  "global_id": 100,
  "match_bucket_number": 8,
  "matches": {
    "200": 0.10,
    "232": 0.0
  },
  "match_count": 2,
  "verbatim": [232]
}
```

`tsp.upsert_bidirectional_titan_matches` saves `100 -> 200` and `200 -> 100`
in the same transaction. It rejects self matches and same-application matches,
calculates deterministic buckets, takes ordered transaction advisory locks,
merges distances without deleting unrelated keys, and creates no row when no
match exists. Python sends at most 3,000 targets per source function call.

The current upsert adds or updates relationships. It does not remove a target
that is absent from a later semantic search. Document Delete handles explicit
document removal; any future reprocessing requirement that must remove stale
semantic relationships needs a separate reconciliation rule.

## 3. Case Similarity

Case Similarity does not call OpenSearch. It summarizes the sentence
relationships already stored in `tsp.titan_matches`.

### Processing steps

1. Receive one `tsp_id` from SQS.
2. Resolve `application_id`, `a_number`, and `analysis_group` from `tsp.text_similarity_input`.
3. Call `tsp.refresh_titan_similar_cases` for the source application.
4. Rebuild the source lists from current sentence relationships.
5. Add or remove the source application on affected target summaries.
6. Publish changed TAH values when live Kafka publishing is enabled.
7. Send the document to API Transform and delete the source message.

### Application rules

| Unique sentence-occurrence relationships | Saved membership |
|---:|---|
| 1 or more | `similar_application_ids` |
| 2 or more | Also `similar_case_ids` |
| 3 or more | Also `tah_case_ids`; therefore `is_tah=true` |

The refresh function takes ordered advisory locks and updates both directions.
If Application 10 no longer matches Application 20, it removes 20 from
Application 10 and 10 from Application 20 while preserving their relationships
with other applications. An application with no relationships has no stored
summary row.

### `tsp.titan_similar_cases`

The table stores `application_id`, `analysis_group`, three JSONB ID lists,
generated similar/TAH counts, generated `is_tah`, and `last_modified`. It does
not store `a_number`; that PII is resolved only when a downstream payload needs
it.

### TAH Kafka behavior

The topic is `rfs.ata.text_analytics.hit`. Live publication occurs only when
the current `is_tah` differs from the last Kafka-confirmed value in
`validation.kafka_ta_hit_lookup`, including both `false -> true` and
`true -> false` changes. Historical processing can disable publication while
still rebuilding case summaries.

One application may publish to multiple unique receipt numbers. A receipt
shared by multiple applications is skipped because it cannot identify one
application safely. Kafka acknowledgement is saved before the message is
considered complete.

Case Similarity uses a two-hour SQS visibility timeout and three bounded
attempts for retryable database, SQS, and Kafka boundaries.

## 4. API Transform

API Transform handles one `tsp_id` at a time and does not call OpenSearch. It
reads the completed PostgreSQL results and preserves the existing application
payload shape without the NER path.

### Processing steps

1. Resolve the current application from `tsp.text_similarity_input`.
2. Load application, confidence, applicant, address, document, OCR, and section data.
3. Load current application sentences from `tsp.sentences_vector`.
4. Merge all 16 `tsp.titan_matches` buckets for each source sentence.
5. Order each sentence's matches by distance from smallest to largest.
6. Return up to the configured `SENTENCE_MATCH_LIMIT`, which defaults to 3,000 matches per source sentence.
7. Load application-level results from `tsp.titan_similar_cases`.
8. Map similar application IDs to A-numbers only where the existing payload requires them.
9. Load the newest public template for Asylee applications.
10. Build one or more Kafka-safe events with `embedding_model="TITAN"`.
11. Publish to `rfs.ata.data_migration.api.events` and wait for every acknowledgement.
12. Save Kafka audit/status data, send the next SQS event, and delete the source message.

### API match limit

The match limit is applied independently to every source sentence. API
Transform reads all 16 bucket rows first, orders the combined relationships by
ascending distance, and then keeps the first `SENTENCE_MATCH_LIMIT` results.
When two distances are equal, target `global_id` gives the results a stable
order.

With the default setting, one sentence sends at most 3,000 matches. Its
`matches_count` reflects the number included in the payload. This limit changes
only the outbound API payload: it does not delete relationships from
`tsp.titan_matches` and does not change the complete relationship set used by
Case Similarity.

If the current `text_similarity_input` record is missing, the handler returns a
successful zero-application result. When the receiving queue URL contains
`backfill`, API Transform skips the next SQS event but still completes its
current source message.

The first Kafka chunk uses `op="c"`; later chunks use `op="u"`. Every event
contains the Titan embedding-model marker. API Transform uses a five-minute SQS
visibility timeout.

## 5. Document Delete

Document Delete is a scheduled batch job, not a continuous SQS listener. The
calling job supplies a list of `tsp_id` values and can run the established and
OpenSearch deletion paths in separate worker threads.

### Per-document deletion order

1. Load the document's records from `sentence_occurrences`.
2. Collect its unique sentence keys and analysis group.
3. Check which keys are still used by another document in the same analysis group.
4. Delete only orphaned vectors from the selected sentence catalog.
5. Delete every occurrence owned by the document.
6. Call `tsp.delete_titan_matches_for_tsp` to remove source buckets and reverse JSONB references.
7. When database cleanup is enabled, delete `tsp.embeddings` and `tsp.sentences_vector` rows.
8. Keep `tsp.text_similarity_input` and set `document_state='DOC_DELETED'`.
9. Send the deleted `tsp_id` to the Case Similarity Backfill queue.

Catalog vectors are processed before occurrences. If the catalog operation
fails, the occurrence references remain available for a safe retry. A catalog
key shared by another document in the same analysis group is never deleted.

The Titan deletion function returns affected application IDs, affected
document IDs, whether the source existed, and deletion counts. It uses ordered
transaction advisory locks, deletes all source bucket rows, removes the deleted
global IDs from reverse match objects, and removes reverse bucket rows that
become empty. If the document has no Titan rows, it returns immediately.

One failed document is logged and does not stop later IDs in the same deletion
batch. Failed IDs are listed for later reprocessing.

## Core PostgreSQL tables

| Table | Main use |
|---|---|
| `tsp.text_similarity_input` | Document text, application ownership, analysis group, section, source type, and document state |
| `tsp.sentences_vector` | Sentence text, key, word count, classifications, and document relationship |
| `tsp.embeddings` | Reusable Titan embedding where `embedding_type=2` |
| `tsp.titan_matches` | Bidirectional sentence relationships in 16 JSONB buckets |
| `tsp.titan_similar_cases` | Bidirectional application summaries and generated TAH state |
| `landing.application_id_lookup` | Application payload data and application-to-A-number mapping |
| `landing.global_document_registry` | Receipt numbers used by TAH Kafka events |
| `validation.kafka_ta_hit_lookup` | Last Kafka-confirmed TAH value and delivery details |
| `api.kafka_events` | Confirmed API Transform Kafka audits |
| `api.transform_status` | Last confirmed API transmission time |

## Message and failure behavior

The normal downstream message is:

```json
{
  "source": "P2",
  "tsp_id": "8205",
  "sent_by": "service-name"
}
```

- A source SQS message is deleted only after required data writes and downstream delivery complete.
- Expected failures leave the message available for SQS redelivery and eventual DLQ alerting.
- Retry wrappers repeat only external boundaries where possible.
- Database transactions commit a complete logical write or roll it back.
- Stable OpenSearch document IDs and PostgreSQL upserts make retries idempotent.
- Logs include operational identifiers and counts but must not include sentence content, A-numbers, SQL parameters, or complete external response bodies.
