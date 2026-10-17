# Titan Sentence Matching Production Plan

## Purpose

This is the current production plan for the Titan sentence flow. It covers
DOC2Vec, Sentence Similarity, Case Similarity, and API Transform.

The design uses OpenSearch for vector creation and semantic retrieval, and
PostgreSQL for the final sentence and case relationships consumed by the rest
of the application.

## Final service flow

```text
Document message
    -> DOC2Vec
       -> create or reuse sentence_key
       -> reuse an existing Titan embedding when available
       -> write every sentence to sentence_occurrences
       -> write each unique key to its analysis-group catalog
       -> refresh the indexes once after all document writes
       -> send tsp_id to Sentence Similarity

    -> Sentence Similarity
       -> load the document sentences in pages of 30
       -> process as many as 30 source sentences concurrently
       -> classify each source against the tracer catalog
       -> skip normal matching when any exclusion flag is true
       -> search the Asylee or Refugee catalog at 76% similarity
       -> expand matching keys through sentence_occurrences
       -> classify candidate keys before saving a relationship
       -> save both directions in 16-bucket titan_matches rows
       -> send tsp_id to Case Similarity

    -> Case Similarity
       -> resolve application_id and analysis_group from tsp_id
       -> rebuild the source application's case summary from titan_matches
       -> update the reverse membership on affected applications
       -> publish changed TAH values to Kafka when the live-flow toggle is on
       -> send tsp_id to API Transform

    -> API Transform
       -> read titan_matches and titan_similar_cases
       -> build the existing response shape without NER for this flow
       -> limit each source sentence to 3000 returned matches
       -> publish the transformed result and complete the document state
```

## OpenSearch resources

There are four runtime indexes and one temporary training index.

| Resource | Purpose | Vector engine |
|---|---|---|
| `sentence_occurrences` | Every sentence occurrence and its document location | None |
| `sentence_semantic_catalog_asylee` | One vector per unique Asylee sentence key | Faiss IVF-SQ16 |
| `sentence_semantic_catalog_refugee` | One vector per unique Refugee sentence key | Faiss IVF-SQ16 |
| `tracer_form_language_catalog` | About 4000 tracer, form-language, and non-matcher references | Lucene HNSW |
| `sentence_semantic_training` | Representative vectors used to train the shared IVF model | Untrained 512-dimension vectors |

The Asylee and Refugee catalogs use the same trained 512-dimension IVF-SQ16
model. They are separate because only about 33,000 sentences overlap across
the two groups out of roughly 100 million sentences. Separate catalogs avoid
cross-group candidates and allow a sentence key to have a different saved
classification in each group.

### Sentence occurrence mapping

The OpenSearch `_id` is the integer `globalId`.

```http
PUT /sentence_occurrences-v1
{
  "settings": {
    "number_of_shards": 18,
    "number_of_replicas": 1
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "globalId": {"type": "long"},
      "applicationId": {"type": "long"},
      "tspId": {"type": "long"},
      "sectionName": {"type": "keyword"},
      "sentIdLocal": {"type": "long"},
      "sentenceKey": {"type": "keyword"},
      "sourceType": {"type": "keyword"},
      "analysisGroup": {"type": "keyword"},
      "isCurrent": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

The occurrence index intentionally does not store `sentenceContent`,
`isNonMatcher`, `isTracer`, or `isFormLanguage`. It stores only the identity,
location, sentence key, and document fields needed to expand a matching key
into occurrences.

The unique sentence text remains in the selected sentence catalog because the
Bedrock ingest pipeline needs it to create the vector. PostgreSQL
`sentences_vector` remains the source for occurrence-level sentence text and
classification fields.

### Asylee sentence catalog mapping

```http
PUT /sentence_semantic_catalog_asylee-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": "<asylee catalog shards>",
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {"type": "keyword"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<trained IVF-SQ16 model ID>"
      },
      "isNonMatcher": {"type": "boolean"},
      "isTracer": {"type": "boolean"},
      "isFormLanguage": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

### Refugee sentence catalog mapping

```http
PUT /sentence_semantic_catalog_refugee-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": "<refugee catalog shards>",
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {"type": "keyword"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<trained IVF-SQ16 model ID>"
      },
      "isNonMatcher": {"type": "boolean"},
      "isTracer": {"type": "boolean"},
      "isFormLanguage": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

The catalog document `_id` is the 64-character `sentenceKey`. The catalog does
not need `analysisGroup`; selecting the physical catalog establishes the
group.

### Why occurrences and catalogs both exist

Assume these two sentences occur in four documents:

| Text | Group | Application | `globalId` | `sentenceKey` |
|---|---|---:|---:|---|
| I worked at the hospital for five years. | Asylee | 10 | 1001 | `KEY-A` |
| I worked at the hospital for five years. | Asylee | 25 | 8507 | `KEY-A` |
| My family left the village after the attack. | Refugee | 10 | 1002 | `KEY-B` |
| My family left the village after the attack. | Refugee | 44 | 9014 | `KEY-B` |

The occurrence index has four documents because there are four sentence
locations. The Asylee catalog has one `KEY-A` vector, and the Refugee catalog
has one `KEY-B` vector. The catalog prevents duplicate vectors while the
occurrence index preserves every `globalId`, `applicationId`, and `tspId`.

### Tracer and form-language catalog mapping

```http
PUT /tracer_form_language_catalog-v1
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "tracerId": {"type": "integer"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "dimension": 512,
        "space_type": "cosinesimil",
        "method": {
          "name": "hnsw",
          "engine": "lucene"
        }
      },
      "analysisGroup": {"type": "keyword"},
      "isNonMatcher": {"type": "boolean"},
      "isFormLanguage": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

The OpenSearch `_id` and `tracerId` are the integer
`tracer_sentence_id`. This index does not have an `isTracer` field. A reference
with both stored flags set to false represents a tracer sentence.

Lucene HNSW is appropriate here because the catalog has only about 4000
vectors. It does not need a trained Faiss model or IVF buckets.

### Training index mapping

```http
PUT /sentence_semantic_training-v1
{
  "settings": {
    "number_of_shards": 6,
    "number_of_replicas": 1,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {"type": "keyword"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "dimension": 512
      },
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

Training data excludes rows already classified as non-matcher, tracer, or
form language. The handler reuses `embedding_type = 2` from the `embeddings`
table when present and creates only missing vectors. It generates a stable key
for deduplication but does not save that training-only key back to
`sentences_vector`.

After representative Asylee and Refugee vectors are loaded, train one
512-dimension Faiss IVF-SQ16 cosine model. Both runtime catalogs must wait
until its state is `created` before their mappings reference its model ID.

### Aliases

```http
POST /_aliases
{
  "actions": [
    {"add": {"index": "sentence_occurrences-v1", "alias": "sentence_occurrences"}},
    {"add": {"index": "sentence_semantic_catalog_asylee-ivf-sq16-v1", "alias": "sentence_semantic_catalog_asylee"}},
    {"add": {"index": "sentence_semantic_catalog_refugee-ivf-sq16-v1", "alias": "sentence_semantic_catalog_refugee"}},
    {"add": {"index": "tracer_form_language_catalog-v1", "alias": "tracer_form_language_catalog"}},
    {"add": {"index": "sentence_semantic_training-v1", "alias": "sentence_semantic_training"}}
  ]
}
```

## DOC2Vec

### Message routes

| Message | Handler | Behavior |
|---|---|---|
| Normal live DOC2Vec message | Existing live DOC2Vec flow | Split the source document and insert new sentence rows |
| `reason=HISTORICAL` | OpenSearch backfill processor | Process existing `sentences_vector` rows without splitting again |
| `reason=TRAIN_MODEL` | OpenSearch backfill processor | Load representative rows into the training index |
| `reason=TRACER` | OpenSearch backfill processor | Load `tracer_sentence` rows into the tracer catalog |

The standalone OpenSearch backfill processor currently supports the three
explicit reasons shown above. The normal live route belongs in the existing
DOC2Vec service behind the OpenSearch feature toggle. Each processor receives
one source SQS message at a time and deletes it only after all required work
finishes.

### Stable sentence identity

DOC2Vec creates `sentence_key` by normalizing the text and hashing it with
SHA-256. If an existing row already has a key, that value is reused.

The key serves two purposes:

1. It is the catalog document `_id`, so repeated text does not create another
   vector in the same analysis group.
2. It prepares the system for a possible future key-level relationship model.

### Live flow

The live route loads document text from `text_similarity_input`, calls the
existing sentence splitter, and inserts the new `sentences_vector` rows. It
receives the generated `global_id` values after each database batch commits.

Document-level values come from `text_similarity_input`:

- `application_id`
- `analysis_group`
- `section_name`
- `is_current`

All four OpenSearch identity fields use numbers where applicable:
`globalId`, `applicationId`, and `tspId` are integers.

### Historical flow

Historical sentences are already split. The route:

1. Counts existing rows for the incoming `tsp_id`.
2. Loads up to 3000 rows at a time.
3. Splits that page into worker batches of 30.
4. Uses up to 100 workers.
5. Reuses an existing `sentence_key`, or creates and saves it inside the
   worker when missing.
6. Writes occurrences and missing catalog documents.

### Embedding reuse

For a missing catalog key, DOC2Vec checks `embeddings` with the sentence
`global_id` and `embedding_type = 2`.

```text
Valid 512-value embedding exists
    -> send that vector in the catalog document

No valid saved embedding exists
    -> omit the vector
    -> the Bedrock ingest pipeline creates it from sentenceContent
```

If the catalog key already exists, no embedding lookup or Titan call is
needed.

### OpenSearch writes and retries

| Setting | Current value |
|---|---:|
| OpenSearch bulk size | 1000 documents |
| Historical worker batch | 30 sentences |
| Historical worker count | 100 |
| Training worker count | 16 |
| Normal retry wait | 5 seconds |
| Bulk retry count | 15 retries after the original attempt |

Bulk retries apply to statuses `408`, `424`, `425`, `429`, `500`, `502`,
`503`, and `504`. Only failed retryable documents are submitted again. Stable
`globalId` and `sentenceKey` values make retrying idempotent.

After all catalog and occurrence writes for the document finish, DOC2Vec
explicitly refreshes the selected group catalog and `sentence_occurrences`
once. It does not use `refresh=wait_for` on every bulk request because that
would reduce ingestion throughput.

## Sentence Similarity

### Readiness and source loading

Sentence Similarity receives one `tsp_id`. It first checks whether at least one
occurrence is searchable. If not, it waits 30 seconds and checks once more.
The extra check covers the small period between DOC2Vec publishing the message
and OpenSearch exposing a refresh. If the second check is empty, processing
stops and the SQS message is not deleted.

The service reads 30 source rows from `sentences_vector` at a time and uses a
30-worker thread pool. Each worker handles one source sentence.

The source vectors are loaded from the analysis-group catalog by
`sentenceKey`. Vector values are read through `docvalue_fields` instead of the
normal `_source`, which avoids expensive vector source loading.

### Tracer classification

The tracer query:

- uses the source's saved 512-value vector;
- filters by `analysisGroup`;
- retrieves the nearest 100 references;
- accepts every returned hit at or above 70% cosine similarity.

All accepted hits contribute to the final classification:

| Qualifying reference | Result |
|---|---|
| `isNonMatcher=true` | `is_non_matcher=true` |
| `isFormLanguage=true` | `is_form_language=true` |
| Both fields false | `is_tracer=true` |

Different qualifying hits may set different flags, so all three final flags
can be true. If any final flag is true, the source does not search the normal
sentence catalog and does not create a Titan relationship.

The final flags are saved on the selected catalog key and copied to the
matching `sentences_vector` occurrences. The current database update changes
only:

```text
is_non_matcher, is_tracer, is_form_language, last_modified
```

It does not update `closest_non_matcher` or `closest_nm_score`.

Current cache behavior reuses a saved classification only when all three
flags are false. A saved excluded key is checked against the tracer catalog
again. This is the implemented behavior and should remain explicit in testing.

### Semantic search

An eligible source searches only its selected catalog:

| Setting | Current value |
|---|---:|
| Similarity threshold | 76% |
| OpenSearch radial minimum score | 0.88 |
| IVF `nprobes` | 64 |
| Composite page size | 1000 keys |

OpenSearch cosine score is not the similarity percentage. The conversion is:

```text
minimum_score = (1 + similarity) / 2
76% similarity -> minimum_score 0.88
```

The service pages through every key meeting the radial threshold. It is not a
fixed top-K sentence search. The source key is also included with distance
`0.0`, allowing exact occurrences to use the same expansion path.

Saved distance is:

```text
cosine_similarity = (2 * OpenSearch _score) - 1
distance = 1 - cosine_similarity
```

Examples:

| OpenSearch `_score` | Similarity | Saved distance |
|---:|---:|---:|
| 1.00 | 100% | 0.00 |
| 0.95 | 90% | 0.10 |
| 0.88 | 76% | 0.24 |

### Expand matching keys into occurrences

Catalog search returns unique sentence keys, not occurrence global IDs. The
service queries `sentence_occurrences` in pages using:

- a `terms` query for matching `sentenceKey` values;
- the source `analysisGroup`;
- exclusion of the source `applicationId`;
- `globalId` sorting and `search_after` pagination.

The terms/page batch size is 1000. All matching occurrences are returned; the
1000 value is a page size, not a total-result limit.

Candidate keys are also classified through the tracer catalog before a
relationship is saved. This protects bidirectional correctness: an eligible
source cannot create a reverse relationship on a tracer, form-language, or
non-matcher target.

### Sentence-level write behavior

For source global ID 100 matching global IDs 200 and 232:

```text
100 -> 200, 232
200 -> 100
232 -> 100
```

The service sends targets to PostgreSQL in batches of 500. One SQL function
call saves the forward and reverse sides in the same transaction. A deadlock
or another transient database failure retries only that failed match batch,
not the tracer and OpenSearch work already completed for the source.

No `titan_matches` row is created when a source has no matches.

## PostgreSQL `titan_matches`

### Table shape

One sentence can own up to 16 rows. The matching target's `global_id`
determines its bucket:

```text
match_bucket_number = abs(target_global_id) % 16
```

Example rows:

| `global_id` | `tsp_id` | `application_id` | bucket | `matches` | generated count | generated verbatim |
|---:|---:|---:|---:|---|---:|---|
| 100 | 10 | 1 | 8 | `{"200": 0.10, "232": 0.0}` | 2 | `{232}` |
| 200 | 20 | 2 | 4 | `{"100": 0.10}` | 1 | `{}` |
| 232 | 30 | 3 | 4 | `{"100": 0.0}` | 1 | `{100}` |

The primary key is `(global_id, match_bucket_number)`. Supporting indexes are
`(tsp_id, global_id)` and `(application_id, global_id)`.

`matches` is a JSONB object of `target_global_id: distance`. PostgreSQL
automatically generates:

- `match_count`: the number of keys in that bucket;
- `verbatim`: target global IDs whose distance is exactly `0.0`;
- `last_modified`: the latest write time.

To get one sentence's full total, read all rows for its `global_id`, merge the
JSONB objects, and add each row's `match_count`.

### Why 16 buckets are used

A sentence may have thousands of matches. Keeping them in one JSONB object
would rewrite and lock the entire value for every small change. Buckets keep
each JSONB value smaller, allow work on different buckets to proceed
independently, and let PostgreSQL TOAST compress large values outside the main
row page.

The tradeoff is up to 16 rows per owner sentence, but lookup remains fast
because the primary key starts with `global_id`.

### SQL function contract

The production function is:

```text
tsp.upsert_bidirectional_titan_matches(
    source_global_id,
    source_tsp_id,
    source_application_id,
    matches_jsonb
)
```

Input example:

```json
{
  "200": {"tsp_id": 20, "application_id": 2, "distance": 0.10},
  "232": {"tsp_id": 30, "application_id": 3, "distance": 0.0}
}
```

The function:

1. Removes self matches and same-application matches.
2. Calculates every forward and reverse bucket.
3. Takes transaction advisory locks in a stable sorted order.
4. Creates missing owner/bucket rows.
5. Merges new distances into existing JSONB objects.
6. Writes the reverse relationship with the same distance.
7. Commits or rolls back both directions together through the caller's
   transaction.

The complete Flyway-ready SQL is in `TITAN_MATCHES_FLYWAY.sql`.

### Important reprocessing limit

The current upsert function adds new relationships and replaces a distance
when the same target key is supplied again. It does not remove an older target
that is absent from a later function call.

Example:

```text
Saved first: 100 -> 200 and 232
Later input: 100 -> 232 only
Current upsert result: 100 still contains 200 and 232
```

Deletion or reclassification that invalidates old relationships therefore
needs a separate cleanup workflow before production relies on relationship
removal. This must not be described as automatic stale-match cleanup.

## Case Similarity

Case Similarity does not run another vector search. It receives one `tsp_id`,
loads its `application_id` and `analysis_group` from
`text_similarity_input`, and summarizes the sentence relationships already in
`titan_matches`.

For the source application, it counts unique source/target occurrence pairs by
target application. The current rules are:

| Sentence pairs between two applications | Saved result |
|---:|---|
| 1 or more | Target in `similar_application_ids` |
| 2 or more | Target also in `similar_case_ids` |
| 3 or more | Target also in `tah_case_ids`; `is_tah=true` |

Example:

| Target application | Sentence pairs | Similar application | Similar case | TAH case |
|---:|---:|---|---|---|
| 200 | 1 | Yes | No | No |
| 300 | 2 | Yes | Yes | No |
| 400 | 3 | Yes | Yes | Yes |

These thresholds count occurrence pairs. They do not currently count unique
source sentence groups. This is the implemented rule and should receive final
business confirmation before production approval.

The PostgreSQL refresh function fully rebuilds the source application's lists
and patches only the source application's membership on each connected target.
It uses ordered advisory locks so concurrent writers cannot overwrite one
another. It also removes a stale reverse case relationship when a previously
connected application is no longer connected. Relationships unrelated to the
source application remain unchanged.

If a source has no case relationships, no empty `titan_similar_cases` row is
kept. An existing source row is removed when its last relationship disappears.

`titan_similar_cases` stores real `application_id` values. It does not store
`a_number` because `a_number` is PII and can be resolved from
`text_similarity_input` only when required.

Generated columns maintain `similar_case_count`, `tah_case_count`, and
`is_tah`. The service publishes a Kafka TAH event only when the live-flow
toggle is enabled and the current `is_tah` differs from the last
Kafka-confirmed value in `kafka_ta_hit_lookup`. Changes from false to true and
true to false are both publishable.

## API Transform

The new API Transform handles one `tsp_id` message at a time. It does not call
OpenSearch and does not rebuild similarity. It reads the prepared PostgreSQL
results.

The Titan path:

1. Loads document and applicant context from the existing application tables.
2. Reads all 16 `titan_matches` buckets for each source sentence.
3. Converts the JSONB object into the existing API match-list shape.
4. Caps the returned matches at 3000 per source sentence, preserving the
   existing output limit.
5. Reads `titan_similar_cases` for case-level fields.
6. Converts stored application IDs to A-numbers through
   `landing.application_id_lookup` only where the existing payload requires
   A-numbers.
7. Builds the existing transformed payload while excluding NER for this flow.
8. Publishes Kafka messages and records delivery/status data.

When the incoming SQS message has `reason=HISTORICAL`, the outgoing payload
includes `historical=true`. This lets consumers distinguish backfill results
from live results without maintaining another processing path.

## Reliability and message behavior

The runtime services pass one document between queues with a body shaped like:

```json
{
  "source": "P2",
  "tsp_id": "8205",
  "sent_by": "<service-name>"
}
```

Backfill messages may also carry `reason=HISTORICAL` so downstream payloads
can be marked as historical.

Messages are deleted only after the service completes its required database,
OpenSearch, Kafka, and next-queue work. A failed message becomes visible again
and follows the queue redrive policy; after the configured receives it moves
to the DLQ, where New Relic alerts the team.

Expected transient operations use focused retries. A retry should repeat only
the failed boundary whenever possible:

- failed OpenSearch bulk documents, not the successful documents;
- one failed Titan match write batch, not the completed vector searches;
- one failed queue or Kafka publication, not the completed sentence work.

## Production checks

Before enabling the OpenSearch toggle, verify all of the following:

1. The shared IVF-SQ16 model is in `created` state and has dimension 512 with
   cosine similarity.
2. Both group catalog aliases point to the intended physical indexes.
3. The occurrence, catalog, and tracer document counts match their source
   counts after backfill.
4. Every catalog vector has 512 values.
5. `globalId`, `applicationId`, and `tspId` are indexed as numbers.
6. A 76% sentence threshold is sent to OpenSearch as minimum score `0.88`.
7. Tracer classification reads up to 100 neighbors and applies the 70%
   threshold across all qualifying results.
8. Same-application and cross-analysis-group relationships are absent.
9. Every saved source-to-target sentence relationship has a target-to-source
   relationship with the same distance.
10. No empty `titan_matches` row exists for a sentence with zero matches.
11. Each `match_count` equals its bucket's JSONB key count.
12. Each `verbatim` value contains only target IDs whose distance is `0.0`.
13. Case summary lists are bidirectional and contain application IDs, not
   global IDs, TSP IDs, or A-numbers.
14. Case rows with no remaining relationships are removed.
15. Live TAH Kafka events publish only when enabled and only when `is_tah`
   changes from its last confirmed value.
16. API Transform produces the existing payload shape, excludes NER for this
   flow, and enforces the 3000-match limit.
17. A failed source message is not deleted and reaches its DLQ after the queue
   retry policy is exhausted.
18. The stale sentence-relationship cleanup gap is resolved or explicitly
   accepted before production cutover.

## Future option: sentence-key relationships

Every sentence continues to receive a stable `sentence_key`. If occurrence-
level relationship storage becomes too large, a future table can store one
relationship per unique key pair and expand keys through
`sentence_occurrences` only when global IDs are needed.

That future design reduces repeated relationships but changes the read path
for API Transform and Case Similarity. It is not part of the initial
`titan_matches` production release.
