# Final DOC2Vec and Sentence Similarity Plan

Status: Final  
Diagram: [FINAL_SENTENCE_PROCESSING_ARCHITECTURE.drawio](FINAL_SENTENCE_PROCESSING_ARCHITECTURE.drawio)

## Goal

This plan focuses on the two services doing the main work:

1. DOC2Vec Service prepares and saves sentences.
2. Sentence Similarity Service finds and saves sentence matches.

For now, Sentence Similarity writes to both the existing `tsp.matches` table
and the new `sentence_key_matches` table.

API Transform continues reading from `tsp.matches`. After the new table is
fully populated and verified, API Transform will use
`sentence_key_matches` instead.

## Simple flow

```text
DOC2Vec receives tsp_id and document text
    -> Split the document into sentences
    -> Mark tracer and form-language sentences
    -> Create sentence_key for every sentence
    -> Insert sentences_vector and receive global_id values
    -> Batch index both OpenSearch indexes

Sentence Similarity receives tsp_id
    -> Load eligible sentences
    -> Take one source sentence
    -> Find its exact and similar matches
    -> Update existing tsp.matches in both directions
    -> Update new sentence_key_matches in both directions
    -> Commit the sentence
    -> Move to the next source sentence
```

## 1. DOC2Vec Service

### Input

DOC2Vec receives `tsp_id`, `application_id`, `analysis_group`, the full
document text, and the document information.

### Split the document

The service splits the text into sentences in their original order. Each
sentence receives `sent_id_local` based on its position in the document.

### Identify tracer and form language

Each sentence is checked against the tracer sentence table and the
form-language table. The service sets `is_tracer` and `is_form_language`.

These sentences are still saved and still receive Titan vectors. They are
excluded only when Sentence Similarity searches for matches.

The checks should be batched. Do not send one database query for every
sentence.

### Create the sentence key

For every sentence:

1. Apply Unicode NFKC normalization.
2. Apply case folding.
3. Remove extra whitespace.
4. Create SHA-256 from the normalized sentence.

The same normalized sentence always produces the same 64-character
`sentence_key`.

### Insert `sentences_vector`

DOC2Vec inserts all document sentences into the existing
`sentences_vector` table.

Important columns are:

```text
global_id, application_id, tsp_id, section_name,
is_tracer, is_form_language, sent_id_local,
sentence_content, source_type, analysis_group,
is_current, sentence_key, created_at, updated_at
```

PostgreSQL creates `global_id`. The insert returns all generated global IDs
before OpenSearch processing begins.

`sentences_vector` remains the main source for sentence occurrence data.

### Insert into both OpenSearch indexes

After PostgreSQL returns the global IDs, DOC2Vec writes to two indexes.

The occurrence index receives every sentence occurrence. Its OpenSearch `_id`
is `global_id`. It contains the application, document, section, sentence key,
sentence text, analysis group, and eligibility flags.

The semantic catalog receives one record for every unique `sentence_key`. It
contains the sentence text and its 512-dimension Titan vector.

If a catalog key already exists, DOC2Vec reuses it. Titan does not create the
same vector again.

### Batch and retry behavior

DOC2Vec uses:

```text
Bulk size: 100
Parallel workers: up to 16
Retry wait: 5 seconds
Immediate retry attempts: 10
```

For each batch:

1. Check which catalog keys already exist.
2. Create only missing catalog vectors.
3. Wait until the catalog work succeeds.
4. Insert the occurrence records for that batch.

Lasso or Titan can temporarily fail. A failed catalog batch waits five seconds
and retries. Other completed batches remain completed.

If all immediate retries fail, the document remains incomplete and is retried
later. The next run uses `sentences_vector` and sends only missing OpenSearch
records.

DOC2Vec sends `tsp_id` to Sentence Similarity only after all expected
occurrence and catalog records exist.

## 2. Sentence Similarity Service

### Input

Sentence Similarity receives `tsp_id` and loads the current sentences from
`sentences_vector`.

It excludes source sentences where `is_tracer` or `is_form_language` is true.

The service processes one source sentence at a time. It does not create an
80-sentence similarity batch and it does not remove repeated source
occurrences. Every eligible `global_id` must receive its own saved matches in
the existing `tsp.matches` table.

For each source sentence:

1. Read its `global_id`, `sentence_key`, application, and analysis group.
2. Find its exact matches.
3. Run one OpenSearch semantic search for its similar matches.
4. Exclude matches from the same application.
5. Update both directions in `tsp.matches`.
6. Update both directions in `sentence_key_matches`.
7. Commit the sentence's database work.
8. Move to the next source sentence.

If one sentence fails, retry that sentence. Already committed sentences do not
need to be processed again.

### Exact matches

An exact match has the same `sentence_key`.

The service searches `sentences_vector` for current sentences with the same
key and analysis group, but a different `application_id`.

Exact global-ID matches are saved with distance `0` in `tsp.matches`.

The key is not added as a match to itself in `sentence_key_matches`. The same
key already represents the exact relationship.

### Similar matches

For the current source sentence:

1. Read its vector from the OpenSearch catalog.
2. Search the catalog for similar sentence keys.
3. Apply the configured matching threshold.
4. Remove the source key itself.
5. Keep candidates from the same `analysis_group`.
6. Find candidate occurrences in `sentences_vector`.
7. Exclude occurrences from the source application.
8. Exclude tracer and form-language occurrences.

Titan is not called during this search. The vector already exists in the
catalog.

The accepted matches are written to both the old and new matching tables before
the service moves to the next source sentence.

## 3. Keep updating existing `tsp.matches`

The old table is still required because API Transform currently reads it.

Each source `global_id` keeps its existing JSONB list. A target entry contains
the target `global_id`, `application_id`, `tsp_id`, and distance.

Updates remain bidirectional. If global ID 1 matches global ID 10:

```text
global ID 1 adds global ID 10
global ID 10 adds global ID 1
```

A retry updates an existing target entry instead of adding a duplicate.

For now, API Transform continues using this table exactly as it does today.

## 4. Add `sentence_key_matches`

The new table saves relationships once per sentence key and analysis group.

```sql
CREATE TABLE sentence_key_matches (
    sentence_key TEXT NOT NULL,
    analysis_group TEXT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sentence_key, analysis_group),
    CHECK (jsonb_typeof(matches) = 'object')
);
```

The JSONB object stores matching keys and distances:

```json
{
  "KEY-B": 0.10,
  "KEY-C": 0.30
}
```

Example:

| sentence_key | analysis_group | matches |
|---|---|---|
| `KEY-A` | `Asylee` | `{"KEY-B": 0.10, "KEY-C": 0.30}` |
| `KEY-B` | `Asylee` | `{"KEY-A": 0.10}` |
| `KEY-C` | `Asylee` | `{"KEY-A": 0.30}` |

If `KEY-A` matches `KEY-B`, both rows are updated in one PostgreSQL
transaction:

```text
KEY-A adds {"KEY-B": 0.10}
KEY-B adds {"KEY-A": 0.10}
```

Before writing a large result, group all changes by sentence key. Update each
key row once instead of once for every match.

## 5. Getting global IDs from the new table

The new table stores keys, not global IDs.

To get all matching sentence records later:

1. Read the source key and analysis group from `sentences_vector`.
2. Read its matching keys from `sentence_key_matches`.
3. Add the source key for exact matches.
4. Search `sentences_vector` using those keys and the analysis group.
5. Exclude the source global ID and source application.
6. Return every matching global ID and sentence record.

Example:

```text
global ID 1 uses KEY-A
KEY-A matches KEY-B and KEY-C

KEY-A exists as global IDs 1 and 2
KEY-B exists as global IDs 10 and 11
KEY-C exists as global ID 20

Result for global ID 1:
global IDs 2, 10, 11, and 20
```

Distance stays saved in the new table, but it does not have to be returned
when the caller only needs all accepted matches.

## 6. Move from the old table to the new table

### Dual write

Sentence Similarity writes every new relationship to both `tsp.matches` and
`sentence_key_matches`.

During this stage, the new table is write-only for production behavior. API
Transform and existing APIs continue reading only `tsp.matches`. No production
read is moved to the new table while historical documents are still being
processed.

### Backfill

Every historical `tsp_id` is sent through Sentence Similarity. The service
loads that document and processes its eligible sentences one at a time using
the same OpenSearch search and dual-write flow used for new documents.

This makes historical processing and new-document processing follow the same
rules. The existing `tsp.matches` data can be used to compare results, but it
does not replace processing the historical sentences.

Historical processing is complete only when every current historical document
has successfully finished all of its eligible source sentences.

### Validate

Before changing API Transform, verify:

- Every old saved relationship is represented in the new table.
- Every A-to-B key relationship has B-to-A.
- Both directions have the same distance.
- No key matches itself.
- No relationship crosses `analysis_group`.
- Expanding new-table keys returns the same global IDs as the old table for
  test documents.

### Change API Transform later

Only after every historical document has been processed and validation passes
can API Transform stop reading `tsp.matches`.

It will read source keys from `sentences_vector`, read matching keys from
`sentence_key_matches`, and expand those keys back to current global IDs.

The old table remains during a verification period. Removing it is a separate
decision after the new read flow is proven in production.

## 7. Implementation order

1. Create `sentence_key_matches`.
2. Add its SQLAlchemy model and database methods.
3. Keep the existing `tsp.matches` writer.
4. Add grouped bidirectional writes to the new table.
5. Keep DOC2Vec catalog-first indexing and retry behavior.
6. Change Sentence Similarity input to `tsp_id`.
7. Process one eligible source sentence at a time.
8. Search, update both tables, and commit before moving to the next sentence.
9. Send every historical `tsp_id` through the same one-sentence-at-a-time
   similarity flow.
10. Compare old-table and new-table results.
11. Change API Transform only after the comparison passes.

## 8. Final acceptance criteria

1. DOC2Vec creates sentences, flags, keys, SQL rows, and global IDs.
2. Every occurrence is indexed in OpenSearch.
3. Every unique key has one catalog vector.
4. Temporary Lasso and OpenSearch failures retry without duplicate data.
5. Sentence Similarity accepts `tsp_id` and processes one eligible source
   sentence at a time.
6. Same-application, tracer, and form-language occurrences are excluded.
7. Every accepted global-ID relationship is bidirectional in `tsp.matches`.
8. Every accepted similar-key relationship is bidirectional in
   `sentence_key_matches` with the same distance.
9. Retrying the same work does not create duplicates.
10. API Transform and production reads continue using only the old table
    during dual write and historical processing.
11. Every historical document is processed into the new table.
12. Old-table and new-table results match before any production read is
    switched.
