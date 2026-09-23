# Final DOC2Vec and Sentence Similarity Plan

## Goal

This plan focuses on two services:

1. DOC2Vec splits and saves document sentences.
2. Sentence Similarity decides whether a sentence is allowed to match, finds
   its matches, and saves both directions.

There are three OpenSearch indexes:

1. `sentence_occurrences`: every sentence occurrence.
2. `sentence_semantic_catalog`: one Titan vector per unique sentence key.
3. `tracer_form_language_catalog`: tracer and form-language reference vectors.

Sentence Similarity checks the small tracer and form-language catalog before
it searches the much larger sentence catalog. If the sentence is a
non-matcher, the service saves no relationships and moves to the next
sentence. This avoids an unnecessary main-catalog search.

For now, accepted relationships are written to both the existing
`matches` table and the new `sentence_key_matches` table. API Transform
continues reading `matches` until historical processing and validation are
complete.

## Simple flow

```text
DOC2Vec receives tsp_id and document text
    -> Split the document into sentences
    -> Normalize each sentence and create sentence_key
    -> Insert every sentence into sentences_vector
    -> Receive every global_id
    -> Index sentence occurrences
    -> Create missing vectors in the sentence catalog
    -> Send tsp_id to Sentence Similarity

Sentence Similarity receives tsp_id
    -> Load one source sentence
    -> Read its saved vector
    -> Search the tracer/form-language catalog first
    -> Non-matcher found: update sentences_vector and occurrence by global_id
    -> Save no relationships and take the next sentence
    -> No non-matcher found: search the main sentence catalog
    -> Remove ineligible and same-application targets
    -> Update matches in both directions
    -> Update sentence_key_matches in both directions
    -> Commit and take the next sentence
```

## 1. The three OpenSearch indexes

### `sentence_occurrences`

This index stores every place where a sentence appears.

Create the physical occurrence index with this complete mapping:

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
      "globalId": {
        "type": "long"
      },
      "applicationId": {
        "type": "keyword"
      },
      "tspId": {
        "type": "keyword"
      },
      "sectionName": {
        "type": "keyword"
      },
      "sentIdLocal": {
        "type": "long"
      },
      "sentenceContent": {
        "type": "text"
      },
      "sentenceKey": {
        "type": "keyword"
      },
      "sourceType": {
        "type": "keyword"
      },
      "analysisGroup": {
        "type": "keyword"
      },
      "isNonMatcher": {
        "type": "boolean"
      },
      "isTracer": {
        "type": "boolean"
      },
      "isFormLanguage": {
        "type": "boolean"
      },
      "nonMatcherChecked": {
        "type": "boolean"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

Create its alias:

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_occurrences-v1",
        "alias": "sentence_occurrences"
      }
    }
  ]
}
```

The OpenSearch document `_id` is `globalId`.

The occurrence index keeps a searchable copy of the classification stored in
`sentences_vector`:

- `isNonMatcher` says the occurrence must not participate in matching.
- `isTracer` says the reason includes a tracer reference.
- `isFormLanguage` says the reason includes a form-language reference.
- `nonMatcherChecked` says Sentence Similarity has completed the reference
  catalog check.

`nonMatcherChecked` is important. When it is false,
`isNonMatcher = false` means "not checked yet," not "safe to match."

### `sentence_semantic_catalog`

This index stores one document for each unique `sentenceKey`.

Create the physical vector catalog with this complete mapping:

```http
PUT /sentence_semantic_catalog-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": 18,
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {
        "type": "keyword"
      },
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<train model id>"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

Replace `<bedrock embedding pipeline>` with the Titan ingest pipeline name.
Replace `<train model id>` with the ID returned after the Faiss IVF-SQ16 model
finishes training.

Create its alias:

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_catalog-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog"
      }
    }
  ]
}
```

The vector is a 512-dimension Amazon Titan embedding. The production design
uses Faiss IVF-SQ16 and cosine similarity.

If the same sentence appears in many documents, this catalog keeps one vector.
The occurrence index keeps all of the places where that sentence appears.

Here are two complete sentence examples and their real SHA-256 keys:

| Example | Original sentence | Normalized text | `sentenceKey` |
|---|---|---|---|
| Sentence A | The applicant lived in Ethiopia before moving to the United States. | the applicant lived in ethiopia before moving to the united states. | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` |
| Sentence B | The applicant submitted supporting documents with the application. | the applicant submitted supporting documents with the application. | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` |

Each sentence appears in two applications below. That creates four occurrence
documents but only two catalog documents:

| Example | OpenSearch index | OpenSearch `_id` | Application | `globalId` | `sentenceKey` | Vector |
|---|---|---|---|---:|---|---|
| Sentence A | `sentence_occurrences` | `1001` | `APP-10` | 1001 | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not stored |
| Sentence A | `sentence_occurrences` | `8507` | `APP-25` | 8507 | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not stored |
| Sentence A | `sentence_semantic_catalog` | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not applicable | Not applicable | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | One 512-dimension vector |
| Sentence B | `sentence_occurrences` | `1002` | `APP-10` | 1002 | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not stored |
| Sentence B | `sentence_occurrences` | `9014` | `APP-44` | 9014 | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not stored |
| Sentence B | `sentence_semantic_catalog` | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not applicable | Not applicable | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | One 512-dimension vector |

Occurrence rows have different `globalId` values because each row represents
one place where the sentence appeared. Rows containing the same normalized
sentence share one `sentenceKey`. The catalog uses that key as its OpenSearch
`_id`, so each unique sentence has only one stored vector regardless of how
many applications contain it.

### `tracer_form_language_catalog`

This is a separate reference catalog. It does not store document occurrences.
It stores the known tracer and form-language sentences used to decide whether
an incoming sentence is allowed to participate in matching.

Create the physical index with this complete mapping:

```http
PUT /tracer_form_language_catalog-v1
{
  "settings": {
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>",
    "number_of_shards": 1,
    "number_of_replicas": 1
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "dimension": 512,
        "space_type": "cosinesimil",
        "method": {
          "name": "hnsw",
          "engine": "lucene"
        }
      },
      "isTracer": {
        "type": "boolean"
      },
      "isFormLanguage": {
        "type": "boolean"
      },
      "analysisGroup": {
        "type": "keyword"
      },
      "isNonMatcher": {
        "type": "boolean"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

Replace `<bedrock embedding pipeline>` with the Titan ingest pipeline name.

This reference index uses Lucene HNSW. It does not use Faiss, IVF, SQ16, or a
trained k-NN model. With about 4,000 reference sentences, one primary shard is
enough and IVF training would add complexity without a useful benefit.

The default pipeline must map `sentenceContent` to
`sentenceContentVector`. It calls the deployed Titan embedding model when a
reference document is indexed, so the caller sends the sentence but does not
need to send the 512 vector. The Titan embedding model is still required; only
the trained IVF model is removed from this index.

Create an alias after the physical index exists:

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "tracer_form_language_catalog-v1",
        "alias": "tracer_form_language_catalog"
      }
    }
  ]
}
```

Use a stable OpenSearch `_id` for every reference document. A safe value is a
SHA-256 hash made from `analysisGroup` and normalized `sentenceContent`.
Reindexing the same reference then updates it instead of creating a duplicate,
even when its flags change.

`isTracer` identifies a tracer reference. `isFormLanguage` identifies a
form-language reference. Both values may be true when the sentence belongs to
both groups.

`isNonMatcher = true` means that a source sentence matching this reference
must not receive exact or similar relationships.

The reference catalog is managed separately from normal document ingestion.
Adding or changing a tracer or form-language rule updates this catalog; DOC2Vec
does not copy every document sentence into it.

## 2. DOC2Vec Service

### Receive and split the document

DOC2Vec receives `tsp_id`, `application_id`, `analysis_group`, the full text,
and the document information.

It splits the text into sentences in the original order. Each sentence gets a
`sent_id_local` showing its position in the document.

DOC2Vec does not search for tracer or form-language matches. That decision is
made later by Sentence Similarity.

### Create the sentence key

For every sentence:

1. Apply Unicode NFKC normalization.
2. Apply case folding.
3. Remove extra whitespace.
4. Create SHA-256 from the normalized sentence.

The same normalized sentence always creates the same 64-character
`sentence_key`.

### Insert `sentences_vector`

DOC2Vec inserts every sentence into the existing PostgreSQL
`sentences_vector` table and receives the generated `global_id` values.

Important columns are:

```text
global_id, application_id, tsp_id, section_name,
is_non_matcher, is_tracer, is_form_language, non_matcher_checked,
sent_id_local, sentence_content, source_type, analysis_group,
is_current, sentence_key, created_at, updated_at
```

DOC2Vec initially saves `is_non_matcher = false`, `is_tracer = false`, and
`is_form_language = false`. It saves `non_matcher_checked = false` because
Sentence Similarity has not made the decision yet. The same initial values are
written to the OpenSearch occurrence document.

Sentence Similarity updates PostgreSQL and its occurrence-index copy after the
reference catalog check. PostgreSQL remains the source of truth.

### Write to OpenSearch

After PostgreSQL returns the global IDs, DOC2Vec:

1. Writes every sentence occurrence to the occurrence index.
2. Checks which sentence keys already exist in the sentence catalog.
3. Sends only missing catalog records through the Titan ingest pipeline.
4. Waits until the batch's vectors exist.

DOC2Vec does not write incoming sentences to the tracer/form-language catalog.

### Batch and retry behavior

DOC2Vec uses:

```text
Bulk size: 100
Parallel workers: up to 16
Retry wait: 5 seconds
Immediate retry attempts: 10
```

For each batch, missing catalog vectors are created before its occurrence rows
are considered complete. Temporary Lasso, Titan, or OpenSearch failures are
retried. A retry uses stable sentence keys and global IDs, so it does not
create duplicate records.

DOC2Vec sends `tsp_id` to Sentence Similarity only after all expected sentence
catalog and occurrence records exist.

## 3. Sentence Similarity Service

### Process one sentence at a time

Sentence Similarity receives `tsp_id` and loads its current sentences from
`sentences_vector`.

It processes one source sentence at a time. Every source occurrence keeps its
own `global_id`, but repeated text reuses the same sentence key and vector.

### First search the non-matcher catalog

For the current source sentence:

1. Read the source vector from the sentence catalog by `sentence_key`.
2. Search the tracer/form-language catalog with that vector.
3. Filter the search by the source `analysis_group`.
4. Apply the configured non-matcher threshold.
5. Only consider reference records where `isNonMatcher = true`.

If a qualifying reference is found:

```text
Update the sentences_vector row using its global_id.
Set is_non_matcher to true.
Copy is_tracer and is_form_language from the qualifying reference flags.
Set non_matcher_checked to true.
Update the occurrence document with the same global_id and values.
Do not search the main sentence catalog.
Do not add exact matches.
Do not update matches.
Do not update sentence_key_matches.
Commit the sentence classification.
Move to the next source sentence.
```

The update uses the source sentence's `global_id`:

```sql
UPDATE sentences_vector
SET
    is_non_matcher = :isNonMatcher,
    is_tracer = :isTracer,
    is_form_language = :isFormLanguage,
    non_matcher_checked = TRUE,
    updated_at = NOW()
WHERE global_id = :globalId;
```

Then update the matching occurrence document. Its OpenSearch `_id` is the
string form of the same `globalId`:

```json
POST /sentence_occurrences/_update/1001
{
  "doc": {
    "isNonMatcher": true,
    "isTracer": false,
    "isFormLanguage": true,
    "nonMatcherChecked": true
  }
}
```

Update PostgreSQL first. If the OpenSearch update temporarily fails, retry it
with the same `globalId`. The update is safe to repeat because it replaces the
same fields on the same occurrence document.

`:isNonMatcher` is true on this path because a qualifying exclusion reference
matched. `:isTracer` is true when at least one qualifying tracer reference matched.
`:isFormLanguage` is true when at least one qualifying form-language reference
matched. At least one of these values must be true on the non-matcher path.

The same update rule applies when a candidate target is checked and found to
be a non-matcher. In that case, the target row is updated using the target's
`global_id`, and no relationship to that target is saved.

The same saved vector is reused for this check and the later main-catalog
search. Titan is not called during Sentence Similarity.

### Search the main catalog only when allowed

When the source is not a non-matcher:

1. Find exact occurrences using the same sentence key and analysis group.
2. Search the main sentence catalog using the already loaded vector.
3. Apply the configured sentence-similarity threshold.
4. Keep candidates from the same analysis group.
5. Expand matching keys into occurrences using `sentences_vector`.
6. Remove the source occurrence and every occurrence from the same
   application.
7. Remove any target known to be a tracer, form-language sentence, or other
   non-matcher.

The last rule is required for bidirectional correctness. Checking only the
source is not enough. An allowed source must never create a reverse
relationship on an ineligible target.

Example:

```text
Source global ID 1001:
"I worked at the hospital for five years."
The source was checked and is allowed to match.

The main catalog returns target global ID 8507:
"I certify that the information above is true and correct."
The target has not been checked yet.
```

If the service checks only global ID 1001, it could save this incorrect pair:

```text
1001 adds 8507
8507 adds 1001
```

The second line is the problem. It gives a form-language sentence a reverse
relationship even though form language is not allowed to match.

Before saving either direction, the service checks target 8507 against
`tracer_form_language_catalog`. Suppose it matches a form-language reference.
The service then:

1. Updates `sentences_vector` row 8507 with `is_non_matcher = true`,
   `is_form_language = true`, and `non_matcher_checked = true`.
2. Updates occurrence document 8507 with the same values.
3. Saves neither 1001-to-8507 nor 8507-to-1001.

The reusable decision must use both `analysisGroup` and `sentenceKey` as its
cache key. The same text can have different rules in different analysis
groups. For example:

```text
Cache key: Asylee + KEY-B
Cache value: checked=true, isNonMatcher=true,
             isTracer=false, isFormLanguage=true
```

When another occurrence uses `KEY-B` in the `Asylee` group, the service reuses
that decision instead of running another tracer/form-language vector search.
It still copies the decision to that occurrence's PostgreSQL row and
OpenSearch document using its own `globalId`.

If a target key has no saved or cached decision, it must be checked before any
forward or reverse relationship is saved.

### Save both directions

After all filters pass, Sentence Similarity updates both tables in one
transaction for the source sentence.

If global ID 1 matches global ID 10:

```text
matches row 1 adds global ID 10
matches row 10 adds global ID 1
```

If `KEY-A` matches `KEY-B`:

```text
sentence_key_matches KEY-A adds KEY-B and the distance
sentence_key_matches KEY-B adds KEY-A and the same distance
```

A retry replaces the existing relationship value instead of adding a
duplicate. The service commits this sentence and then takes the next one.

## 4. Existing and new PostgreSQL match tables

### Existing `matches`

The existing table stores occurrence-level relationships. One source
`global_id` has a JSONB list containing target `global_id`, `application_id`,
`tsp_id`, and distance.

API Transform continues reading this table during historical processing.

### New `sentence_key_matches`

The new table stores one row for each sentence key and analysis group:

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

Example:

```json
{
  "KEY-B": 0.10,
  "KEY-C": 0.30
}
```

Exact occurrences use the same sentence key, so the key is not related to
itself in this table. Exact global-ID relationships are still saved in
`matches`.

## 5. Historical processing and switching reads

Sentence Similarity dual-writes every accepted relationship to the old and new
tables. New and historical documents use the same one-sentence-at-a-time
flow, including the non-matcher catalog check.

During backfill:

- API Transform reads only `matches`.
- `sentence_key_matches` is populated but is not a production read source.
- Every historical `tsp_id` must complete successfully.

Before changing API Transform, verify:

- Every A-to-B relationship has B-to-A.
- Both directions have the same distance.
- No relationship contains a non-matcher sentence.
- No key matches itself.
- No relationship crosses `analysis_group`.
- Expanding new-table keys returns the same eligible global IDs as the old
  table for test documents.

API Transform can switch to `sentence_key_matches` only after historical
processing and validation pass. Removing `matches` is a later, separate
decision.

## 6. Final acceptance criteria

1. Three separate OpenSearch indexes exist: occurrences, sentence catalog,
   and tracer/form-language catalog.
2. DOC2Vec splits documents, creates stable keys, inserts
   `sentences_vector`, and writes the two normal sentence indexes.
3. DOC2Vec does not perform tracer or form-language matching.
4. The reference catalog stores a vector, `isTracer`, `isFormLanguage`,
   `analysisGroup`, and `isNonMatcher` for every reference sentence.
5. The reference catalog uses Lucene HNSW and does not require IVF training.
6. Sentence Similarity processes one source sentence at a time.
7. The tracer/form-language catalog is searched before the main sentence
   catalog.
8. A qualifying non-matcher updates `sentences_vector` and the occurrence
   document by `global_id`, saves no relationships, and moves to the next
   sentence.
9. The main catalog is searched only for an allowed source sentence.
10. Ineligible targets and same-application targets are never saved.
11. Accepted relationships are bidirectional in both PostgreSQL match tables.
12. Retrying the same sentence does not create duplicate relationships.
13. Existing production reads remain on `matches` until backfill and
    validation are complete.
14. Every occurrence classification records whether the non-matcher check has
    actually completed; unchecked rows are never treated as eligible.
