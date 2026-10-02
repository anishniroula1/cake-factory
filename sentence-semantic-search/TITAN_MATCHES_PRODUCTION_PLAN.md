# Final Titan Sentence Matching Production Architecture

## Goal

This plan focuses on two services:

1. DOC2Vec splits and saves document sentences.
2. Sentence Similarity decides whether a sentence is allowed to match, finds
   its matches, and saves both directions.

There are four runtime OpenSearch indexes:

1. `sentence_occurrences`: every sentence occurrence.
2. `sentence_semantic_catalog_asylee`: one Titan vector per unique Asylee
   sentence key.
3. `sentence_semantic_catalog_refugee`: one Titan vector per unique Refugee
   sentence key.
4. `tracer_form_language_catalog`: tracer and form-language reference vectors.

Sentence Similarity checks the small tracer and form-language catalog before
it searches the larger catalog selected by the sentence's `analysis_group`.
If the sentence is a
non-matcher, the service saves no relationships and moves to the next
sentence. This avoids an unnecessary main-catalog search.

The Asylee and Refugee catalogs use the same trained 512-dimension Faiss
IVF-SQ16 model. The model is trained once from representative data from both
groups. Separate catalogs keep searches and saved non-matcher decisions inside
the correct analysis group.

The separate `sentence_semantic_training` index is used only to train that
shared IVF-SQ16 model, so it is not counted as a runtime search index.

Accepted relationships are written to the new `titan_matches` table. One
row belongs to one sentence occurrence, and its JSONB list contains the
matching occurrences. This keeps the data shape familiar to API Transform and
Case Similarity while OpenSearch becomes the source of vector matching.

DOC2Vec still creates and saves a stable `sentence_key` for every sentence.
The key deduplicates vectors in OpenSearch now and prepares the data for a
future sentence-key relationship table after the first production release is
stable.

## Simple flow

```text
DOC2Vec receives tsp_id
    -> Live: load the document text and split it into sentences
    -> Historical: load the sentences already saved in sentences_vector
    -> Normalize each sentence and create or reuse sentence_key
    -> Live: insert the sentences and receive every global_id
    -> Historical: keep the existing global_id values
    -> Reuse an existing Titan embedding for the global_id when available
    -> Generate an embedding only when it does not already exist
    -> Index sentence occurrences
    -> Use analysis_group to select the Asylee or Refugee catalog
    -> Create missing vectors only in the selected catalog
    -> Send tsp_id to Sentence Similarity

Sentence Similarity receives tsp_id
    -> Load one source sentence
    -> Use analysis_group to select the Asylee or Refugee catalog
    -> Read its saved vector and any saved classification
    -> Reuse a saved classification or search the tracer/form-language catalog
    -> Save the classification on the selected catalog document
    -> Update sentences_vector by global_id
    -> Non-matcher found: save no relationships and take the next sentence
    -> No non-matcher found: search only the selected group catalog
    -> Remove ineligible and same-application targets
    -> Update titan_matches for the source and every target in both directions
    -> Commit and take the next sentence
```

## 1. The four runtime OpenSearch indexes

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
        "type": "long"
      },
      "tspId": {
        "type": "long"
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
      "isCurrent": {
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

The occurrence index stores sentence identity, content, location, and analysis
group. It does not store non-matcher classification. The selected Asylee or
Refugee catalog stores the reusable key-level classification, and
`sentences_vector` stores the occurrence-level database copy.

### Group-specific sentence semantic catalogs

The production design replaces the single mixed sentence catalog with two
catalogs:

| Analysis group | Physical index | Alias |
|---|---|---|
| Asylee | `sentence_semantic_catalog_asylee-ivf-sq16-v1` | `sentence_semantic_catalog_asylee` |
| Refugee | `sentence_semantic_catalog_refugee-ivf-sq16-v1` | `sentence_semantic_catalog_refugee` |

Each catalog stores one document for each unique `sentenceKey` in that group.
Both catalogs use the same trained 512-dimension Faiss IVF-SQ16 model, Titan
embedding pipeline, and cosine similarity. Train the model once with
representative Asylee and Refugee vectors; do not train one model per catalog.

Create the Asylee catalog with this complete mapping:

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

Create the Refugee catalog with the same model and pipeline:

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

Set each shard count from that group's expected vector count and target shard
size. Do not automatically give both catalogs the old mixed catalog's full
shard count.

Create both aliases:

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_catalog_asylee-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog_asylee"
      }
    },
    {
      "add": {
        "index": "sentence_semantic_catalog_refugee-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog_refugee"
      }
    }
  ]
}
```

OpenSearch cannot rename or split an existing mixed physical index in place.
If `sentence_semantic_catalog` already contains data, create both new physical
indexes, populate them by `analysis_group`, validate their counts, and then
change the service environment variables to the two new aliases. Keep the old
mixed index until that cutover is verified.

The catalog document does not need `analysisGroup`; the selected physical
catalog already defines the group. The classification flags are saved on the
catalog document because the same key can have a different non-matcher result
in Asylee and Refugee. Sentence Similarity copies those flags to each matching
`sentences_vector` row.

If the same sentence appears in many documents in one group, that group's
catalog keeps one vector. The occurrence index keeps every place where it
appears. A key that exists in both groups is stored once in each catalog. The
measured overlap is about 33,000 exact sentences out of roughly 100 million,
so this small duplication is a good tradeoff for isolated searches and rules.

Here are two complete sentence examples and their real SHA-256 keys:

| Example | Original sentence | Normalized text | `sentenceKey` |
|---|---|---|---|
| Sentence A | The applicant lived in Ethiopia before moving to the United States. | the applicant lived in ethiopia before moving to the united states. | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` |
| Sentence B | The applicant submitted supporting documents with the application. | the applicant submitted supporting documents with the application. | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` |

Each sentence appears in two applications within its analysis group below.
That creates four occurrence documents but only two catalog documents:

| Example | Group | OpenSearch index | OpenSearch `_id` | Application | `globalId` | `sentenceKey` | Vector |
|---|---|---|---|---|---:|---|---|
| Sentence A | Asylee | `sentence_occurrences` | `1001` | `APP-10` | 1001 | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not stored |
| Sentence A | Asylee | `sentence_occurrences` | `8507` | `APP-25` | 8507 | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not stored |
| Sentence A | Asylee | `sentence_semantic_catalog_asylee` | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | Not applicable | Not applicable | `310796ab599358716162cc1f8183e7a3861dae74ec1cf58219e12272e0abd828` | One 512-dimension vector |
| Sentence B | Refugee | `sentence_occurrences` | `1002` | `APP-10` | 1002 | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not stored |
| Sentence B | Refugee | `sentence_occurrences` | `9014` | `APP-44` | 9014 | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not stored |
| Sentence B | Refugee | `sentence_semantic_catalog_refugee` | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | Not applicable | Not applicable | `abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1` | One 512-dimension vector |

Occurrence rows have different `globalId` values because each row represents
one place where the sentence appeared. Rows containing the same normalized
sentence share one `sentenceKey`. The selected group catalog uses that key as
its OpenSearch `_id`, so each unique sentence has only one stored vector per
analysis group regardless of how many applications contain it.

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

### Receive live or historical sentences

DOC2Vec receives `tsp_id` for both flows, but the sentence source is different.

For the live flow, DOC2Vec loads the document text and splits it into sentences
in the original order. Each new sentence receives a `sent_id_local` showing its
position in the document.

For the historical flow, the sentences have already been split and saved in
`sentences_vector`. DOC2Vec loads those rows by `tsp_id`; it does not load the
original document text, split the text again, or insert duplicate sentence
rows. The historical rows keep their existing `global_id` values.

DOC2Vec does not search for tracer or form-language matches. That decision is
made later by Sentence Similarity.

### Create the sentence key

For every sentence:

1. Apply Unicode NFKC normalization.
2. Apply case folding.
3. Remove extra whitespace.
4. Create SHA-256 from the normalized sentence.

The same normalized sentence always creates the same 64-character
`sentence_key`. Live processing saves the key during the sentence insert.
Historical processing generates and updates the key only when the existing row
does not already have it.

### Insert `sentences_vector`

For the live flow, DOC2Vec inserts every new sentence into the existing
PostgreSQL `sentences_vector` table and receives the generated `global_id`
values. Historical processing reuses the rows and global IDs already stored in
that table.

Important columns are:

```text
global_id, tsp_id,
is_non_matcher, is_tracer, is_form_language, non_matcher_checked,
sent_id_local, sentence_content, source_type,
sentence_key, created_at, updated_at
```

DOC2Vec joins `text_similarity_input` by `tsp_id` and reads the document-level
`application_id`, `analysis_group`, `section_name`, and `is_current` values
from that table. One `tsp_id` has one analysis group, so all of its sentence
occurrences use the same catalog.

DOC2Vec initially saves `is_non_matcher = false`, `is_tracer = false`, and
`is_form_language = false`. It saves `non_matcher_checked = false` because
Sentence Similarity has not made the decision yet.

Sentence Similarity updates PostgreSQL and the selected group catalog after
the reference catalog check. PostgreSQL remains the source of truth for each
sentence occurrence.

### Reuse existing Titan embeddings

Both live and historical processing check the `embeddings` table before asking
Titan to create a vector. The lookup uses the sentence `global_id` and
`embedding_type = 2`, which identifies the existing 512-dimension Titan
embedding.

```text
Existing 512-dimension Titan embedding found
    -> Use that vector for OpenSearch
    -> Do not call Titan again

No Titan embedding found and the catalog vector is missing
    -> Generate the 512-dimension vector through the Titan pipeline
    -> Continue only after the vector is available
```

This check applies to historical and live sentences. It avoids paying for and
waiting on an embedding that already exists. A vector is generated only when
the required sentence-catalog record is missing and no reusable Titan
embedding is available for the selected global ID.

### Write to OpenSearch

After DOC2Vec inserts the live rows or loads the historical rows, it:

1. Writes every sentence occurrence to the occurrence index.
2. Reads the document's one `analysis_group` from `text_similarity_input`.
3. Selects `sentence_semantic_catalog_asylee` or
   `sentence_semantic_catalog_refugee` once for that `tsp_id`.
4. Checks all sentence keys for the `tsp_id` in that selected catalog.
5. Skips vector work when the sentence key already exists in the selected
   group catalog.
6. For a missing key, uses the existing `embedding_type = 2` vector when one
   is available for the global ID.
7. Sends the sentence through the Titan ingest pipeline only when the selected
   catalog key and reusable embedding are both missing.
8. Writes the missing key only to its selected group catalog.
9. Waits until every missing catalog vector exists.

DOC2Vec does not write incoming sentences to the tracer/form-language catalog.

### Batch and retry behavior

DOC2Vec uses:

```text
Bulk size: 100
Parallel workers: up to 16
Retry wait: 5 seconds
Immediate retry attempts: 10
```

For each batch, missing catalog vectors are created or supplied from the
existing embeddings in the correct group catalog before its occurrence rows
are considered complete.
Temporary Lasso, Titan, database, or OpenSearch failures are retried. A retry
uses stable sentence keys and global IDs, so it does not create duplicate
records or regenerate a vector that can be reused.

DOC2Vec sends `tsp_id` to Sentence Similarity only after all expected group
catalog and occurrence records exist.

## 3. Sentence Similarity Service

### Process one sentence at a time

Sentence Similarity receives `tsp_id` and loads its current sentences from
`sentences_vector`.

It processes one source sentence at a time. Every source occurrence keeps its
own `global_id`, but repeated text reuses the same sentence key and vector.

### First search the non-matcher catalog

For the current source sentence:

1. Use `analysis_group` to select `sentence_semantic_catalog_asylee` or
   `sentence_semantic_catalog_refugee`.
2. Read the source vector and any saved classification from that catalog by
   `sentence_key`.
3. Reuse the saved classification when the flag fields are present.
4. If the flag fields are missing, search the tracer/form-language catalog
   with the source vector and filter that search by `analysis_group`.
5. Apply the configured non-matcher threshold and only consider reference
   records where `isNonMatcher = true`.
6. Save `isNonMatcher`, `isTracer`, and `isFormLanguage` on the selected group
   catalog document.
7. Copy the completed classification to the source `sentences_vector` row.

If a qualifying reference is found:

```text
Update the sentences_vector row using its global_id.
Set is_non_matcher to true.
Copy is_tracer and is_form_language from the qualifying reference flags.
Set non_matcher_checked to true.
Do not run the normal similarity search on the selected group catalog.
Do not add exact matches.
Do not update titan_matches.
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

The group catalog is updated by `sentenceKey`, and `sentences_vector` is
updated by `globalId`. Both updates are safe to retry because they replace the
same classification fields on stable IDs.

`:isNonMatcher` is true on this path because a qualifying exclusion reference
matched. `:isTracer` is true when at least one qualifying tracer reference matched.
`:isFormLanguage` is true when at least one qualifying form-language reference
matched. At least one of these values must be true on the non-matcher path.

The same update rule applies when a candidate target is checked and found to
be a non-matcher. In that case, the target row is updated using the target's
`global_id`, and no relationship to that target is saved.

The same saved vector is reused for this check and the later group-catalog
search. Titan is not called during Sentence Similarity.

### Search the selected group catalog only when allowed

When the source is not a non-matcher:

1. Find exact occurrences using the same sentence key and analysis group.
2. Search only the catalog selected by the source `analysis_group`, using the
   already loaded vector.
3. Apply the configured sentence-similarity threshold.
4. Expand matching keys into occurrences using `sentences_vector`.
5. Remove the source occurrence and every occurrence from the same
   application.
6. Remove any target known to be a tracer, form-language sentence, or other
   non-matcher.

The selected catalog itself limits vector candidates to the correct analysis
group. The occurrence expansion still filters by `analysis_group` as a safety
check.

The last rule is required for bidirectional correctness. Checking only the
source is not enough. An allowed source must never create a reverse
relationship on an ineligible target.

Example:

```text
Source global ID 1001:
"I worked at the hospital for five years."
The source was checked and is allowed to match.

The selected group catalog returns a matching target `sentenceKey`, and
occurrence expansion finds global ID 8507:
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
2. Saves the classification on the selected group catalog document.
3. Saves neither 1001-to-8507 nor 8507-to-1001.

The reusable decision is stored on the group-specific catalog document. The
same `sentenceKey` can therefore have different rules in the two catalogs. For
example:

```text
Catalog: sentence_semantic_catalog_asylee
Document ID: KEY-B
Saved fields: isNonMatcher=true, isTracer=false, isFormLanguage=true
```

The presence of all three classification fields means the catalog decision is
complete. The `sentences_vector` row still uses `non_matcher_checked` because
its boolean classification fields already exist before the check.

When another occurrence uses `KEY-B` in the `Asylee` group, the service reuses
that decision instead of running another tracer/form-language vector search.
It still copies the decision to that occurrence's `sentences_vector` row using
its own `globalId`.

If a target key has no saved or cached decision, it must be checked before any
forward or reverse relationship is saved.

### Save both directions

After all filters pass, Sentence Similarity expands every matching
`sentence_key` into eligible target occurrences from `sentences_vector`.
Each source-to-target pair is stored by `global_id` in `titan_matches`.

If source global ID 100 matches target global IDs 900 and 901:

```text
100 -> [900, 901]
900 -> [100]
901 -> [100]
```

The same cosine distance is stored in both directions. An existing target
`global_id` is updated with the latest distance instead of being added twice.
All forward and reverse changes for one source sentence commit together.

A retry must also remove stale relationships. If global ID 100 is processed
again and now matches only 901, the completed state is:

```text
100 -> [901]
900 -> []
901 -> [100]
```

Only the relationship between 100 and 900 is removed. Any other relationships
owned by 900 remain unchanged.

Matching is not transitive. If 100 matches 900 and 900 matches 950, the service
does not add 100 to 950 unless that exact pair passes the configured threshold.

## 4. PostgreSQL `titan_matches`

### Why this table is used first

`titan_matches` stores the actual matching sentence occurrences. This is
useful for the first production release because downstream services already
work with global IDs and occurrence-level JSONB lists.

The immediate benefits are:

- API Transform can continue reading matches by `tsp_id`.
- Case Similarity can group target occurrences by `a_number`. It can use
  `tsp_id` to read `application_id` from `text_similarity_input` when needed.
- A saved object identifies the exact source and target sentence locations.
- OpenSearch can be introduced without redesigning every downstream service at
  the same time.
- The rollout can be measured before a larger database migration is started.

### Match object

Each item in the JSONB array describes one target occurrence:

```json
{
  "global_id": 9002,
  "a_number": 900,
  "tsp_id": 8000,
  "distance": 0.10
}
```

`distance` is cosine distance, not the raw OpenSearch `_score`:

- `0.0` means a 100% match.
- Exact occurrences use `0.0`.
- A smaller distance is a stronger semantic match.
- The same distance is stored in both directions.

Sentence Similarity must perform the OpenSearch score conversion before
saving the match.

### Complete bidirectional example

Suppose the source row is:

```text
global_id: 100
a_number: 100
tsp_id: 5000
```

It matches this target:

```text
global_id: 900
a_number: 900
tsp_id: 8000
distance: 0.05
```

The source row stores:

```json
[
  {
    "global_id": 900,
    "a_number": 900,
    "tsp_id": 8000,
    "distance": 0.05
  }
]
```

The target row stores the reverse object:

```json
[
  {
    "global_id": 100,
    "a_number": 100,
    "tsp_id": 5000,
    "distance": 0.05
  }
]
```

The `titan_matches` row has only these columns:

```text
global_id, tsp_id, matches, match_count, verbatim, last_modified
```

`global_id` is the primary key. PostgreSQL calculates `match_count` from the
JSONB list and calculates `verbatim` from matches whose distance is `0.0`.
PostgreSQL also maintains `last_modified`. `a_number` remains in each nested
match object, but it is not a table column. The caller supplies the source
identity needed for reverse match objects and the `matches` list.

Sentence Similarity checks eligibility before calling the database function.
The relationship is valid only when both sentences:

- belong to the same `analysis_group`;
- are current;
- belong to different applications; and
- are not tracer, form-language, or other non-matcher sentences.

### Safe updates from multiple workers

Workers must not read a JSONB list into Python, append an item, and replace the
row. Two workers doing that at the same time can overwrite each other's
changes.

Use a PostgreSQL function or atomic SQLAlchemy upsert that:

1. Receives the source occurrence and its accepted match list.
2. Locks the source and affected target global IDs in sorted order.
3. Adds new relationships in both directions.
4. Creates missing source and target rows.
5. Updates an existing relationship's IDs and distance without duplicating it.
6. Updates only `matches` and `last_modified` when an owner row already exists.
7. Commits all changes for the source sentence in one transaction.

A database failure rolls back every forward and reverse change. Retry a
deadlock or serialization failure with the same input. The operation must be
safe to repeat.

## 5. Live and historical processing

Live and historical documents use the same DOC2Vec and Sentence Similarity
logic.

For each completed `tsp_id`:

- Every sentence has a stable `sentence_key` in `sentences_vector`.
- Every sentence occurrence exists in `sentence_occurrences`.
- Every unique key has one vector in the catalog selected by its
  `analysis_group`.
- Every source and unchecked target has passed the non-matcher check before a
  relationship is saved.
- Every accepted occurrence relationship exists in both directions in
  `titan_matches`.

The source SQS message is deleted only after the entire `tsp_id` completes.
Temporary database, Lasso, Titan, or OpenSearch failures are retried. Stable
global IDs and sentence keys make retries idempotent.

Verify the production data with these rules:

- Every 100-to-900 relationship has 900-to-100.
- Both directions have the same distance.
- No JSONB list contains the same target global ID twice.
- No relationship contains a non-matcher sentence.
- No relationship connects occurrences from the same application.
- No relationship crosses `analysis_group`.
- `match_count` equals the JSONB array length.
- `verbatim` contains only target global IDs whose distance is `0.0`.

## 6. Downstream service use

### API Transform

API Transform will keep using its existing functionality as it is. The Titan
flow stores the same occurrence-level match information needed by the current
summary and response-building process, so this service does not need a new
design.

### Case Similarity

Case Similarity will also keep using its existing functionality as it is. It
will process the occurrence matches produced by the Titan flow using the same
case-level calculation currently used by the service.

The existing `similar_cases_emb` table is renamed to `titan_similar_cases` and
stores the Titan result. The previous case-similarity result remains unchanged.
Keeping both results separate allows the team to compare the previous flow
with the new Titan flow before deciding whether to replace the existing result.

## 7. Future sentence-key relationship design

The occurrence design intentionally keeps generating and saving
`sentence_key`. That gives us a clean future path when the number and size of
`titan_matches` rows become expensive.

Repeated text can appear in thousands of documents. The current table stores
relationships for every matching pair of occurrences. A future table can save
one relationship between two unique sentence keys and expand those keys
through `sentences_vector` only when global IDs are needed.

A future bucketed table can look like this:

```sql
CREATE TABLE sentence_key_matches (
    analysis_group TEXT NOT NULL,
    sentence_key TEXT NOT NULL,
    match_bucket_number SMALLINT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,
    match_key_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        analysis_group,
        sentence_key,
        match_bucket_number
    )
);
```

Its `matches` value is an object of matching key to distance:

```json
{
  "abbe073671a24dd60282a9ca827c1fd54c63306c02803cd7b6557c5d787b99c1": 0.10
}
```

Example forward and reverse rows:

| analysisGroup | sentenceKey | bucket | matches |
|---|---|---:|---|
| Asylee | KEY-A | 11 | `{"KEY-B": 0.10}` |
| Asylee | KEY-B | 17 | `{"KEY-A": 0.10}` |

The matching key deterministically selects one of 32 buckets. The benefits are:

- One semantic relationship is stored once per direction instead of once for
  every occurrence pair.
- Large match collections are split into smaller JSONB values.
- Updating one match rewrites and locks one bucket instead of one very large
  JSONB list.
- PostgreSQL TOAST can compress large buckets while normal table pages remain
  smaller.
- Different workers can update different buckets for the same sentence key.
- Relationship storage grows with unique sentence keys rather than every
  repeated occurrence.

The tradeoff is that API Transform and Case Similarity must expand matching
keys through `sentences_vector` to recover global IDs. That change will be
made only after the production OpenSearch flow and `titan_matches` data are
stable.

The future migration can join each source and target `global_id` in
`titan_matches` to its saved `sentence_key`, deduplicate the key pairs, and
backfill the bucketed table. The original sentence text does not need to be
processed again.

## 8. Final acceptance criteria

1. Four runtime OpenSearch indexes exist: sentence occurrences, the Asylee
   sentence catalog, the Refugee sentence catalog, and the
   tracer/form-language catalog.
2. DOC2Vec splits sentence text only for the live flow; historical processing
   reuses the sentences and global IDs already in `sentences_vector`.
3. DOC2Vec creates and saves a stable SHA-256 `sentence_key` for every
   sentence.
4. Both flows reuse a valid `embedding_type = 2` Titan embedding for the
   global ID and generate a new vector only when one is required and missing.
5. The Asylee and Refugee catalogs use the same trained 512-dimension Faiss
   IVF-SQ16 model and keep sentence keys and classifications separated by
   analysis group.
6. DOC2Vec writes every occurrence and creates a missing catalog vector only
   in the catalog selected by `analysis_group`.
7. Sentence Similarity reuses a saved group-catalog classification or checks
   the non-matcher catalog before normal similarity search.
8. A non-matcher updates its group catalog document and `sentences_vector`
   row and creates no relationship.
9. Exact occurrences are found using the same sentence key.
10. Semantic keys are found using the saved Titan vector and only the selected
    group catalog.
11. Matching keys are expanded into eligible target global IDs.
12. Same-application, cross-analysis-group, and ineligible targets are excluded.
13. Every accepted relationship is stored in both directions in
    `titan_matches`.
14. Both directions contain the same cosine distance.
15. Reprocessing updates distances and prevents duplicate relationships in
    both directions.
16. Concurrent workers cannot overwrite one another's JSONB changes.
17. API Transform keeps its existing summary and response-building behavior.
18. Case Similarity keeps its existing calculation behavior and writes the
    Titan result to `titan_similar_cases` without replacing the existing
    case-similarity result.
19. Live and historical messages use the same matching rules.
20. The SQS message is deleted only after the full `tsp_id` succeeds.
21. Saved sentence keys remain available for the future bucketed relationship
    design.
