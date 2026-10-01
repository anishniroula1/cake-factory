# Final Titan Sentence Matching Production Architecture

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
    -> Update titan_matches for the source and every target in both directions
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
2. Checks which sentence keys already exist in the sentence catalog.
3. Skips vector work when the sentence key already exists in the catalog.
4. For a missing key, uses the existing `embedding_type = 2` vector when one
   is available for the global ID.
5. Sends the sentence through the Titan ingest pipeline only when the catalog
   key and reusable embedding are both missing.
6. Waits until every missing catalog vector exists.

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
existing embeddings before its occurrence rows are considered complete.
Temporary Lasso, Titan, database, or OpenSearch failures are retried. A retry
uses stable sentence keys and global IDs, so it does not create duplicate
records or regenerate a vector that can be reused.

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
- Every unique key has one vector in `sentence_semantic_catalog`.
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

1. Three OpenSearch indexes exist: sentence occurrences, the Titan sentence
   catalog, and the tracer/form-language catalog.
2. DOC2Vec splits sentence text only for the live flow; historical processing
   reuses the sentences and global IDs already in `sentences_vector`.
3. DOC2Vec creates and saves a stable SHA-256 `sentence_key` for every
   sentence.
4. Both flows reuse a valid `embedding_type = 2` Titan embedding for the
   global ID and generate a new vector only when one is required and missing.
5. DOC2Vec writes every occurrence and creates only missing catalog vectors.
6. Sentence Similarity checks the non-matcher catalog before the main catalog.
7. A non-matcher updates `sentences_vector` and its occurrence document and
   creates no relationship.
8. Exact occurrences are found using the same sentence key.
9. Semantic keys are found using the saved Titan vector.
10. Matching keys are expanded into eligible target global IDs.
11. Same-application, cross-analysis-group, and ineligible targets are excluded.
12. Every accepted relationship is stored in both directions in
    `titan_matches`.
13. Both directions contain the same cosine distance.
14. Reprocessing updates distances and prevents duplicate relationships in
    both directions.
15. Concurrent workers cannot overwrite one another's JSONB changes.
16. API Transform keeps its existing summary and response-building behavior.
17. Case Similarity keeps its existing calculation behavior and writes the
    Titan result to `titan_similar_cases` without replacing the existing
    case-similarity result.
18. Live and historical messages use the same matching rules.
19. The SQS message is deleted only after the full `tsp_id` succeeds.
20. Saved sentence keys remain available for the future bucketed relationship
    design.
