# Semantic sentence search

This API saves sentences in OpenSearch and uses Amazon Titan vectors to find
sentences with the same or similar meaning.

## Final production processing plan

The approved DOC2Vec, sentence-similarity, PostgreSQL, OpenSearch, and UI
transformation design is documented in
[FINAL_SENTENCE_PROCESSING_ARCHITECTURE.md](FINAL_SENTENCE_PROCESSING_ARCHITECTURE.md).
Its editable architecture diagram is
[FINAL_SENTENCE_PROCESSING_ARCHITECTURE.drawio](FINAL_SENTENCE_PROCESSING_ARCHITECTURE.drawio).

The final production plan uses three OpenSearch indexes. The current demo
below still has two indexes; the third tracer/form-language reference catalog
belongs to the planned Sentence Similarity flow.

The complete PostgreSQL table, index, bidirectional-write, and lookup SQL is
in [POSTGRES_SENTENCE_MATCHING_SCHEMA.md](POSTGRES_SENTENCE_MATCHING_SCHEMA.md).

The scalable JSONB bucket design for sentence-key relationships is documented
in
[POSTGRES_SENTENCE_MATCH_BUCKET_DESIGN.md](POSTGRES_SENTENCE_MATCH_BUCKET_DESIGN.md).
It explains why buckets are used, how PostgreSQL TOAST helps, how Python and
the shared SQL function write matches, and what must be measured before the
72-million-record data set grows toward 100–150 million records.
For the `sentence_key_matches` table and its upsert function, the bucket guide
supersedes the original one-row sections in the complete schema guide.

That final plan supersedes the older PostgreSQL restore guide for future
implementation decisions. The current demo behavior below still describes the
code that is present today.

The project currently uses OpenSearch only. There is no PostgreSQL connection,
table, background matching job, or saved match list. This makes seeding much
simpler and faster. Matches are calculated when a search endpoint is called.

If saved matching lists and saved application totals are needed later, read
[POSTGRES_MATCHING_RESTORE_GUIDE.md](POSTGRES_MATCHING_RESTORE_GUIDE.md). That
file contains the table design, matching rules, and a ready-to-use prompt for
adding the database flow back.

## What is stored

The project creates two OpenSearch indexes.

### Sentence occurrences

Index: sentence_occurrences-v1  
Alias: sentence_occurrences

This index stores every sentence occurrence. It contains the application,
document, section, global ID, sentence text, flags, analysis group, and
sentenceKey.

globalId is stored as an OpenSearch long, so its value must be numeric. The
internal OpenSearch document _id is the string form of that same number. Values
with letters are rejected, and leading zeros are not preserved.

If the same globalId is indexed again, OpenSearch replaces the old occurrence.
It does not create a second occurrence. The latest row wins. If its sentence
text changed, the new sentence key gets a catalog vector. The old catalog entry
is kept because another occurrence may still use it.

The sentenceKey is a SHA-256 value made from normalized sentence text. The same
sentence text always gets the same key.

### Semantic catalog

Production design: Faiss IVF-SQ16 with 512-dimension Titan vectors and cosine
similarity.

This index stores one record for each unique sentenceKey. It contains the
sentence text and its 512-dimension Titan vector. The physical index and alias
come from OPENSEARCH_CATALOG_INDEX and OPENSEARCH_CATALOG_ALIAS. Production
must point them at the approved SQ16 catalog, not the older PQ comparison
catalog.

Titan creates the embedding. The trained IVF-SQ16 model does not create
embeddings; it organizes existing embeddings into buckets and stores their
values as 16-bit numbers. The search uses radial min_score with
OPENSEARCH_IVF_NPROBES, so no fixed top-K limit is used.

If the same sentence appears in many documents, its vector is created only
once. All occurrence records use the same sentenceKey.

## Seeding flow

1. Read the CSV from top to bottom. The file is not sorted.
2. Validate every sentence and create its sentenceKey.
3. Check which sentence keys already exist in the catalog.
4. Send only new catalog records through the OpenSearch ingest pipeline. The
   pipeline calls Titan and creates the 512-dimension vector.
5. Each worker saves its occurrence batch immediately after that batch's
   catalog vectors succeed. Occurrence writes retry temporary failures too.
6. Refresh both indexes once per worker wave.

The seed flow does not search for matches. It does not build summaries. The
SEED_WORKERS controls how many complete batches can run at the same time. Each
worker creates missing catalog vectors first and then saves that batch's
occurrences. If another worker owns the same sentenceKey, the later worker
waits for that vector before saving its occurrences.

If Lasso or Titan temporarily fails, all catalog workers share a five-second
cooldown before their next catalog attempt. Requests already running are not
cancelled. An occurrence failure pauses only the worker that owns that batch
because occurrence writes do not call Lasso or Titan.

Input CSV columns:

```text
applicationId,tspId,sectionName,globalId,sentIdLocal,sentenceContent,isTracer,isFormLanguage,sentenceKey,sourceType,createdAt,updatedAt,analysisGroup
```

sentenceKey, createdAt, and updatedAt may be empty. The API creates the key and
fills missing dates. globalId must contain a number that fits in an OpenSearch
signed 64-bit long.

## Search flow

Tracer and form-language sentences still get vectors and occurrence records.
They are left out of semantic matching results.

The default analysis group is Asylee. The caller can send another analysis
group. The caller can also send any threshold from 1 through 100.

### Search by sentence key

```text
GET /applications/A1/sentences/semantic-search
    ?sentenceKey=<64-character SHA-256 key>
    &analysisGroup=Asylee
    &threshold=90
```

The API:

1. Checks that sentenceKey has an eligible occurrence in the requested
   analysis group.
2. Rejects a source sentence when it is a tracer or form-language sentence.
3. Reads the saved vector for sentenceKey.
4. Runs a live vector search in the catalog.
5. Keeps catalog results at or above the requested cosine percentage.
6. Reads matching occurrences outside the provided application.

Returned occurrences must use the requested analysisGroup. Tracer and
form-language occurrences are always removed. Results are sorted by
matchPercentage from highest to lowest, followed by sentenceKey and globalId
when scores are equal.

applicationId is used only to exclude that application's occurrences from the
results. The matching sentence key does not have to belong to that application.

No Titan call is made during this search because the vector already exists.
For IVF, the API searches the number of closest buckets configured by
OPENSEARCH_IVF_NPROBES. A larger value improves recall but adds search work.

Use the paginated endpoint when the UI only needs one small result page:

```text
GET /applications/A1/sentences/semantic-search-paginated
    ?sentenceKey=<64-character SHA-256 key>
    &analysisGroup=Asylee
    &threshold=90
    &pageSize=50
```

Pagination happens in the semantic catalog. The endpoint returns at most
pageSize unique matching sentence keys. It then puts every eligible occurrence
for those keys directly in one flat matches list. There is no catalog object
with a nested occurrences list.

Each occurrence includes its normal sentence fields plus matchPercentage and
matchType. The flat list is sorted by matchPercentage from highest to lowest.
Equal scores are sorted by sentenceKey and globalId.

The occurrence records are loaded with internal OpenSearch search-after pages,
but there is no public occurrence token. The public nextToken moves to the next
catalog page. Send it back unchanged.

returnedCatalogMatches tells how many unique catalog keys produced the current
page. totalMatches, returnedMatches, exactMatchCount, and similarMatchCount
cover the flat occurrence records. matchingCountScope is
currentCatalogPageOccurrences.

A catalog key with no occurrence in the requested analysis group is skipped.
The endpoint continues reading catalog keys until it fills the requested page
or reaches the end. A page can still be large when its keys have many
occurrences.

The original semantic-search endpoint still returns every match. Its
matchingCountScope is allResults. Use that endpoint only when the caller really
needs all matching rows in one response.

### Compare one application with other applications

Use this endpoint to rank other applications by matching sentence pairs:

```text
GET /applications/A1/sentences/application-matches
    ?analysisGroup=Asylee
    &threshold=90
```

The endpoint returns 10 applications per page, ranked by matchCount from high
to low. One match means one source globalId paired with one target globalId.
For example, one source sentence matching three sentences in another
application adds three matches. If a second source sentence matches one of
those targets, that source-target pair adds another match.

Every returned match shows the source location, target location, matchType,
and matchPercentage. Both sides must use the requested analysisGroup. Tracer
and form-language sentences are excluded on both sides. Use nextToken exactly
as returned to read the next 10 applications.

The ranking requires evaluating the complete source application before the
top 10 applications are known. Pagination limits the response size, but it
does not remove that full comparison work.

### Application summary

```text
GET /applications/A1/sentences/semantic-summary
    ?analysisGroup=Asylee
    &threshold=90
    &pageSize=100
```

The normal summary endpoint only searches the sentences in the requested page.
For example, pageSize=50 runs matching for no more than 50 source sentences.
Those source searches are sent to OpenSearch in batched msearch requests.

totalMatching and sectionMatches cover only the current page. summaryScope is
currentPage. totalSentences is still the number of eligible sentences in the
application because that is a normal occurrence-index count, not 20,000 vector
searches.

Use nextToken exactly as returned:

```text
GET /applications/A1/sentences/semantic-summary
    ?analysisGroup=Asylee
    &threshold=90
    &pageSize=100
    &nextToken=<returned token>
```

Use the separate endpoint below only when every sentence and one complete
application total are required:

```text
GET /applications/A1/sentences/semantic-summary-all
    ?analysisGroup=Asylee
    &threshold=90
```

This endpoint walks through all summary pages and combines them. Its
summaryScope is allSentences. It can take much longer for a large application
because OpenSearch must run vector matching for every eligible source sentence.

## Other endpoints

```text
GET    /health
GET    /stats
GET    /index-documents?index=sentence_occurrences&count=10
GET    /index-documents?index=sentence_semantic_catalog&count=10
GET    /applications/{applicationId}/sentences/semantic-summary
GET    /applications/{applicationId}/sentences/semantic-summary-all
GET    /applications/{applicationId}/sentences/semantic-search
GET    /applications/{applicationId}/sentences/semantic-search-paginated
GET    /applications/{applicationId}/sentences/application-matches
POST   /admin/init
POST   /admin/seed?reset=false&csvPath=data/seed.csv
POST   /sentences
DELETE /applications/{applicationId}?confirm=true
DELETE /documents/{tspId}?applicationId=A1&confirm=true
DELETE /admin/storage?confirm=true
```

The catalog preview includes the decoded embedding list. Deleting an
application or document also deletes catalog vectors that are no longer used by
any occurrence.

## Environment setup

Copy .env.example to .env and fill in the AWS and OpenSearch values.

Important values:

```text
AWS_REGION=us-east-1
OPENSEARCH_HOST=search-your-domain.us-east-1.es.amazonaws.com
OPENSEARCH_SEMANTIC_MODEL_ID=<deployed OpenSearch model ID>
OPENSEARCH_IVF_MODEL_ID=sentence-ivf-sq16-512-nlist16384-v1
OPENSEARCH_IVF_NPROBES=128
OPENSEARCH_INGEST_PIPELINE=sentence_bedrock_embedding_pipeline
OPENSEARCH_CATALOG_INDEX=sentence_semantic_catalog-ivf-sq16-v1
OPENSEARCH_CATALOG_ALIAS=sentence_semantic_catalog
VECTOR_DIMENSION=512
INDEX_SHARDS=18
INDEX_REPLICAS=1
SEED_BATCH_SIZE=100
SEED_WORKERS=4
MATCH_THRESHOLD=90
```

The deployed Titan model, ingest pipeline, trained IVF-SQ16 model, and catalog must
all use dimension 512. A different dimension causes index creation or indexing
to fail.

The final 100-million-vector resources, shard count, training values, cost, and
production checks are in
[IVF_SQ16_100M_FINAL_CAPACITY_COST.md](IVF_SQ16_100M_FINAL_CAPACITY_COST.md).

[IVF_PQ_MIGRATION.md](IVF_PQ_MIGRATION.md) is kept only as the old PQ comparison
guide. PQ is not the final production choice.

The older [IVF_CATALOG_MIGRATION.md](IVF_CATALOG_MIGRATION.md) explains the
original IVF migration that moves one shared catalog alias.

After creating an IVF-SQ16 test index, use
[IVF_SQ16_STORAGE_CHECK.md](IVF_SQ16_STORAGE_CHECK.md) to compare its disk
storage and loaded native memory with the IVF Flat index.

## Run the API

Run in Docker:

```bash
make dev
```

Swagger opens at:

```text
http://localhost:8008/docs
```

Run locally:

```bash
make local
```

Run tests:

```bash
make test
```

## Reset warning

Sending reset=true to the seed endpoint deletes and recreates the project
indexes before loading the CSV. It also removes an old catalog currently behind
the project alias. Sending reset=false keeps existing data and creates only
missing catalog vectors.

If the occurrence index was created when globalId used the keyword mapping,
recreate it before loading numeric IDs. OpenSearch cannot change an existing
field from keyword to long in place.

The admin storage delete endpoint removes both indexes and aliases. It requires
confirm=true.
