# Semantic sentence search

This is a read-only FastAPI service for searching sentence data that already
exists in Amazon OpenSearch. It never creates, updates, refreshes, or deletes an
OpenSearch document or index.

The service reads three aliases:

- `sentence_occurrences` contains every sentence occurrence and its document
  and application identifiers.
- `sentence_semantic_catalog_asylee` contains unique Asylee sentence vectors.
- `sentence_semantic_catalog_refugee` contains unique Refugee sentence vectors.

The physical indexes, aliases, mappings, Titan embeddings, and IVF model are
managed outside this application.

## Matching rules

Every sentence search uses the same eligibility rules. A source or target
sentence is excluded when:

- `isNonMatcher` is true.
- `isTracer` is true.
- `isFormLanguage` is true.
- `wordCount` is 4 or lower.

These eligibility fields exist in the catalogs, not in
`sentence_occurrences`. The service reads occurrence IDs and sentence keys,
checks those keys in the catalog selected by `analysisGroup`, and joins the
catalog sentence text and word count into the response. Occurrence queries use
only fields defined by the occurrence mapping.

New catalog rows do not contain classification flags until Sentence Similarity
checks them. A missing flag is treated like the initial false value. Once a
flag is explicitly saved as true, the sentence is excluded from every search.

Matching also stays inside the requested `analysisGroup` and excludes
occurrences owned by the source application.

## Search by sentence key

```text
GET /applications/{applicationId}/sentences/semantic-search
    ?sentenceKey=<64-character SHA-256 key>
    &analysisGroup=Asylee
    &threshold=90
```

This returns all eligible exact and semantic matches for one saved sentence
key. Exact matches use the same key. Similar matches use the catalog vector and
the requested cosine threshold.

The paginated version pages unique catalog keys before expanding them into
occurrences:

```text
GET /applications/{applicationId}/sentences/semantic-search-paginated
    ?sentenceKey=<64-character SHA-256 key>
    &analysisGroup=Asylee
    &threshold=90
    &pageSize=100
    &nextToken=<token>
```

## Search by application ID

The summary endpoint processes one source-sentence page and reports how many
matches were found for those sentences:

```text
GET /applications/{applicationId}/sentences/semantic-summary
    ?analysisGroup=Asylee
    &threshold=90
    &pageSize=100
    &nextToken=<token>
```

The complete summary endpoint walks every source page:

```text
GET /applications/{applicationId}/sentences/semantic-summary-all
    ?analysisGroup=Asylee
    &threshold=90
```

The application comparison endpoint ranks other applications by their matching
sentence pairs. It returns ten target applications per page:

```text
GET /applications/{applicationId}/sentences/application-matches
    ?analysisGroup=Asylee
    &threshold=90
    &nextToken=<token>
```

## Search by TSP ID

This endpoint uses only eligible sentences from one TSP document and ranks the
other applications that match them:

```text
GET /documents/{tspId}/sentences/application-matches
    ?analysisGroup=Asylee
    &threshold=90
    &nextToken=<token>
```

The service reads the source application from those occurrence records and
excludes that application from the results.

## Download every match as CSV

These endpoints generate complete CSV downloads without public pagination:

```text
GET /documents/{tspId}/sentences/matches.csv
    ?analysisGroup=Asylee
    &threshold=90

GET /applications/{applicationId}/sentences/matches.csv
    ?analysisGroup=Asylee
    &threshold=90
```

The downloaded files are named `tsp_id-<value>.csv` and
`application_id-<value>.csv`. Each file contains every eligible source-target
sentence pair found at the requested threshold. Source and target sentences
must both pass the non-matcher, tracer, form-language, and five-word rules.

Each CSV row follows the same flat source-versus-target layout used by the SQL
comparison report. It contains source and target application ID, TSP ID,
global ID, local sentence ID, and sentence text. The remaining columns contain
distance, match percentage, raw OpenSearch score, `exact` or `similar` match
type, analysis group, and requested threshold.

The service writes the export to a temporary server-side file while processing
all internal OpenSearch pages. The temporary file is deleted after the download
finishes.

## Read-only system endpoints

```text
GET /health
GET /stats
GET /index-documents?index=sentence_occurrences&count=10
GET /index-documents?index=sentence_semantic_catalog_asylee&count=10
GET /index-documents?index=sentence_semantic_catalog_refugee&count=10
```

The catalog preview includes the decoded embedding list. These endpoints do not
modify OpenSearch.

## Configuration

Copy `.env.example` to `.env`.

Search configuration uses only:

```text
OPENSEARCH_IVF_NPROBES_ENABLED=false
OPENSEARCH_IVF_NPROBES=128
OPENSEARCH_OCCURRENCE_ALIAS=sentence_occurrences
OPENSEARCH_ASYLEE_CATALOG_ALIAS=sentence_semantic_catalog_asylee
OPENSEARCH_REFUGEE_CATALOG_ALIAS=sentence_semantic_catalog_refugee
```

Every endpoint uses the catalog belonging to its `analysisGroup`. Asylee
requests use `OPENSEARCH_ASYLEE_CATALOG_ALIAS`, while Refugee requests use
`OPENSEARCH_REFUGEE_CATALOG_ALIAS`.

Set `OPENSEARCH_IVF_NPROBES_ENABLED=true` only for a catalog that supports IVF
`nprobes`. When it is false, the application does not send
`method_parameters.nprobes` in any vector query. The numeric nprobes value is
ignored while the flag is false.

The OpenSearch client still requires its connection values:

```text
AWS_REGION=us-east-1
OPENSEARCH_HOST=search-your-domain.us-east-1.es.amazonaws.com
IAM_ACCESS_ROLE=
```

Local AWS credentials may use the standard AWS variables
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`.
When the application runs with a pod or instance role, those values can remain
empty.

Port 443, AWS service `es`, vector dimension 512, and the default 90 percent
threshold are code constants because they do not vary for this application.

## Run

Install dependencies and run the tests:

```bash
make test
```

Run locally:

```bash
make local
```

Swagger is available at:

```text
http://localhost:8008/docs
```
