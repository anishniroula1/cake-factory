# Sentence OpenSearch migration

This standalone FastAPI application migrates complete TSP documents from a
paginated external sentence API into the two existing OpenSearch aliases used
by `sentence-semantic-search`.

The S3 CSV is only a document manifest. Its header must be exactly `tsp_id`,
with no other columns, and it contains one TSP ID per row. Each worker owns one
TSP ID, reads all of that document's API pages, creates compatible sentence
keys, indexes every page, sends the page's keys back to another API, and
finally marks the TSP document complete.

There is no PostgreSQL connection in this application.

## Worker flow

For each TSP ID, one worker performs this complete sequence:

1. Call the sentence source API with `tspId` and `page`. The source API owns
   its page size.
2. Convert the returned records to the `sentence-semantic-search` occurrence
   shape and generate the same normalized SHA-256 `sentenceKey`.
3. Create only missing semantic catalog records with the
   `sentenceContentVector` supplied by the source API.
4. Upsert occurrence records using `globalId` as the OpenSearch document ID.
5. Call the sentence-key update API with every `{globalId, sentenceKey}` pair
   from that page.
6. Follow the source response's `nextPage` value until it is `null` or absent.
7. Refresh both aliases, then call the document-state API with the TSP ID.

The document-state API is never called after a partial or failed document. All
three external API calls use the same retry policy. The default is 10 attempts.

Workers detect duplicate `globalId` values within a page and across pages. They
also reject a repeated `nextPage` token so a broken pagination API cannot loop
forever.

## OpenSearch behavior

The occurrence and catalog document shapes match `sentence-semantic-search`.
Catalog documents contain the sentence text and the source API's existing
`sentenceContentVector`. No OpenSearch ingest pipeline is used. Occurrences
contain the sentence metadata and generated key, but not a duplicate vector.

The application never creates, deletes, or changes an OpenSearch index or
alias. It checks that both selected aliases exist before processing the CSV,
which prevents bulk indexing from auto-creating storage. It also does not
construct or ping an OpenSearch client at startup.

Concurrent workers coordinate catalog writes so the same sentence is not
inserted into the catalog simultaneously. Bulk writes are idempotent:
`sentenceKey` is the catalog document ID and `globalId` is the occurrence ID.

### OpenSearch bulk retry statuses

The explicit retry list is a code constant in `sentence_migration/config.py`:

```text
BULK_RETRY_STATUSES = (408, 424, 429, 500, 502, 503, 504)
```

The defaults cover request timeout (`408`), failed dependency (`424`),
throttling (`429`), internal error (`500`), gateway errors (`502`, `504`), and
service unavailable (`503`). A transport failure without an HTTP status also
retries the pending batch. For a partial bulk failure, only documents whose
status is in this list are retried. Any unlisted status fails immediately;
mixed retryable and non-retryable bulk failures also fail immediately.

The code constants use 15 total bulk attempts with a one-second retry pause.
The active values are also visible from `GET /configuration/options`.

## AWS credentials and Swagger selections

S3 uses the application's ambient AWS credential chain directly. It never
assumes an OpenSearch role.

OpenSearch follows the same approach as `sentence-semantic-search`: the client
assumes a configured role and uses `AWSV4SignerAuth`. Add approved role ARNs to
the comma-separated `OPENSEARCH_IAM_ROLES` environment variable.

Occurrence and catalog aliases are configured in these comma-separated lists:

```text
OPENSEARCH_OCCURRENCE_ALIASES
OPENSEARCH_CATALOG_ALIASES
```

The request fields are populated as Swagger dropdowns and reject values that
are not in those lists. All selections are optional; omitting one uses the
first configured value. `GET /configuration/options` returns the active lists.
The API therefore never accepts an arbitrary IAM role or arbitrary alias.

## External API contract

### Read one sentence page

The app sends:

```http
GET https://api.example.com/sentences?tspId=TSP-123&page=1
```

Only `EXTERNAL_API_BASE_URL` is configured. The source path `/sentences` and
the other endpoint paths are code constants. The source API decides how many
records to return; this app never sends `pageSize`.

The response returns its next page number or cursor using `nextPage`:

```json
{
  "items": [],
  "nextPage": 2
}
```

`nextPage` may be an integer, string cursor, or `null`. Nested `data.items` and
`data.nextPage` are also accepted. A bare JSON list is accepted as a terminal
page with no next page.

Each sentence must provide the same fields used by the semantic-search app:

```text
applicationId, tspId, sectionName, globalId, sentIdLocal,
sentenceContent, sentenceContentVector, isTracer, isFormLanguage, sourceType,
createdAt, updatedAt, analysisGroup
```

Every external API sentence field must use camelCase. Snake_case aliases are
rejected. `tspId` may be omitted from a sentence because the worker injects its
current TSP ID. If returned, it must match.

### Update sentence keys for one page

After both OpenSearch writes succeed, the app sends:

```json
{
  "tspId": "TSP-123",
  "sentences": [
    {
      "globalId": 10001,
      "sentenceKey": "64-character-sha256-value"
    }
  ]
}
```

This is posted to the code-defined `/sentences/sentence-keys` endpoint under
`EXTERNAL_API_BASE_URL`.

### Mark the complete document

After the terminal page and OpenSearch refresh, the app posts this to the
code-defined `/documents/state` endpoint:

```json
{
  "tspId": "TSP-123",
  "state": "OPENSEARCH_MIGRATED"
}
```

The app sends `EXTERNAL_API_KEY` in the code-defined `x-api-key` header.

## Configuration

Copy and edit the example:

```bash
cp .env.example .env
```

Important settings:

```text
AWS_REGION
S3_BUCKET

OPENSEARCH_HOST
OPENSEARCH_IAM_ROLES
OPENSEARCH_OCCURRENCE_ALIASES
OPENSEARCH_CATALOG_ALIASES

EXTERNAL_API_BASE_URL
EXTERNAL_API_KEY
```

OpenSearch port `443` and service `es` are fixed code constants. The worker
count (`100`), bulk retry attempts (`15`), bulk retry delay (`1` second),
external API retry attempts (`10`), external retry delay (`2` seconds), and
external timeout (`60` seconds) are also code constants.

The application identity needs `s3:GetObject` for the configured bucket and
`sts:AssumeRole` for every configured OpenSearch role. Each assumed role needs
the required document permissions on the approved aliases.

## Run

Docker Compose:

```bash
make dev
```

Local Python with uv:

```bash
make local
```

Swagger is at <http://localhost:8009/docs>. The Docker health check only calls
the local API and intentionally does not contact OpenSearch or an external API.

Run tests with:

```bash
make test
```

## Endpoints

### Migrate TSP documents

The choices can be omitted to use the first environment values:

```bash
curl -X POST http://localhost:8009/seed/sentences \
  -H 'content-type: application/json' \
  -d '{
    "s3File": "imports/tsp-ids.csv",
    "iamRole": "arn:aws:iam::123456789012:role/sentence-opensearch-primary",
    "occurrenceAlias": "sentence_occurrences",
    "catalogAlias": "sentence_semantic_catalog"
  }'
```

### Explicit OpenSearch ping

```bash
curl -X POST http://localhost:8009/opensearch/ping \
  -H 'content-type: application/json' \
  -d '{}'
```

### Search an approved alias

```bash
curl -X POST http://localhost:8009/opensearch/search \
  -H 'content-type: application/json' \
  -d '{
    "alias": "sentence_occurrences",
    "body": {"size": 10, "query": {"match_all": {}}}
  }'
```

### Guarded read-only OpenSearch request

Use this for cluster health, nodes, CAT views, mappings, settings, stats,
documents, searches, counts, mget, and msearch. Pass a path only; query-string
values belong in `params`.

```bash
curl -X POST http://localhost:8009/opensearch/read-only-request \
  -H 'content-type: application/json' \
  -d '{
    "method": "GET",
    "endpoint": "/_cat/indices",
    "params": {"format": "json"}
  }'
```

Only `GET`, `HEAD`, and a small set of read-only `POST` APIs are allowed.
Index deletion, bulk writes, update/delete-by-query, reindexing, alias changes,
settings changes, mapping changes, refresh/flush/force-merge, restore, rollover,
open/close, shrink, and split operations are rejected before OpenSearch is
called. IAM should still enforce least privilege as the final security layer.

## Portable project generator

`generate_project.py` contains a compressed snapshot of this project. Run it
from any directory to recreate a new `sentence-opensearch-migration` folder:

```bash
python generate_project.py /desired/parent/directory
```

After changing the project, refresh its embedded snapshot with:

```bash
uv run python scripts/refresh_generator.py
```
