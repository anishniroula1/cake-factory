# Sentence Similarity Worker

This is a standalone SQS worker. It is not a FastAPI application and it does
not import files from another project.

## Simple code flow

```text
Listen for one SQS message
    -> Read tsp_id
    -> Load 100 current sentences from sentences_vector
    -> Load all 100 catalog vectors in one OpenSearch request
    -> Process the page with up to 16 sentence workers
    -> Search tracer_form_language_catalog first
    -> Update sentence flags in PostgreSQL and sentence_occurrences
    -> Skip semantic matching when the sentence is a non-matcher
    -> Otherwise search sentence_semantic_catalog at the configured threshold
    -> Keep eligible keys from the same analysis group and another application
    -> Save both key directions through the bucket SQL function
    -> Update document state and send tsp_id to API Transform
    -> Delete the source SQS message
```

Live and historical messages use exactly the same flow:

```json
{"tsp_id": 1040}
```

## Why the page has 100 sentences but only 16 workers

The page size controls how many source rows and vectors are loaded together.
The worker count controls simultaneous database and OpenSearch work.

Starting 100 workers would allow one document to request 100 database
connections and 100 OpenSearch connections at once. The OpenSearch client pool
contains 20 connections, so most of those workers would only wait while adding
memory, thread scheduling, and throttling pressure. The default 16 workers keep
the page parallel without exhausting the connection pools. Increase it only
after a load test; configuration currently prevents values above 20.

## Tracer classification

Every source vector is searched against `tracer_form_language_catalog` using:

- The source row's `analysis_group`.
- `isNonMatcher = true` references.
- The configured `TRACER_THRESHOLD`, default 90 percent.

The result updates these `sentences_vector` columns by `global_id`:

- `is_non_matcher`
- `is_tracer`
- `is_form_language`
- `updated_at`

The same three flags are copied to the occurrence document whose OpenSearch
`_id` is that `global_id`. A non-matcher is not searched against the main
catalog and creates no sentence-key relationship.

## Semantic matching

An eligible source uses its already saved 512-dimension vector. Titan is not
called by this service. The catalog search uses IVF `nprobes` and radial
`min_score`, and composite aggregation pages return every key found at or above
`MATCH_THRESHOLD`.

Before saving, PostgreSQL keeps candidate keys that:

- Exist in the same `analysis_group`.
- Have a current occurrence outside the source application.
- Are not marked as non-matcher, tracer, or form language.

The source key itself is not stored as its own match.

## Only one match table

This worker does not read or write the old `matches` table. It only calls:

```sql
upsert_bidirectional_sentence_key_matches(
    analysis_group,
    source_sentence_key,
    matches_jsonb
)
```

The function stores both directions and uses 32 deterministic buckets. Match
values are cosine distance values where `0.0` means an exact vector match.
Relationships are sent to PostgreSQL in groups of 500 so one transaction does
not lock thousands of bucket rows.

The required table, functions, and query indexes are in
`sql/001_sentence_key_matches.sql`. The two `CREATE INDEX CONCURRENTLY`
statements must run outside a Flyway transaction.

## Failure behavior

- A temporary operation is attempted three times, waiting five seconds between
  failures.
- Stable `global_id` and sentence-key bucket identities make retries safe.
- A failed message is not deleted and becomes available again after the
  two-hour SQS visibility timeout.
- The next SQS message is sent before the source message is deleted.

## Run

Copy `.env.example` to `.env` and set the real PostgreSQL, AWS, OpenSearch, and
SQS values.

```bash
make test
make dev
```

`make local` runs the same foreground worker outside Docker.

## Portable copy

Run `make generate`. It creates `sentence_similarity_service_project.py`.
Copy that file anywhere and run it to rebuild the complete project.
