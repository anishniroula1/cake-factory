# Sentence Similarity Service

This is a standalone SQS worker. The same image is used for real-time and
historical processing; Harness gives each deployment a different source queue.

## Flow

```text
Receive one SQS message with tsp_id
    -> Load 30 sentences from sentences_vector
    -> Read application_id and analysis_group from text_similarity_input
    -> Load their vectors from the selected Asylee or Refugee catalog
    -> Process up to 30 sentences with 30 workers
    -> Reuse saved catalog classification or search the tracer catalog
    -> Update classification in the group catalog and sentences_vector
    -> Skip normal matching for tracer, form-language, or non-matcher sentences
    -> Search the selected sentence catalog for exact and similar keys
    -> Expand keys through sentence_occurrences for other applications
    -> Verify target classifications before saving either direction
    -> Save bidirectional global_id matches in tsp.titan_matches
    -> Repeat until every sentence is complete
    -> Send tsp_id to API Transform
    -> Delete the source SQS message
```

The tracer table and tracer index do not contain `is_tracer`. Sentence
Similarity derives it after a tracer-catalog hit:

```text
is_non_matcher=false and is_form_language=false -> is_tracer=true
is_non_matcher=true                              -> skip sentence matching
is_form_language=true                            -> skip sentence matching
no tracer-catalog hit                            -> search sentence catalog
```

Every tracer-catalog hit skips the normal sentence-catalog search. The search
continues only when the tracer catalog returns no result.

## Titan match buckets

`tsp.titan_matches` uses 16 buckets per source `global_id`. The matching
global ID selects its stable bucket:

```text
bucket = matching global_id % 16
```

The JSONB object stores only the matching ID and cosine distance:

```json
{"200": 0.10, "232": 0.0}
```

PostgreSQL creates the reverse relationship in the same transaction. It also
calculates `match_count` and `verbatim`. Smaller bucket rows reduce the
amount of JSONB rewritten and reduce row-lock contention between workers.

The table and bidirectional upsert function are managed by the shared Flyway
migration.

Database access is separated by table under
`similarity_completed/opensearch/db_store`:

- `SentenceVectorDBStore` loads sentence batches by `tsp_id` and updates
  classifications by `global_id`.
- `TitanMatchesDBStore` saves bidirectional matches through the bucket function.

The handler only controls the order of database and OpenSearch operations.
`TracerFilteringService` owns tracer decisions, source classification, target
classification, and removal of ineligible matches.

OpenSearch access is also separated by index under
`similarity_completed/opensearch/opensearch_store`:

- `OccurrenceOpenSearchStore` finds matching occurrences outside the source application.
- `TracerOpenSearchStore` classifies a sentence using its vector.
- `SentenceCatalogOpenSearchStore` loads records by sentence key, searches semantic
  matches by vector, and updates reusable classification flags by sentence key.

## Configuration

Copy `.env.example` and provide the Vault file, OpenSearch host and role,
occurrence and group catalog aliases, tracer alias, source queue, and API
Transform queue.

Processing constants are kept in
`similarity_completed/opensearch/config.py`:

```text
Source sentence batch: 30 sentences
Workers: 30
Titan vector: 512 dimensions
Retry wait: 5 seconds
Match write batch: 500 occurrences
```

## Run

```bash
make local
```

## Test

```bash
make test
```
