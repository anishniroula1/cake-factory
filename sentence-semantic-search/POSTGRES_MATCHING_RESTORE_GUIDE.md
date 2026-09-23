# Legacy PostgreSQL Matching Restore Guide

This guide has been superseded by
[FINAL_SENTENCE_PROCESSING_ARCHITECTURE.md](FINAL_SENTENCE_PROCESSING_ARCHITECTURE.md).
Do not use this older text-array design for the final implementation. It is
kept only as history for the OpenSearch-only demo. The final design uses a
composite `(sentence_key, analysis_group)` primary key and JSONB matching-key
to distance values.

This file saves the PostgreSQL design that was removed from the sentence
semantic-search project. Keep this file in the project. When PostgreSQL is
needed again, ask Codex to read this file and restore the feature.

## Why PostgreSQL was used

OpenSearch stores sentence occurrences and one vector per unique sentence.
PostgreSQL was used as a fast lookup layer for two things:

1. Save which sentence keys directly match each other.
2. Save the complete matching count for each application and analysis group.

This made the normal 90% summary fast because the API did not need to run a
vector search for every sentence each time the UI loaded.

The current occurrence mapping stores globalId as an OpenSearch long. The
document _id is its string form. Keep this behavior if PostgreSQL is restored.
The current catalog uses a trained Faiss IVF model while Titan still creates
the 512-dimension embeddings. Keep both model IDs separate when restoring the
database flow.

## Table 1: sentence_key_matches

One row represents one unique sentence key.

| Column | Type | Purpose |
|---|---|---|
| sentenceKey | Text primary key | SHA-256 key made from normalized sentence text |
| matchingSentenceKeys | PostgreSQL Text array | Direct matching sentence keys |
| createdAt | Timestamp with timezone | Row creation time |
| updatedAt | Timestamp with timezone | Last relationship update time |

Example:

```text
sentenceKey: key-1
matchingSentenceKeys: [key-10, key-40]
```

Relationships must be bidirectional. If key-1 contains key-40, key-40 must
also contain key-1. Do not create transitive relationships automatically. If
key-1 matches key-10 and key-10 matches key-40, that does not prove key-1
matches key-40. Every pair must pass the vector threshold itself.

Use a GIN index on matchingSentenceKeys so deletion can quickly find arrays
that contain a removed key.

Do not add worker queues, attempt counters, lock timestamps, model versions,
status columns, or error columns to this table.

## Table 2: application_match_summary

One row represents one application and analysis group.

| Column | Type | Purpose |
|---|---|---|
| applicationId | Text primary-key part | Application being summarized |
| analysisGroup | Text primary-key part | Group such as Asylee |
| sectionMatchCounts | JSONB | Matching count grouped by sectionName |
| totalMatching | Big integer | Complete matching count for the application |
| updatedAt | Timestamp with timezone | Time the summary was calculated |

Example:

```text
applicationId: A0001
analysisGroup: Asylee
sectionMatchCounts: {Affidavit: 10, B1: 30}
totalMatching: 40
```

Do not add a queue, status, attempts, available time, lock time, or error
columns to this table.

## SQLAlchemy rules

- Use SQLAlchemy 2.x models.
- Keep model and API field names camelCase.
- Use psycopg 3 through postgresql+psycopg.
- Create the tables during the admin initialization endpoint.
- Use a transaction for every bidirectional relationship update.
- Use a PostgreSQL advisory transaction lock so two workers cannot overwrite
  each other's matching-key arrays.
- Use INSERT ON CONFLICT for registering sentence keys and saving summaries.

## Ingestion flow

For each ingestion wave:

1. Create or reuse catalog vectors in OpenSearch.
2. Save sentence occurrences in OpenSearch.
3. Refresh OpenSearch once for the completed wave.
4. Register every unique sentenceKey in sentence_key_matches.
5. For eligible sentences only, search the catalog at MATCH_THRESHOLD.
6. Tracer and form-language sentences still receive vectors, but they do not
   participate in matching.
7. Replace the source key's direct relationship list with the latest valid
   matches.
8. Add or remove the reverse relationship on every affected key.
9. Refresh application summaries for applications using the source key, newly
   matching keys, or removed matching keys.

The OpenSearch cosine score conversion must be:

```text
minimum OpenSearch score = (1 + threshold / 100) / 2
cosine percentage = (2 * OpenSearch score - 1) * 100
```

A 90% threshold therefore uses an OpenSearch minimum score of 0.95.

## Search flow

Sentence-key search:

- At MATCH_THRESHOLD or higher, read the saved direct keys from PostgreSQL,
  read their vectors from OpenSearch, calculate the exact cosine percentage,
  apply the requested threshold, and fetch occurrences from OpenSearch.
- Below MATCH_THRESHOLD, run a live OpenSearch catalog vector search because
  lower-score relationships were not saved during ingestion.
- Searching must never update PostgreSQL.

Application summary:

- At MATCH_THRESHOLD, read totalMatching and sectionMatchCounts from
  application_match_summary.
- Recheck saved matching keys against catalog vectors before page counts are
  returned. This prevents an old bad relationship from being counted.
- At a different threshold, calculate the complete summary live on the first
  page and put its totals in nextToken for later pages.
- Searching must never update PostgreSQL.

## Deletion flow

When deleting by applicationId or tspId:

1. Find the sentence keys and affected application/group pairs before deleting.
2. Delete matching occurrence documents from OpenSearch.
3. Delete a catalog vector only when no remaining occurrence uses its key.
4. Delete the unused sentence_key_matches row.
5. Remove that key from every surviving matchingSentenceKeys array.
6. Delete summaries for a fully removed application.
7. Refresh summaries for every other affected application and analysis group.

The delete-all endpoint must clear both PostgreSQL tables and delete both
OpenSearch indexes and aliases.

## Runtime pieces to restore

Restore these project pieces when PostgreSQL is enabled again:

- SQLAlchemy and psycopg dependencies.
- DATABASE_URL configuration and .env example.
- PostgreSQL Docker service and health check.
- Makefile command that runs PostgreSQL only.
- database_models.py.
- postgres_store.py.
- match_service.py.
- PostgreSQL wiring in components.py and main.py.
- Matching and summary writes in seed_service.py and sentence_service.py.
- PostgreSQL cleanup in deletion_service.py.
- PostgreSQL counts in /health and /stats.
- Tests for models, bidirectional relationships, summaries, deletion, seeding,
  and transaction-safe updates.

## Prompt to use later

Copy this prompt when PostgreSQL matching is needed again:

```text
Read POSTGRES_MATCHING_RESTORE_GUIDE.md completely and restore the PostgreSQL
matching feature in this sentence semantic-search project. Follow the saved
two-table design exactly. Use SQLAlchemy 2.x, psycopg 3, camelCase fields, and
transaction-safe bidirectional relationship updates. Do not add queue, worker,
attempt, status, model-version, lock-time, or error columns. Keep OpenSearch as
the source for sentence occurrences and vectors. Restore fast saved summaries
at MATCH_THRESHOLD while keeping live OpenSearch search for other thresholds.
Restore deletion cleanup, Docker PostgreSQL, environment configuration, tests,
README instructions, and generate_project.py. Preserve the current API field
names and current cosine score calculation. Run all tests and verify a freshly
generated project before finishing.
```
