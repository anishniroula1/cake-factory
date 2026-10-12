# Titan Case Similarity service

For the complete statement-by-statement flow, transaction boundaries, failure
behavior, performance notes, and operational queries, read
`CASE_SIMILARITY_COMPLETE_FLOW.md`.

## Purpose

This application turns sentence-level Titan matches into case-level results.
It does not create embeddings and it does not run a vector search. Doc2Vec and
Sentence Similarity complete that work before this service receives a message.

Sentence Similarity stores final bidirectional sentence relationships in
`tsp.titan_matches`. This application counts those relationships by
`application_id` and saves the case result in `tsp.titan_similar_cases`.

The folder is named `opensearch` because it belongs to the new OpenSearch/Titan
pipeline. The application itself reads PostgreSQL only.

## What the current application does

The current Case Similarity application supports the existing Word2Vec flow
and an additional Titan/refactored path. Its main flow works as follows.

1. It continuously listens to the Case Similarity SQS queue.
2. Before reading work, it calls the service-status API. When the service is
   paused, it waits and checks again later.
3. It collects a batch of SQS messages. The configured batch can contain up to
   30 `tsp_id` values.
4. It expects each message to include an analysis group and can also inspect an
   embedding type.
5. It separates the received batch by analysis group so Asylee and Refugee
   documents are processed separately.
6. For each group, it uses the received `tsp_id` values to find impacted
   `a_number` values in `text_similarity_input`.
7. It loads eligible sentences from `sentences_vector`. The existing query
   checks current, tracer, form-language, and non-matcher fields.
8. It reads sentence relationships from the existing match table. The current
   flow may read JSONB lists from `matches`; its refactored flow can read the
   one-directional sentence-pair tables.
9. It builds Pandas DataFrames and Python collections to turn sentence matches
   into case-to-case counts.
10. It uses `a_number` as the main case identity. Some paths use
    `ApplicationIdLookup` to translate between `a_number` and `application_id`.
11. A case is considered similar when at least two qualifying sentence groups
    connect the two cases.
12. A case is considered TAH when at least three qualifying sentence groups
    connect the two cases.
13. It updates both the received cases and cases found on the other side of a
    relationship so the case result remains bidirectional.
14. It inserts or updates the current similar-case table. The old implementation
    can append new values to an existing saved result instead of rebuilding the
    complete result from one source of truth.
15. It sends the received `tsp_id` values to the next SQS queue. It may also
    look up and send additional document IDs belonging to affected cases.
16. For the standard flow, it checks `validation.kafka_ta_hit_lookup`, publishes
    new or changed TAH values to Kafka, and writes the broker delivery result.
17. The current Titan branch skips Kafka publication.
18. It deletes the source SQS messages only after the required case and Kafka
    work succeeds.

The current application carries multiple flows in the same process. It has
batch orchestration, analysis-group grouping, legacy and refactored match
logic, DataFrame transformations, service-status API calls, and different
Kafka behavior depending on the selected embedding type.

## What this application does

The new application is only for the Titan/OpenSearch sentence flow. It handles
one document message at a time and uses the final relationships already saved
by Sentence Similarity.

### Input

One SQS message represents one document:

```json
{
  "tsp_id": 100
}
```

No application list, analysis group, `a_number`, or embedding type is required
in the message. The application loads those values from PostgreSQL.

### Step 1: Receive one document

The SQS processor reads one message and validates that `tsp_id` is present. It
adds the `tsp_id` to the common log message so logs for that document are easy
to follow.

### Step 2: Load the source case

The service reads `tsp.text_similarity_input` using `tsp_id` and loads:

- `application_id`
- `a_number`
- `analysis_group`

`application_id` is the case identity used by the new summary flow. `a_number`
is retained only where the Kafka payload requires it.

### Step 3: Find every affected application

The service starts with the source `application_id` and finds two target sets:

1. Current targets found in `tsp.titan_matches`.
2. Targets stored in the source application's previous
   `tsp.titan_similar_cases` summary.

Current targets must be rebuilt because their counts may have increased or
changed. Previous targets must also be rebuilt because a relationship may have
disappeared. Without the previous list, stale case matches could remain saved.

The incoming SQS message does not provide affected application IDs. The
database is the source of truth for this list.

### Step 4: Lock affected summaries

The service calls `tsp.refresh_titan_similar_cases` with the source
`application_id`, analysis group, and the two thresholds. The PostgreSQL
function sorts all affected application IDs before taking locks. For each
application, it takes a transaction advisory lock. This protects both existing
summary rows and rows that have not been created yet.

Existing `tsp.titan_similar_cases` rows are also locked with `FOR UPDATE`.
Every worker requests locks in the same application-ID order, reducing the
chance of a deadlock when two document messages affect the same cases.

The locks last until the transaction commits or rolls back. Reads can continue,
but another writer using the same locking rule waits.

### Step 5: Count sentence relationships by case

For each affected application, the service reads that application's rows from
`tsp.titan_matches`.

Each row owns one source `global_id`. Its `matches` JSONB object uses matching
target `global_id` values as keys. The query expands those keys and connects
each target `global_id` to its owning `application_id`.

The service counts unique source/target sentence pairs per target application.
Every qualifying occurrence pair counts. For example:

- Application 100 sentence 1 matches Application 200 sentence 10.
- Application 100 sentence 1 matches Application 200 sentence 11.
- Application 100 sentence 2 matches Application 200 sentence 12.

Application 100 has three sentence relationships with Application 200.

Relationships pointing back to the same application are excluded.

### Step 6: Apply the case rules

The default rules are:

- Zero or one sentence relationship: not a similar case and not a TAH case.
- Two sentence relationships: similar case only.
- Three or more sentence relationships: both a similar case and a TAH case.

The rule is checked separately for every other application. It is not based on
the total number of similar applications.

For example, assume the service is building the summary for Application 100:

| Other application | Matching sentence pairs with Application 100 | Saved as similar | Saved as TAH |
|---:|---:|---|---|
| 200 | 1 | No | No |
| 300 | 2 | Yes | No |
| 400 | 3 | Yes | Yes |
| 500 | 6 | Yes | Yes |

The saved result for Application 100 is:

```json
{
  "application_id": 100,
  "similar_case_ids": [300, 400, 500],
  "similar_case_count": 3,
  "tah_case_ids": [400, 500],
  "tah_case_count": 2,
  "is_tah": true
}
```

There are three similar cases because Applications 300, 400, and 500 each have
at least two matching sentence pairs. There are only two TAH cases because only
Applications 400 and 500 have at least three matching sentence pairs.

Every application in `tah_case_ids` is also included in `similar_case_ids`.
`is_tah` becomes `true` when `tah_case_ids` contains at least one application:

```text
is_tah = tah_case_count > 0
```

The IDs in these lists are real `application_id` values. They are not sentence
`global_id` values, document `tsp_id` values, or `a_number` values.

### Step 7: Save complete summaries

The PostgreSQL function upserts one complete row into
`tsp.titan_similar_cases` for every affected application. The service supplies:

- `application_id`
- `analysis_group`
- `similar_application_ids`
- `similar_case_ids`
- `tah_case_ids`

PostgreSQL generates `similar_case_count`, `tah_case_count`, and `is_tah`
directly from the saved lists. The application never sends those calculated
values during the upsert. `last_modified` is also set by PostgreSQL.

All three lists contain real `application_id` values. The new flow does not
create a separate generated application identity.

The whole list is replaced on every rebuild. If an application no longer has
matches, empty lists and zero counts are saved so old relationships are
removed. All affected summaries commit in one transaction.

The full replacement table and refresh function are in
`TITAN_SIMILAR_CASES.sql`.

### Step 8: Publish live TAH changes when enabled

Kafka publication is controlled by:

```text
SEND_TAH_KAFKA_EVENTS=true
```

For the live flow, set the toggle to `true`. The service then:

1. Reads the saved `is_tah` value for each affected application.
2. Reads `a_number` and `analysis_group` from `text_similarity_input`.
3. Uses `a_number` to find `receipt_number` in
   `landing.global_document_registry`.
4. Ignores a receipt number if it points to more than one application.
5. Compares the new TAH value with
   `validation.kafka_ta_hit_lookup` for the current Kafka environment.
6. Skips the event when the same TAH value was already delivered.
7. Publishes a new event when no lookup row exists or `is_tah` changed.

The Kafka payload is:

```json
{
  "is_tah": true,
  "application_url": "https://ui.rfs-ata.uscis.dhs.gov/ui/main/100",
  "primary_a_number": "900001",
  "analysis_group": "Asylee"
}
```

The Kafka message key is `receipt_number`. The application uses
`confluent_kafka.Producer` directly and waits for the delivery callback.

Only after Kafka confirms that the broker accepted the message does the service
insert or update `validation.kafka_ta_hit_lookup` with:

- the delivered TAH value
- Kafka environment
- broker timestamp
- partition
- offset

The lookup row confirms broker delivery. It does not confirm that Global has
consumed or completed the message.

### Step 9: Historical behavior

For historical ingestion, use:

```text
SEND_TAH_KAFKA_EVENTS=false
```

The service still rebuilds `tsp.titan_similar_cases`, but it does not:

- create a Kafka producer connection
- read `validation.kafka_ta_hit_lookup`
- publish old TAH hits
- update Kafka delivery information

Kafka environment values and certificates are not required while the toggle is
off.

## Run Kafka locally

The local Docker setup runs one Kafka broker in KRaft mode. It does not run the
Case Similarity application, PostgreSQL, or SQS.

Copy `.env.example` to `.env` and start Kafka:

```bash
make kafka
```

The broker is available at `localhost:9092`. The command also creates the
`rfs.ata.text_analytics.hit` topic used by this service.

Run the application locally with:

```bash
make local
```

`make local` starts Kafka when it is not already running. Use `make kafka-logs`
to view broker logs and `make kafka-stop` to stop the broker. The Kafka data is
kept in a Docker volume, so stopping the container does not delete its topics.

### Step 10: Continue the document flow

After all enabled case and Kafka work succeeds, the service sends one message
to the next SQS queue:

```json
{
  "source": "P2",
  "tsp_id": "100",
  "sent_by": "case-similarity"
}
```

The next service receives the same document identity. A list of affected
applications is not added to the message.

### Step 11: Delete the source message

The source SQS message is deleted only after:

1. Case summaries commit successfully.
2. Required live Kafka events are confirmed and recorded.
3. The next SQS message is sent successfully.

When an operation fails, the service logs the error and does not delete the
source message. SQS can retry it and eventually move it to the configured DLQ.
Database and Kafka operations have a small local retry before control returns
to SQS.

## Main differences

| Area | Current application | New Titan application |
|---|---|---|
| Message size | Processes a batch of document messages | Processes one `tsp_id` message |
| Pause handling | Calls a service-status API | No status API call |
| Flow support | Word2Vec, Titan, and optional refactored paths | Titan/OpenSearch path only |
| Sentence source | Existing match lists or pair tables | `tsp.titan_matches` JSONB buckets |
| Vector search | Not done here | Not done here |
| Case identity | Mainly `a_number` | Real `application_id` |
| Application lookup | Can use `ApplicationIdLookup` | Not used |
| Transform logic | Pandas DataFrames and Python collections | PostgreSQL counting and small Python result mapping |
| Reverse updates | Separate insert/update handling | Rebuilds all current and previous affected applications |
| Saved table | Existing similar-case tables | `tsp.titan_similar_cases` |
| Update style | Can append or merge saved values | Replaces the complete summary |
| Concurrency | Multiple batch paths | Ordered advisory and row locks |
| Kafka | Standard flow publishes; Titan branch skips | Environment toggle controls live versus historical |
| Kafka producer | Custom producer class | Direct `confluent_kafka.Producer` |
| Next queue | Can send source and other affected document IDs | Sends the one completed `tsp_id` |
| Source deletion | After case and required Kafka work | After case, enabled Kafka, and next-queue work |

## Main tables

| Table | Purpose |
|---|---|
| `tsp.text_similarity_input` | Connects `tsp_id` to application, a-number, and analysis group. |
| `tsp.titan_matches` | Stores bidirectional sentence relationships in JSONB buckets. |
| `tsp.titan_similar_cases` | Stores 1+ matched applications, 2+ similar cases, 3+ TAH cases, and generated counts. |
| `landing.global_document_registry` | Converts a-number to the receipt number used as the Kafka key. |
| `validation.kafka_ta_hit_lookup` | Prevents unchanged events and records confirmed Kafka delivery. |

## Shared libraries

The `ta_utils` and `ta_db_connection` folders in this project mimic shared team
libraries. They can be removed when this code is moved into the main repository
that already provides those packages.
