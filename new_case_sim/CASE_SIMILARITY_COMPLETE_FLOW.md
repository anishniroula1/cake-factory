# Titan Case Similarity: complete flow

## 1. What this service is for

This service converts saved sentence matches into an application-level result.

Sentence Similarity has already done the expensive work before this service
starts. It has already:

1. classified tracer, form-language, and non-matcher sentences;
2. searched the sentence catalog in OpenSearch;
3. expanded matching sentence keys into occurrence `global_id` values; and
4. saved the sentence relationships bidirectionally in `tsp.titan_matches`.

Case Similarity does not create embeddings and does not search OpenSearch. It
reads the saved sentence relationships from PostgreSQL, counts them by
`application_id`, and stores the result in `tsp.titan_similar_cases`.

The folder is named `opensearch` because this worker belongs to the new Titan
and OpenSearch pipeline. The worker itself uses PostgreSQL, SQS, and optionally
Kafka.

## 2. The complete flow in one view

```text
Case Similarity SQS message
        |
        | {"tsp_id": 100}
        v
Load text_similarity_input
        |
        | application_id, a_number, analysis_group
        v
Call refresh_titan_similar_cases
        |
        | read titan_matches for the full application
        | find current and previously connected applications
        | lock affected application summaries in sorted order
        | count unique sentence occurrence pairs
        | rebuild all affected case summaries
        v
Commit titan_similar_cases
        |
        +--> Kafka enabled?
        |       |
        |       +--> publish only new or changed TAH values
        |       +--> save Kafka delivery acknowledgement
        |
        v
Send {"source": "case_similarity", "tsp_id": 100} to the next SQS queue
        |
        v
Delete the original SQS message
```

## 3. Important IDs

| Value | Meaning | Where it is used |
|---|---|---|
| `tsp_id` | One document | Incoming and outgoing SQS messages |
| `application_id` | One application or case | Case identity and summary primary key |
| `a_number` | Business case identifier | Kafka payload and receipt lookup |
| `global_id` | One sentence occurrence | Sentence relationship identity |
| `analysis_group` | Asylee or Refugee grouping | Prevents case counting across groups |
| `receipt_number` | Global Kafka record key | Kafka message key |

An application may contain many `tsp_id` documents. Case Similarity receives
one `tsp_id`, but its calculation is for the complete `application_id`.

For example, application 100 may contain 76 documents. Receiving the `tsp_id`
for document 76 does not cause 76 separate document calls. The service resolves
application 100 and reads all saved `titan_matches` rows owned by application
100 through an application-level database query.

## 4. What the service does not do

This worker does not:

- split document text into sentences;
- create `sentence_key` values;
- create Titan embeddings;
- call Bedrock or Lasso;
- call an OpenSearch vector query;
- classify tracer or form-language sentences;
- write sentence-level relationships;
- receive a list of affected applications in SQS; or
- send affected application IDs to the next queue.

Those responsibilities belong to earlier services. Case Similarity treats
`tsp.titan_matches` as the sentence-match source of truth.

## 5. Startup

The entry point is:

```text
case_similarity_service.opensearch.main.run_opensearch_flow
```

Startup performs these steps:

1. Create `Dependencies` unless a prepared dependency object was supplied.
2. Load configuration from Vault and environment variables.
3. Create the PostgreSQL connection and SQLAlchemy session factory.
4. Create one `AwsSQS` client for the Case Similarity receive queue.
5. Create the database stores.
6. Create a Kafka producer only when `SEND_TAH_KAFKA_EVENTS=true`.
7. Create `KafkaProcessor` and `CaseSimilarityHandler`.
8. Start the continuous SQS processor loop.
9. When the process stops, clear the logger's generic message and dispose the
   PostgreSQL connection pool.

### Database credentials

For `ENVIRONMENT=local`, the database connection receives:

```text
use_dynamic_credentials = false
```

For any non-local environment, the database connection receives the file from:

```text
VAULT_YAML_PATH
```

The normal pool keeps 20 database connections, with up to 10 temporary overflow
connections when the pool is busy.

## 6. SQS input

The service asks SQS for one message at a time. The expected body is:

```json
{
  "tsp_id": 100
}
```

`tsp_id` may arrive as a number or string. The service keeps the supplied type
when forwarding it.

The message does not need `application_id`, `a_number`, `analysis_group`, or an
embedding type. Those values are read from PostgreSQL.

The configured visibility timeout is 7200 seconds, or two hours. The service
does not run a visibility heartbeat.

### Missing `tsp_id`

If `tsp_id` is absent or empty:

1. an error is logged;
2. no database work is performed;
3. no next-queue message is sent; and
4. the source message is not deleted.

### Log context

After reading `tsp_id`, the processor calls:

```text
set_generic_message("tsp_id=<value>")
```

The shared logger can add that value to every log for the message. The value is
cleared in a `finally` block after the message finishes or fails.

## 7. Load the document context

The first database query reads `tsp.text_similarity_input`:

```sql
SELECT tsp_id, application_id, a_number, analysis_group
FROM tsp.text_similarity_input
WHERE tsp_id = :tsp_id;
```

Example result:

| tsp_id | application_id | a_number | analysis_group |
|---:|---:|---:|---|
| 100 | 20 | 900001 | Asylee |

The service converts this row into `DocumentContext`.

`one_or_none()` is used intentionally:

- zero rows become `PermanentDataError` because the application cannot be
  identified;
- one row is accepted; and
- unexpected duplicate rows fail rather than choosing an arbitrary row.

Temporary SQLAlchemy errors are retried up to three times with a five-second
wait. A permanent missing row is not locally retried.

## 8. The sentence-level input table

`tsp.titan_matches` stores the relationships created by Sentence Similarity.
One source sentence can have as many as 16 bucket rows.

Example:

| global_id | tsp_id | application_id | bucket | matches |
|---:|---:|---:|---:|---|
| 10 | 100 | 20 | 1 | `{"51": 0.10, "83": 0.24}` |
| 10 | 100 | 20 | 7 | `{"75": 0.00}` |
| 11 | 101 | 20 | 3 | `{"91": 0.08}` |

The JSONB key is the matching target `global_id`. The JSONB value is cosine
distance, where `0.0` is an exact match. Case Similarity currently uses the
relationship itself and does not use distance when applying case thresholds.

Because relationships are bidirectional, if global ID 10 stores global ID 51,
the row owned by global ID 51 also stores global ID 10. This lets the case
summary be rebuilt from either application.

`match_count` and `verbatim` are generated sentence-bucket values. The current
case refresh function does not read those generated columns. It expands the
keys from `matches` directly.

## 9. Find every affected application

The handler calls:

```text
tsp.refresh_titan_similar_cases(
    source_application_id,
    analysis_group,
    2,
    3
)
```

Before rebuilding summaries, the function makes one sorted list of affected
applications from three sources.

### 9.1 The source application

The application found from the received `tsp_id` is always included.

Example:

```text
source application = 100
affected = [100]
```

This means an application with no sentence matches still gets its old summary
cleared to empty lists.

### 9.2 Applications currently connected through `titan_matches`

The function:

1. reads every bucket row owned by the source application;
2. expands every JSONB target `global_id` key;
3. finds the `application_id` that owns each target global ID; and
4. adds that target application to the affected list.

Self-matches are excluded.

Example:

```text
Application 100 has target global IDs owned by applications 200 and 300.
affected = [100, 200, 300]
```

The target-owner lookup currently uses a distinct global-ID/application-ID view
of `tsp.titan_matches`. Bidirectional sentence writes are therefore important:
the target global ID needs its own owner row.

### 9.3 Applications saved in the previous summary

The function also expands the source application's existing
`similar_application_ids` list.

Example:

```text
current targets = [200, 300]
previous targets = [300, 400]
affected = [100, 200, 300, 400]
```

Application 400 must be rebuilt even though it is no longer a current match.
Otherwise, application 400 could keep a stale relationship back to application
100.

`UNION` removes duplicate application IDs, and `ARRAY_AGG ... ORDER BY` returns
them in numeric order.

## 10. Locking and transaction behavior

All affected summary changes occur in one PostgreSQL transaction.

### 10.1 Advisory transaction locks

The function requests one advisory transaction lock for every affected
application:

```text
hash("titan_similar_cases:<application_id>")
```

It requests the locks in sorted application-ID order.

Example:

```text
affected = [100, 200, 400]
lock 100, then 200, then 400
```

If two workers need applications 100 and 200, both ask for them in the same
order. One worker waits instead of the workers taking opposite locks and
deadlocking.

Advisory locks also protect a summary that does not have a table row yet.

### 10.2 Existing-row locks

Existing `tsp.titan_similar_cases` rows are then selected in application-ID
order with `FOR UPDATE`.

These are write locks. Normal reads can continue, but another writer trying to
change the same summary row waits.

### 10.3 Lock duration

Both lock types last until Python commits or rolls back the refresh transaction.
The Python store commits after the function returns every rebuilt row.

The function does not lock `tsp.titan_matches` rows. Sentence Similarity can
continue writing sentence relationships while a case refresh runs. A
relationship committed after the refresh built its affected list may be picked
up by the next Case Similarity message.

## 11. Filter by analysis group

The function builds a set of applications found in
`tsp.text_similarity_input` for the supplied analysis group.

Example:

```text
p_analysis_group = Asylee
```

Only target owners belonging to that group participate in sentence-pair
counting. This prevents Asylee and Refugee sentence relationships from becoming
one case result.

The current SQL checks `analysis_group`; it does not check `is_current` in this
step. The design assumes the application identity and group in
`text_similarity_input` are valid for this flow.

## 12. Build unique sentence pairs

For every affected application, the function reads its `titan_matches` bucket
rows and expands the JSONB keys.

It produces rows shaped like:

```text
source_application_id, source_global_id, target_global_id
```

Example:

| source application | source global ID | target global ID |
|---:|---:|---:|
| 100 | 10 | 50 |
| 100 | 10 | 60 |
| 100 | 11 | 70 |

`DISTINCT` removes the same source/target occurrence pair if it appears more
than once. It does not combine different occurrence pairs merely because they
have the same sentence text or sentence key.

Therefore, all of these count separately:

```text
(source 10, target 50)
(source 10, target 60)
(source 11, target 70)
```

This is occurrence-level counting. Repeated copies of a sentence can produce
multiple occurrence pairs.

## 13. Convert target sentences to target applications

The target-owner CTE connects each target `global_id` to the application that
owns it. The function then groups by:

```text
source_application_id + target_application_id
```

and counts the unique occurrence pairs.

Example:

| Source application | Target application | Sentence-pair count |
|---:|---:|---:|
| 100 | 200 | 1 |
| 100 | 300 | 2 |
| 100 | 400 | 3 |

Any target application equal to the source application is excluded.

## 14. Apply the three case levels

The current constants are:

```text
MIN_SIMILAR_CASE_COUNT = 2
MIN_TAH_CASE_COUNT = 3
```

The function builds three lists for each affected application.

### `similar_application_ids`

Contains every other application with at least one matching sentence pair.
This is the broadest list.

### `similar_case_ids`

Contains every other application with at least two matching sentence pairs.

### `tah_case_ids`

Contains every other application with at least three matching sentence pairs.

Example:

| Target application | Pair count | Similar application | Similar case | TAH case |
|---:|---:|---|---|---|
| 200 | 1 | Yes | No | No |
| 300 | 2 | Yes | Yes | No |
| 400 | 3 | Yes | Yes | Yes |
| 500 | 25 | Yes | Yes | Yes |

The lists are nested by definition:

```text
tah_case_ids is a subset of similar_case_ids
similar_case_ids is a subset of similar_application_ids
```

## 15. Save `tsp.titan_similar_cases`

There is one summary row per `application_id`.

Example:

| application_id | group | similar_application_ids | similar_case_ids | similar_case_count | tah_case_ids | tah_case_count | is_tah |
|---:|---|---|---|---:|---|---:|---|
| 100 | Asylee | `[200,300,400]` | `[300,400]` | 2 | `[400]` | 1 | true |

Python sends only these stored inputs:

- `application_id`
- `analysis_group`
- `similar_application_ids`
- `similar_case_ids`
- `tah_case_ids`
- `last_modified`

PostgreSQL generates:

```text
similar_case_count = length(similar_case_ids)
tah_case_count = length(tah_case_ids)
is_tah = tah_case_count > 0
```

The function performs a full replacement, not an append. This is important for
removing old results.

Example:

```text
old similar_case_ids = [200, 300]
new truth            = [300]
saved list           = [300]
```

Application 200 is removed instead of remaining forever.

If no matches remain, the function saves empty arrays:

```json
{
  "similar_application_ids": [],
  "similar_case_ids": [],
  "tah_case_ids": []
}
```

The generated counts become zero and `is_tah` becomes false.

## 16. Bidirectional case correctness

Suppose application 100 matches application 200. Both applications need to show
the relationship:

```text
100 -> 200
200 -> 100
```

Sentence Similarity first guarantees bidirectional rows in `titan_matches`.
Case Similarity then includes both current target applications and previous
targets in the affected list and rebuilds every affected summary.

Because application 200 owns reverse sentence relationships back to 100, its
rebuild produces the reverse case result.

The same rule removes relationships in both directions. Previous targets are
rebuilt even when the source no longer has a current relationship with them.

## 17. What happens with 76 documents or 10,000 sentences

The received `tsp_id` is used only to identify the application. The summary is
application-level, so it includes sentence relationships from every document
belonging to that application.

The function does not issue one database request per document. It filters
`tsp.titan_matches` by `application_id`.

The important size is not the number of documents by itself. The important
sizes are:

- number of source sentence occurrences;
- number of non-empty bucket rows;
- number of JSONB target keys;
- number of unique source/target occurrence pairs; and
- number of affected applications that must be rebuilt and locked.

For an application with 10,000 sentences, this may still be a large PostgreSQL
operation. It remains preferable to repeating 10,000 OpenSearch vector searches
because the semantic result is already stored in `titan_matches`.

The expected supporting index is:

```sql
CREATE INDEX ix_titan_matches_application_id
ON tsp.titan_matches (application_id, global_id);
```

This lets PostgreSQL find source bucket rows for one application without
scanning every source application.

The current SQL also creates broad distinct target-owner sets. That part should
be measured on production-sized data with:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM tsp.refresh_titan_similar_cases(100, 'Asylee', 2, 3);
```

Use a safe lower environment for `ANALYZE` because the function writes summary
rows. Run it inside a transaction and roll it back when only measuring.

## 18. High-match sentences

The legacy Case Similarity service ignored one source sentence when its match
count was 500 or greater. The current Titan refresh function does not apply
that rule.

The new function counts every unique sentence occurrence relationship saved in
`tsp.titan_matches`.

Example:

```text
global_id 10 has 600 target global IDs
```

All 600 relationships can contribute to application-level counts. The service
assumes Sentence Similarity has already removed tracer, form-language, and
non-matcher sentences before saving relationships.

More detail is available in `HIGH_MATCH_SENTENCE_BEHAVIOR.md`.

## 19. Return the rebuilt summaries to Python

After the upsert, the database function returns every affected row ordered by
`application_id`.

Python maps the JSONB arrays into `CaseSummary` objects. The Python properties
calculate:

```text
similar_case_count = len(similar_case_ids)
tah_case_count = len(tah_case_ids)
is_tah = bool(tah_case_ids)
```

The handler finds the source application's summary for the result returned by
`process()`. The other summaries represent applications updated because they
currently or previously connect to the source.

## 20. Kafka toggle

Kafka is controlled by:

```text
SEND_TAH_KAFKA_EVENTS
```

### Historical or backfill flow

Use:

```text
SEND_TAH_KAFKA_EVENTS=false
```

The service still rebuilds every case summary. It does not create a Kafka
producer, query Kafka delivery history, publish TAH events, or require Kafka
credentials.

### Live flow

Use:

```text
SEND_TAH_KAFKA_EVENTS=true
```

The service creates a Confluent Kafka producer at startup and publishes only
new or changed TAH values.

## 21. Find changed TAH values

The service supplies all affected `application_id` values to
`KafkaEventDBStore`.

The SQL performs these steps:

1. Join `text_similarity_input` to `titan_similar_cases`.
2. Read the application's `a_number`, analysis group, and current `is_tah`.
3. Join `a_number` to `landing.global_document_registry` for receipt numbers.
4. Discard a receipt number associated with more than one application because
   the service cannot safely choose one TAH value for it.
5. Left join `validation.kafka_ta_hit_lookup` using receipt number and Kafka
   environment.
6. Return the event when no delivery record exists.
7. Return the event when the saved and current `is_tah` values differ.
8. Skip the event when the value has not changed.

Example:

| Application | Current is_tah | Last delivered | Publish? |
|---:|---|---|---|
| 100 | true | no row | Yes |
| 200 | true | false | Yes |
| 300 | false | false | No |
| 400 | false | true | Yes |

This means Kafka can receive both changes to true and changes back to false.

## 22. Kafka payload and delivery

The Kafka topic is:

```text
rfs.ata.text_analytics.hit
```

The key is `receipt_number`.

Example value:

```json
{
  "is_tah": true,
  "application_url": "https://ui.rfs-ata.uscis.dhs.gov/ui/main/100",
  "primary_a_number": "900001",
  "analysis_group": "Asylee"
}
```

`primary_a_number` is sent as text. `application_url` is the configured base URL
plus the real application ID.

For each event, the processor:

1. calls `produce()`;
2. calls `poll(0)` to serve callbacks;
3. calls `flush()` to wait for broker delivery;
4. checks the delivery callback for an error;
5. checks that no queued message remains;
6. requires a delivery acknowledgement; and
7. saves the broker timestamp, partition, offset, TAH value, and environment.

The delivery row is written only after Kafka confirms the event. That row stops
an unchanged TAH value from being published again.

The acknowledgement proves broker delivery. It does not prove the Global
consumer has read or completed the event.

## 23. Send the next SQS message

After summary refresh and enabled Kafka work succeed, the handler sends:

```json
{
  "source": "case_similarity",
  "tsp_id": 100
}
```

Only the received `tsp_id` moves forward. The handler does not send the affected
application list.

If the SQS helper returns false, `QueueSendError` stops completion.

## 24. Delete the source SQS message

The SQS processor deletes the original message only after the handler returns.
Therefore, the required order is:

1. commit case summaries;
2. publish and save required Kafka events;
3. send the next SQS message; and
4. delete the source message.

If source deletion fails, the processor logs an error. The already-sent next
message may exist while the source message becomes visible again. Downstream
processing therefore needs to tolerate duplicate `tsp_id` messages.

The case-summary refresh is idempotent because it replaces complete lists.
Kafka uses its lookup table to skip an unchanged value after a successful
delivery acknowledgement.

## 25. Retry behavior

The local retry helper catches:

- `BotoCoreError`
- `SQLAlchemyError`
- `TimeoutError`
- `ConnectionError`
- `KafkaPublishError`
- `QueueSendError`

The default is three total attempts with five seconds between failed attempts.

| Operation | Retry scope |
|---|---|
| Load document context | Only the context query |
| Refresh summaries | Only the database refresh transaction |
| Load changed Kafka events | Only the Kafka candidate query |
| Publish Kafka event | Only the individual event |
| Send next SQS message | The shared SQS helper receives its retry settings |
| Delete source message | One helper call in the current processor |

Completed earlier steps are not repeated by the local retry helper. However,
if the complete SQS message is delivered again, the full message flow runs
again.

When all local attempts fail, `OperationFailedError` reaches the SQS processor.
The error is logged and the source message is not deleted. SQS controls later
redelivery and eventual DLQ handling.

## 26. Transaction boundaries

| Work | Transaction behavior |
|---|---|
| Load document context | Read-only session |
| Refresh all affected summaries | One transaction and one commit |
| Load changed Kafka events | Read-only session |
| Save one Kafka delivery | One transaction per confirmed event |
| SQS send/delete | Outside the database transactions |

There is no distributed transaction covering PostgreSQL, Kafka, and SQS. The
service uses ordering and idempotent reads/writes to recover from partial
completion.

One example is Kafka delivery followed by a database acknowledgement failure.
The retry may publish the same Kafka value again because the service cannot
prove the first broker delivery was recorded. Consumers should tolerate an
occasional duplicate event.

## 27. Error outcomes

| Failure | Summary committed? | Kafka possible? | Next SQS sent? | Source deleted? |
|---|---|---|---|---|
| Missing `tsp_id` | No | No | No | No |
| Missing document row | No | No | No | No |
| Summary refresh fails | No for failed attempt | No | No | No |
| Kafka disabled | Yes | No by design | Yes | Yes after send |
| Kafka publishing fails | Yes | A partial earlier event is possible | No | No |
| Next SQS send fails | Yes | Yes when enabled | No or uncertain | No |
| Source delete fails | Yes | Yes when enabled | Yes | No |

## 28. Main configuration

| Value | Meaning |
|---|---|
| `AWS_RECEIVE_SQS_QUEUE_ENDPOINT` | Queue consumed by Case Similarity |
| `AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT` | Queue receiving the completed `tsp_id` |
| `AWS_DEFAULT_REGION` | AWS region, default `us-east-1` |
| `AWS_LOCAL_ENDPOINT_URL` | Optional local AWS endpoint |
| `ENVIRONMENT` | `local` or deployed environment name |
| `VAULT_YAML_PATH` | Dynamic database credential file outside local |
| `SEND_TAH_KAFKA_EVENTS` | Enables live Kafka publication |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker list |
| `KAFKA_SECURITY_PROTOCOL` | Usually `SSL`; local example uses `PLAINTEXT` |
| `KAFKA_ENV` | Separates delivery history by Kafka environment |
| `KAFKA_SERVER_CACERT` | Kafka CA file when SSL is used |
| `SSL_CERTFILE` | Kafka client certificate when SSL is used |
| `SSL_KEYFILE` | Kafka client key when SSL is used |
| `APPLICATION_URL` | Base URL placed in the Kafka payload |

Fixed code values are:

```text
database pool size       = 20
database max overflow    = 10
similar-case threshold   = 2
TAH threshold            = 3
retry wait               = 5 seconds
maximum attempts         = 3
SQS long poll            = 10 seconds
SQS visibility timeout   = 7200 seconds
Kafka topic              = rfs.ata.text_analytics.hit
```

## 29. Main files

| File | Responsibility |
|---|---|
| `main.py` | Starts and closes the worker |
| `dependencies.py` | Builds database, SQS, Kafka, stores, and handler |
| `config.py` | Loads and validates deployment values |
| `sqs_processor.py` | Receives and conditionally deletes source messages |
| `case_similarity_handler.py` | Runs the steps for one `tsp_id` |
| `case_similarity_db_store.py` | Loads document context and calls the refresh function |
| `kafka_event_db_store.py` | Finds changed TAH values and stores deliveries |
| `kafka_processor.py` | Publishes Kafka events and checks acknowledgements |
| `retry.py` | Retries one failed operation at a time |
| `contracts.py` | Defines the small values passed between components |
| `TITAN_SIMILAR_CASES.sql` | Creates the summary table and refresh function |

## 30. Current implementation details to remember

1. The runtime raw SQL uses `tsp.titan_similar_cases`.
2. The local ORM model is currently named `TitanCaseSim2` and maps
   `tsp.titan_case_sim2`. The runtime does not use that model for its refresh.
   These names should be aligned before treating the local models as the final
   shared-library definition.
3. `TITAN_SIMILAR_CASES.sql` starts with `DROP TABLE IF EXISTS`. That is useful
   for a POC reset but is not a safe production Flyway migration for a table
   containing data.
4. The current function has no 500-match sentence exclusion.
5. Counting is based on unique occurrence pairs, not unique sentence keys.
6. The summary transaction may lock many affected applications. The locks are
   ordered, but a large connected case can still take time.
7. The SQL target-owner CTE should be performance-tested with production-sized
   data because it creates distinct global-ID/application-ID mappings.
8. The service assumes the sentence relationship table is already
   bidirectional and complete enough to resolve target ownership.

## 31. Validation queries

Check one document's case identity:

```sql
SELECT tsp_id, application_id, a_number, analysis_group
FROM tsp.text_similarity_input
WHERE tsp_id = 100;
```

Count source bucket rows for one application:

```sql
SELECT COUNT(*)
FROM tsp.titan_matches
WHERE application_id = 100;
```

Count all stored relationships owned by one application:

```sql
SELECT COALESCE(SUM(match_count), 0) AS stored_relationship_count
FROM tsp.titan_matches
WHERE application_id = 100;
```

Run the refresh manually:

```sql
SELECT *
FROM tsp.refresh_titan_similar_cases(100, 'Asylee', 2, 3);
```

Read the final summary:

```sql
SELECT *
FROM tsp.titan_similar_cases
WHERE application_id = 100;
```

Inspect new or changed Kafka candidates by following the query in
`kafka_event_db_store.py`. Do not manually insert a delivery acknowledgement
unless Kafka has actually accepted the event.

## 32. Final responsibility boundary

The production responsibility should remain:

```text
Sentence Similarity
    OpenSearch semantic work
    tracer/form-language filtering
    bidirectional sentence-level titan_matches

Case Similarity
    application-level PostgreSQL aggregation
    bidirectional titan_similar_cases summaries
    changed TAH Kafka events
    next-step SQS message
```

This avoids repeating vector searches while keeping the case calculation based
on the complete application, including all of its documents.
