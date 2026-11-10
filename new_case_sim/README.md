# Case Similarity Service

Case Similarity is an SQS/PostgreSQL worker that processes one document message
at a time. It turns sentence-level relationships from `tsp.titan_matches` into
application-level similar-case and TAH summaries.

Sentence Similarity has already completed the vector search before this service
runs. Case Similarity does not create embeddings and does not query OpenSearch.
It reads the saved sentence relationships, builds the application summary,
publishes live TAH changes when enabled, and sends the document to the next
service.

## What the service does

For each SQS message, the service:

1. Reads one `tsp_id` from the message.
2. Loads its `application_id`, `a_number`, and `analysis_group` from
   `tsp.text_similarity_input`.
3. Calls `tsp.refresh_titan_similar_cases` for that source application.
4. Reads sentence relationships from `tsp.titan_matches`.
5. Counts unique matching sentence pairs between the source application and
   every target application.
6. Applies the similar-case and TAH thresholds.
7. Rebuilds the source application's summary in
   `tsp.titan_similar_cases`.
8. Updates the reverse relationship on every affected target application.
9. Publishes new or changed TAH values to Kafka when live publishing is enabled.
10. Sends one next-queue message for the completed `tsp_id`.
11. Deletes the source message only after the application refresh and
    next-queue send succeed.

If processing fails, the source message is not deleted. SQS can retry the
message and eventually move it to the configured DLQ.

## Message format

The source queue accepts one document message:

```json
{
  "tsp_id": 100
}
```

After processing, the next queue receives:

```json
{
  "source": "P2",
  "tsp_id": "100",
  "sent_by": "case-similarity"
}
```

## Case matching rules

The service counts how many source sentence occurrences match sentence
occurrences in another application.

| Sentence relationships to another application | Result |
|---:|---|
| 1 | Saved in `similar_application_ids` only |
| 2 | Also saved in `similar_case_ids` |
| 3 or more | Also saved in `tah_case_ids` |

Example for source Application 100:

| Target application | Matching sentence pairs | Similar case | TAH case |
|---:|---:|---|---|
| 200 | 1 | No | No |
| 300 | 2 | Yes | No |
| 400 | 3 | Yes | Yes |

The saved source summary becomes:

```json
{
  "application_id": 100,
  "similar_application_ids": [200, 300, 400],
  "similar_case_ids": [300, 400],
  "similar_case_count": 2,
  "tah_case_ids": [400],
  "tah_case_count": 1,
  "is_tah": true
}
```

PostgreSQL calculates the two counts and `is_tah` from the saved lists. An
application with no matching application does not keep an empty summary row.

## Bidirectional application summaries

Case relationships are bidirectional. If Application 100 is a similar case for
Application 300, Application 300 also contains Application 100.

The database function fully rebuilds the source application's lists. It then
adds or removes only the source application on each affected target row. Other
relationships already owned by those targets remain unchanged.

Application rows are locked in sorted order before updates. This protects
concurrent workers and reduces deadlocks when two documents affect the same
applications.

## Live TAH publishing

Set this value for the live flow:

```text
SEND_TAH_KAFKA_EVENTS=true
```

For every affected application, the service:

1. Reads the current generated `is_tah` value.
2. Uses `a_number` to find receipt numbers in
   `landing.global_document_registry`.
3. Compares the current value with the last Kafka-confirmed value in
   `validation.kafka_ta_hit_lookup`.
4. Publishes when the receipt is new or its TAH value changed.
5. Saves the Kafka timestamp, partition, offset, and confirmed value only after
   Kafka acknowledges delivery.

One application can have multiple receipt numbers. The service publishes one
event for every unique receipt. A receipt shared by multiple affected
applications is skipped because that Kafka key cannot identify one application
safely.

The Kafka record key is the receipt number. Its value is:

```json
{
  "is_tah": true,
  "application_url": "https://ui.rfs-ata.uscis.dhs.gov/ui/main/100",
  "primary_a_number": "900001",
  "analysis_group": "Asylee"
}
```

## Historical processing

Set this value for historical processing:

```text
SEND_TAH_KAFKA_EVENTS=false
```

The service still rebuilds `tsp.titan_similar_cases`, sends the next SQS
message, and deletes the completed source message. It does not create a Kafka
producer, inspect Kafka delivery history, or publish historical TAH events.

## Main tables

| Table | Purpose |
|---|---|
| `tsp.text_similarity_input` | Connects `tsp_id` to application, a-number, and analysis group. |
| `tsp.titan_matches` | Stores bidirectional sentence relationships in JSONB buckets. |
| `tsp.titan_similar_cases` | Stores application match lists, generated counts, and `is_tah`. |
| `landing.global_document_registry` | Supplies receipt numbers used as Kafka keys. |
| `validation.kafka_ta_hit_lookup` | Stores the last Kafka-confirmed TAH value and delivery details. |

## Project layout

```text
case_similarity_completed/opensearch/
  db_store/              PostgreSQL document, summary, and Kafka lookup operations
  handlers/              One-document Case Similarity flow
  config.py              Environment and Vault configuration
  constants.py           Fixed thresholds, retry, SQS, database, and Kafka values
  dependencies.py        Database, SQS, Kafka, stores, and handler construction
  kafka_processor.py     TAH selection, publishing, and delivery confirmation
  sqs_processor.py       Receive, process, and delete one source message
tests/opensearch/        Case Similarity unit tests
```

The package is under `opensearch` because it belongs to the Titan/OpenSearch
processing pipeline. This service itself uses PostgreSQL, SQS, and optional
Kafka; it does not call OpenSearch.

## Configure the service

Create the local environment file:

```bash
cp .env.example .env
```

Configure the source and next SQS endpoints in `.env`. For local live Kafka
testing, keep the following values:

```text
SEND_TAH_KAFKA_EVENTS=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_ENV=local
```

Set `SEND_TAH_KAFKA_EVENTS=false` when Kafka should not run.

The database connection comes from Vault using these keys:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
```

Outside the local environment, `VAULT_YAML_PATH` must point to the dynamic
credential configuration.

## Run locally

Running the Python process locally is faster than rebuilding a container after
every code change.

Start the worker, local Kafka broker, and ZooKeeper:

```bash
make local
```

The command creates the `rfs.ata.text_analytics.hit` Kafka topic when it does
not already exist.

Kafka and ZooKeeper can also be started separately:

```bash
make kafka
```

## Run tests

Run the Case Similarity tests and lint checks:

```bash
make test
```

The tests are located under `tests/opensearch`.
