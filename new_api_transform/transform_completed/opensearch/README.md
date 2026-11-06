# API Transform Service

API Transform is an SQS/PostgreSQL/Kafka worker that processes one document
(`tsp_id`) at a time. It finds the document's application, builds the complete
application response from current PostgreSQL data, and publishes the response
to `rfs.ata.data_migration.api.events`.

Sentence Similarity and Case Similarity have already calculated the Titan
sentence and case relationships before this service runs. API Transform reads
those saved results and prepares them for the API consumer.

## What the service does

For each SQS message, the service:

1. Reads `tsp_id` from the source message.
2. Finds its current `application_id`, `a_number`, and `analysis_group` in
   `tsp.text_similarity_input`.
3. Loads the application record, confidence values, applicant details,
   addresses, and OCR flags.
4. Loads every current document and sentence owned by the application.
5. Combines all 16 `tsp.titan_matches` bucket rows into each sentence's match
   list.
6. Loads similar-application, similar-case, and TAH results from
   `tsp.titan_similar_cases`.
7. Loads the newest public template for an Asylee application.
8. Builds the application Kafka event with `embedding_model="TITAN"`.
9. Splits an oversized sentence list into Kafka-safe events.
10. Publishes the events and waits for every Kafka acknowledgement.
11. Saves confirmed Kafka audits and updates the application's last
    transmission time.
12. Sends the completed `tsp_id` to the next SQS queue.
13. Deletes the source message only after all required work succeeds.

If processing fails, the source message is not deleted. SQS can retry it and
eventually move it to the configured DLQ.

A missing `tsp_id`, or a `tsp_id` with no current row in
`tsp.text_similarity_input`, is treated as a failed message and remains
undeleted.

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
  "sent_by": "api-transform"
}
```

## Kafka event

The normal event uses operation `c` and identifies Titan as the embedding
model:

```json
{
  "after": {
    "pipeline": "Asylee",
    "a_number": 900001,
    "application_id": 100,
    "analysis_group": "Asylee",
    "embedding_model": "TITAN",
    "payload": "{...}"
  },
  "op": "c"
}
```

The payload contains application records, document metadata, applicant data,
sentences, sentence matches, and the optional Asylee public template.

When one application is too large for one Kafka event, only the sentence list
is split. The first event uses `op="c"`; later sentence chunks use `op="u"`.
Every chunk keeps `embedding_model="TITAN"` and the application identifiers.

When reduced Kafka payloads are enabled and only a sentence's match data
changed, that sentence is sent with its `global_id`, matches, and sentence-level
`op="u"` instead of repeating unchanged sentence content.

## Titan sentence matches

Sentence relationships are stored in 16 JSONB bucket rows for each source
`global_id`. API Transform expands the buckets and returns matches in this
format:

```json
{
  "global_id": 200,
  "distance": 0.1
}
```

The configured sentence-match limit applies separately to each source
sentence. Distance `0.0` is an exact vector match, and smaller distances are
better.

## Similar-case mapping

`tsp.titan_similar_cases.similar_case_ids` stores application IDs. The API
field named `similar_case_ids` requires A-numbers, so the application query
maps those application IDs through `landing.application_id_lookup`.

`similar_application_ids` remains a list of application IDs in the Kafka
payload.

## Kafka delivery and auditing

The service uses `application_id` as the Kafka record key. Processing is not
complete until every event receives a successful broker callback.

After confirmation, the service:

1. Stores the delivery timestamp, partition, offset, and event value in
   `api.kafka_events`.
2. Updates `api.transform_status.last_transmission`.
3. Deletes Kafka audit rows older than the configured retention period.

When verbose Kafka auditing is disabled, the audit copy stores the sentence
count instead of the complete sentence list. The Kafka event itself is not
changed.

## Main tables

| Table | Purpose |
|---|---|
| `tsp.text_similarity_input` | Connects `tsp_id` to the application and supplies current document data. |
| `landing.application_id_lookup` | Supplies the application record and maps application IDs to A-numbers. |
| `landing.receipt_number_lookup` | Supplies the receipt number. |
| `landing.global_case_details_history` | Supplies confidence values. |
| `biodata.applicant` | Supplies current applicant data and OCR source values. |
| `biodata.address` | Supplies current residence and mailing addresses. |
| `tsp.sentences_vector` | Supplies the application's current sentences. |
| `tsp.titan_matches` | Supplies bucketed Titan sentence matches and distances. |
| `tsp.titan_similar_cases` | Supplies similar applications, similar cases, and TAH status. |
| `tsp.api_section_map` | Maps internal document sections to API section names. |
| `landing.global_document_registry` | Supplies document file names. |
| `landing.template_registry` | Supplies the newest Asylee public template. |
| `api.kafka_events` | Stores confirmed Kafka delivery audits. |
| `api.transform_status` | Stores the application's last confirmed transmission time. |
| `landing.etlerror` | Stores known transformation failures. |

## Project layout

```text
transform_completed/opensearch/
  db_store/              PostgreSQL application, content, applicant, and audit operations
  handlers/              One-document API Transform flow
  config.py              Environment and Vault configuration
  constants.py           Fixed database, SQS, Kafka, payload, and retry values
  dependencies.py        Database, SQS, Kafka, stores, and handler construction
  kafka_processor.py     Kafka serialization, publishing, and delivery confirmation
  payload_builder.py     Application-event construction and payload splitting
  sqs_processor.py       Source receipt, processing, forwarding, and deletion
tests/opensearch/        API Transform unit tests
```

## Configure the service

Create the local environment file:

```bash
cp .env.example .env
```

Configure the source, DLQ, and next-service SQS endpoints plus the Kafka
connection values in `.env`. For the local Kafka container, use:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_ENV=local
```

The PostgreSQL connection comes from Vault using these keys:

```yaml
pgUser: database-user
pgPassword: database-password
pgHost: database-host
pgDatabase: database-name
pgPort: 5432
```

Dynamic credentials can use `pgDynamicUser` and `pgDynamicPassword`. Outside
the local environment, `VAULT_YAML_PATH` must point to the dynamic credential
configuration.

## Run locally

Running the Python worker locally is faster than rebuilding the application
container after every code change. The configured source, DLQ, and next SQS
queues must already exist.

Start the worker, Kafka broker, and ZooKeeper:

```bash
make local
```

Kafka and ZooKeeper can also be started separately:

```bash
make kafka
```

## Run tests

Run the API Transform tests and lint checks:

```bash
make ostest
```
