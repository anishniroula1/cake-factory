# New API Transform

This worker keeps the existing API Transform output contract while moving the
matching reads to the Titan tables. It receives one document `tsp_id` from each
SQS message, finds that document's application, rebuilds its current API
payload, and publishes the payload to `rfs.ata.data_migration.api.events`.

## Flow

1. Read one document message from the main SQS queue. When it is empty, read
   the messages returned by the DLQ and process each message separately.
2. Find the application connected to the received `tsp_id`.
3. Load case details, applicant data, OCR flags, documents, public templates,
   and sentences for that application.
4. Read sentence relationships from all 16 `tsp.titan_matches` buckets and
   return them in the existing list format.
5. Read case-level lists and TAH status from `tsp.titan_similar_cases`.
6. Build the same Kafka payload used by the current API consumer. This new flow
   intentionally does not include NER and identifies the vector source with
   `embedding_model="TITAN"`.
7. Split only the sentence list when one application event is too large.
8. Publish to Kafka and wait for every delivery acknowledgement.
9. Save confirmed Kafka audit rows and update `api.transform_status`.

10. Forward the completed `tsp_id` to the next queue, then delete that source
    message. A failed step leaves the source message undeleted. SQS retries it
    and moves it to the configured DLQ after the queue's receive limit.

## Important field mapping

`titan_similar_cases.similar_case_ids` contains application IDs. The existing
API field named `similar_case_ids` expects A-numbers, so the application query
maps those IDs through `application_id_lookup`. The Titan
`similar_application_ids` list remains application IDs in the Kafka payload.

The compatibility copies under `ta_utils` and `ta_db_connection` mimic the
shared libraries. They can be removed when this folder is moved into the real
service repository that already provides those packages.

## Configuration

`config.py` reads values that can change between environments, including
database credentials, queue URLs, Kafka connection values, and feature
toggles. `constants.py` holds fixed service behavior such as queue
timing, payload limits, the Kafka topic, database pool sizes, and retry timing.
Fixed values do not need to be repeated in deployment environment variables.

## Run

Copy `.env.example` to `.env` and supply working PostgreSQL and SQS values.
Start the local Kafka broker and create the API Transform topic with:

```bash
make kafka
```

The local broker uses `localhost:9092` and the `PLAINTEXT` protocol already
shown in `.env.example`. Run the worker locally with:

```bash
make local
```

`make local` also starts Kafka when it is not already running. Stop the local
broker with:

```bash
make kafka-stop
```

Run code checks and tests with:

```bash
make test
```
