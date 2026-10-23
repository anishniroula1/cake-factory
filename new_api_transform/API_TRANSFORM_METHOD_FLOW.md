# New API Transform Code Flow

The service processes one `tsp_id` from SQS, rebuilds the API payload for its
application, publishes the payload to Kafka, sends `tsp_id` to the next queue,
and deletes the source message after everything succeeds.

```text
main.py
  -> dependencies.py and config.py
  -> sqs_processor.py
  -> api_transform_handler.py
  -> database stores
  -> payload_builder.py
  -> kafka_processor.py
  -> transmission_db_store.py
  -> next SQS queue
```

## 1. `main.py`

### `run_opensearch_flow()`

Creates the service dependencies and starts `SqsProcessor.run()`. When the
service stops, it clears the logger context and closes the database pool.

## 2. `config.py`

### `read_boolean()`

Reads a true/false environment setting and rejects invalid values.

### `read_integer()`

Reads a whole-number environment setting and checks its minimum value.

### `Config.from_environment()`

Loads database credentials from Vault and loads SQS, Kafka, and transformation
settings from environment variables. It calls `validate()` before returning.

### `Config.validate()`

Checks that required database, SQS, Kafka, and certificate settings are
available before the worker starts.

## 3. `dependencies.py`

### `Dependencies.__init__()`

Creates the shared logger, configuration, database pool, SQS clients, database
stores, payload builder, Kafka producer, and API Transform handler.

### `_create_database_connection()`

Creates the PostgreSQL connection. Local execution uses normal credentials;
other environments use the dynamic credential file.

### `_sqs()`

Creates an SQS client with the shared AWS region, endpoint, wait time, and
two-hour visibility timeout.

### `_kafka_config()`

Builds the Kafka producer configuration. SSL certificate settings are added
when the Kafka security protocol is SSL.

### `close()`

Disposes the database engine and closes its connection pool.

## 4. `sqs_processor.py`

### `run()`

Continuously receives messages and processes them one at a time.

### `_receive_messages()`

Reads the main queue first. When the main queue is empty, it reads the DLQ if a
DLQ client is configured.

### `_read_tsp_id()`

Reads `tsp_id` from the SQS message. It returns `None` when the value is missing
or empty.

### `_process_message()`

Controls one complete document flow:

1. Read `tsp_id` and set the logger context.
2. Detect whether the message reason is `HISTORICAL`.
3. Call `ApiTransformHandler.process()`.
4. Send the completed `tsp_id` to the next queue.
5. Delete the message from the queue that supplied it.

If processing fails, the message is not deleted.

### `_send_next_message()`

Sends this message to the next service after API Transform finishes:

```json
{"source": "P2", "tsp_id": "1001", "sent_by": "api-transform"}
```

### `_leave_message_for_sqs()`

Logs why processing did not finish and why the source message was not deleted.

## 5. `handlers/api_transform_handler.py`

### `process()`

Finds the application connected to `tsp_id`, processes that application, logs
the completion time, and returns the processed application and Kafka event
counts. A known failure is also saved to `landing.etlerror`.

### `_process_application()`

Loads the application record, applicant details, documents, sentences with
Titan matches, and the Asylee public template. It then builds and publishes the
API events.

Each database read is retried separately, so one failed read does not repeat
the successful reads before it.

### `_publish_with_buffer_limit()`

Groups built events without allowing the in-memory buffer to exceed its safety
limit. Each group is then published through `KafkaProcessor.publish()`.

## 6. `db_store` folder

The handler uses the files in this folder for all PostgreSQL reads and writes.
This section covers every database-store file before moving to another folder.

### `application_db_store.py`

#### `load_application_reference()`

Uses `tsp_id` to read `application_id`, `a_number`, and `analysis_group` from
`tsp.text_similarity_input`.

#### `load_application_records()`

Builds the application-level `a_records` section. It loads receipt, applicant,
source-type, confidence, and `tsp.titan_similar_cases` data. Similar application
IDs are also mapped to the A-number list required by the existing API contract.

### `applicant_db_store.py`

#### `load_applicant_details()`

Loads the current applicant and newest residence and mailing addresses. When
Kafka-size reduction is enabled, unchanged applicant information can be left
out.

#### `_build_applicant()`

Builds the five OCR fault flags, removes the private OCR source columns, and
formats `last_modified` for the API payload.

#### `calculate_ocr()`

Returns `True` when more than 10 percent of the available OCR bits are `1`.

#### OCR helper methods

- `_empty_ocr_faults()` returns all five OCR flags as false.
- `_residence_ocr_fields()` lists residence OCR columns.
- `_mailing_ocr_fields()` lists mailing OCR columns.
- `_all_ocr_fields()` combines every OCR column removed from the payload.

### `content_db_store.py`

#### `load_documents()`

Loads every current document for the application, including its section, file
name, load date, and number of sentences with Titan matches.

#### `load_sentences()`

Loads current sentences and combines all `tsp.titan_matches` bucket rows into
the API sentence-match list. The configured limit applies to each source
sentence.

#### `_small_match()`

Keeps only the target `global_id` and distance and floors the distance to four
decimal places.

#### `_is_match_only_update()`

Detects when sentence content is unchanged but its Titan matches changed. That
allows the payload to send a smaller sentence update.

#### `load_public_templates()`

Loads the newest public-template record for each Asylee application.

#### `_load_grouped()`

Runs a shared database query and groups its rows by `application_id`.

### `transmission_db_store.py`

These methods run later in the flow, after Kafka succeeds or when the handler
catches a known transformation failure.

#### `save_deliveries()`

After Kafka confirms delivery, this method:

1. Deletes expired Kafka audit rows.
2. Inserts the new audit rows into `api.kafka_events`.
3. Updates `api.transform_status` with the successful transmission time.
4. Commits the transaction.

#### `audit_value()`

Creates the database audit copy of the Kafka event. When verbose logging is
disabled, it saves the sentence count instead of the complete sentence list.

#### `save_etl_error()`

Writes a known API Transform failure to `landing.etlerror`.

## 7. `payload_builder.py`

### `build_application_events()`

Combines `a_records`, documents, applicant details, sentences, and the optional
public template into the Kafka contract. It also adds the `historical` flag.

The new flow does not add NER data.

### `_clean_application_record()`

Removes identifiers already stored at the event level. It also formats Refugee
receipt numbers before they are sent.

### `_normalize_receipt_number()`

Changes a receipt such as `ABC123` into `ABC-00000123`.

### `serialized_size()`

Returns the JSON event size in bytes.

### `_split_sentences()`

Splits an oversized sentence list into several events. The first event is a
create event; later sentence chunks are update events.

## 8. `kafka_processor.py`

### `publish()`

Publishes every event using `application_id` as the Kafka key, waits for broker
confirmation, and fails if every event is not confirmed.

### `_serialize_payload()`

Converts `after.payload` into JSON text because that is what the current Kafka
consumer expects.

### `_delivery_callback()`

Records either the Kafka error or the confirmed timestamp, partition, and
offset for one event.

## 9. `retry.py`

### `retry_operation()`

Retries only the failed database or Kafka operation. The default is three total
attempts with a five-second wait. A failure after all attempts becomes an
`OperationFailedError`.

## Successful end result

For one source SQS message, the service rebuilds one application, confirms its
Kafka event or events, saves the delivery audit, sends `tsp_id` to the next
queue, and finally deletes the source message.
