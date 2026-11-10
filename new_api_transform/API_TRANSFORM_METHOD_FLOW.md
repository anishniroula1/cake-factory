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

### Description

This is the service entry point. It starts the SQS processing loop and closes
shared resources when the process stops.

### `run_opensearch_flow()`

Creates the service dependencies and starts `SqsProcessor.run()`. When the
service stops, it clears the logger context and closes the database pool.

#### Details

The method accepts prebuilt dependencies for tests or creates the normal
service dependencies when none are supplied. Cleanup runs from `finally`, so
the logger context and database pool are closed even when the worker stops with
an error.

## 2. `config.py`

### Description

This file reads Vault and environment settings, creates the service
configuration, and validates required values before processing begins.

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

### Description

This file creates and connects the shared database, SQS, Kafka, payload, store,
and handler objects used by the service.

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

### Description

This file controls the SQS message lifecycle from receipt through successful
processing, downstream delivery, and source-message deletion.

### `run()`

Continuously receives messages and processes them one at a time.

#### Details

Each loop reads the available source or DLQ messages and passes them to the
document processor. The queue that supplied each message is retained so the
message can be deleted from the correct queue after success.

### `_receive_source_or_dlq_messages()`

Reads the source queue first. When the source queue is empty, it reads the DLQ
if a DLQ client is configured.

#### Details

The method returns the messages together with the SQS client that received
them. This lets the same processing flow handle source and DLQ messages without
mixing their receipt handles.

### `_read_tsp_id()`

Reads `tsp_id` from the SQS message. It returns `None` when the value is missing
or empty.

#### Details

An integer remains an integer and a string remains a string. Whitespace-only
text is treated as missing so an invalid document cannot enter the transform
flow.

### `_process_message()`

Controls one complete document flow:

1. Read `tsp_id` and set the logger context.
2. Call `ApiTransformHandler.process()`.
3. Send the completed `tsp_id` to the next queue.
4. Delete the message from the queue that supplied it.

If processing fails, the message is not deleted.

#### Details

The method sets the logger context for the current document and clears that
context when processing ends. It returns success only after transformation,
the next SQS send, and source-message deletion all finish.

When `tsp_id` is missing, or no current `tsp.text_similarity_input` record
exists for it, processing returns failure and leaves the source message
undeleted. The source queue's redrive policy can then move it to the DLQ after
the configured receive attempts.

### `_send_next_message()`

Sends this message to the next service after API Transform finishes:

```json
{"source": "P2", "tsp_id": "1001", "sent_by": "api-transform"}
```

#### Details

The document identifier is converted to text for a consistent downstream
message. The Boolean result from the SQS client tells the caller whether it is
safe to delete the source message.

### `_leave_message_for_sqs()`

Logs why processing did not finish and why the source message was not deleted.

#### Details

This method only records the failure reason. It does not delete, move, or
change the source message, allowing the configured SQS retry and DLQ behavior
to handle it.

## 5. `handlers/api_transform_handler.py`

### Description

This file coordinates the transformation of one document's application data
into one or more Kafka events.

### `process()`

Finds the application connected to `tsp_id`, processes that application, logs
the completion time, and returns the processed application and Kafka event
counts. A known failure is also saved to `landing.etlerror`.

#### Details

It first loads the current application reference containing `application_id`,
`a_number`, and `analysis_group`. The application payload is processed only
when that reference exists, and each known failure is recorded before control
returns to the SQS processor.

### `_process_application()`

Loads the application record, applicant details, documents, sentences with
Titan matches, and the Asylee public template. It then builds and publishes the
API events.

Each database read is retried separately, so one failed read does not repeat
the successful reads before it.

#### Details

The loaded sections are passed to the payload builder. The resulting event list
is then passed to the size-limited Kafka publishing method.

### `_publish_with_buffer_limit()`

Groups built events without allowing the in-memory buffer to exceed its safety
limit. Each group is then published through `KafkaProcessor.publish()`.

#### Details

The serialized byte size of each event is added to the current buffer total.
The buffer is published before the next event would cross the limit, and the
method returns the total number of events confirmed by Kafka.

## 6. `db_store` folder

The handler uses the files in this folder for all PostgreSQL reads and writes.
This section covers every database-store file before moving to another folder.

### `application_db_store.py`

#### Description

This file reads the document's application reference and builds the
application-level `a_records` section used in the outgoing payload.

#### `load_application_reference()`

Uses `tsp_id` to read `application_id`, `a_number`, and `analysis_group` from
`tsp.text_similarity_input`.

##### Details

Only the current document row is accepted. The method returns one application
reference for the handler or `None` when the document has no current owner.

#### `load_application_records()`

Builds the application-level `a_records` section. It loads receipt, applicant,
source-type, confidence, and `tsp.titan_similar_cases` data. Similar application
IDs are also mapped to the A-number list required by the existing API contract.

##### Details

The query accepts application references, uses their application IDs and
A-numbers, and returns the final records keyed by `application_id`. An empty
input list returns an empty dictionary without running the main query.

### `applicant_db_store.py`

#### Description

This file loads applicant and address data, calculates OCR fault flags, and
removes private OCR source values from the outgoing record.

#### `load_applicant_details()`

Loads the current applicant and newest residence and mailing addresses. When
Kafka-size reduction is enabled, unchanged applicant information can be left
out.

##### Details

Every returned database row is passed to `_build_applicant()`. The final
dictionary is keyed by `application_id` so the handler can attach the correct
applicant to each application event.

#### `_build_applicant()`

Builds the five OCR fault flags, removes the private OCR source columns, and
formats `last_modified` for the API payload.

##### Details

Name, residence address, mailing address, date of birth, and citizenship are
evaluated separately. `GLOBAL` and `GLOBAL_RAD` source records skip OCR
evaluation and receive five false flags.

#### `calculate_ocr()`

Returns `True` when more than 10 percent of the available OCR bits are `1`.

##### Details

Only `0` and `1` characters are counted. The method supports one value or a
list of values, returns false when no usable OCR bits exist, and returns true
when any value in a list crosses the threshold.

#### OCR helper methods

- `_empty_ocr_faults()` returns all five OCR flags as false.
- `_residence_ocr_fields()` lists residence OCR columns.
- `_mailing_ocr_fields()` lists mailing OCR columns.
- `_all_ocr_fields()` combines every OCR column removed from the payload.

##### Details

These helpers keep OCR field definitions in one place. The same field lists
are used to calculate the public fault flags and remove the private OCR source
values from the outgoing payload.

### `content_db_store.py`

#### Description

This file loads document, sentence, Titan match, and public-template data and
groups those records by application for payload construction.

#### `load_documents()`

Loads every current document for the application, including its section, file
name, load date, and number of sentences with Titan matches.

##### Details

Internal section names are mapped through `tsp.api_section_map` when a mapping
exists. The results are grouped by `application_id` so they can be placed
directly in the correct application payload.

#### `load_sentences()`

Loads current sentences, combines all `tsp.titan_matches` bucket rows, and
returns the 3,000 closest relationships for each sentence.

##### Details

The query joins each source sentence to its document, expands every JSONB match
bucket, orders the relationships from the smallest distance to the largest,
and keeps the first 3,000. When only match data has changed, the method can
return a smaller update instead of repeating unchanged sentence content.

#### `_small_match()`

Keeps only the target `global_id` and distance and floors the distance to four
decimal places.

##### Details

The target identifier is converted to an integer. Flooring prevents the saved
distance from being rounded upward and keeps the match object small.

#### `_is_match_only_update()`

Detects when sentence content is unchanged but its Titan matches changed. That
allows the payload to send a smaller sentence update.

##### Details

Both timestamps must exist. A match-only update is reported when the match
timestamp is more than 30 minutes newer than the sentence timestamp.

#### `load_public_templates()`

Loads the newest public-template record for each Asylee application.

##### Details

The query connects current application rows to the template registry by
A-number, sorts newest first, and keeps only one template for each application.

#### `_load_grouped()`

Runs a shared database query and groups its rows by `application_id`.

##### Details

The application ID is removed from each payload record because it becomes the
dictionary key. The returned structure is
`{application_id: [payload records]}`.

### `transmission_db_store.py`

#### Description

This file stores confirmed Kafka delivery audits, updates transmission status,
and records known API Transform failures.

These methods run later in the flow, after Kafka succeeds or when the handler
catches a known transformation failure.

#### `save_deliveries()`

After Kafka confirms delivery, this method:

1. Deletes expired Kafka audit rows.
2. Inserts the new audit rows into `api.kafka_events`.
3. Updates `api.transform_status` with the successful transmission time.
4. Commits the transaction.

##### Details

No database change is made when the confirmed-delivery list is empty. Cleanup,
audit inserts, and transmission-status updates are committed together after
Kafka confirms every event.

#### `audit_value()`

Creates the database audit copy of the Kafka event. When verbose logging is
disabled, it saves the sentence count instead of the complete sentence list.

##### Details

The Kafka value is copied before its inner payload is changed, so the event
already sent to Kafka is not modified. The final audit value is returned as
JSON text.

#### `save_etl_error()`

Writes a known API Transform failure to `landing.etlerror`.

##### Details

The error is saved through a separate transaction and its message is limited
to the database column size. Legacy batch identifiers are set to `-1` because
this worker processes one SQS document rather than a batch file.

## 7. `payload_builder.py`

### Description

This file combines all loaded application sections into the Kafka contract and
splits events that exceed the configured message-size limit.

### `build_application_events()`

Combines `a_records`, documents, applicant details, sentences, and the optional
public template into the Kafka contract. It also adds
`embedding_model="TITAN"`.

The new flow does not add NER data.

#### Details

The method places application identifiers and pipeline information at the event
level and places the loaded sections inside `after.payload`. It returns one
create event when the payload fits or splits a large sentence list into
multiple ordered events.

### `_clean_application_record()`

Removes identifiers already stored at the event level. It also formats Refugee
receipt numbers before they are sent.

#### Details

The application record is copied before it is changed. `application_id`,
`a_number`, and `analysis_group` are removed from the inner record because the
same values already exist at the event level.

### `_normalize_receipt_number()`

Changes a receipt such as `ABC123` into `ABC-00000123`.

#### Details

The method separates the alphabetic prefix from the numeric portion and pads
the number to eight characters. A receipt without a numeric portion is
returned unchanged.

### `serialized_size()`

Returns the JSON event size in bytes.

#### Details

The event is serialized and encoded as UTF-8 so size checks use the bytes Kafka
will receive rather than the number of Python characters or fields.

### `_split_sentences()`

Splits an oversized sentence list into several events. The first event is a
create event; later sentence chunks are update events.

#### Details

Application metadata stays in the first event. Later events contain the next
sentence chunk and the application record needed by the consumer to apply the
updates in order.

## 8. `kafka_processor.py`

### Description

This file serializes API events, publishes them to Kafka, waits for delivery
confirmation, and prepares confirmed delivery records for database auditing.

### `publish()`

Publishes every event using `application_id` as the Kafka key, waits for broker
confirmation, and fails if every event is not confirmed.

#### Details

Each payload is serialized before production and receives its own delivery
callback. After Kafka flushes, confirmed deliveries are saved only when no
delivery errors or pending messages remain.

### `_serialize_payload()`

Converts `after.payload` into JSON text because that is what the current Kafka
consumer expects.

#### Details

The event is deep-copied before conversion so the original payload object is
not modified. The outer event remains structured JSON.

### `_delivery_callback()`

Records either the Kafka error or the confirmed timestamp, partition, and
offset for one event.

#### Details

Successful callbacks also create the database audit value and retain the
application information needed by the transmission store. Failed callbacks add
their error text to the shared error list.

## 9. `retry.py`

### Description

This file provides the shared retry behavior used by database, Kafka, AWS,
timeout, and connection operations.

### `retry_operation()`

Retries only the failed database or Kafka operation. The default is three total
attempts with a five-second wait. A failure after all attempts becomes an
`OperationFailedError`.

#### Details

The supplied operation returns immediately after its first success. Retryable
database, Kafka, AWS, timeout, and connection failures are logged before the
next attempt, while unrelated work that already succeeded is not repeated.

## Successful end result

For one source SQS message, the service rebuilds one application, confirms its
Kafka event or events, saves the delivery audit, sends `tsp_id` to the next
queue, and finally deletes the source message.
