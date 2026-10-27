# Old and New API Transform Flow

This document explains what the current `api-transform` service does and how
`new_api_transform` keeps the same API event contract while reading the Titan
matching tables. The existing `README.md` explains how to configure and run the
new worker; this document focuses on data flow and database usage.

## Purpose of API Transform

API Transform takes a processed document ID (`tsp_id`), finds the application
that owns the document, and builds the application payload consumed by the API.
The payload combines case details, applicant details, documents, sentences, and
sentence matches. It is published to the Kafka topic
`rfs.ata.data_migration.api.events`.

## Old API Transform flow

### Old flow at a glance

```text
Read document messages from SQS
    -> Check whether the service is paused
    -> Filter messages by the enabled embedding flow
    -> Use tsp_id to find application_id and A-number
    -> Load application and confidence information
    -> Load applicant, address, and OCR information
    -> Load document and public-template information
    -> Load sentences and their similarity matches
    -> Load NER entities
    -> Combine all sections into one application payload
    -> Split a large sentence list into multiple Kafka events
    -> Publish events to Kafka
    -> Update transform status and save Kafka audit rows
    -> Send each completed tsp_id to the next SQS queue
    -> Delete the original SQS messages
```

In short, the old service receives document IDs but publishes application-level
events. Multiple received documents can belong to the same application, so it
first resolves the documents to applications and then rebuilds the current API
payload for each affected application.

### 1. Read work from SQS

The old service reads multiple SQS messages and collects up to the configured
batch size, which defaults to 50 `tsp_id` values. AWS returns at most 10 messages
per receive call, so the service may call SQS several times to build one batch.

Before processing, the service:

- Calls the DS-TSP API to determine whether API Transform is paused.
- Reads the normal queue first and the DLQ when the normal queue is empty.
- Uses `USE_TITAN_EMBEDDINGS` and the message's `embeddings_type` to decide
  whether a message can be processed by the old matching flow.
- Deletes a message that is deliberately skipped because its embedding type is
  not enabled.

### 2. Resolve documents to applications

The old service queries `tsp.text_similarity_input` using all received `tsp_id`
values. It keeps current rows and returns each distinct `application_id` and
`a_number` that must be transformed.

Example:

```text
Input tsp_ids: [101, 102]

tsp_id 101 -> application_id 10, a_number 900001
tsp_id 102 -> application_id 10, a_number 900001

One application event is built for application 10.
```

This is application-level output. Receiving one document can rebuild the
payload for every current document and sentence belonging to its application.

### 3. Build the `a_records` section

The old service combines the following data:

- Application ID, A-number, analysis group, and case type.
- Receipt number.
- Applicant case fields such as preparer, attorney, office, filing date, and
  case state.
- Global confidence and match confidence values.
- Similar-case count, similar-case A-numbers, similar application IDs, and the
  TAH flag.
- Source types used by applicant and sentence data.

The old similar-case source is `tsp.micro_service_similar_cases`. Its
`similar_case_ids` are treated as A-numbers. If the corresponding application
ID list is incomplete, the service queries `landing.application_id_lookup` to
map those A-numbers back to application IDs.

For Refugee records, a receipt such as `ABC123` is changed to
`ABC-00000123`. Repeated internal fields such as `application_id`, `a_number`,
and `analysis_group` are removed from the nested `a_records` object because
they already exist at the event level.

### 4. Build applicant details and OCR flags

The service reads the current applicant row from `biodata.applicant`. It reads
the current residence address (`address_type_cd=1`) and mailing address
(`address_type_cd=2`) from `biodata.address`. It returns the applicant fields
and calculates five OCR flags:

- Name fault
- Residence-address fault
- Mailing-address fault
- Date-of-birth fault
- Country-of-citizenship fault

An OCR flag becomes true when more than 10 percent of its available OCR bits
are `1`. `GLOBAL` and `GLOBAL_RAD` source rows skip this calculation and return
all five flags as false. The flags are calculated in Python and added to the
Kafka payload; they are not saved back to `biodata.applicant` or
`biodata.address`.

When `REDUCE_KAFKA_SIZE=true`, applicant and address timestamps are compared
with `api.transform_status.last_transmission`. Unchanged biodata can be omitted
from the event.

### 5. Build document and public-template sections

For every current document in the application, the old service returns:

- API section name
- `tsp_id`
- Load date
- File name
- Number of source sentences that contain matches

The document section name is mapped through `tsp.api_section_map`. File names
come from `landing.global_document_registry`; IVCS rows do not use that file
lookup.

For Asylee applications, the service also joins
`landing.template_registry` by A-number and adds the public-template URL and
load date.

### 6. Build sentences and sentence matches

The source sentence comes from `tsp.sentences_vector` and its document context
comes from `tsp.text_similarity_input`. The old service supports two match
storage designs:

1. `tsp.matches`, where one source sentence owns a JSON list of matches.
2. `tsp.sentence_matches`, where each source-to-target relationship is a row,
   enabled through `USE_SENTENCE_MATCHES`.

For each source sentence, the event contains the sentence ID, local line number,
document ID, section, sentence text, match count, and up to 3,000 matches. Each
match is reduced to:

```json
{"global_id": 5001, "distance": 0.1}
```

Distance is truncated to four decimal places to reduce the Kafka payload size.
When `REDUCE_KAFKA_SIZE=true` and only match data changed, the sentence can be
sent as a smaller update:

```json
{"global_id": 1001, "matches": [{"global_id": 5001, "distance": 0.1}], "op": "u"}
```

### 7. Build NER data

The old flow joins the sentence-entity and named-entity tables for every current
application document. It adds offsets, confidence score, raw text, normalized
text, entity type, and possible-sanction information to the `ner` payload
section.

The new OpenSearch/Titan API Transform flow intentionally does not include this
NER section.

### 8. Build and split Kafka events

The normal old event has this shape:

```json
{
  "after": {
    "pipeline": "Asylee",
    "a_number": 900001,
    "application_id": 10,
    "analysis_group": "Asylee",
    "payload": {
      "a_records": {},
      "documents": [],
      "applicant_details": {},
      "sentences": [],
      "ner": [],
      "public_templates": {}
    }
  },
  "op": "c"
}
```

The service processes application records in chunks of 250. It publishes the
current buffer before its estimated size reaches 450 MB.

If one application event is too large, only its sentence list is split. The
first Kafka message uses `op="c"`; later sentence chunks use `op="u"` and carry
the application record plus the next sentence list.

### 9. Publish, audit, and update status

The old service publishes events with `application_id` as the Kafka key. The
inner `after.payload` object is converted to JSON text because that is the
existing consumer contract.

After publication it:

- Updates or inserts `api.transform_status` for each application.
- Stores Kafka partition, offset, timestamp, environment, and an audit copy in
  `api.kafka_events`.
- Removes Kafka audit rows older than 60 days.
- Stores transformation failures in `landing.etlerror`.

When verbose Kafka logging is disabled, the audit copy stores the number of
sentences instead of copying the complete sentence list. The Kafka message
itself still contains the real sentences.

### 10. Complete the SQS work

After the full batch succeeds, the old service sends each `tsp_id` to the next
SQS queue in this shape:

```json
{"source": "P2", "tsp_id": "101", "sent_by": "api-transform"}
```

It then deletes the source messages in groups of at most 10. When the transform
raises an error, the downstream message and source deletion are not reached.

## Tables touched by the old flow

| Table or shared model | Access | What the old flow uses it for |
|---|---|---|
| `tsp.text_similarity_input` | Read | Resolves `tsp_id` to application/A-number and loads document metadata, section, source type, template flag, and load date. |
| `landing.application_id_lookup` | Read | Loads application, A-number, analysis group, case type, and maps similar-case A-numbers to application IDs. |
| `landing.receipt_number_lookup` | Read | Adds the case receipt number. |
| `landing.global_case_details_history` | Read | Calculates global and application confidence values. |
| `biodata.applicant` | Read | Adds applicant and case fields plus applicant OCR source values. |
| `biodata.address` | Read | Adds current residence and mailing addresses plus address OCR source values. |
| `tsp.micro_service_similar_cases` | Read | Supplies the old similar-case lists, count, similar application IDs, and TAH flag. |
| `tsp.sentences_vector` | Read | Supplies current source sentences and their timestamps. |
| `tsp.matches` | Read | Supplies the existing JSON match list and match count when row-based sentence matches are disabled. |
| `tsp.sentence_matches` | Read | Optional normalized source-to-target matches when `USE_SENTENCE_MATCHES=true`. |
| `tsp.api_section_map` | Read | Converts an internal section name into the API section name. |
| `landing.global_document_registry` | Read | Supplies the original document file name. |
| `landing.template_registry` | Read | Supplies Asylee public-template URLs. |
| `tsp.sentence_entity` | Read | Supplies sentence-level entity occurrence fields for NER. |
| `tsp.named_entity` | Read | Supplies normalized NER values, entity type, and possible-sanction flag. |
| `api.transform_status` | Read/write | Detects reduced-payload biodata changes and records the latest transmission time. |
| `api.kafka_events` | Delete/insert | Removes expired audits and stores broker-confirmed Kafka delivery details. |
| `landing.etlerror` | Insert | Records an API Transform failure. |

The exact ORM schema names come from the shared `ta_db_connection.atatables`
models. The table names above match the database objects referenced by this
project and the replacement SQL.

## New API Transform flow

The new flow keeps the API event structure but processes one source SQS message
at a time:

1. Read one message containing `tsp_id`. The DLQ can return several messages,
   but each message is still transformed independently.
2. Use `tsp.text_similarity_input` to resolve the document to one application.
3. Load application, confidence, applicant, OCR, document, template, and
   sentence data for that application.
4. Read sentence relationships from all 16 rows in `tsp.titan_matches` and
   convert the JSONB key/value objects back into the existing API match list.
5. Read case-level results from `tsp.titan_similar_cases`.
6. Map Titan `similar_case_ids` from application IDs to A-numbers through
   `landing.application_id_lookup`, because the existing API field still expects
   A-numbers.
7. Build the existing Kafka payload without NER and add
   `embedding_model="TITAN"` to identify the vector source.
8. Split only the sentence list when one application payload is too large.
9. Publish to Kafka and require an acknowledgement for every event.
10. In one database transaction, save confirmed audits, update transform status,
    and remove audit rows older than 60 days.
11. Send the completed `tsp_id` to the next queue and delete the source message.
    A failed step leaves the source message available for the configured SQS
    retry and DLQ behavior.

Although the Python package is named `opensearch`, API Transform does not run a
new vector search. Sentence Similarity and Case Similarity have already written
the OpenSearch/Titan results to `tsp.titan_matches` and
`tsp.titan_similar_cases`; API Transform reads those prepared PostgreSQL rows.

## Tables touched by the new flow

| Table | Access | What the new flow uses it for |
|---|---|---|
| `tsp.text_similarity_input` | Read | Resolves one `tsp_id`, loads all current application documents, and supplies source and template fields. |
| `landing.application_id_lookup` | Read | Loads the application record and maps Titan application IDs to API A-number values. |
| `landing.receipt_number_lookup` | Read | Adds the receipt number. |
| `landing.global_case_details_history` | Read | Calculates confidence values. |
| `biodata.applicant` | Read | Adds current applicant details and OCR source values. |
| `biodata.address` | Read | Adds current residence/mailing addresses and OCR source values. |
| `tsp.titan_similar_cases` | Read | Supplies similar applications, similar cases, counts, and TAH status. |
| `tsp.sentences_vector` | Read | Supplies current application sentences. |
| `tsp.titan_matches` | Read | Supplies all bucketed Titan matches and distances for each source sentence. |
| `tsp.api_section_map` | Read | Converts internal sections to API section names. |
| `landing.global_document_registry` | Read | Supplies document file names. |
| `landing.template_registry` | Read | Supplies Asylee public-template information. |
| `api.transform_status` | Read/write | Supports reduced payloads and records a confirmed transmission. |
| `api.kafka_events` | Delete/insert | Removes expired audits and stores confirmed Kafka delivery information. |
| `landing.etlerror` | Insert | Stores a transformation failure. |

The new flow does not read `tsp.matches`, `tsp.sentence_matches`,
`tsp.micro_service_similar_cases`, `tsp.sentence_entity`, or `tsp.named_entity`.

## Main differences

| Area | Old API Transform | New API Transform |
|---|---|---|
| Work size | Collects up to 50 document messages before transforming. | Processes one document message at a time. |
| Matching source | Reads `matches` or optional `sentence_matches`. | Reads all 16 `titan_matches` buckets. |
| Case summary | Reads `micro_service_similar_cases`. | Reads `titan_similar_cases`. |
| Similar-case storage | Similar-case IDs are A-numbers. | Stored IDs are application IDs and are mapped to A-numbers for the existing API field. |
| NER | Reads and sends NER. | Does not include NER in this flow. |
| Embedding model | Not added to the event. | Adds `embedding_model="TITAN"` to every event. |
| Service pause call | Calls the DS-TSP service-status API. | Does not make the service-status API call. |
| Embedding routing | Filters messages with embedding-type toggles. | Dedicated Titan flow; no old embedding filter is needed. |
| Kafka confirmation | Flushes and stores successful callback results. | Requires every event to receive a successful callback before completing. |
| Failure handling | A batch failure prevents completion of that batch. | One failed document remains on SQS and does not block unrelated messages. |

## Result

The new service changes where similarity data comes from, not what the API
consumer receives. Existing document, applicant, sentence, payload-splitting,
Kafka audit, transform-status, and downstream SQS behaviors are preserved. The
intentional differences are one-document processing, Titan tables, the Titan
embedding-model marker, stronger per-message delivery confirmation, and
removal of NER from this flow.
