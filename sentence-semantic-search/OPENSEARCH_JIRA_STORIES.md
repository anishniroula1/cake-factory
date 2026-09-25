# OpenSearch Jira Stories

## Environment variable

```text
ENABLE_OPENSEARCH_SENTENCE_FLOW
```

- `true`: Use the new OpenSearch flow.
- `false` or missing: Continue using the existing flow.

Use the same environment-variable name for DOC2Vec, Similarity Service, and
the State Machine configuration.

## Doc2Vec | Add OpenSearch flow toggle

Story number: 1

### Value

Allow DOC2Vec to switch safely between the existing flow and the new
OpenSearch flow.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
When DOC2Vec processes a document
Then it uses the new OpenSearch flow
```

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is false or missing
When DOC2Vec processes a document
Then it continues using the existing flow without any changes
```

## Similarity Service | Add OpenSearch flow toggle

Story number: 2

### Value

Allow Similarity Service to switch safely between the existing matching flow
and the new OpenSearch flow.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
When Similarity Service processes a document
Then it uses the new OpenSearch similarity flow
```

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is false or missing
When Similarity Service processes a document
Then it continues using the existing flow without any changes
```

## State Machine | Route OpenSearch document statuses

Story number: 3

### Value

Route documents through Similarity Service and API Transform when the
OpenSearch flow is enabled.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
When DOC2Vec finishes processing a document
Then the document status in the tsp table is updated to Similarity
And when Similarity Service finishes, the document status is updated to API Transform
```

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is false or missing
When the document is processed
Then the existing document status flow remains unchanged
```

## DevOps | Provision OpenSearch Bedrock model and pipeline

Story number: 4

### Value

Create the OpenSearch ML resources through Terraform so the sentence services
can generate embeddings using AWS Titan. Expose the created IDs through
Terraform outputs so Harness can add them to the application environment
variables.

### Acceptance Criteria

```gherkin
Given the OpenSearch cluster is available
When Terraform is applied
Then Terraform creates the model group
And creates the connector for the Lasso proxy and AWS Titan
And registers and deploys the embedding model
And creates the OpenSearch embedding ingest pipeline
And the pipeline creates an embedding only when sentenceContentVector is missing
```

```gherkin
Given Terraform finishes successfully
When Terraform outputs are viewed
Then the following values are available for the Harness pipeline:

SENTENCE_OPENSEARCH_MODEL_GROUP_ID
SENTENCE_OPENSEARCH_CONNECTOR_ID
SENTENCE_OPENSEARCH_MODEL_ID
SENTENCE_OPENSEARCH_EMBEDDING_PIPELINE
```

Harness uses these outputs to populate the environment file for the sentence
services. Connector credentials and other secrets must not appear in plain
Terraform outputs.

## DevOps | Create initial OpenSearch sentence indexes and aliases

Story number: 5

### Value

Create the initial OpenSearch indexes required to store sentence occurrences,
collect vectors for IVF training, and identify tracer or form-language
sentences.

The production IVF-SQ16 semantic catalog will be created later, after enough
training vectors are available and the IVF model finishes training.

### Acceptance Criteria

```gherkin
Given the OpenSearch cluster and Bedrock embedding pipeline are available
When Terraform is applied
Then Terraform creates the sentence occurrence index
And creates the sentence training index
And creates the tracer and form-language index
And creates an alias for each index
```

```gherkin
Given Terraform finishes successfully
When Terraform outputs are viewed
Then all three physical index names and alias names are available
And every alias points to its correct physical index
```

### Terraform outputs

```text
OPENSEARCH_OCCURRENCE_INDEX
OPENSEARCH_OCCURRENCE_ALIAS

OPENSEARCH_TRAINING_INDEX
OPENSEARCH_TRAINING_ALIAS

OPENSEARCH_TRACER_FORM_LANGUAGE_INDEX
OPENSEARCH_TRACER_FORM_LANGUAGE_ALIAS
```

### Sentence occurrence index

Physical index: `sentence_occurrences-v1`  
Alias: `sentence_occurrences`

```http
PUT /sentence_occurrences-v1
{
  "settings": {
    "number_of_shards": 18,
    "number_of_replicas": 1
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "globalId": {
        "type": "long"
      },
      "applicationId": {
        "type": "keyword"
      },
      "tspId": {
        "type": "keyword"
      },
      "sectionName": {
        "type": "keyword"
      },
      "sentIdLocal": {
        "type": "long"
      },
      "sentenceContent": {
        "type": "text"
      },
      "sentenceKey": {
        "type": "keyword"
      },
      "sourceType": {
        "type": "keyword"
      },
      "analysisGroup": {
        "type": "keyword"
      },
      "isNonMatcher": {
        "type": "boolean"
      },
      "isTracer": {
        "type": "boolean"
      },
      "isFormLanguage": {
        "type": "boolean"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_occurrences-v1",
        "alias": "sentence_occurrences"
      }
    }
  ]
}
```

### Sentence vector training index

This index collects representative 512-dimension Titan vectors. It does not
use a trained IVF model. After enough representative vectors are loaded,
OpenSearch uses this index as the `training_index` when training the IVF-SQ16
model.

Physical index: `sentence_semantic_training-v1`  
Alias: `sentence_semantic_training`

```http
PUT /sentence_semantic_training-v1
{
  "settings": {
    "number_of_shards": 6,
    "number_of_replicas": 1,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {
        "type": "keyword"
      },
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "dimension": 512
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_training-v1",
        "alias": "sentence_semantic_training"
      }
    }
  ]
}
```

The later IVF training request will use:

```json
{
  "training_index": "sentence_semantic_training-v1",
  "training_field": "sentenceContentVector",
  "dimension": 512
}
```

### Tracer and form-language catalog

This small reference catalog uses Lucene HNSW. It does not require the trained
IVF model.

Physical index: `tracer_form_language_catalog-v1`  
Alias: `tracer_form_language_catalog`

```http
PUT /tracer_form_language_catalog-v1
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "dimension": 512,
        "space_type": "cosinesimil",
        "method": {
          "name": "hnsw",
          "engine": "lucene"
        }
      },
      "isTracer": {
        "type": "boolean"
      },
      "isFormLanguage": {
        "type": "boolean"
      },
      "analysisGroup": {
        "type": "keyword"
      },
      "isNonMatcher": {
        "type": "boolean"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "tracer_form_language_catalog-v1",
        "alias": "tracer_form_language_catalog"
      }
    }
  ]
}
```

The production `sentence_semantic_catalog-ivf-sq16-v1` index and its alias are
created in a separate story after the IVF-SQ16 model state becomes `created`.

## DevOps | Deploy Doc2Vec Backfill

Story number: 6

### Value

Run a second deployment of the existing Doc2Vec Service for historical
documents, model-training data, and tracer sentences. This keeps backfill work
away from the existing Doc2Vec deployment without creating a separate service
that does the same work.

### Acceptance Criteria

```gherkin
Given the existing Doc2Vec Service deployment is available
When the Harness deployment pipeline runs
Then a second deployment named Doc2Vec Backfill is running
And it uses the same Doc2Vec application and container image
And the existing Doc2Vec deployment remains unchanged
```

```gherkin
Given Doc2Vec Backfill is running
When backfill work is received
Then the work is processed by Doc2Vec Backfill
And it does not use the existing Doc2Vec Service consumer
```

## DevOps | Create Doc2Vec Backfill queue and DLQ

Story number: 7

### Value

Create a separate SQS queue for Doc2Vec Backfill so training data, tracer
sentences, and historical data do not affect the existing Doc2Vec queue.

### Acceptance Criteria

```gherkin
Given the infrastructure is deployed
When Terraform is applied
Then a FIFO SQS queue is created for Doc2Vec Backfill
And a FIFO DLQ is created and connected through the queue redrive policy
And Doc2Vec Backfill listens to the new queue
```

```gherkin
Given Terraform finishes successfully
When Terraform outputs are viewed
Then the following values are available:

DOC2VEC_BACKFILL_QUEUE_URL
DOC2VEC_BACKFILL_QUEUE_ARN
DOC2VEC_BACKFILL_DLQ_URL
DOC2VEC_BACKFILL_DLQ_ARN

And Harness adds DOC2VEC_BACKFILL_QUEUE_URL to the Doc2Vec Backfill environment variables
```

## DevOps | Allow Doc Orchestration to send Doc2Vec Backfill messages

Story number: 8

### Value

Allow the Doc Orchestration Lambda to send backfill messages to the new
Doc2Vec Backfill queue.

### Acceptance Criteria

```gherkin
Given the Doc2Vec Backfill queue exists
When Terraform is applied
Then the Doc Orchestration Lambda role has sqs:SendMessage permission for the new queue
```

```gherkin
Given the Doc Orchestration Lambda sends a valid backfill message
When the message is published
Then the message is available in the Doc2Vec Backfill queue
```

## Doc Orchestration Lambda | Route Doc2Vec Backfill files

Story number: 9

### Value

Route model-training and tracer files from the doc-to-process bucket to the
Doc2Vec Backfill queue. The message reason tells Doc2Vec Backfill how to process
the IDs.

### Rule

```json
{
  "source_type": "DOC2VEC_BACKFILL",
  "required_fields": ["reason"],
  "queue_variable": "DOC2VEC_BACKFILL_QUEUE_URL",
  "is_fifo": true
}
```

The Lambda validates the ID field based on the reason:

```text
TRAIN_MODEL requires tsp_ids
TRACER requires tracer_sentence_ids
HISTORICAL requires tsp_id
```

### Acceptance Criteria

```gherkin
Given a document with source_type DOC2VEC_BACKFILL is added to the doc-to-process bucket
When the document contains a supported reason and its required ID field
Then Doc Orchestration sends the reason and IDs to the Doc2Vec Backfill queue

Examples:
  | reason      | required ID field    |
  | TRAIN_MODEL | tsp_ids              |
  | TRACER      | tracer_sentence_ids  |
  | HISTORICAL  | tsp_id               |
```

## Doc2Vec Backfill | Load tracer data into OpenSearch and reuse saved vectors

Story number: 10

### Value

Load tracer and form-language sentences into their OpenSearch catalog so
Sentence Similarity can identify sentences that must not participate in
matching.

During bulk indexing, include `sentenceContentVector` when the tracer-table row
already has a saved 512-dimension vector. When it does not, leave the vector
field out so the Bedrock embedding pipeline creates it.

### OpenSearch client

Harness must populate these environment variables for Doc2Vec Backfill:

```text
AWS_REGION=us-east-1
IAM_ACCESS_ROLE=<OpenSearch role ARN>
OPENSEARCH_HOST=<OpenSearch domain host>
OPENSEARCH_PORT=443
OPENSEARCH_SERVICE=es
OPENSEARCH_TRACER_FORM_LANGUAGE_ALIAS=tracer_form_language_catalog
OPENSEARCH_TRAINING_ALIAS=sentence_semantic_training
```

Create the AWS-signed client using the same approach as the existing
`open_search_client.py`:

```python
import os

import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection


def create_opensearch_client():
    region = os.environ["AWS_REGION"]
    role_arn = os.environ["IAM_ACCESS_ROLE"]
    host = os.environ["OPENSEARCH_HOST"]
    port = int(os.getenv("OPENSEARCH_PORT", "443"))
    service = os.getenv("OPENSEARCH_SERVICE", "es")

    base_session = boto3.Session(region_name=region)
    response = base_session.client("sts").assume_role(
        RoleArn=role_arn,
        RoleSessionName="doc2VecBackfillOpenSearchSession",
    )
    role_credentials = response["Credentials"]
    role_session = boto3.Session(
        aws_access_key_id=role_credentials["AccessKeyId"],
        aws_secret_access_key=role_credentials["SecretAccessKey"],
        aws_session_token=role_credentials["SessionToken"],
        region_name=region,
    )
    auth = AWSV4SignerAuth(role_session.get_credentials(), region, service)

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
        timeout=60,
        max_retries=5,
        retry_on_timeout=True,
    )
```

### Acceptance Criteria

```gherkin
Given Harness provides the OpenSearch host and IAM_ACCESS_ROLE environment values
And Doc2Vec Backfill receives an SQS message with reason TRACER
And the message contains tracer_sentence_ids
When the message is processed
Then Doc2Vec Backfill creates an AWS-signed OpenSearch client using the assumed role
And loads those records from the tracer table
And uses the saved embedding when a record already has a 512-dimension vector
And allows the Bedrock embedding pipeline to create the vector when it is missing
And indexes them in the tracer_form_language_catalog alias
And deletes the source SQS message only after every record is indexed successfully
```

```gherkin
Given a tracer or form-language sentence already exists in the catalog
When the same tracer_sentence_id is processed again
Then the existing OpenSearch document is updated
And a duplicate document is not created
```

## Doc2Vec Backfill | Load training data by tsp_id and reuse saved vectors

Story number: 11

### Value

Load representative sentence vectors into the OpenSearch training index so the
Faiss IVF-SQ16 model can be trained later.

Use the AWS-signed OpenSearch client and environment variables defined in Story
10.

During bulk indexing, include `sentenceContentVector` when the
`sentences_vector` row already has a saved 512-dimension vector. When it does
not, leave the vector field out so the Bedrock embedding pipeline creates it.

The training path generates `sentenceKey` from `sentenceContent` in memory. It
uses the generated key to remove exact duplicates and as the stable OpenSearch
document `_id`. It does not save the generated key back to the
`sentences_vector` table during training.

### Processing rules

1. Split the incoming `tsp_ids` into batches and send each batch to a worker.
2. Each worker creates its own SQLAlchemy session and loads all sentences for
   its `tsp_id` batch with one database query.
3. Generate sentence keys in memory and keep one record for each unique key.
4. Reuse saved vectors and send records without vectors through the Bedrock
   embedding pipeline.
5. Bulk-index the records. Vectors created by the Bedrock pipeline remain in
   OpenSearch and are not saved back to `sentences_vector`.
6. Retry the complete worker function up to 10 times for a temporary failure,
   waiting 5 seconds between attempts.
7. Delete the SQS message only after every worker batch succeeds.

The worker and retry functions must be reusable for tracer processing and
later OpenSearch backfills.

### Acceptance Criteria

```gherkin
Given Doc2Vec Backfill receives an SQS message with reason TRAIN_MODEL
And the message contains tsp_ids
When the message is processed
Then Doc2Vec Backfill uses the tsp_ids to load the matching global_ids from the sentences_vector table
And generates a sentence key in memory without saving it to sentences_vector
And uses the saved embedding when a global_id already has a 512-dimension vector
And allows the Bedrock embedding pipeline to create the vector when it is missing
And indexes each unique sentence in the sentence_semantic_training alias
And deletes the source SQS message only after every worker batch succeeds
```

```gherkin
Given a training worker has a temporary database, OpenSearch, Lasso, or Bedrock failure
When the worker retries the batch
Then it reruns the same reusable processor function
And stable sentence-key document IDs prevent duplicate training documents
And the SQS message remains available until every batch succeeds
```

## DevOps | Train the production IVF-SQ16 model from sentence vectors

Story number: 12

### Value

Train the production Faiss IVF-SQ16 model from the representative 512-dimension
vectors loaded into the sentence training index by Story 11.

Terraform must submit the training request and expose the model ID. This is an
OpenSearch k-NN model and does not use the ML Commons deploy API.

### IVF training information

```text
Training index: sentence_semantic_training-v1
Training field: sentenceContentVector
Dimension: 512
Similarity: cosinesimil
Engine: faiss
Method: ivf
nlist: 16384
nprobes: 128
Encoder: sq
SQ bits: 16
SQ clip: false
Maximum training vectors: 4194304
Search size: 10000
```

```http
POST /_plugins/_knn/models/sentence-ivf-sq16-512-nlist16384-v1/_train
{
  "training_index": "sentence_semantic_training-v1",
  "training_field": "sentenceContentVector",
  "dimension": 512,
  "max_training_vector_count": 4194304,
  "search_size": 10000,
  "description": "Production Faiss IVF-SQ16 model for Titan 512 sentence vectors",
  "space_type": "cosinesimil",
  "method": {
    "name": "ivf",
    "engine": "faiss",
    "parameters": {
      "nlist": 16384,
      "nprobes": 128,
      "encoder": {
        "name": "sq",
        "parameters": {
          "bits": 16,
          "clip": false
        }
      }
    }
  }
}
```

### Acceptance Criteria

```gherkin
Given sentence_semantic_training-v1 contains representative 512-dimension vectors
When the Terraform training deployment runs
Then it submits the IVF-SQ16 training request with the configuration in this story
And Terraform exposes OPENSEARCH_IVF_MODEL_ID
And the training status can be viewed without creating the sentence catalog
```

```gherkin
Given the IVF-SQ16 training request has been submitted
When OpenSearch reports the model state
Then the model must reach created with no error before the catalog deployment can run
And a failed model stops the catalog deployment
```

Check the model status with:

```http
GET /_plugins/_knn/models/sentence-ivf-sq16-512-nlist16384-v1?filter_path=model_id,state,error,dimension,space_type,method
```

## DevOps | Create the production sentence catalog after IVF training

Story number: 13

### Value

Create the production sentence semantic catalog only after the IVF-SQ16 model
reaches the `created` state. The catalog uses the trained model for vector
indexing and the Bedrock pipeline for missing embeddings.

### Index mapping

```http
PUT /sentence_semantic_catalog-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": 18,
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {
        "type": "keyword"
      },
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<trained IVF-SQ16 model ID>"
      },
      "createdAt": {
        "type": "date"
      },
      "updatedAt": {
        "type": "date"
      }
    }
  }
}
```

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_catalog-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog"
      }
    }
  ]
}
```

### Acceptance Criteria

```gherkin
Given OPENSEARCH_IVF_MODEL_ID has state created and no training error
When the Terraform catalog deployment runs
Then it creates sentence_semantic_catalog-ivf-sq16-v1 with that model ID
And creates the sentence_semantic_catalog alias
And exposes OPENSEARCH_CATALOG_INDEX and OPENSEARCH_CATALOG_ALIAS for Harness
```

```gherkin
Given the IVF model is missing, training, or failed
When the catalog deployment runs
Then the deployment stops without creating the sentence catalog
```

## Doc2Vec Backfill | Export historical tsp_id values into chunked CSV files

Story number: 14

### Value

Create CSV files containing historical `tsp_id` values and upload them to the
existing `doc-to-process` bucket. Doc Orchestration can then route each
historical `tsp_id` to the Doc2Vec Backfill SQS queue.

### CSV format

```csv
source_type,reason,tsp_id
DOC2VEC_BACKFILL,HISTORICAL,TSP-10001
DOC2VEC_BACKFILL,HISTORICAL,TSP-10002
```

### Acceptance Criteria

```gherkin
Given historical records exist in the sentences_vector table
When SQL runs
Then it reads the unique tsp_id values from the table
And creates CSV files with reason and source type headers
And every CSV row contains source_type DOC2VEC_BACKFILL, reason HISTORICAL, and one tsp_id
```

```gherkin
Given a historical backfill CSV file is complete
When the file is uploaded
Then it is added to DOC_TO_PROCESS_BUCKET
And Doc Orchestration processes the file using the DOC2VEC_BACKFILL rule
And each tsp_id is routed to the Doc2Vec Backfill SQS queue
```

## Doc2Vec Backfill | Process a historical Sentences

Story number: 15

### Value

Process every historical sentence for one `tsp_id` from the `sentences_vector`
table.

Harness must provide `SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL` to Doc2Vec
Backfill.

### Processing rules

1. Receive a FIFO SQS message containing `reason=HISTORICAL` and one `tsp_id`.
2. Load rows for that `tsp_id` from `sentences_vector` in database batches.
3. Generate `sentenceKey` for every sentence and save it to the corresponding
   `sentences_vector` row.
4. The coordinator owns one SQLAlchemy session. It flushes each database batch
   but does not commit while workers are still running.
5. Workers receive plain record data. A SQLAlchemy session is never shared
   across worker threads.
6. Each worker checks the sentence catalog using `sentenceKey` and creates a
   catalog document only when that key does not already exist.
7. Each worker bulk-writes the missing sentence catalog documents by
   `sentenceKey` and every sentence occurrence document by `globalId`.
8. Existing vectors are reused. Records without vectors use the Bedrock
   embedding pipeline.
9. A failed worker reruns the complete batch function. Stable OpenSearch IDs
   make the retry safe and prevent duplicate documents.
10. Each worker makes up to 10 immediate attempts and waits 5 seconds after a
   temporary failure. If the attempts are exhausted, the database transaction
   is rolled back and the source SQS message is not deleted, allowing SQS to
   deliver it again under the queue redrive policy.
11. Commit the database transaction only after all sentences for the `tsp_id`
    exist in both OpenSearch indexes.
12. After the commit succeeds, send the `tsp_id` to
    `SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL`.
13. Delete the SQS message that Doc2Vec Backfill received.

### Acceptance Criteria

```gherkin
Given Doc2Vec Backfill receives a message with reason HISTORICAL and a tsp_id
When the tsp_id is processed
Then its sentences are loaded from sentences_vector in batches
And sentence keys are generated, saved, and flushed for each batch
And the sentence catalog is checked using each sentence key
And only missing sentence keys are added to the sentence catalog
And multiple workers write the occurrence and sentence catalog documents
And failed worker batches are safely retried
```

```gherkin
Given every sentence for the tsp_id exists in both OpenSearch indexes
When processing completes
Then the sentences_vector transaction is committed once
And the tsp_id is sent to SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL
And the SQS message received by Doc2Vec Backfill is deleted
```

## Doc2Vec | Add incoming document to OpenSearch sentence occurrence and catalog index

Story number: 16

### Value

Process incoming real-time sentences in small parallel batches and add them to
`sentences_vector`, the sentence occurrence index, and the sentence catalog.
Retry temporary failures without losing sentences or creating duplicate
records.

The existing Doc2Vec logic already receives the regular SQS message, loads the
document text using `tsp_id`, and splits the text into sentences. This story
starts after those steps.

### Processing rules

1. Split the generated sentences into batches of 10.
2. Send each batch to the configured worker pool. A worker processes one batch
   and then takes another available batch.
3. Every worker creates its own SQLAlchemy session. Sessions are never shared
   between workers.
4. For each sentence, generate `sentenceKey`, insert or safely reuse its
   `sentences_vector` row, and flush to receive `globalId`.
5. Check the sentence catalog using the batch's sentence keys.
6. Bulk-index only the missing catalog documents and let the Bedrock pipeline
   create their vectors.
7. Bulk-index every occurrence document using `globalId` as its stable
   OpenSearch `_id`.
8. Commit the worker's database batch only after its catalog and occurrence
   writes succeed. The worker can then process another batch.
9. If PostgreSQL, OpenSearch, Lasso, or Bedrock has a temporary failure, roll
   back that worker's database session and rerun the complete batch function.
10. Retry each batch up to 10 times and wait 5 seconds between attempts. A
    Lasso throttling response also activates the shared cooldown so workers do
    not immediately send more embedding requests.
11. Retrying a batch must reuse the same database sentence rows, sentence keys,
    catalog IDs, and occurrence IDs instead of creating duplicates.
12. If a batch still fails after 10 attempts, do not send the State Machine
    message and do not delete the regular Doc2Vec SQS message. SQS can deliver
    the document again according to its redrive policy.
13. After every sentence batch succeeds, send the existing message to the
    State Machine and then delete the regular Doc2Vec SQS message using the
    existing completion flow.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
And Doc2Vec has split the incoming document text into sentences
When the sentences are processed
Then Doc2Vec splits them into batches of 10
And different workers process the available batches
And every worker uses its own SQLAlchemy session
And each successful batch is committed before that worker takes another batch
```

```gherkin
Given a worker processes a sentence batch
When it writes the batch to PostgreSQL and OpenSearch
Then it generates and saves each sentenceKey in sentences_vector
And receives the globalId values after flushing the database session
And checks the sentence catalog using each sentenceKey
And creates a catalog document only when the sentenceKey does not already exist
And creates one sentence occurrence document for every globalId
And retries the complete batch after a temporary failure
And sends the State Machine message and deletes the source SQS message only after every batch succeeds
```

## DevOps | Create Sentence Similarity Backfill FIFO queue and DLQ

Story number: 17

### Value

Create a separate FIFO queue for historical sentence matching so backfill work
does not use the existing real-time Sentence Similarity queue.

### Acceptance Criteria

```gherkin
Given the infrastructure deployment runs
When Terraform is applied
Then a FIFO SQS queue is created for Sentence Similarity Backfill
And a FIFO DLQ is connected through the queue redrive policy
And the queue and DLQ can be viewed in the AWS environment
```

```gherkin
Given Terraform finishes successfully
When Terraform outputs are viewed
Then the following values are available:

SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL
SENTENCE_SIMILARITY_BACKFILL_QUEUE_ARN
SENTENCE_SIMILARITY_BACKFILL_DLQ_URL
SENTENCE_SIMILARITY_BACKFILL_DLQ_ARN
```

## DevOps | Deploy Sentence Similarity Backfill with the existing service image

Story number: 18

### Value

Run a second deployment of the existing Sentence Similarity Service for
historical processing. Real-time and backfill processing use the same
application code and differ only by the SQS queue supplied by Harness.

### Acceptance Criteria

```gherkin
Given the existing Sentence Similarity Service image is available
When the Harness deployment pipeline runs
Then a second deployment named Sentence Similarity Backfill is running
And it uses the same application code and container image as Sentence Similarity
And the existing real-time deployment remains unchanged
```

```gherkin
Given both deployments are running
When Harness provides their environment variables
Then the real-time deployment receives the existing queue URL as SENTENCE_SIMILARITY_QUEUE_URL
And the backfill deployment receives SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL as SENTENCE_SIMILARITY_QUEUE_URL
And both deployments call the same sentence-processing function
```

## Sentence Similarity | Process tsp_id sentences with shared batch workers

Story number: 19

### Value

Use one reusable batch processor for real-time and historical sentence
matching. Each deployment receives messages from its own queue but runs the
same matching code.

### Processing rules

1. Receive an SQS message containing one `tsp_id`.
2. Load the current sentences for that `tsp_id` from `sentences_vector` in
   batches of 10.
3. Send each batch to the configured worker pool.
4. Every worker creates its own SQLAlchemy session and processes one source
   sentence at a time from its batch.
5. Commit one source sentence after its classification and all bidirectional
   match updates succeed.
6. Retry a source sentence up to 10 times for temporary PostgreSQL or
   OpenSearch failures, waiting 5 seconds between attempts.
7. Stable keys and idempotent database updates must make reprocessing safe.
8. If any sentence remains failed, do not delete the received SQS message.
9. Delete the received SQS message only after every sentence for the `tsp_id`
   succeeds.

### Acceptance Criteria

```gherkin
Given either Sentence Similarity deployment receives an SQS message with a tsp_id
When the message is processed
Then sentences_vector is read in batches of 10
And different workers process the available batches
And each worker uses its own SQLAlchemy session
And each source sentence is committed only after its complete matching work succeeds
```

```gherkin
Given a source sentence has a temporary PostgreSQL or OpenSearch failure
When the worker retries it
Then the same reusable sentence processor runs again without duplicate relationships
And the received SQS message remains available until every sentence succeeds
```

## Sentence Similarity | Classify tracer and form-language sentences before matching

Story number: 20

### Value

Check the tracer and form-language catalog before the main sentence catalog so
non-matcher sentences never receive forward or reverse relationships.

### Acceptance Criteria

```gherkin
Given a worker begins processing a source sentence
When its vector is searched against tracer_form_language_catalog for the same analysisGroup
And a qualifying tracer or form-language reference is found
Then sentences_vector is updated with isNonMatcher, isTracer, and isFormLanguage
And the main sentence catalog is not searched for that source sentence
And no relationship is saved for that source sentence
```

```gherkin
Given a source sentence does not match a non-matcher reference
When the main sentence catalog returns a target sentence
Then the target is also checked against tracer_form_language_catalog before either relationship is saved
And an ineligible target is updated in sentences_vector and excluded from both directions
And an eligible target continues to exact and semantic matching
```

## Sentence Similarity | Save exact and semantic matches in both directions

Story number: 21

### Value

Find exact and semantic sentence matches and save both directions in the
existing `matches` JSONB list and the new `sentence_key_matches` table.

### Acceptance Criteria

```gherkin
Given a source sentence is allowed to participate in matching
When Sentence Similarity searches for matches
Then exact occurrences are found using sentenceKey
And semantic keys are found in sentence_semantic_catalog using the source vector and threshold
And matching keys are expanded to sentences_vector occurrences in the same analysisGroup
And occurrences from the same application are excluded
And tracer, form-language, and other non-matcher occurrences are excluded
```

```gherkin
Given an eligible source occurrence matches an eligible target occurrence
When the database transaction is saved
Then the source matches JSONB list contains the target occurrence
And the target matches JSONB list contains the source occurrence
And sentence_key_matches contains both key directions with the same distance
And an existing relationship is updated instead of duplicated
And concurrent updates use a stable lock order so another worker cannot overwrite a relationship
And the received SQS message is deleted after every source sentence succeeds
```

## Java | Add Flyway migration for sentence_key_matches and sentence-key indexes

Story number: 22

### Value

Create a Flyway migration for the new `sentence_key_matches` table and the
`sentences_vector` indexes required to find and expand matching sentence keys
without scanning the full table.

### Table migration

```sql
CREATE TABLE IF NOT EXISTS sentence_key_matches (
    sentence_key TEXT NOT NULL,
    analysis_group TEXT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,
    match_key_count INTEGER GENERATED ALWAYS AS (
        jsonb_object_length(matches)
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (sentence_key, analysis_group),

    CONSTRAINT sentence_key_matches_key_format
        CHECK (sentence_key ~ '^[0-9a-f]{64}$'),

    CONSTRAINT sentence_key_matches_json_object
        CHECK (jsonb_typeof(matches) = 'object')
);
```

### Index migrations

Create the following indexes against the existing `sentences_vector` table:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_sentences_vector_eligible_group_key
ON sentences_vector (analysis_group, sentence_key)
INCLUDE (
    global_id,
    application_id,
    tsp_id,
    section_name,
    sent_id_local
);
```

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_sentences_vector_current_key
ON sentences_vector (sentence_key)
WHERE is_current = TRUE;
```

Create the table in a normal Flyway migration. Run the
`CREATE INDEX CONCURRENTLY` statements in a separate Flyway migration with
`executeInTransaction=false` because PostgreSQL does not allow concurrent index
creation inside a transaction.

After the migration finishes, run:

```sql
ANALYZE sentences_vector;
ANALYZE sentence_key_matches;
```

### Why each index is needed

| Table | Index | Why it is needed |
|---|---|---|
| `sentence_key_matches` | Primary key on `(sentence_key, analysis_group)` | This is the normal lookup path. Sentence Similarity and API reads know the source key and analysis group, so PostgreSQL can fetch one JSONB match object directly. The same index is also the conflict target for safe retries and bidirectional upserts. It prevents duplicate rows for the same key and analysis group. |
| `sentences_vector` | `ix_sentences_vector_eligible_group_key` | This expands one exact or similar sentence key into all eligible occurrence `global_id` values in the same analysis group. The partial conditions keep tracer, form-language, non-matcher, unchecked, old-version rows out of the index. The included columns provide the IDs and document information used by the matching response without placing the large sentence text in the index. |
| `sentences_vector` | `ix_sentences_vector_current_key` | This quickly answers whether a sentence key is still used by any current occurrence. It is used for catalog checks and cleanup even when the occurrence is a tracer or form-language sentence, which is why this smaller index filters only on `is_current`. |

Do not create a JSONB GIN index on `sentence_key_matches` for the initial
release. Every accepted relationship is stored in both directions, so the
application reads the target row through the composite primary key instead of
searching every JSONB object for a reverse key. A GIN index would consume more
storage and make every JSONB update more expensive. It should be added later
only if frequent whole-table reverse-key audit queries become a real
requirement.

### Acceptance Criteria

```gherkin
Given the Flyway migrations are deployed
When the migration completes
Then sentence_key_matches exists with the composite primary key and JSONB constraints
And ix_sentences_vector_eligible_group_key exists
And ix_sentences_vector_current_key exists
And the concurrent-index Flyway migration uses executeInTransaction=false
```

```gherkin
Given a sentence key and analysis group are provided
When Java reads or upserts sentence_key_matches
Then PostgreSQL uses the composite primary key without scanning the table
And matching keys can be expanded through ix_sentences_vector_eligible_group_key
And retrying the same relationship does not create another sentence_key_matches row
```
