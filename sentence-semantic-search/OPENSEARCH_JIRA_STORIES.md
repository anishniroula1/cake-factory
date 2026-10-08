# OpenSearch Jira Stories

## Environment variable

```text
ENABLE_OPENSEARCH_SENTENCE_FLOW
```

- `true`: Use the new OpenSearch flow.
- `false` or missing: Continue using the existing flow.

Use the same environment-variable name for DOC2Vec, Similarity Service, and
the State Machine configuration.

## 1. Doc2Vec | Add OpenSearch flow toggle

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

## 2. Similarity Service | Add OpenSearch flow toggle

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

## 3. State Machine | Route OpenSearch document statuses

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

## 4. DevOps | Provision OpenSearch Bedrock model and pipeline

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

## 5. DevOps | Create initial OpenSearch sentence indexes and aliases

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
And the occurrence mapping does not contain isNonMatcher, isTracer, isFormLanguage, or nonMatcherChecked
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
        "type": "long"
      },
      "tspId": {
        "type": "long"
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
      "isCurrent": {
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
      "tracerId": {
        "type": "integer"
      },
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

The production Asylee and Refugee semantic catalogs and their aliases are
created in a separate story after the shared IVF-SQ16 model state becomes
`created`.

## 6. DevOps | Deploy Doc2Vec Backfill

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

## 7. DevOps | Create Doc2Vec Backfill queue and DLQ

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

## 8. DevOps | Allow Doc Orchestration to send Doc2Vec Backfill messages

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

## 9. Doc Orchestration Lambda | Route Doc2Vec Backfill files

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

## 10. Doc2Vec Backfill | Load tracer data into OpenSearch and reuse saved vectors

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

## 11. Doc2Vec Backfill | Load training data by tsp_id and reuse saved Titan embeddings

### Value

Load representative sentence vectors into the OpenSearch training index. Reuse
the 512-dimension Titan vectors already saved in the `embeddings` table and
generate only the vectors that are missing.

### Acceptance Criteria

```gherkin
Given Doc2Vec Backfill receives a sqs message with reason TRAIN_MODEL with a tsp_id
When the message is processed
Then sentences_vector and embeddings are loaded for the requested tsp_id
And sentence keys are generated in memory and used to remove duplicate training sentences
And the generated keys are not saved to sentences_vector
And embeddings are loaded by global_id where embedding_type equals 2
And an existing 512-dimension embedding is sent to OpenSearch
And the Bedrock pipeline generates a vector only when that embedding is missing
And every unique sentence is indexed in sentence_semantic_training
```

```gherkin
Given a database, OpenSearch, Lasso, or Bedrock call fails temporarily
When the worker retries the batch
Then it reruns the same batch without creating duplicate training documents
And it waits 5 seconds between attempts
And the source SQS message is deleted only after every batch succeeds
```

## 12. DevOps | Train the production IVF-SQ16 model from sentence vectors

### Value

Train one production Faiss IVF-SQ16 model from representative 512-dimension
Asylee and Refugee vectors loaded into the sentence training index by Story
11. Both production catalogs use this same trained model.

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
Given sentence_semantic_training-v1 contains representative 512-dimension Asylee and Refugee vectors
When the Terraform training deployment runs
Then it submits the IVF-SQ16 training request with the configuration in this story
And Terraform exposes OPENSEARCH_IVF_MODEL_ID
And the training status can be viewed without creating either sentence catalog
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

## 13. DevOps | Update the sentence catalog for Asylee and add the Refugee catalog

### Value

Replace the planned single sentence catalog with an Asylee catalog and add a
separate Refugee catalog after the shared IVF-SQ16 model reaches the `created`
state. Both catalogs use the same trained model and Bedrock pipeline. Splitting
the catalogs keeps matching and classification inside the correct analysis
group.

### Asylee catalog mapping

```http
PUT /sentence_semantic_catalog_asylee-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": "<asylee catalog shards>",
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {"type": "keyword"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<trained IVF-SQ16 model ID>"
      },
      "isNonMatcher": {"type": "boolean"},
      "isTracer": {"type": "boolean"},
      "isFormLanguage": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

### Refugee catalog mapping

```http
PUT /sentence_semantic_catalog_refugee-ivf-sq16-v1
{
  "settings": {
    "number_of_shards": "<refugee catalog shards>",
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "<bedrock embedding pipeline>"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceKey": {"type": "keyword"},
      "sentenceContent": {"type": "text"},
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "<trained IVF-SQ16 model ID>"
      },
      "isNonMatcher": {"type": "boolean"},
      "isTracer": {"type": "boolean"},
      "isFormLanguage": {"type": "boolean"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

### Aliases

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_catalog_asylee-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog_asylee"
      }
    },
    {
      "add": {
        "index": "sentence_semantic_catalog_refugee-ivf-sq16-v1",
        "alias": "sentence_semantic_catalog_refugee"
      }
    }
  ]
}
```

### Acceptance Criteria

```gherkin
Given OPENSEARCH_IVF_MODEL_ID has state created and no training error
When the Terraform catalog deployment runs
Then the previous single sentence catalog resource is updated to sentence_semantic_catalog_asylee-ivf-sq16-v1
And the Asylee catalog uses that model ID
And it creates sentence_semantic_catalog_refugee-ivf-sq16-v1 with the same model ID
And it creates the sentence_semantic_catalog_asylee alias
And it creates the sentence_semantic_catalog_refugee alias
And both mappings contain isNonMatcher, isTracer, and isFormLanguage
And neither catalog stores analysisGroup because the selected catalog defines the group
And Terraform exposes both physical index names and both aliases for Harness
And application services no longer use the generic sentence_semantic_catalog alias
```

```gherkin
Given the existing generic sentence catalog contains data
When the two group catalogs are deployed
Then the generic catalog is not deleted during this deployment
And services switch to the group aliases only after both catalogs are populated and validated
```

Harness outputs:

```text
OPENSEARCH_ASYLEE_CATALOG_INDEX
OPENSEARCH_ASYLEE_CATALOG_ALIAS
OPENSEARCH_REFUGEE_CATALOG_INDEX
OPENSEARCH_REFUGEE_CATALOG_ALIAS
```

```gherkin
Given the IVF model is missing, training, or failed
When the catalog deployment runs
Then the deployment stops without creating either sentence catalog
```

## 14. Doc2Vec Backfill | Export historical tsp_id values into chunked CSV files

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

## 15. Doc2Vec Backfill | Process historical sentences without splitting them again

### Value

Process every sentence already stored for one historical `tsp_id`. Generate
and save sentence keys, reuse existing Titan embeddings, and populate the
occurrence index and the catalog selected by `analysis_group` without
splitting or inserting the sentences again.

Harness must provide `SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL`,
`OPENSEARCH_ASYLEE_CATALOG_ALIAS`, and
`OPENSEARCH_REFUGEE_CATALOG_ALIAS` to Doc2Vec Backfill.

### Acceptance Criteria

```gherkin
Given Doc2Vec Backfill receives an SQS message with reason HISTORICAL and a tsp_id
When the tsp_id is processed
Then the total sentence count is read from sentences_vector for that tsp_id
And its existing sentence rows are loaded in batches of 30
And the original document text is not loaded or split again
And duplicate sentence rows are not inserted
And every row keeps its existing global_id
And sentenceKey is generated for every row
And sentences_vector is updated with sentenceKey using global_id
```

```gherkin
Given a batch has generated its sentence keys
When application_id, analysis_group, section_name, and is_current are read from text_similarity_input using tsp_id
Then the one analysis_group for that tsp_id selects sentence_semantic_catalog_asylee or sentence_semantic_catalog_refugee
And all sentence keys in the batch are checked against that selected catalog
And keys that already exist in the selected group catalog are not inserted again
And missing keys are checked against embeddings using global_id and embedding_type 2
And a valid existing 512-dimension embedding is included in its catalog document
And records without a saved embedding are sent without a vector so the OpenSearch Bedrock pipeline creates it
And only missing unique sentence keys are bulk-inserted into their selected group catalog
And every sentence is bulk-inserted into sentence_occurrences
And sentenceKey is the stable catalog document ID
And globalId is the stable occurrence document ID
```

```gherkin
Given the tsp_id has historical sentences to process
When Doc2Vec Backfill creates the worker pool
Then it starts no more than 100 workers
And each worker processes one batch of 30 sentences
And one processing wave handles at most 3000 sentences
And another wave is started when more than 3000 sentences remain
And processing continues until the original sentence count is complete
```

```gherkin
Given a database, OpenSearch, Lasso, or Bedrock operation fails temporarily
When the worker retries the failed batch
Then the complete batch function is run again
And it retries up to 10 times with a 5-second wait between attempts
And stable sentenceKey and globalId values prevent duplicate OpenSearch documents
And the received SQS message is not deleted while any batch remains incomplete
```

```gherkin
Given every source globalId has an occurrence document
And every unique sentenceKey has a catalog document and vector in its analysis-group catalog
When the processed count matches the original sentence count
Then a message containing the tsp_id is sent to SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL
And the received Doc2Vec Backfill SQS message is deleted only after the new message is sent successfully
```

## 16. Doc2Vec | Add incoming document to the OpenSearch occurrence and catalog indexes

### Value

Use the existing live DOC2Vec flow to split incoming text, save sentence keys,
reuse any saved Titan embeddings, and index the new sentence occurrences.
Harness supplies `OPENSEARCH_ASYLEE_CATALOG_ALIAS` and
`OPENSEARCH_REFUGEE_CATALOG_ALIAS` to the service.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
And DOC2Vec receives a live document tsp_id
When the existing code loads and splits the document text
Then sentenceKey is generated for every sentence
And sentences are inserted into sentences_vector in batches of 100
And each successful database flush returns the globalId values
And different workers process the available batches
```

```gherkin
Given a live sentence has a globalId, sentenceKey, and other data from sentences_vector
When DOC2Vec prepares its OpenSearch documents
Then application_id, analysis_group, section_name, and is_current are read from text_similarity_input using tsp_id
And the one analysis_group for that tsp_id selects sentence_semantic_catalog_asylee or sentence_semantic_catalog_refugee
And the selected sentence catalog is checked using sentenceKey
And no vector work is done when that key already exists
And embeddings is checked by global_id and embedding_type 2 for a missing catalog key
And an existing 512-dimension Titan embedding is reused
And the Bedrock pipeline generates a vector only when the catalog key and saved embedding are both missing
And one occurrence document is written using globalId as its OpenSearch document ID
```

```gherkin
Given a database, OpenSearch, Lasso, or Bedrock operation fails temporarily
When the worker retries the batch
Then it reruns the complete batch without duplicating sentences or OpenSearch documents
And it waits 5 seconds between attempts
And the State Machine message is sent and the source SQS message is deleted only after every batch succeeds
```

## 17. DevOps | Create Sentence Similarity Backfill FIFO queue and DLQ

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

## 18. DevOps | Deploy Sentence Similarity Backfill with the existing service image

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

## 19. Java | Create the Titan match table and PostgreSQL functions

### Value

Create the smaller `tsp.titan_matches` table for Titan sentence relationships.
PostgreSQL creates missing source and target rows, saves relationships in both
directions, and maintains `match_count` and `verbatim` automatically. Rename the
existing case-similarity table for the Titan flow.

### Acceptance Criteria

```gherkin
Given the Flyway migration runs
When the migration completes
Then tsp.titan_matches contains global_id, tsp_id, application_id, match_bucket_number, matches, match_count, verbatim, and last_modified
And global_id and match_bucket_number are the composite primary key
And each matching global_id selects one of 16 stable buckets
And match_count and verbatim are stored generated columns
And indexes exist for tsp_id and application_id
And tsp.similar_cases_emb is renamed to tsp.titan_similar_cases
```

```gherkin
Given a source sentence and zero or more matching sentences
When the bidirectional PostgreSQL function runs
Then targets from the same application_id as the source are ignored
And no row is created when there are no cross-application matches
And missing source and target bucket rows are created
And source and target rows contain the reverse relationship
And source and target rows retain their correct tsp_id and application_id
And an existing bucket merges matches without losing earlier matches
And PostgreSQL recalculates match_count and verbatim without receiving either value
```

### Flyway SQL

```sql
/*
 * Pick one bucket for a matching global ID.
 *
 * One sentence can have a very large number of matches. Saving all matches in
 * one JSONB value would make updates slow. We split the matches into 16
 * smaller rows called buckets. The buckets are numbered 0 through 15.
 *
 * The matching global ID decides which bucket is used:
 *
 *   target global ID 200 -> 200 % 16 -> bucket 8
 *   target global ID 232 -> 232 % 16 -> bucket 8
 *   target global ID 233 -> 233 % 16 -> bucket 9
 *
 * The same ID always goes to the same bucket. We never have to search all 16
 * buckets to decide where a new match belongs.
 *
 * IMMUTABLE tells PostgreSQL that the answer never changes for the same ID.
 */
CREATE OR REPLACE FUNCTION tsp.titan_match_bucket(
    matching_global_id INTEGER
)
RETURNS SMALLINT
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT mod(abs(matching_global_id::BIGINT), 16)::SMALLINT;
$$;


/*
 * Count how many matches are saved in one bucket.
 *
 * Input:  {"200": 0.10, "232": 0.0}
 * Output: 2
 *
 * PostgreSQL uses this function to fill match_count. Python does not send the
 * count.
 */
CREATE OR REPLACE FUNCTION tsp.titan_match_object_size(
    match_values JSONB
)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT count(*)::INTEGER
    FROM jsonb_object_keys(match_values);
$$;


/*
 * Make a list containing only exact-match global IDs.
 *
 * Distance 0.0 means exact match. A value larger than 0.0 means similar but
 * not exact.
 *
 * Input:
 *
 *   {"200": 0.10, "232": 0.0, "248": 0.0}
 *
 * Output:
 *
 *   {232, 248}
 *
 * PostgreSQL uses this function to fill verbatim. Python does not send this
 * list. If there is no exact match, the result is an empty list.
 */
CREATE OR REPLACE FUNCTION tsp.titan_verbatim_global_ids(
    match_values JSONB
)
RETURNS INTEGER[]
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT COALESCE(
        array_agg(exact_match.global_id ORDER BY exact_match.global_id),
        ARRAY[]::INTEGER[]
    )
    FROM (
        SELECT match_key::INTEGER AS global_id
        FROM jsonb_each(match_values) AS item(match_key, distance_value)
        WHERE (distance_value #>> '{}')::DOUBLE PRECISION = 0.0
    ) AS exact_match;
$$;


/*
 * This table stores the sentence matches.
 *
 * One sentence can have up to 16 rows. Each row holds one part of its match
 * list. The sentence that owns the match list is global_id.
 *
 * Example row:
 *
 *   global_id:            100
 *   tsp_id:               10
 *   application_id:       1
 *   match_bucket_number:  8
 *   matches:              {"200": 0.10, "232": 0.0}
 *   match_count:          2             -- generated by PostgreSQL
 *   verbatim:             {232}         -- generated by PostgreSQL
 *
 * The matches value stores {matching_global_id: distance}.
 *
 * Distance 0.0 is exact. Distance 0.10 means 90% similar.
 *
 * To get every match for global ID 100, read all rows where global_id = 100
 * and combine their matches. Add their match_count values for the total.
 */
CREATE TABLE tsp.titan_matches (
    global_id INTEGER NOT NULL,
    tsp_id INTEGER NOT NULL,
    application_id INTEGER NOT NULL,
    match_bucket_number SMALLINT NOT NULL,
    matches JSONB NOT NULL DEFAULT '{}'::JSONB,

    /* PostgreSQL updates this count whenever matches changes. */
    match_count INTEGER GENERATED ALWAYS AS (
        tsp.titan_match_object_size(matches)
    ) STORED NOT NULL,

    /* PostgreSQL puts every distance 0.0 global ID in this list. */
    verbatim INTEGER[] GENERATED ALWAYS AS (
        tsp.titan_verbatim_global_ids(matches)
    ) STORED NOT NULL,

    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    /* Makes global_id and bucket searches fast and prevents duplicate rows. */
    PRIMARY KEY (global_id, match_bucket_number),

    CONSTRAINT fk_titan_matches_sentence_vector
        FOREIGN KEY (global_id)
        REFERENCES tsp.sentences_vector (global_id),

    CONSTRAINT fk_titan_matches_text_similarity_input
        FOREIGN KEY (tsp_id)
        REFERENCES tsp.text_similarity_input (tsp_id),

    CONSTRAINT ck_titan_matches_bucket_number
        CHECK (match_bucket_number BETWEEN 0 AND 15),

    CONSTRAINT ck_titan_matches_json_object
        CHECK (jsonb_typeof(matches) = 'object')
);


/*
 * Make document searches fast. This index helps these queries:
 *
 *   WHERE tsp_id = 10
 *   WHERE tsp_id = 10 AND global_id = 100
 *   WHERE tsp_id = 10 ORDER BY global_id
 *
 * Searching only by global_id uses the primary key above.
 */
CREATE INDEX ix_titan_matches_tsp_id
    ON tsp.titan_matches (tsp_id, global_id);


/*
 * Make application searches fast. This index helps these queries:
 *
 *   WHERE application_id = 1
 *   WHERE application_id = 1 AND global_id = 100
 *
 * The function also stores application_id on reverse rows, so every owner row
 * can be traced back to its application without opening the matches JSONB.
 */
CREATE INDEX ix_titan_matches_application_id
    ON tsp.titan_matches (application_id, global_id);


/*
 * Save each match in both directions.
 *
 * If sentence 100 matches sentence 200, both sentences must know about the
 * match:
 *
 *   100 -> 200
 *   200 -> 100
 *
 * This function saves both sides together. If anything fails, PostgreSQL
 * rolls back both sides.
 *
 * Example source:
 *
 *   p_source_global_id      = 100
 *   p_source_tsp_id         = 10
 *   p_source_application_id = 1
 *
 * Example p_matches:
 *
 *   {
 *     "200": {"tsp_id": 20, "application_id": 2, "distance": 0.10},
 *     "232": {"tsp_id": 30, "application_id": 3, "distance": 0.0}
 *   }
 *
 * Rows saved for the source:
 *
 *   100 -> 200 at distance 0.10
 *   100 -> 232 at distance 0.0
 *
 * Rows saved for the matching sentences:
 *
 *   200 -> 100 at distance 0.10
 *   232 -> 100 at distance 0.0
 *
 * Calling the function again does not make duplicate matches. If global ID
 * 200 already exists, its distance is updated.
 *
 * The returned number is the number of matches received for the source. The
 * reverse copies are not counted again.
 */
CREATE OR REPLACE FUNCTION tsp.upsert_bidirectional_titan_matches(
    p_source_global_id INTEGER,
    p_source_tsp_id INTEGER,
    p_source_application_id INTEGER,
    p_matches JSONB
)
RETURNS INTEGER
LANGUAGE PLPGSQL
AS $$
DECLARE
    eligible_targets JSONB;
    processed_match_count INTEGER;
    locked_row_id BIGINT;
BEGIN
    /*
     * Remove matches that should not be saved.
     *
     * A sentence cannot match itself. Sentences from the same application are
     * also removed. OpenSearch already does this, but PostgreSQL checks again
     * to protect the saved data.
     *
     * If p_matches is NULL, use an empty object instead.
     */
    SELECT COALESCE(
        jsonb_object_agg(
            target.match_key,
            jsonb_build_object(
                'tsp_id',
                (target.match_value ->> 'tsp_id')::INTEGER,
                'application_id',
                (target.match_value ->> 'application_id')::INTEGER,
                'distance',
                (target.match_value ->> 'distance')::DOUBLE PRECISION
            )
        ),
        '{}'::JSONB
    )
    INTO eligible_targets
    FROM jsonb_each(COALESCE(p_matches, '{}'::JSONB))
        AS target(match_key, match_value)
    WHERE target.match_key::INTEGER <> p_source_global_id
      AND (target.match_value ->> 'application_id')::INTEGER
          <> p_source_application_id;

    /* Count only the matches that are allowed to be saved. */
    processed_match_count := tsp.titan_match_object_size(eligible_targets);

    /*
     * No relationship means there is nothing to save.
     *
     * Example: if global ID 100 has no eligible match, return 0 without
     * creating an empty bucket row. Rows exist only for real bidirectional
     * relationships.
     */
    IF processed_match_count = 0 THEN
        RETURN 0;
    END IF;

    /*
     * Protect the bucket rows while they are being updated.
     *
     * For 100 -> 200:
     *
     *   forward lock: owner 100, bucket 200 % 16 = 8
     *   reverse lock: owner 200, bucket 100 % 16 = 4
     *
     * Two workers may update the same bucket at the same time. The lock makes
     * one worker wait until the other worker finishes. This prevents one
     * worker from removing the changes made by the other worker.
     *
     * The ORDER BY makes every worker lock rows in the same order. This helps
     * prevent deadlocks. Reading is still allowed. PostgreSQL releases these
     * locks when the transaction finishes or fails.
     */
    FOR locked_row_id IN
        SELECT DISTINCT
            hashtextextended(
                concat_ws(
                    ':',
                    'titan_matches',
                    affected.owner_global_id,
                    affected.bucket_number
                ),
                0
            ) AS lock_id
        FROM (
            SELECT
                p_source_global_id AS owner_global_id,
                tsp.titan_match_bucket(target.match_key::INTEGER)
                    AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)

            UNION

            SELECT
                target.match_key::INTEGER AS owner_global_id,
                tsp.titan_match_bucket(p_source_global_id) AS bucket_number
            FROM jsonb_each(eligible_targets)
                AS target(match_key, match_value)
        ) AS affected
        ORDER BY lock_id
    LOOP
        PERFORM pg_advisory_xact_lock(locked_row_id);
    END LOOP;

    /*
     * Save the matches on the source sentence.
     *
     * Targets 200 and 232 both select bucket 8, so source 100 receives:
     *
     *   global_id: 100, bucket: 8, matches: {"200": 0.10, "232": 0.0}
     *
     * If bucket 8 does not exist, INSERT creates it. If it already exists,
     * ON CONFLICT updates it. The || operator combines the old and new JSONB.
     * A new global ID is added. An existing global ID gets the new distance.
     */
    INSERT INTO tsp.titan_matches (
        global_id,
        tsp_id,
        application_id,
        match_bucket_number,
        matches
    )
    SELECT
        p_source_global_id,
        p_source_tsp_id,
        p_source_application_id,
        tsp.titan_match_bucket(target.match_key::INTEGER),
        jsonb_object_agg(
            target.match_key,
            target.match_value -> 'distance'
        )
    FROM jsonb_each(eligible_targets) AS target(match_key, match_value)
    GROUP BY tsp.titan_match_bucket(target.match_key::INTEGER)
    ORDER BY tsp.titan_match_bucket(target.match_key::INTEGER)
    ON CONFLICT (global_id, match_bucket_number)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /*
     * Save the source ID on every matching sentence.
     *
     * Source 100 selects reverse bucket 4 because 100 % 16 = 4:
     *
     *   owner 200, bucket 4, matches: {"100": 0.10}
     *   owner 232, bucket 4, matches: {"100": 0.0}
     *
     * This is the reverse side of the relationship. The target row is created
     * when needed. If it already exists, the new source ID is merged into its
     * saved matches.
     */
    INSERT INTO tsp.titan_matches (
        global_id,
        tsp_id,
        application_id,
        match_bucket_number,
        matches
    )
    SELECT
        target.match_key::INTEGER,
        (target.match_value ->> 'tsp_id')::INTEGER,
        (target.match_value ->> 'application_id')::INTEGER,
        tsp.titan_match_bucket(p_source_global_id),
        jsonb_build_object(
            p_source_global_id::TEXT,
            (target.match_value ->> 'distance')::DOUBLE PRECISION
        )
    FROM jsonb_each(eligible_targets) AS target(match_key, match_value)
    ORDER BY target.match_key::INTEGER
    ON CONFLICT (global_id, match_bucket_number)
    DO UPDATE SET
        tsp_id = EXCLUDED.tsp_id,
        application_id = EXCLUDED.application_id,
        matches = titan_matches.matches || EXCLUDED.matches,
        last_modified = NOW();

    /* Return the number of allowed source matches handled by this call. */
    RETURN processed_match_count;
END;
$$;


COMMENT ON FUNCTION tsp.upsert_bidirectional_titan_matches(
    INTEGER,
    INTEGER,
    INTEGER,
    JSONB
)
IS
'Saves cross-application Titan matches in both directions using 16 buckets.';
```

## 20. Sentence Similarity | Classify tsp_id sentences with tracer workers

### Value

Load the sentences for one `tsp_id`, classify them against the tracer and
form-language catalog, and save the classification in the selected group
catalog and PostgreSQL before normal similarity matching begins.

### Acceptance Criteria

```gherkin
Given Sentence Similarity receives an SQS message
When the message is processed
Then tsp_id is read from the message payload
And sentences_vector is queried for the sentences belonging to that tsp_id
And the sentences are loaded in batches of 30
And a thread pool with 30 workers processes the batch
And each worker processes one sentence
```

```gherkin
Given a worker receives one sentence
When analysisGroup selects the Asylee or Refugee sentence catalog
Then the worker first reads any saved isNonMatcher, isTracer, and isFormLanguage values from that group catalog
And when those values are missing it checks the sentence vector against tracer_form_language_catalog for the same analysisGroup
And the tracer catalog returns isNonMatcher and isFormLanguage but does not contain isTracer
And isTracer is true only when a tracer result is found and both returned flags are false
And it saves the completed classification on the selected group catalog document
And the sentence row in sentences_vector is updated using global_id with the flags, matching tracerId, and decimal similarity score
And any sentence found in the tracer catalog is not sent to normal sentence similarity matching
```

```gherkin
Given a group-catalog read or update, tracer lookup, or sentences_vector update fails temporarily
When the worker retries the sentence
Then the same sentence is processed again without creating a duplicate
And the group catalog and sentences_vector row contain the same three classification flags after the retry succeeds
And sentences_vector also contains the matching tracerId and decimal similarity score returned by the tracer search
```

## 21. Sentence Similarity | Search eligible sentences and save Titan matches (Assign this to Anish, I already have the code for this.)

### Value

Continue from the previous story by searching only eligible sentences, saving
their Titan matches, and completing the SQS workflow.
Harness supplies `OPENSEARCH_ASYLEE_CATALOG_ALIAS` and
`OPENSEARCH_REFUGEE_CATALOG_ALIAS` to both real-time and backfill deployments.

### Acceptance Criteria

```gherkin
Given the previous story has updated the sentences for a tsp_id
When an eligible sentence requires matching
Then analysis_group selects sentence_semantic_catalog_asylee or sentence_semantic_catalog_refugee
And its saved vector is used to search only that selected catalog
And exact and similar sentence results are collected
And same-application, tracer, form-language, and other non-matcher results are excluded
And the accepted occurrence matches are written to titan_matches using the bidirectional update logic
And the source row contains the target occurrence
And the target row contains the source occurrence with the same distance
And an existing relationship is updated instead of duplicated
```

```gherkin
Given the tsp_id contains multiple eligible sentences
When Sentence Similarity processes them
Then the similarity search and titan_matches update are completed for every eligible sentence
And a temporary failure retries the affected sentence
And the received SQS message remains available while any sentence is incomplete
```

```gherkin
Given every sentence for the tsp_id has completed tracer classification and any required similarity matching
When the full tsp_id succeeds
Then one SQS message containing the tsp_id is sent to the configured next-step queue
And the received Sentence Similarity SQS message is deleted only after the next-step message is sent successfully
```

## 22. API Transform | Keep the existing transformation for Titan matches

### Value

Use the existing API Transform functionality with the occurrence-level Titan
matches. Do not redesign the summary or response format.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
And Sentence Similarity has completed a tsp_id
When API Transform processes the document
Then it reads the document match information from titan_matches
And it uses the existing summary and response-building logic
And the existing output format remains unchanged
```

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is false or missing
When API Transform processes the document
Then it continues using the current match source and behavior without changes
```

## 23. Case Similarity | Save Titan case results separately for comparison

### Value

Keep the existing Case Similarity calculation and save the Titan result in a
separate table so it can be compared with the previous result.

### Acceptance Criteria

```gherkin
Given ENABLE_OPENSEARCH_SENTENCE_FLOW is true
And titan_matches contains the completed occurrence matches for a tsp_id
When Case Similarity processes the tsp_id
Then it uses the existing case-similarity calculation
And it writes the Titan result to tsp.titan_similar_cases
And it does not replace or update the existing Case Similarity result
```

```gherkin
Given both previous and Titan Case Similarity results exist
When the results are reviewed
Then they can be compared using application_id, a_number, and analysis_group
And the Titan comparison write does not change the existing Kafka or downstream behavior
```

## 24. Doc2Vec | Ingest historical and live sentences into OpenSearch (This is already assign to Anish, do not add to the Jira again.)

### Value

Use one shared ingestion function to write prepared historical and live
sentences to the occurrence index and the catalog selected by analysis group.

### Acceptance Criteria

```gherkin
Given historical or live rows contain globalId, sentenceKey, and sentence content
When the shared ingestion runs
Then workers process batches of 30 with no more than 100 workers
And document values are read from text_similarity_input using tsp_id
And the document's one analysis_group selects sentence_semantic_catalog_asylee or sentence_semantic_catalog_refugee for every sentence in that tsp_id
And unique sentence keys are checked in the selected group catalog
And missing keys reuse embeddings with embedding_type 2 when available
And OpenSearch creates only the remaining missing vectors
And only missing unique keys are bulk-written to the selected group catalog
And every occurrence is bulk-written to sentence_occurrences
And stable sentenceKey and globalId document IDs prevent duplicates
And another worker wave starts when more than 3000 sentences exist
```

```gherkin
Given a database, OpenSearch, Lasso, or Bedrock operation fails temporarily
When the failed batch is retried
Then the complete batch runs again up to 10 times
And each retry waits 5 seconds
And when all batches succeed the historical flow sends tsp_id to SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL
And the live flow sends its existing State Machine message
And each flow deletes its received SQS message only after its outgoing message is sent successfully
```
