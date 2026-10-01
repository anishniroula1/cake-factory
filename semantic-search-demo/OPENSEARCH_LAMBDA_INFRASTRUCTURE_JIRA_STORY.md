# Entity Semantic Search Infrastructure Jira Story

## 1. DevOps | Provision entity semantic-search ML resources and SQ16 indexes

### Value

Create the OpenSearch resources required by the NER OpenSearch Lambda. The
occurrence index stores every entity occurrence. The catalog index stores one
512-dimension Titan vector for each unique entity text. Faiss HNSW with the
SQ16 encoder reduces vector memory while keeping the existing cosine semantic
search flow.

### Acceptance Criteria

```gherkin
Given the target environment has an OpenSearch domain and approved Lasso access to Amazon Bedrock
When the infrastructure is deployed
Then a restricted ML Commons model group is created
And a Lasso connector for Amazon Titan Text Embeddings V2 is created
And a 512-dimension normalized float embedding model is registered and deployed
And a text_embedding ingest pipeline maps entitySearchText to entitySearchTextVector
And no model, pipeline, or mapping uses 1024 dimensions
```

```gherkin
Given the entity semantic-search indexes are deployed
When their mappings and aliases are inspected
Then ner_entity_occurrences-v1 exists with alias ner_entity_occurrences
And ner_entity_semantic_catalog-v1 exists with alias ner_entity_semantic_catalog
And the aliases identify their indexes as write indexes
And all entity business ID fields in the occurrence document use the numeric long type
And every occurrence contains semanticKey
And the occurrence index does not store a vector
And the catalog stores one document per unique semanticKey
And entitySearchTextVector has exactly 512 dimensions
And entitySearchTextVector uses Faiss HNSW with cosine similarity and the 16-bit SQ encoder
```

```gherkin
Given Terraform finishes successfully
When its outputs are viewed and supplied to the NER OpenSearch Lambda deployment through Harness
Then the model group ID, connector ID, model ID, pipeline name, index names, aliases, and OpenSearch role ARN are available
And connector credentials are not exposed as Terraform outputs
And the NER OpenSearch Lambda role is authorized to write through both aliases and use the deployed embedding model
```

```gherkin
Given two occurrences contain the same normalized entity text
When the NER OpenSearch Lambda creates their semanticKey and indexes them
Then both occurrence documents contain the same semanticKey
And both occurrence documents remain available in ner_entity_occurrences
And only one catalog document exists for that semanticKey
And the catalog document contains a generated 512-value vector
```

### Required Terraform outputs

```text
ENTITY_SEMANTIC_MODEL_GROUP_ID
ENTITY_SEMANTIC_CONNECTOR_ID
ENTITY_SEMANTIC_MODEL_ID
ENTITY_SEMANTIC_INGEST_PIPELINE
ENTITY_OCCURRENCE_INDEX
ENTITY_OCCURRENCE_ALIAS
ENTITY_SEMANTIC_CATALOG_INDEX
ENTITY_SEMANTIC_CATALOG_ALIAS
ENTITY_OPENSEARCH_ROLE_ARN
```

### ML resource configuration

| Resource | Required configuration |
| --- | --- |
| Model group | Restricted ML Commons model group for the entity semantic flow |
| Connector | Approved Lasso endpoint calling `amazon.titan-embed-text-v2:0` |
| Titan request | `dimensions: 512`, `normalize: true`, and float embeddings |
| OpenSearch model | Remote text-embedding model with `embedding_dimension: 512` and cosine similarity |
| Deployment | Model reaches `DEPLOYED` before the pipeline or NER OpenSearch Lambda is used |
| Pipeline | `entitySearchText` is embedded into `entitySearchTextVector` |

The connector secret must come from the approved secret store. Terraform and
Harness logs must not print it.

### Embedding pipeline

```json
PUT /_ingest/pipeline/<BEDROCK_EMBEDDING_PIPELINE>
{
  "description": "Create 512-dimension Titan embeddings for unique entity text",
  "processors": [
    {
      "text_embedding": {
        "model_id": "<DEPLOYED_TITAN_MODEL_ID>",
        "field_map": {
          "entitySearchText": "entitySearchTextVector"
        }
      }
    }
  ]
}
```

### Entity occurrence index

All ID values in `_source` are numeric. `semanticKey` is intentionally a
keyword because it is the SHA-256 key connecting an occurrence to its unique
catalog document. The NER OpenSearch Lambda creates it from the normalized
`entitySearchText`, so the same normalized entity always receives the same
key.

```json
PUT /ner_entity_occurrences-v1
{
  "settings": {
    "number_of_shards": "<INDEX_SHARDS>",
    "number_of_replicas": "<INDEX_REPLICAS>",
    "analysis": {
      "normalizer": {
        "lowercase_ascii": {
          "type": "custom",
          "filter": ["lowercase", "asciifolding"]
        }
      }
    }
  },
  "aliases": {
    "ner_entity_occurrences": {
      "is_write_index": true
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceEntityId": {"type": "long"},
      "applicationId": {"type": "long"},
      "tspId": {"type": "long"},
      "globalId": {"type": "long"},
      "entityId": {"type": "long"},
      "semanticKey": {"type": "keyword"},
      "rawEntity": {"type": "text"},
      "normalizedText": {
        "type": "keyword",
        "normalizer": "lowercase_ascii"
      },
      "entitySearchText": {"type": "text"},
      "entityType": {"type": "keyword"},
      "possibleSanction": {"type": "boolean"},
      "beginOffset": {"type": "integer"},
      "endOffset": {"type": "integer"},
      "score": {"type": "float"},
      "source": {"type": "keyword"},
      "documentType": {"type": "keyword"},
      "createdAt": {"type": "date"},
      "updatedAt": {"type": "date"}
    }
  }
}
```

### Unique entity semantic catalog

The NER OpenSearch Lambda uses `semanticKey` as the catalog document `_id` and
uses a create operation for new catalog records. If two NER OpenSearch Lambda
runs try to create the same key, one document is created and the duplicate
conflict is treated as already existing. The ingest pipeline generates the
vector from `entitySearchText`.

```json
PUT /ner_entity_semantic_catalog-v1
{
  "settings": {
    "index.knn": true,
    "index.default_pipeline": "<BEDROCK_EMBEDDING_PIPELINE>",
    "number_of_shards": "<INDEX_SHARDS>",
    "number_of_replicas": "<INDEX_REPLICAS>"
  },
  "aliases": {
    "ner_entity_semantic_catalog": {
      "is_write_index": true
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "semanticKey": {"type": "keyword"},
      "entitySearchText": {"type": "text"},
      "entitySearchTextVector": {
        "type": "knn_vector",
        "dimension": 512,
        "method": {
          "name": "hnsw",
          "engine": "faiss",
          "space_type": "cosinesimil",
          "parameters": {
            "encoder": {
              "name": "sq",
              "parameters": {
                "bits": 16
              }
            }
          }
        }
      }
    }
  }
}
```

SQ16 accepts normal 32-bit float embeddings from Titan and stores their vector
index representation as 16-bit values. It does not require a trained model or
a separate training index. OpenSearch documents this as the Faiss `sq` encoder
with `bits: 16`.

Reference: https://docs.opensearch.org/latest/vector-search/optimizing-storage/faiss-16-bit-quantization/

## 2. NER OpenSearch Lambda | Insert entity occurrences and unique catalog vectors

### Value

Process every new sentence-entity event from the database trigger and save the
entity in OpenSearch. Reuse an existing catalog vector when the normalized
entity text already exists, and retry temporary vector-generation failures so
an entity is not lost because Lasso, Titan, or OpenSearch briefly failed.

### Acceptance Criteria

```gherkin
Given the NER OpenSearch Lambda receives a new sentence-entity event from the database trigger
When the event is processed
Then the Lambda reads the numeric sentenceEntityId, applicationId, tspId, globalId, and entityId
And it creates semanticKey from the normalized entitySearchText
And it uses semanticKey as the catalog document ID
And it checks whether that catalog document already exists
And an existing catalog document and vector are reused without calling Titan again
```

```gherkin
Given the semanticKey does not exist in the catalog
When the Lambda creates the catalog document
Then entitySearchText is sent through the configured text_embedding pipeline
And OpenSearch creates a 512-value Titan vector
And the catalog document contains semanticKey, entitySearchText, and entitySearchTextVector
And the catalog document does not contain normalizedText
And the catalog document is saved before the occurrence document
And the occurrence document is written through the ner_entity_occurrences alias with semanticKey
```

```gherkin
Given catalog vector generation or an OpenSearch write fails with a temporary error
When the status is 408, 424, 429, 500, 502, 503, or 504, or a connection timeout occurs
Then the failed operation is retried up to 10 total attempts
And each failed attempt waits 5 seconds before the next attempt
And a successful retry resets the retry count for the next entity event
And the log shows the entity ID, semanticKey, attempt number, status code, and short error reason
And the log does not print the embedding vector or the full event payload
```

```gherkin
Given two Lambda executions try to create the same semanticKey
When one catalog create succeeds and the other receives a 409 conflict
Then the 409 is treated as an existing catalog document
And only one catalog vector is stored
And both occurrence documents are still saved
```

```gherkin
Given a permanent mapping, authorization, model, pipeline, or vector-dimension error occurs
When the Lambda cannot complete both required writes
Then the permanent error is not repeatedly sent to Titan
And the Lambda invocation fails so the configured database-trigger retry or DLQ behavior can handle the event
And the event is reported as successful only after the catalog dependency and occurrence write are complete
```

### Processing order

```text
Database trigger event
  -> Validate numeric IDs and required entity fields
  -> Normalize entitySearchText and create semanticKey
  -> Find catalog document by semanticKey
  -> Existing key: reuse its vector
  -> Missing key: create catalog document through the embedding pipeline
  -> Retry temporary catalog/vector errors up to 10 total attempts
  -> Upsert occurrence using sentenceEntityId as the stable document ID
  -> Return success only after both records are available
```

Using stable document IDs makes the operation safe to repeat:

- Catalog `_id`: `semanticKey`
- Occurrence `_id`: numeric `sentenceEntityId` converted to the OpenSearch
  document ID value
