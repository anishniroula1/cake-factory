# Create and compare an IVF-PQ sentence catalog

This guide creates a new IVF-PQ catalog without moving the working SQ alias.
Both catalogs stay available, so the same vector can be searched against each
one.

The names used in this guide are:

| Purpose | Name |
|---|---|
| Existing working SQ alias | sentence_semantic_catalog |
| New PQ physical index | sentence_semantic_catalog-ivf-pq64-v1 |
| New PQ alias | sentence_semantic_catalog_ivf_pq |
| New PQ model | sentence-ivf-pq64-512-v1 |
| Vector field | sentenceContentVector |
| Vector dimension | 512 |

The important part is that sentence_semantic_catalog and
sentence_semantic_catalog_ivf_pq are different aliases. Creating PQ does not
move, rename, or delete the SQ alias.

## 1. Pause catalog seeding

Pause seeding while the catalog is copied. A sentence added after the copy
starts could otherwise exist in SQ but not in PQ.

The occurrence index does not need to be copied. This change only creates a
second semantic catalog.

## 2. Find the physical SQ index

Run:

```http
GET /_alias/sentence_semantic_catalog
```

Example result:

```json
{
  "sentence_semantic_catalog-ivf-sq16-v1": {
    "aliases": {
      "sentence_semantic_catalog": {}
    }
  }
}
```

In this example, sentence_semantic_catalog-ivf-sq16-v1 is the physical source
index. Use the physical name returned by your cluster in the steps below.

Do not guess this name and do not use the SQ alias as the reindex destination.

## 3. Check the number of training vectors

```http
GET /sentence_semantic_catalog-ivf-sq16-v1/_count
```

OpenSearch recommends this IVF-PQ training count:

```text
max(1,000 x nlist, 1,000 x 2^code_size)
```

For nlist 4096 and code_size 8:

```text
1,000 x 4,096 = 4,096,000
1,000 x 256   =   256,000

Recommended training count = 4,096,000 vectors
```

A smaller set can be used for this comparison, but it is below the recommended
amount for the final production model. If the current catalog contains about
905,000 vectors, use 900000 in max_training_vector_count for this test.

## 4. Train the IVF-PQ model

This setup uses:

- 512-dimension Titan vectors.
- 4,096 IVF buckets.
- 64 buckets searched by default.
- 64 PQ parts per vector.
- 8 bits per PQ part.
- Cosine similarity.

Run:

```http
POST /_plugins/_knn/models/sentence-ivf-pq64-512-v1/_train
{
  "training_index": "sentence_semantic_catalog-ivf-sq16-v1",
  "training_field": "sentenceContentVector",
  "dimension": 512,
  "max_training_vector_count": 900000,
  "search_size": 10000,
  "description": "Faiss IVF-PQ64 model for Titan 512 sentence vectors",
  "space_type": "cosinesimil",
  "method": {
    "name": "ivf",
    "engine": "faiss",
    "parameters": {
      "nlist": 4096,
      "nprobes": 64,
      "encoder": {
        "name": "pq",
        "parameters": {
          "code_size": 8,
          "m": 64
        }
      }
    }
  }
}
```

512 must divide evenly by m. Here, 512 divided by 64 is 8, so the setting is
valid.

This is an OpenSearch k-NN model. It is not an ML Commons model, so there is no
deploy or undeploy step.

## 5. Wait for model training

```http
GET /_plugins/_knn/models/sentence-ivf-pq64-512-v1?filter_path=model_id,state,error,dimension,space_type,method
```

Wait for:

```text
state: created
dimension: 512
space_type: cosinesimil
error: empty
```

Do not create the PQ index until the model state is created.

## 6. Confirm the existing vectors can be copied

The reindex API copies fields stored in _source. Check one source document:

```http
GET /sentence_semantic_catalog-ivf-sq16-v1/_search
{
  "size": 1,
  "_source": [
    "sentenceKey",
    "sentenceContent",
    "sentenceContentVector"
  ],
  "query": {
    "exists": {
      "field": "sentenceContentVector"
    }
  }
}
```

Continue when sentenceContentVector is present in _source. If it is missing,
normal reindexing cannot copy the vector and the catalog must be seeded through
Titan again.

## 7. Configure this project for the PQ catalog

Put these values in .env:

```text
OPENSEARCH_IVF_MODEL_ID=sentence-ivf-pq64-512-v1
OPENSEARCH_IVF_NPROBES=64
OPENSEARCH_CATALOG_INDEX=sentence_semantic_catalog-ivf-pq64-v1
OPENSEARCH_CATALOG_ALIAS=sentence_semantic_catalog_ivf_pq
```

The project defaults already use the PQ index and its separate alias. The
existing sentence_semantic_catalog alias is not changed.

## 8. Create the empty PQ catalog

Use the API:

```http
POST /admin/init
```

Or create it directly:

```http
PUT /sentence_semantic_catalog-ivf-pq64-v1
{
  "settings": {
    "index.knn": true,
    "index.default_pipeline": "sentence_bedrock_embedding_pipeline",
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "-1"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "sentenceContent": {
        "type": "text"
      },
      "sentenceContentVector": {
        "type": "knn_vector",
        "model_id": "sentence-ivf-pq64-512-v1"
      },
      "sentenceKey": {
        "type": "keyword"
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

Using POST /admin/init creates the configured replicas immediately. The direct
request above temporarily uses no replica and disables refresh, which can make
the copy faster. Both options create the same vector mapping.

If the index was created directly, add only the new PQ alias:

```http
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sentence_semantic_catalog-ivf-pq64-v1",
        "alias": "sentence_semantic_catalog_ivf_pq",
        "is_write_index": true
      }
    }
  ]
}
```

There is no remove action. The SQ alias stays where it is.

## 9. Copy vectors without calling Titan

Run the copy against physical index names:

```http
POST /_reindex?wait_for_completion=false&slices=3
{
  "source": {
    "index": "sentence_semantic_catalog-ivf-sq16-v1",
    "size": 1000
  },
  "dest": {
    "index": "sentence_semantic_catalog-ivf-pq64-v1",
    "op_type": "create",
    "pipeline": "_none"
  }
}
```

pipeline _none is important. It copies the saved vector instead of calling
Titan and paying to create the same vector again.

The response contains a task ID:

```json
{
  "task": "node-id:123456"
}
```

## 10. Watch the reindex task

```http
GET /_tasks/node-id:123456?detailed=true
```

Wait until completed is true. Check that:

- failures is empty.
- version_conflicts is zero.
- created is the expected document count.

Three slices match a source catalog with three primary shards. More slices can
add pressure without making this copy faster.

## 11. Restore normal settings

This is only needed when the PQ index was created directly with refresh
disabled and no replicas:

```http
POST /sentence_semantic_catalog-ivf-pq64-v1/_refresh
```

```http
PUT /sentence_semantic_catalog-ivf-pq64-v1/_settings
{
  "index": {
    "refresh_interval": "1s",
    "number_of_replicas": 1
  }
}
```

Wait for the PQ index to become healthy:

```http
GET /_cluster/health/sentence_semantic_catalog-ivf-pq64-v1?wait_for_status=green&timeout=10m
```

## 12. Validate both catalogs

Compare counts through the two aliases:

```http
GET /sentence_semantic_catalog/_count
GET /sentence_semantic_catalog_ivf_pq/_count
```

The counts should match.

Check for missing PQ vectors:

```http
GET /sentence_semantic_catalog_ivf_pq/_count
{
  "query": {
    "bool": {
      "must_not": {
        "exists": {
          "field": "sentenceContentVector"
        }
      }
    }
  }
}
```

Expected count:

```text
0
```

Check storage:

```http
GET /_cat/indices/sentence_semantic_catalog*?v&bytes=mb&h=index,docs.count,pri.store.size,store.size
```

Compare pri.store.size for primary storage. store.size includes replicas.

## 13. Warm the PQ index

```http
GET /_plugins/_knn/warmup/sentence_semantic_catalog-ivf-pq64-v1?pretty
```

Then check k-NN memory and graph information:

```http
GET /_plugins/_knn/stats
```

## 14. Compare the same search against SQ and PQ

Use the same complete 512-number vector in both requests.

SQ request:

```http
GET /sentence_semantic_catalog/_search
{
  "size": 100,
  "_source": ["sentenceKey", "sentenceContent"],
  "query": {
    "knn": {
      "sentenceContentVector": {
        "vector": [<all 512 numbers>],
        "min_score": 0.95,
        "method_parameters": {
          "nprobes": 64
        }
      }
    }
  }
}
```

PQ request:

```http
GET /sentence_semantic_catalog_ivf_pq/_search
{
  "size": 100,
  "_source": ["sentenceKey", "sentenceContent"],
  "query": {
    "knn": {
      "sentenceContentVector": {
        "vector": [<the same 512 numbers>],
        "min_score": 0.95,
        "method_parameters": {
          "nprobes": 64
        }
      }
    }
  }
}
```

In this application, a 90 percent cosine threshold becomes min_score 0.95.

Compare:

- took time.
- Total matching hits.
- Missing matches.
- Extra matches.
- Match scores near the threshold.
- Primary storage.
- CPU, JVM heap, and k-NN native memory.

PQ is lossy. A result close to the threshold can move slightly above or below
the threshold. The match count is therefore more important than checking only
the first few results.

## 15. Use the API with PQ

With OPENSEARCH_CATALOG_ALIAS set to sentence_semantic_catalog_ivf_pq, all API
search, summary, preview, seed, and cleanup operations use PQ. The health
endpoint shows the active physical index, alias, model, and nprobes setting.

To run the application against SQ again, configure its SQ model, physical
index, and alias and restart it. The two aliases remain separate in
OpenSearch, so no alias switch is required.

Do not seed both catalogs independently at the same time unless both must stay
fully current. Reindexing is safer for a controlled comparison because SQ and
PQ start with the same document set and saved Titan vectors.

## 16. Tune PQ when matches are missing

First, increase nprobes:

```text
64
128
256
```

nprobes is a search setting and does not require model training.

If recall is still too low, train another model with m 128:

```json
"encoder": {
  "name": "pq",
  "parameters": {
    "code_size": 8,
    "m": 128
  }
}
```

m 128 stores more vector information and normally improves recall, but uses
more storage than m 64. Changing m, code_size, or nlist requires a new model
and a new index.

## 17. Remove PQ if the test fails

The SQ alias is still working, so no rollback alias request is needed. Stop the
API instance using PQ, then run:

```http
POST /_plugins/_knn/clear_cache/sentence_semantic_catalog-ivf-pq64-v1
```

```http
DELETE /sentence_semantic_catalog-ivf-pq64-v1
```

After no index uses the model:

```http
DELETE /_plugins/_knn/models/sentence-ivf-pq64-512-v1
```

These delete requests remove only the PQ experiment. They do not remove the SQ
catalog or its sentence_semantic_catalog alias.

## Official OpenSearch references

- [Faiss product quantization](https://docs.opensearch.org/latest/vector-search/optimizing-storage/faiss-product-quantization/)
- [k-NN Train API](https://docs.opensearch.org/latest/vector-search/api/knn/)
- [Supported k-NN methods and engines](https://docs.opensearch.org/latest/mappings/supported-field-types/knn-methods-engines/)
- [Reindex API](https://docs.opensearch.org/latest/api-reference/document-apis/reindex/)
- [Bulk API pipeline options](https://docs.opensearch.org/latest/api-reference/document-apis/bulk/)
