# Final IVF-SQ16 plan for 100 million sentence vectors

Last updated: September 21, 2026

This report is for commercial AWS US East (N. Virginia). It is not a GovCloud
price estimate.

This is the final production choice for sentence search:

- Amazon OpenSearch Service 3.7.
- Amazon Titan Text Embeddings V2.
- 512 numbers in every vector.
- Faiss IVF with 16-bit scalar quantization, called IVF-SQ16.
- Cosine similarity.
- Radial `min_score` search, not a fixed top-K search.
- One vector per unique `sentenceKey` in the catalog.
- Sentence occurrences stay in a separate, normal OpenSearch index.

The current load is about 71 million unique vectors. This plan leaves room for
100 million unique vectors without changing the production cluster again.

## Final answer

Use this production OpenSearch domain:

| Item | Final value | Why |
|---|---:|---|
| AWS Region | us-east-1 | This cost report uses commercial US East (N. Virginia) prices. |
| Availability Zones | 3 | A node failure or one AZ failure is less likely to stop the service. |
| Multi-AZ with Standby | No | All six data nodes do useful work. One replica still keeps two copies. |
| Data nodes | 6 × r7g.4xlarge.search | Gives the native memory and CPU needed by 100 million SQ16 vectors. |
| Dedicated cluster managers | 3 × m7g.large.search | Keeps cluster management away from search work. |
| Dedicated coordinators | 3 × m7g.2xlarge.search | Handles the API's multi-search fan-out and result merging. |
| EBS per data node | 750 GiB gp3 | Gives 4.5 TiB total disk and room for reindexing. |
| IOPS per data node | 8,000 | Helps large seeding, segment merging, and reindexing. |
| Throughput per data node | 500 MiB/s | Helps large seeding and reindexing. |
| Catalog primary shards | 18 | Keeps the projected catalog shards near 20 GiB at 100 million vectors. |
| Catalog replicas | 1 | Keeps two copies and gives extra read capacity. |
| Occurrence primary shards | 18 to start | It distributes evenly over six nodes. Verify its real shard size after loading. |
| Occurrence replicas | 1 | Keeps two copies. |
| Vector engine | Faiss IVF-SQ16 | About half the vector memory of 32-bit IVF Flat, with small precision loss. |
| Vector dimension | 512 | Matches the selected Titan output. |
| Similarity | cosinesimil | Matches the application's percentage calculation. |
| nlist | 16,384 | Splits 100 million vectors into reasonably small IVF buckets. |
| Starting nprobes | 128 | Good first balance between speed and recall. Test 64, 128, and 256. |
| Training vectors | 4,194,304 | This is 256 examples for every IVF bucket. |
| Normal search page | 50 source sentences | Keeps one API request small enough for the 3–5 second target. |

This is the recommended production starting point. It is not a promise that
every request will finish in three seconds. The real result depends on query
concurrency, how many matches cross the threshold, how evenly data lands in the
IVF buckets, and whether the vector index is warm. Run the load test described
later before production approval.

## Why six r7g.4xlarge data nodes fit

An r7g.4xlarge.search node has 128 GiB RAM. OpenSearch uses up to about 32 GiB
for JVM heap, leaving about 96 GiB outside the heap. The default k-NN memory
limit is 50% of that remaining memory, or about 48 GiB per node.

Six nodes therefore provide about 288 GiB of k-NN cache.

OpenSearch gives this IVF-SQ16 memory estimate:

```text
1.1 × (((2 bytes × dimension) × vector count)
       + (4 bytes × nlist × dimension))
```

With 512 dimensions, nlist 16,384, and one replica:

| Unique vectors | Primary vector memory | Primary + replica | Average per data node |
|---:|---:|---:|---:|
| 71 million | 74.5 GiB | 149.0 GiB | 24.8 GiB |
| 100 million | 104.9 GiB | 209.9 GiB | 35.0 GiB |

At 100 million vectors, the estimated 35.0 GiB per node uses about 73% of the
default 48 GiB k-NN cache. That leaves about 13 GiB of k-NN cache per node for
normal variation and index overhead.

This calculation assumes only the production SQ16 catalog is warm. Do not warm
the old IVF Flat, HNSW, PQ, or test catalogs on the same six-node domain. If two
complete vector catalogs must stay warm at the same time, temporarily use nine
data nodes or expect cache eviction and slower searches.

Do not use r7g.2xlarge for this plan. Its default k-NN cache is only about 16
GiB per node, so six nodes would provide only about 96 GiB. That does not hold
the projected primary and replica SQ16 vectors.

## Disk plan

The best disk estimate comes from the real SQ16 test already completed:

```text
905,206 documents used about 3,476 MB of primary storage.
```

Using that measured rate:

| Unique vectors | Catalog primary disk | Catalog with one replica |
|---:|---:|---:|
| 71 million | About 254 GiB | About 508 GiB |
| 100 million | About 358 GiB | About 716 GiB |

The six data nodes provide 4,500 GiB of raw EBS storage. Keep disk use below
70%, leaving about 3,150 GiB as the normal working limit. At 100 million unique
vectors, one production catalog with its replica uses about 716 GiB. This leaves
room for the occurrence index, segment merging, and one temporary catalog copy
during a controlled reindex.

The occurrence index does not have vectors, but its final size depends on the
number and length of occurrence records. Check it after the 71-million load. If
the largest occurrence primary shard is above 30 GiB, create its next version
with 24 primary shards. Primary shard count cannot be changed in place.

## Shard plan

Use 18 primary shards and one replica for the catalog:

```json
{
  "number_of_shards": 18,
  "number_of_replicas": 1,
  "index.knn": true
}
```

Why 18:

- It divides evenly across six data nodes.
- At 71 million vectors, the measured catalog size is about 14 GiB per primary
  shard.
- At 100 million vectors, it is about 20 GiB per primary shard.
- AWS recommends roughly 10–30 GiB per shard for search-focused indexes.
- With the occurrence index also using 18 primaries and one replica, the domain
  has 72 shard copies across six nodes, or about 12 copies per node. This is a
  small shard count for these nodes.

Do not increase shards just because the document count is large. Too many small
shards make every multi-search request do more coordination work.

## Final IVF training values

Use these values:

```text
nlist = 16384
nprobes = 128
training vectors = 4194304
dimension = 512
space = cosinesimil
encoder = sq
bits = 16
clip = false
```

At 100 million vectors, nlist 16,384 gives an average of about 6,104 vectors in
each bucket. Searching 128 balanced buckets examines roughly 781,250 candidate
vectors for one source sentence before the score threshold removes weak
matches.

At 71 million vectors, the same settings examine roughly 554,688 candidates.
This is why the same model can be used from 71 million through 100 million.

The 4,194,304-vector training sample is 256 examples for each bucket. Faiss uses
256 as its normal maximum training examples per centroid, so sending much more
usually only makes training slower. The sample must be random and must include
all important analysis groups, section types, source types, languages, and
sentence lengths. Do not train from the first rows of a sorted file.

SQ16 itself does not learn a codebook. Training is needed because IVF must learn
where its 16,384 bucket centers belong.

### Train the final model

The training index must already contain 512-dimension vectors. It can be the
current catalog if that catalog has a representative mix of sentences.

```http
POST /_plugins/_knn/models/sentence-ivf-sq16-512-nlist16384-v1/_train
{
  "training_index": "sentence_semantic_catalog-ivf-sq16-training-v1",
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

Check the model:

```http
GET /_plugins/_knn/models/sentence-ivf-sq16-512-nlist16384-v1?filter_path=model_id,state,error,dimension,space_type,method
```

Wait until state is `created`. This is a k-NN model, not an ML Commons model. It
does not use the ML Commons deploy or undeploy APIs.

Train during a quiet window. Training and large reindexing compete with normal
queries for CPU, memory, and disk. No separate permanent training node is
needed, but the production traffic test should not run at the same time.

## Final catalog mapping

Create a new physical index. Do not change the existing vector mapping in
place.

```http
PUT /sentence_semantic_catalog-ivf-sq16-100m-v1
{
  "settings": {
    "number_of_shards": 18,
    "number_of_replicas": 1,
    "index.knn": true,
    "index.default_pipeline": "sentence_bedrock_embedding_pipeline"
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
        "model_id": "sentence-ivf-sq16-512-nlist16384-v1"
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

If vectors are copied from an existing catalog, use a reindex request that
removes the ingest pipeline. Otherwise, the pipeline can call Titan again for
vectors that already exist.

Do not use `compression_level: 16x`. Do not use the PQ encoder. Both are
different from SQ16 and the tested radial search behavior is not the same.

## Search values

Start production with nprobes 128:

```json
{
  "size": 10000,
  "query": {
    "knn": {
      "sentenceContentVector": {
        "vector": ["512 values"],
        "min_score": 0.95,
        "method_parameters": {
          "nprobes": 128
        }
      }
    }
  }
}
```

In this application, a 90% cosine threshold becomes an OpenSearch score of
0.95. The API must still convert and verify every returned score.

IVF is approximate. Radial `min_score` returns every qualifying candidate found
inside the buckets that IVF visits, not every possible vector in all 16,384
buckets. Higher nprobes improves recall and costs more CPU and time.

Test these values on the same saved query set:

| nprobes | Expected use |
|---:|---|
| 64 | Fastest option, but most likely to miss a valid match. |
| 128 | Final starting value. |
| 256 | Recall check when 128 misses important matches. |

Changing nprobes does not require model retraining or reindexing. Changing
nlist, dimensions, engine, similarity space, or encoder requires a new model
and a new physical index.

Use `sentenceKey` for exact matches. Do not spend a vector query on text that is
already known to be exactly equal.

## Monthly AWS cost in commercial US-East

These are planning prices from the AWS public price list on September 21, 2026.
They use 730 hours per month. AWS can change prices, so DevOps must confirm the
same configuration in AWS Pricing Calculator before purchase.

### On-Demand production cost

| Resource | Quantity and rate | Monthly cost |
|---|---:|---:|
| Data nodes | 6 × r7g.4xlarge.search × $1.422/hour | $6,228.36 |
| Cluster managers | 3 × m7g.large.search × $0.135/hour | $295.65 |
| Coordinators | 3 × m7g.2xlarge.search × $0.542/hour | $1,186.98 |
| gp3 storage | 4,500 GiB × $0.122/GiB-month | $549.00 |
| Extra IOPS | 30,000 IOPS × $0.008/IOPS-month | $240.00 |
| Extra throughput | 1,500 MiB/s × $0.064/MiB/s-month | $96.00 |
| **Estimated monthly total** |  | **$8,595.99** |
| **Estimated yearly total** |  | **$103,151.88** |

Each 750-GiB gp3 volume includes 3,000 IOPS and 250 MiB/s. The cost table bills
only the extra 5,000 IOPS and extra 250 MiB/s on each of the six data nodes.

The extra disk performance costs about $336.00 per month. Keep 8,000 IOPS and
500 MiB/s during the first load and reindex. After production becomes steady,
CloudWatch may show that the included 3,000 IOPS and 250 MiB/s are enough. If
so, removing the extra performance lowers the On-Demand total to about
$8,259.99 per month.

### Reserved instance choices

EBS stays monthly. The savings below apply to the data, manager, and coordinator
instances.

| Purchase choice | Monthly equivalent | Yearly equivalent | Saving from On-Demand |
|---|---:|---:|---:|
| On-Demand | $8,595.99 | $103,151.88 | — |
| 1-year, no upfront | $6,204.51 | $74,454.12 | About 28% |
| 1-year, all upfront | $5,897.25 | $70,767.00 | About 31% |
| 3-year, no upfront | $4,892.70 | $58,712.40 | About 43% |
| 3-year, all upfront | $4,586.33 | $55,036.00 | About 47% |

For the 3-year all-upfront choice, the compute payment is about $133,248.00.
Adding three years of EBS makes the estimated three-year total about
$165,108.00.

Do not buy a long reservation before the 71-million-vector load test proves
that six r7g.4xlarge data nodes meet the real latency and concurrency target.
After that test passes, the 1-year no-upfront choice is the safest first
commitment. The 3-year all-upfront choice is the lowest long-term price.

## Titan vector creation cost and time

Titan Text Embeddings V2 supports 512 dimensions. AWS says English text
averages about 4.7 characters per token. Using an assumed average sentence
length of 150 characters and the current commercial price of $0.02 per million
input tokens:

| Unique sentences | Estimated input tokens | One-time Titan cost |
|---:|---:|---:|
| 71 million | 2.266 billion | About $45.32 |
| 100 million | 3.191 billion | About $63.83 |

This is only the direct Titan model charge. Lasso proxy charges, failed request
retries, network charges, and support charges are not included.

The larger risk is time, not Titan token cost. If one unique sentence requires
one remote request, the rough duration is:

| Successful requests per minute | Time for 71 million | Time for 100 million |
|---:|---:|---:|
| 500 | 98.6 days | 138.9 days |
| 1,000 | 49.3 days | 69.4 days |
| 2,000 | 24.7 days | 34.7 days |
| 5,000 | 9.9 days | 13.9 days |
| 10,000 | 4.9 days | 6.9 days |

Confirm the Bedrock and Lasso request limits before the full load. Request any
needed quota increase first. More API workers do not help after the remote
service starts throttling.

## What is not included in the monthly total

- Lasso proxy cost.
- AWS Support plan.
- Taxes.
- Cross-AZ or internet data transfer, if billed for the final network path.
- Extra CloudWatch logs and custom metrics.
- Manual snapshot storage in S3.
- A separate development or performance-test domain.
- Temporary extra data nodes used during a risky migration.
- Application servers, load balancers, and their logs.

## Production performance test

Before calling this final in production, load all 71 million vectors and test
the same search shape the UI will use.

Use this pass condition:

| Check | Pass condition |
|---|---|
| Page size | 50 source sentences per request |
| Result scope | Counts and matches for the current page only |
| Warm p50 latency | Under 1.5 seconds |
| Warm p95 latency | Under 3 seconds |
| Warm p99 latency | Under 5 seconds |
| Errors | No search rejection or k-NN circuit-breaker event |
| Recall | nprobes 128 returns the accepted match set compared with the saved baseline |
| Disk | Less than 70% on every data node |
| CPU | Normally below 70%; short peaks are acceptable |
| JVM pressure | Normally below 75% |
| k-NN cache | Normally below 80% of its limit |

Run three tests:

1. One user making requests one at a time.
2. The expected normal number of users at the same time.
3. Twice the expected traffic for at least 30 minutes.

An `_msearch` request saves HTTP overhead, but its 50 vector searches are still
50 search operations inside OpenSearch. It does not turn them into one vector
comparison. This is why coordinator nodes and realistic concurrency testing
matter.

## Warm-up

After a restart, deployment, shard move, or blue/green change, the native vector
index may not be in memory. Warm the production catalog before opening traffic:

```http
GET /_plugins/_knn/warmup/sentence_semantic_catalog-ivf-sq16-100m-v1
```

Then run a small set of normal searches. Do not warm old comparison indexes on
the production cluster.

## Monitoring and scale rules

Watch these CloudWatch metrics per node:

- KNNGraphMemoryUsagePercentage.
- KNNGraphQueryErrors.
- KNNLoadExceptionCount.
- CPUUtilization.
- JVMMemoryPressure.
- FreeStorageSpace.
- SearchRate, SearchLatency, and ThreadpoolSearchQueue.
- CoordinatorCPUUtilization on coordinator nodes.

Also check shard placement and index size:

```http
GET /_cat/nodes?v&h=name,ip,node.role,cpu,heap.percent,ram.percent,load_1m
GET /_cat/allocation?v
GET /_cat/shards/sentence_semantic_catalog-ivf-sq16-100m-v1?v
GET /_cat/indices/sentence_semantic_catalog-ivf-sq16-100m-v1,sentence_occurrences-v1?v&bytes=gb
GET /_plugins/_knn/stats
```

Use these scale rules:

- Add three data nodes, going from six to nine, when p95 stays above three
  seconds after query tuning, CPU stays above 70%, or the k-NN cache stays above
  80%.
- Increase coordinator size or count when CoordinatorCPUUtilization stays above
  70% or result merging is the slow part.
- Increase EBS before any node reaches 70% disk.
- Move nprobes from 128 to 256 only when the recall test shows missing matches.
- Move nprobes from 128 to 64 only when recall still passes and lower latency is
  needed.
- Do not change nlist on the working index. Train a new model and create a new
  physical index.

Add nodes in groups of three so the domain stays balanced across three
Availability Zones.

## Safe rollout order

1. Configure the six-data-node production domain.
2. Create a representative training index with at least 4,194,304 vectors.
3. Train the IVF-SQ16 model with nlist 16,384 and nprobes 128.
4. Wait for model state `created`.
5. Create the new 18-primary-shard catalog.
6. Seed or reindex vectors without making duplicate Titan calls.
7. Confirm catalog and occurrence counts.
8. Warm only the new catalog.
9. Run recall tests at nprobes 64, 128, and 256.
10. Run the single-user and concurrent latency tests.
11. Move the production alias in one atomic alias request.
12. Keep the old index closed or cold during the rollback window. Do not warm it.
13. Delete old vector indexes after rollback approval.
14. Buy reserved instances only after the full-scale test passes.

## Final decision summary

For 71 million vectors now and 100 million later, use six r7g.4xlarge data
nodes, three m7g.large cluster managers, three m7g.2xlarge coordinators, and 750
GiB gp3 per data node. Use 18 catalog primaries, one replica, nlist 16,384,
nprobes 128, and 4,194,304 representative training vectors.

The production On-Demand estimate in commercial AWS US-East is about $8,596 per
month. A 1-year no-upfront reservation lowers it to about $6,205 per month after
the full-scale test passes.

This setup is large enough by the SQ16 memory calculation. The final approval
still depends on the 71-million-vector p95 latency and recall test because no
hardware estimate can replace a test using the real sentence distribution.

## Sources

- [OpenSearch Faiss scalar quantization](https://docs.opensearch.org/latest/vector-search/optimizing-storage/faiss-16-bit-quantization/)
- [OpenSearch k-NN methods and IVF parameters](https://docs.opensearch.org/latest/mappings/supported-field-types/knn-methods-engines/)
- [OpenSearch radial search](https://docs.opensearch.org/latest/vector-search/specialized-operations/radial-search-knn/)
- [OpenSearch vector memory settings](https://docs.opensearch.org/latest/vector-search/settings/)
- [AWS OpenSearch operational best practices](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/bp.html)
- [AWS dedicated coordinator nodes](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/Dedicated-coordinator-nodes.html)
- [AWS gp3 OpenSearch storage](https://aws.amazon.com/blogs/big-data/lower-your-amazon-opensearch-service-storage-cost-with-gp3-amazon-ebs-volumes/)
- [Amazon Titan Text Embeddings models](https://docs.aws.amazon.com/bedrock/latest/userguide/titan-embedding-models.html)
- [AWS OpenSearch commercial US-East public price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonES/current/us-east-1/index.json)
- [AWS Bedrock commercial US-East public price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonBedrock/current/us-east-1/index.json)
- [Faiss training sample guidance](https://github.com/facebookresearch/faiss/wiki/FAQ#can-i-ignore-warning-clustering-xxx-points-to-yyy-centroids)
