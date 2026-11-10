# OpenSearch Processing Production Deployment Plan

## Executive summary

This deployment plan uses a rounded production volume of **7,000,000
historical documents**. Documents will be released from the smallest sentence
count to the largest sentence count through the new OpenSearch-based pipeline:

```text
Doc2Vec -> Sentence Similarity -> Case Similarity -> API Transform
```

The proposed production capacity is **50 pods**:

| Service | Pods | Main responsibility |
|---|---:|---|
| Doc2Vec | 10 | Prepare sentences, classification, sentence keys, vectors, catalog records, and occurrence records. |
| Sentence Similarity | 15 | Search OpenSearch and save bidirectional sentence relationships in `tsp.titan_matches`. |
| Case Similarity | 15 | Rebuild application-level similar-case and TAH summaries. |
| API Transform | 10 | Build the final application payload and publish it to Kafka. |

The 100–500 sentence benchmark processed 10,000 documents in 12 hours, or
about 833 documents per hour. Using that conservative result, the processing
floor is approximately **36 to 37 days**. The production commitment should be
**45 to 55 calendar days**, allowing time for production startup, retries,
throttling, threshold readiness, large-document processing, and validation.

Doc2Vec is expected to finish its measured workload in about **5 days**. Sentence
Similarity and Case Similarity are the long-running stages. API Transform is
expected to keep up with the messages it receives.

## Goal

Process the complete 7-million-document historical population safely, starting
with the smallest documents and ending with the largest documents, without
affecting live production traffic, losing SQS messages, or publishing
incomplete Kafka payloads.

The deployment is complete when:

- All 7,000,000 historical documents have reached a final result.
- The additional 60,000–70,000 documents received during historical processing
  have completed their 4–5 day catch-up run.
- All four processing queues have drained.
- No unexplained messages remain in a DLQ.
- OpenSearch, PostgreSQL, SQS, and Kafka remain healthy.
- Stage input and output counts reconcile.
- A representative sample passes business validation.

## How the pipeline works

```text
Historical document IDs
        |
        v
  10 Doc2Vec pods
        |
        | SQS
        v
  15 Sentence Similarity pods
        |
        | SQS
        v
  15 Case Similarity pods
        |--------------------> Optional TAH Kafka event
        |
        | SQS
        v
  10 API Transform pods
        |
        v
   API event Kafka topic
```

Once the threshold is available, each completed document moves immediately to
the next service. We do not wait for Doc2Vec to finish the entire backlog before
starting Sentence Similarity. Because the services run concurrently, the
end-to-end duration is controlled mainly by the slowest stage rather than by
adding all four stage durations.

## Workload and measured throughput

The rates below are treated as the **combined hourly throughput of the proposed
pod count**, not the throughput of one pod. The document counts come from the
most recent production snapshot. Because the snapshot was received a few days
ago, the current total may be approximately 5,000–10,000 documents higher.

| Sentences per document | Documents | Doc2Vec/hour | Doc2Vec hours | Sentence Sim/hour | Sentence Sim hours | Case Sim/hour | Case Sim hours |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.3 million | 95,000 | 24.2 | 40,000 | 57.5 | 40,000 | 57.5 |
| 1–5 | 2.25 million | 70,000 | 32.1 | 12,000 | 187.5 | 12,000 | 187.5 |
| 5–10 | 726,000 | 60,000 | 12.1 | 10,000 | 72.6 | 10,000 | 72.6 |
| 10–50 | 894,000 | 50,000 | 17.9 | 7,000 | 127.7 | 7,000 | 127.7 |
| 50–100 | 250,000 | 35,000 | 7.1 | 4,000–5,000 | 50.0–62.5 | 4,000–5,000 | 50.0–62.5 |
| 100–500 | 259,000 | 12,000 | 21.6 | 833 observed | 310.9 | 833 observed | 310.9 |
| 500–1,000 | 3,000 | Entire cohort | Not separately measured | 500 | 6.0 | 300 | 10.0 |
| 1,000–3,000 | 471 | Entire cohort | Not separately measured | Entire cohort | Not separately measured | Entire cohort | Not separately measured |
| More than 3,000 | 154 | Entire cohort | Not separately measured | Entire cohort | Not separately measured | Entire cohort | Not separately measured |

For safe planning, the observed 833-per-hour rate is applied to both Sentence
Similarity and Case Similarity for the 100–500 group. This assumption should be
replaced with the measured production rate when that information is available.

The final 625 documents with more than 1,000 sentences represent less than
**0.01%** of the population. Their benchmark uses “entire cohort” rather than
an hourly rate, so their duration is not calculated separately in the ETA.

## Estimated completion window

The snapshot contains approximately 6.68 million documents. The estimates
below extrapolate the measured rates to the safer **7-million-document**
production planning volume, which also covers recent growth and additional
documents identified before processing starts.

| Stage | Estimated workload duration | Planning interpretation |
|---|---:|---|
| Doc2Vec | About 121 hours / 5.0 days, plus the small groups without separate timing | Fastest data-producing stage. |
| Sentence Similarity | About 851–864 hours / 35.4–36.0 days, plus the groups without separate timing | One of the two limiting stages. |
| Case Similarity | About 855–868 hours / 35.6–36.2 days, plus the groups without separate timing | Expected end-to-end bottleneck. |
| API Transform | Keeps up with received messages | Not expected to build a sustained backlog. |

The benchmark-based completion floor is therefore approximately **36 to 37
days**. The recommended production target is **45 to 55 days**.

This estimate assumes:

- The pod counts remain available throughout the backfill.
- The measured production rates remain stable.
- PostgreSQL and OpenSearch do not experience sustained throttling.
- Kafka and SQS remain available.
- Live traffic either has reserved capacity or can take priority over backfill.
- The rates already represent the total throughput of the stated pod counts.

## Similarity threshold decision gate

The similarity threshold is a required business value from the PO. Its status
will be checked on the production start date.

### Threshold available by production start

Run the complete pipeline in sentence-count order:

```text
Doc2Vec -> Sentence Similarity -> Case Similarity -> API Transform
```

The 45–55 day historical target applies.

### Threshold not available by production start

Do not block the OpenSearch migration. Run Doc2Vec and add all historical
documents to the sentence catalog and sentence occurrence indexes. Do not start
Sentence Similarity, Case Similarity, or API Transform for this historical
backlog until the PO supplies the approved threshold.

For 7,000,000 documents, the Doc2Vec-only benchmark estimate is approximately:

```text
121 processing hours / 24 = 5.0 days
```

The safe Doc2Vec-only production estimate is **6–8 days**, including production
startup and the largest document groups.

When the threshold arrives:

1. Configure and verify the approved value.
2. Validate the threshold with a representative Sentence Similarity sample.
3. Start Sentence Similarity with the smallest document group.
4. Allow successful documents to continue to Case Similarity and API Transform.
5. Continue through the sentence-count groups in ascending order.

If the threshold is received during the first 6–8 days, the 45–55 day plan can
remain the working target. A longer threshold delay extends the completion date
by approximately the additional waiting time because the downstream historical
work cannot begin.

## Post-historical catch-up and live activation

Approximately **60,000–70,000 new documents** are expected to arrive while the
historical population is being processed. After the historical queues are
complete, run these documents as a separate catch-up group.

The catch-up group is planned for **4–5 days**. After it completes and its
counts are validated, enable the normal live flow.

| Milestone | Planned duration |
|---|---:|
| Historical processing | 45–55 days |
| New-document catch-up | 4–5 days |
| Expected time before live activation | 49–60 days |

The 49–60 day range assumes the similarity threshold is available early enough
to avoid adding a separate waiting period.

## Monitoring dashboard

### Service and SQS measurements

Monitor these values for every stage:

- Documents completed per hour.
- Source queue depth and in-flight count.
- Age of the oldest message.
- Retry count and DLQ growth.
- Processing duration by document size.
- Pod CPU, memory, restarts, and unavailable replicas.
- Difference between incoming and completed document counts.

### OpenSearch measurements

- Cluster health.
- Search and indexing latency.
- CPU and JVM memory pressure.
- Thread-pool rejections and throttling.
- Ingestion pipeline failures.
- Catalog and occurrence document-count growth.

### PostgreSQL measurements

- CPU, memory, active connections, and connection-pool usage.
- Slow queries and SQL-function duration.
- Lock waits and deadlocks.
- Transaction failures and retry counts.
- Growth of `tsp.titan_matches` and `tsp.titan_similar_cases`.

### Kafka measurements

- Publish successes and failures.
- Delivery callback errors.
- Partition and consumer lag.
- Message size and rejected messages.
- Difference between generated and confirmed event counts.

## Pause criteria

Pause new backfill submissions when any of these conditions occurs:

- A DLQ grows continuously without an understood reason.
- The age of the oldest live message increases continuously.
- PostgreSQL experiences sustained high CPU, connection exhaustion, or lock
  waits.
- OpenSearch becomes unhealthy or repeatedly rejects requests.
- Kafka rejects messages or confirmed delivery counts do not match generated
  event counts.
- A stage produces an unexplained count difference.
- Pod restart or failure rates increase after capacity changes.

Pausing means stopping new source submissions first. In-flight SQS messages
remain recoverable and should be allowed to finish when the infrastructure is
healthy.

## Recovery and rollback

The flow deletes an SQS message only after its required work succeeds. A failed
message remains available for retry and eventually moves to its DLQ.

Recovery steps:

1. Stop releasing new historical messages.
2. Keep or reduce consumers according to infrastructure health.
3. Identify the failed `tsp_id` values from structured logs and DLQs.
4. Correct the application, data, permission, or infrastructure problem.
5. Replay only the failed messages.
6. Reconcile counts before resuming historical processing.

Database migrations should not be rolled back while processed rows depend on
the new schema. Application pods can be scaled down or returned to the last
stable image while SQS safely retains unfinished work.

## Data validation checklist

- Doc2Vec saved the correct sentence keys, word counts, classifications,
  catalog records, and occurrence records.
- Sentence Similarity excluded classified and short sentences from normal
  sentence matching.
- `tsp.titan_matches` contains both directions without duplicate ownership.
- Case Similarity produces the expected application lists:
  - One sentence pair: similar application.
  - Two sentence pairs: similar case.
  - Three or more sentence pairs: TAH case.
- API Transform includes every Titan match stored for each source sentence.
- Large application payloads use the expected create and update Kafka events.
- Kafka delivery audits match confirmed broker acknowledgements.
- Asylee and Refugee payloads use the correct data and catalog paths.

## Important production risk: very large single sentences

API Transform includes all matches for every source sentence. Its current
payload splitting divides the list of sentences across Kafka events. It does
not divide the match list inside one individual sentence.

Before full production, measure the largest serialized single-sentence record.
If it can exceed the downstream Kafka/API limit, the producer and consumer need
an agreed match-chunk contract so later chunks are merged rather than replacing
earlier matches. This check is a production-readiness gate because silently
dropping matches is not acceptable.

