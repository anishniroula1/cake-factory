# Find a TSP ID for Kafka TAH Testing

Use this query to find a `tsp_id` that is suitable for testing the full Case
Similarity Kafka flow. Each returned row has:

- A current document in `text_similarity_input`.
- At least one sentence in `sentences_vector` for the returned `tsp_id`.
- Exactly one receipt for its application.
- A receipt that belongs to exactly one application.

This query does not read `titan_similar_cases` or `titan_matches`. It only finds
a document with a clean one-to-one application and receipt mapping. Whether the
document produces a TAH event is determined when Case Similarity processes it.

It also does not read `kafka_ta_hit_lookup`.

```sql
WITH candidate_documents AS MATERIALIZED (
    /* Start with recent current documents that have a receipt number. */
    SELECT DISTINCT
        input.tsp_id,
        input.application_id,
        input.a_number,
        input.analysis_group,
        registry.receipt_number
    FROM tsp.text_similarity_input AS input
    JOIN landing.global_document_registry AS registry
        ON registry.a_number = input.a_number
    WHERE input.is_current = TRUE
      AND registry.receipt_number IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM tsp.sentences_vector AS sentence
          WHERE sentence.tsp_id = input.tsp_id
      )
    ORDER BY input.tsp_id DESC
    LIMIT 10000
),
application_receipt_counts AS (
    /* Count receipts only for applications in the small candidate list. */
    SELECT
        input.application_id,
        COUNT(DISTINCT registry.receipt_number) AS receipt_count
    FROM tsp.text_similarity_input AS input
    JOIN landing.global_document_registry AS registry
        ON registry.a_number = input.a_number
    WHERE input.is_current = TRUE
      AND registry.receipt_number IS NOT NULL
      AND input.application_id IN (
          SELECT application_id
          FROM candidate_documents
      )
    GROUP BY input.application_id
),
receipt_application_counts AS (
    /* Count applications only for receipts in the small candidate list. */
    SELECT
        registry.receipt_number,
        COUNT(DISTINCT input.application_id) AS application_count
    FROM landing.global_document_registry AS registry
    JOIN tsp.text_similarity_input AS input
        ON input.a_number = registry.a_number
    WHERE input.is_current = TRUE
      AND registry.receipt_number IS NOT NULL
      AND registry.receipt_number IN (
          SELECT receipt_number
          FROM candidate_documents
      )
    GROUP BY registry.receipt_number
),
test_candidates AS (
    /* Return one tsp_id for each one-to-one application and receipt mapping. */
    SELECT DISTINCT ON (candidate.application_id)
        candidate.tsp_id,
        candidate.application_id,
        candidate.a_number,
        candidate.analysis_group,
        candidate.receipt_number,
        application_receipt.receipt_count,
        receipt_application.application_count
    FROM candidate_documents AS candidate
    JOIN application_receipt_counts AS application_receipt
        ON application_receipt.application_id = candidate.application_id
    JOIN receipt_application_counts AS receipt_application
        ON receipt_application.receipt_number = candidate.receipt_number
    WHERE application_receipt.receipt_count = 1
      AND receipt_application.application_count = 1
    ORDER BY candidate.application_id, candidate.tsp_id DESC
)
SELECT
    tsp_id,
    application_id,
    a_number,
    analysis_group,
    receipt_number,
    receipt_count,
    application_count
FROM test_candidates
ORDER BY application_id
LIMIT 20;
```

The first pass checks the latest 10000 current documents. If it returns no
candidate, increase that limit and run it again.

The two count columns should both be `1`:

| tsp_id | application_id | receipt_number | receipt_count | application_count |
|---:|---:|---|---:|---:|
| 12345 | 100 | ABC123 | 1 | 1 |

Pick one returned `tsp_id` and send it through Case Similarity. If processing
produces a TAH event, the application and receipt mapping will not be ambiguous.
