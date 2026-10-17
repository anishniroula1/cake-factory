# Find a TSP ID for Kafka TAH Testing

Use this query to find a `tsp_id` that is suitable for testing the full Case
Similarity Kafka flow. Each returned row has:

- A current TAH value of `true` and at least one TAH case.
- Exactly one receipt for its application.
- A receipt that belongs to exactly one application.
- No Kafka lookup row for the selected environment.

Replace `YOUR_KAFKA_ENV` before running the query.

```sql
WITH tah_candidates AS (
    SELECT DISTINCT
        input.tsp_id,
        input.application_id,
        input.a_number,
        input.analysis_group,
        summary.tah_case_count,
        registry.receipt_number
    FROM tsp.titan_similar_cases AS summary
    JOIN tsp.text_similarity_input AS input
        ON input.application_id = summary.application_id
       AND input.analysis_group = summary.analysis_group
    JOIN landing.global_document_registry AS registry
        ON registry.a_number = input.a_number
    LEFT JOIN validation.kafka_ta_hit_lookup AS published
        ON published.receipt_number = registry.receipt_number
       AND published.kafka_environment = 'YOUR_KAFKA_ENV'
    WHERE summary.is_tah = TRUE
      AND summary.tah_case_count > 0
      AND registry.receipt_number IS NOT NULL
      AND published.receipt_number IS NULL
),
receipt_owner_counts AS (
    SELECT
        registry.receipt_number,
        COUNT(DISTINCT input.application_id) AS application_count
    FROM landing.global_document_registry AS registry
    JOIN tsp.text_similarity_input AS input
        ON input.a_number = registry.a_number
    WHERE registry.receipt_number IN (
        SELECT receipt_number
        FROM tah_candidates
    )
    GROUP BY registry.receipt_number
),
application_receipt_counts AS (
    SELECT
        input.application_id,
        COUNT(DISTINCT registry.receipt_number) AS receipt_count
    FROM tsp.text_similarity_input AS input
    JOIN landing.global_document_registry AS registry
        ON registry.a_number = input.a_number
    WHERE input.application_id IN (
        SELECT application_id
        FROM tah_candidates
    )
    GROUP BY input.application_id
),
one_document_per_application AS (
    SELECT DISTINCT ON (candidate.application_id)
        candidate.tsp_id,
        candidate.application_id,
        candidate.a_number,
        candidate.analysis_group,
        candidate.tah_case_count,
        candidate.receipt_number
    FROM tah_candidates AS candidate
    JOIN receipt_owner_counts AS receipt_owner
        ON receipt_owner.receipt_number = candidate.receipt_number
    JOIN application_receipt_counts AS application_receipt
        ON application_receipt.application_id = candidate.application_id
    WHERE receipt_owner.application_count = 1
      AND application_receipt.receipt_count = 1
    ORDER BY candidate.application_id, candidate.tsp_id
)
SELECT *
FROM one_document_per_application
ORDER BY tah_case_count DESC
LIMIT 20;
```

Pick one returned `tsp_id` and send it through Case Similarity. After Kafka
confirms delivery, verify that the lookup row was saved:

```sql
SELECT *
FROM validation.kafka_ta_hit_lookup
WHERE receipt_number = 'RETURNED_RECEIPT_NUMBER'
  AND kafka_environment = 'YOUR_KAFKA_ENV';
```

Expected result: one row containing the receipt, Kafka environment, and the
published `is_tah` value.
