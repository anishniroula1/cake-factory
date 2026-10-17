# Kafka TAH Event Diagnostic

Use this check when Case Similarity updates `titan_similar_cases` but does not
publish a Kafka event or recreate a deleted `kafka_ta_hit_lookup` row.

## Expected behavior

`validation.kafka_ta_hit_lookup` stores the last TAH value confirmed by Kafka.
After its row is deleted and the deletion is committed, the next Case
Similarity run should treat the current value as new, publish it, and insert a
new lookup row.

The current TAH value comes from `tsp.titan_similar_cases`. The Kafka query gets
`application_id`, `a_number`, and `analysis_group` from
`tsp.text_similarity_input`, then uses `a_number` to find the Kafka receipt in
`landing.global_document_registry`. It does not read
`landing.application_id_lookup`.

## Diagnostic query

Replace `YOUR_APPLICATION_ID` and `YOUR_KAFKA_ENV`. Run `COMMIT;` first if the
lookup row was deleted in a manual transaction.

```sql
COMMIT;

SELECT DISTINCT
    input.application_id,
    input.a_number,
    input.analysis_group,
    summary.is_tah AS current_is_tah,
    summary.tah_case_count,
    registry.receipt_number,
    published.kafka_environment,
    published.is_tah AS published_is_tah,
    CASE
        WHEN registry.receipt_number IS NULL
            THEN 'GDR JOIN FAILED'
        WHEN published.receipt_number IS NULL
            THEN 'SHOULD PUBLISH AS NEW'
        WHEN published.is_tah IS DISTINCT FROM summary.is_tah
            THEN 'SHOULD PUBLISH CHANGED VALUE'
        ELSE 'UNCHANGED'
    END AS kafka_decision
FROM tsp.text_similarity_input AS input
LEFT JOIN tsp.titan_similar_cases AS summary
    ON summary.application_id = input.application_id
   AND summary.analysis_group = input.analysis_group
LEFT JOIN landing.global_document_registry AS registry
    ON registry.a_number = input.a_number
LEFT JOIN validation.kafka_ta_hit_lookup AS published
    ON published.receipt_number = registry.receipt_number
   AND published.kafka_environment = 'YOUR_KAFKA_ENV'
WHERE input.application_id = YOUR_APPLICATION_ID;
```

The expected result after deleting the lookup row is:

| current_is_tah | tah_case_count | receipt_number | published_is_tah | kafka_decision |
|---|---:|---|---|---|
| true | 9 | ABC123 | null | SHOULD PUBLISH AS NEW |

## Reasons now written to the service log

| Log reason | Meaning |
|---|---|
| `MISSING_TEXT_SIMILARITY_INPUT` | The affected application does not have values needed for Kafka. |
| `MISSING_GDR_RECEIPT` | Its `a_number` did not find a receipt in GDR. |
| `RECEIPT_HAS_MULTIPLE_APPLICATIONS` | The same receipt belongs to multiple affected applications, so one safe value cannot be chosen. |
| `UNCHANGED_TAH_VALUE` | The current value equals the last Kafka-confirmed value. |

Missing database mappings are warnings. An unchanged value is an informational
message. Logs include candidate counts plus sample application IDs and receipt
numbers without printing one line for every affected application.

## Multiple receipt numbers

Multiple different receipt numbers for one application do not cause a skip.
Each receipt is checked separately and can produce its own Kafka event and
`kafka_ta_hit_lookup` row.

Example:

| Receipt | Application | Result |
|---|---:|---|
| R1 | 100 | Send |
| R2 | 100 | Send |

A receipt is skipped only when that same receipt belongs to multiple affected
applications:

| Receipt | Application | Result |
|---|---:|---|
| R1 | 100 | Skip R1 |
| R1 | 200 | Skip R1 |
| R2 | 100 | R2 can still be sent |

Use this query to check how many applications share one receipt:

```sql
SELECT
    registry.receipt_number,
    COUNT(DISTINCT input.application_id) AS application_count,
    ARRAY_AGG(DISTINCT input.application_id ORDER BY input.application_id)
        AS application_ids
FROM tsp.text_similarity_input AS input
JOIN landing.global_document_registry AS registry
    ON registry.a_number = input.a_number
WHERE registry.receipt_number = 'YOUR_RECEIPT_NUMBER'
GROUP BY registry.receipt_number;
```

Use this query to list every receipt belonging to one application:

```sql
SELECT
    input.application_id,
    input.a_number,
    COUNT(DISTINCT registry.receipt_number) AS receipt_count,
    ARRAY_AGG(
        DISTINCT registry.receipt_number
        ORDER BY registry.receipt_number
    ) AS receipt_numbers
FROM tsp.text_similarity_input AS input
JOIN landing.global_document_registry AS registry
    ON registry.a_number = input.a_number
WHERE input.application_id = YOUR_APPLICATION_ID
GROUP BY input.application_id, input.a_number;
```
