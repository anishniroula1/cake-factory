# Check Skipped Kafka TAH Candidates

The service log only shows how many Kafka candidates were skipped and why. Use
this query when the complete application and receipt list is needed.

Replace `YOUR_TSP_ID` and `YOUR_KAFKA_ENV`, then run the full query in pgAdmin.
It follows the same rules as the Case Similarity service and returns every
candidate with its current value, last Kafka-confirmed value, and decision.

```sql
WITH parameters AS (
    SELECT
        YOUR_TSP_ID::INTEGER AS tsp_id,
        'YOUR_KAFKA_ENV'::TEXT AS kafka_environment
),
source_context AS (
    /* Find the application and analysis group for the processed document. */
    SELECT DISTINCT
        input.application_id,
        input.analysis_group
    FROM tsp.text_similarity_input AS input
    JOIN parameters
        ON parameters.tsp_id = input.tsp_id
),
affected_applications AS (
    /* The source application is always checked. */
    SELECT source.application_id
    FROM source_context AS source

    UNION

    /* Add applications currently connected through sentence matches. */
    SELECT DISTINCT target_owner.application_id
    FROM source_context AS source
    JOIN tsp.titan_matches AS source_match
        ON source_match.application_id = source.application_id
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    JOIN tsp.titan_matches AS target_owner
        ON target_owner.global_id = target.target_global_id::INTEGER
    JOIN tsp.text_similarity_input AS target_input
        ON target_input.application_id = target_owner.application_id
       AND target_input.analysis_group = source.analysis_group
    WHERE target_owner.application_id <> source.application_id

    UNION

    /* Include relationships already saved in the source summary. */
    SELECT previous_target.application_id::INTEGER
    FROM source_context AS source
    JOIN tsp.titan_similar_cases AS summary
        ON summary.application_id = source.application_id
       AND summary.analysis_group = source.analysis_group
    CROSS JOIN LATERAL jsonb_array_elements_text(
        summary.similar_application_ids
    ) AS previous_target(application_id)
),
application_values AS (
    /* Load the values needed to build one Kafka candidate. */
    SELECT DISTINCT
        affected.application_id,
        input.a_number,
        input.analysis_group,
        input.application_id IS NOT NULL AS has_application_values,
        COALESCE(summary.is_tah, FALSE) AS is_tah
    FROM affected_applications AS affected
    LEFT JOIN tsp.text_similarity_input AS input
        ON input.application_id = affected.application_id
    LEFT JOIN tsp.titan_similar_cases AS summary
        ON summary.application_id = input.application_id
       AND summary.analysis_group = input.analysis_group
),
candidates AS (
    /* One application can produce more than one row when it has many receipts. */
    SELECT DISTINCT
        registry.receipt_number,
        application.application_id,
        application.a_number,
        application.analysis_group,
        application.has_application_values,
        application.is_tah
    FROM application_values AS application
    LEFT JOIN landing.global_document_registry AS registry
        ON registry.a_number = application.a_number
),
receipt_application_counts AS (
    /* A receipt shared by multiple affected applications cannot be published. */
    SELECT
        receipt_number,
        COUNT(DISTINCT application_id) AS application_count
    FROM candidates
    WHERE receipt_number IS NOT NULL
    GROUP BY receipt_number
),
candidate_decisions AS (
    SELECT
        candidates.application_id,
        candidates.a_number,
        candidates.analysis_group,
        candidates.receipt_number,
        candidates.is_tah AS current_is_tah,
        published.is_tah AS kafka_confirmed_is_tah,
        COALESCE(receipt_counts.application_count, 0) AS receipt_application_count,
        CASE
            WHEN NOT candidates.has_application_values
                THEN 'MISSING_TEXT_SIMILARITY_INPUT'
            WHEN candidates.receipt_number IS NULL
                THEN 'MISSING_GDR_RECEIPT'
            WHEN receipt_counts.application_count > 1
                THEN 'RECEIPT_HAS_MULTIPLE_APPLICATIONS'
            WHEN published.receipt_number IS NULL
                THEN 'PUBLISH_NEW_VALUE'
            WHEN published.is_tah IS DISTINCT FROM candidates.is_tah
                THEN 'PUBLISH_CHANGED_VALUE'
            ELSE 'UNCHANGED_TAH_VALUE'
        END AS decision
    FROM candidates
    LEFT JOIN receipt_application_counts AS receipt_counts
        ON receipt_counts.receipt_number = candidates.receipt_number
    LEFT JOIN validation.kafka_ta_hit_lookup AS published
        ON published.receipt_number = candidates.receipt_number
       AND published.kafka_environment = (
           SELECT kafka_environment
           FROM parameters
       )
)
SELECT *
FROM candidate_decisions
ORDER BY decision, application_id, receipt_number NULLS FIRST;
```

To see only the candidates represented by this log:

```text
Skipped 97 TAH Kafka candidate(s) because the current TAH value already
matches the last Kafka-confirmed value.
```

add this line before the final `ORDER BY`:

```sql
WHERE decision = 'UNCHANGED_TAH_VALUE'
```

The other available decision values are:

| Decision | Meaning |
|---|---|
| `MISSING_TEXT_SIMILARITY_INPUT` | The affected application has no TSI values. |
| `MISSING_GDR_RECEIPT` | Its a-number did not resolve to a receipt. |
| `RECEIPT_HAS_MULTIPLE_APPLICATIONS` | One receipt belongs to multiple affected applications. |
| `PUBLISH_NEW_VALUE` | No Kafka-confirmed value exists yet. |
| `PUBLISH_CHANGED_VALUE` | The current TAH value differs from the confirmed value. |
| `UNCHANGED_TAH_VALUE` | Kafka already received the current value. |

This query reads the current database state. If relationships change after the
service log was written, its results may not exactly reproduce the older run.
