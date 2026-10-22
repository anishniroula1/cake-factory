# Compare Legacy and Titan Sentence Matches

These queries show the same information from the two sentence-match tables:

- `tsp.matches` is the legacy table. Its `matches` column is a JSONB list.
- `tsp.titan_matches` is the OpenSearch/Titan table. Its `matches` column is a
  JSONB object split across bucket rows.

Every detailed query returns the source sentence, matching sentence, distance,
match percentage, source and target `tsp_id`, and source and target
`application_id`.

The percentage is calculated as:

```text
match percentage = (1 - distance) * 100
```

Examples:

| Distance | Match percentage | Match type |
|---:|---:|---|
| `0.00` | `100%` | Exact |
| `0.10` | `90%` | Similar |
| `0.24` | `76%` | Similar |

Replace the value in the `parameters` section before running a query in
pgAdmin.

## 1. Legacy `tsp.matches`: search by `tsp_id`

This query expands the legacy JSONB list for one document and shows every
matching sentence.

```sql
WITH parameters AS (
    SELECT 1001::INTEGER AS source_tsp_id
),
matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        source_input.application_id AS source_application_id,
        (target.item ->> 'global_id')::INTEGER AS target_global_id,
        (target.item ->> 'distance')::DOUBLE PRECISION AS distance
    FROM tsp.matches AS source_match
    JOIN tsp.text_similarity_input AS source_input
      ON source_input.tsp_id = source_match.tsp_id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(source_match.matches, '[]'::JSONB)
    ) AS target(item)
    JOIN parameters
      ON parameters.source_tsp_id = source_match.tsp_id
)
SELECT
    pair.source_application_id,
    pair.source_tsp_id,
    pair.source_global_id,
    source_sentence.sent_id_local AS source_sent_id_local,
    source_sentence.sentence_content AS source_sentence,
    target_input.application_id AS target_application_id,
    target_sentence.tsp_id AS target_tsp_id,
    pair.target_global_id,
    target_sentence.sent_id_local AS target_sent_id_local,
    target_sentence.sentence_content AS target_sentence,
    pair.distance,
    ROUND(((1.0 - pair.distance) * 100.0)::NUMERIC, 2) AS match_percentage,
    CASE WHEN pair.distance = 0.0 THEN 'exact' ELSE 'similar' END AS match_type
FROM matching_pairs AS pair
JOIN tsp.sentences_vector AS source_sentence
  ON source_sentence.global_id = pair.source_global_id
JOIN tsp.sentences_vector AS target_sentence
  ON target_sentence.global_id = pair.target_global_id
JOIN tsp.text_similarity_input AS target_input
  ON target_input.tsp_id = target_sentence.tsp_id
ORDER BY pair.source_global_id, pair.distance, pair.target_global_id;
```

### Get only the distinct `tsp_id` values

This returns every source and target document involved in the matches. The
result can be exported to CSV and sent through the OpenSearch backfill flow.

```sql
WITH parameters AS (
    SELECT 1001::INTEGER AS source_tsp_id
),
matching_documents AS (
    SELECT
        source_match.tsp_id AS source_tsp_id,
        target_sentence.tsp_id AS target_tsp_id
    FROM tsp.matches AS source_match
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(source_match.matches, '[]'::JSONB)
    ) AS target(item)
    JOIN tsp.sentences_vector AS target_sentence
      ON target_sentence.global_id = (target.item ->> 'global_id')::INTEGER
    JOIN parameters
      ON parameters.source_tsp_id = source_match.tsp_id
)
SELECT DISTINCT document.tsp_id
FROM (
    SELECT source_tsp_id AS tsp_id FROM matching_documents
    UNION
    SELECT target_tsp_id AS tsp_id FROM matching_documents
) AS document
ORDER BY document.tsp_id;
```

## 2. Titan `tsp.titan_matches`: search by `tsp_id`

This query combines all 16 possible bucket rows for the document. The bucket
number does not change the result; it only keeps each JSONB object smaller.

```sql
WITH parameters AS (
    SELECT 1001::INTEGER AS source_tsp_id
),
matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        source_match.application_id AS source_application_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
    JOIN parameters
      ON parameters.source_tsp_id = source_match.tsp_id
)
SELECT
    pair.source_application_id,
    pair.source_tsp_id,
    pair.source_global_id,
    source_sentence.sent_id_local AS source_sent_id_local,
    source_sentence.sentence_content AS source_sentence,
    target_input.application_id AS target_application_id,
    target_sentence.tsp_id AS target_tsp_id,
    pair.target_global_id,
    target_sentence.sent_id_local AS target_sent_id_local,
    target_sentence.sentence_content AS target_sentence,
    pair.distance,
    ROUND(((1.0 - pair.distance) * 100.0)::NUMERIC, 2) AS match_percentage,
    CASE WHEN pair.distance = 0.0 THEN 'exact' ELSE 'similar' END AS match_type
FROM matching_pairs AS pair
JOIN tsp.sentences_vector AS source_sentence
  ON source_sentence.global_id = pair.source_global_id
JOIN tsp.sentences_vector AS target_sentence
  ON target_sentence.global_id = pair.target_global_id
JOIN tsp.text_similarity_input AS target_input
  ON target_input.tsp_id = target_sentence.tsp_id
ORDER BY pair.source_global_id, pair.distance, pair.target_global_id;
```

### Get only the distinct `tsp_id` values

```sql
WITH parameters AS (
    SELECT 1001::INTEGER AS source_tsp_id
),
matching_documents AS (
    SELECT
        source_match.tsp_id AS source_tsp_id,
        target_sentence.tsp_id AS target_tsp_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    JOIN tsp.sentences_vector AS target_sentence
      ON target_sentence.global_id = target.target_global_id::INTEGER
    JOIN parameters
      ON parameters.source_tsp_id = source_match.tsp_id
)
SELECT DISTINCT document.tsp_id
FROM (
    SELECT source_tsp_id AS tsp_id FROM matching_documents
    UNION
    SELECT target_tsp_id AS tsp_id FROM matching_documents
) AS document
ORDER BY document.tsp_id;
```

## 3. Legacy `tsp.matches`: search by `application_id`

One application can contain several documents. This query reads all of those
documents and returns all of their saved legacy sentence matches.

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        source_input.application_id AS source_application_id,
        (target.item ->> 'global_id')::INTEGER AS target_global_id,
        (target.item ->> 'distance')::DOUBLE PRECISION AS distance
    FROM tsp.matches AS source_match
    JOIN tsp.text_similarity_input AS source_input
      ON source_input.tsp_id = source_match.tsp_id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(source_match.matches, '[]'::JSONB)
    ) AS target(item)
    JOIN parameters
      ON parameters.source_application_id = source_input.application_id
)
SELECT
    pair.source_application_id,
    pair.source_tsp_id,
    pair.source_global_id,
    source_sentence.sent_id_local AS source_sent_id_local,
    source_sentence.sentence_content AS source_sentence,
    target_input.application_id AS target_application_id,
    target_sentence.tsp_id AS target_tsp_id,
    pair.target_global_id,
    target_sentence.sent_id_local AS target_sent_id_local,
    target_sentence.sentence_content AS target_sentence,
    pair.distance,
    ROUND(((1.0 - pair.distance) * 100.0)::NUMERIC, 2) AS match_percentage,
    CASE WHEN pair.distance = 0.0 THEN 'exact' ELSE 'similar' END AS match_type
FROM matching_pairs AS pair
JOIN tsp.sentences_vector AS source_sentence
  ON source_sentence.global_id = pair.source_global_id
JOIN tsp.sentences_vector AS target_sentence
  ON target_sentence.global_id = pair.target_global_id
JOIN tsp.text_similarity_input AS target_input
  ON target_input.tsp_id = target_sentence.tsp_id
ORDER BY pair.source_tsp_id, pair.source_global_id, pair.distance, pair.target_global_id;
```

### Get only the distinct `tsp_id` values

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
matching_documents AS (
    SELECT
        source_match.tsp_id AS source_tsp_id,
        target_sentence.tsp_id AS target_tsp_id
    FROM tsp.matches AS source_match
    JOIN tsp.text_similarity_input AS source_input
      ON source_input.tsp_id = source_match.tsp_id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(source_match.matches, '[]'::JSONB)
    ) AS target(item)
    JOIN tsp.sentences_vector AS target_sentence
      ON target_sentence.global_id = (target.item ->> 'global_id')::INTEGER
    JOIN parameters
      ON parameters.source_application_id = source_input.application_id
)
SELECT DISTINCT document.tsp_id
FROM (
    SELECT source_tsp_id AS tsp_id FROM matching_documents
    UNION
    SELECT target_tsp_id AS tsp_id FROM matching_documents
) AS document
ORDER BY document.tsp_id;
```

## 4. Titan `tsp.titan_matches`: search by `application_id`

This is the Titan equivalent of the previous query. It reads every bucket row
owned by the requested application and combines the results into one list.

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
matching_pairs AS (
    SELECT DISTINCT
        source_match.global_id AS source_global_id,
        source_match.tsp_id AS source_tsp_id,
        source_match.application_id AS source_application_id,
        target.target_global_id::INTEGER AS target_global_id,
        target.distance::DOUBLE PRECISION AS distance
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_each_text(source_match.matches)
        AS target(target_global_id, distance)
    JOIN parameters
      ON parameters.source_application_id = source_match.application_id
)
SELECT
    pair.source_application_id,
    pair.source_tsp_id,
    pair.source_global_id,
    source_sentence.sent_id_local AS source_sent_id_local,
    source_sentence.sentence_content AS source_sentence,
    target_input.application_id AS target_application_id,
    target_sentence.tsp_id AS target_tsp_id,
    pair.target_global_id,
    target_sentence.sent_id_local AS target_sent_id_local,
    target_sentence.sentence_content AS target_sentence,
    pair.distance,
    ROUND(((1.0 - pair.distance) * 100.0)::NUMERIC, 2) AS match_percentage,
    CASE WHEN pair.distance = 0.0 THEN 'exact' ELSE 'similar' END AS match_type
FROM matching_pairs AS pair
JOIN tsp.sentences_vector AS source_sentence
  ON source_sentence.global_id = pair.source_global_id
JOIN tsp.sentences_vector AS target_sentence
  ON target_sentence.global_id = pair.target_global_id
JOIN tsp.text_similarity_input AS target_input
  ON target_input.tsp_id = target_sentence.tsp_id
ORDER BY pair.source_tsp_id, pair.source_global_id, pair.distance, pair.target_global_id;
```

### Get only the distinct `tsp_id` values

```sql
WITH parameters AS (
    SELECT 10::INTEGER AS source_application_id
),
matching_documents AS (
    SELECT
        source_match.tsp_id AS source_tsp_id,
        target_sentence.tsp_id AS target_tsp_id
    FROM tsp.titan_matches AS source_match
    CROSS JOIN LATERAL jsonb_object_keys(source_match.matches)
        AS target(target_global_id)
    JOIN tsp.sentences_vector AS target_sentence
      ON target_sentence.global_id = target.target_global_id::INTEGER
    JOIN parameters
      ON parameters.source_application_id = source_match.application_id
)
SELECT DISTINCT document.tsp_id
FROM (
    SELECT source_tsp_id AS tsp_id FROM matching_documents
    UNION
    SELECT target_tsp_id AS tsp_id FROM matching_documents
) AS document
ORDER BY document.tsp_id;
```

## 5. Legacy `tsp.matches`: get all matched `tsp_id` values for 50 applications

This query finds documents whose total legacy `match_count` is greater than
20. It uses those documents to select 50 applications, including at least 20
qualifying Refugee applications. It then expands the matches saved by every
document owned by those 50 source applications and returns all unique target
`tsp_id` values. This is useful for preparing the matched documents for
OpenSearch comparison testing.

The match count is summed because `tsp.matches` stores one row per source
sentence. For example, if one document has three sentence rows with match
counts of 8, 7, and 6, its document match count is 21 and it qualifies.

Change the three values in the `parameters` section when needed. A
`minimum_match_count` of 20 means the qualifying document must have at least
21 saved matches. The query can return 20 Refugee applications only when at
least 20 Refugee applications satisfy that match-count rule.

```sql
WITH parameters AS (
    SELECT
        20::INTEGER AS minimum_match_count,
        50::INTEGER AS application_limit,
        20::INTEGER AS minimum_refugee_applications
),
document_match_counts AS (
    SELECT
        input.application_id,
        input.analysis_group,
        source_match.tsp_id,
        SUM(source_match.match_count)::BIGINT AS document_match_count
    FROM tsp.matches AS source_match
    JOIN tsp.text_similarity_input AS input
      ON input.tsp_id = source_match.tsp_id
    GROUP BY
        input.application_id,
        input.analysis_group,
        source_match.tsp_id
),
qualifying_documents AS (
    SELECT
        application_id,
        analysis_group,
        tsp_id,
        document_match_count
    FROM document_match_counts
    WHERE document_match_count >
          (SELECT minimum_match_count FROM parameters)
),
application_match_counts AS (
    SELECT
        application_id,
        analysis_group,
        SUM(document_match_count)::BIGINT AS application_match_count
    FROM qualifying_documents
    GROUP BY application_id, analysis_group
),
refugee_applications AS (
    SELECT
        application_id,
        analysis_group,
        application_match_count
    FROM application_match_counts
    WHERE LOWER(analysis_group) = 'refugee'
    ORDER BY application_match_count DESC, application_id
    LIMIT (SELECT minimum_refugee_applications FROM parameters)
),
remaining_applications AS (
    SELECT
        candidate.application_id,
        candidate.analysis_group,
        candidate.application_match_count
    FROM application_match_counts AS candidate
    WHERE NOT EXISTS (
        SELECT 1
        FROM refugee_applications AS refugee
        WHERE refugee.application_id = candidate.application_id
    )
    ORDER BY candidate.application_match_count DESC, candidate.application_id
    LIMIT (
        SELECT application_limit - (SELECT COUNT(*) FROM refugee_applications)
        FROM parameters
    )
),
selected_applications AS (
    SELECT * FROM refugee_applications
    UNION ALL
    SELECT * FROM remaining_applications
),
matched_target_documents AS (
    SELECT DISTINCT
        target_sentence.tsp_id AS matched_tsp_id
    FROM selected_applications AS application
    JOIN tsp.text_similarity_input AS source_input
      ON source_input.application_id = application.application_id
     AND source_input.analysis_group = application.analysis_group
    JOIN tsp.matches AS source_match
      ON source_match.tsp_id = source_input.tsp_id
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(source_match.matches, '[]'::JSONB)
    ) AS target(item)
    JOIN tsp.sentences_vector AS target_sentence
      ON target_sentence.global_id = (target.item ->> 'global_id')::INTEGER
    JOIN tsp.text_similarity_input AS target_input
      ON target_input.tsp_id = target_sentence.tsp_id
    WHERE target_input.application_id <> source_input.application_id
      AND target_input.analysis_group = source_input.analysis_group
)
SELECT matched_tsp_id AS tsp_id
FROM matched_target_documents
ORDER BY matched_tsp_id;
```

The limit applies only to the 50 source applications. It does not limit their
matched documents. For example, if one selected application owns 10 source
documents and those documents match 20 other documents, the 20 target
`tsp_id` values are returned. Across 50 source applications, the result can
therefore contain 20,000, 40,000, or more unique target `tsp_id` values.

The final `DISTINCT` inside `matched_target_documents` makes each target
`tsp_id` appear once, even when several source sentences or source
applications match that same document.

## Reading the results

- `source_*` columns identify the sentence and document used to start the
  lookup.
- `target_*` columns identify the matched sentence and its document.
- `distance=0.0` means an exact vector match.
- A smaller distance means a stronger match.
- `match_percentage` is the readable percentage calculated from distance.
- The distinct queries include the source documents and their matched target
  documents, but each `tsp_id` appears only once.
