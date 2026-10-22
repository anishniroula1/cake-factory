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

## Reading the results

- `source_*` columns identify the sentence and document used to start the
  lookup.
- `target_*` columns identify the matched sentence and its document.
- `distance=0.0` means an exact vector match.
- A smaller distance means a stronger match.
- `match_percentage` is the readable percentage calculated from distance.
- The distinct queries include the source documents and their matched target
  documents, but each `tsp_id` appears only once.
