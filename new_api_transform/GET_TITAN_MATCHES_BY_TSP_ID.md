# Get Titan Matches by `tsp_id`

This query returns one row for a document. It combines all 16 Titan match
buckets and includes the source sentence, target sentence, distance, and total
counts.

Replace `12345` with the required `tsp_id`.

```sql
WITH requested_document AS (
    SELECT 12345::BIGINT AS tsp_id
)
SELECT
    requested_document.tsp_id,
    COUNT(DISTINCT titan_match.global_id)
        FILTER (WHERE match_item.key IS NOT NULL)
        AS matched_source_sentence_count,
    COUNT(match_item.key) AS relationship_count,
    COALESCE(
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'source_global_id', titan_match.global_id,
                'source_sentence', source_sentence.sentence_content,
                'source_application_id', titan_match.application_id,
                'target_global_id', match_item.key::INTEGER,
                'target_tsp_id', target_sentence.tsp_id,
                'target_sentence', target_sentence.sentence_content,
                'distance',
                    (match_item.value #>> '{}')::DOUBLE PRECISION
            )
            ORDER BY
                titan_match.global_id,
                (match_item.value #>> '{}')::DOUBLE PRECISION,
                match_item.key::INTEGER
        ) FILTER (WHERE match_item.key IS NOT NULL),
        '[]'::JSONB
    ) AS matches
FROM requested_document
LEFT JOIN tsp.titan_matches AS titan_match
    ON titan_match.tsp_id = requested_document.tsp_id
LEFT JOIN tsp.sentences_vector AS source_sentence
    ON source_sentence.global_id = titan_match.global_id
LEFT JOIN LATERAL JSONB_EACH(titan_match.matches)
    AS match_item(key, value)
    ON TRUE
LEFT JOIN tsp.sentences_vector AS target_sentence
    ON target_sentence.global_id = match_item.key::INTEGER
GROUP BY requested_document.tsp_id;
```

## Output

- `tsp_id`: The requested document ID.
- `matched_source_sentence_count`: Number of source sentences that have at
  least one match.
- `relationship_count`: Total number of source-to-target relationships.
- `matches`: One JSON array containing every relationship, ordered by source
  `global_id` and then by the smallest distance.

If the document has no Titan matches, the query still returns one row with
zero counts and `matches = []`.
