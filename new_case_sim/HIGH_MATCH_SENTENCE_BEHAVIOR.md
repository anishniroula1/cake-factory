# High-match sentence behavior

## Purpose

This note explains what the existing Case Similarity service does when one
sentence has many matches, why the 500-match rule probably exists, and how the
new Titan flow currently behaves.

## Existing Case Similarity behavior

The existing service first removes sentences marked as:

- non-matcher
- tracer
- form language
- not current

It then reads the existing `matches` row for every remaining source
`global_id`. The row is used only when:

```text
match_count > 0 and match_count < 500
```

This means a sentence with 499 matches is included. A sentence with exactly
500 matches, or more than 500 matches, is completely ignored by Case
Similarity.

The entire application is not ignored. Only the high-match sentence is
removed from the case calculation. Other sentences from the same application
can still make the application similar to another application.

### Existing-flow example

Application 100 contains these sentences:

| Global ID | Match count | Existing service behavior |
|---:|---:|---|
| 10 | 600 | Ignore this sentence |
| 11 | 20 | Use this sentence |
| 12 | 15 | Use this sentence |

Application 100 is still processed using global IDs 11 and 12. If those
sentences provide enough qualifying matches with Application 200, Application
200 can still be saved as a similar or TAH case.

If global ID 10 was the only connection between Application 100 and
Application 200, that case relationship is not saved by the existing service.

## Why the existing service probably has this rule

The existing code does not document the original reason. The most likely
reason is to stop very common sentences from creating false case matches.

A sentence can have more than 500 matches when:

- a tracer or form-language sentence was not classified correctly;
- the same sentence appears in hundreds or thousands of documents;
- common text has many small wording differences;
- the similarity threshold returns many broad matches;
- one matching catalog key expands into many occurrence `global_id` values;
- documents contain repeated or duplicate sentences.

For example, one unique catalog sentence may match only a few unique texts,
but those texts may occur in 5000 documents. Expanding those catalog results
into sentence occurrences can create thousands of relationships.

The 500-match rule is therefore a second safety check. It protects Case
Similarity when tracer, form-language, or non-matcher classification misses a
common sentence.

A high match count does not automatically prove that a sentence is a
non-matcher. It may be valid language that happens to be common.

## New Titan behavior

The new flow classifies tracer, form-language, and non-matcher sentences in
Sentence Similarity before saving case relationships. Qualifying
relationships are saved bidirectionally in `tsp.titan_matches`.

The new `tsp.refresh_titan_similar_cases` function currently has no 500-match
limit. It expands the saved JSONB buckets and counts every unique
source/target `global_id` pair.

For example, if source global ID 10 has 600 saved target relationships:

- the existing service ignores global ID 10;
- the new Titan service counts all 600 unique sentence relationships;
- those relationships can place target applications in
  `similar_application_ids`, `similar_case_ids`, or `tah_case_ids`.

This follows the new counting rule: if one source sentence matches three
different sentences in another application, it contributes three sentence
matches.

## Bucket match counts

One sentence can have up to 16 rows in `tsp.titan_matches`. Each row stores one
bucket and has its own generated `match_count`. The complete match count for a
sentence is the sum of `match_count` across all of its bucket rows.

The following query finds sentences with at least 500 saved relationships:

```sql
SELECT
    global_id,
    application_id,
    SUM(match_count) AS total_matches
FROM tsp.titan_matches
GROUP BY global_id, application_id
HAVING SUM(match_count) >= 500
ORDER BY total_matches DESC;
```

## What should be checked before adding the limit

The current new implementation counts all saved relationships. Before copying
the old 500-match rule, we should inspect high-match sentences and answer:

1. Are most of them missed tracer, form-language, or non-matcher sentences?
2. Are they valid sentences repeated across many documents?
3. Are high counts caused by duplicate occurrence records?
4. How much do these sentences change similar-case and TAH results?

If the high-match sentences are mostly noise, the preferred solution is:

1. Improve the tracer/non-matcher catalog so they are stopped earlier.
2. Keep a maximum-match safety rule for sentences the classifier misses.
3. Apply the same eligibility rule to both sides of a relationship so the
   case result remains bidirectional.

Do not automatically mark every sentence with 500 matches as a non-matcher.
The count should be treated as a warning and safety condition, not a final
classification by itself.

## Behavior summary

| Flow | Sentence with 500 or more matches |
|---|---|
| Existing Case Similarity | Ignores that sentence but continues processing the application |
| New Titan Case Similarity | Counts every saved unique sentence relationship |
| Possible future safety rule | Excludes high-match sentences consistently on both relationship sides |
