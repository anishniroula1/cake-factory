# PostgreSQL Sentence Match Bucket POC

## Use case

Every sentence keeps its matching sentence keys and distances. Matches must be
bidirectional.

```text
A matches B at 0.10

A stores {B: 0.10}
B stores {A: 0.10}
```

A popular sentence could eventually have 10,000 or even 100,000 matching keys.

## Problem with one JSONB row

Without buckets, all matches for sentence A live in one growing JSONB value:

```text
Asylee | A | {B: 0.10, C: 0.20, D: 0.30, ...10,000 keys}
```

Adding one new match does not change only a small part of that JSONB. PostgreSQL
creates a new version of the entire JSONB value. This produces more work, WAL
(Write-Ahead Log), dead rows, and vacuum work.

The row is also locked during the update. If two workers add different matches
to A, one worker must wait because both are changing the same row.

Bidirectional writes make this more noticeable. When A matches Z, we update A
with Z and Z with A. If A has 10,000 matches and Z already has 1,000 matches,
both large JSONB values may need to be rewritten just to add one key.

## How PostgreSQL TOAST helps

PostgreSQL uses TOAST when a JSONB value becomes too large for the normal table
row. TOAST compresses the value and can store it outside the main table page.
This allows PostgreSQL to store a large JSONB value without breaking the row-size
limit.

TOAST helps with storage, but it does not make a large JSONB value cheap to
update. PostgreSQL still has to read, change, and write a new version of that
large value. A very large value also creates larger TOAST updates and more WAL.

## Bucket solution

Split each sentence's matches across 32 rows. The matching key decides the
bucket, so the same key always goes to the same place.

```python
bucketNumber = int(matchingSentenceKey[:2], 16) % 32
```

Example table:

| analysisGroup | sentenceKey | bucketNumber | matches |
|---|---|---:|---|
| Asylee | A | 3 | `{B: 0.10, D: 0.30}` |
| Asylee | A | 11 | `{C: 0.20}` |
| Asylee | A | 25 | `{Z: 0.15}` |
| Asylee | B | 10 | `{A: 0.10}` |
| Asylee | Z | 10 | `{A: 0.15}` |

Sentence A has multiple rows, but each row contains a smaller JSONB object.
Empty bucket rows are not created.

## What changes when a new match is added

If A matches Z, calculate bucket(Z) and update only that one A bucket. For the
reverse relationship, calculate bucket(A) and update only that one Z bucket.
The other bucket rows for A and Z are not touched or locked.

With evenly distributed SHA-256 keys:

| Matches for one sentence | One JSONB row | Average with 32 buckets |
|---:|---:|---:|
| 1,000 | 1,000 keys | about 31 keys per bucket |
| 10,000 | 10,000 keys | about 313 keys per bucket |
| 100,000 | 100,000 keys | about 3,125 keys per bucket |

Some bucket values may still use TOAST, especially near 100,000 matches, but
PostgreSQL rewrites only one smaller bucket instead of one JSONB containing every
match for that sentence.

## Main benefits

- A new relationship updates two small bucket rows: one forward and one reverse.
- Workers changing different buckets for the same sentence can continue separately.
- Row locks cover only the bucket being changed, not every match for the sentence.
- TOAST values, WAL records, dead rows, and vacuum work are smaller per update.
- The primary key prevents duplicate bucket rows for the same sentence.
- JSONB prevents duplicate matching keys inside a bucket and replaces old distance.
- All buckets for one sentence are still easy to fetch by analysis group and key.

This design does create more rows, but those smaller rows make frequent
bidirectional updates safer as match lists grow.
