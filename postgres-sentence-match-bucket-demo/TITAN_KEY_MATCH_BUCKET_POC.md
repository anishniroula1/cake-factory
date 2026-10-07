# Titan Match Storage POC

## Purpose

This POC compares three ways to store bidirectional sentence matches in
PostgreSQL. Each Python file creates test data, calls its PostgreSQL function,
and records the time needed to save the data.

All three designs automatically save both directions. If sentence 100 matches
sentence 200, sentence 100 stores 200 and sentence 200 stores 100.

## Files

| File | What it tests |
|---|---|
| `titan_matches_performance_loader.py` | Stores every match as a full object inside a JSONB list |
| `titan_key_matches_performance_loader.py` | Stores one JSONB object using `global_id` as the key and distance as the value |
| `titan_key_match_buckets_performance_loader.py` | Stores the same key-value data but splits each sentence across 32 smaller bucket rows |

Each loader has constants at the top for match count, batch size, worker count,
and test IDs. Each loader uses values from `.env` to connect to PostgreSQL.

## 1. JSONB List

File: `titan_matches_performance_loader.py`

Table: `test_matches.titan_matches`

Every match repeats its global ID, document ID, A-number, and distance.

| global_id | tsp_id | matches | match_count | verbatim |
|---:|---:|---|---:|---|
| 100 | 10 | `[{"global_id":200,"tsp_id":20,"a_number":9002,"distance":0.10}]` | 1 | `{}` |
| 200 | 20 | `[{"global_id":100,"tsp_id":10,"a_number":9001,"distance":0.10}]` | 1 | `{}` |

This is close to the existing production match format, but the repeated fields
make the JSONB value larger.

## 2. JSONB Key and Distance

File: `titan_key_matches_performance_loader.py`

Table: `test_matches.titan_key_matches`

The owner row stores its TSP and application once. The `matches` object stores
only matching global IDs and distances.

| global_id | tsp_id | application_id | matches | matches_count | verbatim |
|---:|---:|---:|---|---:|---|
| 100 | 10 | 1 | `{"200":0.10,"232":0.0}` | 2 | `{232}` |
| 200 | 20 | 2 | `{"100":0.10}` | 1 | `{}` |
| 232 | 22 | 3 | `{"100":0.0}` | 1 | `{100}` |

This uses less space than the list because match metadata is not repeated.
An incoming value for an existing key replaces its saved distance. Matches
from the same application are ignored.

## 3. Bucketed JSONB Key and Distance

File: `titan_key_match_buckets_performance_loader.py`

Table: `test_matches.titan_key_match_buckets`

This stores the same small key-value matches, but one global ID can have up to
32 rows.

| global_id | tsp_id | application_id | bucket | matches | count | verbatim |
|---:|---:|---:|---:|---|---:|---|
| 100 | 10 | 1 | 8 | `{"200":0.10,"232":0.0}` | 2 | `{232}` |
| 200 | 20 | 2 | 4 | `{"100":0.10}` | 1 | `{}` |
| 232 | 22 | 3 | 4 | `{"100":0.0}` | 1 | `{100}` |

## How the Bucket Works

The matching global ID chooses the bucket:

```text
bucket = matching global ID % 32

200 % 32 = 8
232 % 32 = 8
100 % 32 = 4
```

The forward matches from sentence 100 go into bucket 8. The reverse match
stored by sentences 200 and 232 points to global ID 100, so it goes into
bucket 4.

PostgreSQL creates only the bucket rows that are needed. It does not create 32
empty rows.

Without buckets, changing one match can rewrite one large JSONB value. With 32
buckets, 3,000 matches are spread across about 32 rows, or roughly 94 matches
per row. This makes each update smaller and lets workers update different
buckets without locking the same row.

The tradeoff is more rows. To get the total for one global ID, add the counts
from all its bucket rows.

## Run

```bash
uv run python titan_matches_performance_loader.py
uv run python titan_key_matches_performance_loader.py
uv run python titan_key_match_buckets_performance_loader.py
```

## Performance Report

