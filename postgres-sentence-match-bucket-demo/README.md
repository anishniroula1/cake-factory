# PostgreSQL Sentence Match Bucket Demo

This small standalone application compares two ways to save bidirectional
sentence-key matches:

1. A PostgreSQL function owns the bucket calculation and inserts.
2. Python calculates the buckets and SQLAlchemy performs a bulk upsert with
   bind parameters.

There is no fake-sentences table. The fake generator only returns SHA-256 keys
and distances that can be copied into either insert endpoint.

## Start it

```bash
make dev
```

Open Swagger:

http://localhost:8010/docs

## Swagger flow

### 1. Check the connection

Call `GET /health` when you want to check PostgreSQL. Docker does not repeatedly
call this API endpoint.

### 2. Initialize and generate a fake request

Call `POST /init`:

```json
{
  "analysisGroup": "Asylee",
  "matchCount": 10,
  "randomSeed": 42,
  "clearExisting": true
}
```

This creates the table and functions, optionally clears existing bucket rows,
and returns an `insertRequest`. It does not insert the fake matches.

### 3. Test the PostgreSQL function

Copy `insertRequest` into `POST /matches/sql-function`.

PostgreSQL calculates the forward and reverse bucket numbers, locks the affected
rows in a stable order, and saves both directions in one transaction.

### 4. Test the Python and SQLAlchemy approach

Call `POST /init` again with `clearExisting` set to true. Copy the new
`insertRequest` into `POST /matches/python`.

Python calculates the forward and reverse bucket rows. SQLAlchemy sends all rows
through one reusable bind-parameter upsert statement.

The important part is:

```python
baseInsert = insert(sentenceKeyMatches).values(
    analysis_group=bindparam("analysisGroup"),
    sentence_key=bindparam("sentenceKey"),
    match_bucket_number=bindparam("matchBucketNumber"),
    matches=bindparam("incomingMatches", type_=JSONB),
)

statement = baseInsert.on_conflict_do_update(
    index_elements=[
        sentenceKeyMatches.c.analysis_group,
        sentenceKeyMatches.c.sentence_key,
        sentenceKeyMatches.c.match_bucket_number,
    ],
    set_={
        "matches": case(
            (
                sentenceKeyMatches.c.matches.contains(
                    baseInsert.excluded.matches
                ),
                sentenceKeyMatches.c.matches,
            ),
            else_=(
                sentenceKeyMatches.c.matches
                .op("||")(baseInsert.excluded.matches)
            ),
        ),
        "updated_at": func.now(),
    },
)

session.execute(statement, bucketRows)
session.commit()
```

This is better than an update-only `case` statement for the bucket table because
a bucket row may not exist yet. `INSERT ... ON CONFLICT DO UPDATE` handles both
cases:

- it inserts a missing bucket row;
- it merges into an existing bucket row;
- the `case` keeps the JSONB unchanged when the same keys and distances are
  already present;
- the JSONB object replaces the old distance for the same matching key instead
  of creating a duplicate.

Both insert endpoints return `sourceMatches` and `reverseMatches`, so the full
bidirectional result is visible in the same Swagger response.

## Files

- `db/init/001_sentence_match_buckets.sql` contains the PostgreSQL functions and
  bucket table.
- `bucket_demo/sql_function_service.py` contains the complete PostgreSQL-function
  endpoint logic.
- `bucket_demo/python_service.py` contains the complete Python and SQLAlchemy
  endpoint logic.
- The two service files do not call shared match helpers. Each flow owns its
  validation, insert, forward read, and reverse read.
- `bucket_demo/routes.py` contains the four API calls.
- `generate_project.py` recreates this complete standalone folder.

## Commands

```bash
make dev       # Run PostgreSQL and the API
make postgres  # Run only PostgreSQL
make local     # Run the API locally
make test      # Run tests
make psql      # Open a PostgreSQL shell
make down      # Stop containers and keep database data
```

## Generate another standalone copy

```bash
python3 generate_project.py /path/where/you/want/the/project
```
