# PostgreSQL Sentence Match POC

This standalone application compares two storage designs and two insert methods:

1. Bucketed JSONB objects keyed by SHA-256 sentence key.
2. Production-style JSONB lists keyed by numeric global ID.
3. PostgreSQL functions that own the complete bidirectional write.
4. Python and SQLAlchemy `bindparam` writes.

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

This creates both tables and functions, optionally clears existing rows, and
returns two payloads. `insertRequest` is for the bucket endpoints.
`listInsertRequest` is for the production-style JSONB-list endpoints.

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

## Production-style JSONB list comparison

The second table copies the existing shape:

```text
global_id + analysis_group + tsp_id + matches JSONB list
```

Each list item contains `global_id`, `distance`, `tsp_id`, and `application_id`.
Copy `listInsertRequest` from `/init` into one of these endpoints:

- `POST /list-matches/sql-function`
- `POST /list-matches/python`

The SQL function filters duplicate global IDs inside PostgreSQL. The Python
version groups the forward and reverse relationships into one row per owner,
then uses one bulk `INSERT ... ON CONFLICT DO UPDATE`. A new global ID is
appended. An existing global ID is replaced, so its latest distance and other
values are saved in both directions. Neither endpoint reads the full saved list
into Python before saving.

## Files

- `db/init/001_sentence_match_buckets.sql` contains the PostgreSQL functions and
  bucket table.
- `db/init/002_sentence_match_lists.sql` contains the production-style JSONB
  list table and function.
- `bucket_demo/sql_function_service.py` contains the complete PostgreSQL-function
  endpoint logic.
- `bucket_demo/python_service.py` contains the complete Python and SQLAlchemy
  endpoint logic.
- The two service files do not call shared match helpers. Each flow owns its
  validation, insert, forward read, and reverse read.
- `bucket_demo/list_sql_function_service.py` owns the complete JSONB-list SQL
  function endpoint.
- `bucket_demo/list_python_service.py` owns the complete JSONB-list Python
  endpoint.
- `bucket_demo/routes.py` contains the six API calls.
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
