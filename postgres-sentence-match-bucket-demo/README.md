# PostgreSQL Sentence Match POC

This small POC tests bidirectional sentence-match writes through PostgreSQL
functions. SQLAlchemy is used only to connect to PostgreSQL and call a
function. Python does not build or execute direct match-table upserts.

All POC tables and functions are stored in the `test_matches` schema. Each SQL
file creates the schema when it is missing.

## Start

```bash
make dev
```

Open Swagger at http://localhost:8010/docs.

The API contains:

- `GET /health`
- `POST /init`
- `POST /matches/sql-function`
- `POST /list-matches/sql-function`

`POST /init` creates the schema objects, optionally clears the POC tables, and
returns example requests. The insert endpoints only call a PostgreSQL function,
commit, and return the number of processed relationships. They do not read the
saved rows back.

## SQL files

- `001_sentence_match_buckets.sql` stores SHA-256 sentence-key matches in 32
  buckets.
- `002_sentence_match_lists.sql` stores production-shaped JSONB arrays.
- `003_sentence_matches_object.sql` stores match metadata in a JSONB object.
- `004_titan_key_matches.sql` stores `{global_id: distance}` in one owner row.
- `005_titan_key_match_buckets.sql` stores `{global_id: distance}` across 32
  smaller owner rows.

## Performance scripts

Every performance script calls a SQL function. The scripts log progress and
elapsed time but do not query the inserted rows for validation.

Run the non-bucket key-value test:

```bash
uv run python titan_key_matches_performance_loader.py
```

Run the bucketed key-value test:

```bash
uv run python titan_key_match_buckets_performance_loader.py
```

Watch PostgreSQL resources:

```bash
make stats
```

## Commands

```bash
make dev       # Run PostgreSQL and the API
make postgres  # Run only PostgreSQL
make local     # Run the API locally
make test      # Run tests
make psql      # Open a PostgreSQL shell
make stats     # Watch PostgreSQL CPU and memory
make down      # Stop containers and keep database data
```
