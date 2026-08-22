# DuckDB Explore Database

A read-only DuckDB database over dumps from external sources: IPDB, OPDB, Fandom, this project's web cache system, pinball glossaries.

It holds no Flipcommons catalog data and reads none. Comparing this external data against the Flipcommons database is reconciliation, which is the job of Flippatch's external data sources layer.

## Using it

### Query it

Query it via python or the DuckDB CLI:

```python
import duckdb
con = duckdb.connect("explore.duckdb", read_only=True)
con.execute("FROM ipdb.models LIMIT 5").show()
```

Don't use MotherDuck. This is a local DB, it's not on MotherDuck.

### Building it

```bash
make explore   # rebuild from SQL layers
```

The database is a build artifact (gitignored). Rebuild whenever the source dumps change or the SQL changes. The build **fails** if integrity checks don't pass. `checks.violations` is a real table and the rows survive the abort, so a failed build can be queried afterwards.

## SQL layers

Files in `sql/`. They load in numeric order.

## Schemas

`main` is deliberately empty; every relation lives in a schema that says which layer it belongs to. Per external source:

| schema         | what it holds                                                           | read it? |
| -------------- | ----------------------------------------------------------------------- | -------- |
| `<source>_raw` | reads of source **files**. If the `FROM` names a relation, it isn't raw | internal |
| `<source>_stg` | parsing, merging and correcting the dump                                | internal |
| `<source>_ref` | hand-curated lookups and exception lists                                | internal |
| `<source>`     | the published mart — that source, in our vocabulary                     | **yes**  |

Only the unsuffixed mart is a contract. Not every source has every schema. Fandom, for example, doesn't have a mart because we're not using for anything right now.

Other schemas:

- `web_cache`: the scrape cache, materialized
- `ingest`: one row per ingested artifact, any source
- `glossary`: the three pinball glossaries and their comparison
- `checks`: the build's own internal verdicts

## Related scripts

- `scripts/rebuild_explore.py` — build `explore.duckdb` from the SQL layers
- `scripts/cloud_store/{pull,push}_ingest_sources.py` — sync ingest sources with R2
- `scripts/web_scrape/web_fetch.py` + `web_cache.py` — fetch and query the web evidence cache (see [WebCache.md](WebCache.md))
- `scripts/glossary/parse_*_glossary.py` — parse saved glossary HTML dumps into JSON

## Cloudflare R2

Ingest source files are stored in Cloudflare R2 for sync'ing across developer machines.

```bash
make pull   # download R2 → local ingest_sources/
make push   # upload local ingest_sources/ → R2 (requires credentials)
```

AI sessions are NOT allowed to do this; these are human-only operations.

### Rebuilding from R2

```bash
uv run python scripts/rebuild_explore.py --remote   # reads JSON from R2 instead of local files
```

AI sessions are NOT allowed to do this; these are human-only operations.
