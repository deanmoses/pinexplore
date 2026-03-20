# Pinexplore

This project is a tool for exploring and validating pinball catalog data. 

It builds a read-only DuckDB database from [Pindata](https://github.com/deanmoses/pindata) catalog records and external source dumps like OPDB and IPDB, then runs integrity checks, cross-source comparisons, and gap analysis.

It's meant as both an engine for helping clean this data as well as an ad-hoc analysis tool.

## Prerequisites

- Python 3
- [uv](https://docs.astral.sh/uv/) (manages the venv and dependencies)
- Node.js (for pre-commit hooks)

## Quick Start

```bash
cp .env.example .env     # configure R2_PUBLIC_URL for remote data access
make pull                 # download source data from Cloudflare R2
make explore              # build explore.duckdb from SQL layers
```

Or do both in one step:

```bash
make all
```

## Querying the Database

```python
import duckdb
con = duckdb.connect("explore.duckdb", read_only=True)
con.execute("FROM machines LIMIT 5").show()
```

... or (even easier) use a locally installed DuckDB CLI.

## Development Commands

| Command        | Description                                |
| -------------- | ------------------------------------------ |
| `make all`     | Pull ingest sources + rebuild explore DB   |
| `make pull`    | Download ingest sources from Cloudflare R2 |
| `make push`    | Upload ingest sources to Cloudflare R2     |
| `make explore` | Rebuild explore.duckdb from SQL layers     |
| `make clean`   | Remove DuckDB build artifacts              |
