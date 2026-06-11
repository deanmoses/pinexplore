.PHONY: all pull push explore clean agent-docs test lint typecheck sql-check check

all: pull explore

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy .

sql-check:
	uv run python scripts/check_sql.py

check: lint typecheck sql-check test

pull:
	uv run python scripts/cloud_store/pull_ingest_sources.py

push:
	uv run python scripts/cloud_store/push_ingest_sources.py

explore:
	uv run python scripts/rebuild_explore.py

clean:
	rm -f explore.duckdb explore.duckdb.wal

agent-docs:
	uv run python scripts/agent_docs/build_agent_docs.py
