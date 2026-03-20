.PHONY: all pull push explore clean agent-docs

all: pull explore

pull:
	uv run python scripts/pull_ingest_sources.py

push:
	uv run python scripts/push_ingest_sources.py

explore:
	uv run python scripts/rebuild_explore.py

clean:
	rm -f explore.duckdb explore.duckdb.wal

agent-docs:
	uv run python scripts/build_agent_docs.py
