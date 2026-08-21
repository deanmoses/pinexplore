"""Tests for rebuild_explore's empty-schema stand-ins for the web cache tables.

``03_raw_web.sql`` is skipped in ``--remote`` mode and on a checkout with no
local cache, so the build creates empty ``web_cache.pages`` / ``.fetches`` instead.
Those stubs are a hand-written copy of the SQLite schema, which means they drift
silently: a column added to ``web_cache._SCHEMA`` is simply absent in those build
modes, and a query over it fails only there. This compares the two directly.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import duckdb
import pytest
import web_cache

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_rebuild_explore():
    """Import scripts/rebuild_explore.py (not on sys.path, and not a package)."""
    path = REPO_ROOT / "scripts" / "rebuild_explore.py"
    spec = importlib.util.spec_from_file_location("rebuild_explore", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sqlite_columns(table: str) -> list[str]:
    con = sqlite3.connect(":memory:")
    try:
        web_cache.init_schema(con)
        return [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
    finally:
        con.close()


def _stub_columns(table: str) -> list[str]:
    con = duckdb.connect(":memory:")
    try:
        # The real build creates the schemas in 01_schemas.sql, before the stub
        # ever runs; here the stub is executed on its own.
        con.execute("CREATE SCHEMA IF NOT EXISTS web_cache")
        con.execute(_load_rebuild_explore().WEB_STUB_SQL)
        return [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    finally:
        con.close()


@pytest.mark.parametrize(
    ("stub_table", "sqlite_table"),
    [("web_cache.pages", "pages"), ("web_cache.fetches", "fetches")],
)
def test_stub_schema_matches_the_sqlite_cache(stub_table, sqlite_table):
    # Same columns, same order: the populated path is `SELECT *` off the SQLite
    # table, so a consumer must see one shape whichever mode built the DB.
    assert _stub_columns(stub_table) == _sqlite_columns(sqlite_table)


def test_legacy_cache_is_migrated_before_it_is_attached(tmp_path, monkeypatch):
    # 03_raw_web.sql ATTACHes the cache READ_ONLY and materializes SELECT *, so a
    # cache written before these columns existed produces web_cache.pages and
    # .fetches that simply lack them — and the documented `WHERE text_source = 'ocr'`
    # query fails until some unrelated fetch happens to migrate the file. That's
    # the shape `make pull` hands you until a migrated cache is pushed.
    web_dir = tmp_path / "web"
    web_dir.mkdir(parents=True)
    monkeypatch.setattr(web_cache, "WEB_DIR", web_dir)
    monkeypatch.setattr(web_cache, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(web_cache, "RAW_DIR", web_dir / "raw")
    con = sqlite3.connect(web_cache.DB_PATH)
    con.executescript(
        """
        CREATE TABLE pages (
          url TEXT PRIMARY KEY, raw_url TEXT, content_sha TEXT NOT NULL,
          first_fetched_at TEXT NOT NULL, last_fetched_at TEXT NOT NULL,
          last_updated TEXT, title TEXT, http_status INTEGER, content_type TEXT,
          text TEXT, rendered INTEGER
        );
        CREATE TABLE fetches (
          id INTEGER PRIMARY KEY, url TEXT NOT NULL, fetched_at TEXT NOT NULL,
          search_query TEXT, http_status INTEGER, content_sha TEXT, changed INTEGER
        );
        """
    )
    con.commit()
    con.close()

    _load_rebuild_explore().migrate_web_cache()

    con = sqlite3.connect(web_cache.DB_PATH)
    try:
        pages = {r[1] for r in con.execute("PRAGMA table_info(pages)")}
        fetches = {r[1] for r in con.execute("PRAGMA table_info(fetches)")}
    finally:
        con.close()
    assert {"text_source", "imported"} <= pages
    assert "imported" in fetches
    assert "text_sha" not in fetches  # retired; migration must not resurrect it


def test_migrate_is_a_no_op_without_a_cache(tmp_path, monkeypatch):
    # A fresh checkout has no cache; the build stubs the tables instead. Migration
    # must not conjure a database file as a side effect of looking for one.
    monkeypatch.setattr(web_cache, "DB_PATH", tmp_path / "absent.sqlite")
    monkeypatch.setattr(web_cache, "RAW_DIR", tmp_path / "raw")
    _load_rebuild_explore().migrate_web_cache()
    assert not (tmp_path / "absent.sqlite").exists()
