"""Shared test fixtures for the web evidence cache.

These are the project's first tests. They cover the web-scrape cache
(scripts/web_scrape/), the most logic-heavy Python in the repo. Everything here
runs fully offline: pure functions, a throwaway SQLite under tmp_path, and a
monkeypatched ``_http_get`` — no network.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator

# The web-cache modules are scripts run from a flat dir; import them the same way
# the fetcher and the cross-repo patch authors do (add the dir to sys.path).
SCRIPTS_WEB = Path(__file__).resolve().parent.parent / "scripts" / "web_scrape"
sys.path.insert(0, str(SCRIPTS_WEB))

import web_cache  # noqa: E402


@pytest.fixture
def cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[sqlite3.Connection]:
    """Point the cache at a fresh tmp dir; return an initialized write connection.

    Monkeypatches web_cache's path globals so connect()/get()/html_path() all
    resolve under tmp_path — every test gets an isolated DB + html blob dir.
    """
    web_dir = tmp_path / "web"
    monkeypatch.setattr(web_cache, "WEB_DIR", web_dir)
    monkeypatch.setattr(web_cache, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(web_cache, "HTML_DIR", web_dir / "html")
    con = web_cache.connect()
    web_cache.init_schema(con)
    yield con
    con.close()
