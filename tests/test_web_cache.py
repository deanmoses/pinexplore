"""Tests for web_cache: URL normalization, hashing, FTS, and the versioned store."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import web_cache as wc

if TYPE_CHECKING:
    import sqlite3

# --------------------------------------------------------------------------- #
# normalize_url (pure)
# --------------------------------------------------------------------------- #


def test_normalize_strips_tracking_but_keeps_ref():
    # utm_* dropped, fragment dropped, params sorted; bare `ref` is content-bearing
    # and deliberately kept.
    assert (
        wc.normalize_url("https://e.com/p?utm_source=x&ref=main&b=2#frag")
        == "https://e.com/p?b=2&ref=main"
    )


def test_normalize_schemeless_assumes_https():
    assert wc.normalize_url("example.com/foo") == "https://example.com/foo"
    assert wc.normalize_url("www.site.com") == "https://www.site.com/"


def test_normalize_dedups_bare_and_explicit_https():
    assert wc.normalize_url("example.com/foo") == wc.normalize_url(
        "https://example.com/foo"
    )


def test_normalize_host_port_slash_fragment():
    # scheme+host lowercased, default port dropped, non-root trailing slash dropped,
    # fragment removed, content-bearing query kept.
    assert (
        wc.normalize_url("HTTP://Example.com:80/a/?x=1#frag")
        == "http://example.com/a?x=1"
    )


def test_normalize_root_keeps_slash():
    assert wc.normalize_url("https://example.com") == "https://example.com/"


# --------------------------------------------------------------------------- #
# content_sha / _fts_query (pure)
# --------------------------------------------------------------------------- #


def test_content_sha_matches_sha256_and_is_content_addressed():
    assert wc.content_sha(b"abc") == hashlib.sha256(b"abc").hexdigest()
    assert wc.content_sha(b"abc") != wc.content_sha(b"abd")


def test_fts_query_quotes_each_token_and_escapes_quotes():
    assert wc._fts_query("foo bar") == '"foo" "bar"'
    assert wc._fts_query('a"b') == '"a""b"'


# --------------------------------------------------------------------------- #
# store: upsert / get / search / quote
# --------------------------------------------------------------------------- #


def _seed(
    con: sqlite3.Connection,
    *,
    url: str,
    raw_url: str | None = None,
    text: str | None = None,
    title: str | None = None,
    content: str | bytes = "x",
) -> str:
    sha = wc.content_sha(content.encode() if isinstance(content, str) else content)
    wc.upsert_page(
        con,
        url=url,
        raw_url=raw_url or url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        title=title,
        text=text,
    )
    return sha


def _page(con: sqlite3.Connection, url: str) -> wc.PageRow:
    """Fetch a page row the test expects to exist, narrowing away None."""
    row = wc.get(url, con=con)
    assert row is not None, f"expected a stored page for {url}"
    return row


def test_get_search_quote_roundtrip(cache):
    url = wc.normalize_url("https://haggis.com/about")
    _seed(
        cache,
        url=url,
        title="Haggis Pinball",
        text="Haggis Pinball closed in 2024. It was Australian.",
    )
    assert _page(cache, url)["title"] == "Haggis Pinball"
    hits = wc.search("haggis", con=cache)
    assert [h["url"] for h in hits] == [url]
    assert wc.quote(url, "2024", con=cache) == ["Haggis Pinball closed in 2024."]


def test_get_normalizes_lookup(cache):
    url = wc.normalize_url("https://haggis.com/about")
    _seed(cache, url=url)
    # trailing slash + scheme-less should resolve to the same row
    assert _page(cache, "haggis.com/about")["url"] == url


def test_get_by_raw_url_finds_redirect_origin(cache):
    final = wc.normalize_url("https://site.com/x")
    _seed(cache, url=final, raw_url="http://site.com/x")
    origin = wc.get_by_raw_url("http://site.com/x", con=cache)
    assert origin is not None
    assert origin["url"] == final
    assert wc.get_by_raw_url("http://absent.com", con=cache) is None


# --------------------------------------------------------------------------- #
# upsert conflict behavior
# --------------------------------------------------------------------------- #


def test_upsert_preserves_first_fetched_on_conflict(cache):
    url = wc.normalize_url("https://a.com/")
    _seed(cache, url=url, content="v1")
    first = _page(cache, url)["first_fetched_at"]
    sha2 = _seed(
        cache, url=url, content="v2"
    )  # refetch points the row at the new version
    row = _page(cache, url)
    assert row["first_fetched_at"] == first
    assert row["content_sha"] == sha2


# --------------------------------------------------------------------------- #
# blob path helpers — extension parameter (HTML default; PDFs later)
# --------------------------------------------------------------------------- #


def test_blob_path_defaults_to_html():
    assert wc.blob_path("abc").name == "abc.html"
    assert wc.blob_path("abc").parent == wc.RAW_DIR


def test_blob_path_accepts_extension():
    assert wc.blob_path("abc", ext="pdf").name == "abc.pdf"


# --------------------------------------------------------------------------- #
# rendered provenance flag — storage + migration
# --------------------------------------------------------------------------- #


def test_rendered_flag_stored_on_page_and_fetch(cache):
    url = wc.normalize_url("https://spa.com/x")
    sha = wc.content_sha(b"x")
    wc.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        rendered=True,
    )
    assert _page(cache, url)["rendered"] == 1
    wc.append_fetch(
        cache,
        url=url,
        fetched_at=wc.now_iso(),
        search_query=None,
        http_status=200,
        content_sha=sha,
        changed=True,
        rendered=True,
    )
    assert cache.execute("SELECT rendered FROM fetches").fetchone()[0] == 1


def test_rendered_defaults_to_null_when_omitted(cache):
    url = wc.normalize_url("https://plain.com/x")
    _seed(cache, url=url)  # _seed never passes rendered
    assert _page(cache, url)["rendered"] is None


def test_init_schema_migrates_legacy_cache(tmp_path, monkeypatch):
    # A cache.sqlite from before `rendered` existed and while a blob path was still
    # stored in `html_file`: init_schema must ALTER `rendered` onto the existing
    # tables (not just CREATE-IF-NOT-EXISTS around them) and DROP the obsolete
    # `html_file` column — the extension now derives from content_type.
    web_dir = tmp_path / "web"
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    con = wc.connect()
    con.executescript(
        """
        CREATE TABLE pages (
          url TEXT PRIMARY KEY, raw_url TEXT, content_sha TEXT NOT NULL,
          first_fetched_at TEXT NOT NULL, last_fetched_at TEXT NOT NULL,
          last_updated TEXT, title TEXT, http_status INTEGER, content_type TEXT,
          html_file TEXT NOT NULL, text TEXT
        );
        CREATE TABLE fetches (
          id INTEGER PRIMARY KEY, url TEXT NOT NULL, fetched_at TEXT NOT NULL,
          search_query TEXT, http_status INTEGER, content_sha TEXT, changed INTEGER
        );
        """
    )
    con.execute(
        "INSERT INTO pages (url, content_sha, first_fetched_at, last_fetched_at, "
        "content_type, html_file, text) VALUES "
        "('https://x.com/p', 'abc', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', "
        "'text/html', 'html/abc.html', 'hi')"
    )
    con.commit()

    def _cols(table: str) -> set[str]:
        return {
            r[0] for r in con.execute("SELECT name FROM pragma_table_info(?)", (table,))
        }

    assert "rendered" not in _cols("pages")
    assert "rendered" not in _cols("fetches")
    assert "html_file" in _cols("pages")

    wc.init_schema(con)  # idempotent + migrating

    assert "rendered" in _cols("pages")
    assert "rendered" in _cols("fetches")
    assert "html_file" not in _cols("pages")  # dropped
    # The row's content survives the migration; content_type still drives the blob.
    row = wc.get("https://x.com/p", con=con)
    assert row is not None
    assert row["content_type"] == "text/html"
    con.close()
