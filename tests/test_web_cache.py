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
        html_file=wc.html_rel(sha),
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
