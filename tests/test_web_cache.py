"""Tests for web_cache: URL normalization, hashing, FTS, and the versioned store."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path  # noqa: TC003 — runtime annotation on a test helper

import pytest
import web_cache as wc

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


def test_fts_query_ands_bare_words_as_quoted_tokens():
    assert wc._fts_query("foo bar") == '"foo" "bar"'
    assert wc._fts_query("  foo   bar  ") == '"foo" "bar"'


def test_fts_query_keeps_a_quoted_run_as_one_phrase():
    # The bug this syntax fixes: splitting on whitespace turned a phrase into an
    # AND of its words, which matches strictly more documents with no signal.
    assert wc._fts_query('"upper magnet" knocker') == '"upper magnet" "knocker"'
    assert (
        wc._fts_query('"upper magnet" "coil positions" knocker')
        == '"upper magnet" "coil positions" "knocker"'
    )


def test_fts_query_keeps_operator_characters_inert():
    # Every unit goes out quoted, so FTS5 syntax in user input is never syntax.
    assert wc._fts_query("foo AND bar") == '"foo" "AND" "bar"'
    assert wc._fts_query("NEAR(a b) OR c*") == '"NEAR(a" "b)" "OR" "c*"'
    assert wc._fts_query('"a OR b"') == '"a OR b"'


def test_fts_query_consumes_a_quote_as_a_separator_not_as_nothing():
    # unicode61 reads '"' as a separator, so a"b is the two tokens "a b".
    # Deleting the quote instead of standing a space in for it would make that
    # the single token "ab" — a different document — so this is the assertion
    # that keeps consuming the quote from quietly changing what was asked.
    assert wc._fts_query('a"b') == '"a b"'
    assert wc._fts_query('say ""hi""') == '"say" "hi"'
    # Whitespace normalization keeps the substituted spaces out of the output.
    assert wc._fts_query('  "upper   magnet" \t knocker ') == '"upper magnet" "knocker"'
    assert wc._fts_query('""') == ""
    assert wc._fts_query("") == ""


def test_fts_query_substitutes_nul_the_way_it_substitutes_a_quote():
    # FTS5 scans its query as a C string, so a NUL truncates it mid-token and
    # raises "unterminated string" — the one other character the guard cannot
    # pass through. unicode61 reads it as a separator, so a space is lossless.
    assert wc._fts_query("a\0b") == '"a b"'
    assert wc._fts_query("\0") == ""
    # It is not a quote, though: it must not toggle the phrase state.
    assert wc._fts_units('"a\0b" c') == (["a b", "c"], False)


def test_fts_query_runs_an_unbalanced_quote_to_end_of_string():
    assert wc._fts_query('the "upper magnet') == '"the" "upper magnet"'
    assert wc._fts_units('the "upper magnet') == (["the", "upper magnet"], True)
    assert wc._fts_units('"upper magnet"') == (["upper magnet"], False)
    assert wc._fts_units('"') == ([], True)
    # An open quote need not own a whole unit, or any of one.
    assert wc._fts_units('foo"bar') == (["foo bar"], True)
    assert wc._fts_units('foo "') == (["foo"], True)


# --------------------------------------------------------------------------- #
# store: upsert / get / search / quote
# --------------------------------------------------------------------------- #


def _seed(
    con: sqlite3.Connection,
    *,
    url: str,
    raw_url: str | None = None,
    text: str | None = None,
    ocr_text: str | None = None,
    title: str | None = None,
    content: str | bytes = "x",
    content_type: str | None = None,
    text_source: str | None = None,
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
        ocr_text=ocr_text,
        content_type=content_type,
        text_source=text_source,
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


def test_search_phrase_excludes_a_page_holding_only_the_words(cache):
    both = wc.normalize_url("https://a.com/both")
    apart = wc.normalize_url("https://a.com/apart")
    _seed(cache, url=both, text="The upper magnet holds the ball.")
    _seed(cache, url=apart, text="The upper playfield has no magnet.")
    # Bare words AND anywhere in the page, so both match…
    assert {h["url"] for h in wc.search("upper magnet", con=cache)} == {both, apart}
    # …while the quoted phrase asks for the words adjacent, and only one is.
    assert [h["url"] for h in wc.search('"upper magnet"', con=cache)] == [both]


def test_search_returns_no_hits_for_a_term_with_nothing_to_match(cache):
    # An empty FTS expression is a syntax error, not an empty result — so these
    # have to be caught before the query rather than raised at the caller.
    _seed(cache, url=wc.normalize_url("https://a.com/x"), text="anything")
    assert wc.search("", con=cache) == []
    assert wc.search('  ""  ', con=cache) == []


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


def test_get_by_raw_url_matches_the_normalized_alias(cache):
    # The fetcher's freshness skip runs through this, so an exact-string match
    # would send it back over the network for a page already held whenever a
    # source list spells the old address a little differently.
    final = wc.normalize_url("https://site.com/new")
    _seed(cache, url=final, raw_url="https://site.com/old/path")
    for spelling in [
        "https://site.com/old/path/",
        "https://SITE.com/old/path",
        "https://site.com:443/old/path",
        "https://site.com/old/path?utm_source=news",
    ]:
        hit = wc.get_by_raw_url(spelling, con=cache)
        assert hit is not None, spelling
        assert hit["url"] == final
    # A genuinely different page still misses.
    assert wc.get_by_raw_url("https://site.com/other", con=cache) is None


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


# --------------------------------------------------------------------------- #
# text_source — how the row's text was derived (extraction-quality provenance)
# --------------------------------------------------------------------------- #


def test_text_source_stored_on_page(cache):
    url = wc.normalize_url("https://ipdb.org/images/1/x.jpg")
    wc.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=wc.content_sha(b"jpeg"),
        fetched_at=wc.now_iso(),
        text="MECATRONICS",
        text_source="ocr",
    )
    assert _page(cache, url)["text_source"] == "ocr"


def test_text_source_defaults_to_null_when_omitted(cache):
    url = wc.normalize_url("https://a.com/legacy")
    _seed(cache, url=url)  # _seed never passes text_source
    assert _page(cache, url)["text_source"] is None


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
    assert "text_source" not in _cols("pages")
    assert "imported" not in _cols("pages")
    assert "text_sha" not in _cols("fetches")
    assert "imported" not in _cols("fetches")
    assert "html_file" in _cols("pages")

    wc.init_schema(con)  # idempotent + migrating

    assert "rendered" in _cols("pages")
    assert "rendered" in _cols("fetches")
    # text_source arrived with image OCR: rows written before it stay NULL rather
    # than being back-filled with a guess about how their text was extracted.
    assert "text_source" in _cols("pages")
    # `imported` arrived with the manual-import path; a pre-existing row was
    # genuinely fetched, and NULL says "this predates the distinction" rather
    # than asserting anything about how those bytes were obtained.
    assert "imported" in _cols("pages")
    # `text_sha` was retired; the migration must not resurrect it.
    assert "text_sha" not in _cols("fetches")
    assert "imported" in _cols("fetches")
    assert "html_file" not in _cols("pages")  # dropped
    # A destructive migration (the html_file drop) was pending, so exactly one
    # pre-migration safety copy must exist beside the DB, dot-prefixed so
    # `make push` never ships it.
    backups = list(web_dir.glob(".cache.sqlite.bak-*"))
    assert len(backups) == 1
    # The row's content survives the migration; content_type still drives the blob.
    row = wc.get("https://x.com/p", con=con)
    assert row is not None
    assert row["content_type"] == "text/html"
    assert row["text_source"] is None
    con.close()


def test_init_schema_drops_text_sha_with_a_backup(tmp_path, monkeypatch):
    # A cache.sqlite from the era when fetches logged a text_sha: init_schema must
    # DROP the retired column, and — because a drop is destructive and this file is
    # the system-of-record — write a timestamped safety copy first. The copy still
    # holds the column, so a botched migration is recoverable from disk rather
    # than from R2 (whose cache may be older than the live one).
    web_dir = tmp_path / "web"
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    con = wc.connect()
    wc.init_schema(con)
    con.execute("ALTER TABLE fetches ADD COLUMN text_sha TEXT")
    con.commit()
    con.close()

    con = wc.connect()
    wc.init_schema(con)
    cols = {r[0] for r in con.execute("SELECT name FROM pragma_table_info('fetches')")}
    assert "text_sha" not in cols
    con.close()
    backups = list(web_dir.glob(".cache.sqlite.bak-*"))
    assert len(backups) == 1
    backup_con = sqlite3.connect(backups[0])
    backup_cols = {
        r[0]
        for r in backup_con.execute("SELECT name FROM pragma_table_info('fetches')")
    }
    backup_con.close()
    assert "text_sha" in backup_cols


def test_init_schema_skips_backup_when_no_drop_pending(tmp_path, monkeypatch):
    # The safety copy fires only for a pending destructive migration. A fresh DB
    # (and every routine re-open after it) must not accrete backups — silently
    # copying the store on every open is the failure mode the guard exists for.
    web_dir = tmp_path / "web"
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    for _ in range(2):
        con = wc.connect()
        wc.init_schema(con)
        con.close()
    assert list(web_dir.glob(".cache.sqlite.bak-*")) == []


# --------------------------------------------------------------------------- #
# imported provenance flag — storage + migration
# --------------------------------------------------------------------------- #


def test_imported_flag_stored_on_page_and_fetch(cache):
    url = wc.normalize_url("https://blocked.example/flyer.jpg")
    sha = wc.content_sha(b"jpeg")
    wc.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        imported=True,
    )
    assert _page(cache, url)["imported"] == 1
    wc.append_fetch(
        cache,
        url=url,
        fetched_at=wc.now_iso(),
        search_query=None,
        http_status=None,
        content_sha=sha,
        changed=True,
        imported=True,
    )
    row = cache.execute("SELECT imported, http_status FROM fetches").fetchone()
    assert row[0] == 1
    assert row[1] is None  # an import never claims an HTTP status


def test_imported_defaults_to_null_when_omitted(cache):
    url = wc.normalize_url("https://fetched.example/x")
    _seed(cache, url=url)  # _seed never passes imported
    assert _page(cache, url)["imported"] is None


# --------------------------------------------------------------------------- #
# Structural reads: outline() / section() / quote(context=)
# --------------------------------------------------------------------------- #

# An assembled page the way the HTML extractor emits one, with the traps the
# helpers must survive: a page's own "## body" heading (colliding with the
# body pseudo-section's name), a duplicated heading, and a heading-shaped
# line inside a code fence.
STRUCTURED_TEXT = """---
title: Structured Doc
og:description: A test document.
---

# Intro

Intro line one.
Intro line two.

## Machine List

- Cavalier
- Wizard

### Sub List

deep item

## body

This page's own h2 named body.

```
# fake heading inside a fence
```

## Machine List

Second list section."""


def _seed_structured(cache) -> str:
    url = wc.normalize_url("https://structured.example/doc")
    # text_source="html" because the frontmatter is the HTML extractor's
    # assembly — frontmatter semantics only apply to rows it provably wrote.
    _seed(
        cache,
        url=url,
        title="Structured Doc",
        text=STRUCTURED_TEXT,
        text_source="html",
    )
    return url


def test_outline_headings_levels_and_counts(cache):
    url = _seed_structured(cache)
    entries = wc.outline(url, con=cache)
    # The two "## Machine List" blocks collapse to one row, held at the first
    # one's position — so "## body" stays after it, in document order.
    assert [(e["level"], e["heading"]) for e in entries] == [
        (0, "metadata"),
        (0, "body"),
        (1, "Intro"),
        (2, "Machine List"),
        (3, "Sub List"),
        (2, "body"),
    ]
    # A fence-hidden heading is not a heading.
    assert all("fake" not in e["heading"] for e in entries)
    # Each count is the size of the block section() returns for that heading.
    intro = next(e for e in entries if e["heading"] == "Intro")
    assert intro["chars"] > len("# Intro\nIntro line one.")
    # The body pseudo-section spans everything after the frontmatter — here
    # exactly the # Intro block, since Intro is the document's only h1.
    body_entry = entries[1]
    assert body_entry["chars"] >= intro["chars"]


def test_outline_collapses_repeated_headings_with_summed_size(cache):
    url = _seed_structured(cache)
    entries = wc.outline(url, con=cache)
    repeated = next(e for e in entries if e["heading"] == "Machine List")
    assert repeated["count"] == 2
    # The collapsed size is what section() returns for that name, all blocks.
    blocks = wc.section(url, "Machine List", con=cache)
    assert repeated["chars"] == sum(len(b["text"]) for b in blocks)
    # A name appearing once is unremarkable — count 1, not absent.
    assert all(e["count"] == 1 for e in entries if e["heading"] != "Machine List")


def test_outline_does_not_collapse_same_name_at_different_levels(cache):
    url = wc.normalize_url("https://levels.example/doc")
    _seed(cache, url=url, text="## Specs\n\na\n\n### Specs\n\nb")
    entries = wc.outline(url, con=cache)
    # Same text, different level: two rows. A level-2 section and the level-3
    # one nested under it are not the same place in the document — and merging
    # them would count the inner block's chars inside the outer one twice.
    assert [(e["level"], e["count"]) for e in entries] == [(2, 1), (3, 1)]


def test_outline_does_not_collapse_same_name_under_different_parents(cache):
    url = wc.normalize_url("https://parents.example/doc")
    _seed(cache, url=url, text="# A\n\n## Specs\n\na\n\n# B\n\n## Specs\n\nb")
    entries = wc.outline(url, con=cache)
    # One "Specs" belongs to A and the other to B. Collapsing on name alone
    # would file both under A and claim B has no Specs at all.
    assert [(e["level"], e["heading"], e["count"]) for e in entries] == [
        (1, "A", 1),
        (2, "Specs", 1),
        (1, "B", 1),
        (2, "Specs", 1),
    ]
    # section() still ignores the tree and answers with both blocks; it is the
    # two rows' counts together that add up to what it returns.
    blocks = wc.section(url, "Specs", con=cache)
    assert len(blocks) == 2
    assert sum(e["chars"] for e in entries if e["heading"] == "Specs") == sum(
        len(b["text"]) for b in blocks
    )


def test_outline_keeps_a_repeated_parent_that_has_children(cache):
    url = wc.normalize_url("https://orphan.example/doc")
    _seed(cache, url=url, text="# A\n\n# B\n\n# A\n\n## X\n\nx body")
    entries = wc.outline(url, con=cache)
    # The outline is flat, so a row's parent is the nearest lower level above
    # it. Folding the second A into the first would print A, B, X — putting X
    # under B, whose child it is not.
    assert [(e["level"], e["heading"]) for e in entries] == [
        (1, "A"),
        (1, "B"),
        (1, "A"),
        (2, "X"),
    ]


def test_outline_collapses_identical_siblings_under_the_same_parent(cache):
    url = wc.normalize_url("https://siblings.example/doc")
    _seed(cache, url=url, text="# A\n\n## Tab\n\none\n\n## Tab\n\ntwo")
    entries = wc.outline(url, con=cache)
    # Same name, same level, same parent: genuinely the same place, so these
    # collapse — this is the page-builder repetition the feature exists for.
    assert [(e["heading"], e["count"]) for e in entries] == [("A", 1), ("Tab", 2)]


def test_outline_without_frontmatter_is_plain_headings(cache):
    url = wc.normalize_url("https://plain.example/doc")
    _seed(cache, url=url, text="prose\n\n## Section A\n\ncontent")
    assert wc.outline(url, con=cache) == [
        {
            "level": 2,
            "heading": "Section A",
            "chars": len("## Section A\n\ncontent"),
            "count": 1,
            "tier": "text",
        }
    ]


def test_section_returns_block_until_same_or_higher_heading(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "machine list", con=cache)  # case-insensitive
    # Duplicate headings: every matching block returned, document order.
    assert len(blocks) == 2
    # The first block leads with its heading line and includes its subsection,
    # stopping at the page's own "## body" (same level).
    assert blocks[0]["text"].startswith("## Machine List")
    assert "### Sub List" in blocks[0]["text"]
    assert "deep item" in blocks[0]["text"]
    assert "own h2 named body" not in blocks[0]["text"]
    assert blocks[1]["text"] == "## Machine List\n\nSecond list section."


def test_section_body_pseudo_spans_whole_document_body(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "body", con=cache)
    # Two blocks: the body pseudo-section (everything after the frontmatter)
    # and the page's own h2 named "body" — ambiguity surfaces rather than
    # silently picking one.
    assert len(blocks) == 2
    body_block = blocks[0]["text"]
    assert body_block.startswith("# Intro")  # no marker line, just the body
    assert "Second list section." in body_block  # spans to EOF
    own_h2_block = blocks[1]["text"]
    assert own_h2_block.startswith("## body")
    assert "own h2 named body" in own_h2_block
    assert "Second list section." not in own_h2_block  # closed by ## Machine List


def test_section_metadata_is_frontmatter_lines_without_delimiters(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "metadata", con=cache)
    assert len(blocks) == 1
    assert (
        blocks[0]["text"] == "title: Structured Doc\nog:description: A test document."
    )


def test_quote_context_zero_matches_sentence_behavior(cache):
    url = _seed_structured(cache)
    assert wc.quote(url, "Intro line one", con=cache) == ["Intro line one."]


def test_quote_hits_name_the_enclosing_section(cache):
    url = _seed_structured(cache)
    (hit,) = wc.quote_hits(url, "deep item", con=cache)
    assert hit["text"] == "deep item"
    # The nearest heading at or above the line — the h3, not the h2 it nests in.
    assert hit["heading"] == "Sub List"
    # And the name round-trips: it is what section() takes.
    first = wc.section(url, hit["heading"], con=cache)[0]
    assert first["text"].startswith("### Sub List")


def test_quote_hits_name_frontmatter_metadata(cache):
    url = _seed_structured(cache)
    (hit,) = wc.quote_hits(url, "A test document", con=cache)
    assert hit["heading"] == "metadata"


def test_quote_hits_are_contained_by_the_section_they_name(cache):
    url = _seed_structured(cache)
    # The invariant the labels rest on: whatever a hit shows, all of it lives
    # in the section named — so any span lifted out can carry that locator.
    for needle in ["deep item", "A test document", "Cavalier", "Intro line"]:
        for context in (0, 1, 5, 99):
            for hit in wc.quote_hits(url, needle, context=context, con=cache):
                if hit["heading"] is None:
                    continue
                blocks = wc.section(url, hit["heading"], con=cache)
                assert any(hit["text"] in b["text"] for b in blocks), (
                    f"{needle!r} at context={context} escaped {hit['heading']!r}"
                )


def test_quote_hits_have_no_heading_above_the_first_one(cache):
    url = wc.normalize_url("https://preamble.example/doc")
    _seed(cache, url=url, text="Opening prose.\n\n## Section A\n\ncontent")
    (hit,) = wc.quote_hits(url, "Opening", con=cache)
    # No heading to name, and "body" would overclaim — it spans Section A too.
    assert hit["heading"] is None


def test_quote_hits_name_the_match_not_the_widened_window(cache):
    url = _seed_structured(cache)
    # Widening pulls the window back across "### Sub List" into its parent
    # block, but the match itself never moves — so neither does its locator.
    # A display flag must not change where the evidence is said to live.
    for context in range(0, 6):
        (hit,) = wc.quote_hits(url, "deep item", context=context, con=cache)
        assert hit["heading"] == "Sub List", f"moved at context={context}"
    # And the window never leaves that section however wide it is asked to be:
    # padding clips at the section's own bounds, so it opens on the heading
    # line and never reaches the parent's list items above it.
    (wide,) = wc.quote_hits(url, "deep item", context=99, con=cache)
    assert wide["text"].startswith("### Sub List")
    assert "Cavalier" not in wide["text"]


def test_quote_hits_do_not_merge_across_a_section_boundary(cache):
    url = wc.normalize_url("https://merged.example/doc")
    _seed(
        cache,
        url=url,
        text="## Section A\n\nneedle one\n\n## Section B\n\nneedle two",
    )
    # ±3 would overlap these two matches into one window, but they sit either
    # side of a heading. One window would carry evidence from both sections
    # under a single name, so citing the far match would silently attribute it
    # to the near one. They stay two hits, each labelled with its own section.
    hits = wc.quote_hits(url, "needle", context=3, con=cache)
    assert [h["heading"] for h in hits] == ["Section A", "Section B"]
    assert "needle one" in hits[0]["text"]
    assert "needle two" in hits[1]["text"]


def test_quote_hits_never_show_another_section_s_match(cache):
    url = wc.normalize_url("https://merged.example/wide")
    _seed(
        cache,
        url=url,
        text="## Section A\n\nneedle one\n\n## Section B\n\nneedle two",
    )
    # Splitting the windows is not enough on its own: unclipped padding would
    # still pull the other section's match into both hits, so either could be
    # quoted from under the wrong name. Clipping is what closes that.
    for context in (3, 4, 99):
        hits = wc.quote_hits(url, "needle", context=context, con=cache)
        assert [h["heading"] for h in hits] == ["Section A", "Section B"]
        assert "needle two" not in hits[0]["text"], f"spilled at context={context}"
        assert "needle one" not in hits[1]["text"], f"spilled at context={context}"


def test_quote_hits_still_merge_within_one_section(cache):
    url = wc.normalize_url("https://merged.example/same")
    _seed(cache, url=url, text="## Only\n\nneedle one\n\nneedle two")
    # No boundary between them, so the overlap collapses as before — the split
    # above is about section identity, not a retreat from merging.
    (hit,) = wc.quote_hits(url, "needle", context=3, con=cache)
    assert "needle one" in hit["text"]
    assert "needle two" in hit["text"]
    assert hit["heading"] == "Only"


def test_quote_hits_keep_same_named_sections_apart(cache):
    url = wc.normalize_url("https://merged.example/twins")
    _seed(cache, url=url, text="## Specs\n\nneedle one\n\n## Specs\n\nneedle two")
    # Identically named sections are still two places; comparing names rather
    # than section identity would merge these back into one hit.
    hits = wc.quote_hits(url, "needle", context=3, con=cache)
    assert [h["heading"] for h in hits] == ["Specs", "Specs"]


def test_quote_still_splits_the_whole_text_into_sentences(cache):
    """quote() moved to a per-line walk to keep each hit's line index.

    Pin the property that made the move safe: _SENTENCE_SPLIT breaks on ``\\n+``,
    so no sentence spans a line and splitting line-by-line yields exactly what
    splitting the whole text at once did.
    """
    url = _seed_structured(cache)
    text = _page(cache, url)["text"]
    assert text is not None
    for needle in ["list", "Intro line", "e", "."]:
        assert wc.quote(url, needle, con=cache) == [
            s for s in wc.sentences(text) if needle.lower() in s.lower()
        ]


def test_quote_matches_quote_hits_text_exactly(cache):
    url = _seed_structured(cache)
    for needle, context in [("list", 0), ("intro line", 1), ("deep item", 2)]:
        assert wc.quote(url, needle, context=context, con=cache) == [
            h["text"] for h in wc.quote_hits(url, needle, context=context, con=cache)
        ]


def test_quote_context_windows_merge_in_document_order(cache):
    url = _seed_structured(cache)
    # Two adjacent matching lines with ±1 context overlap: one merged window.
    windows = wc.quote(url, "intro line", context=1, con=cache)
    assert len(windows) == 1
    assert "Intro line one.\nIntro line two." in windows[0]
    # Distant matches stay separate windows, in document order.
    windows = wc.quote(url, "list", context=1, con=cache)
    assert len(windows) >= 2
    joined = "\n---\n".join(windows)
    assert joined.index("Machine List") < joined.index("Second list section.")


# --------------------------------------------------------------------------- #
# quote: collapsed-whitespace matching + the PDF page axis
# --------------------------------------------------------------------------- #


def _assert_hit_invariants(text: str, needle: str, hits: list[wc.QuoteHit]) -> None:
    """The two containment invariants guarding the span-mapping bisect, whose
    failure mode is returning the line *beside* the match — something a recall
    count alone scores as a pass. (a) The right span came back. (b) Nothing
    was invented: every hit is a span of ``pages.text`` under the quote gate's
    own normalization — collapsed, not literal, because a cross-page hit drops
    its ``\\f`` marker lines and so is deliberately not a literal substring.
    """
    for hit in hits:
        assert wc._match_norm(needle) in wc._match_norm(hit["text"])
        assert wc._match_norm(hit["text"]) in wc._match_norm(text)


def test_quote_finds_a_phrase_spanning_a_line_break(cache):
    # The Houdini shape: reading-order PDF text breaks lines mid-phrase, and
    # per-line matching returned nothing for a phrase really on the page.
    url = wc.normalize_url("https://pdfish.example/houdini")
    text = "The shot hits the left pop\nbumper. Then it drains."
    _seed(cache, url=url, text=text)
    hits = wc.quote_hits(url, "left pop bumper", con=cache)
    # A match spanning lines returns those lines whole and verbatim — nothing
    # is joined or reflowed, so the break survives as visible structure.
    assert [h["text"] for h in hits] == [text]
    _assert_hit_invariants(text, "left pop bumper", hits)


def test_quote_hit_crossing_a_page_break_reports_both_pages(cache):
    url = wc.normalize_url("https://pdfish.example/crossing")
    text = "alpha bravo\n\f\ncharlie delta\n\f"
    _seed(cache, url=url, text=text)
    (hit,) = wc.quote_hits(url, "bravo charlie", con=cache)
    # The marker line is dropped from hit text (no viewer renders it); the
    # page list carries the boundary as a field instead.
    assert hit["text"] == "alpha bravo\ncharlie delta"
    assert "\f" not in hit["text"]
    assert hit["pdf_document_page_numbers"] == [1, 2]
    _assert_hit_invariants(text, "bravo charlie", [hit])


def test_one_page_document_reports_page_one_not_two(cache):
    # The phantom-page bug: poppler terminates the last page too, so a lone
    # trailing marker means one page, not two.
    url = wc.normalize_url("https://pdfish.example/flyer")
    _seed(cache, url=url, text="only line of the flyer\n\f")
    (hit,) = wc.quote_hits(url, "only line", con=cache)
    assert hit["pdf_document_page_numbers"] == [1]


def test_document_without_markers_reports_no_pages(cache):
    # Absent, not [1]: an unpaginated document's page structure is unknown —
    # the OCR-imported manual row — which is different from having one page.
    url = wc.normalize_url("https://html.example/plain")
    _seed(cache, url=url, text="a plain paragraph with a needle in it")
    (hit,) = wc.quote_hits(url, "needle", con=cache)
    assert hit["pdf_document_page_numbers"] is None


def test_blank_middle_page_keeps_page_numbers(cache):
    # The Time Machine shape: blank pages mid-document. If an empty page lost
    # its marker, every page after it would carry a confidently wrong number.
    url = wc.normalize_url("https://pdfish.example/blankpage")
    text = "page one text\n\f\n\f\npage three needle\n\f"
    _seed(cache, url=url, text=text)
    (hit,) = wc.quote_hits(url, "needle", con=cache)
    assert hit["pdf_document_page_numbers"] == [3]


def test_merged_window_reports_the_page_range_it_spans(cache):
    # The deliberate asymmetry with the heading rule: `heading` names the
    # match — a claim about meaning, so widening must not move it — while
    # `pdf_document_page_numbers` names the window, a claim about where the
    # ink is, so it must cover everything shown, crossed pages included.
    url = wc.normalize_url("https://pdfish.example/merged")
    text = "needle a\n\f\nmiddle filler\n\f\nneedle b\n\f"
    _seed(cache, url=url, text=text)
    (hit,) = wc.quote_hits(url, "needle", context=9, con=cache)
    assert hit["text"] == "needle a\nmiddle filler\nneedle b"
    assert hit["pdf_document_page_numbers"] == [1, 2, 3]
    _assert_hit_invariants(text, "needle", [hit])


def test_window_opening_on_a_dropped_marker_is_labelled_by_what_it_shows(cache):
    # The pages describe the *displayed* text: a context window that opens on
    # a marker line (which then disappears) must not claim the page before it.
    url = wc.normalize_url("https://pdfish.example/edge")
    _seed(cache, url=url, text="page one text\n\f\nneedle line\n\f")
    (hit,) = wc.quote_hits(url, "needle", context=1, con=cache)
    assert hit["text"] == "needle line"
    assert hit["pdf_document_page_numbers"] == [2]


def test_context_zero_keeps_sentence_granularity(cache):
    # Two matching sentences on one line stay two hits...
    url = wc.normalize_url("https://grouping.example/two-sentences")
    text = "Needle one. Needle two.\nplain line"
    _seed(cache, url=url, text=text)
    hits = wc.quote_hits(url, "needle", con=cache)
    assert [h["text"] for h in hits] == ["Needle one.", "Needle two."]
    _assert_hit_invariants(text, "needle", hits)


def test_two_occurrences_in_one_sentence_are_one_hit(cache):
    # ...and two occurrences inside one sentence stay one hit — the grouping
    # today's per-sentence walk produced, preserved through the span rewrite.
    url = wc.normalize_url("https://grouping.example/one-sentence")
    text = "The needle meets the needle here.\nplain line"
    _seed(cache, url=url, text=text)
    hits = wc.quote_hits(url, "needle", con=cache)
    assert [h["text"] for h in hits] == ["The needle meets the needle here."]


def test_context_on_a_long_single_line_returns_the_whole_line(cache):
    # Narrowing keys on the context *setting*, never on whether the widened
    # result occupies one line — else a --context call on a one-line paragraph
    # would silently sentence-clip what it returns whole today.
    url = wc.normalize_url("https://forum.example/comment")
    long_line = "Sentence of filler. " * 20 + "The needle sits here. More follows."
    text = f"{long_line}\nsecond line"
    _seed(cache, url=url, text=text)
    (hit,) = wc.quote_hits(url, "needle sits", context=1, con=cache)
    assert long_line in hit["text"]


def test_context_zero_still_narrows_to_the_sentence(cache):
    # The flip side, pinned so HTML doesn't regress: markdown paragraphs are
    # single long lines, and a context=0 hit inside one returns its sentence,
    # not the 900-word line.
    url = wc.normalize_url("https://forum.example/comment2")
    long_line = "Sentence of filler. " * 20 + "The needle sits here. More follows."
    _seed(cache, url=url, text=f"{long_line}\nsecond line")
    (hit,) = wc.quote_hits(url, "needle sits", con=cache)
    assert hit["text"] == "The needle sits here."


def test_needle_crossing_a_heading_boundary_returns_an_unlabeled_hit(cache):
    # No single name is true of a match spanning two sections; None is the
    # answer the code already gives for prose above the first heading.
    # Rejecting the occurrence would silently drop a real match.
    url = wc.normalize_url("https://cross.example/doc")
    text = "## A\na see ## B target a\nend see\n## B\ntarget start\nb see ## B target b"
    _seed(cache, url=url, text=text)
    needle = "see ## B target"
    hits = wc.quote_hits(url, needle, con=cache)
    assert [h["heading"] for h in hits] == ["A", None, "B"]
    assert hits[1]["text"] == "end see\n## B\ntarget start"
    _assert_hit_invariants(text, needle, hits)
    # And under a wide context the cross-section hit merges with neither
    # neighbour, though all three windows touch: the merge guard compares
    # whole section-id tuples, and (A,) != (A, B) != (B,).
    wide = wc.quote_hits(url, needle, context=9, con=cache)
    assert [h["heading"] for h in wide] == ["A", None, "B"]
    assert "target start" not in wide[0]["text"]  # clipped at A's boundary
    _assert_hit_invariants(text, needle, wide)


def test_empty_or_whitespace_needle_returns_no_hits(cache):
    # "" collapses to a needle str.find reports at every position while a scan
    # never advances; the CLI accepts "" as an argument, so it is reachable.
    url = wc.normalize_url("https://degenerate.example/doc")
    _seed(cache, url=url, text="any text at all")
    assert wc.quote(url, "", con=cache) == []
    assert wc.quote(url, "  \n\t ", con=cache) == []
    assert wc.main(["quote", url, ""]) == 1


def test_matching_is_case_insensitive_but_not_casefolded(cache):
    url = wc.normalize_url("https://case.example/doc")
    text = "Die Straße war lang."
    _seed(cache, url=url, text=text)
    assert wc.quote(url, "die straße", con=cache) == [text]
    # lower(), not casefold(): the aggressive cross-script fold would quietly
    # widen quote()'s documented contract, so STRASSE must keep missing Straße.
    assert wc.quote(url, "STRASSE", con=cache) == []


def test_smart_quotes_straighten_on_both_sides(cache):
    # The Sonic flyer's COLLECTOR'S has a curly apostrophe; a phrase typed
    # off a rendered page has a straight one. Returned text stays verbatim.
    # RUF001 waived: the curly apostrophe is the test's whole subject.
    url = wc.normalize_url("https://smart.example/doc")
    text = "COLLECTOR’S EDITION includes a topper."  # noqa: RUF001
    _seed(cache, url=url, text=text)
    assert wc.quote(url, "COLLECTOR'S EDITION", con=cache) == [text]
    assert wc.quote(url, "COLLECTOR’S EDITION", con=cache) == [text]  # noqa: RUF001


def test_blank_lines_do_not_skew_the_bisect(cache):
    # An empty normalized line contributes no separator — the one place the
    # prefix sum could drift and hand back the line beside the match.
    url = wc.normalize_url("https://blanks.example/doc")
    text = "first target line\n\n\n\nother needle here\n\n\nlast line"
    _seed(cache, url=url, text=text)
    (hit,) = wc.quote_hits(url, "needle", con=cache)
    assert hit["text"] == "other needle here"
    _assert_hit_invariants(text, "needle", [hit])


def test_html_hits_carry_no_page_numbers(cache):
    url = _seed_structured(cache)
    for context in (0, 2):
        for hit in wc.quote_hits(url, "deep item", context=context, con=cache):
            assert hit["pdf_document_page_numbers"] is None


def test_frontmatter_delimiters_are_not_matchable(cache):
    # The --- delimiters are assembly syntax, excluded from matching like \f
    # markers. Left matchable, a context window opening on one gets clipped
    # past its own match line by the metadata section's bounds — a hit that
    # doesn't contain its needle.
    url = wc.normalize_url("https://fm.example/delims")
    _seed(
        cache,
        url=url,
        text="---\ntitle: Sonic Flyer\nsource: example\n---\nbody",
        text_source="html",
    )
    for context in (0, 1, 5):
        assert wc.quote_hits(url, "---", context=context, con=cache) == []


def test_a_body_thematic_break_still_matches(cache):
    # Only the two delimiter lines are excluded — a markdown thematic break in
    # the body is the page's own text and stays quotable.
    url = wc.normalize_url("https://fm.example/thematic")
    text = "---\ntitle: Doc\n---\nabove\n---\nbelow"
    _seed(cache, url=url, text=text, text_source="html")
    hits = wc.quote_hits(url, "---", con=cache)
    assert [h["text"] for h in hits] == ["---"]
    _assert_hit_invariants(text, "---", hits)


def test_no_span_crosses_the_frontmatter_boundary(cache):
    # The frontmatter is this cache's own assembly, so a phrase joining it to
    # body text is never evidence — it would read our synthetic text as the
    # document's, and it would verify at the gate because the frontmatter is
    # in pages.text. With the delimiters unmatchable the two sit adjacent in
    # matching space, so the boundary must be a hard barrier.
    url = wc.normalize_url("https://fm.example/barrier")
    _seed(
        cache,
        url=url,
        text="---\ntitle: Sonic Flyer\nsource: example\n---\nbody starts here",
        text_source="html",
    )
    assert wc.quote_hits(url, "example body starts", con=cache) == []
    assert wc.quote_hits(url, "example --- body", con=cache) == []
    # Each side still matches on its own.
    assert wc.quote(url, "source: example", con=cache) == ["source: example"]
    assert wc.quote(url, "body starts", con=cache) == ["body starts here"]


def test_a_boundary_shaped_needle_still_finds_its_body_match(cache):
    # Why the two sides are scanned separately rather than filtered after: a
    # cross-boundary candidate that is found first and then rejected has
    # already consumed the scan position, hiding a valid body occurrence that
    # overlaps it. Metadata ends "alpha", body begins "alpha\nalpha target" —
    # the real match is wholly inside the body and must come back.
    url = wc.normalize_url("https://fm.example/overlap")
    text = "---\ntitle: alpha\n---\nalpha\nalpha target"
    _seed(cache, url=url, text=text, text_source="html")
    hits = wc.quote_hits(url, "alpha alpha", con=cache)
    assert [h["text"] for h in hits] == ["alpha\nalpha target"]
    _assert_hit_invariants(text, "alpha alpha", hits)


def test_frontmatter_semantics_apply_only_to_extracted_html(cache):
    # Frontmatter is the HTML extractor's assembly, so only a row whose
    # text_source proves that provenance gets its semantics. A document whose
    # own first line happens to be --- (a PDF's rule line, a transcript) is
    # page text through and through: its dashes stay matchable, phrases cross
    # them, and nothing gets labeled metadata — the alternative silently
    # suppresses real evidence on the strength of a coincidence.
    url = wc.normalize_url("https://pdfish.example/dashes")
    text = "---\nOwner's Note\n---\nreal content here\n\f"
    _seed(cache, url=url, text=text, content_type="application/pdf", text_source="pdf")
    (hit,) = wc.quote_hits(url, "Note --- real content", con=cache)
    assert hit["text"] == "Owner's Note\n---\nreal content here"
    assert hit["heading"] is None  # no headings at all, not "metadata"
    assert wc.quote(url, "---", con=cache) == ["---", "---"]
    _assert_hit_invariants(text, "Note --- real content", [hit])


def test_parent_prose_spanning_into_a_child_keeps_the_parent_label(cache):
    # Deliberate: a parent section's block runs through its subsections, so
    # the parent's name is true of every line here (section(parent) returns
    # the child's text too) — unlike a span across sibling sections, there is
    # no misattribution to prevent, and None would discard a true locator.
    url = wc.normalize_url("https://nested.example/doc")
    text = "## Parent\nparent intro alpha\n### Child\nchild beta text"
    _seed(cache, url=url, text=text)
    needle = "alpha ### Child child beta"
    (hit,) = wc.quote_hits(url, needle, con=cache)
    assert hit["heading"] == "Parent"
    assert hit["text"] == "parent intro alpha\n### Child\nchild beta text"
    _assert_hit_invariants(text, needle, [hit])


# --------------------------------------------------------------------------- #
# CLI — the same five reads as shell commands
# --------------------------------------------------------------------------- #


def test_cli_search_prints_hits_and_quote_prints_matches(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["search", "structured"]) == 0
    out = capsys.readouterr().out
    assert url in out
    assert "Structured Doc" in out

    assert wc.main(["quote", url, "intro line", "--context", "1"]) == 0
    out = capsys.readouterr().out
    assert "Intro line one.\nIntro line two." in out


def test_cli_quote_pdf_prints_pages_and_blob_path(cache, capsys):
    # The render handoff: everything Read(<blob>, pages=N) needs. The blob is a
    # fact about the page, so it goes to stderr once; the page numbers genuinely
    # vary per hit, so they ride each one on stdout.
    url = wc.normalize_url("https://pdf.example/manual.pdf")
    sha = _seed(
        cache,
        url=url,
        text="alpha needle intro\n\f\nneedle closer\n\f",
        content_type="application/pdf",
        text_source="pdf",
    )
    assert wc.main(["quote", url, "needle"]) == 0
    captured = capsys.readouterr()
    assert captured.out == (
        "alpha needle intro\n"
        "pdf document pages: 1\n"
        "\n"
        "needle closer\n"
        "pdf document pages: 2\n"
    )
    assert captured.err == (
        f"blob: {wc.blob_path(sha, 'pdf')}\n"
        "not yet OCR'd: sheet-image content is invisible to search "
        "(scripts/web_scrape/web_pdfocr.py reads it)\n"
    )


def test_cli_quote_transcribed_pdf_prints_path_and_pages_unavailable(cache, capsys):
    # A hand-typed transcription has no markers, but the blob is a PDF and
    # renders fine — the row whose text most needs the go-look-at-the-page
    # step. Silence about pages would read as "one page"; like the blob, it is
    # a fact about the page and prints once, on stderr.
    url = wc.normalize_url("https://pdf.example/scan.pdf")
    sha = _seed(
        cache,
        url=url,
        text="typed text of a scanned manual",
        content_type="application/pdf",
        text_source="manual",
    )
    assert wc.main(["quote", url, "scanned manual"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "typed text of a scanned manual\n"
    assert captured.err == (
        f"blob: {wc.blob_path(sha, 'pdf')}\npdf document pages: unavailable\n"
        "not yet OCR'd: sheet-image content is invisible to search "
        "(scripts/web_scrape/web_pdfocr.py reads it)\n"
    )


def test_cli_quote_pdf_without_markers_says_pages_unavailable(cache, capsys):
    # A poppler-read row whose text predates the page markers: same answer.
    url = wc.normalize_url("https://pdf.example/old-extraction.pdf")
    sha = _seed(
        cache,
        url=url,
        text="pre-marker extraction text",
        content_type="application/pdf",
        text_source="pdf",
    )
    assert wc.main(["quote", url, "extraction"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "pre-marker extraction text\n"
    assert captured.err == (
        f"blob: {wc.blob_path(sha, 'pdf')}\npdf document pages: unavailable\n"
        "not yet OCR'd: sheet-image content is invisible to search "
        "(scripts/web_scrape/web_pdfocr.py reads it)\n"
    )


def test_cli_quote_ocr_image_prints_path_without_page_noise(cache, capsys):
    # An OCR'd JPEG has a renderable blob too, but telling someone their JPEG
    # has no PDF pages is noise — the unavailable line keys on content type.
    url = wc.normalize_url("https://img.example/flyer.jpg")
    sha = _seed(
        cache,
        url=url,
        text="flyer text via a reviewed transcription",
        content_type="image/jpeg",
        text_source="manual",
    )
    assert wc.main(["quote", url, "flyer text"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "flyer text via a reviewed transcription\n"
    assert captured.err == f"blob: {wc.blob_path(sha, 'jpg')}\n"


def test_cli_quote_miss_on_a_pdf_still_prints_the_blob_path(cache, capsys):
    # "no matches" alone would read as absence when the needle may simply be
    # artwork on a sheet nobody has looked at.
    url = wc.normalize_url("https://pdf.example/manual.pdf")
    sha = _seed(
        cache,
        url=url,
        text="alpha intro\n\f\ncloser\n\f",
        content_type="application/pdf",
        text_source="pdf",
    )
    assert wc.main(["quote", url, "upper magnet"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        f"no matches for 'upper magnet' in {url}\nblob: {wc.blob_path(sha, 'pdf')}\n"
        "not yet OCR'd: sheet-image content is invisible to search "
        "(scripts/web_scrape/web_pdfocr.py reads it)\n"
    )


def test_cli_quote_on_a_text_less_pdf_says_nothing_can_match(cache, capsys):
    # A row with no text can't match anything, and saying so beats a bare "no
    # matches" that reads as the document not saying it.
    url = wc.normalize_url("https://pdf.example/quick-reference.pdf")
    sha = _seed(
        cache,
        url=url,
        text=None,
        content_type="application/pdf",
        text_source="pdf",
    )
    assert wc.main(["quote", url, "magnet"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert f"blob: {wc.blob_path(sha, 'pdf')}" in captured.err
    assert "no stored text" in captured.err
    # But it claims neither cause. An image-only document and an extraction
    # that was unavailable on the fetching host both land here, and the row
    # records neither, so naming one would assert what nothing checked.
    assert "image-only, or extraction may have been unavailable" in captured.err
    # No markers either, but saying so would be the duller way of saying the
    # line above — the two never both fire.
    assert "pdf document pages: unavailable" not in captured.err


def test_cli_quote_on_a_text_less_html_row_names_no_causes(cache, capsys):
    # A page with no blob line gets the bare form: "image-only" is nonsense
    # about HTML, and "read the blob" would point at a path never printed.
    url = wc.normalize_url("https://html.example/empty")
    _seed(cache, url=url, text=None, content_type="text/html", text_source="html")
    assert wc.main(["quote", url, "anything"]) == 1
    captured = capsys.readouterr()
    assert "no stored text, so nothing can match it\n" in captured.err
    assert "image-only" not in captured.err
    assert "blob:" not in captured.err


def test_cli_quote_html_output_is_unchanged(cache, capsys):
    # The corpus's most common type gains no lines at all: no pages, no blob.
    url = wc.normalize_url("https://html.example/page")
    _seed(
        cache,
        url=url,
        text="## Section\n\nNeedle sentence one. Other.\n\nNeedle again.",
        content_type="text/html",
        text_source="html",
    )
    assert wc.main(["quote", url, "needle"]) == 0
    assert capsys.readouterr().out == (
        "[Section]\nNeedle sentence one.\n\n[Section]\nNeedle again.\n"
    )


def test_cli_outline_indents_by_level(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["outline", url]) == 0
    lines = capsys.readouterr().out.split("\n")
    assert any(ln.startswith("metadata  [") for ln in lines)
    assert any(ln.startswith("  Intro  [") for ln in lines)  # level 1 under body


def test_cli_section_notes_ambiguity_on_stderr_only(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["section", url, "machine list"]) == 0
    captured = capsys.readouterr()
    assert "Second list section." in captured.out
    assert "2 sections match" in captured.err
    assert "sections match" not in captured.out  # stdout stays pure page text


def test_cli_quote_prints_the_enclosing_heading_with_each_hit(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["quote", url, "deep item"]) == 0
    out = capsys.readouterr().out
    # One command yields both halves of a cite: the locator, then the span.
    assert out == "[Sub List]\ndeep item\n"


def test_cli_quote_omits_the_label_when_there_is_no_heading(cache, capsys):
    url = wc.normalize_url("https://preamble.example/doc")
    _seed(cache, url=url, text="Opening prose.\n\n## Section A\n\ncontent")
    assert wc.main(["quote", url, "Opening"]) == 0
    assert capsys.readouterr().out == "Opening prose.\n"


def test_cli_section_miss_says_where_the_string_does_appear(cache, capsys):
    url = wc.normalize_url("https://builder.example/doc")
    # "Playfield Features" is on the page, but as body text under a real
    # heading — the exact shape a page-builder site produces.
    _seed(
        cache,
        url=url,
        text="## Additional Features\n\nPlayfield Features\n\n- a spinner",
    )
    assert wc.main(["section", url, "Playfield Features"]) == 1
    err = capsys.readouterr().err
    assert "no section 'Playfield Features'" in err
    assert "appears as text in section(s): Additional Features" in err


def test_cli_section_miss_offers_headings_that_contain_the_needle(cache, capsys):
    url = wc.normalize_url("https://builder.example/near")
    _seed(cache, url=url, text="## 100th Anniversary\n\nan edition")
    assert wc.main(["section", url, "Anniversary"]) == 1
    err = capsys.readouterr().err
    # A heading does contain it, so this is a near miss on an exact-match read
    # — calling it body text would be false.
    assert "did you mean: 100th Anniversary" in err
    assert "not a heading" not in err


def test_cli_section_miss_stays_quiet_when_the_page_lacks_the_string(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["section", url, "zzznomatch"]) == 1
    err = capsys.readouterr().err
    assert "no section 'zzznomatch'" in err
    assert "appears as text" not in err  # a genuine absence reads as one
    assert "did you mean" not in err


def test_cli_section_miss_does_not_hint_on_an_empty_heading(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["section", url, ""]) == 1
    err = capsys.readouterr().err
    # "" is a substring of every heading; reciting the whole outline back would
    # say nothing.
    assert "no section ''" in err
    assert "did you mean" not in err
    assert "Machine List" not in err


def test_cli_outline_collapses_duplicates_and_filters_by_size(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["outline", url]) == 0
    out = capsys.readouterr().out
    assert "Machine List  x2  [" in out
    assert "Sub List  [" in out  # a single occurrence carries no xN

    assert wc.main(["outline", url, "--min-chars", "40"]) == 0
    captured = capsys.readouterr()
    assert "Sub List" not in captured.out  # the small block is filtered out
    # What was withheld is stated, and on stderr so the map itself stays clean.
    assert "headings under 40 chars hidden" in captured.err
    assert "hidden" not in captured.out


def test_have_reports_holdings_in_the_order_asked(cache):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")
    holdings = wc.have([held, "https://absent.example/b"], con=cache)
    assert [h["asked"] for h in holdings] == [held, "https://absent.example/b"]
    assert holdings[0]["page"] is not None
    assert holdings[1]["page"] is None


def test_have_finds_a_page_that_redirected_elsewhere(cache):
    # The fetcher files a redirected page under its destination; the requested
    # address survives only in raw_url. A plain get() loop calls this missing
    # and sends a campaign off to refetch something already in hand.
    final = wc.normalize_url("https://site.example/new/path")
    asked = "https://site.example/old/path"
    _seed(cache, url=final, raw_url=asked, text="body")
    assert wc.get(asked, con=cache) is None  # the naive check
    (holding,) = wc.have([asked], con=cache)
    assert holding["page"] is not None
    assert holding["stored_url"] == final


def test_have_normalizes_the_redirect_alias(cache):
    final = wc.normalize_url("https://site.example/new/path")
    _seed(cache, url=final, raw_url="https://site.example/old/path", text="body")
    # Equivalent spellings of the old address must resolve too — an exact
    # string match would reintroduce, on this path, the duplicate-identity
    # problem normalize_url exists to prevent.
    for spelling in [
        "https://site.example/old/path/",
        "HTTPS://Site.Example/old/path",
        "https://site.example:443/old/path",
        "https://site.example/old/path?utm_source=news",
    ]:
        (holding,) = wc.have([spelling], con=cache)
        assert holding["page"] is not None, spelling
        assert holding["stored_url"] == final


def test_have_alias_is_superseded_by_a_later_fetch(cache):
    # Documented limitation, pinned so it can't change silently: raw_url holds
    # the most recent fetch's address, so refetching through the canonical URL
    # drops the old alias and `have` goes back to reporting it missing.
    final = wc.normalize_url("https://site.example/new/path")
    asked = "https://site.example/old/path"
    _seed(cache, url=final, raw_url=asked, text="body")
    assert wc.have([asked], con=cache)[0]["page"] is not None
    _seed(cache, url=final, raw_url=final, text="body")  # refetched canonically
    assert wc.have([asked], con=cache)[0]["page"] is None
    # The page itself is still held under its own address — nothing was lost
    # but the alias, so the cost is a redundant refetch, never missing evidence.
    assert wc.have([final], con=cache)[0]["page"] is not None


def test_have_alias_prefers_the_most_recent_destination(cache):
    # One requested address can redirect to different destinations over time,
    # leaving two rows claiming the same alias. The current destination wins,
    # rather than whichever row the database happens to return first.
    asked = "https://site.example/old"
    old = wc.normalize_url("https://site.example/first-home")
    new = wc.normalize_url("https://site.example/second-home")
    wc.upsert_page(
        cache,
        url=old,
        raw_url=asked,
        content_sha=wc.content_sha(b"1"),
        fetched_at="2024-01-01T00:00:00Z",
        text="old",
    )
    wc.upsert_page(
        cache,
        url=new,
        raw_url=asked,
        content_sha=wc.content_sha(b"2"),
        fetched_at="2026-01-01T00:00:00Z",
        text="new",
    )
    (holding,) = wc.have([asked], con=cache)
    assert holding["stored_url"] == new


def test_have_isolates_an_unparseable_url(cache):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")
    holdings = wc.have(
        [held, "https://host:notaport/x", "https://absent.example/b"], con=cache
    )
    # The bad entry is reported, and the other two still get answered — a bulk
    # read over a hand-written list can't be all-or-nothing.
    assert holdings[0]["page"] is not None
    assert holdings[1]["page"] is None
    assert holdings[1]["error"] is not None
    assert holdings[2]["page"] is None
    assert holdings[2]["error"] is None  # genuinely uncached, not malformed


def test_cli_have_marks_invalid_apart_from_missing(cache, capsys, tmp_path):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")
    listing = tmp_path / "urls.txt"
    listing.write_text(
        f"{held}\tquery one\nhttps://host:notaport/x\tquery two\n"
        "https://absent.example/b\tquery three\n",
        encoding="utf-8",
    )
    assert wc.main(["have", "--from-file", str(listing)]) == 1
    captured = capsys.readouterr()
    assert "INVALID  https://host:notaport/x" in captured.out
    assert "MISSING  https://absent.example/b" in captured.out
    assert "1/3 cached, 1 unparseable" in captured.err


def test_have_builds_the_alias_index_only_on_a_miss(cache, monkeypatch):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")

    def explode(_con):
        raise AssertionError("alias index built when nothing missed")

    monkeypatch.setattr(wc, "_alias_index", explode)
    # Every URL found under its own key, so the redirect fallback is never
    # consulted — and its scan of every redirected page is not paid for. That
    # scan grows with the corpus; this call should not.
    assert wc.have([held], con=cache)[0]["page"] is not None
    assert wc.have([], con=cache) == []


def test_have_reports_no_stored_url_for_a_direct_hit(cache):
    url = wc.normalize_url("https://direct.example/a")
    _seed(cache, url=url, text="body")
    (holding,) = wc.have([url], con=cache)
    # Only a redirect sets stored_url — an ordinary hit needs no annotation.
    assert holding["stored_url"] is None


def test_cli_have_lists_misses_and_exits_nonzero(cache, capsys):
    url = wc.normalize_url("https://held.example/a")
    _seed(cache, url=url, text="body text")
    assert wc.main(["have", url, "https://absent.example/b"]) == 1
    captured = capsys.readouterr()
    assert "cached   " in captured.out
    assert "MISSING  https://absent.example/b" in captured.out
    assert "1/2 cached" in captured.err  # the tally stays off stdout


def test_cli_have_exits_zero_when_everything_is_held(cache, capsys):
    url = wc.normalize_url("https://held.example/a")
    _seed(cache, url=url, text="body")
    assert wc.main(["have", url]) == 0
    assert "MISSING" not in capsys.readouterr().out


def test_cli_have_reads_web_fetch_s_own_tsv(cache, capsys, tmp_path):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")
    listing = tmp_path / "urls.tsv"
    # Comments, blanks and a query column, exactly as web_fetch --from-file
    # takes — so one source list drives both checking and fetching.
    listing.write_text(
        f"# campaign sources\n{held}\thow it works\n\n"
        "https://absent.example/b\twhy it matters\n",
        encoding="utf-8",
    )
    assert wc.main(["have", "--from-file", str(listing)]) == 1
    out = capsys.readouterr().out
    assert f"cached   {held}" in out
    assert "MISSING  https://absent.example/b" in out
    assert "# campaign sources" not in out


def test_cli_have_needs_urls(cache, capsys):
    assert wc.main(["have"]) == 2
    assert "no URLs given" in capsys.readouterr().err


def test_cli_have_reports_an_unreadable_list_cleanly(cache, capsys, tmp_path):
    assert wc.main(["have", "--from-file", str(tmp_path / "typo.tsv")]) == 2
    err = capsys.readouterr().err
    # A mistyped path is the likeliest way to call this wrong; the message has
    # to name the path rather than bury it in a traceback.
    assert "cannot read" in err
    assert "typo.tsv" in err


def test_cli_have_does_not_guess_a_type_it_never_recorded(cache, capsys):
    url = wc.normalize_url("https://legacy.example/a")
    # content_type is nullable and _seed never sets it — a row from before the
    # column was populated. Printing "html" there would assert something the
    # row does not say.
    _seed(cache, url=url, text="body")
    assert wc.main(["have", url]) == 0
    out = capsys.readouterr().out
    assert "type unrecorded" in out
    assert "html" not in out


def test_cli_get_splits_metadata_to_stderr_text_to_stdout(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["get", url]) == 0
    captured = capsys.readouterr()
    assert captured.out.startswith("---")  # the stored text, nothing else
    assert "content_sha:" in captured.err
    assert "content_sha:" not in captured.out


def test_cli_misses_exit_nonzero_with_stderr_message(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["search", "zzznomatch"]) == 1
    assert "no pages match" in capsys.readouterr().err
    assert wc.main(["quote", url, "zzznomatch"]) == 1
    assert "no matches" in capsys.readouterr().err
    with pytest.raises(SystemExit, match="1"):
        wc.main(["quote", "https://absent.example/x", "needle"])
    assert "no cached page" in capsys.readouterr().err


def test_cli_search_names_the_phrase_it_read_from_an_unbalanced_quote(cache, capsys):
    url = wc.normalize_url("https://a.com/both")
    _seed(cache, url=url, text="The upper magnet holds the ball.")
    # The search still runs — the library never raises on user input — but
    # reinterpreting a query silently is the failure the syntax exists to fix,
    # so the shell face has to show what it ran.
    assert wc.main(["search", 'the "upper magnet']) == 0
    captured = capsys.readouterr()
    assert 'unbalanced quote; searched "the" "upper magnet"' in captured.err
    assert url in captured.out
    # The open quote owns no unit here, so naming one as "the" phrase would
    # report a bare word as something the user had quoted.
    assert wc.main(["search", 'magnet "']) == 0
    assert 'unbalanced quote; searched "magnet"' in capsys.readouterr().err
    # …and when it owns nothing at all there is no expression to show.
    assert wc.main(["search", '"']) == 1
    assert "unbalanced quote; searched nothing" in capsys.readouterr().err


def test_cli_search_labels_non_html_text_sources(cache, capsys):
    wc.upsert_page(
        cache,
        url=wc.normalize_url("https://ipdb.org/images/1/flyer.jpg"),
        raw_url="https://ipdb.org/images/1/flyer.jpg",
        content_sha=wc.content_sha(b"jpeg"),
        fetched_at=wc.now_iso(),
        title="Nordamatic flyer",
        text="NORDAMATIC ANTARES flyer text",
        content_type="image/jpeg",
        text_source="ocr",
    )
    wc.upsert_page(
        cache,
        url=wc.normalize_url("https://a.example/antares"),
        raw_url="https://a.example/antares",
        content_sha=wc.content_sha(b"html"),
        fetched_at=wc.now_iso(),
        title="Antares page",
        text="Antares was a Nordamatic machine.",
        content_type="text/html",
        text_source="html",
    )
    assert wc.main(["search", "antares"]) == 0
    out = capsys.readouterr().out
    assert "type: image" in out  # the non-web hit says what it is…
    assert "text_source: ocr" in out  # …and how its text was derived
    assert "type:" not in out.split("text_source: ocr")[1]  # web hit unlabeled
    assert "text_source: html" not in out


# --------------------------------------------------------------------------- #
# Page maps: outline/section on a paginated (PDF) document
# --------------------------------------------------------------------------- #

# Three sheets, the middle one yielding no text — the shape a seventh of the
# corpus's mapped sheets have, and the one a walk must not mistake for absence.
PAGED_TEXT = "first sheet text\n\f\n\f\nthird sheet has more words\n\f"


def _seed_paged(cache, text: str = PAGED_TEXT) -> str:
    url = wc.normalize_url("https://paged.example/manual.pdf")
    _seed(
        cache,
        url=url,
        text=text,
        content_type="application/pdf",
        text_source="pdf",
    )
    return url


def test_outline_maps_a_paginated_document_by_page(cache):
    url = _seed_paged(cache)
    entries = wc.outline(url, con=cache)
    assert [e["heading"] for e in entries] == ["page 1", "page 2", "page 3"]
    assert [e["chars"] for e in entries] == [16, 0, 26]
    # Flat and unrepeated: sheets are ordinals, not a tree.
    assert {e["level"] for e in entries} == {0}
    assert {e["count"] for e in entries} == {1}


def test_outline_page_map_wins_over_a_heading_misparse(cache):
    # A PDF's text can carry a line that looks like ATX markdown. Pages are the
    # document's real division, so the map is by sheet either way.
    url = _seed_paged(cache, "# Not A Heading\nbody\n\f\nsecond sheet\n\f")
    assert [e["heading"] for e in wc.outline(url, con=cache)] == ["page 1", "page 2"]


def test_section_returns_one_page_without_its_marker(cache):
    url = _seed_paged(cache)
    assert wc.section(url, "page 3", con=cache) == [
        {"text": "third sheet has more words", "tier": "text"}
    ]
    assert "\f" not in wc.section(url, "page 1", con=cache)[0]["text"]


def test_section_page_name_is_case_insensitive_like_every_other_name(cache):
    url = _seed_paged(cache)
    expected = [{"text": "third sheet has more words", "tier": "text"}]
    assert wc.section(url, "Page 3", con=cache) == expected
    assert wc.section(url, "  page 3  ", con=cache) == expected


@pytest.mark.parametrize("name", ["page 03", "page 3a", "page 0", "page -1", "page"])
def test_section_rejects_names_that_are_not_page_names(cache, name):
    # One spelling per sheet: the name outline() printed. A near-miss that
    # silently resolved to sheet 3 would let a walk cite the wrong page.
    url = _seed_paged(cache)
    assert wc.section(url, name, con=cache) == []


def test_section_textless_page_returns_no_blocks(cache):
    # `[]` means "no blocks of text", which is true of such a sheet. The
    # difference from an absent page is carried by the hint, not by a sentinel
    # empty string that every caller would have to know about.
    url = _seed_paged(cache)
    assert wc.section(url, "page 2", con=cache) == []
    assert wc.outline(url, con=cache)[1] == {
        "level": 0,
        "heading": "page 2",
        "chars": 0,
        "count": 1,
        "tier": "text",
    }


def test_section_page_name_is_reserved_on_a_paginated_document(cache):
    # A heading block runs to the next heading at its level, so composing it
    # with the sheet would answer "give me sheet 2" with other sheets' text.
    url = _seed_paged(cache, "# page 2\nheading body\n\f\nsecond sheet\n\f")
    assert wc.section(url, "page 2", con=cache) == [
        {"text": "second sheet", "tier": "text"}
    ]


def test_section_out_of_range_page_is_not_rescued_by_a_heading(cache):
    # The trap the reservation closes: a misparsed `# page 99` would otherwise
    # make a sheet that does not exist answer as though it did.
    url = _seed_paged(cache, "# page 99\nheading body\n\f\nsecond sheet\n\f")
    assert wc.section(url, "page 99", con=cache) == []


def test_section_page_name_matches_a_heading_when_unpaginated(cache):
    # Nothing to reserve without sheets, so the name is just a heading — and a
    # `quote` hit labelled "page 2" on such a page stays resolvable.
    url = wc.normalize_url("https://plain.example/paged-looking")
    _seed(cache, url=url, text="# page 2\nheading body", text_source="html")
    assert wc.section(url, "page 2", con=cache) == [
        {"text": "# page 2\nheading body", "tier": "text"}
    ]


def test_section_out_of_range_page_is_a_miss_not_a_clamp(cache):
    url = _seed_paged(cache)
    assert wc.section(url, "page 99", con=cache) == []


def test_cli_outline_ignores_min_chars_on_a_page_map(cache, capsys):
    url = _seed_paged(cache)
    assert wc.main(["outline", url, "--min-chars", "20"]) == 0
    captured = capsys.readouterr()
    # Every sheet still shown — hiding one would assert a gap in the ordinals.
    assert "page 1  [16 chars]" in captured.out
    assert "page 2  [0 chars]" in captured.out
    assert "ignored" in captured.err
    assert "hidden" not in captured.err


def test_cli_outline_says_nothing_was_filtered_at_min_chars_zero(cache, capsys):
    # An explicit 0 asks to see everything, which is what a page map shows, so
    # there is nothing withheld to report.
    url = _seed_paged(cache)
    assert wc.main(["outline", url, "--min-chars", "0"]) == 0
    err = capsys.readouterr().err
    assert "ignored" not in err
    assert "hidden" not in err


def test_cli_outline_on_a_pdf_without_page_markers_names_what_it_lacks(cache, capsys):
    url = wc.normalize_url("https://paged.example/scan.pdf")
    _seed(cache, url=url, text="ocr text", content_type="application/pdf")
    assert wc.main(["outline", url]) == 1
    assert "no page markers" in capsys.readouterr().err


def test_cli_outline_on_a_row_with_no_text_blames_the_text_not_the_markers(
    cache, capsys
):
    # A fifth of the cached PDFs are image-only scans. "no page markers" would
    # send a reader to re-extract a document that has none to find; `quote`
    # draws the same line and these reads must agree.
    url = wc.normalize_url("https://paged.example/image-only.pdf")
    _seed(cache, url=url, text=None, content_type="application/pdf", text_source="pdf")
    assert wc.main(["outline", url]) == 1
    assert "no stored text" in capsys.readouterr().err

    assert wc.main(["section", url, "page 2"]) == 1
    assert "no stored text" in capsys.readouterr().err


def test_cli_outline_on_html_still_says_no_headings(cache, capsys):
    url = wc.normalize_url("https://plain.example/page")
    _seed(cache, url=url, text="just prose", text_source="html")
    assert wc.main(["outline", url]) == 1
    assert "no headings" in capsys.readouterr().err


def test_cli_section_miss_tells_textless_from_absent_from_unpaginated(cache, capsys):
    paged = _seed_paged(cache)
    assert wc.main(["section", paged, "page 2"]) == 1
    assert "page 2 has no extracted text" in capsys.readouterr().err

    assert wc.main(["section", paged, "page 99"]) == 1
    assert "this document has 3 pages" in capsys.readouterr().err

    # A PDF whose text lost its markers (OCR'd, or hand-supplied) reads the
    # name as a page, because on a PDF that is what it means.
    scan = wc.normalize_url("https://paged.example/ocr.pdf")
    _seed(cache, url=scan, text="ocr text", content_type="application/pdf")
    assert wc.main(["section", scan, "page 2"]) == 1
    assert "no page markers" in capsys.readouterr().err


def test_cli_section_miss_on_html_keeps_heading_guidance_for_a_page_name(cache, capsys):
    # `section()` reserves nothing on an unpaginated page, so "page 2" is an
    # ordinary heading name there — a forum index headed "Page 2 of 5" must get
    # the near-heading hint, not a remark about PDF sheets it never had.
    url = wc.normalize_url("https://forum.example/thread")
    _seed(cache, url=url, text="# Page 2 of 5\nreplies", text_source="html")
    assert wc.main(["section", url, "page 2"]) == 1
    err = capsys.readouterr().err
    assert "did you mean: Page 2 of 5" in err
    assert "page markers" not in err


def test_cli_section_miss_on_a_page_map_does_not_recite_the_map(cache, capsys):
    # "page" is a substring of every row of a page map; offering all of them as
    # near misses would print the map instead of a hint.
    url = _seed_paged(cache)
    assert wc.main(["section", url, "sheet"]) == 1
    err = capsys.readouterr().err
    assert "did you mean" not in err
    assert "page 1" not in err


# --------------------------------------------------------------------------- #
# The blob path: every entry point that reports page-level facts prints it
# --------------------------------------------------------------------------- #


def test_cli_outline_prints_the_blob_path_on_stderr(cache, capsys):
    url = _seed_paged(cache)
    sha = _page(cache, url)["content_sha"]
    assert wc.main(["outline", url]) == 0
    captured = capsys.readouterr()
    assert captured.err == f"blob: {wc.blob_path(sha, 'pdf')}\n"
    assert "blob:" not in captured.out  # the map itself stays clean


def test_cli_outline_prints_the_blob_path_even_with_nothing_to_map(cache, capsys):
    # The path must precede the giving-up message, not replace it.
    url = wc.normalize_url("https://paged.example/scan-only.pdf")
    sha = _seed(cache, url=url, text=None, content_type="application/pdf")
    assert wc.main(["outline", url]) == 1
    err = capsys.readouterr().err
    assert f"blob: {wc.blob_path(sha, 'pdf')}" in err
    assert "no stored text" in err


def test_cli_outline_does_not_offer_a_blob_for_an_html_page(cache, capsys):
    # The stored markdown is the better read of an HTML blob.
    url = _seed_structured(cache)
    assert wc.main(["outline", url]) == 0
    assert capsys.readouterr().err == ""


def test_cli_get_prints_the_blob_path_after_the_stored_columns(cache, capsys):
    url = _seed_paged(cache)
    sha = _page(cache, url)["content_sha"]
    assert wc.main(["get", url]) == 0
    captured = capsys.readouterr()
    lines = captured.err.rstrip("\n").split("\n")
    assert lines[-1] == f"blob: {wc.blob_path(sha, 'pdf')}"
    assert any(ln.startswith("content_sha: ") for ln in lines)
    assert "blob:" not in captured.out  # `get > page.md` still lands the text


def test_cli_get_prints_a_blob_path_for_every_type_not_just_renderable_ones(
    cache, capsys
):
    url = wc.normalize_url("https://plain.example/article")
    sha = _seed(
        cache, url=url, text="body", content_type="text/html", text_source="html"
    )
    assert wc.main(["get", url]) == 0
    assert f"blob: {wc.blob_path(sha, 'html')}" in capsys.readouterr().err


def test_cli_get_omits_the_blob_line_when_the_type_maps_to_no_extension(cache, capsys):
    # Without an extension there is no `raw/<sha>.<ext>` to name, and a guess
    # would point at a file that isn't there.
    url = wc.normalize_url("https://odd.example/thing")
    _seed(cache, url=url, text="body", content_type="application/x-unknown")
    assert wc.main(["get", url]) == 0
    err = capsys.readouterr().err
    assert "blob:" not in err
    assert "content_sha: " in err


def test_cli_section_prints_the_blob_path_with_the_sheet(cache, capsys):
    url = _seed_paged(cache)
    sha = _page(cache, url)["content_sha"]
    assert wc.main(["section", url, "page 3"]) == 0
    captured = capsys.readouterr()
    assert captured.err == f"blob: {wc.blob_path(sha, 'pdf')}\n"
    assert captured.out.startswith("third sheet")  # stdout stays pure page text


def _write_blob(rec: wc.PageRow, size: int, ext: str = "pdf") -> Path:
    """Put a real file of `size` bytes where a seeded row's blob would live."""
    path = wc.blob_path(rec["content_sha"], ext)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    return path


def test_cli_section_blob_line_carries_the_size_when_the_blob_is_on_disk(cache, capsys):
    url = _seed_paged(cache)
    path = _write_blob(_page(cache, url), 2_500_000)
    assert wc.main(["section", url, "page 3"]) == 0
    assert capsys.readouterr().err == f"blob: {path}  (2.5MB)\n"


def test_cli_section_blob_line_omits_the_size_when_the_blob_is_absent(cache, capsys):
    # Blobs are R2-backed, so a checkout need not hold this one.
    url = _seed_paged(cache)
    sha = _page(cache, url)["content_sha"]
    assert wc.main(["section", url, "page 3"]) == 0
    assert capsys.readouterr().err == f"blob: {wc.blob_path(sha, 'pdf')}\n"


def test_oversized_pdf_prints_a_pdftoppm_handoff(cache, monkeypatch):
    monkeypatch.setattr(wc, "_READ_PDF_MAX_BYTES", 1_000)
    url = _seed_paged(cache)
    rec = _page(cache, url)
    path = _write_blob(rec, 1_001)
    line = wc._render_handoff_line(rec)
    assert line is not None
    out = f"/tmp/sheet-{rec['content_sha'][:8]}-pN"  # noqa: S108 — printed, not opened
    assert line.splitlines() == [
        f"blob: {path}  (1KB — over Read's 100MB cap)",
        "      render one sheet instead (same N in both places; "
        "-r 288 for a contested glyph):",
        f"      pdftoppm -f N -l N -r 144 -png -singlefile {path} {out}",
        f"      → Read {out}.png",
    ]


def test_pdftoppm_handoff_quotes_a_blob_path_with_a_space(cache, tmp_path, monkeypatch):
    # RAW_DIR follows the checkout, which can sit under a path with a space —
    # unquoted, the command hands pdftoppm three positional args.
    monkeypatch.setattr(wc, "_READ_PDF_MAX_BYTES", 1_000)
    monkeypatch.setattr(wc, "RAW_DIR", tmp_path / "my repo" / "raw")
    url = _seed_paged(cache)
    rec = _page(cache, url)
    path = _write_blob(rec, 1_001)
    line = wc._render_handoff_line(rec)
    assert line is not None
    assert f"-singlefile '{path}' /tmp/" in line


def test_pdftoppm_handoff_passes_singlefile(cache, monkeypatch):
    # Without it poppler pads the page suffix to the width of the document's
    # page count, so the printed output path names a file that isn't there.
    monkeypatch.setattr(wc, "_READ_PDF_MAX_BYTES", 1_000)
    url = _seed_paged(cache)
    rec = _page(cache, url)
    _write_blob(rec, 1_001)
    line = wc._render_handoff_line(rec)
    assert line is not None
    assert "-singlefile" in line


def test_pdf_under_the_cap_gets_no_pdftoppm_line(cache, monkeypatch):
    # Read is the render step for any PDF that fits one; an alternative beside
    # it would steer readers onto two calls where one works.
    monkeypatch.setattr(wc, "_READ_PDF_MAX_BYTES", 1_000)
    url = _seed_paged(cache)
    rec = _page(cache, url)
    _write_blob(rec, 1_000)
    line = wc._render_handoff_line(rec)
    assert line is not None
    assert "pdftoppm" not in line


def test_oversized_image_gets_no_pdftoppm_line(cache, monkeypatch):
    # An image blob is already the picture; it has no sheets to rasterize.
    monkeypatch.setattr(wc, "_READ_PDF_MAX_BYTES", 1_000)
    url = wc.normalize_url("https://flyer.example/scan.jpg")
    _seed(cache, url=url, content_type="image/jpeg", ocr_text="ink")
    rec = _page(cache, url)
    _write_blob(rec, 1_001, ext="jpg")
    line = wc._render_handoff_line(rec)
    assert line is not None
    assert "pdftoppm" not in line


def test_cli_section_blank_page_hint_says_where_the_sheet_is(cache, capsys):
    # The hint's claim that the sheet may still hold ink is only actionable
    # with the path beside it.
    url = _seed_paged(cache)
    sha = _page(cache, url)["content_sha"]
    assert wc.main(["section", url, "page 2"]) == 1
    err = capsys.readouterr().err
    assert err.startswith(f"blob: {wc.blob_path(sha, 'pdf')}\n")
    assert "no extracted text" in err


def test_cli_section_on_html_offers_no_blob_path(cache, capsys):
    url = _seed_structured(cache)
    assert wc.main(["section", url, "Intro"]) == 0
    assert capsys.readouterr().err == ""


# --------------------------------------------------------------------------- #
# search scopes: global, document, section
# --------------------------------------------------------------------------- #

# Three sheets, two of which say the word.
COIL_PAGES = (
    "first sheet mentions the coil once\n"
    "\f\n"
    "second sheet is quiet\n"
    "\f\n"
    "third sheet says coil and coil again\n"
    "\f"
)


def test_search_counts_matches_and_the_sections_they_fall_in(cache):
    url = _seed_paged(cache, COIL_PAGES)
    (hit,) = wc.search("coil", con=cache)
    assert (hit["matches"], hit["sections"]) == (3, 2)
    assert (hit["ocr_matches"], hit["ocr_sections"]) == (0, 0)
    assert hit["has_text"] is True
    assert hit["has_ocr"] is False
    # The two scopes must not disagree about how spread out a term is.
    assert hit["sections"] == len(wc.search_sections(url, "coil", con=cache))


def test_search_counts_every_matched_phrase_and_loose_word(cache):
    url = wc.normalize_url("https://a.com/mixed")
    _seed(cache, url=url, text="upper magnet, bananas bananas, upper magnet")
    (hit,) = wc.search('"upper magnet" bananas', con=cache)
    # Two phrase hits plus two loose words.
    assert hit["matches"] == 4


def test_search_withholds_a_count_from_a_document_holding_a_marker(cache):
    # Absent by measurement, not by rule, so a document could carry one. The
    # count is withheld; the other hit is answered as normal.
    poisoned = wc.normalize_url("https://a.com/poisoned")
    clean = wc.normalize_url("https://a.com/clean")
    _seed(cache, url=poisoned, text=f"coil{wc._MARK_OPEN} and coil")
    _seed(cache, url=clean, text="coil and coil")
    hits = {h["url"]: h for h in wc.search("coil", con=cache)}
    assert (hits[poisoned]["matches"], hits[poisoned]["sections"]) == (None, None)
    assert hits[clean]["matches"] == 2
    with pytest.raises(wc.MarkerCollisionError):
        wc.search_sections(poisoned, "coil", con=cache)


def test_search_tells_a_dark_row_from_one_whose_text_lacks_the_term(cache):
    # Both match on url alone and count zero, and are not the same fact: one
    # holds bytes nothing can read yet, the other just doesn't say it.
    dark = wc.normalize_url("https://a.com/uploads/dark.pdf")
    lit = wc.normalize_url("https://a.com/uploads/lit.pdf")
    _seed(cache, url=dark, text=None, content_type="application/pdf")
    _seed(cache, url=lit, text="This document never says the word.")
    hits = {h["url"]: h for h in wc.search("uploads", con=cache)}
    assert (hits[dark]["has_text"], hits[dark]["matches"]) == (False, 0)
    assert hits[dark]["snippet"] is None  # nothing to snippet — this used to crash
    assert (hits[lit]["has_text"], hits[lit]["matches"]) == (True, 0)


def test_search_limit_zero_returns_every_hit(cache):
    for i in range(3):
        _seed(cache, url=wc.normalize_url(f"https://a.com/{i}"), text="coil")
    assert len(wc.search("coil", limit=0, con=cache)) == 3
    assert len(wc.search("coil", limit=2, con=cache)) == 2


def test_search_sections_maps_a_paginated_document_by_sheet(cache):
    url = _seed_paged(cache, COIL_PAGES)
    rows = wc.search_sections(url, "coil", con=cache)
    # Document order, quiet sheets absent, counts per sheet.
    assert [(r["section"], r["matches"]) for r in rows] == [
        ("page 1", 1),
        ("page 3", 2),
    ]


def test_search_sections_follows_outlines_rule_on_an_unpaginated_pdf(cache):
    # No sheets to name, so it falls to headings — including whatever ATX line
    # the extractor misparsed. The names `outline` prints and `section` takes.
    url = _seed_paged(cache, "coil above\n\n# 6-32 x /4 Phil.M.S.\n\ncoil below")
    assert [e["heading"] for e in wc.outline(url, con=cache)] == ["6-32 x /4 Phil.M.S."]
    rows = wc.search_sections(url, "coil", con=cache)
    assert [(r["section"], r["matches"]) for r in rows] == [
        (None, 1),
        ("6-32 x /4 Phil.M.S.", 1),
    ]


def test_search_sections_sums_a_repeated_name_into_one_row(cache):
    # `section()` answers a repeated name with every block bearing it.
    url = wc.normalize_url("https://a.com/repeat")
    _seed(
        cache, url=url, text="# Specs\n\ncoil\n\n# Other\n\nx\n\n# Specs\n\ncoil coil"
    )
    rows = wc.search_sections(url, "coil", con=cache)
    assert [(r["section"], r["matches"]) for r in rows] == [("Specs", 3)]


def test_search_sections_collapses_unheaded_text_to_one_row(cache):
    url = wc.normalize_url("https://a.com/unheaded")
    _seed(cache, url=url, text="coil up here\n\n# Heading\n\ncoil under it")
    rows = wc.search_sections(url, "coil", con=cache)
    assert [(r["section"], r["matches"]) for r in rows] == [(None, 1), ("Heading", 1)]


def test_search_sections_names_the_frontmatter_metadata(cache):
    # `metadata` is an address `section()` already accepts, so a hit in the
    # assembly's frontmatter stays reachable.
    url = _seed_structured(cache)
    rows = wc.search_sections(url, "structured", con=cache)
    assert rows[0]["section"] == "metadata"


def test_search_matches_returns_verbatim_stored_lines(cache):
    url = wc.normalize_url("https://a.com/verbatim")
    text = "Flipper coil resistance should read 4.2 ohms across the winding."
    _seed(cache, url=url, text=text)
    (hit,) = wc.search_matches(url, "coil", con=cache)
    # Unmarked and unmodified, so any part of it can be lifted into a cite.
    assert hit["text"] == text
    assert wc._MARK_OPEN not in hit["text"]


def test_search_matches_sizes_the_window_in_words_and_returns_whole_lines(cache):
    url = wc.normalize_url("https://a.com/words")
    _seed(
        cache,
        url=url,
        text="one two three\nfour five six\ncoil here\nseven eight nine\nten more words",
    )
    (hit,) = wc.search_matches(url, "coil", surrounding_words=3, con=cache)
    assert hit["text"] == "four five six\ncoil here\nseven eight nine"
    # Zero words is the match's own line, still whole.
    (tight,) = wc.search_matches(url, "coil", surrounding_words=0, con=cache)
    assert tight["text"] == "coil here"


def test_search_matches_does_not_cross_a_blank_run_to_reach_its_words(cache):
    # Blank lines hold no words, so without the line cap the window walks the
    # gap for free and merges two distant matches.
    url = wc.normalize_url("https://a.com/gap")
    _seed(cache, url=url, text="coil one" + "\n" * 30 + "coil two")
    hits = wc.search_matches(url, "coil", surrounding_words=2, con=cache)
    assert [h["text"] for h in hits] == ["coil one", "coil two"]


def test_search_matches_keeps_a_lines_own_indentation(cache):
    # Verbatim includes a line's own indentation; only blank edge lines go.
    url = wc.normalize_url("https://a.com/indented")
    _seed(cache, url=url, text="# H\n\n    indented coil line\n    second line")
    (hit,) = wc.search_matches(url, "coil", surrounding_words=0, con=cache)
    assert hit["text"] == "    indented coil line"
    # `quote` returns the same span the same way — the two reads must not
    # disagree about what the stored text is.
    assert wc.quote(url, "indented coil", context=1, con=cache) == [
        "    indented coil line\n    second line"
    ]


def test_search_matches_never_truncates_a_phrase_at_a_line_break(cache):
    # FTS5 matches a phrase across a newline, so the match opens on one line
    # and closes on another. Reading only the first returns "the upper".
    url = wc.normalize_url("https://a.com/wrapped")
    _seed(cache, url=url, text="the upper\nmagnet holds the ball")
    (hit,) = wc.search_matches(url, '"upper magnet"', surrounding_words=0, con=cache)
    assert hit["text"] == "the upper\nmagnet holds the ball"


def test_search_matches_a_phrase_crossing_a_sheet_keeps_both_halves(cache):
    # Unlabelled, because no single section name is true of it — and marked as
    # straddling, since a paginated document has no unheaded region and calling
    # this "(no heading)" would be a false locator rather than a vague one.
    url = _seed_paged(cache, "upper\n\f\nmagnet\n\f")
    (hit,) = wc.search_matches(url, '"upper magnet"', surrounding_words=0, con=cache)
    assert hit["text"] == "upper\nmagnet"
    assert (hit["section"], hit["straddles"]) == (None, True)
    assert wc.quote_hits(url, "upper magnet", con=cache)[0]["text"] == hit["text"]
    # Counted once, so section counts still sum to the document total.
    assert wc.search_sections(url, '"upper magnet"', con=cache) == [
        {"section": "page 1", "matches": 1, "tier": "text"}
    ]


def test_search_matches_clips_a_window_to_its_own_section(cache):
    # A window never leaves the section it is filed under, so it can be
    # lifted with that section's locator.
    url = wc.normalize_url("https://a.com/clip")
    _seed(
        cache,
        url=url,
        text="# First\n\nsecret words here\n\n# Second\n\ncoil sits here",
    )
    (hit,) = wc.search_matches(url, "coil", surrounding_words=50, con=cache)
    assert hit["section"] == "Second"
    assert "secret" not in hit["text"]


def test_search_matches_merges_overlapping_windows_and_says_how_many(cache):
    url = wc.normalize_url("https://a.com/merge")
    _seed(cache, url=url, text="coil one\ncoil two\n\n\n\n\n\n\n\n\n\nfar away coil")
    hits = wc.search_matches(url, "coil", surrounding_words=2, con=cache)
    # The adjacent pair is one window carrying both; the distant one stays.
    assert [h["matches"] for h in hits] == [2, 1]
    assert hits[0]["text"] == "coil one\ncoil two"


def test_search_matches_drops_page_markers_from_a_window(cache):
    url = _seed_paged(cache, COIL_PAGES)
    hits = wc.search_matches(url, "coil", surrounding_words=50, con=cache)
    # A character no viewer renders is residue, not evidence.
    assert all("\f" not in h["text"] for h in hits)
    assert hits[0]["text"] == "first sheet mentions the coil once"


def test_search_matches_takes_a_section_name_or_a_sheet_range(cache):
    url = _seed_paged(cache, COIL_PAGES)
    assert [h["section"] for h in wc.search_matches(url, "coil", con=cache)] == [
        "page 1",
        "page 3",
    ]
    named = wc.search_matches(url, "coil", section="Page 3", con=cache)
    assert [h["section"] for h in named] == ["page 3"]  # matched like section()
    ranged = wc.search_matches(url, "coil", pages=(2, 3), con=cache)
    assert [h["section"] for h in ranged] == ["page 3"]
    # A range past the end is an answer, not an error.
    assert wc.search_matches(url, "coil", pages=(40, 500), con=cache) == []


def test_search_matches_addresses_unheaded_text_by_name(cache):
    url = wc.normalize_url("https://a.com/unheaded2")
    _seed(cache, url=url, text="coil up here\n\n# Heading\n\ncoil under it")
    hits = wc.search_matches(url, "coil", section=wc.NO_HEADING, con=cache)
    assert [h["text"] for h in hits] == ["coil up here"]
    # An unheaded region, not a straddling match — the CLI prints them apart.
    assert hits[0]["straddles"] is False


def test_search_matches_refuses_two_addresses_for_one_thing(cache):
    url = _seed_paged(cache, COIL_PAGES)
    with pytest.raises(ValueError, match="at most one"):
        wc.search_matches(url, "coil", section="page 1", pages=(1, 2), con=cache)


def test_search_scopes_return_nothing_for_a_term_with_nothing_to_match(cache):
    url = _seed_paged(cache, COIL_PAGES)
    assert wc.search_sections(url, "", con=cache) == []
    assert wc.search_matches(url, "", con=cache) == []


# --------------------------------------------------------------------------- #
# search scopes: CLI
# --------------------------------------------------------------------------- #


def test_cli_search_prints_the_count_beside_the_snippet(cache, capsys):
    _seed_paged(cache, COIL_PAGES)
    assert wc.main(["search", "coil"]) == 0
    out = capsys.readouterr().out
    assert "matches: 3 in 2 sections" in out
    assert "snippet: " in out  # the one-command answer is still there


def test_cli_search_labels_the_two_kinds_of_zero(cache, capsys):
    _seed(
        cache,
        url=wc.normalize_url("https://a.com/uploads/dark.pdf"),
        text=None,
        content_type="application/pdf",
    )
    _seed(cache, url=wc.normalize_url("https://a.com/uploads/lit.pdf"), text="nothing")
    assert wc.main(["search", "uploads"]) == 0  # and no traceback
    out = capsys.readouterr().out
    assert "matches: url/title match, no text layer" in out
    assert "matches: url/title match, 0 text matches" in out


def test_cli_search_labels_a_straddling_match_apart_from_an_unheaded_one(cache, capsys):
    # `(no heading)` on a paginated document would be a false locator: every
    # line there sits on a named sheet, so a cross-sheet match has too many
    # names rather than none. The label is also shaped to not look addressable.
    url = _seed_paged(cache, "upper\n\f\nmagnet\n\f")
    assert wc.main(["search", '"upper magnet"', "--url", url]) == 0
    out = capsys.readouterr().out
    assert "[section boundary]" in out
    assert wc.NO_HEADING not in out


def test_cli_search_document_scope_lists_sections_with_counts(cache, capsys):
    url = _seed_paged(cache, COIL_PAGES)
    assert wc.main(["search", "coil", "--url", url]) == 0
    out = capsys.readouterr().out
    assert "page 1                    1 match\n" in out
    assert "page 3                    2 matches\n" in out


def test_cli_search_document_scope_collapses_a_single_section(cache, capsys):
    # A one-row list would restate the global count, so the hop is skipped.
    url = wc.normalize_url("https://a.com/single")
    _seed(cache, url=url, text="the coil sits here alone")
    assert wc.main(["search", "coil", "--url", url]) == 0
    out = capsys.readouterr().out
    assert f"[{wc.NO_HEADING}]" in out
    assert "the coil sits here alone" in out


def test_cli_search_section_scope_reports_what_it_withheld(cache, capsys):
    url = wc.normalize_url("https://a.com/many")
    _seed(cache, url=url, text="\n\n\n".join(f"coil {i}" for i in range(6)))
    argv = ["search", "coil", "--url", url, "--limit", "2", "--surrounding-words", "0"]
    assert wc.main(argv) == 0
    captured = capsys.readouterr()
    assert captured.out.count("[") == 2
    # Silent truncation would read as "that's all of them".
    assert "4 more windows not shown" in captured.err


def test_cli_search_pages_on_an_unpaginated_document_says_so(cache, capsys):
    url = wc.normalize_url("https://a.com/nopages")
    _seed(cache, url=url, text="coil", content_type="application/pdf")
    assert wc.main(["search", "coil", "--url", url, "--pages", "1-5"]) == 1
    assert "no page markers in this document" in capsys.readouterr().err


def test_cli_search_a_miss_under_an_address_says_where_matches_are(cache, capsys):
    url = _seed_paged(cache, COIL_PAGES)
    assert wc.main(["search", "coil", "--url", url, "--section", "nope"]) == 1
    err = capsys.readouterr().err
    assert "sections with matches: page 1, page 3" in err


def test_cli_search_rejects_an_address_without_a_document(cache):
    # A section address across documents is meaningless; argparse exits 2.
    with pytest.raises(SystemExit) as exc:
        wc.main(["search", "coil", "--section", "page 1"])
    assert exc.value.code == 2


def test_cli_search_rejects_a_context_size_with_nothing_to_size(cache, capsys):
    # The global scope prints no match windows, so this cannot be honoured.
    _seed(cache, url=wc.normalize_url("https://a.com/x"), text="coil")
    with pytest.raises(SystemExit) as exc:
        wc.main(["search", "coil", "--surrounding-words", "99"])
    assert exc.value.code == 2
    assert "--surrounding-words needs --url" in capsys.readouterr().err


def test_cli_search_rejects_an_empty_section_rather_than_dropping_it(cache):
    # Falsy but typed: truthiness would silently answer the unscoped question.
    with pytest.raises(SystemExit) as exc:
        wc.main(["search", "coil", "--section", ""])
    assert exc.value.code == 2


def test_cli_search_reports_a_context_size_the_section_list_cannot_use(cache, capsys):
    # A document still needing an address prints no window, so the size asked
    # for cannot be honoured and must not be silently dropped.
    url = _seed_paged(cache, COIL_PAGES)
    argv = ["search", "coil", "--url", url, "--surrounding-words", "1"]
    assert wc.main(argv) == 0
    assert "--surrounding-words 1 ignored" in capsys.readouterr().err


def test_cli_search_rejects_an_empty_url_rather_than_searching_everything(cache):
    # An unset shell variable must not widen the scope to the whole corpus.
    _seed(cache, url=wc.normalize_url("https://a.com/x"), text="coil")
    with pytest.raises(SystemExit) as exc:
        wc.main(["search", "coil", "--url", ""])
    assert exc.value.code == 2


def test_cli_search_rejects_two_addresses_and_a_backwards_range(cache):
    url = _seed_paged(cache, COIL_PAGES)
    for argv in (
        ["search", "coil", "--url", url, "--section", "page 1", "--pages", "1-2"],
        ["search", "coil", "--url", url, "--pages", "50-2"],
        ["search", "coil", "--url", url, "--pages", "page 4"],
    ):
        with pytest.raises(SystemExit) as exc:
            wc.main(argv)
        assert exc.value.code == 2


# --------------------------------------------------------------------------- #
# The OCR tier — machine-read sheet text: findable, interleaved, not a text layer
# --------------------------------------------------------------------------- #

# Two sheets in each column, sheet counts agreeing, with `magnet` printed once
# as text-layer ink on sheet 1 and once as diagram ink (OCR-only) on sheet 2.
TWO_TIER_TEXT = "fuse table magnet\n\f\ncaption only\n\f"
TWO_TIER_OCR = "coil chart\n\f\nUPPER MAGNET Q12\n\f"


def _seed_two_tier(con) -> str:
    url = wc.normalize_url("https://pdf.example/two-tier.pdf")
    _seed(
        con,
        url=url,
        text=TWO_TIER_TEXT,
        ocr_text=TWO_TIER_OCR,
        content_type="application/pdf",
        text_source="pdf",
    )
    return url


def _seed_dark(con) -> str:
    url = wc.normalize_url("https://pdf.example/dark.pdf")
    _seed(
        con,
        url=url,
        text=None,
        ocr_text="dark sheet words\n\f\nmagnet on sheet two\n\f",
        content_type="application/pdf",
        text_source=None,
    )
    return url


def test_search_reports_each_tier_own_counts_on_one_row(cache):
    url = _seed_two_tier(cache)
    (hit,) = wc.search("magnet", con=cache)
    assert hit["url"] == url
    # One physical word per tier — never summed, never double-counted.
    assert (hit["matches"], hit["sections"]) == (1, 1)
    assert (hit["ocr_matches"], hit["ocr_sections"]) == (1, 1)
    assert hit["has_text"]
    assert hit["has_ocr"]
    # The text tier's snippet leads when it has matches.
    assert hit["snippet_tier"] == "text"


def test_search_finds_a_dark_document_through_its_ocr_tier(cache):
    url = _seed_dark(cache)
    (hit,) = wc.search("magnet", con=cache)
    assert hit["url"] == url
    assert (hit["matches"], hit["sections"]) == (0, 0)
    assert (hit["ocr_matches"], hit["ocr_sections"]) == (1, 1)
    assert hit["has_text"] is False
    assert hit["snippet_tier"] == "ocr"
    assert hit["snippet"] is not None


def test_search_backfills_the_tier_that_did_not_rank(cache):
    # A term matching only the ocr tier of one document and only the text tier
    # of another: each merged row still reports both tiers' counts.
    _seed_two_tier(cache)
    _seed_dark(cache)
    hits = {h["url"]: h for h in wc.search("magnet", con=cache)}
    assert len(hits) == 2
    for hit in hits.values():
        assert hit["ocr_matches"] is not None


def test_search_sections_interleaves_tiers_in_sheet_order(cache):
    url = _seed_two_tier(cache)
    rows = wc.search_sections(url, "magnet", con=cache)
    assert [(r["section"], r["tier"], r["matches"]) for r in rows] == [
        ("page 1", "text", 1),
        ("page 2", "ocr", 1),
    ]


def test_search_matches_windows_carry_their_tier(cache):
    url = _seed_two_tier(cache)
    hits = wc.search_matches(url, "magnet", con=cache)
    assert [(h["section"], h["tier"]) for h in hits] == [
        ("page 1", "text"),
        ("page 2", "ocr"),
    ]
    assert hits[1]["text"] == "UPPER MAGNET Q12"


def test_search_matches_pages_range_reaches_the_ocr_tier(cache):
    url = _seed_two_tier(cache)
    hits = wc.search_matches(url, "magnet", pages=(2, 2), con=cache)
    assert [(h["section"], h["tier"]) for h in hits] == [("page 2", "ocr")]


def test_ocr_only_terms_never_rank_the_text_tier(cache):
    # The two-index design: a term only the OCR holds must not surface a text
    # count, and vice versa.
    url = _seed_two_tier(cache)
    (hit,) = wc.search('"upper magnet"', con=cache)
    assert hit["url"] == url
    assert (hit["matches"], hit["ocr_matches"]) == (0, 1)
    assert hit["snippet_tier"] == "ocr"


def test_outline_maps_a_dark_document_by_its_ocr_pages(cache):
    url = _seed_dark(cache)
    assert wc.outline(url, con=cache) == [
        {
            "level": 0,
            "heading": "page 1",
            "chars": len("dark sheet words"),
            "count": 1,
            "tier": "ocr",
        },
        {
            "level": 0,
            "heading": "page 2",
            "chars": len("magnet on sheet two"),
            "count": 1,
            "tier": "ocr",
        },
    ]


def test_outline_prefers_the_text_layer_when_it_has_markers(cache):
    url = _seed_two_tier(cache)
    entries = wc.outline(url, con=cache)
    assert all(e["tier"] == "text" for e in entries)


def test_section_answers_page_names_from_the_ocr_map_on_a_dark_document(cache):
    url = _seed_dark(cache)
    assert wc.section(url, "page 2", con=cache) == [
        {"text": "magnet on sheet two", "tier": "ocr"}
    ]


def test_section_never_reads_ocr_when_the_text_layer_is_paginated(cache):
    # Text is primary: on a mixed document `page N` is the extracted sheet, and
    # the machine-read sheet never masquerades as it.
    url = _seed_two_tier(cache)
    assert wc.section(url, "page 2", con=cache) == [
        {"text": "caption only", "tier": "text"}
    ]


def test_quote_never_answers_from_the_ocr_tier(cache):
    # The architecture's line: quote verification happens against `text` only.
    url = _seed_dark(cache)
    assert wc.quote(url, "magnet on sheet two", con=cache) == []


def test_cli_search_labels_ocr_sections_and_windows(cache, capsys):
    url = _seed_two_tier(cache)
    assert wc.main(["search", "magnet", "--url", url]) == 0
    captured = capsys.readouterr()
    assert "page 2" in captured.out
    assert "(ocr)" in captured.out
    assert "machine-read" in captured.err  # the one-line tier note


def test_cli_search_prints_a_coverage_line_for_unocrd_pdfs(cache, capsys):
    _seed(
        cache,
        url=wc.normalize_url("https://pdf.example/unread.pdf"),
        text=None,
        content_type="application/pdf",
    )
    assert wc.main(["search", "zzz-no-such-term"]) == 1
    err = capsys.readouterr().err
    assert "not yet OCR'd" in err
    assert "web_pdfocr" in err


def test_upsert_keeps_ocr_text_while_the_bytes_are_unchanged(cache):
    url = wc.normalize_url("https://pdf.example/stale.pdf")
    _seed(cache, url=url, text="layer", content="v1", content_type="application/pdf")
    cache.execute("UPDATE pages SET ocr_text = 'a reading' WHERE url = ?", (url,))
    cache.commit()
    # Same bytes, writer with no opinion: the reading survives.
    _seed(cache, url=url, text="layer", content="v1", content_type="application/pdf")
    assert _page(cache, url)["ocr_text"] == "a reading"
    # Changed bytes: the reading no longer describes them and clears.
    _seed(cache, url=url, text="layer2", content="v2", content_type="application/pdf")
    assert _page(cache, url)["ocr_text"] is None


def test_legacy_ocr_text_source_rows_move_to_the_ocr_tier(tmp_path, monkeypatch):
    # The pre-ocr_text shape: machine-read words stored as `text` and labelled
    # text_source='ocr' (the imported Jurassic Park manual). Migration moves
    # the words to the tier built for them — and NULLing a text column on the
    # system-of-record takes the same safety copy a column drop does.
    web_dir = tmp_path / "web"
    web_dir.mkdir(parents=True)
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    con = wc.connect()
    wc.init_schema(con)
    url = wc.normalize_url("https://www.ipdb.org/files/1343/jp.pdf")
    _seed(
        con,
        url=url,
        text="imported ocr words",
        content_type="application/pdf",
        text_source="ocr",
    )
    wc.init_schema(con)  # a later open runs the migration

    row = wc.get(url, con=con)
    assert row is not None
    assert row["text"] is None
    assert row["ocr_text"] == "imported ocr words"
    assert row["text_source"] is None
    assert len(list(web_dir.glob(".cache.sqlite.bak-*"))) == 1
    # Idempotent: nothing matches any more, so no second backup.
    wc.init_schema(con)
    assert len(list(web_dir.glob(".cache.sqlite.bak-*"))) == 1
    # Both FTS indexes followed the move.
    assert wc.search("imported", con=con)[0]["ocr_matches"] == 1
    con.close()


def test_outline_appends_the_ocr_page_map_to_an_unpaginated_transcription(cache):
    # A hand-typed transcription has headings but no page markers, while the
    # OCR tier defines `page N` (see _page_doc). The map must list both, or
    # outline and section()/search would speak different address vocabularies.
    url = wc.normalize_url("https://pdf.example/transcribed.pdf")
    _seed(
        cache,
        url=url,
        text="# Rules\n\ntyped rules text",
        ocr_text="sheet one ink\n\f\nsheet two ink\n\f",
        content_type="application/pdf",
        text_source="manual",
    )
    entries = wc.outline(url, con=cache)
    assert [(e["heading"], e["tier"]) for e in entries] == [
        ("Rules", "text"),
        ("page 1", "ocr"),
        ("page 2", "ocr"),
    ]
    # And the appended addresses resolve where they claim to.
    assert wc.section(url, "page 2", con=cache) == [
        {"text": "sheet two ink", "tier": "ocr"}
    ]


def test_search_raises_on_a_broken_text_index_rather_than_answering_empty(cache):
    # Only the missing-OCR-schema case is tolerated (a pre-migration cache); a
    # genuinely broken index must raise, because an empty result here reads as
    # a considered "no matches" about the corpus.
    _seed(cache, url=wc.normalize_url("https://a.com/x"), text="coil words")
    cache.executescript("DROP TRIGGER pages_au; DROP TABLE pages_fts;")
    with pytest.raises(sqlite3.OperationalError):
        wc.search("coil", con=cache)
    with pytest.raises(sqlite3.OperationalError):
        wc.search_sections("https://a.com/x", "coil", con=cache)


def test_search_tolerates_a_cache_from_before_the_ocr_tier(tmp_path, monkeypatch):
    # A pulled pre-migration cache read through a read-only connection: the
    # text tier answers, the absent OCR tier contributes nothing, and nothing
    # raises. The first writable open migrates.
    web_dir = tmp_path / "web"
    web_dir.mkdir(parents=True)
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    con = sqlite3.connect(wc.DB_PATH)
    con.executescript(
        """
        CREATE TABLE pages (
          url TEXT PRIMARY KEY, raw_url TEXT, content_sha TEXT NOT NULL,
          first_fetched_at TEXT NOT NULL, last_fetched_at TEXT NOT NULL,
          last_updated TEXT, title TEXT, http_status INTEGER, content_type TEXT,
          text TEXT, rendered INTEGER, text_source TEXT, imported INTEGER);
        CREATE VIRTUAL TABLE pages_fts USING fts5(
          url, title, text, content='pages', content_rowid='rowid');
        INSERT INTO pages VALUES ('https://a.com/x', NULL, 's', 't', 't', NULL,
          'A page', 200, 'text/html', 'coil words here', NULL, 'html', NULL);
        INSERT INTO pages_fts(rowid, url, title, text)
          SELECT rowid, url, title, text FROM pages;
        """
    )
    con.commit()
    con.row_factory = sqlite3.Row
    (hit,) = wc.search("coil", con=con)
    assert hit["url"] == "https://a.com/x"
    assert (hit["matches"], hit["ocr_matches"]) == (1, 0)
    assert wc.search_sections("https://a.com/x", "coil", con=con) == [
        {"section": None, "matches": 1, "tier": "text"}
    ]
    con.close()


def test_outline_withholds_page_shaped_headings_the_ocr_map_reserves(cache):
    # A transcription's "# Page 1" heading names something section() will
    # never resolve to it once the OCR map owns the page namespace — listing
    # it would show an address that resolves to different text. Same rule as
    # a text-paginated document, where such headings are never listed either.
    url = wc.normalize_url("https://pdf.example/transcribed-paged.pdf")
    _seed(
        cache,
        url=url,
        text="# Page 1\n\ntyped first page\n\n# Rules\n\ntyped rules",
        ocr_text="sheet one ink\n\f\nsheet two ink\n\f",
        content_type="application/pdf",
        text_source="manual",
    )
    entries = wc.outline(url, con=cache)
    assert [(e["heading"], e["tier"]) for e in entries] == [
        ("Rules", "text"),
        ("page 1", "ocr"),
        ("page 2", "ocr"),
    ]
    # The invariant the filter protects: every listed name resolves to the
    # text it was listed for.
    assert wc.section(url, "page 1", con=cache) == [
        {"text": "sheet one ink", "tier": "ocr"}
    ]
    assert wc.section(url, "Rules", con=cache) == [
        {"text": "# Rules\n\ntyped rules", "tier": "text"}
    ]


# --------------------------------------------------------------------------- #
# links: the navigation read — outbound links derived from the stored blob
# --------------------------------------------------------------------------- #


def _seed_html(
    con: sqlite3.Connection, url: str, html: str, *, raw_url: str | None = None
) -> str:
    """Seed a page and write its blob, which `_seed` alone does not."""
    body = html.encode()
    normalized = wc.normalize_url(url)
    _seed(
        con,
        url=normalized,
        raw_url=raw_url or url,
        content=body,
        content_type="text/html",
        text_source="html",
    )
    path = wc.blob_path(wc.content_sha(body), ext="html")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return normalized


def test_links_resolves_relative_hrefs_and_normalizes_to_cache_keys(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<a href="/manuals/x.pdf">X Manual</a>'
        '<a href="https://other.com/y.pdf?utm_source=nav">Y Manual</a>',
    )
    found = wc.links(url, con=cache)
    assert [(link["url"], link["anchor"], link["ext"]) for link in found] == [
        ("https://a.com/manuals/x.pdf", "X Manual", "pdf"),
        ("https://other.com/y.pdf", "Y Manual", "pdf"),
    ]


def test_links_resolve_against_the_unnormalized_fetch_address(cache):
    # normalize_url strips a trailing slash, and `manual.pdf` means something
    # different with and without it — so resolution uses raw_url, not the key.
    url = _seed_html(
        cache,
        "https://a.com/support/",
        '<a href="manual.pdf">Manual</a>',
        raw_url="https://a.com/support/",
    )
    assert url == "https://a.com/support"  # the key really did lose the slash
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/support/manual.pdf"
    ]


def test_links_never_resolve_against_a_scheme_less_stored_address(cache):
    # normalize_url accepts a scheme-less address and web_fetch stores raw_url
    # verbatim, so such a base would resolve "/manuals/x.pdf" host-less.
    url = _seed_html(
        cache,
        "https://a.com/support/",
        '<a href="/manuals/x.pdf">X</a>',
        raw_url="a.com/support/",
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/manuals/x.pdf"
    ]


def test_links_fall_back_to_the_stored_url_when_the_page_redirected(cache):
    # raw_url holds what was requested, which on a redirect is another page.
    url = _seed_html(
        cache,
        "https://a.com/final/page",
        '<a href="manual.pdf">Manual</a>',
        raw_url="https://a.com/old/address",
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/final/manual.pdf"
    ]


def test_links_honor_a_base_href(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<head><base href="https://cdn.a.com/docs/"></head>'
        '<body><a href="manual.pdf">Manual</a></body>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://cdn.a.com/docs/manual.pdf"
    ]


def test_links_take_the_first_base_that_carries_an_href(cache):
    # `<base target=_blank>` is legal and common; the href-bearing one wins.
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<head><base target="_blank"><base href="https://cdn.a.com/docs/"></head>'
        '<body><a href="manual.pdf">Manual</a></body>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://cdn.a.com/docs/manual.pdf"
    ]


def test_links_survive_a_malformed_base_href(cache):
    # urljoin raises on an invalid IPv6 literal, which would otherwise abort
    # extraction for the whole page.
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<head><base href="http://[::1"></head>'
        '<body><a href="/manual.pdf">Manual</a></body>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/manual.pdf"
    ]


def test_links_ignore_stylesheets_and_images_but_keep_image_maps(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<head><link rel="stylesheet" href="/site.css"></head>'
        '<body><img src="/logo.png">'
        '<map><area href="/manuals/a.pdf" alt="A"></map>'
        '<a href="/manuals/b.pdf">B</a></body>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/manuals/a.pdf",
        "https://a.com/manuals/b.pdf",
    ]


def test_links_drop_non_web_schemes_and_addresses_of_the_page_itself(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<a href="mailto:x@a.com">Mail</a>'
        '<a href="tel:+1">Call</a>'
        '<a href="javascript:void(0)">Menu</a>'
        '<a href="#top">Back to top</a>'
        '<a href="">Empty</a>'
        '<a href="https://a.com/support">This page</a>'
        '<a href="/real.pdf">Real</a>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/real.pdf"
    ]


def test_links_dedupe_by_target_keeping_the_first_anchor_text(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<a href="/manual.pdf">Operations Manual</a>'
        '<a href="/manual.pdf?utm_source=footer">click here</a>',
    )
    found = wc.links(url, con=cache)
    assert [(link["url"], link["anchor"]) for link in found] == [
        ("https://a.com/manual.pdf", "Operations Manual")
    ]


def test_links_survive_an_unparseable_href(cache):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<a href="https://a.com:999999999/x.pdf">Bad</a><a href="/good.pdf">Good</a>',
    )
    assert [link["url"] for link in wc.links(url, con=cache)] == [
        "https://a.com/good.pdf"
    ]


def test_links_returns_empty_when_the_blob_is_missing(cache):
    # `_seed` writes no blob, so this row's bytes are gone from disk.
    url = wc.normalize_url("https://a.com/support")
    _seed(cache, url=url, content="<a href='/x.pdf'>X</a>", content_type="text/html")
    assert wc.links(url, con=cache) == []


def _seed_pdf(con: sqlite3.Connection, url: str, body: bytes) -> str:
    """Seed a PDF row and its blob — a type whose handler yields no links."""
    normalized = wc.normalize_url(url)
    _seed(con, url=normalized, content=body, content_type="application/pdf")
    path = wc.blob_path(wc.content_sha(body), ext="pdf")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return normalized


def test_links_returns_empty_for_a_type_with_no_link_structure(cache, make_pdf):
    url = _seed_pdf(cache, "https://a.com/manual.pdf", make_pdf())
    assert wc.links(url, con=cache) == []


def test_cli_links_ext_filter_still_reports_the_buckets_it_excluded(cache, capsys):
    url = _seed_html(
        cache,
        "https://a.com/support",
        '<a href="/a.pdf">A</a><a href="/b.pdf">B</a>'
        '<a href="/download?id=7">Hidden manual</a><a href="/c.zip">C</a>',
    )
    assert wc.main(["links", url, "--ext", "pdf"]) == 0
    captured = capsys.readouterr()
    assert captured.out.count("\n") == 2
    assert "pdf:2" in captured.err
    assert "(none):1" in captured.err  # the extensionless one it did NOT show
    assert "zip:1" in captured.err


def test_cli_links_stdout_is_pure_tsv(cache, capsys):
    url = _seed_html(cache, "https://a.com/s", '<a href="/a.pdf">A Manual</a>')
    assert wc.main(["links", url]) == 0
    captured = capsys.readouterr()
    assert captured.out == "https://a.com/a.pdf\tA Manual\n"
    assert "unique outbound links" in captured.err


def test_cli_links_anchor_text_cannot_break_the_tsv(cache, capsys):
    url = _seed_html(
        cache, "https://a.com/s", '<a href="/a.pdf">Operations\tManual\n(rev B)</a>'
    )
    assert wc.main(["links", url]) == 0
    out = capsys.readouterr().out
    assert out == "https://a.com/a.pdf\tOperations Manual (rev B)\n"
    assert out.count("\t") == 1


def test_cli_links_host_keeps_only_that_host(cache, capsys):
    url = _seed_html(
        cache,
        "https://a.com/s",
        '<a href="/in.pdf">In</a>'
        '<a href="https://cdn.a.com/x.pdf">CDN</a>'
        '<a href="https://b.com/out.pdf">Out</a>',
    )
    assert wc.main(["links", url, "--host", "cdn.a.com"]) == 0
    # An exact host match, not a suffix one: a.com must not sweep in cdn.a.com.
    assert capsys.readouterr().out == "https://cdn.a.com/x.pdf\tCDN\n"


def test_cli_links_selects_the_extensionless_bucket_by_name(cache, capsys):
    url = _seed_html(
        cache, "https://a.com/s", '<a href="/a.pdf">A</a><a href="/dl?id=7">B</a>'
    )
    assert wc.main(["links", url, "--ext", "none"]) == 0
    assert capsys.readouterr().out == "https://a.com/dl?id=7\tB\n"


def test_cli_links_truncates_at_limit_and_says_so(cache, capsys):
    url = _seed_html(
        cache,
        "https://a.com/s",
        "".join(f'<a href="/{i}.pdf">Doc {i}</a>' for i in range(5)),
    )
    assert wc.main(["links", url, "--limit", "2"]) == 0
    captured = capsys.readouterr()
    assert captured.out.count("\n") == 2
    assert "showing 2 of 5" in captured.err


def test_cli_links_external_keeps_only_offsite_targets(cache, capsys):
    url = _seed_html(
        cache,
        "https://a.com/s",
        '<a href="/inside.pdf">In</a><a href="https://b.com/out.pdf">Out</a>',
    )
    assert wc.main(["links", url, "--external"]) == 0
    assert capsys.readouterr().out == "https://b.com/out.pdf\tOut\n"


def test_cli_links_on_a_pdf_names_the_type_rather_than_claiming_none(
    cache, capsys, make_pdf
):
    url = _seed_pdf(cache, "https://a.com/manual.pdf", make_pdf())
    assert wc.main(["links", url]) == 1
    assert "pdf documents carry no links" in capsys.readouterr().err


def test_cli_links_reports_a_missing_blob_apart_from_an_empty_page(cache, capsys):
    # `_seed` writes no blob — a broken checkout, not a page without links.
    url = wc.normalize_url("https://a.com/support")
    _seed(cache, url=url, content="<a href='/x.pdf'>X</a>", content_type="text/html")
    assert wc.main(["links", url]) == 1
    assert "blob missing" in capsys.readouterr().err


def test_cli_have_reads_urls_from_stdin(cache, capsys, monkeypatch):
    import io

    url = _seed_html(cache, "https://a.com/held.pdf", "<a href='/x'>x</a>")
    monkeypatch.setattr("sys.stdin", io.StringIO(f"{url}\nhttps://a.com/absent\n"))
    assert wc.main(["have", "--from-file", "-"]) == 1
    captured = capsys.readouterr()
    assert f"cached   {url}" in captured.out
    assert "MISSING  https://a.com/absent" in captured.out
