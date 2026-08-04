"""Tests for web_cache: URL normalization, hashing, FTS, and the versioned store."""

from __future__ import annotations

import hashlib
import sqlite3

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
    _seed(cache, url=url, title="Structured Doc", text=STRUCTURED_TEXT)
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
    assert repeated["chars"] == sum(len(b) for b in blocks)
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
        len(b) for b in blocks
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
        }
    ]


def test_section_returns_block_until_same_or_higher_heading(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "machine list", con=cache)  # case-insensitive
    # Duplicate headings: every matching block returned, document order.
    assert len(blocks) == 2
    # The first block leads with its heading line and includes its subsection,
    # stopping at the page's own "## body" (same level).
    assert blocks[0].startswith("## Machine List")
    assert "### Sub List" in blocks[0]
    assert "deep item" in blocks[0]
    assert "own h2 named body" not in blocks[0]
    assert blocks[1] == "## Machine List\n\nSecond list section."


def test_section_body_pseudo_spans_whole_document_body(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "body", con=cache)
    # Two blocks: the body pseudo-section (everything after the frontmatter)
    # and the page's own h2 named "body" — ambiguity surfaces rather than
    # silently picking one.
    assert len(blocks) == 2
    body_block = blocks[0]
    assert body_block.startswith("# Intro")  # no marker line, just the body
    assert "Second list section." in body_block  # spans to EOF
    own_h2_block = blocks[1]
    assert own_h2_block.startswith("## body")
    assert "own h2 named body" in own_h2_block
    assert "Second list section." not in own_h2_block  # closed by ## Machine List


def test_section_metadata_is_frontmatter_lines_without_delimiters(cache):
    url = _seed_structured(cache)
    blocks = wc.section(url, "metadata", con=cache)
    assert len(blocks) == 1
    assert blocks[0] == "title: Structured Doc\nog:description: A test document."


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
    assert wc.section(url, hit["heading"], con=cache)[0].startswith("### Sub List")


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
                assert any(hit["text"] in b for b in blocks), (
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

    # An unparseable URL never reaches the miss list: the fetcher can't take it
    # either, so emitting it would just turn a bad line into a failed fetch.
    assert wc.main(["have", "--from-file", str(listing), "--missing"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "https://absent.example/b\tquery three\n"
    assert "unparseable URL" in captured.err


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


def test_cli_have_missing_prints_bare_urls_for_web_fetch(cache, capsys, tmp_path):
    held = wc.normalize_url("https://held.example/a")
    _seed(cache, url=held, text="body")
    listing = tmp_path / "urls.txt"
    # Comments and a TSV query column, exactly as web_fetch --from-file takes.
    listing.write_text(
        f"# campaign sources\n{held}\thow it works\n\n"
        "https://absent.example/b\twhy it matters\n",
        encoding="utf-8",
    )
    assert wc.main(["have", "--from-file", str(listing), "--missing"]) == 1
    # The source line verbatim, so the output is a valid web_fetch --from-file
    # that still carries the search intent the campaign recorded — a bare URL
    # would refetch the same page while dropping its provenance.
    assert capsys.readouterr().out == "https://absent.example/b\twhy it matters\n"


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
