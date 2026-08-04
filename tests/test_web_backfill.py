"""Tests for web_backfill — re-extraction of cached HTML from stored blobs.

All offline: rows and blobs are seeded into the tmp cache, then the backfill
runs against that connection. What matters here is the *selection contract*
(which rows a re-extraction may touch) and the guards, not the extraction
itself — that is test_content_types' job.
"""

from __future__ import annotations

import web_backfill
import web_cache as wc


def _row(cache, url: str) -> wc.PageRow:
    """Fetch a page row the test expects to exist, narrowing away None."""
    row = wc.get(url, con=cache)
    assert row is not None, f"expected a stored page for {url}"
    return row


def _seed_blob(html: bytes) -> str:
    """Write an HTML blob into the tmp raw dir; return its content sha."""
    sha = wc.content_sha(html)
    path = wc.blob_path(sha, ext="html")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(html)
    return sha


def _seed_row(
    cache,
    url: str,
    html: bytes,
    *,
    text: str | None = "old extracted text",
    text_source: str | None = None,
    content_type: str = "text/html",
    title: str | None = "old title",
    write_blob: bool = True,
) -> str:
    sha = _seed_blob(html) if write_blob else wc.content_sha(html)
    url = wc.normalize_url(url)
    wc.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        last_updated="2023-01-01",
        title=title,
        http_status=200,
        content_type=content_type,
        text=text,
        text_source=text_source,
    )
    return url


PAGE = (
    b"<html><head><title>New Title</title></head>"
    b"<body><footer><p>1510 Webster Street</p></footer></body></html>"
)


def test_backfill_rewrites_html_and_null_rows_and_stamps_source(cache):
    url_html = _seed_row(cache, "https://a.example/html", PAGE, text_source="html")
    url_null = _seed_row(cache, "https://a.example/null", PAGE, text_source=None)
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 2
    for url in (url_html, url_null):
        row = _row(cache, url)
        assert "1510 Webster Street" in (row["text"] or "")
        assert row["title"] == "New Title"
        assert row["text_source"] == "html"  # a fact about the new text
        assert row["last_updated"] == "2023-01-01"  # untouched


def test_backfill_skips_foreign_text_sources(cache):
    url_manual = _seed_row(
        cache,
        "https://a.example/manual",
        PAGE,
        text="a human transcription",
        text_source="manual",
    )
    url_ocr = _seed_row(
        cache,
        "https://a.example/ocr",
        PAGE,
        text="machine-read import text",
        text_source="ocr",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 0
    assert tally["skipped (manual)"] == 1
    assert tally["skipped (other text_source)"] == 1
    manual = _row(cache, url_manual)
    ocr = _row(cache, url_ocr)
    assert manual["text"] == "a human transcription"
    assert manual["text_source"] == "manual"
    assert ocr["text"] == "machine-read import text"
    assert ocr["text_source"] == "ocr"


def test_backfill_ignores_non_html_rows(cache):
    url = _seed_row(
        cache,
        "https://a.example/doc.pdf",
        b"%PDF-1.4 junk",
        text="pdf text layer",
        text_source="pdf",
        content_type="application/pdf",
    )
    tally = web_backfill.backfill(con=cache)
    assert all(count == 0 for count in tally.values())
    assert _row(cache, url)["text"] == "pdf text layer"


def test_backfill_never_blanks_nonempty_text(cache, capsys):
    # An empty blob re-extracts to nothing; the stored text must survive —
    # re-running cannot recover from a blanking bug.
    url = _seed_row(cache, "https://a.example/blank", b"", text="precious text")
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 0
    assert tally["skipped (empty re-extraction)"] == 1
    assert "keeping stored text" in capsys.readouterr().err
    assert _row(cache, url)["text"] == "precious text"


def test_backfill_skips_missing_blob(cache, capsys):
    url = _seed_row(
        cache, "https://a.example/lost", PAGE, text="still here", write_blob=False
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["skipped (missing blob)"] == 1
    assert "blob missing" in capsys.readouterr().err
    assert _row(cache, url)["text"] == "still here"


def test_backfill_warns_on_new_replacement_characters(cache, capsys):
    # A blob whose declared charset can't decode a byte: U+FFFD appears where
    # the stored text (decoded at fetch time with the header charset) had none.
    mojibake = (
        b'<html><head><meta charset="utf-8"><title>T</title></head>'
        b"<body><p>caf\xe9 latin-1 byte</p></body></html>"
    )
    _seed_row(cache, "https://a.example/moji", mojibake, text="cafe latin-1 byte")
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 1  # rewritten, but loudly
    assert "U+FFFD" in capsys.readouterr().err


def test_backfill_writes_no_fetch_rows_and_updates_fts(cache):
    _seed_row(cache, "https://a.example/fts", PAGE)
    before = cache.execute("SELECT COUNT(*) FROM fetches").fetchone()[0]
    web_backfill.backfill(con=cache)
    after = cache.execute("SELECT COUNT(*) FROM fetches").fetchone()[0]
    assert after == before  # no fetch happened, none is logged
    # The UPDATE went through the pages triggers, so FTS sees the new text.
    hits = wc.search("Webster", con=cache)
    assert [h["url"] for h in hits] == [wc.normalize_url("https://a.example/fts")]
