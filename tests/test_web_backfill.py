"""Tests for web_backfill — re-extraction of cached documents from stored blobs.

All offline: rows and blobs are seeded into the tmp cache, then the backfill
runs against that connection. What matters here is the *selection contract*
(which rows a re-extraction may touch) and the guards, not the extraction
itself — that is test_content_types' job. The PDF cases need poppler, since a
selection contract that never runs the extractor proves nothing about it.
"""

from __future__ import annotations

import shutil

import pytest
import web_backfill
import web_cache as wc
from content_types import extension_for

needs_poppler = pytest.mark.skipif(
    shutil.which("pdftotext") is None,
    reason="poppler's pdftotext is not installed on this host",
)


def _row(cache, url: str) -> wc.PageRow:
    """Fetch a page row the test expects to exist, narrowing away None."""
    row = wc.get(url, con=cache)
    assert row is not None, f"expected a stored page for {url}"
    return row


def _seed_blob(body: bytes, ext: str = "html") -> str:
    """Write a blob into the tmp raw dir under its type's extension; return its sha.

    The extension is not cosmetic: the backfill re-opens a blob by the extension
    its content type declares, so a PDF seeded as ``.html`` reads as missing.
    """
    sha = wc.content_sha(body)
    path = wc.blob_path(sha, ext=ext)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return sha


def _seed_row(
    cache,
    url: str,
    body: bytes,
    *,
    text: str | None = "old extracted text",
    text_source: str | None = None,
    content_type: str = "text/html",
    title: str | None = "old title",
    write_blob: bool = True,
) -> str:
    ext = extension_for(content_type) or "html"
    sha = _seed_blob(body, ext) if write_blob else wc.content_sha(body)
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


def test_backfill_skips_a_row_whose_charset_only_the_header_knew(cache):
    # The HTTP charset is never stored, so a page that declared none and isn't
    # utf-8 can only be re-read by detection — whose guess carries no U+FFFD for
    # the regression guard below to catch. Keep the fetch-time text instead of
    # rewriting "Réglage" as a plausible-looking "RÈglage".
    body = "<html><body><p>Réglage du flipper « Sonic »</p></body></html>"
    url = _seed_row(
        cache,
        "https://a.example/cp1252",
        body.encode("cp1252"),
        text="Réglage du flipper « Sonic »",  # what the header-bearing fetch stored
        text_source="html",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 0
    assert tally["skipped (charset unrecoverable)"] == 1
    assert _row(cache, url)["text"] == "Réglage du flipper « Sonic »"


def test_backfill_rewrites_a_row_whose_blob_declares_its_charset(cache):
    # The same bytes, once the page states its own encoding: recoverable from
    # the blob alone, so the re-extraction runs and stays faithful.
    body = (
        '<html><head><meta charset="windows-1252"><title>T</title></head>'
        "<body><p>Réglage du flipper</p></body></html>"
    )
    url = _seed_row(
        cache, "https://a.example/declared", body.encode("cp1252"), text_source="html"
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 1
    assert "Réglage du flipper" in (_row(cache, url)["text"] or "")


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


@needs_poppler
def test_backfill_rewrites_pdf_rows(cache, make_pdf):
    url = _seed_row(
        cache,
        "https://a.example/flyer.pdf",
        make_pdf(text="9 Stand-Up Targets", title="Flyer"),
        text="text from the old extractor",
        text_source="pdf",
        content_type="application/pdf",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 1
    row = _row(cache, url)
    # The trailing "\f" line is the one-page document's page marker.
    assert row["text"] == "9 Stand-Up Targets\n\f"
    assert row["text_source"] == "pdf"


@needs_poppler
def test_backfill_leaves_ocr_text_on_a_pdf_row_alone(cache, make_pdf):
    # A scan is OCR'd precisely because its text layer is empty, so re-extracting
    # would trade recovered evidence for nothing.
    url = _seed_row(
        cache,
        "https://a.example/scan.pdf",
        make_pdf(text=""),
        text="OCR'd from the scan by hand",
        text_source="ocr",
        content_type="application/pdf",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["skipped (other text_source)"] == 1
    row = _row(cache, url)
    assert row["text"] == "OCR'd from the scan by hand"
    assert row["text_source"] == "ocr"


def test_backfill_ignores_types_that_are_not_backfillable(cache):
    # Images declare backfillable=False, so no PDF extractor change churns them.
    url = _seed_row(
        cache,
        "https://a.example/flyer.jpg",
        b"\xff\xd8\xff junk",
        text="OCR draft",
        text_source="ocr",
        content_type="image/jpeg",
    )
    tally = web_backfill.backfill(con=cache)
    assert all(count == 0 for count in tally.values())
    assert _row(cache, url)["text"] == "OCR draft"


def test_backfill_keeps_a_curated_title_on_an_imported_html_row(cache):
    # `--title` leaves text_source as the handler's own, so text_source can't
    # protect a curated title and `imported` must. The text still refreshes.
    url = _seed_row(cache, "https://a.example/imported", PAGE, text_source="html")
    cache.execute(
        "UPDATE pages SET title = ?, imported = 1 WHERE url = ?",
        ("A Curated Title", url),
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 1
    row = _row(cache, url)
    assert row["title"] == "A Curated Title"  # not the blob's <title>
    assert "1510 Webster Street" in (row["text"] or "")  # text still refreshed


@needs_poppler
def test_backfill_keeps_a_curated_title_on_an_imported_pdf_row(cache, make_pdf):
    # A PDF's declared title is often worse than useless: Stern's feature matrix
    # carries the internal codename "CHEDDAR Matrix" in its Info dict.
    url = _seed_row(
        cache,
        "https://a.example/matrix.pdf",
        make_pdf(text="Feature matrix", title="CHEDDAR Matrix"),
        text_source="pdf",
        content_type="application/pdf",
    )
    cache.execute(
        "UPDATE pages SET title = ?, imported = 1 WHERE url = ?",
        ("Transformers MTMTE Feature Matrix (Stern, 2026)", url),
    )
    web_backfill.backfill(con=cache)
    row = _row(cache, url)
    assert row["title"] == "Transformers MTMTE Feature Matrix (Stern, 2026)"
    assert row["text"] == "Feature matrix\n\f"


def test_backfill_rewrites_the_title_on_a_fetched_row(cache):
    # The flip side, so the guard above can't quietly freeze every title.
    url = _seed_row(cache, "https://a.example/fetched", PAGE, text_source="html")
    web_backfill.backfill(con=cache)
    assert _row(cache, url)["title"] == "New Title"


def test_backfill_writes_nothing_when_the_extractor_is_unavailable(
    cache, make_pdf, monkeypatch
):
    # A poppler-less host must change nothing. The never-blank guard doesn't
    # cover this: the stored text is already empty, like a scan's. Title/date
    # come from pypdf and still resolve, but half an extraction is not one.
    import web_pdftext

    def _unavailable(_raw: bytes) -> str | None:
        raise web_pdftext.PdfTextUnavailableError("no poppler on this host")

    monkeypatch.setattr(web_pdftext, "pdf_text", _unavailable)
    url = _seed_row(
        cache,
        "https://a.example/scan.pdf",
        make_pdf(text="", title="New Title From The Info Dict"),
        text=None,
        text_source="pdf",
        title="the title already stored",
        content_type="application/pdf",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 0
    assert tally["skipped (extractor unavailable)"] == 1
    row = _row(cache, url)
    assert row["text"] is None
    assert row["title"] == "the title already stored"


def test_backfill_does_not_apply_the_charset_guard_to_binary_types(
    cache, make_pdf, monkeypatch
):
    # A PDF has no header charset to lose and no refetch that would change its
    # output, so the guard could only strand the row. Stubbed because the subject
    # is the guard's scope, not how U+FFFD got there.
    import web_pdftext

    monkeypatch.setattr(web_pdftext, "pdf_text", lambda _raw: "unmapped glyph � here")
    url = _seed_row(
        cache,
        "https://a.example/glyphs.pdf",
        make_pdf(),
        text="text with no replacement char",
        text_source="pdf",
        content_type="application/pdf",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["skipped (decode regression)"] == 0
    assert tally["rewritten"] == 1
    assert "�" in (_row(cache, url)["text"] or "")


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


def test_backfill_skips_on_decode_regression(cache, capsys):
    # A blob whose declared charset can't decode a byte: U+FFFD appears where
    # the stored text (decoded at fetch time with the header charset) had
    # none. The stored text is the only artifact encoding the correct
    # charset, so the row is skipped — a --force refetch is the fix.
    mojibake = (
        b'<html><head><meta charset="utf-8"><title>T</title></head>'
        b"<body><p>caf\xe9 latin-1 byte</p></body></html>"
    )
    url = _seed_row(cache, "https://a.example/moji", mojibake, text="cafe latin-1 byte")
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 0
    assert tally["skipped (decode regression)"] == 1
    assert "U+FFFD" in capsys.readouterr().err
    assert _row(cache, url)["text"] == "cafe latin-1 byte"


def test_backfill_includes_xhtml_rows(cache):
    url = _seed_row(
        cache,
        "https://a.example/xhtml",
        PAGE,
        content_type="application/xhtml+xml",
    )
    tally = web_backfill.backfill(con=cache)
    assert tally["rewritten"] == 1
    assert "1510 Webster Street" in (_row(cache, url)["text"] or "")


def test_backfill_writes_no_fetch_rows_and_updates_fts(cache):
    _seed_row(cache, "https://a.example/fts", PAGE)
    before = cache.execute("SELECT COUNT(*) FROM fetches").fetchone()[0]
    web_backfill.backfill(con=cache)
    after = cache.execute("SELECT COUNT(*) FROM fetches").fetchone()[0]
    assert after == before  # no fetch happened, none is logged
    # The UPDATE went through the pages triggers, so FTS sees the new text.
    hits = wc.search("Webster", con=cache)
    assert [h["url"] for h in hits] == [wc.normalize_url("https://a.example/fts")]
