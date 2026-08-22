"""Tests for web_import — handing the cache evidence the fetcher can't retrieve.

All offline: a tmp SQLite + blob dir (the ``cache`` fixture), files under
tmp_path, and a stubbed OCR backend. Covers the honesty properties that make an
imported row distinguishable from a fetched one, the mandatory-text rule, and
the verbatim contract flippatch's ``make verify-quotes`` depends on.
"""

from __future__ import annotations

import re
import sqlite3
from typing import TYPE_CHECKING

import pytest
import web_cache as wc
import web_import
import web_ocr
from content_types import extension_for

if TYPE_CHECKING:
    from pathlib import Path

JPEG_BYTES = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00 pretend flyer scan"
PNG_BYTES = b"\x89PNG\r\n\x1a\n\x00 pretend screenshot"
FLYER_URL = "https://www.ipdb.org/images/4583/image-3.jpg"


@pytest.fixture(autouse=True)
def _no_real_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the real Vision backend out of tests that don't care about it.

    Most of these import a transcription, so OCR's result is discarded — but
    calling it anyway made the suite behave differently per platform (Vision on
    macOS, the missing-backend path elsewhere) and spray framework errors over
    the output. Tests that exercise OCR override this in their own body.
    """
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: None)


@pytest.fixture
def jpeg(tmp_path: Path) -> Path:
    path = tmp_path / "flyer.jpg"
    path.write_bytes(JPEG_BYTES)
    return path


def _run(con, path: Path, **kwargs):
    """Import one file, defaulting the args every call needs."""
    kwargs.setdefault("url", FLYER_URL)
    kwargs.setdefault("text", None)
    return web_import.import_one(con, path, **kwargs)


def _page(con, url: str) -> wc.PageRow:
    row = wc.get(url, con=con)
    assert row is not None, f"expected a stored page for {url}"
    return row


def _fetch_rows(con) -> list[dict[str, object]]:
    return [dict(r) for r in con.execute("SELECT * FROM fetches ORDER BY id")]


# --------------------------------------------------------------------------- #
# Provenance — an import must never look like a fetch
# --------------------------------------------------------------------------- #


def test_imported_page_is_flagged_and_claims_no_http_status(cache, jpeg):
    _run(cache, jpeg, text="O MELHOR FLIPPER JAMAIS FABRICADO")
    row = _page(cache, FLYER_URL)
    assert row["imported"] == 1
    # No HTTP request happened. A fabricated 200 would make the audit log lie
    # about the one thing it exists to record.
    assert row["http_status"] is None
    assert not row["rendered"]


def test_import_appends_a_truthful_audit_row(cache, jpeg):
    _run(cache, jpeg, text="some flyer text", query="mecatronics space shuttle flyer")
    (row,) = _fetch_rows(cache)
    assert row["imported"] == 1
    assert row["http_status"] is None
    assert row["content_sha"] == wc.content_sha(JPEG_BYTES)
    assert row["changed"] == 1
    assert row["search_query"] == "mecatronics space shuttle flyer"


def test_reimporting_identical_bytes_is_unchanged(cache, jpeg):
    _run(cache, jpeg, text="same text")
    _run(cache, jpeg, text="same text", force=True)
    assert [r["changed"] for r in _fetch_rows(cache)] == [1, 0]


def test_import_refuses_to_overwrite_an_existing_row_without_force(cache, jpeg):
    _run(cache, jpeg, text="first")
    with pytest.raises(web_import.ImportRefusedError, match="already cached"):
        _run(cache, jpeg, text="second")
    assert _page(cache, FLYER_URL)["text"] == "first"


# --------------------------------------------------------------------------- #
# The blob — real bytes, right extension, resolvable from the row
# --------------------------------------------------------------------------- #


def test_blob_is_written_verbatim_under_its_type_extension(cache, jpeg):
    _run(cache, jpeg, text="text")
    row = _page(cache, FLYER_URL)
    assert row["content_type"] == "image/jpeg"
    assert extension_for(row["content_type"]) == "jpg"
    blob = wc.blob_path(row["content_sha"], ext="jpg")
    # Byte-identical to the file handed over: a quote re-verifies against the
    # original picture, not a re-encoding of it.
    assert blob.read_bytes() == JPEG_BYTES


def test_type_is_sniffed_from_content_not_the_filename(cache, tmp_path):
    # A browser "Save image as" often lands a JPEG under a .txt/.jpeg/no suffix;
    # the magic bytes decide, so the blob extension stays right regardless.
    path = tmp_path / "downloaded"
    path.write_bytes(PNG_BYTES)
    _run(cache, path, text="screenshot text")
    row = _page(cache, FLYER_URL)
    assert row["content_type"] == "image/png"
    assert wc.blob_path(row["content_sha"], ext="png").exists()


def test_unsupported_file_type_is_refused(cache, tmp_path):
    path = tmp_path / "archive.zip"
    path.write_bytes(b"PK\x03\x04 nope")
    with pytest.raises(web_import.ImportRefusedError, match="content type"):
        _run(cache, path, text="text")
    assert wc.get(FLYER_URL, con=cache) is None


def test_text_file_imports_on_its_suffix(cache, tmp_path):
    # No magic bytes, so the suffix is the only route in. The file's own words
    # are the evidence, so no transcription is needed.
    path = tmp_path / "sonic_changelog.txt"
    path.write_text("1.05 - Fixed the ball lock.\n", encoding="utf-8")
    _run(cache, path, url="https://www.jerseyjackpinball.com/sonic_changelog.txt")
    row = _page(cache, "https://www.jerseyjackpinball.com/sonic_changelog.txt")
    assert row["content_type"] == "text/plain"
    assert row["text_source"] == "text"
    assert row["text"] == "1.05 - Fixed the ball lock.\n"
    assert wc.blob_path(row["content_sha"], ext="txt").exists()


def test_non_utf8_text_file_is_refused_not_guessed_at(cache, tmp_path):
    # An import has no charset header and a text file can't restate its own, so
    # these bytes would resolve by detection — which reads them as a different
    # single-byte codec and stores the result as citable text. Refuse instead,
    # the same call --text-file makes: re-saving puts the encoding decision on
    # someone who can see whether the accents came out right.
    path = tmp_path / "changelog.txt"
    path.write_bytes("V1.05 — Réglage « Sonic »\n".encode("cp1252"))
    with pytest.raises(web_import.ImportRefusedError, match="not valid UTF-8"):
        _run(cache, path, url="https://example.com/changelog.txt")
    assert wc.get("https://example.com/changelog.txt", con=cache) is None


def test_non_utf8_html_without_a_declared_charset_is_refused(cache, tmp_path):
    # The same rule, and the reason it asks the handler instead of the file type:
    # a saved page that left its encoding to the HTTP header has none left by the
    # time it reaches an import.
    path = tmp_path / "page.html"
    path.write_bytes(
        "<html><body><p>Réglage « Sonic »</p></body></html>".encode("cp1252")
    )
    with pytest.raises(web_import.ImportRefusedError, match="declares no charset"):
        _run(cache, path, url="https://example.com/p.html")
    assert wc.get("https://example.com/p.html", con=cache) is None


def test_html_declaring_its_own_charset_imports_intact(cache, tmp_path):
    # And the other side of it: a page that states windows-1252 carries its
    # encoding in the blob, so it must keep importing — and keep its accents.
    path = tmp_path / "declared.html"
    path.write_bytes(
        '<html><head><meta charset="windows-1252"></head>'
        "<body><p>Réglage « Sonic »</p></body></html>".encode("cp1252")
    )
    _run(cache, path, url="https://example.com/d.html")
    assert "Réglage « Sonic »" in (_page(cache, "https://example.com/d.html")["text"])


def test_non_utf8_bytes_do_not_block_a_binary_import(cache, jpeg):
    # The rule is about types whose text is a bare charset decode. A JPEG's
    # bytes are not text and must not be held to it.
    _run(cache, jpeg, text="MECATRONICS")
    assert _page(cache, FLYER_URL)["content_type"] == "image/jpeg"


def test_markdown_file_imports_under_its_own_extension(cache, tmp_path):
    path = tmp_path / "notes.md"
    path.write_text("# Notes\n\nProse.\n", encoding="utf-8")
    _run(cache, path, url="https://example.com/notes.md")
    row = _page(cache, "https://example.com/notes.md")
    assert row["content_type"] == "text/markdown"
    assert extension_for(row["content_type"]) == "md"
    assert wc.blob_path(row["content_sha"], ext="md").exists()


def test_jpeg_saved_under_a_txt_suffix_is_still_a_jpeg(cache, tmp_path):
    # A browser "Save image as" routinely lands a JPEG under .txt. Signatures
    # run first so the picture isn't decoded into mojibake and stored as text.
    path = tmp_path / "flyer.txt"
    path.write_bytes(JPEG_BYTES)
    _run(cache, path, text="MECATRONICS")
    row = _page(cache, FLYER_URL)
    assert row["content_type"] == "image/jpeg"
    assert wc.blob_path(row["content_sha"], ext="jpg").exists()


# --------------------------------------------------------------------------- #
# Text is mandatory — a blank-text page defeats the whole point
# --------------------------------------------------------------------------- #


def test_blank_text_is_refused_and_writes_nothing(cache, jpeg, monkeypatch):
    # flippatch's evidence_pages drops text-less rows and FTS can't index them,
    # so an import with nothing to quote is a no-op, loudly.
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: None)
    with pytest.raises(web_import.ImportRefusedError, match="no text"):
        _run(cache, jpeg)
    assert wc.get(FLYER_URL, con=cache) is None
    assert _fetch_rows(cache) == []


# --------------------------------------------------------------------------- #
# text_source — supplied text and machine text are distinguishable
# --------------------------------------------------------------------------- #


def test_supplied_text_is_recorded_as_manual(cache, jpeg):
    _run(cache, jpeg, text="A human typed this out.")
    assert _page(cache, FLYER_URL)["text_source"] == "manual"


def test_supplied_text_form_feeds_become_line_breaks(cache, jpeg):
    # A line that is exactly "\f" means one thing corpus-wide: a PDF page
    # boundary written by web_pdftext, the page axis quote_hits() reads. A
    # paste or an outside OCR run must not mint phantom pages on a manual/ocr
    # row — and the guard sits in import_one, the storage boundary, so it
    # covers programmatic callers as well as the CLI's --text-file reader.
    # Replaced, never deleted: "page one\fpage two" must not fuse words.
    _run(cache, jpeg, text="page one\fpage two\n\f\npage three")
    assert _page(cache, FLYER_URL)["text"] == "page one\npage two\n\n\npage three"


def test_ocr_text_lands_in_the_machine_read_tier(cache, jpeg, monkeypatch):
    # An image imported without a transcription is findable, not quotable: its
    # OCR lands in ocr_text, and no text layer is claimed.
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: "MACHINE READ THIS")
    _run(cache, jpeg)
    row = _page(cache, FLYER_URL)
    assert row["text"] is None
    assert row["ocr_text"] == "MACHINE READ THIS"
    assert row["text_source"] is None


def test_supplied_text_wins_over_ocr(cache, jpeg, monkeypatch):
    # The reviewed transcription is authoritative — OCR is only ever a draft.
    # The draft still rides along in the findable tier, so words the person
    # didn't transcribe stay searchable.
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: "MEGATRONICS")
    _run(cache, jpeg, text="MECATRONICS")
    row = _page(cache, FLYER_URL)
    assert row["text"] == "MECATRONICS"
    assert row["text_source"] == "manual"
    assert row["ocr_text"] == "MEGATRONICS"


def test_machine_read_text_is_not_an_importable_label(cache, tmp_path, make_pdf):
    # `ocr` is not a text_source: machine-read words are not a text layer, so
    # outside OCR can't be imported as a text layer any more — the OCR pass
    # (web_pdfocr) reads a scanned PDF's sheets into ocr_text instead.
    path = tmp_path / "scan.pdf"
    path.write_bytes(make_pdf(title="Scan"))
    url = "https://www.ipdb.org/files/1343/scan.pdf"
    with pytest.raises(web_import.ImportRefusedError, match="text-source"):
        _run(cache, path, url=url, text="1990 JANICE AVE", text_source="ocr")


def test_scanned_pdf_imports_without_text_for_the_ocr_pass(cache, tmp_path, make_pdf):
    # A scanned PDF has no text layer and needs none to be worth importing:
    # the blob lands, and web_pdfocr later reads its sheets into ocr_text.
    path = tmp_path / "scan.pdf"
    path.write_bytes(make_pdf(title="Scan", text=""))
    url = "https://www.ipdb.org/files/1343/scan.pdf"
    _run(cache, path, url=url)
    row = _page(cache, url)
    assert row["text"] is None
    assert row["ocr_text"] is None  # not yet — the pass fills it
    assert row["content_type"] == "application/pdf"


def test_declared_text_source_must_be_one_the_cache_knows(cache, jpeg):
    with pytest.raises(web_import.ImportRefusedError, match="text-source"):
        _run(cache, jpeg, text="MECATRONICS", text_source="vision")


def test_declared_text_source_needs_text_to_describe(cache, jpeg):
    # Without --text-file the handler's own extraction is stored, and its
    # provenance is a fact about the handler, not something to declare.
    with pytest.raises(web_import.ImportRefusedError, match="text-source"):
        _run(cache, jpeg, text_source="manual")


def test_imported_pdf_keeps_its_handler_text_source(cache, tmp_path, make_pdf):
    # A hand-downloaded PDF (403 to the fetcher, fine in a browser) still gets
    # its text from the PDF handler; only the *provenance of the bytes* differs.
    path = tmp_path / "flyer.pdf"
    path.write_bytes(make_pdf(title="Spec Sheet", moddate="D:20240115093000Z"))
    _run(cache, path, url="https://www.ipdb.org/files/4583/spec.pdf")
    row = _page(cache, "https://www.ipdb.org/files/4583/spec.pdf")
    assert "Hello PDF evidence" in (row["text"] or "")
    assert row["text_source"] == "pdf"
    assert row["imported"] == 1
    assert row["title"] == "Spec Sheet"
    assert row["last_updated"] == "2024-01-15"


def test_imported_html_is_extracted_by_the_html_handler(cache, tmp_path):
    path = tmp_path / "page.html"
    path.write_text(
        "<html><body><article><p>"
        + "Readable saved-page prose. " * 20
        + "</p></article></body></html>",
        encoding="utf-8",
    )
    _run(cache, path, url="https://blocked.example/article")
    row = _page(cache, "https://blocked.example/article")
    assert "Readable saved-page prose." in (row["text"] or "")
    assert row["text_source"] == "html"
    assert row["content_type"] == "text/html"


# --------------------------------------------------------------------------- #
# Title / date — supplied by hand, never invented
# --------------------------------------------------------------------------- #


def test_title_and_date_are_stored_when_supplied(cache, jpeg):
    _run(cache, jpeg, text="text", title="Space Shuttle flyer", date="1986-01-01")
    row = _page(cache, FLYER_URL)
    assert row["title"] == "Space Shuttle flyer"
    assert row["last_updated"] == "1986-01-01"


def test_title_and_date_stay_null_when_not_supplied(cache, jpeg):
    _run(cache, jpeg, text="text")
    row = _page(cache, FLYER_URL)
    assert row["title"] is None
    assert row["last_updated"] is None


def test_supplied_title_overrides_the_extracted_one(cache, tmp_path, make_pdf):
    path = tmp_path / "doc.pdf"
    path.write_bytes(make_pdf(title="Untitled-1"))
    _run(cache, path, url="https://x.example/doc.pdf", title="Mecatronics price list")
    assert (
        _page(cache, "https://x.example/doc.pdf")["title"] == "Mecatronics price list"
    )


def test_malformed_date_is_refused(cache, jpeg):
    # A date is evidence too: take it in one unambiguous form or not at all.
    with pytest.raises(web_import.ImportRefusedError, match="date"):
        _run(cache, jpeg, text="text", date="Jan 1986")


# --------------------------------------------------------------------------- #
# URL handling
# --------------------------------------------------------------------------- #


def test_url_is_normalized_and_raw_url_preserved(cache, jpeg):
    _run(cache, jpeg, text="text", url="HTTPS://WWW.IPDB.ORG/images/4583/image-3.jpg")
    row = _page(cache, FLYER_URL)
    assert row["url"] == FLYER_URL
    assert row["raw_url"] == "HTTPS://WWW.IPDB.ORG/images/4583/image-3.jpg"


def test_non_http_url_is_refused(cache, jpeg):
    # The cache key must be the address the bytes actually live at on the web —
    # a local path is not a quotable source.
    with pytest.raises(web_import.ImportRefusedError, match="http"):
        _run(cache, jpeg, text="text", url="file:///tmp/flyer.jpg")


# --------------------------------------------------------------------------- #
# Dry run — the review step before an OCR draft becomes evidence
# --------------------------------------------------------------------------- #


def test_dry_run_writes_nothing_but_shows_the_text(cache, jpeg, monkeypatch, capsys):
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: "DRAFT TEXT FROM OCR")
    _run(cache, jpeg, dry_run=True)
    assert wc.get(FLYER_URL, con=cache) is None
    assert _fetch_rows(cache) == []
    assert not any(wc.RAW_DIR.iterdir()) if wc.RAW_DIR.exists() else True
    assert "DRAFT TEXT FROM OCR" in capsys.readouterr().out


def test_dry_run_reports_blank_text_without_raising(cache, jpeg, monkeypatch, capsys):
    # The point of a dry run is to *see* the problem; refusing here would hide
    # the very thing you asked to inspect.
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: None)
    _run(cache, jpeg, dry_run=True)
    assert "no text" in capsys.readouterr().out.lower()


# --------------------------------------------------------------------------- #
# The downstream contract: flippatch's verbatim quote gate
# --------------------------------------------------------------------------- #

_SMART = {"\u201c": '"', "\u201d": '"', "\u2018": "'", "\u2019": "'"}


def _verify_quotes_normalize(text: str) -> str:
    """flippatch's scripts/quote_verify/verify_quotes.py normalize(), verbatim.

    Duplicated deliberately: this test exists to catch pinexplore drifting away
    from a contract that lives in another repo, so it must not import from it.
    """
    for smart, straight in _SMART.items():
        text = text.replace(smart, straight)
    return re.sub(r"\s+", " ", text)


def test_transcription_verifies_through_the_ordinary_quote_path(cache, jpeg):
    # An accurate transcription must make a quote verify with no special-casing
    # in flippatch: line structure is preserved on the way in, and the gate's
    # whitespace collapsing does the rest.
    transcription = (
        "“O MELHOR FLIPPER JAMAIS FABRICADO”\n"
        "Apresentação\n"
        "1. Para um, dois, três ou quatro participantes\n"
        "2. Na forma de kit ou máquina completa\n"
        "MECATRONICS INDÚSTRIA E COMÉRCIO LTDA.\n"
        "Av. Cardoso de Mello, 671 - Vl. Olimpia\n"
        "CEP 04548 - Fones: 543-1055 - 543-0296"
    )
    _run(cache, jpeg, text=transcription)
    stored = _page(cache, FLYER_URL)["text"]
    assert stored is not None
    source = _verify_quotes_normalize(stored)
    # A multi-line quote from the footer, as a patch would carry it.
    quote = (
        "MECATRONICS INDÚSTRIA E COMÉRCIO LTDA. "
        "Av. Cardoso de Mello, 671 - Vl. Olimpia "
        "CEP 04548 - Fones: 543-1055 - 543-0296"
    )
    assert quote in source
    # Smart quotes in the source are straightened by the gate, so a patch written
    # with straight quotes matches the flyer's typographic ones.
    assert '"O MELHOR FLIPPER JAMAIS FABRICADO"' in source


def test_imported_page_is_quotable_and_searchable(cache, jpeg):
    # The two read paths patch authors actually use.
    _run(cache, jpeg, text="Space Shuttle: O maior desafio com o melhor visual.")
    assert wc.quote(FLYER_URL, "maior desafio", con=cache) == [
        "Space Shuttle: O maior desafio com o melhor visual."
    ]
    assert [h["url"] for h in wc.search("desafio", con=cache)] == [FLYER_URL]


# --------------------------------------------------------------------------- #
# CLI behavior: dry run really writes nothing; unreadable input is reported
# --------------------------------------------------------------------------- #


@pytest.fixture
def fresh_cache_paths(tmp_path, monkeypatch):
    """Point the cache at a tmp dir WITHOUT creating it (unlike `cache`)."""
    web_dir = tmp_path / "web"
    monkeypatch.setattr(wc, "WEB_DIR", web_dir)
    monkeypatch.setattr(wc, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(wc, "RAW_DIR", web_dir / "raw")
    return web_dir


def test_dry_run_on_a_fresh_checkout_creates_nothing(
    fresh_cache_paths, jpeg, capsys, tmp_path
):
    # "Nothing is written" has to mean nothing: no database file, no blob dir.
    text_file = tmp_path / "t.txt"
    text_file.write_text("a reviewed transcription", encoding="utf-8")
    rc = web_import.main(
        [str(jpeg), "--url", FLYER_URL, "--text-file", str(text_file), "--dry-run"]
    )
    capsys.readouterr()
    assert rc == 0
    assert not fresh_cache_paths.exists()
    assert not wc.DB_PATH.exists()


def test_dry_run_reports_that_the_url_is_already_cached(cache, jpeg, capsys, tmp_path):
    _run(cache, jpeg, text="first transcription")
    text_file = tmp_path / "t.txt"
    text_file.write_text("second transcription", encoding="utf-8")
    web_import.main(
        [str(jpeg), "--url", FLYER_URL, "--text-file", str(text_file), "--dry-run"]
    )
    out = capsys.readouterr().out
    assert "already cached" in out
    assert "--force" in out


def test_missing_text_file_is_a_controlled_error(cache, jpeg, capsys, tmp_path):
    missing = tmp_path / "nope.txt"
    rc = web_import.main([str(jpeg), "--url", FLYER_URL, "--text-file", str(missing)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "nope.txt" in err  # names the transcription, not the image
    assert wc.get(FLYER_URL, con=cache) is None


def test_non_utf8_text_file_is_a_controlled_error(cache, jpeg, capsys, tmp_path):
    # UnicodeDecodeError is a ValueError, not an OSError — it needs its own arm.
    bad = tmp_path / "latin1.txt"
    bad.write_bytes("MECATRONICS INDÚSTRIA".encode("latin-1"))
    rc = web_import.main([str(jpeg), "--url", FLYER_URL, "--text-file", str(bad)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "UTF-8" in err
    assert "latin1.txt" in err


def test_missing_image_file_is_a_controlled_error(cache, capsys, tmp_path):
    rc = web_import.main([str(tmp_path / "gone.jpg"), "--url", FLYER_URL])
    assert rc == 1
    assert "gone.jpg" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# Dates that don't exist
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad", ["2026-99-99", "2025-02-30", "0000-00-00"])
def test_impossible_dates_are_refused(cache, jpeg, bad):
    # Shape isn't enough: these are all YYYY-MM-DD and none of them is a date.
    with pytest.raises(web_import.ImportRefusedError, match="date"):
        _run(cache, jpeg, text="text", date=bad)


def test_real_leap_day_is_accepted(cache, jpeg):
    _run(cache, jpeg, text="text", date="2024-02-29")
    assert _page(cache, FLYER_URL)["last_updated"] == "2024-02-29"


def test_dry_run_survives_a_pre_migration_cache(fresh_cache_paths, jpeg, capsys):
    # A dry run skips migration by design (read-only), so it can meet a cache
    # written before `imported`/`text_source` existed — which is what `make pull`
    # hands you until a migrated cache is pushed. A preview must report what it
    # can, not raise KeyError on a legacy-shaped row.
    fresh_cache_paths.mkdir(parents=True)
    con = sqlite3.connect(wc.DB_PATH)
    con.executescript(
        """
        CREATE TABLE pages (
          url TEXT PRIMARY KEY, raw_url TEXT, content_sha TEXT NOT NULL,
          first_fetched_at TEXT NOT NULL, last_fetched_at TEXT NOT NULL,
          last_updated TEXT, title TEXT, http_status INTEGER, content_type TEXT,
          text TEXT, rendered INTEGER
        );
        """
    )
    con.execute(
        "INSERT INTO pages (url, content_sha, first_fetched_at, last_fetched_at, text)"
        " VALUES (?, 'abc', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', 'hi')",
        (FLYER_URL,),
    )
    con.commit()
    con.close()

    rc = web_import.main([str(jpeg), "--url", FLYER_URL, "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "already cached" in out


def test_blank_transcription_file_is_refused_not_silently_ocred(
    cache, jpeg, monkeypatch
):
    # An empty --text-file means the transcription didn't save, or the wrong path
    # was passed. Falling back to the OCR draft would import unreviewed machine
    # text under the banner of a reviewed import — the one outcome the review
    # step exists to prevent.
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: "MEGATRONICS SAUTTL")
    with pytest.raises(web_import.ImportRefusedError, match="--text-file"):
        _run(cache, jpeg, text="")
    assert wc.get(FLYER_URL, con=cache) is None


def test_whitespace_only_transcription_is_refused_even_when_ocr_would_work(
    cache, jpeg, monkeypatch
):
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: "MEGATRONICS SAUTTL")
    with pytest.raises(web_import.ImportRefusedError, match="--text-file"):
        _run(cache, jpeg, text="   \n\t  ")
