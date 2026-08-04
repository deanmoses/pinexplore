"""Tests for web_pdftext — the poppler ``pdftotext -layout`` backend.

Normalization is pure and runs everywhere; the poppler bridge is skipped where
the binary isn't installed. The error paths stub ``subprocess.run``, since
neither a missing binary nor a hung one is convenient to arrange for real.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest
import web_pdftext

needs_poppler = pytest.mark.skipif(
    shutil.which("pdftotext") is None,
    reason="poppler's pdftotext is not installed on this host",
)


# --------------------------------------------------------------------------- #
# Normalization (pure) — what poppler's stdout becomes before it is stored
# --------------------------------------------------------------------------- #


def test_normalize_turns_page_breaks_into_line_breaks():
    # Left in, a form feed is an invisible control character inside a quote.
    assert web_pdftext._normalize("page one\fpage two") == "page one\npage two"


def test_normalize_keeps_leading_whitespace_and_drops_trailing():
    # Leading spaces are the column layout; trailing are padding to page width.
    out = web_pdftext._normalize("ARCADE      SPECIAL   \n• Black Armor      ")
    assert out == "ARCADE      SPECIAL\n• Black Armor"


def test_normalize_keeps_the_first_content_line_indented():
    # On Stern's feature matrix this line belongs to the description column;
    # losing its offset pulls it into the label column.
    out = web_pdftext._normalize("\n\n          In Stern Pinball's TRANSFORMERS\n")
    assert out == "          In Stern Pinball's TRANSFORMERS"


def test_normalize_still_drops_blank_lines_at_both_ends():
    assert web_pdftext._normalize("\n\f\nreal text\n\n\f") == "real text"


def test_normalize_returns_none_for_whitespace_only_output():
    # None, never "" — the caller's thin-content warning fires on None.
    assert web_pdftext._normalize("   \n\n\f  \n") is None


# --------------------------------------------------------------------------- #
# The two failure types — a fact about the host vs a fact about the document
# --------------------------------------------------------------------------- #


def test_missing_poppler_raises_unavailable_with_install_hint(monkeypatch):
    def _boom(*_args: object, **_kwargs: object) -> object:
        raise FileNotFoundError(2, "No such file or directory: 'pdftotext'")

    monkeypatch.setattr(subprocess, "run", _boom)
    with pytest.raises(web_pdftext.PdfTextUnavailableError) as exc:
        web_pdftext.pdf_text(b"%PDF-1.4")
    # The operator has to fix this by hand, so the message must say how.
    assert "brew install poppler" in str(exc.value)


def test_unreadable_document_raises_failed_not_unavailable(monkeypatch):
    # One asks the operator to look at a file, the other to install software.
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_a, **_k: subprocess.CompletedProcess(
            [], 1, stdout=b"", stderr=b"Syntax Error: Couldn't find trailer dictionary"
        ),
    )
    with pytest.raises(web_pdftext.PdfTextFailedError) as exc:
        web_pdftext.pdf_text(b"%PDF-1.4 junk")
    assert "trailer dictionary" in str(exc.value)


def test_timeout_raises_failed_not_unavailable(monkeypatch):
    # A hang is a fact about the document, not a missing backend.
    def _hang(*_args: object, **_kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd="pdftotext", timeout=60)

    monkeypatch.setattr(subprocess, "run", _hang)
    with pytest.raises(web_pdftext.PdfTextFailedError) as exc:
        web_pdftext.pdf_text(b"%PDF-1.4")
    assert "did not finish" in str(exc.value)


def test_the_subprocess_call_is_bounded(monkeypatch):
    # An unbounded call looks identical to a bounded one until something hangs.
    seen: dict[str, object] = {}

    def _capture(*_args: object, **kwargs: object) -> object:
        seen.update(kwargs)
        return subprocess.CompletedProcess([], 0, stdout=b"text", stderr=b"")

    monkeypatch.setattr(subprocess, "run", _capture)
    web_pdftext.pdf_text(b"%PDF-1.4")
    assert seen["timeout"] == web_pdftext._TIMEOUT_SECONDS


def test_recovered_damage_keeps_the_text_poppler_managed_to_read(monkeypatch):
    # poppler reports non-zero for damage it recovered from; those pages are
    # still evidence the bytes support.
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_a, **_k: subprocess.CompletedProcess(
            [], 1, stdout=b"page one survived", stderr=b"Syntax Error: bad xref"
        ),
    )
    assert web_pdftext.pdf_text(b"%PDF-1.4") == "page one survived"


def test_clean_exit_with_no_text_is_a_finding_not_a_failure(monkeypatch):
    # An image-only PDF is read perfectly and correctly yields nothing.
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_a, **_k: subprocess.CompletedProcess([], 0, stdout=b"", stderr=b""),
    )
    assert web_pdftext.pdf_text(b"%PDF-1.4") is None


# --------------------------------------------------------------------------- #
# The poppler bridge itself — only where the binary exists
# --------------------------------------------------------------------------- #


@needs_poppler
def test_reads_a_real_pdf_from_bytes(make_pdf):
    assert web_pdftext.pdf_text(make_pdf(text="Hello PDF evidence")) == (
        "Hello PDF evidence"
    )


@needs_poppler
def test_image_only_pdf_yields_none(make_pdf):
    assert web_pdftext.pdf_text(make_pdf(text="")) is None


@needs_poppler
def test_garbage_bytes_raise_failed():
    with pytest.raises(web_pdftext.PdfTextFailedError):
        web_pdftext.pdf_text(b"%PDF-1.4 not really a pdf")
