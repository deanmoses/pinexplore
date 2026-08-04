"""Tests for web_pdftext — the poppler ``pdftotext -layout`` backend.

The pure normalization logic runs everywhere; the actual poppler bridge is
exercised only where the binary is installed, so the system-dependency glue is
covered where it can run and skipped where it can't. The two error types are
tested by stubbing ``subprocess.run``, since neither a missing binary nor a
damaged document is convenient to arrange for real.
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


def test_normalize_turns_page_breaks_into_blank_lines():
    # poppler separates pages with a form feed. Left in, it is an invisible
    # control character sitting in quotable evidence; as a newline it is just
    # another break for sentences() to split on.
    assert web_pdftext._normalize("page one\fpage two") == "page one\npage two"


def test_normalize_keeps_leading_whitespace_and_drops_trailing():
    # Leading spaces ARE the column layout — the whole reason for -layout — so
    # they must survive. Trailing spaces are padding to the page's widest line.
    out = web_pdftext._normalize("ARCADE      SPECIAL   \n• Black Armor      ")
    assert out == "ARCADE      SPECIAL\n• Black Armor"


def test_normalize_returns_none_for_whitespace_only_output():
    # None, never "" — the caller's thin-content warning fires on None, and a
    # blank-text page must not be stored silently.
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
    # A damaged document must never masquerade as a missing backend: one asks
    # the operator to look at a file, the other to install software.
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


def test_recovered_damage_keeps_the_text_poppler_managed_to_read(monkeypatch):
    # poppler reports non-zero for damage it recovered from. Output that exists
    # is evidence the bytes support, so a partly-broken manual keeps the pages
    # it could read instead of being thrown away wholesale.
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_a, **_k: subprocess.CompletedProcess(
            [], 1, stdout=b"page one survived", stderr=b"Syntax Error: bad xref"
        ),
    )
    assert web_pdftext.pdf_text(b"%PDF-1.4") == "page one survived"


def test_clean_exit_with_no_text_is_a_finding_not_a_failure(monkeypatch):
    # An image-only (scanned) PDF is read perfectly and correctly yields
    # nothing. That is a fact about the document, so it returns rather than
    # raising — the caller stores the blob and warns that it needs OCR.
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
def test_garbage_bytes_raise_failed(make_pdf):
    with pytest.raises(web_pdftext.PdfTextFailedError):
        web_pdftext.pdf_text(b"%PDF-1.4 not really a pdf")
