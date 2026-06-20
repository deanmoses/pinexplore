"""Tests for web_http: the content-type gate, PDF sniff, and wire-safe URL
encoding (no network). Charset decoding now lives with the HTML handler — see
test_content_types.py."""

from __future__ import annotations

import urllib.request
from typing import TYPE_CHECKING

import web_http

if TYPE_CHECKING:
    import pytest


# --------------------------------------------------------------------------- #
# request_url — wire-safe encoding of a readable normalized URL
# --------------------------------------------------------------------------- #


def test_request_url_percent_encodes_non_ascii_path():
    # The bug this fixes: a non-ASCII path raised UnicodeEncodeError in urllib.
    got = web_http.request_url("https://www.weblio.jp/content/サンワイズ")
    assert got == (
        "https://www.weblio.jp/content/%E3%82%B5%E3%83%B3%E3%83%AF%E3%82%A4%E3%82%BA"
    )
    assert got.isascii()


def test_request_url_idempotent_on_ascii_and_encoded():
    plain = "https://example.com/foo/bar?a=1&b=2"
    assert web_http.request_url(plain) == plain
    # already-percent-encoded path is not double-encoded (%E3 stays %E3)
    enc = "https://www.weblio.jp/content/%E3%82%B5%E3%83%B3"
    assert web_http.request_url(enc) == enc


def test_request_url_preserves_ipv6_brackets():
    # parts.hostname drops the brackets an IPv6 literal needs; without them the
    # rebuilt netloc (::1:8080) is ambiguous/malformed. Host stays ASCII, so this
    # also guards the non-IDNA path.
    assert web_http.request_url("http://[::1]:8080/x") == "http://[::1]:8080/x"
    assert web_http.request_url("http://[2001:db8::1]/p") == "http://[2001:db8::1]/p"


def test_request_url_idna_encodes_non_ascii_host():
    got = web_http.request_url("https://日本.example/x")
    assert got.startswith("https://xn--")
    assert got.endswith("/x")
    assert got.isascii()


# --------------------------------------------------------------------------- #
# http_get — content-type gate, PDF binary path, and %PDF- magic-byte sniff
# --------------------------------------------------------------------------- #


class _FakeHeaders:
    def __init__(self, content_type: str, charset: str | None) -> None:
        self._ct = content_type
        self._cs = charset

    def get_content_type(self) -> str:
        return self._ct

    def get_content_charset(self) -> str | None:
        return self._cs


class _FakeResp:
    """A minimal stand-in for the urlopen() response context manager."""

    def __init__(
        self,
        *,
        status: int,
        content_type: str,
        body: bytes,
        url: str,
        charset: str | None,
        may_read: bool,
    ) -> None:
        self.status = status
        self.headers = _FakeHeaders(content_type, charset)
        self._body = body
        self._url = url
        self._may_read = may_read

    def geturl(self) -> str:
        return self._url

    def read(self, _n: int = -1) -> bytes:
        # A skipped (non-extractable) type must decline the body unread; reading
        # here means http_get downloaded something it should have skipped.
        assert self._may_read, "http_get read a body it should have skipped"
        return self._body

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *_: object) -> bool:
        return False


def _stub_urlopen(
    monkeypatch: pytest.MonkeyPatch,
    *,
    content_type: str,
    body: bytes,
    status: int = 200,
    charset: str | None = None,
    may_read: bool = True,
) -> None:
    def _open(req: urllib.request.Request, timeout: float | None = None) -> _FakeResp:
        # Echo the requested wire URL as geturl() → no redirect.
        return _FakeResp(
            status=status,
            content_type=content_type,
            body=body,
            url=req.full_url,
            charset=charset,
            may_read=may_read,
        )

    monkeypatch.setattr(urllib.request, "urlopen", _open)


PDF_BYTES = b"%PDF-1.4\n%fake minimal pdf bytes\n"


def test_http_get_pdf_kept_as_binary(monkeypatch):
    _stub_urlopen(monkeypatch, content_type="application/pdf", body=PDF_BYTES)
    resp = web_http.http_get("https://x.com/doc.pdf")
    assert resp.content_type == "application/pdf"
    assert resp.raw == PDF_BYTES  # stored verbatim
    assert resp.text is None  # not charset-decoded
    assert resp.skip is None


def test_http_get_octet_stream_pdf_is_sniffed(monkeypatch):
    # A real PDF served as octet-stream: the %PDF- signature reclassifies it.
    _stub_urlopen(monkeypatch, content_type="application/octet-stream", body=PDF_BYTES)
    resp = web_http.http_get("https://x.com/download")
    assert resp.content_type == "application/pdf"
    assert resp.raw == PDF_BYTES
    assert resp.skip is None


def test_http_get_pdf_magic_overrides_wrong_html_label(monkeypatch):
    # The signature is authoritative even when the header claims text/html.
    _stub_urlopen(monkeypatch, content_type="text/html", body=PDF_BYTES)
    resp = web_http.http_get("https://x.com/p")
    assert resp.content_type == "application/pdf"
    assert resp.text is None


def test_http_get_headerless_pdf_is_sniffed(monkeypatch):
    # No Content-Type header surfaces as text/plain (get_content_type's default);
    # the %PDF- signature must still rescue a PDF served that way.
    _stub_urlopen(monkeypatch, content_type="text/plain", body=PDF_BYTES)
    resp = web_http.http_get("https://x.com/untyped")
    assert resp.content_type == "application/pdf"
    assert resp.text is None
    assert resp.skip is None


def test_http_get_octet_stream_non_pdf_skipped(monkeypatch):
    # A genuine binary download (not a PDF) is read, fails the sniff, then skips.
    _stub_urlopen(
        monkeypatch, content_type="application/octet-stream", body=b"PK\x03\x04zip"
    )
    resp = web_http.http_get("https://x.com/archive.zip")
    assert resp.skip == "content-type"
    assert resp.raw is None


def test_http_get_plain_text_non_pdf_skipped(monkeypatch):
    # A real text/plain document (not a PDF) is read for the sniff, then skipped —
    # PDF support doesn't turn plain text into evidence, only rescues mislabeled PDFs.
    _stub_urlopen(monkeypatch, content_type="text/plain", body=b"just some notes")
    resp = web_http.http_get("https://x.com/notes.txt")
    assert resp.skip == "content-type"
    assert resp.raw is None


def test_http_get_image_skipped_without_reading_body(monkeypatch):
    # A non-extractable, non-sniffable type declines the body entirely (may_read).
    _stub_urlopen(
        monkeypatch, content_type="image/png", body=b"\x89PNG", may_read=False
    )
    resp = web_http.http_get("https://x.com/pic.png")
    assert resp.skip == "content-type"
    assert resp.raw is None


def test_http_get_html_still_decoded(monkeypatch):
    # Regression guard: the HTML path still decodes to text as before.
    _stub_urlopen(
        monkeypatch,
        content_type="text/html",
        body="<html>café</html>".encode("latin-1"),
        charset="latin-1",
    )
    resp = web_http.http_get("https://x.com/p")
    assert resp.content_type == "text/html"
    assert resp.text == "<html>café</html>"
    assert resp.skip is None
