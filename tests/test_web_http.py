"""Tests for web_http: the content-type gate, PDF sniff, and wire-safe URL
encoding (no network). Charset resolution is a content-type concern — see
test_content_types.py."""

from __future__ import annotations

import datetime
import ssl
import urllib.error
import urllib.request

import pytest
import web_http
from content_types.pdf import PdfHandler
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import AuthorityInformationAccessOID, NameOID

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
    def __init__(
        self, content_type: str, charset: str | None, body_len: int | None = None
    ) -> None:
        self._ct = content_type
        self._cs = charset
        self._len = body_len

    def get_content_type(self) -> str:
        return self._ct

    def get_content_charset(self) -> str | None:
        return self._cs

    def get(self, name: str, default: str | None = None) -> str | None:
        if name.lower() == "content-length" and self._len is not None:
            return str(self._len)
        return default


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
        self.headers = _FakeHeaders(content_type, charset, len(body))
        self._body = body
        self.url = url
        self._may_read = may_read
        self._pos = 0
        # Every read length asked for, so a test can assert how much of an
        # oversized body was buffered before it was refused.
        self.reads: list[int] = []

    def read(self, n: int = -1) -> bytes:
        # A skipped (non-extractable) type must decline the body unread; reading
        # here means http_get downloaded something it should have skipped.
        assert self._may_read, "http_get read a body it should have skipped"
        self.reads.append(n)
        # Honour n like a real stream: a stub that always returns everything
        # cannot tell a bounded read from an unbounded one.
        chunk = (
            self._body[self._pos :] if n < 0 else self._body[self._pos : self._pos + n]
        )
        self._pos += len(chunk)
        return chunk

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def _stub_urlopen(
    monkeypatch: pytest.MonkeyPatch,
    *,
    content_type: str,
    body: bytes,
    status: int = 200,
    charset: str | None = None,
    may_read: bool = True,
) -> list[_FakeResp]:
    """Patch urlopen; return the list responses are appended to as they're made."""

    def _open(
        req: urllib.request.Request,
        timeout: float | None = None,
        *,
        context: ssl.SSLContext | None = None,
    ) -> _FakeResp:
        # Verification is the point of passing a context at all; a None one
        # would silently fall back to the interpreter's own trust store.
        assert context is not None
        assert context.verify_mode is ssl.CERT_REQUIRED
        # Echo the requested wire URL back as the landed URL → no redirect.
        resp = _FakeResp(
            status=status,
            content_type=content_type,
            body=body,
            url=req.full_url,
            charset=charset,
            may_read=may_read,
        )
        made.append(resp)
        return resp

    made: list[_FakeResp] = []
    monkeypatch.setattr(urllib.request, "urlopen", _open)
    return made


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


def test_http_get_plain_text_is_read_as_a_document(monkeypatch):
    # A manufacturer's changelog is often served exactly this way.
    _stub_urlopen(monkeypatch, content_type="text/plain", body=b"just some notes")
    resp = web_http.http_get("https://x.com/notes.txt")
    assert resp.skip is None
    assert resp.content_type == "text/plain"
    assert resp.text == "just some notes"


def test_http_get_headerless_unrecognized_bytes_cache_as_text(monkeypatch):
    # The cost of claiming text/plain: a header-less response surfaces under it,
    # so unlabelled bytes matching no signature cache rather than being refused.
    # Locked as a deliberate trade. The repair (letting a handler reject a body)
    # buys a rare case with a new interface, so it waits for a real one.
    _stub_urlopen(monkeypatch, content_type="text/plain", body=b"\x00\x01binary junk")
    resp = web_http.http_get("https://x.com/untyped")
    assert resp.skip is None
    assert resp.content_type == "text/plain"


def test_http_get_archive_skipped_without_reading_body(monkeypatch):
    # A non-extractable, non-sniffable type declines the body entirely (may_read).
    _stub_urlopen(
        monkeypatch, content_type="application/zip", body=b"PK\x03\x04", may_read=False
    )
    resp = web_http.http_get("https://x.com/dump.zip")
    assert resp.skip == "content-type"
    assert resp.raw is None


def test_http_get_image_is_read_as_binary_evidence(monkeypatch):
    # Images became extractable when OCR gave them text: the body is read and
    # carried through as raw bytes (text stays None — the handler OCRs later),
    # exactly like a PDF.
    _stub_urlopen(
        monkeypatch, content_type="image/jpeg", body=b"\xff\xd8\xff\xe0 jpegish"
    )
    resp = web_http.http_get("https://x.com/flyer.jpg")
    assert resp.skip is None
    assert resp.raw == b"\xff\xd8\xff\xe0 jpegish"
    assert resp.text is None
    assert resp.content_type == "image/jpeg"


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


# --------------------------------------------------------------------------- #
# Response-size cap — per type, because "too big" is a fact about the format
# --------------------------------------------------------------------------- #


@pytest.fixture
def small_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrink the caps so the branches are reachable without 65MB of test data."""
    monkeypatch.setattr(web_http, "MAX_RESPONSE_BYTES", 100)
    monkeypatch.setattr(PdfHandler, "max_response_bytes", 400)


def test_oversized_html_is_skipped(small_caps, monkeypatch):
    _stub_urlopen(monkeypatch, content_type="text/html", body=b"x" * 101)
    assert web_http.http_get("https://x.com/p").skip == "too-large"


def test_a_pdf_over_the_default_cap_is_kept(small_caps, monkeypatch):
    # The point of the per-type cap: a manual that dwarfs any sane HTML page is
    # an ordinary document. American Pinball's GTF quick reference is 15.5MB.
    body = PDF_BYTES + b"x" * 300
    _stub_urlopen(monkeypatch, content_type="application/pdf", body=body)
    resp = web_http.http_get("https://x.com/manual.pdf")
    assert resp.skip is None
    assert resp.raw == body


def test_a_pdf_over_its_own_cap_is_still_skipped(small_caps, monkeypatch):
    _stub_urlopen(
        monkeypatch, content_type="application/pdf", body=PDF_BYTES + b"x" * 500
    )
    assert web_http.http_get("https://x.com/huge.pdf").skip == "too-large"


def test_an_octet_stream_pdf_gets_the_pdf_cap(small_caps, monkeypatch):
    # The type isn't known until the bytes are sniffed, so the read has to allow
    # the widest cap or a mislabeled PDF would be judged by the default.
    body = PDF_BYTES + b"x" * 300
    _stub_urlopen(monkeypatch, content_type="application/octet-stream", body=body)
    resp = web_http.http_get("https://x.com/blob")
    assert resp.skip is None
    assert resp.content_type == "application/pdf"


@pytest.mark.parametrize("label", ["text/html", "image/jpeg", "application/pdf"])
def test_a_large_pdf_is_kept_however_it_is_labelled(small_caps, monkeypatch, label):
    # A signature outranks the header, so the read ceiling can't come from the
    # header either: a mislabelled PDF must not be cut to the wrong type's limit
    # before its bytes can be sniffed.
    body = PDF_BYTES + b"x" * 300
    _stub_urlopen(monkeypatch, content_type=label, body=body)
    resp = web_http.http_get("https://x.com/doc")
    assert resp.skip is None
    assert resp.content_type == "application/pdf"


def test_an_oversized_body_is_not_buffered_to_the_widest_cap(small_caps, monkeypatch):
    # Resolving the type from a short prefix is what keeps the per-type cap a
    # resource guard: a big HTML page must not be pulled into memory under the
    # PDF allowance just because some other type is permitted that much.
    made = _stub_urlopen(monkeypatch, content_type="text/html", body=b"x" * 5000)
    assert web_http.http_get("https://x.com/p").skip == "too-large"
    # HTML's cap is 100 here: one byte past it proves too-large, plus the sniff
    # prefix. Reading to the PDF allowance instead would buffer 401.
    assert made[0]._pos <= 101 + web_http._SIGNATURE_BYTES, made[0]._pos


def test_an_unsniffable_binary_is_refused_after_a_few_bytes(small_caps, monkeypatch):
    # A ZIP served as octet-stream matches no signature, so it is refused on its
    # prefix rather than downloaded in full and thrown away.
    made = _stub_urlopen(
        monkeypatch,
        content_type="application/octet-stream",
        body=b"PK\x03\x04" + b"x" * 5000,
    )
    assert web_http.http_get("https://x.com/blob").skip == "content-type"
    assert made[0]._pos <= 16, made[0]._pos


def test_too_large_reports_the_declared_size(small_caps, monkeypatch):
    # The read stops at the cap, so the body itself can't say how far over it is.
    _stub_urlopen(monkeypatch, content_type="text/html", body=b"x" * 5000)
    assert web_http.http_get("https://x.com/p").declared_size == 5000


def test_too_large_reports_the_cap_it_hit(small_caps, monkeypatch):
    # The caller can't know which type's limit applied, so the response says.
    _stub_urlopen(monkeypatch, content_type="text/html", body=b"x" * 101)
    assert web_http.http_get("https://x.com/p").limit == 100


def test_sniffing_down_to_a_narrower_type_re_applies_its_cap(small_caps, monkeypatch):
    # An image served as octet-stream is read under the widest cap but judged by
    # its own — otherwise mislabeling a body would buy it the PDF's headroom.
    _stub_urlopen(
        monkeypatch,
        content_type="application/octet-stream",
        body=b"\xff\xd8\xff\xe0" + b"x" * 300,
    )
    assert web_http.http_get("https://x.com/blob").skip == "too-large"


def test_pdfs_are_configured_larger_than_everything_else():
    # Locks the policy itself, not just the mechanism the tests above shrink.
    assert PdfHandler.max_response_bytes == 128 * 1024 * 1024
    assert PdfHandler.max_response_bytes > web_http.MAX_RESPONSE_BYTES


# --------------------------------------------------------------------------- #
# Certificate chain repair — rebuilding what a server omits, and refusing the
# rest. Certificates are minted in-process; nothing here touches the network.
# --------------------------------------------------------------------------- #


def _make_key() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def _make_cert(
    cn,
    key,
    *,
    issuer_cn=None,
    issuer_key=None,
    aia_uri=None,
    ocsp_uri=None,
):
    """Mint a certificate, self-signed unless an issuer is supplied."""
    now = datetime.datetime.now(datetime.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
        .issuer_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn or cn)])
        )
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
    )
    access = [
        x509.AccessDescription(method, x509.UniformResourceIdentifier(uri))
        for method, uri in (
            (AuthorityInformationAccessOID.OCSP, ocsp_uri),
            (AuthorityInformationAccessOID.CA_ISSUERS, aia_uri),
        )
        if uri is not None
    ]
    if access:
        builder = builder.add_extension(
            x509.AuthorityInformationAccess(access), critical=False
        )
    return builder.sign(issuer_key or key, hashes.SHA256())


@pytest.fixture(autouse=True)
def _clear_repair_cache():
    """A repaired context is memoized per host; don't leak one between tests."""
    web_http._repaired_contexts.clear()
    yield
    web_http._repaired_contexts.clear()


def test_ca_issuer_uri_prefers_ca_issuers_over_ocsp():
    key = _make_key()
    cert = _make_cert(
        "leaf", key, aia_uri="http://ca.example/i.der", ocsp_uri="http://ocsp.example/"
    )
    assert web_http._ca_issuer_uri(cert) == "http://ca.example/i.der"


@pytest.mark.parametrize(
    "uri",
    [
        "http://127.0.0.1/ca.der",
        "http://localhost/ca.der",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.1/ca.der",
        "http://[::1]/ca.der",
    ],
)
def test_fetch_ca_cert_refuses_non_public_addresses(uri, monkeypatch):
    # The pointer comes from an unverified certificate — the one place a stranger
    # picks a URL for us. It must not reach the loopback or a metadata endpoint.
    def _opened(*_args, **_kwargs):
        raise AssertionError(f"fetched a non-public address: {uri}")

    monkeypatch.setattr(web_http._CA_OPENER, "open", _opened)
    assert web_http._fetch_ca_cert(uri) is None


def test_ca_fetches_do_not_follow_redirects():
    # A redirect would reach past the address check above.
    assert web_http._NoRedirect().redirect_request() is None


def test_ca_issuer_uri_none_when_absent():
    assert web_http._ca_issuer_uri(_make_cert("leaf", _make_key())) is None


def _stub_chain_probe(monkeypatch, leaf, issuers):
    """Serve ``leaf`` as the host's certificate and ``issuers`` by AIA URI."""
    monkeypatch.setattr(
        ssl,
        "get_server_certificate",
        lambda addr, timeout=None: leaf.public_bytes(Encoding.PEM).decode("ascii"),
    )
    monkeypatch.setattr(web_http, "_fetch_ca_cert", lambda uri: issuers.get(uri))


def test_aia_chain_refuses_a_self_nominated_root(monkeypatch):
    # The attack the repair must not enable: a loaded certificate becomes a
    # *trust anchor*, so a server must not get to point at its own root.
    rogue_key = _make_key()
    rogue_root = _make_cert("Rogue Root", rogue_key)
    leaf_key = _make_key()
    leaf = _make_cert(
        "evil.example",
        leaf_key,
        issuer_cn="Rogue Root",
        issuer_key=rogue_key,
        aia_uri="http://rogue.example/root.der",
    )
    _stub_chain_probe(monkeypatch, leaf, {"http://rogue.example/root.der": rogue_root})
    assert web_http._aia_chain("evil.example", 443) is None


def test_aia_chain_returns_the_certificates_the_server_omitted(monkeypatch):
    # The case worth repairing: two hops up (omitted intermediate, then the
    # cross-signed root tying it back) the chain does reach a trusted root.
    root_key = _make_key()
    root = _make_cert("Trusted Root", root_key)
    cross_key = _make_key()
    cross = _make_cert(
        "Rotated Root",
        cross_key,
        issuer_cn="Trusted Root",
        issuer_key=root_key,
        aia_uri="http://ca.example/root.der",
    )
    inter_key = _make_key()
    inter = _make_cert(
        "Intermediate",
        inter_key,
        issuer_cn="Rotated Root",
        issuer_key=cross_key,
        aia_uri="http://ca.example/cross.der",
    )
    leaf = _make_cert(
        "host.example",
        _make_key(),
        issuer_cn="Intermediate",
        issuer_key=inter_key,
        aia_uri="http://ca.example/inter.der",
    )
    monkeypatch.setattr(web_http, "_trusted_roots", lambda: (root,))
    _stub_chain_probe(
        monkeypatch,
        leaf,
        {
            "http://ca.example/inter.der": inter,
            "http://ca.example/cross.der": cross,
            "http://ca.example/root.der": root,
        },
    )
    chain = web_http._aia_chain("host.example", 443)
    assert chain is not None
    assert [c.subject.rfc4514_string() for c in chain] == [
        "CN=Intermediate",
        "CN=Rotated Root",
    ]


def test_aia_chain_refuses_a_certificate_that_did_not_sign_the_one_below(monkeypatch):
    # An AIA pointer naming a real trusted CA that simply didn't issue this leaf
    # must not drag that CA in: every link is checked by signature, not by name.
    root_key = _make_key()
    root = _make_cert("Trusted Root", root_key)
    unrelated = _make_cert(
        "Unrelated CA", _make_key(), issuer_cn="Trusted Root", issuer_key=root_key
    )
    leaf = _make_cert(
        "host.example",
        _make_key(),
        issuer_cn="Unrelated CA",
        issuer_key=_make_key(),  # signed by someone else entirely
        aia_uri="http://ca.example/unrelated.der",
    )
    monkeypatch.setattr(web_http, "_trusted_roots", lambda: (root,))
    _stub_chain_probe(monkeypatch, leaf, {"http://ca.example/unrelated.der": unrelated})
    assert web_http._aia_chain("host.example", 443) is None


def test_repaired_context_verifies_as_strictly_as_the_original(monkeypatch, capsys):
    # The promise of the repair: supply missing certificates, never relax the check.
    root_key = _make_key()
    inter = _make_cert(
        "Intermediate",
        _make_key(),
        issuer_cn="Trusted Root",
        issuer_key=root_key,
    )
    monkeypatch.setattr(web_http, "_aia_chain", lambda host, port: [inter])
    context = web_http._repaired_context("host.example", 443)
    assert context is not None
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname
    # Without this, verification could stop at a fetched certificate instead of
    # having to reach a certifi root — the walk checks signatures, not expiry.
    assert not context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
    # Loud, because trusting certificates a server didn't send is worth seeing.
    assert "Intermediate" in capsys.readouterr().err


def test_repaired_context_is_built_once_per_host(monkeypatch):
    # Including an unrepairable host: a dozen URLs must not mean a dozen probes.
    probes = []

    def _unrepairable(host, port):
        probes.append(host)

    monkeypatch.setattr(web_http, "_aia_chain", _unrepairable)
    assert web_http._repaired_context("host.example", 443) is None
    assert web_http._repaired_context("host.example", 443) is None
    assert probes == ["host.example"]


def _missing_issuer_error():
    err = ssl.SSLCertVerificationError("unable to get local issuer certificate")
    err.verify_code = web_http._VERIFY_MISSING_ISSUER
    return urllib.error.URLError(err)


def _raising_urlopen(exc):
    def _open(*_args, **_kwargs):
        raise exc

    return _open


def test_urlopen_retries_once_with_the_repaired_context(monkeypatch):
    repaired = ssl.create_default_context()
    monkeypatch.setattr(web_http, "_repaired_context", lambda host, port: repaired)
    contexts = []

    def _open(req, timeout=None, *, context=None):
        contexts.append(context)
        if len(contexts) == 1:
            raise _missing_issuer_error()
        return "response"

    monkeypatch.setattr(urllib.request, "urlopen", _open)
    assert web_http._urlopen("https://x.com/p") == "response"
    assert contexts == [web_http._SSL_CONTEXT, repaired]


def test_urlopen_starts_from_a_repair_already_made_this_run(monkeypatch):
    # The second URL off a broken host shouldn't repeat the doomed handshake.
    repaired = ssl.create_default_context()
    web_http._repaired_contexts[("x.com", 443)] = repaired
    contexts = []

    def _open(req, timeout=None, *, context=None):
        contexts.append(context)
        return "response"

    monkeypatch.setattr(urllib.request, "urlopen", _open)
    assert web_http._urlopen("https://x.com/p") == "response"
    assert contexts == [repaired]


def test_urlopen_does_not_repair_a_genuinely_bad_certificate(monkeypatch):
    # A real trust failure: the probe must not even be attempted.
    def _repair(host, port):
        raise AssertionError("repair attempted for a non-chain TLS failure")

    monkeypatch.setattr(web_http, "_repaired_context", _repair)
    err = ssl.SSLCertVerificationError("certificate has expired")
    err.verify_code = 10  # X509_V_ERR_CERT_HAS_EXPIRED
    monkeypatch.setattr(
        urllib.request, "urlopen", _raising_urlopen(urllib.error.URLError(err))
    )
    with pytest.raises(urllib.error.URLError):
        web_http._urlopen("https://x.com/p")


def test_urlopen_reraises_when_the_chain_cannot_be_repaired(monkeypatch):
    monkeypatch.setattr(web_http, "_repaired_context", lambda host, port: None)
    monkeypatch.setattr(
        urllib.request, "urlopen", _raising_urlopen(_missing_issuer_error())
    )
    with pytest.raises(urllib.error.URLError):
        web_http._urlopen("https://x.com/p")
