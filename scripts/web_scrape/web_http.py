#!/usr/bin/env python3
"""HTTP transport for the web evidence cache (see docs/WebCache.md).

The bytes layer beneath the fetcher: GET a URL with a polite User-Agent, gate on
content-type and response size, and hand the body to its content-type handler to
turn into text (charset-resolved for HTML, left as bytes for a binary type like a
PDF). The handler registry (``content_types``) owns every per-type decision, so
this stays content-agnostic. Returns a ``Resp`` the orchestrator (``web_fetch``)
and the render fallback (``web_render``) both speak. No SQLite, no policy.
"""

from __future__ import annotations

import contextlib
import functools
import http.client
import ipaddress
import re
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, cast, override

import certifi
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import AuthorityInformationAccessOID, ExtensionOID

# Allow `import content_types` whether run as a script or imported (mirrors the
# sibling-import dance the other web_scrape modules do).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from content_types import SNIFF_BYTES, SNIFFABLE_CONTENT_TYPES, handler_for, sniff

if TYPE_CHECKING:
    from content_types.base import ContentHandler

USER_AGENT = (
    "pinexplore-webevidence/1.0 "
    "(+https://github.com/deanmoses/pindata; pinball catalog evidence cache)"
)
# Default cap on the body we'll buffer + extract; a type raises it via
# ``max_response_bytes``.
MAX_RESPONSE_BYTES = 10 * 1024 * 1024


def _cap(handler: ContentHandler) -> int:
    """The byte cap for a resolved handler."""
    return handler.max_response_bytes or MAX_RESPONSE_BYTES


# Verify TLS against certifi's CA bundle, not whatever the interpreter happens to
# trust. `urlopen` with no context uses the latter, which on a stock macOS Python
# lacks intermediates plenty of real sites serve: americanpinball.com failed with
# "self-signed certificate in certificate chain" while curl fetched it fine. That
# failure is indistinguishable from a dead site at the call site, so a page the
# author can see in a browser silently becomes unquotable.
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


# --------------------------------------------------------------------------- #
# Certificate chain repair
# --------------------------------------------------------------------------- #

# A server that omits the intermediates above its own certificate fails
# verification even when the chain does reach a root certifi trusts. Browsers
# repair that by following its caIssuers pointer (RFC 5280 Authority Information
# Access) and fetching what was left out; so do we.

# X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY: the server's packaging, not a
# real trust problem. Expiry and hostname mismatch must keep failing.
_VERIFY_MISSING_ISSUER = 20

# Certificates are kilobytes; the cap stops a hostile pointer streaming at us.
_MAX_CA_CERT_BYTES = 64 * 1024

# Two hops covers a rotated CA — the omitted intermediate, then the cross-signed
# root tying it to an older trusted one — and the bound abandons a circular chain.
_MAX_AIA_HOPS = 4

# One repair per host per run. A cached None is a host that couldn't be repaired,
# so a batch doesn't re-probe it once per URL.
_repaired_contexts: dict[tuple[str, int], ssl.SSLContext | None] = {}


_PEM_CERTIFICATE = re.compile(
    rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", re.DOTALL
)


@functools.cache
def _trusted_roots() -> tuple[x509.Certificate, ...]:
    """certifi's roots, for checking where a fetched chain lands.

    Parsed one at a time: the bundle carries roots with legacy quirks that newer
    ``cryptography`` refuses, and one of those should cost us that root rather
    than every repair.
    """
    bundle = Path(certifi.where()).read_bytes()
    roots = []
    for pem in _PEM_CERTIFICATE.findall(bundle):
        try:
            roots.append(x509.load_pem_x509_certificate(pem))
        except ValueError:
            continue
    return tuple(roots)


def _issued_by(cert: x509.Certificate, issuer: x509.Certificate) -> bool:
    """True when ``issuer`` actually signed ``cert`` — the signature, not a claim."""
    try:
        cert.verify_directly_issued_by(issuer)
    except ValueError, TypeError, InvalidSignature:
        return False
    return True


def _issued_by_trusted_root(cert: x509.Certificate) -> bool:
    """True when one of certifi's roots signed ``cert``.

    The terminating condition of a repair, and the whole of its safety:
    ``load_verify_locations`` installs a certificate as a *trust anchor*, so a
    chain accepted on its own say-so would let a server nominate its own root.
    """
    return any(
        root.subject == cert.issuer and _issued_by(cert, root)
        for root in _trusted_roots()
    )


def _ca_issuer_uri(cert: x509.Certificate) -> str | None:
    """Where a certificate says its issuer can be downloaded, if it says."""
    try:
        aia = cert.extensions.get_extension_for_oid(
            ExtensionOID.AUTHORITY_INFORMATION_ACCESS
        ).value
    except x509.ExtensionNotFound:
        return None
    if not isinstance(aia, x509.AuthorityInformationAccess):
        return None
    for desc in aia:
        if desc.access_method != AuthorityInformationAccessOID.CA_ISSUERS:
            continue
        location = desc.access_location
        # Plain http is normal here, not a mistake: needing TLS to repair TLS
        # would be circular.
        if isinstance(
            location, x509.UniformResourceIdentifier
        ) and location.value.lower().startswith(("http://", "https://")):
            return location.value
    return None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Turns a redirect into an error instead of following it."""

    @override
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


# CA endpoints serve the certificate directly, so a redirect here is only a way
# to reach somewhere the address check below already refused.
_CA_OPENER = urllib.request.build_opener(_NoRedirect())


def _is_public_host(host: str) -> bool:
    """True when every address ``host`` resolves to is on the public internet."""
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    return bool(infos) and all(
        ipaddress.ip_address(info[4][0]).is_global for info in infos
    )


def _fetch_ca_cert(uri: str) -> x509.Certificate | None:
    """Download the certificate an AIA pointer names (DER, or PEM from some CAs).

    None when it isn't a parseable certificate. Nothing here is trusted on
    arrival — the caller checks who signed it.

    The URI comes from a certificate we haven't verified, which makes it the one
    place a stranger picks a URL for us. Public addresses only: otherwise it
    would name the loopback or a cloud metadata endpoint and we would dutifully
    GET it.
    """
    host = urllib.parse.urlsplit(uri).hostname
    if host is None or not _is_public_host(host):
        return None
    req = urllib.request.Request(uri, headers={"User-Agent": USER_AGENT})
    try:
        with _CA_OPENER.open(req, timeout=15) as resp:
            body = resp.read(_MAX_CA_CERT_BYTES + 1)
    except (
        urllib.error.URLError,
        TimeoutError,
        ValueError,
        http.client.HTTPException,
    ):
        return None
    if len(body) > _MAX_CA_CERT_BYTES:
        return None
    for load in (x509.load_der_x509_certificate, x509.load_pem_x509_certificate):
        try:
            return load(body)
        except ValueError:
            continue
    return None


def _aia_chain(host: str, port: int) -> list[x509.Certificate] | None:
    """The certificates a server omitted, followed up from its AIA pointers.

    None unless the walk reaches a certificate certifi's roots vouch for. Every
    link is signature-checked, so a chain can't smuggle an anchor in between two
    that do check out.
    """
    # Deliberately unverified — this is the certificate we're repairing around.
    try:
        leaf_pem = ssl.get_server_certificate((host, port), timeout=15)
    except OSError:  # ssl.SSLError included — it subclasses OSError
        return None
    try:
        cert = x509.load_pem_x509_certificate(leaf_pem.encode())
    except ValueError:
        return None

    chain: list[x509.Certificate] = []
    for _ in range(_MAX_AIA_HOPS):
        uri = _ca_issuer_uri(cert)
        if uri is None:
            return None
        issuer = _fetch_ca_cert(uri)
        if issuer is None or not _issued_by(cert, issuer):
            return None
        chain.append(issuer)
        if _issued_by_trusted_root(issuer):
            return chain
        cert = issuer
    return None


def _repaired_context(host: str, port: int) -> ssl.SSLContext | None:
    """A context that can verify ``host`` despite the chain it fails to send.

    None when the gap can't be closed, leaving the original error to stand.
    """
    key = (host, port)
    if key in _repaired_contexts:
        return _repaired_contexts[key]
    chain = _aia_chain(host, port)
    context: ssl.SSLContext | None = None
    if chain is not None:
        context = ssl.create_default_context(cafile=certifi.where())
        # A fetched certificate is loaded as a trust anchor, and PARTIAL_CHAIN
        # (on by default) would let verification stop at one — accepting an
        # expired or non-CA certificate, neither of which the walk's signature
        # check rules out. Cleared, OpenSSL must still reach a certifi root.
        context.verify_flags &= ~ssl.VERIFY_X509_PARTIAL_CHAIN
        context.load_verify_locations(
            cadata="".join(
                cert.public_bytes(Encoding.PEM).decode("ascii") for cert in chain
            )
        )
        names = ", ".join(cert.subject.rfc4514_string() for cert in chain)
        print(
            f"repaired incomplete certificate chain for {host} via AIA ({names})",
            file=sys.stderr,
        )
    _repaired_contexts[key] = context
    return context


def _urlopen(wire_url: str) -> http.client.HTTPResponse:
    """GET with certifi verification, rebuilding an incomplete chain once per host.

    The retry verifies as strictly as the first attempt: the repair supplies
    missing certificates, it doesn't relax the check.
    """
    req = urllib.request.Request(wire_url, headers={"User-Agent": USER_AGENT})
    parts = urllib.parse.urlsplit(wire_url)
    host = parts.hostname
    port = parts.port or 443
    # Start from a repair already made this run, rather than re-failing the same
    # handshake once per URL.
    known = _repaired_contexts.get((host, port)) if host is not None else None
    try:
        return cast(
            "http.client.HTTPResponse",
            urllib.request.urlopen(req, timeout=60, context=known or _SSL_CONTEXT),
        )
    except urllib.error.URLError as exc:
        reason = exc.reason
        if (
            not isinstance(reason, ssl.SSLCertVerificationError)
            or reason.verify_code != _VERIFY_MISSING_ISSUER
            or host is None
        ):
            raise
        context = _repaired_context(host, port)
        if context is None:
            raise
        return cast(
            "http.client.HTTPResponse",
            urllib.request.urlopen(req, timeout=60, context=context),
        )


SkipReason = Literal["content-type", "too-large"]


class Resp(NamedTuple):
    """Result of an HTTP GET. ``raw`` and ``text`` are both None when we declined
    the body (``skip`` set to 'content-type' or 'too-large'). On success ``raw`` is
    always set; ``text`` is the decoded body for a text type (HTML) but None for a
    binary type (a PDF), whose ``text`` is filled later by the extractor.
    ``final_url`` is post-redirect."""

    status: int
    content_type: str
    final_url: str
    raw: bytes | None
    text: str | None
    skip: SkipReason | None
    # The cap that was exceeded, when ``skip`` is 'too-large'. Reported rather
    # than assumed by the caller, which can't know which type's limit applied.
    limit: int | None = None
    # The body size the server declared, when it declared one. The read stops at
    # the cap, so this is the only way to say *how far* over a response is —
    # which is what decides whether the cap should move. Trustworthy here
    # because we never ask for a content encoding, so it counts the same bytes
    # we would have stored.
    declared_size: int | None = None
    # The HTTP Content-Type charset, when the server sent one. ``text`` was
    # already decoded with it; it rides along for the one consumer that must
    # decode *again* — the archive client, whose ``id_`` replay can arrive
    # content-encoded, so the first decode saw compressed bytes and the header
    # is the only place a legacy page's charset may live.
    header_charset: str | None = None


# --------------------------------------------------------------------------- #
# HTTP GET
# --------------------------------------------------------------------------- #


def request_url(url: str) -> str:
    """Make a normalized URL safe to put on the HTTP request line.

    ``normalize_url`` keeps the readable UTF-8 form (the cache key), but urllib
    will not encode it: a non-ASCII host or path (e.g. a Japanese Weblio article,
    ``/content/サンワイズ``) raises ``UnicodeEncodeError`` when urllib tries to
    ASCII-encode the request line. So IDNA-encode a non-ASCII host and
    percent-encode non-ASCII path/query bytes here, for the wire only. Idempotent:
    an already-ASCII or already-percent-encoded URL is unchanged (``%`` is in the
    safe set, so ``%E3`` is not double-encoded).
    """
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    if host and not host.isascii():
        # leave as-is on failure; urlopen surfaces a clear error if it's truly bad
        with contextlib.suppress(UnicodeError):
            host = host.encode("idna").decode("ascii")
    # parts.hostname strips the brackets an IPv6 literal needs; restore them, or
    # the rebuilt netloc (e.g. ``::1:8080``) becomes ambiguous/malformed. A domain
    # or IDNA-encoded host never contains ':', so this only fires for IPv6.
    netloc = f"[{host}]" if ":" in host else host
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        netloc = (
            parts.username
            + (f":{parts.password}" if parts.password else "")
            + f"@{netloc}"
        )
    path = urllib.parse.quote(parts.path, safe="/%:@-._~!$&'()*+,;=")
    query = urllib.parse.quote(parts.query, safe="%:@-._~!$&'()*+,;=/?")
    return urllib.parse.urlunsplit((parts.scheme, netloc, path, query, ""))


def http_get(url: str) -> Resp:
    """GET a URL with our UA, gating on content-type and response size.

    Skips downloading the body for a content type no handler extracts, and caps
    the read at the type's own limit so a giant response can't be buffered into
    memory then decoded into garbage. ``final_url`` is the post-redirect URL — the readable
    input ``url`` when we landed where we asked, else the redirect target.
    """
    wire_url = request_url(url)
    with _urlopen(wire_url) as resp:
        status = resp.status
        content_type = resp.headers.get_content_type()
        landed = resp.url
        # It echoes the (encoded) request URL when there was no redirect;
        # return the original readable url then, so a non-ASCII page isn't re-keyed
        # to its percent-encoded form on every fetch.
        final_url = url if landed == wire_url else landed
        # None (not a utf-8 default) when the server omits the charset, so the HTML
        # handler can fall through to the page's own <meta>/detection — the
        # headerless Shift-JIS case that a blind utf-8 decode mojibakes.
        header_charset = resp.headers.get_content_charset()
        # Read the body for an extractable type, or for a generic/empty label that a
        # signature sniff might reveal to be one. Anything else skips unread.
        handler = handler_for(content_type)
        if handler is None and content_type not in SNIFFABLE_CONTENT_TYPES:
            return Resp(status, content_type, final_url, None, None, "content-type")
        # Sniff a short prefix before buffering the rest: a signature is
        # authoritative — it identifies the type whatever the header claimed
        # (octet-stream, a wrong text/* label, or nothing) — so the header cannot
        # be trusted to pick the cap, and the cap cannot be picked after the body
        # is already in memory. A generic label matching no signature is a
        # genuine non-evidence download, refused here having read a few bytes.
        prefix = resp.read(SNIFF_BYTES)
        sniffed = sniff(prefix)
        if sniffed is not None:
            handler = sniffed
            content_type = sniffed.canonical_mime
        elif handler is None:
            return Resp(status, content_type, final_url, None, None, "content-type")
        cap = _cap(handler)
        declared = resp.headers.get("Content-Length")
        raw = prefix + resp.read(max(cap + 1 - len(prefix), 0))
    if len(raw) > cap:
        size = int(declared) if declared is not None and declared.isdigit() else None
        return Resp(status, content_type, final_url, None, None, "too-large", cap, size)
    # The handler decodes to text (HTML) or returns None for a binary type (a PDF),
    # whose bytes are stored verbatim and whose text the extractor fills in later.
    text = handler.decode(raw, header_charset)
    return Resp(
        status, content_type, final_url, raw, text, None, header_charset=header_charset
    )
