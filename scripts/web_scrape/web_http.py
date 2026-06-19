#!/usr/bin/env python3
"""HTTP transport for the web evidence cache (see docs/WebCache.md).

The bytes-and-charset layer beneath the fetcher: GET a URL with a polite
User-Agent, gate on content-type and response size, and decode the body to text
resolving the charset carefully (the headerless Shift-JIS case old Japanese
pages hit). Returns a ``Resp`` the orchestrator (``web_fetch``) and the render
fallback (``web_render``) both speak. No SQLite, no extraction, no policy.
"""

from __future__ import annotations

import contextlib
import re
import urllib.parse
import urllib.request
from typing import Literal, NamedTuple

USER_AGENT = (
    "pinexplore-webevidence/1.0 "
    "(+https://github.com/deanmoses/pindata; pinball catalog evidence cache)"
)
MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # cap the body we'll buffer + extract
HTML_CONTENT_TYPES = {"text/html", "application/xhtml+xml"}
PDF_CONTENT_TYPE = "application/pdf"
# Content types we can turn into evidence. HTML is charset-decoded then extracted
# with trafilatura; a PDF's bytes are stored verbatim and parsed by web_extract
# (no charset decode). Anything else still skips with skip="content-type".
EXTRACTABLE_CONTENT_TYPES = HTML_CONTENT_TYPES | {PDF_CONTENT_TYPE}
# Generic/ambiguous labels worth a %PDF- magic-byte sniff: servers routinely serve
# a real PDF as octet-stream, and a response with no Content-Type header surfaces
# (via get_content_type's default) as text/plain. We read the (size-capped) body
# and let the signature decide, rather than skip a citable document.
_SNIFFABLE_CONTENT_TYPES = {
    "application/octet-stream",
    "binary/octet-stream",
    "text/plain",
}
_PDF_MAGIC = b"%PDF-"


SkipReason = Literal["content-type", "too-large"]


class Resp(NamedTuple):
    """Result of an HTTP GET. ``raw`` and ``text`` are both None when we declined
    the body (``skip`` set to 'content-type' or 'too-large'). On success ``raw`` is
    always set; ``text`` is the decoded body for HTML but None for a binary type
    (a PDF), whose ``text`` is filled later by the extractor. ``final_url`` is
    post-redirect."""

    status: int
    content_type: str
    final_url: str
    raw: bytes | None
    text: str | None
    skip: SkipReason | None


# --------------------------------------------------------------------------- #
# Charset decoding
# --------------------------------------------------------------------------- #


# A page can declare its charset in an HTML <meta> tag two ways: the HTML5
# ``<meta charset="...">`` and the legacy
# ``<meta http-equiv="Content-Type" content="text/html; charset=...">``. Both put
# the value right after a ``charset=``, so one pattern catches them.
_META_CHARSET_RE = re.compile(
    rb"""<meta[^>]+?charset\s*=\s*["']?\s*([a-zA-Z0-9_.:-]+)""",
    re.IGNORECASE,
)

# Windows-authored Japanese pages routinely declare ``Shift_JIS`` but actually use
# cp932 (its superset, with NEC/IBM extension characters like ①, ㈱). Python's
# strict ``shift_jis`` codec mangles those extension bytes, so decode the whole
# family as cp932 — it round-trips genuine Shift_JIS unchanged.
_CHARSET_ALIASES = {
    "shift_jis": "cp932",
    "shift-jis": "cp932",
    "shiftjis": "cp932",
    "sjis": "cp932",
    "x-sjis": "cp932",
}


def _sniff_meta_charset(raw: bytes) -> str | None:
    """Return the charset an HTML page declares in a ``<meta>`` tag, or None.

    Per the HTML spec the declaration must appear in the first 1024 bytes, so we
    only scan that prefix (and bound a stray match deeper in the body). Matches
    both ``<meta charset="shift_jis">`` and the legacy
    ``<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">``.
    """
    match = _META_CHARSET_RE.search(raw[:1024])
    if match is None:
        return None
    return match.group(1).decode("ascii", errors="replace")


def _detect_charset(raw: bytes) -> str | None:
    """Statistically detect the charset of undeclared bytes, or None.

    The last resort when neither the HTTP header nor the HTML declares a charset
    (common for old Japanese pages served as Shift-JIS). charset-normalizer is
    already an indirect dependency via trafilatura.
    """
    from charset_normalizer import from_bytes

    best = from_bytes(raw).best()
    return best.encoding if best is not None else None


def _try_decode(raw: bytes, label: str | None) -> str | None:
    """Decode ``raw`` using charset ``label``, or None if the label is empty or
    unknown to Python's codecs (a junk ``charset=`` shouldn't raise and lose the
    page). The Shift_JIS family is upgraded to its cp932 superset first."""
    if not label:
        return None
    codec = _CHARSET_ALIASES.get(label.strip().lower(), label)
    try:
        return raw.decode(codec, errors="replace")
    except LookupError:
        return None


def _decode_body(raw: bytes, header_charset: str | None) -> str:
    """Decode response bytes to text, resolving the charset in priority order:

    1. the HTTP ``Content-Type`` charset, when the server sent one (authoritative);
    2. a ``<meta>`` charset the HTML declares about itself;
    3. charset-normalizer's statistical detection;
    4. utf-8, as a last resort.

    The motivating bug: old Japanese pages served as Shift-JIS/cp932 with *no*
    charset header — a blind utf-8 decode turned their titles to mojibake. An
    unknown/junk label (e.g. a bogus ``charset=utf-8x-bogus``, which would raise
    ``LookupError`` and escape ``fetch_one``'s except tuple) is skipped rather than
    allowed to lose the page; ``errors="replace"`` throughout so undecodable bytes
    never raise either.
    """
    # Header then <meta>; only if neither yields a usable label do we run the
    # (relatively costly) statistical detection.
    for label in (header_charset, _sniff_meta_charset(raw)):
        decoded = _try_decode(raw, label)
        if decoded is not None:
            return decoded
    detected = _try_decode(raw, _detect_charset(raw))
    return detected if detected is not None else raw.decode("utf-8", errors="replace")


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

    Skips downloading the body for non-HTML content types, and caps the read at
    MAX_RESPONSE_BYTES so a giant response can't be buffered into memory then
    charset-decoded into garbage. ``final_url`` is the post-redirect URL — the
    readable input ``url`` when we landed where we asked, else the redirect target.
    """
    wire_url = request_url(url)
    req = urllib.request.Request(wire_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        status = resp.status
        content_type = resp.headers.get_content_type()
        landed = resp.geturl()
        # geturl() echoes the (encoded) request URL when there was no redirect;
        # return the original readable url then, so a non-ASCII page isn't re-keyed
        # to its percent-encoded form on every fetch.
        final_url = url if landed == wire_url else landed
        # None (not a utf-8 default) when the server omits the charset, so
        # _decode_body can fall through to the page's own <meta>/detection — the
        # headerless Shift-JIS case that a blind utf-8 decode mojibakes.
        header_charset = resp.headers.get_content_charset()
        # Read the body for an extractable type, or for a generic/empty label that
        # a magic-byte sniff might reveal to be a PDF. Anything else skips unread.
        if (
            content_type not in EXTRACTABLE_CONTENT_TYPES
            and content_type not in _SNIFFABLE_CONTENT_TYPES
        ):
            return Resp(status, content_type, final_url, None, None, "content-type")
        raw = resp.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        # An over-cap sniffable type (octet-stream / text/plain) reports too-large
        # rather than content-type — both skip + log; we can't sniff without reading.
        return Resp(status, content_type, final_url, None, None, "too-large")
    # A %PDF- signature is authoritative: it identifies a PDF whatever the header
    # claimed (octet-stream, a wrong text/* label, or nothing).
    if raw.startswith(_PDF_MAGIC):
        content_type = PDF_CONTENT_TYPE
    elif content_type not in EXTRACTABLE_CONTENT_TYPES:
        # A sniffable type that isn't a PDF (a genuine octet-stream download).
        return Resp(status, content_type, final_url, None, None, "content-type")
    if content_type == PDF_CONTENT_TYPE:
        # Binary: store the raw bytes, skip charset decoding. text is filled in
        # later by the PDF extractor (web_extract.extract_pdf).
        return Resp(status, content_type, final_url, raw, None, None)
    text = _decode_body(raw, header_charset)
    return Resp(status, content_type, final_url, raw, text, None)
