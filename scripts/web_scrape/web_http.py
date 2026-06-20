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
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Literal, NamedTuple

# Allow `import content_types` whether run as a script or imported (mirrors the
# sibling-import dance the other web_scrape modules do).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from content_types import SNIFFABLE_CONTENT_TYPES, handler_for, sniff

USER_AGENT = (
    "pinexplore-webevidence/1.0 "
    "(+https://github.com/deanmoses/pindata; pinball catalog evidence cache)"
)
MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # cap the body we'll buffer + extract


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

    Skips downloading the body for a content type no handler extracts, and caps the
    read at MAX_RESPONSE_BYTES so a giant response can't be buffered into memory
    then decoded into garbage. ``final_url`` is the post-redirect URL — the readable
    input ``url`` when we landed where we asked, else the redirect target.
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
        # None (not a utf-8 default) when the server omits the charset, so the HTML
        # handler can fall through to the page's own <meta>/detection — the
        # headerless Shift-JIS case that a blind utf-8 decode mojibakes.
        header_charset = resp.headers.get_content_charset()
        # Read the body for an extractable type, or for a generic/empty label that a
        # signature sniff might reveal to be one. Anything else skips unread.
        handler = handler_for(content_type)
        if handler is None and content_type not in SNIFFABLE_CONTENT_TYPES:
            return Resp(status, content_type, final_url, None, None, "content-type")
        raw = resp.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        # An over-cap sniffable type (octet-stream / text/plain) reports too-large
        # rather than content-type — both skip + log; we can't sniff without reading.
        return Resp(status, content_type, final_url, None, None, "too-large")
    # A signature is authoritative: it identifies the type whatever the header
    # claimed (octet-stream, a wrong text/* label, or nothing). Otherwise a generic
    # sniffable label that matched no signature is a genuine non-evidence download.
    sniffed = sniff(raw)
    if sniffed is not None:
        handler = sniffed
        content_type = sniffed.canonical_mime
    elif handler is None:
        return Resp(status, content_type, final_url, None, None, "content-type")
    # The handler decodes to text (HTML) or returns None for a binary type (a PDF),
    # whose bytes are stored verbatim and whose text the extractor fills in later.
    text = handler.decode(raw, header_charset)
    return Resp(status, content_type, final_url, raw, text, None)
