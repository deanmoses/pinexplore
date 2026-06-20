#!/usr/bin/env python3
"""The content-type handler interface for the web evidence cache (docs/WebCache.md).

A ``ContentHandler`` is everything the cache needs to know about one kind of
document — which HTTP content types it claims, how to recognize it from its first
bytes, whether its body is text or binary, how to pull readable evidence from it,
and how it behaves in the fetch pipeline (render-eligible? what blob extension?
what to warn when it extracts to nothing?).

The point is locality: a new document type is a single new file implementing this
interface plus one line in the registry (``content_types/__init__``), not an
``if pdf`` / ``if html`` branch threaded through the transport, the fetcher, and
the extractor. ``web_http`` and ``web_fetch`` only ever talk to the registry and
this interface; they never name a concrete type.
"""

from __future__ import annotations

from typing import NamedTuple


class ExtractedMeta(NamedTuple):
    """Readable content pulled from a document. Any field may be None."""

    title: str | None
    last_updated: str | None
    text: str | None


class ContentHandler:
    """Base class for a document type the cache can turn into citable evidence.

    Subclasses set the class-level attributes and override the methods. The
    registry instantiates one singleton per subclass; handlers are stateless.
    """

    # HTTP content types whose responses this handler claims. Drives both the
    # extractable gate and routing (``handler_for``); the registry rejects a
    # handler that declares none. Required.
    mime_types: frozenset[str] = frozenset()
    # The label written to Resp.content_type when ``signature`` reclassifies a
    # response whose header lied (or was absent). Must be one of ``mime_types`` (the
    # registry checks), so the re-lookup that drives extraction still finds this
    # handler. Unused — and unset — by a type with no signature.
    canonical_mime: str = ""
    # A leading-bytes signature that authoritatively identifies this type whatever
    # the header claimed (a PDF's b"%PDF-"), or None when the type can't be sniffed.
    signature: bytes | None = None
    # Extension for the stored raw blob, so it re-opens in the right viewer on
    # verify (a PDF as ``.pdf``, not mislabeled ``.html``). No default — each type
    # states its own, or the registry rejects it. Required.
    extension: str = ""
    # Whether a thin extraction should escalate to a headless render. HTML yes (an
    # SPA skeleton); a binary document like a PDF no — a browser can't read it
    # either (that needs OCR, out of scope).
    renderable: bool = False

    def decode(self, raw: bytes, header_charset: str | None) -> str | None:
        """The text the cache stores/extracts from, or None for a binary type.

        Called by the transport once the body is in hand. The load-bearing
        contract with ``extract``: a *text* type (HTML) resolves the charset and
        returns the decoded string, which arrives back as ``extract``'s ``text``; a
        *binary* type (PDF) returns None and makes ``extract`` work from ``raw``
        instead. The base default is binary — a text type MUST override this, or its
        ``extract`` is handed None.
        """
        return None

    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        """Pull title / text / date from the fetched document.

        Receives both representations — the ``raw`` bytes and the decoded ``text``
        (what ``decode`` returned, or a render's DOM) — and uses whichever its type
        needs: a text type reads ``text`` (non-None per the ``decode`` contract
        above), a binary type reads ``raw``. ``url`` is the document's address, for
        handlers that resolve relative links or dates from it (HTML); a
        self-contained type (PDF) ignores it. Required — every handler implements it.
        """
        raise NotImplementedError

    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        """The stderr warning when extraction came back thin, or None to stay quiet.

        A thin result is the silent-200 bug surfacing (a JS-only page, a scanned
        PDF). The render flags let a renderable type tailor the message to whether
        a render was tried; a non-renderable type ignores them. Default: quiet.
        """
        return None
