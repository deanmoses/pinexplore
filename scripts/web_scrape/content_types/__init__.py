#!/usr/bin/env python3
"""Content-type handler registry for the web evidence cache (docs/WebCache.md).

The one place that knows the set of document types the cache understands. Add a
type by writing a ``ContentHandler`` in its own module here and listing it in
``HANDLERS`` — the transport (``web_http``) and the fetcher (``web_fetch``) pick
it up through the lookup helpers below without changing, because they branch on
the *handler*, never on a concrete type.
"""

from __future__ import annotations

from .base import ContentHandler, ExtractedMeta
from .html import HtmlHandler
from .pdf import PdfHandler

# The registered handlers. One per document type; order is the sniff order (the
# first whose signature matches a body wins, so list more specific signatures
# first if two could ever overlap).
HANDLERS: tuple[ContentHandler, ...] = (HtmlHandler(), PdfHandler())


def _validate(handlers: tuple[ContentHandler, ...]) -> None:
    """Fail at import if a handler is misconfigured, not at fetch time.

    The defaults that bite are the plausible-looking ones: a handler that forgets
    ``extension`` would silently mislabel its blobs ``.html``, and one that sets a
    ``signature`` but a ``canonical_mime`` it doesn't actually claim would stamp a
    content type the post-sniff re-lookup can't resolve — crashing *after* a
    successful fetch. Turn both into an import-time error the author sees at once.
    """
    for handler in handlers:
        name = type(handler).__name__
        assert handler.mime_types, f"{name} declares no mime_types"
        assert handler.extension, f"{name} sets no blob extension"
        if handler.signature is not None:
            assert handler.canonical_mime in handler.mime_types, (
                f"{name}.canonical_mime must be one of its own mime_types"
            )


_validate(HANDLERS)

# content_type -> handler, for every type a handler claims.
_BY_MIME: dict[str, ContentHandler] = {
    mime: handler for handler in HANDLERS for mime in handler.mime_types
}

# The content types we can turn into evidence: read the body, then dispatch to a
# handler. Anything else skips with skip="content-type".
EXTRACTABLE_CONTENT_TYPES: frozenset[str] = frozenset(_BY_MIME)

# Generic/ambiguous labels worth reading so a handler's signature gets a chance:
# servers routinely serve a real PDF as octet-stream, and a response with no
# Content-Type header surfaces (via get_content_type's default) as text/plain. We
# read the (size-capped) body and let ``sniff`` decide, rather than skip a citable
# document. A sniffable body that matches no signature still skips.
SNIFFABLE_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "application/octet-stream",
        "binary/octet-stream",
        "text/plain",
    }
)


def handler_for(content_type: str) -> ContentHandler | None:
    """The handler that claims ``content_type``, or None if no type extracts it."""
    return _BY_MIME.get(content_type)


def sniff(raw: bytes) -> ContentHandler | None:
    """The handler whose signature matches these leading bytes, or None.

    A signature is authoritative: it identifies the type whatever the header
    claimed (octet-stream, a wrong text/* label, or nothing). Used to rescue a
    document served under a generic/wrong content type.
    """
    for handler in HANDLERS:
        if handler.signature is not None and raw.startswith(handler.signature):
            return handler
    return None


__all__ = [
    "EXTRACTABLE_CONTENT_TYPES",
    "HANDLERS",
    "SNIFFABLE_CONTENT_TYPES",
    "ContentHandler",
    "ExtractedMeta",
    "handler_for",
    "sniff",
]
