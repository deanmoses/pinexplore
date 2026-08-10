#!/usr/bin/env python3
"""Plain-text content handlers for the web evidence cache (see docs/WebCache.md).

Some primary evidence ships as a text file and nothing else: a manufacturer's
code-release changelog, a README filed beside a ROM download. The decoded bytes
*are* the text — there is no extraction step to weigh.

Claiming ``text/plain`` also claims every response with *no* ``Content-Type``
header, which surfaces under that label. Unlabelled bytes matching no signature
therefore cache here rather than being refused — the price of reading the
``.txt`` files that are labelled correctly. A binary that *is* labelled stays
refused; claimed and sniffed types are separate gates.
"""

from __future__ import annotations

from typing import override

from . import charset
from .base import ContentHandler, ExtractedMeta


def _document_text(text: str) -> str | None:
    """The document's words on the corpus's conventions, or None if it has none.

    A leading BOM is an encoding signature, and text is the one type with no
    parser to absorb one, so it would ride into the first quote as an invisible
    character. CRLF collapses because structure parsing compares whole lines. A
    form feed becomes a line break because a lone ``\\f`` line means a PDF page
    boundary corpus-wide; replaced, not deleted, so no words fuse.

    A NUL means these bytes aren't text — the unlabelled binary the module
    docstring accepts, giving itself away. Storing it as a page of control
    characters is the outcome worth ruling out; catching every binary is not.
    That and a whitespace-only file are findings, so the blob is still stored
    and the thin warning fires.
    """
    normalized = (
        text.removeprefix("\ufeff")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\f", "\n")
    )
    if "\x00" in normalized:
        return None
    return normalized if normalized.strip() else None


class TextHandler(ContentHandler):
    """Shared behavior for every plain-text format: decode, and that's the text.

    Title and date stay None — a first line is as often a version number as a
    title, and for evidence no date beats a wrong one. An import supplies both
    by hand. Not renderable: a browser would return the same words in a ``<pre>``.
    """

    text_source: str | None = "text"
    renderable = False
    backfillable = True

    @override
    def decode(self, raw: bytes, header_charset: str | None) -> str | None:
        # No self-declaration: a text file has nowhere to state its own charset.
        return charset.decode_body(raw, header_charset, None)

    @override
    def rereads_faithfully(self, raw: bytes) -> bool:
        # Nowhere to declare a charset, so only valid utf-8 re-reads faithfully;
        # anything else would come back as detection's guess — a cp1252
        # changelog's "Réglage" as "RÈglage", wrong and plausible enough to
        # read past.
        return charset.resolves_without_detection(raw, None)

    @override
    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        assert text is not None  # a text type always carries its decoded body
        return ExtractedMeta(title=None, last_updated=None, text=_document_text(text))

    @override
    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        # Hedged where other types are blunt: a text file has no reading that
        # could have failed, so this fires on a short release note too.
        return f"WARNING: little text in {url} — a short document, or an empty one"


class PlainTextHandler(TextHandler):
    """Plain text — changelogs, release notes, READMEs served as ``.txt``.

    No signature: any prefix short enough to be one would match half the
    binaries on the web, so recognition is the content type or the file suffix.
    """

    mime_types = frozenset({"text/plain"})
    canonical_mime = "text/plain"
    signature: bytes | None = None
    extension = "txt"


class MarkdownHandler(TextHandler):
    """Markdown served as source — its ``#`` headings are the same ATX syntax the
    stored text uses, so a cached ``.md`` gets an ``outline`` where a ``.txt`` is
    one whole-document section. Servers still send the older ``text/x-markdown``;
    both store a ``.md`` blob so the alias can't split one document in two."""

    mime_types = frozenset({"text/markdown", "text/x-markdown"})
    canonical_mime = "text/markdown"
    signature: bytes | None = None
    extension = "md"
