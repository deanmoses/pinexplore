#!/usr/bin/env python3
"""WebVTT caption handler for the web evidence cache (see docs/WebCache.md).

Caption tracks arrive through the video transport (``web_video``), which stores
the VTT bytes verbatim as the blob; this handler turns them into a quotable
transcript. YouTube's auto-captions are "rolling": consecutive cues repeat each
line (as the second row of one cue and the first row of the next) and lace the
text with inline timing/class tags — extraction strips the scaffolding and
dedupes the repeats so the text reads as spoken lines, one per row.

Timestamps are deliberately dropped from the text: quotes verify against
whitespace-collapsed prose, and a timestamp mid-sentence would break span
contiguity. The stored ``.vtt`` blob keeps the full timing for anyone hunting a
``locator:`` moment.
"""

from __future__ import annotations

import re
from typing import override

from .base import ContentHandler, ExtractedMeta

# Inline cue markup: timing tags (<00:00:01.199>) and class/voice tags (<c>,
# </c>, <v Speaker>). VTT escapes literal '<' as &lt;, so this never eats text.
_INLINE_TAG_RE = re.compile(r"<[^>]*>")


def transcript_text(vtt: str) -> str | None:
    """The spoken-line transcript of a VTT document, or None when it has no cues.

    Skips the WEBVTT header block and NOTE/STYLE/REGION blocks, drops cue timing
    lines and cue identifiers, strips inline tags, and collapses the
    rolling-caption repetition (a line identical to the previous kept line).
    """
    lines: list[str] = []
    in_note = False
    for raw_line in vtt.splitlines():
        line = raw_line.strip()
        if not line:
            in_note = False
            continue
        if line.startswith(("WEBVTT", "NOTE", "STYLE", "REGION")):
            in_note = True
            continue
        if in_note:  # header/NOTE metadata (Kind:, Language:, style rules)
            continue
        if "-->" in line:  # a cue timing line (optionally with settings)
            continue
        text = _INLINE_TAG_RE.sub("", line).strip()
        if not text:
            continue
        if lines and lines[-1] == text:
            continue
        lines.append(text)
    return "\n".join(lines) or None


class VttHandler(ContentHandler):
    """WebVTT caption tracks: bytes stored verbatim, extracted to a transcript.

    Reached via the video transport rather than an HTTP content-type header, but
    registered like any other type so blob extensions, extraction, and future
    direct fetches of caption URLs all resolve through the one registry. The
    ``WEBVTT`` magic doubles as a signature. Not renderable — there is no page.
    """

    mime_types = frozenset({"text/vtt"})
    canonical_mime = "text/vtt"
    signature: bytes | None = b"WEBVTT"
    extension = "vtt"
    text_source: str | None = "vtt"
    renderable = False

    @override
    def decode(self, raw: bytes, header_charset: str | None) -> str | None:
        return raw.decode("utf-8", errors="replace")  # VTT is UTF-8 by spec

    @override
    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        # Title and date belong to the *video*, which only the transport knows;
        # it overlays them on the page row. The VTT itself yields just the text.
        vtt = text if text is not None else self.decode(raw, None)
        assert vtt is not None
        return ExtractedMeta(title=None, last_updated=None, text=transcript_text(vtt))

    @override
    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        return f"WARNING: caption track extracted to little/no transcript: {url}"
