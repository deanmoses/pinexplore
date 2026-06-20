#!/usr/bin/env python3
"""HTML content handler for the web evidence cache (see docs/WebCache.md).

Everything HTML-specific lives here: resolving the charset of the fetched bytes
(the headerless Shift-JIS case old Japanese pages hit), running trafilatura for
readable text + title, and extracting a conservative date. The transport and the
fetcher reach this only through the ``ContentHandler`` interface — they never
mention HTML.
"""

from __future__ import annotations

import re

from .base import ContentHandler, ExtractedMeta

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
# Extraction
# --------------------------------------------------------------------------- #


def _extract_html(html: str, url: str) -> ExtractedMeta:
    """Run trafilatura for text/title; extract the date conservatively.

    ``last_updated`` is a real date stated on the page, or None — never a guess.
    trafilatura's default date extraction pads a weak year-only signal (a stray
    "© 2024", a bare-year meta) up to a fabricated `YYYY-01-01`, which would
    corrupt the "is this still live / need a 2023+ source" judgment. We instead
    ask htmldate with ``extensive_search=False``, which returns None rather than
    pad — for evidence, no date beats a wrong one. ``original_date`` is left False
    (htmldate's default) so we get the page's most recent date, matching the
    ``last_updated`` column name and the recency check.
    """
    import htmldate
    import trafilatura

    title: str | None = None
    text: str | None = None
    doc = trafilatura.bare_extraction(html, url=url, with_metadata=True)
    if doc is not None:
        title = getattr(doc, "title", None)
        text = getattr(doc, "text", None)
    # Fall back to a plain text extraction if metadata extraction yielded none.
    if not text:
        text = trafilatura.extract(html, url=url)
    try:
        last_updated = htmldate.find_date(html, extensive_search=False)
    except Exception:
        last_updated = None
    return ExtractedMeta(title=title, last_updated=last_updated, text=text)


class HtmlHandler(ContentHandler):
    """Web pages: charset-decoded then extracted with trafilatura, render-eligible.

    Has no magic signature (HTML is recognized by its content type, not a fixed
    prefix) and is the only renderable type — a thin extraction means a JS-only
    page a headless render may rescue.
    """

    mime_types = frozenset({"text/html", "application/xhtml+xml"})
    canonical_mime = "text/html"
    signature = None
    extension = "html"
    renderable = True

    def decode(self, raw: bytes, header_charset: str | None) -> str | None:
        return _decode_body(raw, header_charset)

    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        assert text is not None  # HTML always carries decoded text (or a render's)
        return _extract_html(text, url)

    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        if rendered:
            return f"WARNING: still thin after render: {url}"
        if not render_attempted:
            # A render might rescue it; suggest it only when we didn't already try.
            # --force too, since this page is now fresh and would otherwise skip.
            return (
                f"WARNING: thin content, likely JS-only: {url} "
                "(retry with --force --render after "
                "`uv run playwright install chromium`)"
            )
        # A render was attempted and failed — render already logged why.
        return None
