#!/usr/bin/env python3
"""Charset resolution for the cache's text content types (see docs/WebCache.md).

Shared rather than per-handler because the problem is identical for every text
type: HTML has a ``<meta>`` declaration to offer as a second opinion and plain
text has none, but both want the same ladder underneath.
"""

from __future__ import annotations

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


def try_decode(raw: bytes, label: str | None) -> str | None:
    """Decode ``raw`` using charset ``label``, or None if the label is empty or
    unknown to Python's codecs (a junk ``charset=`` shouldn't raise and lose the
    document). The Shift_JIS family is upgraded to its cp932 superset first."""
    if not label:
        return None
    codec = _CHARSET_ALIASES.get(label.strip().lower(), label)
    try:
        return raw.decode(codec, errors="replace")
    except LookupError:
        return None


def detect_charset(raw: bytes) -> str | None:
    """Statistically detect the charset of undeclared bytes, or None.

    The last resort when nothing declares a charset (common for old Japanese
    pages served as Shift-JIS). charset-normalizer is a direct dependency.
    """
    from charset_normalizer import from_bytes

    best = from_bytes(raw).best()
    return best.encoding if best is not None else None


def decode_body(raw: bytes, header_charset: str | None, declared: str | None) -> str:
    """Decode response bytes to text, resolving the charset in priority order:

    1. the HTTP ``Content-Type`` charset, when the server sent one (authoritative);
    2. ``declared`` — a charset the document states about *itself*, for a type
       that can say so (HTML's ``<meta>``); None for one that can't;
    3. utf-8, when the bytes are valid utf-8;
    4. charset-normalizer's statistical detection;
    5. utf-8 with replacement, as a last resort.

    Two failure modes pull opposite ways. Old Japanese pages served as
    Shift-JIS/cp932 with no charset header mojibake under a blind utf-8 decode,
    which is why detection exists; but detection is statistical and goes wrong on
    short input — a two-line utf-8 changelog reads as ``mac_latin2``. Step 3
    settles it: utf-8 is self-validating, so a clean *strict* decode is near-proof,
    while cp932 bytes fail it and fall through to detection.

    A junk label (a bogus ``charset=utf-8x-bogus`` would raise ``LookupError`` and
    escape ``fetch_one``'s except tuple) is skipped rather than allowed to lose the
    document; the final decode replaces undecodable bytes rather than raising.
    """
    for label in (header_charset, declared):
        decoded = try_decode(raw, label)
        if decoded is not None:
            return decoded
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    detected = try_decode(raw, detect_charset(raw))
    return detected if detected is not None else raw.decode("utf-8", errors="replace")


def resolves_without_detection(raw: bytes, declared: str | None) -> bool:
    """Whether these bytes reach a charset without step 4's guess.

    True when the document states an encoding that works, or is valid utf-8 —
    both facts about the bytes themselves, so the same bytes always decode the
    same way. False means only statistical detection is left, whose answer is a
    guess that can differ from the one the HTTP header once gave.

    The header is deliberately not a parameter: every caller of this is re-reading
    a stored blob, and the header was never stored.
    """
    return try_decode(raw, declared) is not None or _is_utf8(raw)


def _is_utf8(raw: bytes) -> bool:
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
