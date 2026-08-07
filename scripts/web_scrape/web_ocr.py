#!/usr/bin/env python3
"""OCR backend for image evidence (see docs/WebCache.md).

Turns the bytes of an image — a scanned flyer, a photographed manual page, a
screenshot of a page we can't scrape — into text the cache can index and quote.
Uses **macOS Vision** (via pyobjc), which needs no system binary, no model
download, and no network: the framework ships with the OS.

Deliberately a *deterministic* text layer, not a smart one. Vision reads the
pixels the same way every run; it garbles hard glyphs (stylized logo art comes
out as junk tokens) but it does not invent sentences the way a language model
asked to "read this flyer" will. That property is the whole point here: a page
in this cache is quoted verbatim downstream, so a fluent wrong transcription
would be far more damaging than obviously-broken text. Low-confidence lines are
dropped (``MIN_CONFIDENCE``) so the junk doesn't reach the FTS index; whatever
survives is still machine output, stored in ``pages.ocr_text`` — the findable
tier — rather than as a text layer. (An **image** needs a
human transcription through ``web_import.py --text-file`` to carry a quote at
all; that is the one path where a person is answerable for the words.)

Imported lazily by the image content handler — a batch with no images never
loads pyobjc, and a non-macOS host only fails if it actually meets an image.
"""

from __future__ import annotations

# Recognized lines below this confidence are dropped. Vision reports coarse
# confidences (0.3 / 0.5 / 1.0 in practice); on the Mecatronics Space Shuttle
# flyer this floor removes exactly the stylized playfield/backglass art
# ("SPAC.", "SAUTTL", "BUMON") while keeping every line of real prose.
MIN_CONFIDENCE = 0.6

# One recognized line: its confidence and its best candidate string.
type OcrLine = tuple[float, str]


class OcrUnavailableError(RuntimeError):
    """OCR was needed but the macOS Vision backend can't be used *here*.

    A setup/platform failure (not macOS, or pyobjc missing) — a fact about the
    host, not about any one image. It won't fix itself mid-run, so a CLI that was
    explicitly asked to OCR reports it and stops, while the image handler
    downgrades it to a warning rather than killing a whole fetch batch. Callers
    treat it as "no opinion about this document", which is why it must never be
    raised for a file that simply can't be read (see ``OcrFailedError``).
    """


class OcrFailedError(RuntimeError):
    """Vision ran but returned an error for this request.

    Apple's ``VNErrorCode`` covers both the file being unreadable (an invalid or
    zero-dimensioned image) and the host having a bad moment (timeout,
    out-of-memory, internal error), and the two aren't reliably separable without
    mapping an enum Apple can extend. So this promises only what it knows: **no
    text was produced**, which is not the same as "this document has no text".
    Callers must treat it as no result rather than an empty one, or a transient
    failure will blank text that the identical bytes still support.

    Separate from ``OcrUnavailableError`` because the operator is asked for
    different things — look at this file, versus install the backend or
    transcribe by hand — not because callers should behave differently.
    """


def filter_lines(lines: list[OcrLine], min_confidence: float) -> str | None:
    """Join recognized lines above ``min_confidence``, in reading order, or None.

    None (never "") when nothing survives, so the caller's thin-content warning
    fires instead of a blank-text page being stored silently.
    """
    kept = [text for confidence, text in lines if confidence >= min_confidence]
    return "\n".join(kept) or None


def recognize_lines(handler: object) -> list[OcrLine]:
    """Run one Vision text-recognition request against a ``VNImageRequestHandler``.

    The one place the recognition settings live, so the image path (bytes in)
    and the PDF-sheet path (``web_pdfocr``, a ``CGImage`` in) cannot drift
    apart. Raises ``OcrFailedError`` when Vision runs but refuses the input; a
    successful run over pixels with no legible text returns an empty list —
    the two must stay distinct, or a failure becomes indistinguishable from a
    genuinely blank page.

    ``usesLanguageCorrection`` is ON deliberately: measured on a Portuguese
    flyer, turning it off degraded real prose ("Unrted States", "NAO" losing
    its tilde) without making proper nouns any more faithful — and an A/B on
    parts pages dense with alphanumeric part numbers recovered identically
    either way. No recognition languages are set — Vision's own detection
    matched a hand-pinned Portuguese+English list exactly, so there is no
    language config to keep current as the corpus grows.
    """
    import Vision

    request = Vision.VNRecognizeTextRequest.alloc().init()
    request.setRecognitionLevel_(0)  # 0 = accurate (1 = fast)
    request.setUsesLanguageCorrection_(True)
    ok, error = handler.performRequests_error_([request], None)  # type: ignore[attr-defined]
    if not ok:
        # Vision loaded fine and refused this input — the input's problem, not
        # the host's, so it must not masquerade as a missing backend.
        raise OcrFailedError(f"Vision could not read this image: {error}")
    lines: list[OcrLine] = []
    for observation in request.results() or []:
        candidates = observation.topCandidates_(1)
        if candidates:
            lines.append((candidates[0].confidence(), candidates[0].string()))
    return lines


def _recognize(raw: bytes) -> list[OcrLine]:
    """Run macOS Vision text recognition over an image's bytes.

    Raises ``OcrUnavailableError`` when the framework isn't importable (not
    macOS, or pyobjc not installed), and ``OcrFailedError`` when Vision runs but
    rejects these bytes (corrupt, truncated, or not an image at all). A
    successful run over an image with no legible text returns an empty list.
    """
    try:
        import Vision
        from Foundation import NSData
    except ImportError as exc:  # not macOS, or pyobjc-framework-Vision missing
        raise OcrUnavailableError(
            "image OCR needs macOS Vision (pyobjc-framework-Vision); it is "
            "installed by `uv sync` on macOS only. Supply the text by hand "
            "instead of OCRing on this host."
        ) from exc

    data = NSData.dataWithBytes_length_(raw, len(raw))
    handler = Vision.VNImageRequestHandler.alloc().initWithData_options_(data, None)
    return recognize_lines(handler)


def ocr_text(raw: bytes, min_confidence: float = MIN_CONFIDENCE) -> str | None:
    """The readable text of an image, or None when nothing legible was found.

    Raises ``OcrUnavailableError`` if the backend isn't available on this host,
    or ``OcrFailedError`` if it is but can't read these particular bytes.
    """
    return filter_lines(_recognize(raw), min_confidence)
