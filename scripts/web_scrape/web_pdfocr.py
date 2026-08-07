#!/usr/bin/env python3
"""OCR pass for cached PDFs — reads every sheet's pixels into ``pages.ocr_text``.

22% of cached PDFs are fully image-only, 28% of sheets yield no text, and the
critical words (model names as table headers, coil charts baked into diagrams)
are often ink the text layer never held. This pass rasterizes each sheet with
Quartz and reads it with macOS Vision, storing the result in ``ocr_text`` — the
findable tier ``search`` indexes alongside the text layer. The division of
labor: **OCR points you at the sheet; your eyes do the transcribing.** The words
on a sheet are quotable, but not from this column — a plausible misreading
(``1/16"`` for ``11/16"``) is invisible here, so a quote comes from rendering the
sheet and reading the ink.

    # every un-OCR'd PDF in the cache (~560ms/sheet, single-threaded)
    uv run python scripts/web_scrape/web_pdfocr.py
    # one document; --force re-reads one that already has an OCR tier
    uv run python scripts/web_scrape/web_pdfocr.py --url <url> [--force]

macOS-only, like the image OCR it shares a backend with: Quartz and Vision ship
with the OS, cost no model download and no network, and ``pyobjc-framework-Vision``
(a macOS-only dependency) brings Quartz with it. A host that can't OCR loses
nothing — the blobs are on disk and R2, and ``upsert_page`` keeps the stored
tier across refetches until the bytes themselves change.

Failure shape, measured across the corpus before building this: CoreGraphics
logs scary decoder noise mid-document (JBIG2 symbol dictionaries, allocation
failures) that is fully recovered — every sheet of the documents that logged
them came back complete — so stderr noise is never wired to a failure path.
What is distinguished, per sheet and only in flight: the raster refusing,
Vision erroring, and Vision finding nothing. The first two warn; the last is a
real finding about the sheet (the corpus's only empty sheets are its three
genuinely blank pages). All three leave the same empty run between markers once
written, which v1 accepts — per-sheet outcome recording would be insurance
against a failure measured at zero.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3

# Allow sibling imports whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_cache
import web_ocr

# 144dpi. Line yield plateaus here: scale 1.0 loses a third of the recognized
# lines and scale 4.0 gains none, so there is one setting and one code path.
SCALE = 2.0


def _require_quartz() -> tuple[Any, Any]:
    """The Quartz and Vision modules, or ``OcrUnavailableError`` off macOS."""
    try:
        import Quartz
        import Vision
    except ImportError as exc:
        raise web_ocr.OcrUnavailableError(
            "PDF OCR needs macOS Quartz + Vision (pyobjc-framework-Vision); "
            "they are installed by `uv sync` on macOS only. The blobs are "
            "stored regardless — run this pass from a Mac."
        ) from exc
    return Quartz, Vision


# ANN401 waived: Quartz is a pyobjc dynamic bridge with no static types — the
# module, its pages, and its CGImages are all runtime-shaped, and a Protocol
# restating one caller's usage would be documentation cosplaying as a type.
def _render_sheet(quartz: Any, page: Any) -> Any | None:  # noqa: ANN401
    """Rasterize one PDF page to a ``CGImage`` at ``SCALE``, or None if refused.

    The bitmap is filled white before drawing: a fresh context is transparent,
    and Vision reads dark-on-transparent as nothing at all — the sheet would
    come back silently empty, indistinguishable from an image-only page with no
    legible text. The drawing transform is taken against the crop box, with
    width/height swapped for 90/270 rotation, so landscape schematics land on
    the canvas instead of rendering off it.
    """
    q = quartz  # the module, passed in so this file imports Quartz once
    box = q.CGPDFPageGetBoxRect(page, q.kCGPDFCropBox)
    rotation = q.CGPDFPageGetRotationAngle(page) % 360
    width, height = box.size.width, box.size.height
    if rotation in (90, 270):
        width, height = height, width
    px_w, px_h = max(int(width * SCALE), 1), max(int(height * SCALE), 1)
    ctx = q.CGBitmapContextCreate(
        None,
        px_w,
        px_h,
        8,
        0,
        q.CGColorSpaceCreateDeviceRGB(),
        q.kCGImageAlphaNoneSkipLast,
    )
    if ctx is None:
        return None
    q.CGContextSetRGBFillColor(ctx, 1.0, 1.0, 1.0, 1.0)
    q.CGContextFillRect(ctx, q.CGRectMake(0, 0, px_w, px_h))
    q.CGContextScaleCTM(ctx, SCALE, SCALE)
    transform = q.CGPDFPageGetDrawingTransform(
        page, q.kCGPDFCropBox, q.CGRectMake(0, 0, width, height), 0, True
    )
    q.CGContextConcatCTM(ctx, transform)
    q.CGContextDrawPDFPage(ctx, page)
    return q.CGBitmapContextCreateImage(ctx)


def ocr_pdf(raw: bytes, url: str) -> tuple[str, int]:
    """OCR every sheet of a PDF; returns ``(ocr_text, sheet_count)``.

    ``ocr_text`` carries a ``\\f`` marker line after every sheet — the final one
    included, matching ``web_pdftext``'s poppler normalization — so both columns
    split identically and a sheet's ordinal means the same thing in each. A
    blank sheet still contributes its marker; dropping it would shift every
    later sheet's ordinal, the one thing the markers promise.

    A sheet whose raster is refused or whose Vision request errors warns on
    stderr and contributes an empty sheet; Vision finding nothing is silent —
    a real finding, not a failure. Raises ``OcrUnavailableError`` off macOS and
    ``OcrFailedError`` when Quartz cannot open the document at all (which
    leaves it in the un-OCR'd gap, retried on the next run).
    """
    quartz, vision = _require_quartz()
    from Foundation import NSData

    data = NSData.dataWithBytes_length_(raw, len(raw))
    provider = quartz.CGDataProviderCreateWithCFData(data)
    # Hoisted out of the sheet loop: creating the document per sheet reparses
    # the whole blob every time, and the heaviest cached manual is 65MB.
    doc = quartz.CGPDFDocumentCreateWithProvider(provider)
    if doc is None:
        raise web_ocr.OcrFailedError("Quartz could not open this PDF")
    sheet_count = quartz.CGPDFDocumentGetNumberOfPages(doc)
    if not sheet_count:
        raise web_ocr.OcrFailedError("Quartz reads this PDF as zero pages")

    parts: list[str] = []
    for n in range(1, sheet_count + 1):
        text: str | None = None
        page = quartz.CGPDFDocumentGetPage(doc, n)
        image = _render_sheet(quartz, page) if page is not None else None
        if image is None:
            print(f"WARNING: sheet {n} raster refused: {url}", file=sys.stderr)
        else:
            # The CGImage goes to Vision directly — a PNG round trip costs
            # ~15x the render time and gains nothing.
            handler = vision.VNImageRequestHandler.alloc().initWithCGImage_options_(
                image, None
            )
            try:
                lines = web_ocr.recognize_lines(handler)
            except web_ocr.OcrFailedError as exc:
                print(
                    f"WARNING: sheet {n} Vision errored: {url} ({exc})", file=sys.stderr
                )
            else:
                # The confidence floor is the only filter: short numeric lines
                # (part numbers, pin designators) are exactly what catalog
                # corrections search for and must not be dropped as junk.
                text = web_ocr.filter_lines(lines, web_ocr.MIN_CONFIDENCE)
        if text:
            parts.append(text)
        parts.append("\f")
    return "\n".join(parts), sheet_count


def _text_sheet_count(text: str | None) -> int | None:
    """How many sheets a stored text layer is divided into, or None if it has
    no page markers (an unpaginated import, or no text at all)."""
    if not text:
        return None
    count = sum(1 for line in text.split("\n") if line == "\f")
    return count or None


def ocr_one(con: sqlite3.Connection, rec: web_cache.PageRow) -> bool:
    """OCR one cached PDF row and store the result; True when written.

    The write is one UPDATE, its own transaction, so an interrupted run leaves
    whole documents done or untouched and the ``ocr_text IS NULL`` gap query
    stays accurate at document grain. Before writing, the sheet counts of the
    two columns are asserted equal wherever the text layer is paginated —
    the only thing standing between a rasterization/extraction mismatch and
    ``page 41`` meaning two different sheets in the two columns.
    """
    url = rec["url"]
    blob = web_cache.blob_path(rec["content_sha"], ext="pdf")
    if not blob.exists():
        print(f"WARNING: blob missing: {blob.name} for {url}", file=sys.stderr)
        return False
    started = time.monotonic()
    try:
        ocr_text, sheets = ocr_pdf(blob.read_bytes(), url)
    except web_ocr.OcrFailedError as exc:
        print(f"WARNING: OCR failed for {url}: {exc}", file=sys.stderr)
        return False
    text_sheets = _text_sheet_count(rec["text"])
    if text_sheets is not None and text_sheets != sheets:
        print(
            f"ERROR: sheet count mismatch for {url}: text layer has "
            f"{text_sheets}, raster has {sheets} — not writing, the two "
            f"columns would disagree about which sheet 'page N' is",
            file=sys.stderr,
        )
        return False
    # Guarded on the sha captured before the (minutes-long) OCR: a refetch or
    # forced import can swap the row's bytes mid-read, and writing by URL alone
    # would attach the old blob's reading to the new version — bypassing the
    # staleness rule upsert_page enforces. A miss leaves the row in the
    # ocr_text IS NULL gap, where the next run reads the current bytes.
    cur = con.execute(
        "UPDATE pages SET ocr_text = ? WHERE url = ? AND content_sha = ?",
        (ocr_text, url, rec["content_sha"]),
    )
    con.commit()
    if cur.rowcount == 0:
        print(
            f"WARNING: not written — the row changed while OCR ran "
            f"(new bytes or removed): {url}; the next pass reads the "
            f"current version",
            file=sys.stderr,
        )
        return False
    elapsed = time.monotonic() - started
    print(f"ocr'd ({sheets} sheets, {len(ocr_text):,} chars, {elapsed:.0f}s): {url}")
    return True


def run(url: str | None = None, *, force: bool = False) -> int:
    """OCR the un-OCR'd cached PDFs (or one URL); returns a process exit code.

    Documents that fail stay un-OCR'd and are retried on the next run — v1
    records no per-document failure state, accepting perpetual retry over
    machinery for a case that has not occurred.
    """
    con = web_cache.connect()
    web_cache.init_schema(con)
    try:
        if url is not None:
            rec = web_cache.get(url, con=con)
            if rec is None:
                print(f"not cached: {url}", file=sys.stderr)
                return 1
            if rec["content_type"] != "application/pdf":
                print(
                    f"not a PDF ({rec['content_type']}): {url} — images OCR at "
                    f"fetch time, and other types have no sheets to read",
                    file=sys.stderr,
                )
                return 1
            if rec["ocr_text"] is not None and not force:
                print(
                    f"already OCR'd: {url} (--force to re-read it)",
                    file=sys.stderr,
                )
                return 1
            rows = [rec]
        else:
            rows = [
                web_cache.PageRow(**dict(r))  # type: ignore[typeddict-item]
                for r in con.execute(
                    "SELECT * FROM pages WHERE content_type = 'application/pdf' "
                    "AND ocr_text IS NULL ORDER BY url"
                ).fetchall()
            ]
            if not rows:
                print("every cached PDF already has an OCR tier")
                return 0
        try:
            written = sum(ocr_one(con, rec) for rec in rows)
        except web_ocr.OcrUnavailableError as exc:
            # A fact about the host, not any document; it won't fix itself
            # mid-run, so stop with the actionable message.
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        print(f"{written}/{len(rows)} documents OCR'd")
        return 0 if written == len(rows) else 1
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="OCR cached PDFs' sheets into the searchable ocr_text tier."
    )
    parser.add_argument(
        "--url", help="One cached PDF to read (default: every un-OCR'd PDF)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-OCR a document that already has an OCR tier (needs --url).",
    )
    args = parser.parse_args(argv)
    if args.force and not args.url:
        parser.error("--force needs --url: a corpus-wide re-read is never routine")
    return run(args.url, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
