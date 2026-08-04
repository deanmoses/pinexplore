#!/usr/bin/env python3
"""Re-extract text/title for cached pages from their stored blobs.

Run after an extraction-pipeline change so the whole corpus reflects the
current extractor, instead of splitting into "pages fetched before" and
"pages fetched after". No network: every page is re-extracted from the raw
blob already on disk, which takes seconds for the full corpus and is
idempotent — so the run is unconditional (no version comparison, no stale-row
selector to get wrong) and there is no ``--dry-run`` (if the output is wrong,
fix the extractor and run it again; to preview one page, run the extractor on
its blob).

Scope is the handler's own declaration, ``backfillable`` — HTML and PDF today,
the types read by a parser whose output is a pure function of the stored bytes.
A type read by a *recognizer* (OCR over an image) declares False and is never
swept; see that flag's comment in ``content_types/base.py``.

Within that scope the rule is one rule, applied per row: re-extract only what
this handler's own extractor produced, which is the row whose ``text_source``
is the handler's own (or ``NULL``, from pages written before the column
existed — all machine fetches, none carrying an import marker). Rewritten rows
are stamped with the handler's ``text_source``: for a ``NULL`` row that is not
the back-filled guess ``init_schema`` refuses to make, because after
re-extraction it is a fact about the new text by construction.

Any *other* ``text_source`` is skipped and tallied, and the two that matter
both sit on documents in scope: ``manual`` is a human transcription (mirroring
``_resolve_text``'s rule), and ``ocr`` is text a machine read from pixels — a
scanned PDF whose words were recovered outside this tool, precisely because
its text layer is empty. Re-extracting either would trade reviewed or
recovered text for that emptiness, so neither is touched here; changing them
stays a deliberate act through ``web_import.py``.

Two guards earn their place. Never overwrite non-empty text with empty —
re-running cannot recover from a blanking bug, because the same bug produces
the same empty result. This also covers the host that lacks a backend
entirely: a machine without poppler re-extracts every PDF to nothing, and the
guard is what stops that from emptying the corpus (``unavailable`` is the
fetch path's version of the same promise). And never overwrite on a decode
regression: the backfill decodes from the blob alone, without the original
HTTP header charset (never stored), so re-extracted text containing U+FFFD
where the stored text had none means the stored text is the only artifact
that encoded the correct charset — replacing it would be unrecoverable. Such
rows are skipped and tallied; a ``web_fetch.py --force`` refetch restores the
header charset and re-extracts properly. Measured, blob-only decoding
reproduces fetch-time decoding for the whole current corpus — every HTML blob
either declares a ``<meta>`` charset or is valid UTF-8 — so this guard is
insurance for the blob that measurement hasn't met yet. It costs nothing on a
binary type, which resolves its own encoding internally and never consults a
header charset.

``title`` is rewritten along with ``text`` — both come from the same
extractor. ``last_updated`` is left alone: neither htmldate nor the PDF Info
dict changed, so recomputing is a no-op. No ``fetches`` row is written — no
fetch happened. Updates go through a normal SQL ``UPDATE`` on ``pages``, so
the FTS sync triggers keep the index current.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

# Allow sibling imports whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_cache
from content_types import extension_for, handler_for


def backfill(con: sqlite3.Connection | None = None) -> dict[str, int]:
    """Re-extract every eligible page; return the tally by outcome."""
    own = con is None
    db = con if con is not None else web_cache.connect()
    if own:
        # Like every other writable entry point: create/migrate the schema
        # before touching it, so a fresh checkout or an older pulled cache
        # fails on nothing rather than on a missing table or column.
        web_cache.init_schema(db)
    tally = {
        "rewritten": 0,
        "skipped (manual)": 0,
        "skipped (other text_source)": 0,
        "skipped (missing blob)": 0,
        "skipped (empty re-extraction)": 0,
        "skipped (decode regression)": 0,
    }
    try:
        rows = db.execute(
            "SELECT url, content_sha, content_type, text, text_source FROM pages "
            "ORDER BY url"
        ).fetchall()
        for row in rows:
            # Routed by handler, never by a MIME literal: the HTML handler
            # claims application/xhtml+xml as well as text/html, and an XHTML
            # row must not be silently left on the old extraction forever.
            handler = handler_for(row["content_type"])
            if handler is None or not handler.backfillable:
                continue
            source = row["text_source"]
            if source is not None and source != handler.text_source:
                key = (
                    "skipped (manual)"
                    if source == "manual"
                    else "skipped (other text_source)"
                )
                tally[key] += 1
                print(f"skip ({source}): {row['url']}")
                continue
            ext = extension_for(row["content_type"]) or "html"
            blob = web_cache.blob_path(row["content_sha"], ext=ext)
            if not blob.exists():
                tally["skipped (missing blob)"] += 1
                print(
                    f"WARNING: blob missing: {blob.name} for {row['url']}",
                    file=sys.stderr,
                )
                continue
            raw = blob.read_bytes()
            # No header charset — it was never stored. The blob's own <meta>
            # declaration or a statistical detection stands in; see the module
            # docstring for why that reproduces fetch-time decoding.
            decoded = handler.decode(raw, None)
            meta = handler.extract(raw, decoded, row["url"])
            old_text = row["text"] or ""
            new_text = meta.text or ""
            if old_text and not new_text:
                tally["skipped (empty re-extraction)"] += 1
                print(
                    f"WARNING: re-extraction came back empty, keeping stored text: "
                    f"{row['url']}",
                    file=sys.stderr,
                )
                continue
            if "�" in new_text and "�" not in old_text:
                tally["skipped (decode regression)"] += 1
                print(
                    f"WARNING: re-extraction introduced replacement characters "
                    f"(U+FFFD) — the stored text was decoded with the original "
                    f"header charset and must not be replaced; refetch with "
                    f"--force to re-extract properly: {row['url']}",
                    file=sys.stderr,
                )
                continue
            db.execute(
                "UPDATE pages SET text = ?, title = ?, text_source = ? WHERE url = ?",
                (meta.text, meta.title, handler.text_source, row["url"]),
            )
            tally["rewritten"] += 1
        db.commit()
    finally:
        if own:
            db.close()
    return tally


def main() -> int:
    """CLI entry point."""
    tally = backfill()
    print()
    for key, count in tally.items():
        if count:
            print(f"{key}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
