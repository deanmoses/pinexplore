#!/usr/bin/env python3
"""Re-extract text/title for cached HTML pages from their stored blobs.

Run after an extraction-pipeline change so the whole corpus reflects the
current extractor, instead of splitting into "pages fetched before" and
"pages fetched after". No network: every page is re-extracted from the raw
blob already on disk, which takes seconds for the full corpus and is
idempotent — so the run is unconditional (no version comparison, no stale-row
selector to get wrong) and there is no ``--dry-run`` (if the output is wrong,
fix the extractor and run it again; to preview one page, run the extractor on
its blob).

What it may replace is exactly the text the HTML extractor produced:

- ``text_source = 'html'`` rows, plus ``NULL`` rows — pages written before the
  ``text_source`` column existed, all of them machine fetches (none carry an
  import marker). Rewritten rows get ``text_source = 'html'``: for a NULL row
  that is not the back-filled guess ``init_schema`` refuses to make, because
  after re-extraction it is a fact about the new text by construction.
- Any other ``text_source`` on an HTML row is skipped and tallied: ``manual``
  is a human transcription (mirroring ``_resolve_text``'s rule), and the
  importer's ``--text-source`` can store other machine-read text (``ocr``)
  that this extractor didn't produce and must not replace.
- Non-HTML types (PDF, VTT, image) are untouched; only the HTML path changed.

Two guards earn their place. Never overwrite non-empty text with empty —
re-running cannot recover from a blanking bug, because the same bug produces
the same empty result. And never overwrite on a decode regression: the
backfill decodes from the blob alone, without the original HTTP header
charset (never stored), so re-extracted text containing U+FFFD where the
stored text had none means the stored text is the only artifact that encoded
the correct charset — replacing it would be unrecoverable. Such rows are
skipped and tallied; a ``web_fetch.py --force`` refetch restores the header
charset and re-extracts properly. Measured, blob-only decoding reproduces
fetch-time decoding for the whole current corpus — every HTML blob either
declares a ``<meta>`` charset or is valid UTF-8 — so this guard is insurance
for the blob that measurement hasn't met yet.

``title`` is rewritten along with ``text`` — both come from the same
extractor. ``last_updated`` is left alone: htmldate and its inputs are
unchanged, so recomputing is a no-op. No ``fetches`` row is written — no fetch
happened. Updates go through a normal SQL ``UPDATE`` on ``pages``, so the FTS
sync triggers keep the index current.
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
    """Re-extract every eligible HTML page; return the tally by outcome."""
    own = con is None
    db = con if con is not None else web_cache.connect()
    tally = {
        "rewritten": 0,
        "skipped (manual)": 0,
        "skipped (other text_source)": 0,
        "skipped (missing blob)": 0,
        "skipped (empty re-extraction)": 0,
        "skipped (decode regression)": 0,
    }
    try:
        handler = handler_for("text/html")
        assert handler is not None
        # Every MIME type the HTML handler claims (text/html and
        # application/xhtml+xml), not a literal — an XHTML row must not be
        # silently left on the old extraction forever.
        mimes = sorted(handler.mime_types)
        placeholders = ", ".join("?" for _ in mimes)
        rows = db.execute(
            f"SELECT url, content_sha, content_type, text, text_source FROM pages "  # noqa: S608
            f"WHERE content_type IN ({placeholders}) ORDER BY url",
            mimes,
        ).fetchall()
        for row in rows:
            source = row["text_source"]
            if source is not None and source != "html":
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
                "UPDATE pages SET text = ?, title = ?, text_source = 'html' "
                "WHERE url = ?",
                (meta.text, meta.title, row["url"]),
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
