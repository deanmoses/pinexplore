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

Scope is the handler's ``backfillable`` flag — HTML and PDF today; see that
flag in ``content_types/base.py`` for why a type opts out.

Within it, one selection rule: re-extract only what this handler's extractor
produced — the row whose ``text_source`` is the handler's own, or ``NULL``
from before that column existed. Rewritten rows are stamped with it, which for
a ``NULL`` row is a fact about the new text by construction.

Any other ``text_source`` is skipped and tallied. ``manual`` is a human
transcription; ``ocr`` is a scanned PDF whose words were recovered elsewhere
*because* its text layer is empty — re-extracting would trade either for that
emptiness. Both change only through ``web_import.py``.

``title`` follows the text, except on an imported row where it may be a
person's: ``web_import.py --title`` leaves ``text_source`` as the handler's
own, so only ``imported`` distinguishes a curated title from an extracted one.
``last_updated`` is never recomputed — the other field ``--date`` lets a person
set, and unchanged extractors would only rewrite it to itself.

No ``fetches`` row is written; no fetch happened. Updates go through a normal
SQL ``UPDATE`` on ``pages``, so the FTS sync triggers keep the index current.
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
from content_types import handler_for


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
        "skipped (extractor unavailable)": 0,
        "skipped (empty re-extraction)": 0,
        "skipped (decode regression)": 0,
    }
    try:
        rows = db.execute(
            "SELECT url, content_sha, content_type, text, text_source, title, "
            "imported FROM pages ORDER BY url"
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
            # None means a type no handler claims, which ``handler_for`` above
            # has already ruled out for this row — but it lands in the same
            # tally as a blob that isn't there: either way nothing to re-extract.
            blob = web_cache.blob_for(row)
            if blob is None or not blob.exists():
                tally["skipped (missing blob)"] += 1
                name = blob.name if blob is not None else f"<{row['content_type']}>"
                print(
                    f"WARNING: blob missing: {name} for {row['url']}",
                    file=sys.stderr,
                )
                continue
            raw = blob.read_bytes()
            # No header charset — it was never stored. The blob's own <meta>
            # declaration or a statistical detection stands in; see the module
            # docstring for why that reproduces fetch-time decoding.
            decoded = handler.decode(raw, None)
            meta = handler.extract(raw, decoded, row["url"])
            if meta.unavailable:
                # The extractor produced no *result* — its backend is missing on
                # this host, or it choked on this document — which says nothing
                # about the page. Drops the title/date too, though those come
                # from the half that still worked: half an extraction is not one.
                tally["skipped (extractor unavailable)"] += 1
                continue
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
            # Re-extraction decodes from the blob alone; the HTTP header charset
            # is never stored. So U+FFFD appearing where the stored text had none
            # means that stored text is the only artifact that ever held the
            # right charset, and a --force refetch is the only way back.
            #
            # Text types only: a type that decodes nothing has no header charset
            # to lose and no refetch that would change its output, so here the
            # guard could only strand a row behind advice that cannot work.
            if decoded is not None and "�" in new_text and "�" not in old_text:
                tally["skipped (decode regression)"] += 1
                print(
                    f"WARNING: re-extraction introduced replacement characters "
                    f"(U+FFFD) — the stored text was decoded with the original "
                    f"header charset and must not be replaced; refetch with "
                    f"--force to re-extract properly: {row['url']}",
                    file=sys.stderr,
                )
                continue
            # `--title` stores a curated title while leaving text_source as the
            # handler's own, so only `imported` separates a person's title from an
            # extracted one. Safe when none was supplied: identical bytes extract
            # identically, so the stored value already is this extraction.
            title = row["title"] if row["imported"] else meta.title
            db.execute(
                "UPDATE pages SET text = ?, title = ?, text_source = ? WHERE url = ?",
                (meta.text, title, handler.text_source, row["url"]),
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
