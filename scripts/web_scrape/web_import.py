#!/usr/bin/env python3
"""Import hand-obtained evidence into the web cache (see docs/WebCache.md).

The escape hatch for sources ``web_fetch.py`` cannot retrieve. Some sites refuse
us outright — ipdb.org answers HTTP 403 site-wide, rendered or not — while a
person with a browser can open the very same page. This takes the file that
person saved and files it in the cache like any other evidence: content-addressed
blob, extracted text, FTS index, quotable and citable downstream.

    # a flyer scan the fetcher can't reach, with a reviewed transcription
    uv run python scripts/web_scrape/web_import.py flyer.jpg \\
        --url https://www.ipdb.org/images/4583/image-3.jpg \\
        --text-file flyer.txt --query "mecatronics space shuttle flyer"

    # see what OCR makes of it first (writes nothing) — then correct and import
    uv run python scripts/web_scrape/web_import.py flyer.jpg --url <url> --dry-run

Two rules keep an import honest.

**The row says it was imported.** ``imported=1`` on both the page and the audit
row, and ``http_status`` stays NULL — no request was made, and a fabricated 200
would make the fetch log lie about the one thing it exists to record. The bytes
are stored verbatim, so a citation still re-verifies against the original file.

**The URL is where the bytes live, not where you looked at them.** Store a JPEG
under the address that serves that JPEG. Filing image bytes under a viewer page
(``showpic.pl?id=...``, which serves HTML) would make ``content_type`` and
``content_sha`` describe a resource that URL does not return — a quiet lie that
survives into every citation.

Text is mandatory: a page with no text is dropped from flippatch's
``evidence_pages`` and can't be indexed or quoted, so importing one would be a
no-op dressed as evidence. Supply it with ``--text-file`` (recorded as
``text_source='manual'``, or what ``--text-source`` declares) or let the file's
own handler extract it — OCR for an image, pypdf for a PDF, trafilatura for
saved HTML. A **scanned PDF** is the case that needs the override: its handler
reads a text layer that isn't there, so its words have to come from OCR run
outside this tool, and ``--text-source ocr`` keeps ``manual`` meaning what it
says — a person is answerable for those words. For an image, prefer
reviewing the OCR draft (``--dry-run``) against the picture and importing the
corrected text: OCR is faithful to pixels but garbles stylized lettering, and a
correction should be justified by something visible in the document, never by
what the text is expected to say.
"""

from __future__ import annotations

import argparse
import datetime
import re
import sys
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

# Allow sibling imports whether run as a script or imported (mirrors web_fetch).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_cache
from content_types import HANDLERS, ContentHandler, handler_for, sniff

# What ``--text-source`` may declare: every provenance a handler can produce,
# plus ``manual``. Derived from the registry rather than spelled out, so a new
# handler's label is accepted here the moment it exists — and a typo is not.
TEXT_SOURCES: frozenset[str] = frozenset(
    {handler.text_source for handler in HANDLERS} | {"manual"}
)

# A date the importer will accept: exactly one unambiguous form. A date is
# evidence too, and "Jan 1986" or "01/02/86" would have to be guessed at. The
# shape check stays even though date.fromisoformat does the real validation —
# fromisoformat also accepts "20260101" and ISO week forms, which this column
# should never hold.
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _check_date(date: str) -> None:
    """Reject a --date that isn't a real calendar date.

    Shape alone lets through 2026-99-99 and 2025-02-30, which would sit in
    ``last_updated`` looking like evidence.
    """
    if not _ISO_DATE.match(date):
        raise ImportRefusedError(f"--date must be YYYY-MM-DD, got {date!r}")
    try:
        datetime.date.fromisoformat(date)
    except ValueError as exc:
        raise ImportRefusedError(f"--date {date!r} is not a real date ({exc})") from exc


def _check_text_source(text_source: str, *, has_text: bool) -> None:
    """Reject a --text-source that isn't a known label, or has nothing to label.

    Without ``--text-file`` the stored text is the handler's own extraction, and
    its provenance is a fact about that handler — declaring one would overwrite
    the truth with a preference.
    """
    if not has_text:
        raise ImportRefusedError(
            "--text-source describes the text --text-file supplies; without it "
            "the handler's own extraction is stored under its own label"
        )
    if text_source not in TEXT_SOURCES:
        known = ", ".join(sorted(TEXT_SOURCES))
        raise ImportRefusedError(
            f"--text-source {text_source!r} is not one the cache records ({known})"
        )


class ImportRefusedError(ValueError):
    """The import can't proceed, with a message saying what to fix.

    Raised before anything is written, so a refused import leaves neither a row
    nor a blob behind. ``main`` turns it into a non-zero exit with the message.
    """


def _resolve_handler(raw: bytes, path: Path) -> ContentHandler:
    """The content handler for these bytes: magic-byte signature, then suffix.

    The signature leads because a hand-saved file's name is unreliable — a
    browser's "Save image as" lands JPEGs under ``.txt``, ``.jpeg``, or no
    suffix at all — while its first bytes are not. The suffix is the fallback
    for a type with no signature (saved HTML).
    """
    handler = sniff(raw)
    if handler is not None:
        return handler
    suffix_types = {
        ".html": "text/html",
        ".htm": "text/html",
        ".xhtml": "application/xhtml+xml",
    }
    by_suffix = handler_for(suffix_types.get(path.suffix.lower(), ""))
    if by_suffix is not None:
        return by_suffix
    raise ImportRefusedError(
        f"unsupported content type for {path.name}: no handler recognizes these "
        f"bytes or the '{path.suffix}' suffix. Supported: HTML, PDF, JPEG, PNG."
    )


def _read_transcription(path: Path) -> str:
    """Read a --text-file, turning any read failure into a controlled refusal.

    Both arms are needed and neither is an ``OSError``-only case: a missing or
    unreadable file raises ``OSError``, while a file in some other encoding
    raises ``UnicodeDecodeError`` (a ``ValueError``). Left unhandled, either
    prints a traceback instead of the CLI's message — and naming this path
    matters, since the image is a *different* file that can fail the same way.
    """
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ImportRefusedError(f"cannot read --text-file {path}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ImportRefusedError(
            f"--text-file {path} is not valid UTF-8 ({exc}); re-save it as UTF-8 "
            f"so accented characters survive into the quote"
        ) from exc


def _check_url(raw_url: str) -> str:
    """Validate + normalize the URL these bytes will be cached under."""
    try:
        normalized = web_cache.normalize_url(raw_url)
        parts = urllib.parse.urlsplit(normalized)
    except ValueError as exc:
        raise ImportRefusedError(f"malformed --url {raw_url!r} ({exc})") from exc
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ImportRefusedError(
            f"--url must be the http(s) address the bytes live at, not {raw_url!r}"
        )
    return normalized


def import_one(
    con: sqlite3.Connection | None,
    path: Path,
    *,
    url: str,
    text: str | None = None,
    text_source: str | None = None,
    title: str | None = None,
    date: str | None = None,
    query: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    """Import one hand-obtained file into the cache under ``url``.

    ``text`` is supplied text and always wins over what the file's handler
    extracts; it lands on ``text_source='manual'`` unless ``text_source`` says
    which machine read it instead. That override exists for the document a
    handler cannot read at all — a scanned PDF routes to pypdf, which finds no
    text layer, so OCR run outside this tool is the only way to give it words,
    and calling that a transcription would put a person's name on a machine's
    reading. ``title``/``date`` likewise override extracted values — for an
    image both are otherwise null, since OCR'd words are not a title and an
    EXIF timestamp is not the document's date.

    Raises ``ImportRefusedError`` (before writing anything) on an unusable URL or
    date, an unrecognized file type, text that is blank, a ``text_source``
    outside :data:`TEXT_SOURCES` or given without ``text``, or an already-cached
    URL without ``force``. ``dry_run`` prints what would be stored and writes
    nothing — the review step for an OCR draft; it accepts ``con=None`` so a
    fresh checkout can preview an import without a database being created as a
    side effect of inspecting one.
    """
    if date is not None:
        _check_date(date)
    if text_source is not None:
        _check_text_source(text_source, has_text=text is not None)
    normalized = _check_url(url)
    raw = path.read_bytes()
    handler = _resolve_handler(raw, path)

    existing = web_cache.get(normalized, con=con) if con is not None else None
    if existing is not None and not force and not dry_run:
        how = "imported" if existing["imported"] else "fetched"
        raise ImportRefusedError(
            f"{normalized} is already cached ({how} {existing['last_fetched_at']}); "
            f"pass --force to replace it"
        )

    # Extract even when a transcription was supplied: on a dry run the draft is
    # what you're reviewing, and the handler also yields a title/date to fall
    # back on for a document type that states them.
    meta = handler.extract(raw, handler.decode(raw, None), normalized)
    if text is not None and not text.strip():
        # Passing --text-file is a claim that a reviewed transcription exists.
        # Quietly falling back to the OCR/parser draft would import unreviewed
        # machine text under the banner of a reviewed import — the one outcome
        # the review step exists to prevent — and an empty file usually means it
        # never saved, or the wrong path was typed.
        raise ImportRefusedError(
            "--text-file is empty: nothing to store as the reviewed transcription "
            "(omit it to store what extraction found, or fill it in)"
        )
    if text is not None:
        page_text: str | None = text
        stored_source = text_source or "manual"
    else:
        page_text = meta.text
        stored_source = handler.text_source

    sha = web_cache.content_sha(raw)
    blob = web_cache.blob_path(sha, ext=handler.extension)
    page_title = title or meta.title
    last_updated = date or meta.last_updated

    if dry_run:
        print(f"dry run — nothing written: {normalized}")
        print(f"    file         {path} ({len(raw):,} bytes)")
        if existing is not None:
            # A dry run skips migration by design, so this row can predate
            # `imported`/`text_source` — the shape `make pull` hands you until a
            # migrated cache is pushed. Report what's there; a preview that
            # raises KeyError over a missing column is worse than one that says
            # "unknown".
            how = "imported" if existing.get("imported") else "fetched"
            source = existing.get("text_source") or "unknown"
            needs = "" if force else " — replacing it needs --force"
            print(
                f"    already cached  yes ({how} {existing['last_fetched_at']}, "
                f"text_source {source}){needs}"
            )
        print(f"    content type {handler.canonical_mime or handler.extension}")
        print(f"    blob would be {blob}")
        print(f"    title        {page_title or '(none)'}")
        print(f"    date         {last_updated or '(none)'}")
        print(f"    text_source  {stored_source}")
        if page_text and page_text.strip():
            print(f"    text ({len(page_text):,} chars) — review against the original:")
            print("\n".join(f"      {line}" for line in page_text.splitlines()))
        else:
            print(
                "    text         (none) — no text, so nothing to index or quote; "
                "supply --text-file"
            )
        return

    if not page_text or not page_text.strip():
        raise ImportRefusedError(
            f"no text for {normalized}: an untranscribed page can't be indexed or "
            f"quoted (it is dropped from flippatch's evidence_pages). Supply "
            f"--text-file, or run --dry-run to see what extraction found."
        )

    # Only a real import reaches here, so the writable connection is required;
    # dry runs return above and may hold None.
    assert con is not None, "import_one needs a writable connection unless dry_run"
    imported_at = web_cache.now_iso()
    changed = existing is None or existing.get("content_sha") != sha
    if not blob.exists():
        blob.write_bytes(raw)

    web_cache.upsert_page(
        con,
        url=normalized,
        raw_url=url,
        content_sha=sha,
        fetched_at=imported_at,
        last_updated=last_updated,
        title=page_title,
        # No HTTP request happened; NULL rather than a plausible-looking 200.
        http_status=None,
        content_type=handler.canonical_mime or None,
        text=page_text,
        rendered=False,
        text_source=stored_source,
        imported=True,
    )
    web_cache.append_fetch(
        con,
        url=normalized,
        fetched_at=imported_at,
        search_query=query,
        http_status=None,
        content_sha=sha,
        changed=changed,
        imported=True,
    )
    state = "new" if existing is None else ("changed" if changed else "unchanged")
    print(f"imported [{stored_source}] ({state}): {normalized}\n    {blob}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import a hand-obtained file into the web evidence cache."
    )
    parser.add_argument("file", help="Path to the file saved from a browser.")
    parser.add_argument(
        "--url",
        required=True,
        help=(
            "The http(s) address these bytes live at — the image URL for an "
            "image, not the viewer page that displays it."
        ),
    )
    parser.add_argument(
        "--text-file",
        help=(
            "Text to store as the page text (recorded as text_source=manual "
            "unless --text-source says otherwise). Wins over whatever the "
            "file's handler extracts."
        ),
    )
    parser.add_argument(
        "--text-source",
        choices=sorted(TEXT_SOURCES),
        help=(
            "How the --text-file text was derived, when it isn't a human "
            "transcription: 'ocr' for pixels a machine read (a scanned PDF, "
            "whose handler finds no text layer to extract)."
        ),
    )
    parser.add_argument("--title", help="Title for the page row.")
    parser.add_argument(
        "--date", help="The date the document itself states (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--query", help="The search intent that led to this source (logged)."
    )
    parser.add_argument(
        "--force", action="store_true", help="Replace an already-cached URL."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be stored (including the OCR draft) and write nothing.",
    )
    args = parser.parse_args(argv)

    # A dry run must not touch the store it is previewing: connect() would create
    # ingest_sources/web/ and the database on a fresh checkout, and init_schema
    # would commit migrations — writes, however harmless, that the flag promises
    # not to make. Read-only when there's something to read, otherwise nothing.
    con: sqlite3.Connection | None
    if args.dry_run:
        con = web_cache.connect(read_only=True) if web_cache.DB_PATH.exists() else None
    else:
        con = web_cache.connect()
        web_cache.init_schema(con)
    try:
        text = _read_transcription(Path(args.text_file)) if args.text_file else None
        import_one(
            con,
            Path(args.file),
            url=args.url,
            text=text,
            text_source=args.text_source,
            title=args.title,
            date=args.date,
            query=args.query,
            force=args.force,
            dry_run=args.dry_run,
        )
    except ImportRefusedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"ERROR: cannot read {args.file}: {exc}", file=sys.stderr)
        return 1
    finally:
        if con is not None:
            con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
