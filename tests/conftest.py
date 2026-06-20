"""Shared test fixtures for the web evidence cache.

These are the project's first tests. They cover the web-scrape cache
(scripts/web_scrape/), the most logic-heavy Python in the repo. Everything here
runs fully offline: pure functions, a throwaway SQLite under tmp_path, and a
monkeypatched ``http_get`` — no network.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator

# The web-cache modules are scripts run from a flat dir; import them the same way
# the fetcher and the cross-repo patch authors do (add the dir to sys.path).
SCRIPTS_WEB = Path(__file__).resolve().parent.parent / "scripts" / "web_scrape"
sys.path.insert(0, str(SCRIPTS_WEB))

import web_cache  # noqa: E402


@pytest.fixture
def make_pdf():
    """Factory building a minimal, well-formed PDF as bytes (no pypdf writer needed).

    Returns a callable. The default produces a one-page PDF whose text extracts to
    "Hello PDF evidence"; pass ``text=""`` for a scanned-style PDF that extracts to
    nothing, and ``title`` / ``moddate`` / ``creationdate`` to populate the Info
    dict. Offsets are computed so pypdf parses it strictly (no xref recovery)."""

    def _make(
        *,
        title: str | None = None,
        moddate: str | None = None,
        creationdate: str | None = None,
        text: str = "Hello PDF evidence",
    ) -> bytes:
        stream = b"BT /F1 24 Tf 100 700 Td (" + text.encode("latin-1") + b") Tj ET"
        objs = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
            b"<< /Length "
            + str(len(stream)).encode()
            + b" >>\nstream\n"
            + stream
            + b"\nendstream",
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        ]
        info = b"<<"
        if title:
            info += b" /Title (" + title.encode("latin-1") + b")"
        if moddate:
            info += b" /ModDate (" + moddate.encode("ascii") + b")"
        if creationdate:
            info += b" /CreationDate (" + creationdate.encode("ascii") + b")"
        info += b" >>"
        objs.append(info)

        out = bytearray(b"%PDF-1.4\n")
        offsets: list[int] = []
        for i, body in enumerate(objs, start=1):
            offsets.append(len(out))
            out += str(i).encode() + b" 0 obj\n" + body + b"\nendobj\n"
        xref_pos = len(out)
        n = len(objs) + 1
        out += b"xref\n0 " + str(n).encode() + b"\n0000000000 65535 f \n"
        for off in offsets:
            out += f"{off:010d} 00000 n \n".encode()
        out += (
            b"trailer\n<< /Size " + str(n).encode() + b" /Root 1 0 R /Info 6 0 R >>\n"
        )
        out += b"startxref\n" + str(xref_pos).encode() + b"\n%%EOF\n"
        return bytes(out)

    return _make


@pytest.fixture
def cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[sqlite3.Connection]:
    """Point the cache at a fresh tmp dir; return an initialized write connection.

    Monkeypatches web_cache's path globals so connect()/get()/blob_path() all
    resolve under tmp_path — every test gets an isolated DB + raw blob dir.
    """
    web_dir = tmp_path / "web"
    monkeypatch.setattr(web_cache, "WEB_DIR", web_dir)
    monkeypatch.setattr(web_cache, "DB_PATH", web_dir / "cache.sqlite")
    monkeypatch.setattr(web_cache, "RAW_DIR", web_dir / "raw")
    con = web_cache.connect()
    web_cache.init_schema(con)
    yield con
    con.close()
