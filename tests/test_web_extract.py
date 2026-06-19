"""Tests for web_extract: conservative date extraction (no network)."""

from __future__ import annotations

import web_extract


def test_extract_date_null_when_only_weak_year_signal():
    html = (
        '<html><head><meta name="date" content="2024"></head>'
        "<body><article><p>Defunct maker.</p>"
        "<footer>© 2024 Acme</footer></article></body></html>"
    )
    assert web_extract.extract(html, "http://x").last_updated is None


def test_extract_date_is_most_recent_real_date():
    html = (
        "<html><head>"
        '<meta property="article:published_time" content="2023-06-15">'
        '<meta property="article:modified_time" content="2024-08-01">'
        "</head><body><article><p>y</p></article></body></html>"
    )
    assert web_extract.extract(html, "http://x").last_updated == "2024-08-01"
