"""Tests for web_render: the thin trigger and render error handling (no browser).

The real headless browser is not unit-tested; ``render``'s error handling is
exercised with a fake browser, and the thin-detection logic is a pure function.
"""

from __future__ import annotations

import pytest
import web_render

# --------------------------------------------------------------------------- #
# is_thin — the render trigger
# --------------------------------------------------------------------------- #


def test_is_thin_true_for_none_and_short_and_whitespace():
    assert web_render.is_thin(None)
    assert web_render.is_thin("short")
    assert web_render.is_thin("   \n  ")  # whitespace-only counts as thin


def test_is_thin_false_for_long_text():
    assert not web_render.is_thin("x" * (web_render.THIN_TEXT_CHARS + 1))


def test_is_thin_threshold_is_configurable():
    assert web_render.is_thin("x" * 50, thin_chars=100)
    assert not web_render.is_thin("x" * 50, thin_chars=10)


# --------------------------------------------------------------------------- #
# render error handling (real render/_render_get, fake browser — no Chromium)
# --------------------------------------------------------------------------- #


def test_render_propagates_browser_unavailable():
    # A setup failure must propagate so the batch stops, not be swallowed as None.
    class _FailGet:
        def get(self):
            raise web_render.BrowserUnavailableError("no chromium")

    with pytest.raises(web_render.BrowserUnavailableError):
        web_render.render("https://x.com/p", _FailGet())


def test_render_returns_none_on_playwright_error(capsys):
    # A per-page navigation/timeout error is caught and logged, returning None so
    # the caller falls back to the plain result.
    from playwright.sync_api import Error as PlaywrightError

    class _FakeLive:
        def new_page(self, **_kwargs: object) -> object:
            raise PlaywrightError("navigation timed out")

    class _FakeBrowser:
        def get(self) -> object:
            return _FakeLive()

    assert web_render.render("https://x.com/p", _FakeBrowser()) is None
    assert "render failed" in capsys.readouterr().err
