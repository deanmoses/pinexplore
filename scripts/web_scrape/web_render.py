#!/usr/bin/env python3
"""Headless-render fallback for JavaScript-only pages (see docs/WebCache.md).

A client-rendered (React/Vue/Angular) page returns a skeleton to the plain GET
that extracts to near-nothing. When that happens the fetcher escalates here:
execute the page's JavaScript in headless Chromium (Playwright) and return the
live DOM as a ``web_http.Resp`` the orchestrator can store like any other fetch.
Playwright is imported lazily so the stdlib path never pays for it, and the
browser is launched once per run (``LazyBrowser``) only when a render is actually
needed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.sync_api import Browser, Playwright, Route

# Allow `import web_http` whether run as a script or imported (mirrors web_fetch).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from web_http import USER_AGENT, Resp, request_url

# Extracted-text length below which a page is judged thin (an SPA skeleton that
# extracted to near-nothing) and the headless-render fallback kicks in.
THIN_TEXT_CHARS = 200
# After networkidle, a short settle for late hydration before serializing the DOM.
RENDER_SETTLE_MS = 800
# Sub-resource types a render doesn't need: blocking them is politer + faster.
_HEAVY_RESOURCE_TYPES = {"image", "media", "font"}


def is_thin(text: str | None, thin_chars: int = THIN_TEXT_CHARS) -> bool:
    """True when extracted text is missing or below the thin threshold.

    The symptom of a client-rendered (JS-only) page: the plain GET returns a
    skeleton that trafilatura extracts to near-nothing. This is the trigger for
    the headless-render fallback, and — when no render rescues it — the signal
    that turns a silent ``[200] (new)`` into a loud warning.
    """
    return text is None or len(text.strip()) < thin_chars


class BrowserUnavailableError(RuntimeError):
    """Rendering was requested but Playwright/Chromium isn't available to launch.

    A setup failure, distinct from a per-page render error: it won't fix itself
    mid-run, so ``main`` stops the batch with the actionable message rather than
    retrying it for every URL. Subclassing keeps ``main``'s handler from catching
    unrelated ``RuntimeError``s raised elsewhere in ``fetch_one``.
    """


class LazyBrowser:
    """A headless Chromium launched on first use and reused for the rest of a run.

    Lazy so the common all-stdlib batch never pays browser startup, and so the
    "install Chromium" error surfaces only when a render is actually needed.
    ``main()`` owns one instance and ``close()``s it; ``fetch_one`` asks for the
    live browser via ``get()`` only when escalating to a render.
    """

    def __init__(self) -> None:
        self._browser: Browser | None = None
        self._playwright: Playwright | None = None

    def get(self) -> Browser:
        """The launched browser, starting it on first call.

        Raises ``BrowserUnavailableError`` if Playwright or its Chromium binary is
        missing — a setup failure that should stop the run, not be swallowed
        per-page.
        """
        if self._browser is None:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError as exc:
                raise BrowserUnavailableError(
                    "playwright is not installed — run `uv sync`"
                ) from exc
            try:
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch()
            except Exception as exc:
                self.close()  # tear down a partially-started Playwright
                raise BrowserUnavailableError(
                    "headless rendering needs Chromium — run "
                    "`uv run playwright install chromium` (or pass --no-render)"
                ) from exc
        return self._browser

    def close(self) -> None:
        """Tear down the browser + Playwright if they were started (else a no-op)."""
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None


def _block_heavy_resources(route: Route) -> None:
    """Abort image/media/font sub-requests during a render; allow the rest.

    A render needs the document and the scripts that build it, not its imagery —
    blocking the heavy types loads the source less and renders faster.
    """
    if route.request.resource_type in _HEAVY_RESOURCE_TYPES:
        route.abort()
    else:
        route.continue_()


def _render_get(url: str, browser: Browser) -> Resp:
    """Render ``url`` in headless Chromium and return a ``Resp`` of the live DOM.

    Mirrors ``http_get``'s shape so ``fetch_one`` can treat a render like any
    other fetch. ``page.content()`` is the serialized post-JS DOM (already a
    ``str``), so the raw-bytes charset machinery is bypassed; we re-encode it as
    utf-8 for the stored blob and its ``content_sha``. May raise ``playwright``
    errors (timeout, navigation); ``render`` turns those into a None fall-back.
    """
    wire_url = request_url(url)
    page = browser.new_page(user_agent=USER_AGENT)
    try:
        page.route("**/*", _block_heavy_resources)
        response = page.goto(wire_url, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(RENDER_SETTLE_MS)
        html = page.content()
        status = response.status if response is not None else 200
        landed = page.url
    finally:
        page.close()
    # Echo the readable input url when we didn't redirect (mirrors http_get), so
    # a non-ASCII page isn't re-keyed to its percent-encoded request form.
    final_url = url if landed == wire_url else landed
    raw = html.encode("utf-8")
    return Resp(status, "text/html", final_url, raw, html, None)


def render(url: str, browser: LazyBrowser) -> Resp | None:
    """Attempt a headless render; return its ``Resp``, or None on render failure.

    A setup ``BrowserUnavailableError`` from ``browser.get()`` (no Chromium) is
    let through to stop the run with an actionable message. A per-page render error
    (timeout, navigation) is caught and logged, returning None so the caller keeps
    the plain (thin) result instead of crashing the batch.
    """
    live = browser.get()
    from playwright.sync_api import Error as PlaywrightError

    try:
        return _render_get(url, live)
    except PlaywrightError as exc:
        print(
            f"WARNING: render failed, keeping plain result: {url} ({exc})",
            file=sys.stderr,
        )
        return None
