#!/usr/bin/env python3
"""Download ingest source files from Cloudflare R2.

Uses only stdlib (urllib.request, hashlib, json).
Fetches both the root manifest (ingest sources like IPDB, OPDB, Fandom)
and the pindata/ manifest (catalog exports from pindata), then downloads
files whose size or SHA-256 don't match.

Usage:
    python scripts/cloud_store/pull_ingest_sources.py [--url URL] [--dest DIR]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from http.client import HTTPResponse

DEFAULT_URL = "https://pub-8a5220445534421c879b6ff9ede350f1.r2.dev"

_OPENER = urllib.request.build_opener()
_OPENER.addheaders = [("User-Agent", "pinexplore/1.0")]


def _urlopen(url: str) -> HTTPResponse:
    resp: HTTPResponse = _OPENER.open(url)
    return resp


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _pull_manifest(base_url: str, manifest_path: str, dest: str) -> tuple[int, int]:
    """Fetch a manifest and download any changed files.

    Returns (downloaded, skipped) counts.
    """
    manifest_url = f"{base_url}/{manifest_path}"
    print(f"Fetching manifest from {manifest_url}")
    with _urlopen(manifest_url) as resp:
        manifest = json.loads(resp.read())

    # Determine the URL prefix and local prefix from the manifest path.
    # e.g. "pindata/manifest.json" -> url prefix "pindata/", local prefix "pindata/"
    prefix = manifest_path.rsplit("manifest.json", 1)[0]

    downloaded = 0
    skipped = 0

    for entry in manifest:
        rel_path = entry["path"]
        expected_size = entry["size"]
        expected_sha = entry["sha256"]
        local_path = Path(dest) / (prefix + rel_path)

        if (
            local_path.exists()
            and local_path.stat().st_size == expected_size
            and _sha256(local_path) == expected_sha
        ):
            skipped += 1
            continue

        local_path.parent.mkdir(parents=True, exist_ok=True)
        file_url = f"{base_url}/{prefix}{rel_path}"
        print(f"  {prefix}{rel_path}")
        with _urlopen(file_url) as resp, local_path.open("wb") as f:
            f.write(resp.read())

        actual_sha = _sha256(local_path)
        if actual_sha != expected_sha:
            print(
                f"ERROR: Checksum mismatch for {prefix}{rel_path}: "
                f"expected {expected_sha}, got {actual_sha}",
                file=sys.stderr,
            )
            sys.exit(1)
        downloaded += 1

    return downloaded, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Download ingest sources from R2.")
    parser.add_argument(
        "--url",
        default=os.environ.get("R2_PUBLIC_URL", DEFAULT_URL),
        help="Base URL of the R2 public bucket (default: R2_PUBLIC_URL env var).",
    )
    parser.add_argument(
        "--dest",
        default="ingest_sources",
        help="Local directory to download into (default: ingest_sources).",
    )
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    total_downloaded = 0
    total_skipped = 0

    # Pull root manifest (ingest sources: IPDB, OPDB, Fandom, etc.)
    d, s = _pull_manifest(base_url, "manifest.json", args.dest)
    total_downloaded += d
    total_skipped += s

    # Pull pindata manifest (catalog exports from pindata)
    d, s = _pull_manifest(base_url, "pindata/manifest.json", args.dest)
    total_downloaded += d
    total_skipped += s

    print(f"Done. {total_downloaded} downloaded, {total_skipped} up-to-date.")


if __name__ == "__main__":
    main()
