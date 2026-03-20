#!/usr/bin/env python3
"""Push ingest source files to Cloudflare R2.

Builds a manifest with SHA-256 checksums, then uploads changed files
using boto3 (S3-compatible API). Manifest is uploaded last so consumers
never see references to objects that haven't been uploaded yet.

Usage:
    python scripts/push_ingest_sources.py

Requires R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
in environment or .env.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO_ROOT / "ingest_sources"
EXCLUDE = {
    "manifest.json",
    ".DS_Store",
}


def _load_dotenv():
    """Load .env file into os.environ (key=value lines only)."""
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _collect_files(src: Path) -> list[dict]:
    """Walk src and return manifest entries, excluding dotfiles and stale files."""
    entries = []
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for f in files:
            if f.startswith(".") or f in EXCLUDE:
                continue
            full = Path(root) / f
            rel = full.relative_to(src).as_posix()
            entries.append(
                {
                    "path": rel,
                    "size": full.stat().st_size,
                    "sha256": _sha256(full),
                }
            )
    entries.sort(key=lambda e: e["path"])
    return entries


def main() -> int:
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 is required. uv add boto3", file=sys.stderr)
        return 1

    _load_dotenv()

    # Validate env vars
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    bucket = os.environ.get("R2_BUCKET")

    missing = [
        name
        for name, val in [
            ("R2_ACCOUNT_ID", account_id),
            ("R2_ACCESS_KEY_ID", access_key),
            ("R2_SECRET_ACCESS_KEY", secret_key),
            ("R2_BUCKET", bucket),
        ]
        if not val
    ]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}", file=sys.stderr)
        return 1

    if not SOURCE_DIR.exists():
        print(f"ERROR: {SOURCE_DIR} not found.", file=sys.stderr)
        return 1

    # Build manifest
    print("Building manifest...")
    entries = _collect_files(SOURCE_DIR)
    manifest_path = SOURCE_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    print(f"  {len(entries)} files in manifest")

    # Upload to R2
    print("Uploading to R2...")
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # Upload files first, manifest last
    uploaded = 0
    skipped = 0
    for entry in entries:
        local_path = SOURCE_DIR / entry["path"]
        key = entry["path"]

        # Skip if remote file matches size AND content hash
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            remote_size = head["ContentLength"]
            remote_etag = head["ETag"].strip('"')
            local_md5 = hashlib.md5(local_path.read_bytes()).hexdigest()
            if remote_size == entry["size"] and remote_etag == local_md5:
                skipped += 1
                continue
        except s3.exceptions.ClientError:
            pass  # File doesn't exist remotely yet

        print(f"  {key}")
        s3.upload_file(str(local_path), bucket, key)
        uploaded += 1

    # Upload manifest last
    s3.upload_file(str(manifest_path), bucket, "manifest.json")

    print(f"Done. {uploaded} uploaded, {skipped} unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
