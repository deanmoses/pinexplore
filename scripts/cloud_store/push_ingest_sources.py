#!/usr/bin/env python3
"""Push ingest source files to Cloudflare R2.

Uploads raw ingest source files (IPDB, OPDB, Fandom, etc.) and builds a
root-level manifest.json covering only these files.  The pindata/
prefix is owned by pindata's push script and excluded here.

Usage:
    python scripts/cloud_store/push_ingest_sources.py

Requires R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
in environment or .env.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, TypedDict

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


class _Entry(TypedDict):
    path: str
    size: int
    sha256: str


class _Planned(NamedTuple):
    """A file that needs uploading, and the reason it does."""

    entry: _Entry
    reason: str


R2_ENV_VARS = (
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET",
)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_DIR = REPO_ROOT / "ingest_sources"
MANIFEST_KEY = "manifest.json"
EXCLUDE = {
    MANIFEST_KEY,
    ".DS_Store",
}

# How many planned uploads to list before collapsing the rest into a count.
PLAN_PREVIEW = 10
# Column width for paths in the plan and upload listings.
PATH_WIDTH = 44


def _load_dotenv() -> None:
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


def _require_env() -> dict[str, str] | None:
    """Return the R2 credentials, or None having named every missing one.

    Reports all missing names at once — discovering them one failed run at a
    time is worse — and returns a fully narrowed dict so callers need no
    None-checks of their own.
    """
    found = {name: os.environ.get(name) for name in R2_ENV_VARS}
    missing = [name for name, value in found.items() if not value]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}", file=sys.stderr)
        return None
    return {name: value for name, value in found.items() if value}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _collect_files(src: Path) -> list[_Entry]:
    """Walk src and return manifest entries, excluding dotfiles and stale files.

    Skips the pindata/ subtree — that prefix is owned by pindata.
    """
    entries: list[_Entry] = []
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "pindata"]
        for f in files:
            # Skip dotfiles, the manifest, and transient SQLite sidecars
            # (-wal/-shm/-journal): machine-local state that must never be
            # transported. The web cache uses DELETE journal mode so these
            # normally don't exist, but never upload them if they do.
            if (
                f.startswith(".")
                or f in EXCLUDE
                or f.endswith(("-wal", "-shm", "-journal"))
            ):
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


def _format_decimal_bytes(n: int) -> str:
    """Match how R2 and its dashboard report sizes, so numbers can be compared."""
    if n < 1000:
        return f"{n} B"
    size = float(n)
    for unit in ("KB", "MB", "GB"):
        size /= 1000
        if size < 1000:
            return f"{size:.1f} {unit}"
    return f"{size / 1000:.1f} TB"


def _elide(text: str, width: int) -> str:
    """Middle-truncate text to width, keeping both ends recognizable."""
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    keep = width - 1
    head = (keep + 1) // 2
    tail = keep - head
    # text[-0:] is the whole string, not the empty one — spell the empty case out.
    return text[:head] + "…" + (text[-tail:] if tail else "")


def _upload_reason(
    entry: _Entry, remote_size: int | None, remote_sha: str | None
) -> str | None:
    """Return why entry needs uploading, or None if the bucket already has it.

    Size comes from the bucket listing (what is actually stored), hash from the
    committed manifest (what the last completed push recorded). Existence is
    judged by the listing, so an object deleted out-of-band is restored even
    while the manifest still claims it is there.
    """
    if remote_size is None:
        return "new"
    if remote_size != entry["size"]:
        return f"size differs (remote {_format_decimal_bytes(remote_size)})"
    if remote_sha is None:
        return "not in manifest"
    if remote_sha != entry["sha256"]:
        return "content differs"
    return None


def _list_remote(s3: S3Client, bucket: str) -> dict[str, int]:
    """Return key -> size for every object in the bucket (one request per 1000)."""
    remote: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            remote[obj["Key"]] = obj["Size"]
    return remote


def _fetch_remote_manifest(s3: S3Client, bucket: str) -> dict[str, str] | None:
    """Return path -> sha256 from the committed manifest, or None if unusable.

    None means "no trustworthy record" — absent (a first push) or malformed. The
    caller then re-uploads every file, because a hash is only worth trusting if
    the push that recorded it also wrote the bytes. Skipping same-sized objects
    instead would publish locally assumed hashes for objects nobody verified,
    and `make pull` exits 1 on the first that disagrees.

    Permission and transport errors propagate rather than degrade: a 403 read as
    "no manifest" is indistinguishable from an empty bucket.
    """
    try:
        body = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)["Body"].read()
    except s3.exceptions.NoSuchKey:
        # A normal comparison prints nothing, so degraded ones must speak up.
        print("  No remote manifest yet — every file will be uploaded.")
        return None
    try:
        parsed = json.loads(body)
    except ValueError:
        parsed = None
    # A JSON object parses fine and iterates as its keys, so an unchecked dict
    # would read as a valid manifest describing nothing, and every local file
    # would look absent from it.
    if isinstance(parsed, list):
        try:
            return {entry["path"]: entry["sha256"] for entry in parsed}
        except TypeError, KeyError:
            pass
    print("  WARNING: remote manifest is unreadable — re-uploading every file.")
    return None


def _compare(
    s3: S3Client, bucket: str, entries: list[_Entry]
) -> tuple[list[_Planned], list[str]]:
    """Decide the whole plan from two requests, and report abandoned objects."""
    print("Comparing with R2...")
    remote_sizes = _list_remote(s3, bucket)
    remote_hashes = _fetch_remote_manifest(s3, bucket)
    known = remote_hashes if remote_hashes is not None else {}

    planned: list[_Planned] = []
    for entry in entries:
        reason = _upload_reason(
            entry, remote_sizes.get(entry["path"]), known.get(entry["path"])
        )
        if reason is not None:
            planned.append(_Planned(entry, reason))

    # Published by an earlier push, no longer present locally. Push never
    # deletes, so these accumulate until someone removes them by hand.
    local_paths = {entry["path"] for entry in entries}
    orphans = sorted((set(known) - local_paths) & set(remote_sizes))

    return planned, orphans


def _print_plan(planned: list[_Planned], unchanged: int) -> None:
    if not planned:
        print(f"\nNothing to upload, {unchanged} unchanged")
        return

    total = sum(p.entry["size"] for p in planned)
    print(
        f"\nPlan: {len(planned)} to upload ({_format_decimal_bytes(total)}), {unchanged} unchanged"
    )
    for item in planned[:PLAN_PREVIEW]:
        path = _elide(item.entry["path"], PATH_WIDTH)
        size = _format_decimal_bytes(item.entry["size"])
        print(f"  {path:<{PATH_WIDTH}} {size:>9}   {item.reason}")
    if len(planned) > PLAN_PREVIEW:
        print(f"  … and {len(planned) - PLAN_PREVIEW} more")


def _report_aborted_before_upload(header: str) -> None:
    """Report a stop during scan or compare, when nothing has been written yet."""
    print(f"\n{header} while comparing.")
    print("  Nothing was uploaded; the manifest is unchanged.")


def _report_incomplete(header: str, uploaded: int, total: int) -> None:
    """Explain that the run stopped before the manifest commit.

    Uploads overwrite live keys before the manifest lands, so a partial push
    leaves the previous snapshot stale rather than merely incomplete: objects it
    never listed are skipped by `make pull`, and ones it lists under a
    superseded hash fail pull's post-download checksum outright.
    """
    print()
    if uploaded >= total and total > 0:
        print(f"{header} after all {total} uploads, before the manifest commit.")
    else:
        print(f"{header} after {uploaded} of {total} uploads.")
    print("  Manifest NOT updated — R2 still advertises the previous snapshot.")
    if uploaded:
        print(f"  {uploaded} uploaded objects are not described by it, so `make pull`")
        print("  will skip new files and fail its checksum check on changed ones.")
    print("  Re-run `make push` to finish.")


def main() -> int:
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 is required. uv add boto3", file=sys.stderr)
        return 1

    _load_dotenv()

    env = _require_env()
    if env is None:
        return 1
    bucket = env["R2_BUCKET"]

    if not SOURCE_DIR.exists():
        print(f"ERROR: {SOURCE_DIR} not found.", file=sys.stderr)
        return 1

    tty = sys.stdout.isatty()

    print("Scanning local files...")
    entries = _collect_files(SOURCE_DIR)
    total_bytes = sum(e["size"] for e in entries)
    print(f"  {len(entries)} files, {_format_decimal_bytes(total_bytes)}")

    endpoint = f"https://{env['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    )

    # Decide the whole plan before uploading anything: the work is then visible
    # up front, and a stop here costs nothing because nothing has been written.
    try:
        planned, orphans = _compare(s3, bucket, entries)
    except KeyboardInterrupt:
        _report_aborted_before_upload("INTERRUPTED")
        return 130
    except Exception as exc:
        sys.stdout.flush()
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.stderr.flush()
        _report_aborted_before_upload("FAILED")
        return 1
    unchanged = len(entries) - len(planned)
    _print_plan(planned, unchanged)
    if orphans:
        count = (
            "1 object remains"
            if len(orphans) == 1
            else f"{len(orphans)} objects remain"
        )
        print(
            f"\nNote: {count} in R2 with no local file (e.g. {orphans[0]}).\n"
            f"  Push never deletes; remove them by hand if unwanted."
        )

    manifest_path = SOURCE_DIR / "manifest.json"
    uploaded = 0
    uploaded_bytes = 0
    uploading_manifest = False

    try:
        if planned:
            print("\nUploading...")
        width = len(str(len(planned)))
        for i, item in enumerate(planned, 1):
            key = item.entry["path"]
            upload_started = time.monotonic()
            # Hash the bytes actually sent, not the ones the scan saw, so the
            # manifest can never advertise a hash for content it did not upload —
            # `make pull` exits 1 when a download's sha256 misses the manifest.
            # Updating in place works because this is the dict the manifest is
            # serialized from.
            data = (SOURCE_DIR / key).read_bytes()
            item.entry["size"] = len(data)
            item.entry["sha256"] = hashlib.sha256(data).hexdigest()
            s3.upload_fileobj(io.BytesIO(data), bucket, key)
            uploaded += 1
            uploaded_bytes += len(data)
            line = (
                f"  [{i:>{width}}/{len(planned)}] {_elide(key, PATH_WIDTH):<{PATH_WIDTH}} "
                f"{_format_decimal_bytes(item.entry['size']):>9}"
            )
            if tty:
                line += f"   {time.monotonic() - upload_started:.1f}s"
            print(line)

        # The manifest is the commit point — written locally only once the
        # objects it describes are all in the bucket, then uploaded last.
        manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
        print("\nUploading manifest... ", end="", flush=True)
        uploading_manifest = True
        s3.upload_file(str(manifest_path), bucket, MANIFEST_KEY)
        print("ok")
        uploading_manifest = False
    except KeyboardInterrupt:
        if uploading_manifest:
            print()  # terminate the partially written "Uploading manifest" line
        _report_incomplete("INTERRUPTED", uploaded, len(planned))
        return 130
    except Exception as exc:
        if uploading_manifest:
            print()
        sys.stdout.flush()  # keep the report ordered after the error when piped
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.stderr.flush()
        _report_incomplete("FAILED", uploaded, len(planned))
        return 1

    volume = f" ({_format_decimal_bytes(uploaded_bytes)})" if uploaded else ""
    print(f"Done. {uploaded} uploaded{volume}, {unchanged} unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
