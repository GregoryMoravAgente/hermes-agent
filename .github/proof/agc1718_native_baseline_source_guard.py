#!/usr/bin/env python3
"""AGC-1718 official-base native Windows baseline source guard.

The diagnostic commit is exactly one transport commit above the immutable
official Core base. This guard hashes the actual checked-out bytes, verifies
that topology, and records before/after receipts. It does not compile a whole
source closure: the complete selected test file is the executable check.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHA1 = re.compile(r"^[0-9a-f]{40}$")
_OFFICIAL_BASE = "22063fc099ec9ad8357d7f8f6e6248a0400680d3"
_MANIFEST_REL = ".github/proof/agc1718_native_baseline_manifest.json"
_GUARD_REL = ".github/proof/agc1718_native_baseline_source_guard.py"
_WORKFLOW_REL = ".github/workflows/windows-venv-e2e.yml"
_TRANSPORT_PATHS = {_WORKFLOW_REL, _GUARD_REL, _MANIFEST_REL}
_EXPECTED_TESTS = ["tests/tui_gateway/test_tui_gateway_server.py"]


def _fail(message: str) -> NoReturn:
    print(f"agc1718 baseline source guard: {message}", file=sys.stderr)
    raise SystemExit(1)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=root, check=False, capture_output=True, text=True,
    )
    if result.returncode:
        _fail(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _git_bytes(root: Path, revision: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode:
        _fail(f"git show failed for {revision}:{path}")
    return result.stdout


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _safe_path(root: Path, raw: object, category: str) -> Path:
    if not isinstance(raw, str) or not raw or Path(raw).is_absolute():
        _fail(f"{category} contains a non-relative path")
    relative = Path(raw)
    if ".." in relative.parts:
        _fail(f"{category} escapes the checkout: {raw!r}")
    path = (root / relative).resolve()
    if root != path and root not in path.parents:
        _fail(f"{category} resolves outside the checkout: {raw!r}")
    if not path.is_file():
        _fail(f"{category} is not a file in the checkout: {raw!r}")
    return path


def _tracked_files(root: Path) -> set[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=False, capture_output=True,
    )
    if result.returncode:
        _fail(f"git ls-files failed: {result.stderr.decode('utf-8', 'replace').strip()}")
    try:
        return {
            item.decode("utf-8")
            for item in result.stdout.split(b"\0")
            if item
        }
    except UnicodeDecodeError:
        _fail("git ls-files returned a non-UTF-8 path")


def _verify_entry(
    root: Path,
    entry: object,
    category: str,
    transport: set[str],
    tracked_files: set[str],
) -> tuple[str, str]:
    if not isinstance(entry, dict):
        _fail(f"{category} entry is not an object")
    raw_path = entry.get("path")
    expected = entry.get("sha256")
    if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
        _fail(f"{category} has an unresolved or malformed sha256 for {raw_path!r}")
    path = _safe_path(root, raw_path, category)
    relative = path.relative_to(root).as_posix()
    if relative in transport:
        _fail(f"{category} overlaps a transport path: {relative}")
    if relative not in tracked_files:
        _fail(f"{category} is not tracked at {relative}")
    actual = _sha256(path)
    if actual != expected:
        _fail(f"{category} bytes differ at {relative}: {actual} != {expected}")
    return relative, actual


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--proof-sha", required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    root = Path.cwd().resolve()
    manifest_path = args.manifest.resolve()
    try:
        manifest_relative = manifest_path.relative_to(root).as_posix()
    except ValueError:
        _fail("manifest is outside the checkout")
    if manifest_relative != _MANIFEST_REL:
        _fail(f"manifest must be {_MANIFEST_REL}")
    if not _SHA1.fullmatch(args.proof_sha):
        _fail("proof SHA is malformed")
    actual_head = _git(root, "rev-parse", "HEAD")
    if actual_head != args.proof_sha:
        _fail(f"HEAD {actual_head} does not equal proof SHA {args.proof_sha}")
    parent = _git(root, "rev-parse", "HEAD^")
    if parent != _OFFICIAL_BASE:
        _fail(f"HEAD^ {parent} is not the official Core base {_OFFICIAL_BASE}")
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        _fail(f"checkout is dirty: {status[:500]}")
    changed = _git(root, "diff", "--name-only", "--no-renames", "HEAD^", "HEAD").splitlines()
    if set(changed) != _TRANSPORT_PATHS or len(changed) != len(_TRANSPORT_PATHS):
        _fail(f"diagnostic commit must change exactly {_TRANSPORT_PATHS}; got {changed}")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"cannot read manifest: {type(exc).__name__}")
    if manifest.get("candidate_commit") != parent:
        _fail(f"manifest candidate_commit does not equal HEAD^ {parent}")
    if manifest.get("transport_paths") != sorted(_TRANSPORT_PATHS):
        _fail("manifest transport_paths are not the exact three diagnostic paths")
    transport_hashes = manifest.get("transport_hashes")
    if not isinstance(transport_hashes, dict) or set(transport_hashes) != {"workflow", "guard"}:
        _fail("manifest must hash workflow and guard only; manifest is not self-hashed")
    for key, relative in (("workflow", _WORKFLOW_REL), ("guard", _GUARD_REL)):
        expected = transport_hashes.get(key)
        if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
            _fail(f"manifest transport hash {key} is unresolved or malformed")
        if _sha256(root / relative) != expected:
            _fail(f"{key} transport bytes do not match manifest")
    if _git_bytes(root, "HEAD", _MANIFEST_REL) != manifest_path.read_bytes():
        _fail("manifest bytes differ from the manifest stored in the diagnostic commit")

    source_entries = manifest.get("source_files")
    test_entries = manifest.get("test_files")
    fixture_entries = manifest.get("fixture_files")
    if not isinstance(source_entries, list) or not source_entries:
        _fail("source_files must contain the materialized official source closure")
    if not isinstance(test_entries, list) or [
        e.get("path") for e in test_entries if isinstance(e, dict)
    ] != _EXPECTED_TESTS:
        _fail("test_files does not equal the one-file baseline bank")
    if not isinstance(fixture_entries, list):
        _fail("fixture_files must be a list, possibly empty for this baseline")
    tracked_files = _tracked_files(root)
    checked: dict[str, str] = {}
    for category, entries in (
        ("source_files", source_entries),
        ("test_files", test_entries),
        ("fixture_files", fixture_entries),
    ):
        for entry in entries:
            relative, actual = _verify_entry(
                root, entry, category, _TRANSPORT_PATHS, tracked_files,
            )
            if relative in checked:
                _fail(f"duplicate baseline path across source/test/fixture groups: {relative}")
            checked[relative] = actual

    receipt = {
        "schema": "agc1718.native_windows_baseline_receipt.r1.v1",
        "purpose": "official-core-base-failure-signature-diagnostic",
        "phase": args.phase,
        "proof_sha": actual_head,
        "candidate_commit": parent,
        "workspace": str(root),
        "transport_paths": sorted(_TRANSPORT_PATHS),
        "selected_test_files": _EXPECTED_TESTS,
        "files": checked,
        "clean_checkout": True,
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in receipt.items() if key != "files"} | {"verified_inputs": len(checked)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
