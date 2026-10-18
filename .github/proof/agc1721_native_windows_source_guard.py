#!/usr/bin/env python3
"""AGC-1721 own-fork native Windows source-only transport guard.

This guard is copied into a single diagnostic transport commit whose parent is
an already-qualified candidate. It batches one ``git ls-files -z`` call, hashes
the materialized source/test/fixture byte closure, and requires the after
closure to equal the before closure. The manifest is deliberately excluded
from its own transport hash; all angle-bracket values must be replaced once by
the receiving owner before this file is committed.
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
_MANIFEST_REL = '.github/proof/agc1721_native_windows_manifest.json'
_GUARD_REL = '.github/proof/agc1721_native_windows_source_guard.py'
_WORKFLOW_REL = '.github/workflows/windows-venv-e2e.yml'
_TRANSPORT_PATHS = {'.github/proof/agc1721_native_windows_manifest.json', '.github/proof/agc1721_native_windows_source_guard.py', '.github/workflows/windows-venv-e2e.yml'}
_EXPECTED_GROUPS = {
    "exporter_profile": [
    "tests/cron/test_complete_export.py",
    "tests/cron/test_complete_export_fences.py",
    "tests/cron/test_complete_export_sources.py",
    "tests/cron/test_complete_export_sqlite_refusals.py",
    "tests/cron/test_export_writer_custody.py",
    "tests/cron/test_file_writer_jobs_custody.py",
    "tests/cron/test_file_writer_monitor_custody.py",
    "tests/cron/test_file_writer_profile_custody.py",
    "tests/hermes_state/test_complete_export.py",
    "tests/hermes_state/test_complete_export_schema30_optional.py",
    "tests/hermes_state/test_preservation_receipt_budget_order.py",
    "tests/hermes_state/test_preservation_runtime_relation_shadow.py",
    "tests/hermes_state/test_preservation_runtime_snapshot.py",
    "tests/test_hermes_cron_custody_windows.py",
    "tests/test_hermes_cron_export_custody.py",
    "tests/tui_gateway/test_compute_host_borrowed_lease.py",
    "tests/tui_gateway/test_compute_host_late_compress_ack.py",
    "tests/tui_gateway/test_compute_host_profile_fencing.py",
    "tests/tui_gateway/test_compute_host_profile_ownership.py",
    "tests/tui_gateway/test_profiles_list_canonical_session.py",
    "tests/tui_gateway/test_tui_gateway_server.py"
],
    "windows_receiving_tests": [
    "tests/hermes_cli/test_ipv6_url_authorities.py",
    "tests/tools/test_voice_stop_phrase.py",
    "tests/tui_gateway/test_composite_carrier_rewind.py",
    "tests/tui_gateway/test_desktop_session_row_git_meta.py",
    "tests/tui_gateway/test_free_tier_task_done.py",
    "tests/tui_gateway/test_immediate_thread_shim.py",
    "tests/tui_gateway/test_lazy_session_regressions.py",
    "tests/tui_gateway/test_out_of_band_history_adoption.py",
    "tests/tui_gateway/test_profile_resume_cwd.py",
    "tests/tui_gateway/test_routing_provenance_reopen.py",
    "tests/tui_gateway/test_session_git_metadata_generation.py",
    "tests/tui_gateway/test_ssh_session_cwd_persistence.py",
    "tests/tui_gateway/test_tui_gateway_server.py",
    "tests/tui_gateway/test_tui_gateway_server_deep_truncate.py",
    "tests/tui_gateway/test_remote_workdir_path_semantics.py"
],
    "native_original": [
    "tests/tui_gateway/test_compute_host.py",
    "tests/tui_gateway/test_compute_host_turn_protocol.py",
    "tests/tui_gateway/test_interrupt_dead_compute_host.py",
    "tests/tui_gateway/test_served_profile_child_env.py",
    "tests/tui_gateway/test_served_profile_child_env_authority.py"
],
}
_EXPECTED_TESTS = [
  "tests/cron/test_complete_export.py",
  "tests/cron/test_complete_export_fences.py",
  "tests/cron/test_complete_export_sources.py",
  "tests/cron/test_complete_export_sqlite_refusals.py",
  "tests/cron/test_export_writer_custody.py",
  "tests/cron/test_file_writer_jobs_custody.py",
  "tests/cron/test_file_writer_monitor_custody.py",
  "tests/cron/test_file_writer_profile_custody.py",
  "tests/hermes_state/test_complete_export.py",
  "tests/hermes_state/test_complete_export_schema30_optional.py",
  "tests/hermes_state/test_preservation_receipt_budget_order.py",
  "tests/hermes_state/test_preservation_runtime_relation_shadow.py",
  "tests/hermes_state/test_preservation_runtime_snapshot.py",
  "tests/test_hermes_cron_custody_windows.py",
  "tests/test_hermes_cron_export_custody.py",
  "tests/tui_gateway/test_compute_host_borrowed_lease.py",
  "tests/tui_gateway/test_compute_host_late_compress_ack.py",
  "tests/tui_gateway/test_compute_host_profile_fencing.py",
  "tests/tui_gateway/test_compute_host_profile_ownership.py",
  "tests/tui_gateway/test_profiles_list_canonical_session.py",
  "tests/tui_gateway/test_tui_gateway_server.py",
  "tests/hermes_cli/test_ipv6_url_authorities.py",
  "tests/tools/test_voice_stop_phrase.py",
  "tests/tui_gateway/test_composite_carrier_rewind.py",
  "tests/tui_gateway/test_desktop_session_row_git_meta.py",
  "tests/tui_gateway/test_free_tier_task_done.py",
  "tests/tui_gateway/test_immediate_thread_shim.py",
  "tests/tui_gateway/test_lazy_session_regressions.py",
  "tests/tui_gateway/test_out_of_band_history_adoption.py",
  "tests/tui_gateway/test_profile_resume_cwd.py",
  "tests/tui_gateway/test_routing_provenance_reopen.py",
  "tests/tui_gateway/test_session_git_metadata_generation.py",
  "tests/tui_gateway/test_ssh_session_cwd_persistence.py",
  "tests/tui_gateway/test_tui_gateway_server_deep_truncate.py",
  "tests/tui_gateway/test_remote_workdir_path_semantics.py",
  "tests/tui_gateway/test_compute_host.py",
  "tests/tui_gateway/test_compute_host_turn_protocol.py",
  "tests/tui_gateway/test_interrupt_dead_compute_host.py",
  "tests/tui_gateway/test_served_profile_child_env.py",
  "tests/tui_gateway/test_served_profile_child_env_authority.py"
]


def _fail(message: str) -> NoReturn:
    print(f"agc1721 native transport guard: {message}", file=sys.stderr)
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
        cwd=root, check=False, capture_output=True,
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
    """Load the tracked-path set once; never spawn one Git process per entry."""
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=False, capture_output=True,
    )
    if result.returncode:
        _fail(f"git ls-files failed: {result.stderr.decode('utf-8', 'replace').strip()}")
    try:
        return {item.decode("utf-8") for item in result.stdout.split(b"\0") if item}
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


def _closure_digest(files: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(files):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(files[relative]))
        digest.update(b"\0")
    return digest.hexdigest()


def _read_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"cannot read {label}: {type(exc).__name__}")
    if not isinstance(value, dict):
        _fail(f"{label} is not a JSON object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--proof-sha", required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--baseline-receipt", type=Path)
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
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        _fail(f"checkout is dirty: {status[:500]}")
    changed = _git(root, "diff", "--name-only", "--no-renames", "HEAD^", "HEAD").splitlines()
    if set(changed) != _TRANSPORT_PATHS or len(changed) != len(_TRANSPORT_PATHS):
        _fail(f"proof commit must change exactly {sorted(_TRANSPORT_PATHS)}; got {changed}")

    manifest = _read_json(manifest_path, "manifest")
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
        _fail("manifest bytes differ from the manifest stored in the proof commit")

    bank = manifest.get("bank")
    if not isinstance(bank, dict):
        _fail("bank metadata is missing")
    for name, paths in _EXPECTED_GROUPS.items():
        if bank.get(name) != paths:
            _fail(f"bank group {name} differs from the reviewed bank")
    if bank.get("excluded_shared_fixture") != "tests/_fixtures/thread_shim.py":
        _fail("bank exclusion for the shared fixture helper is missing")
    if bank.get("excluded_production") != "tui_gateway/session_workdir.py":
        _fail("bank exclusion for the production path is missing")

    source_entries = manifest.get("source_files")
    test_entries = manifest.get("test_files")
    fixture_entries = manifest.get("fixture_files")
    if not isinstance(source_entries, list) or not source_entries:
        _fail("source_files must contain the materialized source byte closure")
    if not isinstance(test_entries, list) or [
        e.get("path") for e in test_entries if isinstance(e, dict)
    ] != _EXPECTED_TESTS:
        _fail("test_files does not equal the explicit reviewed union")
    if not isinstance(fixture_entries, list):
        _fail("fixture_files must be a list, possibly empty")
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
                _fail(f"duplicate byte-closure path across groups: {relative}")
            checked[relative] = actual
    if "tests/_fixtures/thread_shim.py" in [
        path for path in checked if path in _EXPECTED_TESTS
    ]:
        _fail("shared fixture helper must not be in the native test bank")

    closure_digest = _closure_digest(checked)
    if args.phase == "after":
        if args.baseline_receipt is None:
            _fail("after phase requires --baseline-receipt")
        baseline = _read_json(args.baseline_receipt.resolve(), "before receipt")
        if baseline.get("files") != checked:
            _fail("selected byte closure changed between before and after")
        if baseline.get("byte_closure_sha256") != closure_digest:
            _fail("byte closure digest changed between before and after")

    receipt = {
        "schema": "agc1721.native_windows_source_only_receipt.r1.v1",
        "purpose": "own-fork-source-only-native-windows-proof-transport",
        "phase": args.phase,
        "proof_sha": actual_head,
        "candidate_commit": parent,
        "workspace": str(root),
        "transport_paths": sorted(_TRANSPORT_PATHS),
        "selected_test_files": _EXPECTED_TESTS,
        "files": checked,
        "byte_closure_sha256": closure_digest,
        "clean_checkout": True,
        "zero_file_retries": True,
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    summary = {
        "phase": args.phase,
        "proof_sha": actual_head,
        "candidate_commit": parent,
        "source_files": len(source_entries),
        "test_files": len(test_entries),
        "fixture_files": len(fixture_entries),
        "byte_closure_files": len(checked),
        "byte_closure_sha256": closure_digest,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
