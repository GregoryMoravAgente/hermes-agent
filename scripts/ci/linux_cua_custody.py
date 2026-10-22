#!/usr/bin/env python3
"""Owned-input, capacity, and receipt guard for the hosted Linux CUA lane.

The workflow is the only caller.  This file has no product fallback: it resolves
the PM-pinned x64 binaries, calls the existing Bot Desktop owner, records the
same host/cgroup capacity contract as ``tools.bot_desktop.resources``, and
refuses a changed source or owned environment after the two complete files.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from typing import Any


MIN_FREE_MB = 1536
METADATA_SUBPROCESS_TIMEOUT_SECONDS = 30.0
# Executable-member hashes derived from the Linux x64 archives verified against
# pm/lock.json. Archive hashes and extracted executable hashes are distinct.
DRIVER_SHA256 = "43d926a1b938012379db57cf351530f81e75a574017676815f85d1e13aff23f7"
CHROMIUM_SHA256 = "481fea1516a1f2b76454664272f12cd9dd1f20117b21e1f1498e08bc7f872c00"
TEST_FILES = (
    "tests/gateway/test_cua_managed_admission_delivery.py",
    "tests/tools/test_computer_use_cua_custody.py",
)
RUNNER_FILE = "scripts/run_tests_parallel.py"
SOURCE_CONTRACT_FILES = TEST_FILES + (RUNNER_FILE,)
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache"}
SKIP_SUFFIXES = {".pyc", ".pyo"}
SYSTEM_PACKAGES = (
    "tigervnc-standalone-server",
    "xfce4-panel",
    "xfwm4",
    "xfdesktop4",
    "xfce4-settings",
    "xfce4-terminal",
    "dbus-x11",
    "x11-xserver-utils",
    "x11-utils",
    "x11-xkb-utils",
    "xauth",
    "fonts-dejavu-core",
    "libasound2t64",
    "libatk-bridge2.0-0",
    "libatk1.0-0",
    "libcairo2",
    "libcups2",
    "libdbus-1-3",
    "libdrm2",
    "libgbm1",
    "libglib2.0-0",
    "libgtk-3-0",
    "libnspr4",
    "libnss3",
    "libpango-1.0-0",
    "libx11-6",
    "libxcb1",
    "libxcomposite1",
    "libxdamage1",
    "libxext6",
    "libxfixes3",
    "libxrandr2",
    "libxshmfence1",
)


def proof_dir() -> Path:
    value = os.environ.get("HERMES_CUA_PROOF_DIR")
    if not value:
        raise SystemExit("HERMES_CUA_PROOF_DIR is required")
    result = Path(value).resolve()
    result.mkdir(parents=True, exist_ok=True)
    return result


def repo_root() -> Path:
    value = os.environ.get("GITHUB_WORKSPACE")
    result = Path(value).resolve() if value else Path.cwd().resolve()
    if not (result / ".git").exists():
        raise SystemExit("the checkout root is not the expected Git worktree")
    return result


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(name: str, value: Any) -> None:
    destination = proof_dir() / name
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(destination)


def append_github_env(values: dict[str, str]) -> None:
    destination = os.environ.get("GITHUB_ENV")
    if not destination:
        raise SystemExit("GITHUB_ENV is required for owned runtime paths")
    with Path(destination).open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            if any(character in key + value for character in "\r\n\0"):
                raise SystemExit(f"invalid GitHub environment value for {key}")
            stream.write(f"{key}={value}\n")


def git_bytes(root: Path, *args: str) -> bytes:
    return subprocess.check_output(
        ["git", *args], cwd=root, timeout=METADATA_SUBPROCESS_TIMEOUT_SECONDS
    )


def _owned_entry(path: Path) -> dict[str, str]:
    """Describe one owned input without following a symlink target."""
    if path.is_symlink():
        return {"kind": "symlink", "target": os.readlink(path)}
    if path.is_file():
        return {"kind": "file", "sha256": sha_file(path)}
    raise SystemExit(f"owned input entry is not a regular file or symlink: {path}")


def _worktree_sha256(path: Path) -> str:
    if path.is_symlink():
        return sha_bytes(os.readlink(path).encode("utf-8"))
    if path.is_file():
        return sha_file(path)
    raise SystemExit(f"required source file is not a regular file or symlink: {path}")


def git_inventory(root: Path) -> dict[str, dict[str, str]]:
    raw = git_bytes(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    names = sorted(name.decode("utf-8") for name in raw.split(b"\0") if name)
    result: dict[str, dict[str, str]] = {}
    for name in names:
        path = root / name
        result[name] = _owned_entry(path)
    return result


def tree_inventory(root: Path) -> dict[str, dict[str, str]]:
    if not root.is_dir():
        raise SystemExit(f"owned input root is missing: {root}")
    result: dict[str, dict[str, str]] = {}

    def visit(directory: Path) -> None:
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError as exc:
            raise SystemExit(f"cannot inventory owned input root: {directory}") from exc
        for entry in entries:
            if entry.name in SKIP_DIRS:
                continue
            path = Path(entry.path)
            relative = str(path.relative_to(root))
            if entry.is_symlink():
                result[relative] = {"kind": "symlink", "target": os.readlink(path)}
                continue
            if entry.is_dir(follow_symlinks=False):
                visit(path)
                continue
            if entry.name.endswith(tuple(SKIP_SUFFIXES)):
                continue
            if entry.is_file(follow_symlinks=False):
                result[relative] = {"kind": "file", "sha256": sha_file(path)}
    visit(root)
    return result


def git_state(root: Path) -> dict[str, str]:
    return {
        "head": git_bytes(root, "rev-parse", "HEAD").decode().strip(),
        "index_sha256": sha_bytes(git_bytes(root, "ls-files", "-s")),
        "status_sha256": sha_bytes(git_bytes(root, "status", "--porcelain=v1", "-z")),
        "diff_sha256": sha_bytes(git_bytes(root, "diff", "--raw", "--abbrev=40")),
        "cached_diff_sha256": sha_bytes(git_bytes(root, "diff", "--cached", "--raw", "--abbrev=40")),
    }


def source_contract(expected_head: str) -> None:
    """Require the candidate checkout to contain the exact complete CUA files.

    The managed CUA modules and strict runner are source inputs, not private
    working-tree fixtures. This check intentionally runs before PM setup and
    before the static gate, so an untracked file or checkout at the wrong ref
    cannot be presented as a CI proof.
    """
    root = repo_root()
    actual_head = git_bytes(root, "rev-parse", "HEAD").decode().strip()
    expected = expected_head or os.environ.get("HERMES_CUA_EXPECTED_HEAD", "").strip()
    if not expected or actual_head != expected:
        raise SystemExit(f"candidate HEAD mismatch: expected {expected or '<missing>'}, got {actual_head}")
    status = git_bytes(root, "status", "--porcelain=v1", "--untracked-files=all").decode()
    if status:
        raise SystemExit("candidate checkout is not clean before the hosted CUA proof")
    files: dict[str, dict[str, Any]] = {}
    for name in SOURCE_CONTRACT_FILES:
        try:
            subprocess.run(
                ["git", "cat-file", "-e", f"HEAD:{name}"],
                cwd=root,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=METADATA_SUBPROCESS_TIMEOUT_SECONDS,
            )
            git_bytes(root, "ls-files", "--error-unmatch", "--", name)
            blob_oid = git_bytes(root, "rev-parse", f"HEAD:{name}").decode().strip()
            blob_bytes = git_bytes(root, "cat-file", "blob", blob_oid)
        except (subprocess.CalledProcessError, OSError) as exc:
            raise SystemExit(f"required CUA source file is not tracked at HEAD: {name}") from exc
        path = root / name
        working = _worktree_sha256(path)
        blob_sha256 = sha_bytes(blob_bytes)
        if working != blob_sha256:
            raise SystemExit(f"required CUA source file differs from HEAD: {name}")
        working_entry = _owned_entry(path)
        files[name] = {
            "git_blob_oid": blob_oid,
            "git_blob_sha256": blob_sha256,
            "working_tree_sha256": working,
            "working_tree": working_entry,
        }
    write_json("source-contract.json", {"expected_head": expected, "actual_head": actual_head, "files": files})


def system_packages() -> dict[str, str]:
    if not shutil.which("dpkg-query"):
        raise SystemExit("dpkg-query is required for the hosted Ubuntu receipt")
    output = subprocess.check_output(
        ["dpkg-query", "-W", "-f=${binary:Package}\t${Version}\n", *SYSTEM_PACKAGES],
        text=True,
        timeout=METADATA_SUBPROCESS_TIMEOUT_SECONDS,
    )
    lines = [line for line in output.splitlines() if line]
    if len(lines) != len(SYSTEM_PACKAGES):
        raise SystemExit("the exact Bot Desktop/Chromium system package receipt is incomplete")
    return {line.split("\t", 1)[0]: line.split("\t", 1)[1] for line in lines}


def read_meminfo() -> dict[str, int]:
    result: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8-sig").splitlines():
        key, separator, rest = line.partition(":")
        if not separator:
            continue
        pieces = rest.split()
        if pieces and pieces[0].isdigit():
            result[key] = int(pieces[0]) * 1024
    return result


def read_integer(path: Path) -> int | None:
    try:
        value = path.read_text(encoding="utf-8-sig").strip()
    except OSError:
        return None
    return int(value) if value.isdigit() else None


def read_stat(path: Path, key: str) -> int | None:
    try:
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            name, separator, value = line.partition(" ")
            if name == key and separator:
                return int(value.strip())
    except (OSError, ValueError):
        return None
    return None


def capacity(stage: str, source_floor: bool) -> None:
    meminfo = read_meminfo()
    host_available = meminfo.get("MemAvailable")
    host_total = meminfo.get("MemTotal")
    cgroup_root = Path("/sys/fs/cgroup")
    limit = read_integer(cgroup_root / "memory.max")
    usage = read_integer(cgroup_root / "memory.current")
    inactive_file = read_stat(cgroup_root / "memory.stat", "inactive_file")
    cgroup_version = "v2"
    if limit is None and usage is None:
        cgroup_root = Path("/sys/fs/cgroup/memory")
        limit = read_integer(cgroup_root / "memory.limit_in_bytes")
        usage = read_integer(cgroup_root / "memory.usage_in_bytes")
        inactive_file = read_stat(cgroup_root / "memory.stat", "total_inactive_file")
        cgroup_version = "v1"
        if limit is not None and limit >= 1 << 60:
            limit = None
    working_usage = None if usage is None else max(usage - (inactive_file or 0), 0)
    candidates = [value for value in (host_available, None if limit is None or working_usage is None else limit - working_usage) if value is not None]
    available = min(candidates) if candidates else None
    floor = MIN_FREE_MB
    configured_floor = None
    if source_floor:
        sys.path.insert(0, str(repo_root()))
        from tools.bot_desktop import resources

        configured_floor = resources.min_free_mb()
        floor = max(floor, configured_floor)
    receipt = {
        "stage": stage,
        "host_mem_total_bytes": host_total,
        "host_mem_available_bytes": host_available,
        "cgroup_version": cgroup_version,
        "cgroup_limit_bytes": limit,
        "cgroup_current_bytes": usage,
        "cgroup_inactive_file_bytes": inactive_file,
        "cgroup_working_set_bytes": working_usage,
        "available_bytes": available,
        "available_mb": None if available is None else available // (1024 * 1024),
        "minimum_free_mb": floor,
        "configured_source_floor_mb": configured_floor,
        "interpreter": sys.executable,
    }
    write_json(f"capacity-{stage}.json", receipt)
    available_mb = receipt["available_mb"]
    if available_mb is None or available_mb < floor:
        raise SystemExit(f"capacity gate refused {stage}: {available_mb} MiB available, {floor} MiB required")


def capacity_monitor(interval: float) -> None:
    """Record the same capacity predicate while the canonical two-file SUT runs."""
    if interval <= 0:
        raise SystemExit("capacity monitor interval must be positive")
    stop_file = proof_dir() / "capacity-monitor.stop"
    if stop_file.exists():
        raise SystemExit("capacity monitor stop marker already exists")
    samples: list[dict[str, Any]] = []
    failure: str | None = None
    while not stop_file.exists():
        stage = f"during-sut-{len(samples):04d}"
        try:
            capacity(stage, source_floor=True)
        except SystemExit as exc:
            receipt_path = proof_dir() / f"capacity-{stage}.json"
            if receipt_path.is_file():
                samples.append(json.loads(receipt_path.read_text(encoding="utf-8-sig")))
            failure = str(exc)
            break
        receipt_path = proof_dir() / f"capacity-{stage}.json"
        if not receipt_path.is_file():
            failure = f"capacity monitor did not write {receipt_path.name}"
            break
        samples.append(json.loads(receipt_path.read_text(encoding="utf-8-sig")))
        if not stop_file.exists():
            time.sleep(interval)
    write_json(
        "capacity-during-sut.json",
        {
            "sample_count": len(samples),
            "samples": samples,
            "stopped_by": "workflow" if failure is None else "capacity-failure",
            "failure": failure,
        },
    )
    if failure is not None:
        raise SystemExit(failure)


def executable_hash(path: str, expected: str, label: str) -> str:
    candidate = Path(path).resolve()
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise SystemExit(f"{label} is not an executable file: {candidate}")
    actual = sha_file(candidate)
    if actual != expected:
        raise SystemExit(f"{label} hash mismatch: {actual}")
    return str(candidate)


def resolve_tools() -> None:
    if platform.machine().lower() not in {"x86_64", "amd64"}:
        raise SystemExit("hosted CUA lane requires Linux x86_64 PM artifacts")
    driver = shutil.which("cua-driver")
    if not driver:
        raise SystemExit("PM did not place cua-driver on PATH")
    driver_path = executable_hash(driver, DRIVER_SHA256, "cua-driver")
    sys.path.insert(0, str(repo_root()))
    from hermes_cli.browser_runtime import chromium_executable

    chromium = chromium_executable(allow_override=False)
    if not chromium:
        raise SystemExit("PM did not expose Chromium")
    chromium_path = executable_hash(chromium, CHROMIUM_SHA256, "Chromium")
    path_bin = proof_dir() / "bin"
    path_bin.mkdir(parents=True, exist_ok=True, mode=0o700)
    chromium_link = path_bin / "chromium"
    if chromium_link.exists() or chromium_link.is_symlink():
        raise SystemExit(f"owned Chromium PATH entry already exists: {chromium_link}")
    chromium_link.symlink_to(chromium_path)
    if chromium_link.resolve() != Path(chromium_path).resolve():
        raise SystemExit("owned Chromium PATH entry does not resolve to the sealed PM binary")
    receipt = {
        "architecture": platform.machine(),
        "cua_driver": {"path": driver_path, "sha256": DRIVER_SHA256},
        "chromium": {
            "path": chromium_path,
            "sha256": CHROMIUM_SHA256,
            "path_entry": str(chromium_link),
            "path_entry_target": str(chromium_link.resolve()),
        },
    }
    write_json("resolved-tools.json", receipt)
    existing_path = os.environ.get("PATH", "")
    append_github_env({
        "HERMES_CUA_DRIVER_CMD": driver_path,
        "AGC_CUA_PUBLIC_DRIVER": driver_path,
        "AGENT_BROWSER_EXECUTABLE_PATH": chromium_path,
        "PATH": f"{path_bin}:{existing_path}",
    })


def _required_linux_signal(name: str) -> int:
    """Resolve a POSIX signal only after proving this helper is on Linux."""
    if platform.system() != "Linux" or os.name != "posix":
        raise SystemExit("the hosted CUA runner requires Linux POSIX signals")
    value = getattr(signal, name, None)
    if value is None:
        raise SystemExit(f"Linux signal {name} is unavailable")
    return int(value)


def _signal_is_default(name: str) -> bool:
    signal_number = _required_linux_signal(name)
    default = getattr(signal, "SIG_DFL", None)
    if default is None:
        raise SystemExit("the signal module has no SIG_DFL sentinel")
    return signal.getsignal(signal_number) == default


def _kill_owned_process_group(process: subprocess.Popen[bytes], signal_name: str) -> None:
    """Use killpg only through an explicit Linux capability check."""
    if platform.system() != "Linux" or os.name != "posix":
        raise SystemExit("owned process-group cleanup requires Linux POSIX support")
    killpg = getattr(os, "killpg", None)
    if not callable(killpg):
        raise SystemExit("owned process-group cleanup requires native group signaling")
    killpg(process.pid, _required_linux_signal(signal_name))


def terminate_owned_process(process: subprocess.Popen[bytes], timeout: float = 5.0) -> None:
    if process.poll() is not None:
        return
    try:
        _kill_owned_process_group(process, "SIGTERM")
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + max(timeout, 0.0)
    remaining = max(deadline - time.monotonic(), 0.0)
    if remaining:
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            pass
    if process.poll() is None:
        try:
            _kill_owned_process_group(process, "SIGKILL")
        except ProcessLookupError:
            pass
        try:
            process.kill()
        except (ProcessLookupError, OSError):
            pass
        remaining = max(deadline - time.monotonic(), 0.0)
        if remaining:
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass


def browser_preflight() -> None:
    tools = json.loads((proof_dir() / "resolved-tools.json").read_text(encoding="utf-8-sig"))
    browser = tools["chromium"]["path"]
    profile = proof_dir() / "browser-preflight-profile"
    profile.mkdir(mode=0o700)
    command = [
        browser,
        "--headless=new",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        f"--user-data-dir={profile}",
        "--dump-dom",
        "data:,",
    ]
    context = _enable_subreaper()
    started = time.monotonic()
    deadline = started + 30.0
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    timed_out = False
    stdout: bytes = b""
    stderr: bytes = b""
    remaining = max(deadline - time.monotonic(), 0.0)
    if remaining <= 0.0:
        timed_out = True
        terminate_owned_process(process, timeout=0.0)
    else:
        try:
            stdout, stderr = process.communicate(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            stdout = exc.stdout or b""
            stderr = exc.stderr or b""
            terminate_owned_process(process, timeout=max(deadline - time.monotonic(), 0.0))
    remaining = max(deadline - time.monotonic(), 0.0)
    reap = _reap_owned_descendants(deadline)
    after_subreaper = _subreaper_state()
    after_sigchld = _signal_is_default("SIGCHLD")
    closure_ok = (
        after_subreaper
        and after_sigchld
        and reap["reached_echild"]
        and not reap["live_children"]
        and reap["waitid_error"] is None
    )
    success = not timed_out and process.returncode == 0 and closure_ok
    receipt = {
        "pid": process.pid,
        "returncode": process.returncode,
        "stdout_sha256": sha_bytes(stdout),
        "stderr_sha256": sha_bytes(stderr),
        "timed_out": timed_out,
        "subreaper_before_launch": context["enabled"],
        "subreaper_after_wait": after_subreaper,
        "sigchld_default_before_launch": context["sigchld"] == "SIG_DFL",
        "sigchld_default_after_wait": after_sigchld,
        "original_deadline_seconds": 30.0,
        "remaining_deadline_seconds": remaining,
        "success": success,
        **reap,
    }
    write_json("browser-preflight.json", receipt)
    if timed_out:
        raise SystemExit("owned Chromium preflight exceeded 30 seconds")
    if process.returncode != 0:
        raise SystemExit("owned Chromium preflight failed")
    if not closure_ok:
        raise SystemExit("owned Chromium preflight descendants did not reach ECHILD")


def _subreaper_state() -> bool:
    if platform.system() != "Linux":
        raise SystemExit("the hosted CUA runner requires Linux child-subreaper support")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.restype = ctypes.c_int
    value = ctypes.c_int()
    if prctl(37, ctypes.byref(value), 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise SystemExit(f"PR_GET_CHILD_SUBREAPER failed: errno {error}")
    return value.value == 1


def _enable_subreaper() -> dict[str, Any]:
    if not _signal_is_default("SIGCHLD"):
        raise SystemExit("SIGCHLD must have its default disposition before the owned runner")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.restype = ctypes.c_int
    if prctl(36, 1, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise SystemExit(f"PR_SET_CHILD_SUBREAPER failed: errno {error}")
    if not _subreaper_state():
        raise SystemExit("PR_SET_CHILD_SUBREAPER did not remain enabled")
    return {"enabled": True, "sigchld": "SIG_DFL"}


def _runner_deadline_seconds() -> float:
    value = os.environ.get("HERMES_TEST_FILE_TIMEOUT", "300").strip()
    try:
        result = float(value)
    except ValueError as exc:
        raise SystemExit("HERMES_TEST_FILE_TIMEOUT must be numeric") from exc
    if result <= 0:
        raise SystemExit("HERMES_TEST_FILE_TIMEOUT must be positive")
    return result


def _waitid_flags() -> int:
    required = ("P_ALL", "WEXITED", "WNOHANG")
    missing = [name for name in required if not hasattr(os, name)]
    if missing:
        raise SystemExit(f"native waitid is missing required constants: {', '.join(missing)}")
    # Linux exposes __WALL as a kernel wait flag; Python does not export it on
    # every supported interpreter. It is required so cloned descendants are
    # observed by waitid(P_ALL, ...).
    wall = int(getattr(os, "__WALL", 0x40000000))
    return int(os.WEXITED) | int(os.WNOHANG) | wall


def _waitid_record(info: Any) -> dict[str, int]:
    fields = ("si_pid", "si_uid", "si_status", "si_code")
    record: dict[str, int] = {}
    for field in fields:
        value = getattr(info, field, None)
        if not isinstance(value, int):
            raise SystemExit(f"native waitid returned an invalid {field}")
        record[field] = value
    if record["si_pid"] <= 0:
        raise SystemExit("native waitid returned an invalid child identity")
    return record


def _reap_owned_descendants(deadline: float) -> dict[str, Any]:
    flags = _waitid_flags()
    started = time.monotonic()
    deadline_seconds = max(deadline - started, 0.0)
    adopted: list[dict[str, int]] = []
    nohang_polls = 0
    waitid_error: dict[str, Any] | None = None
    reached_echild = False
    live_children = False
    deadline_exceeded = False
    while True:
        if time.monotonic() >= deadline:
            deadline_exceeded = True
            live_children = True
            break
        try:
            info = os.waitid(os.P_ALL, 0, flags)
        except ChildProcessError:
            if time.monotonic() >= deadline:
                deadline_exceeded = True
                live_children = True
            else:
                reached_echild = True
            break
        except InterruptedError:
            if time.monotonic() >= deadline:
                deadline_exceeded = True
                live_children = True
                break
            continue
        except OSError as exc:
            if time.monotonic() >= deadline:
                deadline_exceeded = True
                live_children = True
            else:
                waitid_error = {"errno": exc.errno, "type": type(exc).__name__}
            break
        if time.monotonic() >= deadline:
            deadline_exceeded = True
            live_children = True
            break
        if info is None:
            nohang_polls += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                deadline_exceeded = True
                live_children = True
                break
            time.sleep(min(0.05, remaining))
            continue
        adopted.append(_waitid_record(info))
    return {
        "waitid_flags": flags,
        "waitid_none_polls": nohang_polls,
        "adopted_statuses": adopted,
        "reached_echild": reached_echild,
        "live_children": live_children,
        "waitid_error": waitid_error,
        "deadline_exceeded": deadline_exceeded,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "deadline_seconds": deadline_seconds,
    }


def _write_child_output(value: str | bytes | None) -> None:
    if not value:
        return
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    sys.stdout.write(value)
    sys.stdout.flush()


def run_owned_child(command: list[str], deadline_seconds: float) -> int:
    """Run one pytest root and prove its descendants closed before success.

    This mode is called only by the strict hosted Linux runner. It deliberately
    has no proof-directory writes or test orchestration: the per-file runner
    owns summaries, retries, and cleanup policy.
    """
    if not command or deadline_seconds <= 0:
        raise SystemExit("owned child requires a command and positive deadline")
    context = _enable_subreaper()
    started = time.monotonic()
    deadline = started + deadline_seconds
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    remaining = max(deadline - time.monotonic(), 0.0)
    if remaining <= 0.0:
        print(
            "owned-child-closure "
            + json.dumps(
                {
                    "root_returncode": 124,
                    "reached_echild": False,
                    "live_children": True,
                    "timeout": True,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 124
    try:
        output, _ = process.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        _write_child_output(exc.stdout)
        print(
            "owned-child-closure "
            + json.dumps(
                {
                    "root_returncode": 124,
                    "reached_echild": False,
                    "live_children": True,
                    "timeout": True,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 124
    _write_child_output(output)
    reap = _reap_owned_descendants(deadline)
    after_subreaper = _subreaper_state()
    after_sigchld = _signal_is_default("SIGCHLD")
    result = {
        "root_returncode": process.returncode,
        "subreaper_before_launch": context["enabled"],
        "subreaper_after_wait": after_subreaper,
        "sigchld_default_before_launch": context["sigchld"] == "SIG_DFL",
        "sigchld_default_after_wait": after_sigchld,
        **reap,
    }
    print("owned-child-closure " + json.dumps(result, sort_keys=True), flush=True)
    if (
        not result["subreaper_after_wait"]
        or not result["sigchld_default_after_wait"]
        or not result["reached_echild"]
        or result["live_children"]
        or result["waitid_error"] is not None
    ):
        return 125
    return int(process.returncode)


def run_tests() -> None:
    root = repo_root()
    receipt_context = _enable_subreaper()
    deadline_seconds = _runner_deadline_seconds()
    command = [
        str(root / "scripts/run_tests.sh"),
        "--fail-on-skip",
        "-j",
        "1",
        "--file-retries",
        "0",
        "--strict-owned-descendants",
        *TEST_FILES,
        "--",
        "-m",
        "integration or not integration",
    ]
    output_path = proof_dir() / "cua-tests.stdout"
    runner_started = time.monotonic()
    runner_deadline = runner_started + deadline_seconds
    process = subprocess.Popen(
        command,
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if process.stdout is None:
        raise SystemExit("canonical CUA runner did not expose stdout")
    with output_path.open("w", encoding="utf-8") as output:
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            output.write(line)
    remaining_deadline_seconds = max(runner_deadline - time.monotonic(), 0.0)
    try:
        returncode = process.wait(timeout=remaining_deadline_seconds)
    except subprocess.TimeoutExpired as exc:
        raise SystemExit("canonical CUA runner did not exit before the existing deadline") from exc
    reap = _reap_owned_descendants(runner_deadline)
    after_subreaper = _subreaper_state()
    after_sigchld = _signal_is_default("SIGCHLD")
    receipt = {
        "command": command,
        "root_pid": process.pid,
        "root_returncode": returncode,
        "subreaper_before_launch": receipt_context["enabled"],
        "subreaper_after_wait": after_subreaper,
        "sigchld_default_before_launch": receipt_context["sigchld"] == "SIG_DFL",
        "sigchld_default_after_wait": after_sigchld,
        "original_deadline_seconds": deadline_seconds,
        "remaining_deadline_seconds": remaining_deadline_seconds,
        **reap,
    }
    write_json("runner-reap.json", receipt)
    if not receipt["subreaper_after_wait"] or not receipt["sigchld_default_after_wait"]:
        raise SystemExit("child-subreaper or SIGCHLD state changed during the owned runner")
    if not receipt["reached_echild"] or receipt["live_children"] or receipt["waitid_error"] is not None:
        raise SystemExit("owned descendants did not reach ECHILD before the existing runner deadline")
    if returncode != 0:
        raise SystemExit(returncode)


def snapshot(label: str) -> None:
    root = repo_root()
    pm_root = Path(sys.prefix).resolve()
    base_root = Path(sys.base_prefix).resolve()
    tools_root = Path(os.environ.get("HERMES_RUNTIME_DIR", "")).resolve()
    if pm_root == base_root:
        raise SystemExit("snapshot requires the isolated PM test interpreter")
    receipt = {
        "label": label,
        "source": git_inventory(root),
        "git": git_state(root),
        "system_packages": system_packages(),
        "pm": {"root": str(pm_root), "files": tree_inventory(pm_root)},
        "base": {"root": str(base_root), "files": tree_inventory(base_root)},
        "tools": {"root": str(tools_root), "files": tree_inventory(tools_root)},
        "interpreter": {"executable": sys.executable, "version": sys.version, "prefix": sys.prefix},
    }
    write_json(f"snapshot-{label}.json", receipt)


def test_summary() -> dict[str, int]:
    output = (proof_dir() / "cua-tests.stdout").read_text(encoding="utf-8-sig", errors="replace")
    pattern = re.compile(
        r"^=== Summary: (?P<files>\d+) files, (?P<passed>\d+) tests passed, "
        r"(?P<failed>\d+) failed(?P<tail>.*?) \(",
        re.MULTILINE,
    )
    matches = list(pattern.finditer(output))
    if len(matches) != 1:
        raise SystemExit("canonical runner summary is missing or ambiguous")
    match = matches[0]
    counts = {name: int(match.group(name)) for name in ("files", "passed", "failed")}
    if "skipped" in match.group("tail") or "CRASHED" in match.group("tail"):
        raise SystemExit("canonical runner reported skipped or crashed tests")
    if "deselected" in output:
        raise SystemExit("canonical runner reported deselected tests")
    for test_file in TEST_FILES:
        if test_file not in output:
            raise SystemExit(f"canonical runner did not report complete file: {test_file}")
    return counts


def _proof_json(name: str) -> dict[str, Any]:
    return json.loads((proof_dir() / name).read_text(encoding="utf-8-sig"))


def _verify_test_output(test_outcome: str) -> tuple[dict[str, int], str]:
    if test_outcome != "success":
        raise SystemExit(f"CUA test step outcome was {test_outcome}")
    counts = test_summary()
    if counts != {"files": 2, "passed": 12, "failed": 0}:
        raise SystemExit(f"expected 2 files and 12 passed tests, received {counts}")
    output = (proof_dir() / "cua-tests.stdout").read_text(encoding="utf-8-sig", errors="replace")
    if "process tree SIGKILL'd" in output or "emergency cleanup" in output.lower():
        raise SystemExit("canonical runner reported emergency cleanup")
    return counts, output


def _verify_snapshots() -> None:
    before_static = _proof_json("snapshot-before-static.json")
    before = _proof_json("snapshot-before-sut.json")
    after = _proof_json("snapshot-after-sut.json")
    for key in ("source", "git", "system_packages", "pm", "base", "tools"):
        if before_static[key] != before[key]:
            raise SystemExit(f"owned {key} changed during the static gate")
        if before[key] != after[key]:
            raise SystemExit(f"owned {key} changed during the CUA tests")


def _verify_tool_receipt() -> None:
    resolved = _proof_json("resolved-tools.json")
    chromium = resolved.get("chromium") or {}
    path_entry = chromium.get("path_entry")
    path_target = chromium.get("path_entry_target")
    if not isinstance(path_entry, str) or not isinstance(path_target, str):
        raise SystemExit("sealed Chromium PATH entry is missing from the tool receipt")
    if str(Path(path_entry).resolve()) != path_target:
        raise SystemExit("sealed Chromium PATH entry changed after resolution")


def _verify_capacity_receipts() -> None:
    monitor = _proof_json("capacity-during-sut.json")
    if not isinstance(monitor.get("samples"), list) or not monitor["samples"] or monitor.get("failure") is not None:
        raise SystemExit("capacity was not observed successfully during the canonical CUA run")
    for path in sorted(proof_dir().glob("capacity-*.json")):
        if path.name == "capacity-during-sut.json":
            continue
        receipt = json.loads(path.read_text(encoding="utf-8-sig"))
        if receipt.get("available_mb") is None or receipt["available_mb"] < receipt["minimum_free_mb"]:
            raise SystemExit(f"capacity receipt failed: {path.name}")


def _required_reap_receipt_values() -> dict[str, Any]:
    return {
        "subreaper_before_launch": True,
        "subreaper_after_wait": True,
        "sigchld_default_before_launch": True,
        "sigchld_default_after_wait": True,
        "reached_echild": True,
        "live_children": False,
        "waitid_error": None,
    }


def _verify_reap_receipt() -> dict[str, Any]:
    reap = _proof_json("runner-reap.json")
    required = _required_reap_receipt_values()
    for key, expected in required.items():
        if reap.get(key) is not expected:
            raise SystemExit(f"owned descendant receipt failed {key}: {reap.get(key)!r}")
    original_deadline = reap.get("original_deadline_seconds")
    remaining_deadline = reap.get("remaining_deadline_seconds")
    if not isinstance(original_deadline, (int, float)) or not isinstance(remaining_deadline, (int, float)):
        raise SystemExit("owned runner receipt is missing its absolute deadline accounting")
    if remaining_deadline < 0 or remaining_deadline > original_deadline:
        raise SystemExit("owned runner reaping used a fresh or invalid deadline budget")
    if not isinstance(reap.get("adopted_statuses"), list):
        raise SystemExit("owned descendant receipt has no waitid status list")
    if reap.get("root_returncode") != 0:
        raise SystemExit("owned runner returned a nonzero status")
    return reap


def _child_closure_receipts(output: str) -> list[dict[str, Any]]:
    child_receipts: list[dict[str, Any]] = []
    prefix = "owned-child-closure "
    for line in output.splitlines():
        if not line.startswith(prefix):
            continue
        try:
            child_receipts.append(json.loads(line[len(prefix) :]))
        except ValueError as exc:
            raise SystemExit("owned child closure line is not valid JSON") from exc
    return child_receipts


def _verify_child_receipts(output: str) -> int:
    child_receipts = _child_closure_receipts(output)
    if len(child_receipts) != len(TEST_FILES):
        raise SystemExit("strict hosted runner did not emit one closure receipt per CUA file")
    child_files = [child.get("file") for child in child_receipts]
    if not all(isinstance(path, str) for path in child_files):
        raise SystemExit(f"strict hosted runner closure identity is missing: {child_files!r}")
    if sorted(child_files) != sorted(TEST_FILES) or len(set(child_files)) != len(TEST_FILES):
        raise SystemExit(f"strict hosted runner closure identities are not exact: {child_files!r}")
    required = _required_reap_receipt_values()
    for child in child_receipts:
        for key, expected in required.items():
            if child.get(key) is not expected:
                raise SystemExit(f"owned child closure failed {key}: {child.get(key)!r}")
        if child.get("root_returncode") != 0:
            raise SystemExit("owned child pytest root returned a nonzero status")
    return len(child_receipts)


def verify(test_outcome: str) -> None:
    counts, output = _verify_test_output(test_outcome)
    _verify_snapshots()
    _verify_tool_receipt()
    _verify_capacity_receipts()
    _verify_reap_receipt()
    child_count = _verify_child_receipts(output)
    write_json(
        "verified.json",
        {
            "tests": counts,
            "owned_inputs_unchanged": True,
            "capacity_observed_during_sut": True,
            "owned_descendants_reaped": True,
            "waitid_reached_echild": True,
            "strict_owned_child_receipts": child_count,
            "qualification": "qualified_with_strict_owned_runner_boundary",
        },
    )


def _dispatch_owned_child(args: argparse.Namespace) -> None:
    command = list(args.child_argv)
    if command[:1] == ["--"]:
        command = command[1:]
    raise SystemExit(run_owned_child(command, args.deadline_seconds))


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    capacity_parser = subparsers.add_parser("capacity")
    capacity_parser.add_argument("--stage", required=True)
    capacity_parser.add_argument("--source-floor", action="store_true")
    source_parser = subparsers.add_parser("source-contract")
    source_parser.add_argument("--expected-head", required=True)
    monitor_parser = subparsers.add_parser("capacity-monitor")
    monitor_parser.add_argument("--interval", type=float, default=1.0)
    subparsers.add_parser("run-tests")
    child_parser = subparsers.add_parser("run-owned-child")
    child_parser.add_argument("--deadline-seconds", type=float, required=True)
    child_parser.add_argument("child_argv", nargs=argparse.REMAINDER)
    subparsers.add_parser("resolve-tools")
    subparsers.add_parser("browser-preflight")
    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("--label", required=True, choices=("before-static", "before-sut", "after-sut"))
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--test-outcome", required=True)
    args = parser.parse_args()
    handlers = {
        "source-contract": lambda: source_contract(args.expected_head),
        "capacity": lambda: capacity(args.stage, args.source_floor),
        "capacity-monitor": lambda: capacity_monitor(args.interval),
        "run-tests": run_tests,
        "run-owned-child": lambda: _dispatch_owned_child(args),
        "resolve-tools": resolve_tools,
        "browser-preflight": browser_preflight,
        "snapshot": lambda: snapshot(args.label),
        "verify": lambda: verify(args.test_outcome),
    }
    try:
        handler = handlers[args.command]
    except KeyError as exc:
        raise SystemExit(f"unknown command: {args.command}") from exc
    handler()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
