#!/usr/bin/env python3
"""Run one canonical Windows test command under the product-owned Job seam."""

from __future__ import annotations

import argparse
import datetime as dt
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OUTER_TIMEOUT_SECONDS = 600.0
logger = logging.getLogger(__name__)


def utc_now() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.write_text(content, encoding="utf-8")


def digest(path: Path) -> str | None:
    if not path.is_file():
        return None
    return sha256(path.read_bytes()).hexdigest()


def display_argument(command: list[str], value: str) -> str:
    if command and value == command[0]:
        return "<git-bash>"
    if value.startswith(str(ROOT)):
        return Path(value).relative_to(ROOT).as_posix()
    if value.startswith("/") or ":/" in value or "\\" in value:
        return "<runner-path>"
    return value


def record_failure(event: dict[str, Any], stage: str, exc: BaseException) -> None:
    event["failure"] = {"stage": stage, "type": type(exc).__name__}


def record_exit_failure(event: dict[str, Any], exit_code: int) -> None:
    event["failure"] = {
        "stage": "canonical-runner-exit",
        "type": "ProcessExit",
        "exit_code": exit_code,
    }


def record_cleanup_error(event: dict[str, Any], step: str, exc: BaseException) -> None:
    event["forced_cleanup"] = True
    event.setdefault("cleanup_errors", []).append(
        {"step": step, "type": type(exc).__name__}
    )


def _native_handle_identity(handle: Any) -> dict[str, Any]:
    """Record the exact retained native handle; invalid handles fail closed."""
    native_value = int(handle)
    if native_value <= 0:
        raise RuntimeError("retained-native-handle-invalid")
    return {
        "python_type": type(handle).__name__,
        "python_object_id": id(handle),
        "native_value": native_value,
    }


def observe_owned_job_empty(
    job: Any,
    process_ref: Any,
    deadline: float,
    event: dict[str, Any],
) -> None:
    """Observe the same owned Job until it is empty, without reopening or census."""
    started = time.monotonic()
    samples: list[dict[str, Any]] = []
    event["native_job_observation_started_monotonic"] = started
    event["native_job_observation_deadline_monotonic"] = deadline
    event["native_job_observation_budget_seconds"] = max(0.0, deadline - started)
    event["native_job_observation_samples_after_exit"] = samples
    event["native_job_observation_retained_python_objects"] = {
        "job": id(job), "process_ref": id(process_ref)
    }
    event["native_job_observation"] = "pending"
    while True:
        sampled_at = time.monotonic()
        sample: dict[str, Any] = {
            "monotonic_seconds": sampled_at - started,
            "active_processes": None,
            "exact_process_handle_member": None,
        }
        samples.append(sample)
        try:
            sample["exact_process_handle_member"] = job.contains_process_handle(process_ref)
            sample["active_processes"] = job.active_processes()
        except BaseException as exc:
            sample["error"] = type(exc).__name__
            event["native_runner_tree_empty_after_exit"] = False
            event["native_job_observation"] = "native-query-failed"
            logger.exception("retained Windows Job observation failed")
            raise
        membership = sample["exact_process_handle_member"]
        active = sample["active_processes"]
        if len(samples) == 1:
            event["exact_process_handle_member_after_exit"] = membership
            event["native_job_active_processes_after_exit"] = active
        if membership is not True:
            event["native_runner_tree_empty_after_exit"] = False
            event["native_job_observation"] = "membership_unproven"
            raise RuntimeError("exact-membership-after-exit-unproven")
        if type(active) is not int or active < 0:
            event["native_runner_tree_empty_after_exit"] = False
            event["native_job_observation"] = "active-count-unavailable"
            raise RuntimeError("owned-job-active-count-unavailable")
        if active == 0:
            event["native_runner_tree_empty_after_exit"] = True
            event["native_job_observation"] = "empty"
            event["native_job_active_processes_after_exit_settled"] = active
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            event["native_runner_tree_empty_after_exit"] = False
            event["native_job_observation"] = "persistent-nonzero"
            event["native_job_active_processes_after_exit_settled"] = active
            raise RuntimeError("owned-job-has-active-descendants")
        time.sleep(min(0.05, remaining))


def close_owned_job(job: Any, event: dict[str, Any]) -> bool:
    """Close the returned Job; recover only through that same handle and fail closed."""
    if job is None:
        return False
    try:
        job.close()
    except BaseException as exc:
        logger.exception("owned Windows Job close failed")
        record_cleanup_error(event, "owned-job-close", exc)
        try:
            job.terminate()
        except BaseException as terminate_exc:
            logger.exception("owned Windows Job termination failed")
            record_cleanup_error(event, "owned-job-terminate", terminate_exc)
        try:
            job.close()
        except BaseException as retry_exc:
            logger.exception("owned Windows Job close retry failed")
            record_cleanup_error(event, "owned-job-close-retry", retry_exc)
    event["job_handle_closed"] = getattr(job, "_handle", None) is None
    if not event["job_handle_closed"]:
        record_cleanup_error(
            event, "owned-job-handle-retained", RuntimeError("job handle retained")
        )
    return event["job_handle_closed"] and not event["cleanup_errors"]


def reap_exact_process(proc: Any, event: dict[str, Any]) -> bool:
    """Reap and close the retained Popen handle; never reopen or census a PID."""
    if proc is None:
        return False
    reaped = False
    try:
        proc.wait(timeout=10)
        reaped = True
    except subprocess.TimeoutExpired as exc:
        logger.exception("exact child wait timed out before cleanup")
        record_cleanup_error(event, "exact-child-wait", exc)
        try:
            proc.kill()
        except BaseException as kill_exc:
            logger.exception("exact child kill failed")
            record_cleanup_error(event, "exact-child-kill", kill_exc)
        try:
            proc.wait(timeout=10)
            reaped = True
        except BaseException as wait_exc:
            logger.exception("exact child wait after kill failed")
            record_cleanup_error(event, "exact-child-wait-after-kill", wait_exc)
    except BaseException as exc:
        logger.exception("exact child wait failed")
        record_cleanup_error(event, "exact-child-wait", exc)

    handle = getattr(proc, "_handle", None)
    close = getattr(handle, "Close", None)
    if not callable(close):
        record_cleanup_error(
            event, "exact-process-handle-close", RuntimeError("handle missing")
        )
        return False
    try:
        close()
    except BaseException as exc:
        logger.exception("exact process handle close failed")
        record_cleanup_error(event, "exact-process-handle-close", exc)
        return False
    event["process_handle_closed"] = True
    return reaped


def run(
    command: list[str],
    receipt: Path,
    stdout_path: Path,
    junit_path: Path,
    timeout: float,
) -> int:
    event: dict[str, Any] = {
        "schema": "windows-native-custody-event-v2",
        "command": [display_argument(command, value) for value in command],
        "started_at_utc": utc_now(),
        "outcome": "not_started",
        "forced_cleanup": False,
        "cleanup_errors": [],
        "job_handle_closed": False,
        "process_handle_closed": False,
        "artifacts": {
            "receipt": receipt.name,
            "stdout": stdout_path.name,
            "junit": junit_path.name,
        },
        "native_launcher": "hermes_cli.local_runtime.processes.spawn_server",
    }
    proc = None
    job = None
    result = 1
    try:
        if sys.platform != "win32":
            raise RuntimeError("real-win32-required")
        if not command:
            raise ValueError("canonical-command-required")
        if not all(path.is_absolute() for path in (receipt, stdout_path, junit_path)):
            raise ValueError("native-output-paths-required")
        sys.path.insert(0, str(ROOT))
        from hermes_cli.local_runtime.processes import spawn_server

        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with stdout_path.open("wb") as stream:
            proc, job = spawn_server(
                command,
                cwd=str(ROOT),
                env=os.environ.copy(),
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            raw_handle = getattr(proc, "_handle", None)
            if job is None or proc is None or raw_handle is None:
                raise RuntimeError("owned-job-launch-proof-missing")
            process_ref = SimpleNamespace(raw_handle=raw_handle)
            event["native_job_handle_identity"] = _native_handle_identity(
                getattr(job, "_handle", None)
            )
            event["native_root_process_handle_identity"] = _native_handle_identity(raw_handle)
            event["exact_process_handle_member_after_resume"] = (
                job.contains_process_handle(process_ref) is True
            )
            if not event["exact_process_handle_member_after_resume"]:
                raise RuntimeError("exact-membership-after-resume-unproven")
            wait_deadline = time.monotonic() + max(0.0, timeout)
            try:
                event["exit_code"] = proc.wait(
                    timeout=max(0.0, wait_deadline - time.monotonic())
                )
            except subprocess.TimeoutExpired as exc:
                event["outcome"] = "timeout"
                event["outer_timeout_seconds"] = timeout
                record_failure(event, "canonical-runner-wait", exc)
                result = 124
            else:
                event["outcome"] = "completed" if event["exit_code"] == 0 else "failed"
                observe_owned_job_empty(job, process_ref, wait_deadline, event)
                if event["exit_code"] != 0:
                    record_exit_failure(event, event["exit_code"])
                    result = 1
                else:
                    result = 0
    except BaseException as exc:
        logger.exception("canonical runner custody boundary failed")
        if event["outcome"] == "not_started":
            event["outcome"] = "failed"
        record_failure(event, "canonical-runner", exc)
        result = 1
    finally:
        if job is not None and not close_owned_job(job, event):
            result = 1
        if proc is not None and not reap_exact_process(proc, event):
            result = 1
        event["stdout_present"] = stdout_path.is_file()
        event["stdout_sha256"] = digest(stdout_path)
        event["junit_present"] = junit_path.is_file()
        event["junit_sha256"] = digest(junit_path)
        event["cleanup_verified"] = bool(
            event["job_handle_closed"]
            and event["process_handle_closed"]
            and not event["cleanup_errors"]
            and not event["forced_cleanup"]
        )
        event["success"] = bool(
            result == 0
            and event["outcome"] == "completed"
            and event.get("exit_code") == 0
            and event.get("exact_process_handle_member_after_resume") is True
            and event.get("exact_process_handle_member_after_exit") is True
            and event.get("native_runner_tree_empty_after_exit") is True
            and event["cleanup_verified"]
        )
        if not event["success"]:
            result = 1
        event["finished_at_utc"] = utc_now()
        write_json(receipt, event)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--stdout", required=True, type=Path)
    parser.add_argument("--junit", required=True, type=Path)
    parser.add_argument("--outer-timeout", type=float, default=OUTER_TIMEOUT_SECONDS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = list(args.command)
    if command[:1] == ["--"]:
        command = command[1:]
    return run(command, args.receipt, args.stdout, args.junit, args.outer_timeout)


if __name__ == "__main__":
    raise SystemExit(main())
