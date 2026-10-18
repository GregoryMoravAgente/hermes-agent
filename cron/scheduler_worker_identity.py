"""Strict identity receipts for restart-safe Cron external workers.

The scheduler owns the durable execution row and the parent owns the launcher Job.  On Windows
the launcher returned by ``spawn_server`` can be a venv redirector, while the worker that adopts
the row publishes the ACK.  This module binds that ACK to a retained native process handle and
the already-owned Job without turning a PID census or a later Job membership query into proof.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
import json
import secrets
import time
from pathlib import Path
from typing import Any


class WorkerIdentityUnavailable(RuntimeError):
    """The worker could not publish or retain a complete native identity receipt."""

    def __init__(self, code: str, *, retained_handle: Any | None = None):
        self.code = code
        self.retained_handle = retained_handle
        super().__init__(code)


@dataclass(frozen=True)
class WindowsWorkerReceipt:
    """The native worker identity retained from one authenticated ACK."""

    pid: int
    process_started_at: int
    creation_filetime: int
    process_handle: Any


_GENERATION_KEYS = {"process_instance", "authority_epoch", "close_generation"}
_ACCEPTANCE_KEYS = {
    "decision",
    "handoff_nonce",
    "execution_id",
    "pid",
    "process_started_at",
    "process_creation_filetime",
    "dispatch_mode",
    "scope_unit",
    "scope_control_group",
    "custody_generation",
}
_ACK_REQUIRED_KEYS = {
    "execution_id",
    "pid",
    "process_started_at",
    "dispatch_mode",
    "scope_unit",
    "scope_control_group",
    "custody_generation",
}
_ACK_OPTIONAL_KEYS = {"process_creation_filetime"}
_DISPATCH_MODES = {"direct", "degraded", "scoped"}


def _valid_generation(value: Any) -> bool:
    return (
        type(value) is dict
        and set(value) == _GENERATION_KEYS
        and type(value["process_instance"]) is str
        and bool(value["process_instance"])
        and type(value["authority_epoch"]) is int
        and value["authority_epoch"] >= 1
        and type(value["close_generation"]) is int
        and value["close_generation"] >= 0
    )


def _valid_worker_identity(
    value: Any, *, require_nonce: bool, allow_decision: bool = False,
) -> bool:
    if type(value) is not dict:
        return False
    required_keys = set(_ACK_REQUIRED_KEYS)
    if require_nonce:
        required_keys.add("handoff_nonce")
    if allow_decision:
        required_keys.add("decision")
    allowed_keys = required_keys | _ACK_OPTIONAL_KEYS
    if not required_keys.issubset(value) or not set(value).issubset(allowed_keys):
        return False
    nonce = value.get("handoff_nonce")
    process_creation_filetime = value.get("process_creation_filetime")
    dispatch_mode = value.get("dispatch_mode")
    scope_unit = value.get("scope_unit")
    scope_control_group = value.get("scope_control_group")
    if (
        (require_nonce and (type(nonce) is not str or not nonce))
        or (not require_nonce and nonce is not None and (type(nonce) is not str or not nonce))
        or type(value.get("execution_id")) is not str
        or not value["execution_id"]
        or type(value.get("pid")) is not int
        or value["pid"] <= 0
        or type(value.get("process_started_at")) is not int
        or value["process_started_at"] <= 0
        or (process_creation_filetime is not None
            and (type(process_creation_filetime) is not int or process_creation_filetime <= 0))
        or type(dispatch_mode) is not str
        or dispatch_mode not in _DISPATCH_MODES
    ):
        return False
    if dispatch_mode == "scoped":
        return (
            type(scope_unit) is str
            and scope_unit.endswith(".scope")
            and type(scope_control_group) is str
            and scope_control_group.startswith("/")
        )
    return scope_unit is None and scope_control_group is None


def worker_custody_generation(handle: Any) -> dict[str, Any] | None:
    """Copy the already-issued coordinator generation into the handoff receipt.

    The child never chooses these values.  A ``ChildReservation`` is the only source for a
    managed worker handoff; an un-managed POSIX launch deliberately has no coordinator
    generation and is checked by its durable execution/Popen identity instead.
    """
    if handle is None:
        return None
    values = {
        "process_instance": getattr(handle, "process_instance", None),
        "authority_epoch": getattr(handle, "authority_epoch", None),
        "close_generation": getattr(handle, "close_generation", None),
    }
    if (
        type(values["process_instance"]) is not str
        or not values["process_instance"]
        or type(values["authority_epoch"]) is not int
        or values["authority_epoch"] < 1
        or type(values["close_generation"]) is not int
        or values["close_generation"] < 0
    ):
        raise WorkerIdentityUnavailable("worker_generation_unavailable")
    return values


def _generation_matches(expected: Any, observed: Any) -> bool:
    if expected is None:
        return observed is None
    return _valid_generation(expected) and _valid_generation(observed) and expected == observed


def worker_ack_matches_generation(acknowledgement: Any, expected: Any) -> bool:
    """Check that a ready ACK repeats the parent-issued reservation generation exactly."""
    return (
        _valid_worker_identity(acknowledgement, require_nonce=True)
        and _generation_matches(expected, acknowledgement.get("custody_generation"))
    )


def new_handoff_nonce() -> str:
    """Create the one-use challenge that binds a ready ACK to its parent decision."""
    return secrets.token_urlsafe(32)


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp{os.getpid()}")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(value, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def write_worker_acceptance(
    accept_path: Path, acknowledgement: dict[str, Any], *,
    expected_generation: Any = None, decision: str = "accepted",
) -> None:
    """Publish the parent's authenticated decision for one ready worker.

    The decision repeats the complete identity receipt.  A worker therefore cannot turn a stale
    or foreign file into permission to run merely by knowing its execution id.
    """
    if decision != "accepted" or not isinstance(acknowledgement, dict):
        raise WorkerIdentityUnavailable("worker_acceptance_refused")
    nonce = acknowledgement.get("handoff_nonce")
    execution_id = acknowledgement.get("execution_id")
    pid = acknowledgement.get("pid")
    started_at = acknowledgement.get("process_started_at")
    if (
        not _valid_worker_identity(acknowledgement, require_nonce=True)
        or not _generation_matches(expected_generation, acknowledgement.get("custody_generation"))
    ):
        raise WorkerIdentityUnavailable("worker_acceptance_identity_mismatch")
    acceptance = {
        "decision": decision,
        "handoff_nonce": nonce,
        "execution_id": execution_id,
        "pid": pid,
        "process_started_at": started_at,
        "process_creation_filetime": acknowledgement.get("process_creation_filetime"),
        "dispatch_mode": acknowledgement.get("dispatch_mode"),
        "scope_unit": acknowledgement.get("scope_unit"),
        "scope_control_group": acknowledgement.get("scope_control_group"),
        "custody_generation": expected_generation,
    }
    _atomic_json_write(accept_path, acceptance)


def wait_for_worker_acceptance(
    accept_path: Path, acknowledgement: dict[str, Any], *, timeout: float,
) -> bool:
    """Wait for one exact parent decision, returning false on timeout or mismatch."""
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        return False
    if not _valid_worker_identity(acknowledgement, require_nonce=True):
        return False
    expected = {
        "handoff_nonce": acknowledgement.get("handoff_nonce"),
        "execution_id": acknowledgement.get("execution_id"),
        "pid": acknowledgement.get("pid"),
        "process_started_at": acknowledgement.get("process_started_at"),
        "process_creation_filetime": acknowledgement.get("process_creation_filetime"),
        "dispatch_mode": acknowledgement.get("dispatch_mode"),
        "scope_unit": acknowledgement.get("scope_unit"),
        "scope_control_group": acknowledgement.get("scope_control_group"),
        "custody_generation": acknowledgement.get("custody_generation"),
    }
    deadline = time.monotonic() + float(timeout)
    while time.monotonic() < deadline:
        if accept_path.exists():
            try:
                decision = json.loads(accept_path.read_text(encoding="utf-8-sig"))
            except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
                return False
            finally:
                try:
                    accept_path.unlink(missing_ok=True)
                except OSError:
                    pass
            if (
                type(decision) is not dict
                or set(decision) != _ACCEPTANCE_KEYS
                or decision.get("decision") != "accepted"
                or not _valid_worker_identity(
                    decision, require_nonce=True, allow_decision=True)
            ):
                return False
            return (
                all(type(decision.get(key)) is type(value) for key, value in expected.items()
                    if key != "custody_generation")
                and _generation_matches(
                    expected["custody_generation"], decision["custody_generation"])
                and all(decision[key] == value for key, value in expected.items())
            )
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    return False


def retained_windows_worker_matches_ack(
    acknowledgement: Any, record: Any, native_job: Any, *,
    handle: Any, retained_pid: Any, retained_started_at: Any,
    retained_creation_filetime: Any, job_closed: bool,
) -> bool:
    """Revalidate one retained native receipt without reopening its PID."""
    if (
        job_closed or native_job is None or handle is None
        or not isinstance(acknowledgement, dict)
        or getattr(handle, "is_open", None) is not True
        or not isinstance(record, dict)
        or type(retained_pid) is not int
        or type(retained_started_at) is not int
        or type(retained_creation_filetime) is not int
        or retained_creation_filetime <= 0
        or type(acknowledgement.get("pid")) is not int
        or type(acknowledgement.get("process_started_at")) is not int
        or type(acknowledgement.get("process_creation_filetime")) is not int
        or acknowledgement.get("pid") != retained_pid
        or acknowledgement.get("process_started_at") != retained_started_at
        or acknowledgement.get("process_creation_filetime") != retained_creation_filetime
        or record.get("pid") != retained_pid
        or record.get("process_started_at") != retained_started_at
    ):
        return False
    try:
        return native_job.contains_process_handle(handle) is True
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def reuse_or_capture_windows_worker_receipt(
    acknowledgement: Any, record: Any, native_job: Any, *,
    retained_handle: Any, retained_pid: Any, retained_started_at: Any,
    retained_creation_filetime: Any, job_closed: bool, already_active: bool,
) -> tuple[WindowsWorkerReceipt | None, bool]:
    """Reuse an exact retained receipt or capture it once from the owned Job."""
    if retained_handle is not None:
        if not retained_windows_worker_matches_ack(
            acknowledgement, record, native_job,
            handle=retained_handle,
            retained_pid=retained_pid,
            retained_started_at=retained_started_at,
            retained_creation_filetime=retained_creation_filetime,
            job_closed=job_closed,
        ):
            raise WorkerIdentityUnavailable("worker_identity_already_retained")
        if already_active:
            return None, True
        return WindowsWorkerReceipt(
            pid=retained_pid,
            process_started_at=retained_started_at,
            creation_filetime=retained_creation_filetime,
            process_handle=retained_handle,
        ), False
    return capture_windows_worker_receipt(acknowledgement, native_job), False


def build_worker_ack_identity() -> dict[str, int]:
    """Return the platform-specific identity fields for a ready worker ACK.

    POSIX keeps its existing PID/start fingerprint contract.  Windows adds the creation
    FILETIME captured by the worker itself, before any job side effect, so the parent can open
    and retain the exact process object.  A missing native value refuses the ready ACK instead of
    publishing a partial identity.
    """
    if os.name != "nt":
        return {}
    try:
        from tui_gateway.windows_producer_identity import current_process_creation_filetime

        creation_filetime = current_process_creation_filetime()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
        raise WorkerIdentityUnavailable("worker_creation_identity_unavailable") from exc
    if type(creation_filetime) is not int or creation_filetime <= 0:
        raise WorkerIdentityUnavailable("worker_creation_identity_unavailable")
    return {"process_creation_filetime": creation_filetime}


def write_worker_ack(
    ack_path: Path, *, execution_id: str, process_started_at: Any,
    dispatch_mode: Any, scope_unit: Any, scope_control_group: Any,
    handoff_nonce: str | None = None, custody_generation: Any = None,
) -> dict[str, Any]:
    """Atomically publish the complete ready ACK after native identity capture."""
    worker_identity = build_worker_ack_identity()
    ack_tmp_path = ack_path.with_name(f"{ack_path.name}.tmp{os.getpid()}")
    fd = os.open(ack_tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as ack_file:
            acknowledgement = {
                "pid": os.getpid(),
                "process_started_at": process_started_at,
                "execution_id": execution_id,
                "dispatch_mode": dispatch_mode,
                "scope_unit": scope_unit,
                "scope_control_group": scope_control_group,
                "handoff_nonce": handoff_nonce,
                "custody_generation": custody_generation,
            }
            acknowledgement.update(worker_identity)
            json.dump(acknowledgement, ack_file)
            ack_file.flush()
            os.fsync(ack_file.fileno())
        os.replace(ack_tmp_path, ack_path)
    except BaseException:
        ack_tmp_path.unlink(missing_ok=True)
        raise
    return acknowledgement


def capture_windows_worker_receipt(
    acknowledgement: Any, native_job: Any,
) -> WindowsWorkerReceipt:
    """Capture a worker handle only when the ACK and exact owned Job agree.

    The handle is deliberately returned to the caller and remains open through terminal/reaper
    settlement.  Refusal exceptions carry any opened handle so the caller can retain it for
    retryable cleanup; a newly opened PID is never used as post-exit recovery proof.
    """
    if os.name != "nt":
        raise WorkerIdentityUnavailable("worker_native_identity_unavailable")
    if native_job is None:
        raise WorkerIdentityUnavailable("worker_job_membership_unproven")
    if not isinstance(acknowledgement, dict):
        raise WorkerIdentityUnavailable("worker_ack_identity_malformed")
    pid = acknowledgement.get("pid")
    process_started_at = acknowledgement.get("process_started_at")
    creation_filetime = acknowledgement.get("process_creation_filetime")
    if (
        type(pid) is not int or pid <= 0
        or type(process_started_at) is not int or process_started_at <= 0
        or type(creation_filetime) is not int or creation_filetime <= 0
    ):
        raise WorkerIdentityUnavailable("worker_ack_identity_malformed")

    from tui_gateway.windows_producer_identity import (
        ProducerIdentityError,
        WindowsProducerHandle,
    )

    try:
        process_handle = WindowsProducerHandle.open(pid)
    except ProducerIdentityError as exc:
        raise WorkerIdentityUnavailable(
            "worker_process_handle_unavailable", retained_handle=exc.retained_handle
        ) from None
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise WorkerIdentityUnavailable("worker_process_handle_unavailable") from None

    try:
        in_owned_job = native_job.contains_process_handle(process_handle)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise WorkerIdentityUnavailable(
            "worker_job_membership_unproven", retained_handle=process_handle
        ) from None
    if in_owned_job is not True:
        raise WorkerIdentityUnavailable(
            "worker_job_membership_unproven", retained_handle=process_handle
        )
    if process_handle.creation_filetime != creation_filetime:
        raise WorkerIdentityUnavailable(
            "worker_creation_identity_mismatch", retained_handle=process_handle
        )
    return WindowsWorkerReceipt(
        pid=pid,
        process_started_at=process_started_at,
        creation_filetime=creation_filetime,
        process_handle=process_handle,
    )


def worker_receipt_matches_record(
    receipt: WindowsWorkerReceipt, record: Any,
) -> bool:
    """Compare an authenticated native receipt with the durable row without coercion."""
    return (
        isinstance(receipt, WindowsWorkerReceipt)
        and isinstance(record, dict)
        and type(record.get("pid")) is int
        and type(record.get("process_started_at")) is int
        and receipt.pid == record.get("pid")
        and receipt.process_started_at == record.get("process_started_at")
    )
