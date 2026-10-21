"""Typed Windows producer identity and handle lifetime for ComputeHost startup."""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

PRODUCER_STARTUP_SCHEMA_POSIX = 1
PRODUCER_STARTUP_SCHEMA_WINDOWS = 2
PRODUCER_STARTUP_TYPE = "producer.startup"
PRODUCER_STARTUP_KEYS_POSIX = frozenset({"type", "schema", "host_pid", "host_boot_id"})
PRODUCER_STARTUP_KEYS_WINDOWS = frozenset({
    "type", "schema", "host_pid", "host_boot_id", "host_creation_filetime",
})
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
SYNCHRONIZE = 0x00100000
_MAX_FILETIME = (1 << 64) - 1


class ProducerIdentityError(RuntimeError):
    """A child startup record could not bind to one stable OS process."""

    def __init__(self, message: str, *, retained_handle: Any | None = None):
        super().__init__(message)
        self.retained_handle = retained_handle


def _filetime_value(value: wintypes.FILETIME) -> int:
    return (int(value.dwHighDateTime) << 32) | int(value.dwLowDateTime)


def _bind_process_api(api: Any) -> None:
    api.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    api.OpenProcess.restype = wintypes.HANDLE
    api.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
    ]
    api.GetProcessTimes.restype = wintypes.BOOL
    api.CloseHandle.argtypes = [wintypes.HANDLE]
    api.CloseHandle.restype = wintypes.BOOL


def _creation_filetime(api: Any, handle: wintypes.HANDLE) -> int:
    created = wintypes.FILETIME()
    exited = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    user = wintypes.FILETIME()
    if not api.GetProcessTimes(
        handle, ctypes.byref(created), ctypes.byref(exited),
        ctypes.byref(kernel), ctypes.byref(user),
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    value = _filetime_value(created)
    if value <= 0 or value > _MAX_FILETIME:
        raise ProducerIdentityError("invalid producer creation time")
    return value


def current_process_creation_filetime() -> int:
    """Read the current child process creation FILETIME before public hello."""
    if os.name != "nt":
        raise ProducerIdentityError("Windows producer identity requested on a non-Windows host")
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.GetCurrentProcess.argtypes = []
    api.GetCurrentProcess.restype = wintypes.HANDLE
    _bind_process_api(api)
    return _creation_filetime(api, api.GetCurrentProcess())


class WindowsProducerHandle:
    """A stable query/synchronization handle for one exact producer process."""

    def __init__(self, api: Any, handle: wintypes.HANDLE, pid: int, creation_filetime: int):
        self._api = api
        self._handle = handle
        self.pid = pid
        self.creation_filetime = creation_filetime
        self._lock = threading.Lock()

    @classmethod
    def open(cls, pid: int) -> WindowsProducerHandle:
        if os.name != "nt" or type(pid) is not int or pid <= 0:
            raise ProducerIdentityError("invalid Windows producer PID")
        api = ctypes.WinDLL("kernel32", use_last_error=True)
        _bind_process_api(api)
        access = PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE
        handle = api.OpenProcess(access, False, pid)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            creation_filetime = _creation_filetime(api, handle)
        except BaseException as exc:  # health: preserve native handle if cleanup itself fails
            if api.CloseHandle(handle):
                raise
            retained = cls(api, handle, pid, 0)
            raise ProducerIdentityError(
                "producer handle close failed", retained_handle=retained
            ) from exc
        return cls(api, handle, pid, creation_filetime)

    @property
    def raw_handle(self) -> wintypes.HANDLE | None:
        with self._lock:
            return self._handle

    @property
    def is_open(self) -> bool:
        return self.raw_handle is not None

    def close(self) -> None:
        """Close without clearing the handle when native close fails."""
        with self._lock:
            if self._handle is None:
                return
            if not self._api.CloseHandle(self._handle):
                raise ctypes.WinError(ctypes.get_last_error())
            self._handle = None


@dataclass
class StartupHandleCustody:
    """A native candidate retained until its exact startup generation can retry close."""

    proc: Any
    host_generation: int
    handle: WindowsProducerHandle


def _close_startup_candidate(
    supervisor: Any, candidate: WindowsProducerHandle | None, *,
    proc: Any, host_generation: int,
) -> bool:
    if candidate is None:
        return True
    try:
        candidate.close()
    except Exception:  # health: allow BLE001 -- native candidate close is opaque; retain typed custody without values
        with supervisor._hello_lock:
            supervisor._startup_orphan_handles[host_generation] = StartupHandleCustody(
                proc=proc, host_generation=host_generation, handle=candidate)
        return False
    return True


def cancel_producer_startup(supervisor: Any, *, proc: Any, host_generation: int) -> None:
    """Cancel one startup generation before its owner/lease cleanup begins."""
    with supervisor._hello_lock:
        if supervisor._proc is not proc or supervisor._host_generation != host_generation:
            return
        supervisor._startup_cancelled_generations.add(host_generation)
        if not supervisor._startup_event.is_set():
            supervisor._startup_error = "startup:cancelled"
            supervisor._startup_event.set()


def build_producer_startup_record(*, pid: int, boot_id: str) -> dict[str, object]:
    """Build the exact private startup record emitted before public hello."""
    record: dict[str, object] = {
        "type": PRODUCER_STARTUP_TYPE,
        "schema": PRODUCER_STARTUP_SCHEMA_POSIX,
        "host_pid": pid,
        "host_boot_id": boot_id,
    }
    if os.name == "nt":
        record["schema"] = PRODUCER_STARTUP_SCHEMA_WINDOWS
        record["host_creation_filetime"] = current_process_creation_filetime()
    return record


def valid_producer_startup_record(record: object, *, is_windows: bool) -> bool:
    """Validate platform-specific startup fields without coercion."""
    if not isinstance(record, dict):
        return False
    keys = PRODUCER_STARTUP_KEYS_WINDOWS if is_windows else PRODUCER_STARTUP_KEYS_POSIX
    schema = PRODUCER_STARTUP_SCHEMA_WINDOWS if is_windows else PRODUCER_STARTUP_SCHEMA_POSIX
    if set(record) != keys or record.get("type") != PRODUCER_STARTUP_TYPE:
        return False
    if type(record.get("schema")) is not int or record["schema"] != schema:
        return False
    if type(record.get("host_pid")) is not int or record["host_pid"] <= 0:
        return False
    if type(record.get("host_boot_id")) is not str or not record["host_boot_id"]:
        return False
    if not is_windows:
        return True
    creation = record.get("host_creation_filetime")
    return type(creation) is int and 0 < creation <= _MAX_FILETIME


def bind_producer_identity(
    *, process: WindowsProducerHandle, expected_pid: int,
    expected_creation_filetime: int, in_owned_job: bool | None,
) -> bool:
    """Pure acceptance predicate used by the native opener and portable tests."""
    return (
        process.is_open
        and process.pid == expected_pid
        and process.creation_filetime == expected_creation_filetime
        and in_owned_job is True
    )


def _read_launcher_pid(proc: Any) -> tuple[int, str]:
    """Query the fallible launcher identity outside the startup CAS lock."""
    try:
        launcher_pid = int(proc.pid or 0)
    except Exception:  # health: allow BLE001 -- launcher PID query is opaque; refuse without exception values
        return 0, "startup:launcher_pid_query_failed"
    error = "" if launcher_pid > 0 else "startup:missing_launcher_pid"
    return launcher_pid, error


def accept_producer_startup(
    supervisor: Any, record: dict[str, Any], *, proc: Any, host_generation: int,
) -> bool:
    """CAS one startup receipt and retain the exact Windows process handle.

    The entry reservation and final publication use ``_hello_lock``. Native
    process queries run outside it, so a timed-out parent can cancel the exact
    generation without making the reader wait on the lifecycle lock. A candidate
    from a stale or cancelled reservation is closed before the reservation clears.
    """
    token = (proc, host_generation)
    with supervisor._hello_lock:
        if supervisor._proc is not proc or supervisor._host_generation != host_generation:
            return False
        if host_generation in supervisor._startup_cancelled_generations:
            supervisor._startup_error = "startup:cancelled"
            supervisor._startup_event.set()
            return False
        if supervisor._startup_seen:
            supervisor._startup_error = "startup:duplicate"
            supervisor._startup_event.set()
            return False
        supervisor._startup_seen = True
        if not valid_producer_startup_record(record, is_windows=os.name == "nt"):
            supervisor._startup_error = "startup:invalid_record"
            supervisor._startup_event.set()
            return False
        actual_pid = int(record["host_pid"])
        boot_id = str(record["host_boot_id"])
        owner = supervisor._owner
        # Reserve the generation before touching the fallible Popen/native
        # identity surface.  Parent timeout/cancellation must be able to
        # acquire this lock while a launcher PID read is blocked.
        supervisor._startup_binding = token

    candidate: WindowsProducerHandle | None = None
    accepted = False
    startup_error = ""
    launcher_pid, startup_error = _read_launcher_pid(proc)
    if not startup_error and os.name == "nt":
        contains_pid = getattr(owner, "contains_pid", None)
        launcher_member = producer_member = None
        if not callable(contains_pid):
            startup_error = "startup:job_membership_unavailable"
        else:
            try:
                launcher_member = contains_pid(launcher_pid)
                producer_member = contains_pid(actual_pid)
                candidate = WindowsProducerHandle.open(actual_pid)
                in_owned_job = owner.contains_process_handle(candidate)
                accepted = bind_producer_identity(
                    process=candidate,
                    expected_pid=actual_pid,
                    expected_creation_filetime=int(record["host_creation_filetime"]),
                    in_owned_job=in_owned_job,
                )
            except ProducerIdentityError as exc:
                candidate = exc.retained_handle
                startup_error = "startup:producer_handle_unproven"
            except Exception:  # health: allow BLE001 -- native startup queries are opaque; refuse without exception values
                startup_error = "startup:native_query_failed"
            if launcher_member is not True or producer_member is not True:
                startup_error = "startup:job_membership_unproven"
            elif not accepted and not startup_error:
                startup_error = "startup:producer_handle_unproven"
    elif not startup_error and actual_pid != launcher_pid:
        startup_error = "startup:pid_mismatch"
    elif not startup_error:
        accepted = True

    with supervisor._hello_lock:
        current = supervisor._proc is proc and supervisor._host_generation == host_generation
        cancelled = host_generation in supervisor._startup_cancelled_generations
        if cancelled:
            startup_error = "startup:cancelled"
        if current and accepted and not startup_error and not cancelled:
            supervisor._producer_process_handle = candidate
            supervisor._producer_creation_filetime = (
                int(record["host_creation_filetime"])
                if candidate is not None else None
            )
            supervisor._startup_identity = (actual_pid, boot_id)
            supervisor._startup_error = ""
            supervisor._startup_binding = None
            supervisor._startup_event.set()
            return True
        if current and not startup_error:
            startup_error = "startup:stale_generation"
        if current:
            supervisor._startup_error = startup_error or "startup:rejected"

    closed = _close_startup_candidate(
        supervisor, candidate, proc=proc, host_generation=host_generation)
    with supervisor._hello_lock:
        if supervisor._startup_binding == token:
            supervisor._startup_binding = None
        if current and not supervisor._startup_event.is_set():
            supervisor._startup_error = (
                "startup:handle_close_failed" if not closed
                else startup_error or "startup:rejected"
            )
            supervisor._startup_event.set()
    return False


def try_close_producer_handle(supervisor: Any) -> bool:
    """Close only after owner/ACK ordering; retain the handle on failure."""
    with supervisor._hello_lock:
        if supervisor._startup_binding is not None:
            # A release/close request retires the pending generation.  Mark
            # that cancellation while holding the same CAS lock so a blocked
            # PID/native query cannot publish after custody cleanup begins.
            _, host_generation = supervisor._startup_binding
            supervisor._startup_cancelled_generations.add(host_generation)
            if not supervisor._startup_event.is_set():
                supervisor._startup_error = "startup:cancelled"
                supervisor._startup_event.set()
            return False
        for generation, custody in list(supervisor._startup_orphan_handles.items()):
            try:
                custody.handle.close()
            except Exception:  # health: allow BLE001 -- generation-bound native close is opaque; custody stays retained
                logger.warning("compute host stale producer handle close failed; custody retained")
                return False
            supervisor._startup_orphan_handles.pop(generation, None)
        handle = supervisor._producer_process_handle
        if handle is None:
            return True
        try:
            handle.close()
        except Exception:  # health: allow BLE001 -- native handle close is opaque; custody stays retained
            logger.warning("compute host producer handle close failed; custody retained")
            return False
        if supervisor._producer_process_handle is handle:
            supervisor._producer_process_handle = None
            supervisor._producer_creation_filetime = None
        return True
