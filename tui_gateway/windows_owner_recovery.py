"""Small, fail-closed recovery seam for the compute-host Windows owner Job."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import time
import uuid
from typing import Any, Callable

from hermes_cli.local_runtime.processes import (
    _WindowsJob,
    _WindowsJobAbsent,
    _valid_windows_job_name,
)

logger = logging.getLogger(__name__)

WINDOWS_OWNER_RECORD_SCHEMA = 2
WINDOWS_JOB_NAME_PREFIX = "Local\\AgenteDesktop.ComputeHost."

_WINDOWS_OWNER_RECORD_KEYS = frozenset({
    "owner_record_schema", "owner_kind", "host_pid", "boot_id", "host_generation",
    "job_name", "job_token", "build_sha", "started_at", "argv",
})
_UNOWNED_OWNER_RECORD_KEYS = frozenset({
    "owner_record_schema", "owner_kind", "host_pid", "boot_id", "host_generation",
    "build_sha", "started_at", "argv",
})
_LEGACY_UNOWNED_OWNER_RECORD_KEYS = frozenset({
    "owner_kind", "host_pid", "boot_id", "host_generation", "build_sha", "started_at", "argv",
})


class RegistryLease:
    """Hold the permanent registry lock inode across one owner lifecycle."""

    def __init__(self, handle: Any):
        self._handle = handle

    @classmethod
    def acquire(cls, registry_path: Path) -> RegistryLease | None:
        handle = None
        try:
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            handle = open(
                registry_path.with_name(registry_path.name + ".lock"),
                "a+",
                encoding="utf-8",
            )
            from gateway.status import _try_acquire_file_lock
            if not _try_acquire_file_lock(handle):
                handle.close()
                return None
            return cls(handle)
        except (OSError, ImportError):
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
            return None

    def release(self) -> bool:
        handle = self._handle
        if handle is None:
            return True
        # Closing the sole text handle releases the platform lock.  Do not call an
        # explicit unlock first: a later close failure must not expose an inode that
        # this lease still reports as retained.
        try:
            handle.close()
        except Exception:  # health: allow BLE001 -- lease close is opaque; retain the exact handle
            return False
        self._handle = None
        return True


def owner_record_key(
    *, owner_identity: tuple[int, str, int], owner_kind: str, owner_job_name: str | None,
) -> tuple[str, int, str, int, str | None]:
    return (
        owner_kind, owner_identity[0], owner_identity[1], owner_identity[2], owner_job_name)


def record_matches_key(record: object, key: tuple[str, int, str, int, str | None]) -> bool:
    if not isinstance(record, dict):
        return False
    return (
        record.get("owner_kind"), record.get("host_generation"), record.get("boot_id"),
        record.get("host_pid"), record.get("job_name")
    ) == key


def remove_registry_record(registry_path: Path, expected: object | None) -> bool:
    """Delete only the record held by this lifecycle lease."""
    key = expected
    if isinstance(key, dict):
        key = (
            key.get("owner_kind"), key.get("host_generation"), key.get("boot_id"),
            key.get("host_pid"), key.get("job_name"),
        )
    try:
        if not registry_path.exists():
            return key is None
        if not isinstance(key, tuple):
            return False
        current = json.loads(registry_path.read_text(encoding="utf-8"))
        if not record_matches_key(current, key):
            return False
        registry_path.unlink(missing_ok=True)
    except Exception:  # health: allow BLE001 -- registry read/unlink is opaque; retain the exact key and lease
        return False
    return True


def new_windows_job_name() -> str:
    """Create one per-spawn named Job token for source-backed recovery."""
    return f"{WINDOWS_JOB_NAME_PREFIX}{uuid.uuid4().hex}"


def _valid_owner_record_fields(record: dict[str, Any]) -> bool:
    if type(record.get("host_pid")) is not int or record["host_pid"] <= 0:
        return False
    if type(record.get("host_generation")) is not int or record["host_generation"] <= 0:
        return False
    if type(record.get("boot_id")) is not str or not record["boot_id"]:
        return False
    if type(record.get("build_sha")) is not str:
        return False
    started_at = record.get("started_at")
    if type(started_at) not in {int, float} or started_at <= 0:
        return False
    argv = record.get("argv")
    return isinstance(argv, list) and bool(argv) and all(type(value) is str and value for value in argv)


def _valid_owner_record_common(record: dict[str, Any], keys: frozenset[str]) -> bool:
    return (
        set(record) == keys
        and record.get("owner_record_schema") == WINDOWS_OWNER_RECORD_SCHEMA
        and _valid_owner_record_fields(record)
    )


def valid_windows_owner_record(record: object) -> bool:
    if not isinstance(record, dict) or not _valid_owner_record_common(record, _WINDOWS_OWNER_RECORD_KEYS):
        return False
    name = record.get("job_name")
    token = record.get("job_token")
    return (
        _valid_windows_job_name(name)
        and type(token) is str
        and token == name.rsplit(".", 1)[-1]
    )


def valid_unowned_owner_record(record: object) -> bool:
    if not isinstance(record, dict):
        return False
    if _valid_owner_record_common(record, _UNOWNED_OWNER_RECORD_KEYS):
        return True
    return (
        set(record) == _LEGACY_UNOWNED_OWNER_RECORD_KEYS
        and record.get("owner_kind") == "unowned"
        and _valid_owner_record_fields(record)
    )


def persist_owner_record(
    *, registry_path: Path, owner_identity: tuple[int, str, int], owner_kind: str,
    build_sha: str, argv: list[str], owner_job_name: str | None,
) -> None:
    """Atomically persist the validated host identity and exact Windows owner name."""
    if owner_kind not in {"windows_job", "unowned"}:
        raise RuntimeError("cannot persist an unknown compute-host owner kind")
    if owner_kind == "windows_job" and not _valid_windows_job_name(owner_job_name):
        raise RuntimeError("cannot persist an unnamed Windows Job owner")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "owner_record_schema": WINDOWS_OWNER_RECORD_SCHEMA,
        "host_pid": owner_identity[2], "boot_id": owner_identity[1],
        "host_generation": owner_identity[0], "owner_kind": owner_kind,
        "build_sha": build_sha, "started_at": time.time(),
        "argv": list(argv),
    }
    if owner_kind == "windows_job":
        assert owner_job_name is not None
        payload["job_name"] = owner_job_name
        payload["job_token"] = owner_job_name.rsplit(".", 1)[-1]
    tmp = registry_path.with_suffix(registry_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    tmp.replace(registry_path)


def owner_state(owner: Any | None) -> str:
    if owner is None:
        return "unavailable"
    try:
        empty = owner.empty()
    except Exception:  # health: allow BLE001 -- native owner query is opaque; any failure leaves custody unknown
        return "unknown"
    if empty is None:
        return "unknown"
    return "empty" if empty else "nonempty"


def try_close_owner(owner: Any | None) -> bool:
    if owner is None:
        return True
    try:
        owner.close()
    except Exception:  # health: allow BLE001 -- native Job close is opaque; failure retains owner custody
        logger.warning("compute host owner close failed; custody retained")
        return False
    return True


def release_owner(supervisor: Any) -> bool:
    """Release producer, Job, record, and lease while retaining retryable custody on failure."""
    with supervisor._lock:
        owner = supervisor._owner
        recovery_owner = supervisor._recovery_owner
        if recovery_owner is not None or not supervisor._try_close_producer_handle():
            return False
        if owner is None:
            if supervisor.registry_path.exists():
                return False
            if not supervisor._release_registry_lease():
                return False
            supervisor._registry_owner_key = None
            supervisor._registry_record_removed = False
            supervisor._producer_identity = None
            supervisor._host_resource_custody_known = False
            return True
        if not supervisor._owner_closed:
            if not try_close_owner(owner):
                return False
            supervisor._owner_closed = True
        if not supervisor._remove_registry(supervisor._registry_owner_key):
            return False
        if not supervisor._release_registry_lease():
            return False
        if supervisor._owner is owner:
            supervisor._owner = None
            supervisor._owner_job_name = None
            supervisor._owner_closed = False
            supervisor._owner_identity = None
            supervisor._producer_identity = None
            supervisor._registry_owner_key = None
            supervisor._registry_record_removed = False
            supervisor._host_resource_custody_known = False
        return True


_OWNER_RECORD_MISSING = object()


def _read_owner_record(registry_path: Path) -> object:
    try:
        return json.loads(registry_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _OWNER_RECORD_MISSING
    except Exception:  # health: allow BLE001 -- malformed/unreadable owner metadata cannot prove absence
        return None


def _reconcile_missing_record(
    recovery_owner: _WindowsJob | None,
) -> tuple[str, _WindowsJob | None]:
    if recovery_owner is None:
        return "none", None
    try:
        active = recovery_owner.active_processes()
    except Exception:  # health: allow BLE001 -- native recovery query cannot prove absence
        active = None
    if active != 0:
        return "owner-unavailable", recovery_owner
    try:
        recovery_owner.close()
    except Exception:  # health: allow BLE001 -- native recovery close failure retains the query handle
        return "owner-unavailable", recovery_owner
    return "none", None


def _reconcile_windows_record(
    *,
    data: dict[str, Any],
    recovery_owner: _WindowsJob | None,
    remove_registry: Callable[[object | None], bool],
    pid_alive: Callable[[int], bool],
) -> tuple[str, _WindowsJob | None]:
    if not valid_windows_owner_record(data):
        logger.warning("compute host Windows owner record is malformed; refusing duplicate start")
        return "owner-unavailable", recovery_owner
    pid = data["host_pid"]
    if pid_alive(pid):
        logger.warning("compute host Windows owner remains live; refusing duplicate start")
        return "owner-unavailable", recovery_owner
    job_name = data["job_name"]
    if recovery_owner is not None and recovery_owner.name != job_name:
        logger.warning("compute host recovery Job does not match registry; refusing duplicate start")
        return "owner-unavailable", recovery_owner
    if recovery_owner is None:
        try:
            recovery_owner = _WindowsJob.open_existing(job_name)
        except _WindowsJobAbsent:
            if not remove_registry(data):
                return "owner-unavailable", None
            return "owner-unavailable-absent", None
        except Exception:  # health: allow BLE001 -- inaccessible Job cannot prove absence
            logger.warning("compute host recovery Job could not be queried; custody retained")
            return "owner-unavailable", None
    try:
        active = recovery_owner.active_processes()
    except Exception:  # health: allow BLE001 -- native recovery query cannot prove absence
        active = None
    if active is None or active != 0:
        logger.warning("compute host recovery Job is nonempty or unknown; refusing duplicate start")
        return "owner-unavailable", recovery_owner
    try:
        recovery_owner.close()
    except Exception:  # health: allow BLE001 -- recovery Job close failure retains exact custody
        logger.warning("compute host recovery Job close failed; custody retained")
        return "owner-unavailable", recovery_owner
    if not remove_registry(data):
        return "owner-unavailable", None
    return "owner-unavailable-absent", None


def _reconcile_unowned_record(
    *,
    data: dict[str, Any],
    recovery_owner: _WindowsJob | None,
    remove_registry: Callable[[object | None], bool],
    pid_alive: Callable[[int], bool],
    pid_matches_compute_host: Callable[[int], bool],
    terminate_pid: Callable[..., Any],
    shutdown_timeout_secs: float,
) -> tuple[str, _WindowsJob | None]:
    if data.get("owner_kind") != "unowned" or not valid_unowned_owner_record(data):
        logger.warning("compute host owner record is unknown or malformed; refusing duplicate start")
        return "owner-unavailable", recovery_owner
    pid = data["host_pid"]
    if pid <= 0 or not pid_alive(pid):
        outcome = "not-running"
    elif not pid_matches_compute_host(pid):
        outcome = "pid-reuse-ignored"
    else:
        terminate_pid(pid, timeout=shutdown_timeout_secs)
        outcome = "terminated"
    if not remove_registry(data):
        return "owner-unavailable", recovery_owner
    return outcome, recovery_owner


def reconcile_startup_orphan(
    *,
    registry_path: Path,
    recovery_owner: _WindowsJob | None,
    remove_registry: Callable[[object | None], bool],
    pid_alive: Callable[[int], bool],
    pid_matches_compute_host: Callable[[int], bool],
    terminate_pid: Callable[..., Any],
    shutdown_timeout_secs: float,
) -> tuple[str, _WindowsJob | None]:
    """Return an outcome and the query handle that must remain retained.

    The named Job is queried only for the exact validated record.  A nonempty
    or unknown Job remains held by the caller while duplicate startup is refused;
    the helper never assigns, adopts, or terminates a recovered Job.
    """
    data = _read_owner_record(registry_path)
    if data is _OWNER_RECORD_MISSING:
        return _reconcile_missing_record(recovery_owner)
    if not isinstance(data, dict):
        return "owner-unavailable", recovery_owner
    owner_kind = data.get("owner_kind")
    if recovery_owner is not None and owner_kind != "windows_job":
        logger.warning("compute host recovery Job does not match the registry owner kind")
        return "owner-unavailable", recovery_owner
    if owner_kind == "windows_job":
        return _reconcile_windows_record(
            data=data, recovery_owner=recovery_owner, remove_registry=remove_registry,
            pid_alive=pid_alive)
    return _reconcile_unowned_record(
        data=data, recovery_owner=recovery_owner, remove_registry=remove_registry,
        pid_alive=pid_alive, pid_matches_compute_host=pid_matches_compute_host,
        terminate_pid=terminate_pid, shutdown_timeout_secs=shutdown_timeout_secs)
