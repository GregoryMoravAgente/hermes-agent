"""Durable cross-process gateway lifecycle handoff.

The lifecycle sentinel is the only durable handoff authority.  This module does not mint a second
lock or an attach token: the profile locks, the runtime epoch rows, and the authenticated control
socket remain the authorities.  The journal merely records the source-owned witnesses needed to
join those authorities across a predecessor process boundary.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping
import uuid

logger = logging.getLogger("gateway.lifecycle_handoff")

HANDOFF_VERSION = 1
_ACTIVE_STATES = frozenset({"prepared", "released", "unknown"})
_KNOWN_STATES = _ACTIVE_STATES | frozenset({"ready", "historical"})
_JOURNAL_READ_FAILURES = frozenset({"malformed", "unreadable"})


def _digest(value: Any) -> str:
    """Hash canonical JSON while keeping journal digests independent of dict insertion order."""
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_record(home: Path | None = None) -> dict[str, Any] | None:
    from gateway.lifecycle_ledger import _read_json, get_lifecycle_sentinel_path

    return _read_json(get_lifecycle_sentinel_path(home))


def _read_record_status(home: Path | None = None) -> tuple[dict[str, Any] | None, str]:
    from gateway.lifecycle_ledger import _read_json_status, get_lifecycle_sentinel_path

    record, status = _read_json_status(get_lifecycle_sentinel_path(home))
    if status == "ok" and isinstance(record, dict):
        failure = record.get("lifecycle_record_read_failure")
        if failure is not None:
            status = failure if isinstance(failure, str) and failure in _JOURNAL_READ_FAILURES else "malformed"
            return None, status
    return record, status


def _read_handoff_record(
    home: Path | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    record, status = _read_record_status(home)
    if status != "ok":
        return record, None, status
    if record is None:
        return None, None, "absent"
    if "handoff" not in record:
        return record, None, "ordinary"
    handoff = record.get("handoff")
    if not isinstance(handoff, dict):
        return record, None, "malformed_handoff"
    return record, handoff, "handoff"


def _write_record(home: Path, record: Mapping[str, Any]) -> bool:
    """Atomically write one journal record and report the actual write result."""
    try:
        from hermes_constants import mkdir_under_hermes_home
        from utils import atomic_json_write

        path = __import__("gateway.lifecycle_ledger", fromlist=["get_lifecycle_sentinel_path"])
        sentinel = path.get_lifecycle_sentinel_path(home)
        mkdir_under_hermes_home(sentinel.parent)
        atomic_json_write(sentinel, dict(record), indent=None)
        return True
    except BaseException:
        logger.warning("Could not persist the gateway lifecycle handoff", exc_info=True)
        return False


def _birth(pid: int) -> float | None:
    try:
        from hermes_cli.process_identity import _process_create_time

        return _process_create_time(pid)
    except BaseException:  # health: allow BLE001 -- process birth is an opaque native identity query
        return None


def _process_snapshot() -> dict[str, Any] | None:
    try:
        from gateway.status import _build_pid_record

        record = _build_pid_record()
        pid = record.get("pid")
        birth = _birth(pid) if type(pid) is int and pid > 0 else None
        if type(pid) is not int or pid <= 0 or birth is None:
            return None
        return {
            "pid": pid,
            "start_time": record.get("start_time"),
            "birth": birth,
            "kind": record.get("kind"),
            "hermes_home": record.get("hermes_home"),
        }
    except BaseException:  # health: allow BLE001 -- unreadable process identity stays UNKNOWN
        return None


def _authority_epoch(runner: Any, home: Path, descriptor: Mapping[str, Any]) -> int | None:
    try:
        registry = getattr(runner, "session_authorities", None)
        authority = registry.for_home(home) if registry is not None else None
        epoch = getattr(authority, "epoch", None)
        if type(epoch) is int and epoch > 0:
            return epoch
        epoch = descriptor.get("authority_epoch")
        return epoch if type(epoch) is int and epoch > 0 else None
    except BaseException:  # health: allow BLE001 -- authority epoch read failure stays UNKNOWN
        return None


def _profile_bindings(runner: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Read the exact current reservations and their predecessor records."""
    try:
        from gateway.run_runtime import reserved_profile_homes
        from gateway.runtime_ownership import process_ownership

        descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
        process = _process_snapshot()
        if process is None:
            return None
        current: list[dict[str, Any]] = []
        predecessors: list[dict[str, Any]] = []
        for name, home_value in reserved_profile_homes(runner):
            home = Path(home_value).resolve()
            evidence = process_ownership.reservation_evidence(home)
            if not isinstance(evidence, dict):
                return None
            record = evidence.get("record")
            owner_epoch = evidence.get("owner_epoch")
            authority_epoch = _authority_epoch(runner, home, descriptor)
            if (
                not isinstance(record, dict)
                or type(owner_epoch) is not str
                or not owner_epoch
                or type(authority_epoch) is not int
                or authority_epoch < 1
                or record.get("pid") != process["pid"]
            ):
                return None
            binding = {
                "name": str(name),
                "home": str(home),
                "owner_epoch": owner_epoch,
                "authority_epoch": authority_epoch,
                "pid": process["pid"],
                "start_time": record.get("start_time"),
                "birth": process["birth"],
                "kind": record.get("kind"),
            }
            if not binding["kind"] or binding["start_time"] is None:
                return None
            current.append(binding)
            prior = process_ownership.predecessor_record(home)
            predecessors.append({"home": str(home), "record": prior})
        return current, predecessors
    except BaseException:
        logger.warning("Could not read source-owned profile handoff bindings", exc_info=True)
        return None


def _proofs(native_receipt: Any) -> tuple[dict[str, Any], str, str] | None:
    try:
        from hermes_state_registry import SessionDbCloseReceipt

        db = native_receipt.db_close_receipt
        if not isinstance(db, SessionDbCloseReceipt) or db.verified is not True:
            return None
        if native_receipt.ready() is not True:
            return None
        close = {
            "source": db.source,
            "attempted": db.attempted,
            "succeeded": db.succeeded,
            "failed": db.failed,
            "pending": db.pending,
        }
        native = {
            "action": native_receipt.action,
            "settled": native_receipt.settled is True,
            "shutdown_event": native_receipt.shutdown_event.is_set() is True,
            "db_close": close,
        }
        return close, _digest(close), _digest(native)
    except BaseException:  # health: allow BLE001 -- checked close proof failure stays UNKNOWN
        return None


def _lineage_payload(
    *, process: Mapping[str, Any], descriptor: Mapping[str, Any], profiles: list[dict[str, Any]],
    predecessors: list[dict[str, Any]], close_digest: str, native_digest: str,
) -> dict[str, Any]:
    return {
        "process_home": process.get("hermes_home"),
        "profiles": [
            {
                "home": item["home"], "owner_epoch": item["owner_epoch"],
                "authority_epoch": item["authority_epoch"], "pid": item["pid"],
                "start_time": item["start_time"], "birth": item["birth"],
            }
            for item in sorted(profiles, key=lambda value: value["home"])
        ],
        # Keep the acquisition-time predecessor witnesses inside the signed lineage.  The
        # top-level copy remains convenient for recovery diagnostics, but it is not an
        # authority unless it is byte-for-byte the same canonical witness set that was
        # covered by this digest.
        "predecessor_records": predecessors,
        "predecessor_instance": descriptor.get("instance_id"),
        "predecessor_authority_epoch": descriptor.get("authority_epoch"),
        "close_digest": close_digest,
        "native_digest": native_digest,
    }


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _canonical_home(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(Path(value).expanduser().resolve())
    except (OSError, RuntimeError, TypeError, ValueError):
        return None


def _validate_handoff_header(
    handoff: Mapping[str, Any],
) -> tuple[
    dict[str, Any], dict[str, Any], list[Any], dict[str, Any], list[Any], str,
] | None:
    if handoff.get("version") != HANDOFF_VERSION or not isinstance(handoff.get("handoff_id"), str):
        return None
    lineage = handoff.get("lineage")
    payload = handoff.get("lineage_payload")
    proofs = handoff.get("proofs")
    profiles = handoff.get("profiles")
    predecessor = handoff.get("predecessor")
    predecessor_records = handoff.get("predecessor_records")
    if (
        not _is_digest(lineage)
        or not isinstance(payload, dict)
        or not isinstance(proofs, dict)
        or not isinstance(profiles, list)
        or not isinstance(predecessor, dict)
        or not isinstance(predecessor_records, list)
        or _digest(payload) != lineage
    ):
        return None
    if set(payload) != {
        "process_home", "profiles", "predecessor_instance", "predecessor_authority_epoch",
        "predecessor_records", "close_digest", "native_digest",
    } or set(proofs) != {"close_digest", "native_digest"}:
        return None
    if not _is_digest(payload.get("close_digest")) or not _is_digest(payload.get("native_digest")):
        return None
    if payload["close_digest"] != proofs.get("close_digest"):
        return None
    if payload["native_digest"] != proofs.get("native_digest"):
        return None
    process_home = _canonical_home(payload.get("process_home"))
    if process_home is None or _canonical_home(predecessor.get("hermes_home")) != process_home:
        return None
    if (
        payload.get("predecessor_instance") != predecessor.get("instance_id")
        or payload.get("predecessor_authority_epoch") != predecessor.get("authority_epoch")
        or not isinstance(predecessor.get("instance_id"), str)
        or type(predecessor.get("authority_epoch")) is not int
        or predecessor["authority_epoch"] < 1
    ):
        return None
    required = {"pid", "birth", "start_time", "kind", "hermes_home", "instance_id", "authority_epoch"}
    if not required <= set(predecessor):
        return None
    if type(predecessor.get("pid")) is not int or predecessor["pid"] <= 0:
        return None
    if type(predecessor.get("birth")) not in (int, float) or predecessor["birth"] <= 0:
        return None
    if not isinstance(predecessor.get("kind"), str) or not predecessor["kind"]:
        return None
    if type(predecessor.get("start_time")) is not int:
        return None
    return payload, proofs, profiles, predecessor, predecessor_records, lineage


def _validate_handoff_profiles(
    profiles: list[Any], payload: Mapping[str, Any], predecessor: Mapping[str, Any],
) -> set[str] | None:
    if not profiles:
        return None
    homes: set[str] = set()
    projected_profiles: list[dict[str, Any]] = []
    for profile in profiles:
        if not isinstance(profile, dict):
            return None
        home = _canonical_home(profile.get("home"))
        if home is None or home in homes:
            return None
        homes.add(home)
        if not isinstance(profile.get("name"), str) or not profile["name"]:
            return None
        if not isinstance(profile.get("owner_epoch"), str) or not profile["owner_epoch"]:
            return None
        if type(profile.get("authority_epoch")) is not int or profile["authority_epoch"] < 1:
            return None
        if profile.get("pid") != predecessor.get("pid"):
            return None
        if not _same_start_time(profile.get("start_time"), predecessor.get("start_time")):
            return None
        if not _same_birth(profile.get("birth"), predecessor.get("birth")):
            return None
        if profile.get("kind") != predecessor.get("kind"):
            return None
        projected_profiles.append({
            "home": home,
            "owner_epoch": profile["owner_epoch"],
            "authority_epoch": profile["authority_epoch"],
            "pid": profile["pid"],
            "start_time": profile["start_time"],
            "birth": profile["birth"],
        })
    projected_profiles.sort(key=lambda value: value["home"])
    return homes if payload.get("profiles") == projected_profiles else None


def _canonical_predecessor_records(records: list[Any]) -> list[dict[str, Any]] | None:
    """Normalize acquisition witnesses without turning them into a second authority.

    A predecessor record is historical evidence from the exact profile reservation.  It may be
    ``None`` for a first owner, but when present its home must name the same canonical profile.
    Keeping the complete record in the signed payload also makes a forged/replaced historical
    witness invalidate the lineage instead of merely preserving the same home count.
    """
    if not isinstance(records, list):
        return None
    normalized: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            return None
        home = _canonical_home(item.get("home"))
        if home is None:
            return None
        record = item.get("record")
        if record is not None:
            if not isinstance(record, dict):
                return None
            record_home = _canonical_home(record.get("hermes_home"))
            if (
                record_home is None or record_home != home
                or type(record.get("pid")) is not int or record["pid"] <= 0
                or type(record.get("start_time")) is not int
                or not isinstance(record.get("kind"), str) or not record["kind"]
            ):
                return None
            record = dict(record)
            if record_home is not None:
                record["hermes_home"] = record_home
        normalized.append({"home": home, "record": record})
    return normalized


def _validate_handoff_records(
    predecessor_records: list[Any], homes: set[str], signed_records: Any,
) -> bool:
    signed = _canonical_predecessor_records(signed_records)
    observed = _canonical_predecessor_records(predecessor_records)
    if signed is None or observed is None or signed != observed:
        return False
    if len(predecessor_records) != len(homes):
        return False
    recorded_homes = {item["home"] for item in observed}
    return recorded_homes == homes


def _validate_handoff_attempts(attempts: Any, lineage: str) -> bool:
    if not isinstance(attempts, list):
        return False
    for attempt in attempts:
        if not isinstance(attempt, dict):
            return False
        if attempt.get("lineage") not in (None, lineage):
            return False
        epoch = attempt.get("authority_epoch")
        if type(epoch) is not int or epoch < 1:
            return False
    return True


def _validate_handoff_integrity(handoff: Mapping[str, Any]) -> bool:
    """Validate source-generated journal relations before trusting a handoff."""
    try:
        header = _validate_handoff_header(handoff)
        if header is None:
            return False
        payload, _proofs, profiles, predecessor, predecessor_records, lineage = header
        homes = _validate_handoff_profiles(profiles, payload, predecessor)
        return (
            homes is not None
            and _validate_handoff_records(
                predecessor_records, homes, payload.get("predecessor_records"),
            )
            and _validate_handoff_attempts(handoff.get("attempts"), lineage)
        )
    except (OSError, RuntimeError, TypeError, ValueError, KeyError):
        return False


def prepare_predecessor_handoff(runner: Any, native_receipt: Any) -> bool:
    """Persist a source-generated ``prepared`` handoff after actual shutdown settlement."""
    process = _process_snapshot()
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    bindings = _profile_bindings(runner)
    proofs = _proofs(native_receipt)
    if process is None or bindings is None or proofs is None:
        return False
    profiles, predecessors = bindings
    close_digest = proofs[1]
    native_digest = proofs[2]
    lineage_payload = _lineage_payload(
        process=process, descriptor=descriptor, profiles=profiles,
        predecessors=predecessors,
        close_digest=close_digest, native_digest=native_digest,
    )
    if not isinstance(lineage_payload["predecessor_instance"], str):
        return False
    old = _read_record(Path(process["hermes_home"])) or {}
    handoff = {
        "version": HANDOFF_VERSION,
        "handoff_id": uuid.uuid4().hex,
        "state": "prepared",
        "lineage": _digest(lineage_payload),
        "lineage_payload": lineage_payload,
        "predecessor": {
            "instance_id": descriptor.get("instance_id"),
            "authority_epoch": descriptor.get("authority_epoch"),
            **process,
        },
        "profiles": profiles,
        "predecessor_records": predecessors,
        "proofs": {"close_digest": close_digest, "native_digest": native_digest},
        "attempts": [],
    }
    old["handoff"] = handoff
    return _write_record(Path(process["hermes_home"]), old)


def _same_birth(left: Any, right: Any) -> bool:
    """Compare the native process-birth values without mixing units or widening identity."""
    return (
        type(left) in (int, float) and type(right) in (int, float)
        and not isinstance(left, bool) and not isinstance(right, bool)
        and float(left) == float(right)
    )


def _same_start_time(left: Any, right: Any) -> bool:
    """Compare status start-time fingerprints with the canonical platform comparator."""
    if type(left) is not int or type(right) is not int:
        return False
    try:
        from gateway.status import start_time_fingerprints_match

        return start_time_fingerprints_match(left, right)
    except (ImportError, TypeError, ValueError, OverflowError):
        return left == right


def _proven_dead(pid: Any, birth: Any) -> bool:
    if type(pid) is not int or pid <= 0 or type(birth) not in (int, float):
        return False
    try:
        from gateway.status import _pid_exists

        if _pid_exists(pid):
            actual = _birth(pid)
            return actual is not None and not _same_birth(actual, birth)
        return True
    except BaseException:  # health: allow BLE001 -- PID birth mismatch query stays UNKNOWN
        return False


def _current_binding_map(runner: Any) -> dict[str, dict[str, Any]] | None:
    bindings = _profile_bindings(runner)
    if bindings is None:
        return None
    return {item["home"]: item for item in bindings[0]}


def _handoff_for_successor(home: Path) -> dict[str, Any] | None:
    _record, handoff, status = _read_handoff_record(home)
    return handoff if status == "handoff" else None


def handoff_requires_successor(home: Path) -> bool:
    _record, handoff, status = _read_handoff_record(home)
    if status in {*_JOURNAL_READ_FAILURES, "malformed_handoff"}:
        return True
    if status != "handoff" or not isinstance(handoff, dict):
        return False
    state = handoff.get("state")
    if state not in _KNOWN_STATES:
        return True
    if state in _ACTIVE_STATES:
        return True
    # A corrupt historical READY record must not silently turn into an ordinary boot.  It is not
    # adoptable, but keeping the bootstrap gate active makes the corruption fail closed.
    return not _validate_handoff_integrity(handoff)


def handoff_needs_release(home: Path) -> bool:
    _record, handoff, status = _read_handoff_record(Path(home).resolve())
    if status in {*_JOURNAL_READ_FAILURES, "malformed_handoff"}:
        return True
    return bool(handoff and handoff.get("state") == "prepared")


def _mark_historical(runner: Any, home: Path, record: dict[str, Any], handoff: dict[str, Any]) -> bool:
    if handoff.get("state") != "ready":
        return False
    updated = dict(record)
    next_handoff = dict(handoff)
    next_handoff["state"] = "historical"
    updated["handoff"] = next_handoff
    return _write_record(home, updated)


def _validate_successor_inputs(
    runner: Any, handoff: Mapping[str, Any], process: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]] | None:
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    current = _current_binding_map(runner)
    expected_profiles = handoff.get("profiles")
    predecessor = handoff.get("predecessor")
    if process is None or current is None or not isinstance(expected_profiles, list):
        return None
    if set(current) != {item.get("home") for item in expected_profiles if isinstance(item, dict)}:
        return None
    if not isinstance(predecessor, dict) or not _proven_dead(
        predecessor.get("pid"), predecessor.get("birth")
    ):
        return None
    if predecessor.get("instance_id") == descriptor.get("instance_id"):
        return None
    if type(descriptor.get("authority_epoch")) is not int:
        return None
    for expected in expected_profiles:
        if not isinstance(expected, dict):
            return None
        actual = current.get(expected.get("home"))
        if actual is None or actual["owner_epoch"] == expected.get("owner_epoch"):
            return None
        if actual["authority_epoch"] <= int(expected.get("authority_epoch", 0)):
            return None
    attempts = handoff.get("attempts")
    if not isinstance(attempts, list):
        return None
    return predecessor, expected_profiles, attempts, current


def _served_home_set(value: Any) -> set[str] | None:
    if not isinstance(value, list):
        return None
    result: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            return None
        home = _canonical_home(item.get("home"))
        profile_id = _canonical_home(item.get("profile_id"))
        if home is None or profile_id is None or home != profile_id or home in result:
            return None
        result.add(home)
    return result


def _validate_successor_runtime(
    runner: Any, handoff: Mapping[str, Any], attempt: Mapping[str, Any],
    process: Mapping[str, Any], payload: Mapping[str, Any],
) -> bool:
    """Re-read physical owner and served-set witnesses immediately before READY."""
    current = _current_binding_map(runner)
    profiles = handoff.get("profiles")
    owner_epochs = attempt.get("owner_epochs")
    authority_epochs = attempt.get("authority_epochs")
    if (
        current is None or not isinstance(profiles, list)
        or not isinstance(owner_epochs, dict) or not isinstance(authority_epochs, dict)
    ):
        return False
    expected_homes = {
        _canonical_home(item.get("home")) for item in profiles if isinstance(item, dict)
    }
    expected_homes.discard(None)
    if set(current) != expected_homes or set(owner_epochs) != expected_homes:
        return False
    if set(authority_epochs) != expected_homes:
        return False
    for home, binding in current.items():
        if (
            binding.get("owner_epoch") != owner_epochs.get(home)
            or binding.get("authority_epoch") != authority_epochs.get(home)
            or binding.get("pid") != process.get("pid")
            or binding.get("kind") != process.get("kind")
            or not _same_start_time(binding.get("start_time"), process.get("start_time"))
            or not _same_birth(binding.get("birth"), process.get("birth"))
        ):
            return False
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    served = _served_home_set(payload.get("served_profiles"))
    descriptor_served = _served_home_set(descriptor.get("served_profiles"))
    if served is None or descriptor_served is None or served != descriptor_served:
        return False
    return served <= expected_homes


def _reconcile_successor_attempts(
    attempts: list[dict[str, Any]], launch_epoch: Any,
) -> list[dict[str, Any]] | None:
    if type(launch_epoch) is not int or launch_epoch < 1:
        return None
    # A failed attempt is still an authority claim.  Recovery may only use an epoch strictly newer
    # than every recorded attempt, including an attempt that never reached READY.
    for item in attempts:
        if not isinstance(item, dict) or type(item.get("authority_epoch")) is not int:
            return None
        if launch_epoch <= item["authority_epoch"]:
            return None
    if attempts and isinstance(attempts[-1], dict) and attempts[-1].get("state") == "starting":
        latest = attempts[-1]
        if not _proven_dead(latest.get("pid"), latest.get("birth")):
            return None
        failed = dict(latest)
        failed["state"] = "failed"
        failed["failure"] = "successor_exited_before_ready"
        attempts = [*attempts[:-1], failed]
    if any(
        isinstance(item, dict) and item.get("authority_epoch") == launch_epoch
        and item.get("state") in {"starting", "ready"}
        for item in attempts
    ):
        return None
    return attempts


def adopt_successor_handoff(runner: Any, home: Path) -> bool | None:
    """Adopt a prepared/released handoff only after real lock, PID-birth and epoch checks."""
    home = Path(home).resolve()
    record, handoff, status = _read_handoff_record(home)
    if status in {"absent", "ordinary"}:
        return None
    if status != "handoff" or not isinstance(record, dict) or not isinstance(handoff, dict):
        return False
    if not _validate_handoff_integrity(handoff):
        return False
    if handoff.get("state") == "ready":
        # A READY receipt is historical after its recorded process has exited. It cannot bless this
        # boot; a future restart will mint a fresh source-generated handoff.
        ready = handoff.get("ready") or {}
        if _proven_dead(ready.get("pid"), ready.get("birth")):
            return None if _mark_historical(runner, home, record, handoff) else False
        return False
    if handoff.get("state") not in _ACTIVE_STATES or handoff.get("version") != HANDOFF_VERSION:
        return False
    process = _process_snapshot()
    inputs = _validate_successor_inputs(runner, handoff, process)
    if inputs is None:
        return False
    predecessor, expected_profiles, attempts, current = inputs
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    launch_epoch = descriptor.get("authority_epoch")
    attempts = _reconcile_successor_attempts(attempts, launch_epoch)
    if attempts is None:
        return False
    lineage = handoff.get("lineage")
    if not isinstance(lineage, str) or not lineage:
        return False
    attempt = {
        "attempt_id": uuid.uuid4().hex,
        "state": "starting",
        "instance_id": descriptor.get("instance_id"),
        "authority_epoch": launch_epoch,
        "pid": process["pid"],
        "birth": process["birth"],
        "owner_epochs": {home_name: value["owner_epoch"] for home_name, value in sorted(current.items())},
        "authority_epochs": {
            home_name: value["authority_epoch"] for home_name, value in sorted(current.items())
        },
        "lineage": lineage,
    }
    next_handoff = dict(handoff)
    next_handoff["state"] = "released"
    next_handoff["attempts"] = [*attempts, attempt]
    if handoff.get("state") == "prepared":
        next_handoff["release"] = {
            "mode": "recovery-after-death",
            "outcomes": {item["home"]: "released" for item in expected_profiles},
            "pid_removed": False,
        }
    updated = dict(record)
    updated["handoff"] = next_handoff
    if not _write_record(home, updated):
        return False
    descriptor.update({
        "handoff_id": handoff.get("handoff_id"),
        "handoff_lineage": lineage,
        "handoff_attempt_id": attempt["attempt_id"],
        "handoff_predecessor_instance": predecessor.get("instance_id"),
    })
    return True


def finalize_predecessor_release(home: Path, release_summary: Any, *, pid_removed: bool) -> bool:
    """Record literal profile release outcomes and this process's own PID-marker removal."""
    home = Path(home).resolve()
    record = _read_record(home)
    handoff = record.get("handoff") if isinstance(record, dict) else None
    if (
        not isinstance(handoff, dict)
        or handoff.get("state") != "prepared"
        or not _validate_handoff_integrity(handoff)
    ):
        return False
    # The marker still names this process at this point. If it names another process, the bool from
    # remove_pid_file_checked is only a successor-fence observation and cannot certify this release.
    predecessor = handoff.get("predecessor") or {}
    if predecessor.get("pid") != os.getpid() or not pid_removed:
        return False
    marker_present, _marker = _read_pid_marker(home)
    if marker_present:
        # ``remove_owned_pid_file_checked`` returns True when it sees a validated successor marker and
        # deliberately leaves that marker in place.  That is a successful fence for ordinary cleanup,
        # but it cannot certify this predecessor's own handoff release.
        return False
    outcomes = getattr(release_summary, "outcomes", None)
    if not isinstance(outcomes, dict):
        return False
    expected = {item.get("home") for item in handoff.get("profiles", ()) if isinstance(item, dict)}
    actual = {
        str(path.resolve()): getattr(outcome, "value", outcome)
        for path, outcome in outcomes.items()
    }
    if set(actual) != expected or any(value != "released" for value in actual.values()):
        return False
    updated = dict(record)
    next_handoff = dict(handoff)
    next_handoff["state"] = "released"
    next_handoff["release"] = {
        "mode": "self",
        "outcomes": actual,
        "pid_removed": True,
    }
    updated["handoff"] = next_handoff
    return _write_record(home, updated)


def finalize_predecessor_release_if_needed(
    home: Path, release_summary: Any, *, pid_removed: bool,
) -> bool:
    """Treat ordinary exits as already complete; require the prepared bridge to commit."""
    return not handoff_needs_release(home) or finalize_predecessor_release(
        home, release_summary, pid_removed=pid_removed,
    )


def release_predecessor_at_shutdown(
    home: Path, release_ownership: Any, remove_pid: Any,
) -> bool:
    """Run canonical physical release callbacks and commit a prepared handoff."""
    try:
        _record, handoff, status = _read_handoff_record(home)
        if status in {*_JOURNAL_READ_FAILURES, "malformed_handoff"}:
            logger.error("Gateway shutdown could not read its lifecycle handoff journal")
            return False
        if status == "handoff" and (
            not isinstance(handoff, dict) or not _validate_handoff_integrity(handoff)
        ):
            logger.error("Gateway shutdown found a malformed lifecycle handoff journal")
            return False
        needs_release = handoff_needs_release(home)
        if needs_release:
            marker_present, marker = _read_pid_marker(home)
            process = _process_snapshot()
            if (
                not marker_present or not isinstance(marker, dict) or process is None
                or marker.get("pid") != os.getpid()
                or marker.get("pid") != process.get("pid")
                or not _same_start_time(marker.get("start_time"), process.get("start_time"))
            ):
                logger.error("Gateway shutdown could not authenticate its own physical-owner PID marker")
                return False
        summary = release_ownership()
        if summary is None or getattr(summary, "failed", True):
            logger.error("Gateway shutdown retained physical profile custody")
            return False
        if remove_pid() is not True:
            logger.error("Gateway shutdown could not prove removal of the physical-owner PID marker")
            return False
        marker_present, _marker = _read_pid_marker(home)
        if needs_release and marker_present:
            logger.error("Gateway shutdown observed a successor or foreign PID marker during release")
            return False
        if not finalize_predecessor_release_if_needed(home, summary, pid_removed=True):
            logger.error("Gateway shutdown could not persist the released gateway handoff")
            return False
        return True
    except BaseException:  # health: allow BLE001 -- unknown cleanup stays UNKNOWN
        logger.error("Gateway shutdown could not resolve physical lifecycle custody", exc_info=True)
        return False


def _read_pid_marker(home: Path) -> tuple[bool, dict[str, Any] | None]:
    try:
        from gateway.status import _read_json_file

        path = Path(home).resolve() / "gateway.pid"
        if not path.exists():
            return False, None
        marker = _read_json_file(path, bare_pid_ok=True)
        return True, marker if isinstance(marker, dict) else None
    except BaseException:  # health: allow BLE001 -- an unreadable marker stays UNKNOWN
        return True, None


def _control_endpoint_identity(home: Path) -> dict[str, Any] | None:
    try:
        from gateway.control_socket import resolve_client_socket_path, windows_pipe_name

        path = windows_pipe_name(home) if os.name == "nt" else resolve_client_socket_path(home)
        identity: dict[str, Any] = {"transport": "windows-pipe" if os.name == "nt" else "unix"}
        identity["endpoint"] = str(path) if path is not None else None
        if path is not None and os.name != "nt":
            import socket
            import struct

            stat = os.stat(path)
            identity.update({
                "device": stat.st_dev, "inode": stat.st_ino, "mode": stat.st_mode,
                # The same process can recreate a listener and the filesystem may reuse its
                # inode immediately.  Bind the actual socket incarnation as well as its inode.
                "ctime_ns": stat.st_ctime_ns,
            })
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
                peer.settimeout(2.0)
                peer.connect(str(path))
                if hasattr(socket, "SO_PEERCRED"):
                    identity["peer_pid"] = struct.unpack(
                        "3i", peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                    )[0]
                    identity["peer_birth"] = _birth(identity["peer_pid"])
        elif path is not None:
            from gateway.runtime_bootstrap_windows import probe_runtime_control_peer

            identity.update(probe_runtime_control_peer(home, timeout=2.0))
            identity["peer_birth"] = _birth(identity["peer_pid"])
        return identity if identity["endpoint"] else None
    except BaseException:  # health: allow BLE001 -- control endpoint identity failure stays UNKNOWN
        return None


def _control_observation(
    home: Path, payload: Mapping[str, Any] | None = None,
    *, endpoint: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if payload is not None and not isinstance(payload, Mapping):
        return None
    identity = dict(endpoint) if isinstance(endpoint, Mapping) else _control_endpoint_identity(home)
    if identity is None:
        return None
    if payload is not None:
        identity.update({"pid": payload.get("pid"), "start_time": payload.get("start_time")})
    return identity if identity.get("endpoint") else None


def _control_endpoint_key(identity: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(identity.get(key) for key in (
        "transport", "endpoint", "device", "inode", "mode", "ctime_ns", "peer_pid", "peer_birth",
        "peer_subject",
    ))


def _mark_successor_unknown(
    record: Mapping[str, Any], handoff: Mapping[str, Any], attempts: list[dict[str, Any]], reason: str,
) -> bool:
    next_attempt = dict(attempts[-1])
    next_attempt["state"] = "unknown"
    next_attempt["failure"] = reason
    next_handoff = dict(handoff)
    next_handoff["state"] = "unknown"
    next_handoff["attempts"] = [*attempts[:-1], next_attempt]
    updated = dict(record)
    updated["handoff"] = next_handoff
    _write_record(Path(handoff["predecessor"]["hermes_home"]), updated)
    return False


def _mark_current_successor_unknown(home: Path, reason: str) -> bool:
    record = _read_record(home)
    handoff = record.get("handoff") if isinstance(record, dict) else None
    attempts = handoff.get("attempts") if isinstance(handoff, dict) else None
    if not isinstance(record, dict) or not isinstance(handoff, dict) or handoff.get("state") != "released":
        return False
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], dict):
        return False
    return _mark_successor_unknown(record, handoff, attempts, reason)


def _record_successor_control(
    home: Path, runner: Any, payload: Mapping[str, Any],
    *, control_before: Mapping[str, Any] | None = None,
    control_after: Mapping[str, Any] | None = None,
    expected_runtime_state: str = "ready",
) -> bool:
    """Commit READY only after the authenticated control response matches the durable attempt."""
    home = Path(home).resolve()
    if expected_runtime_state not in {"starting", "ready"}:
        return False
    record = _read_record(home)
    handoff = record.get("handoff") if isinstance(record, dict) else None
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    if not isinstance(handoff, dict) or handoff.get("state") != "released":
        return False
    if not _validate_handoff_integrity(handoff):
        return False
    attempts = handoff.get("attempts")
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], dict):
        return False
    attempt = attempts[-1]
    endpoint_before = dict(control_before) if isinstance(control_before, Mapping) else _control_endpoint_identity(home)
    observation = _control_observation(home, payload, endpoint=control_after)
    process = _process_snapshot()
    if endpoint_before is None or observation is None or process is None:
        return _mark_successor_unknown(record, handoff, attempts, "control_observation_unavailable")
    refused = (
        _control_endpoint_key(endpoint_before) != _control_endpoint_key(observation)
        or not _validate_successor_runtime(runner, handoff, attempt, process, payload)
        or attempt.get("state") != "starting"
        or attempt.get("instance_id") != descriptor.get("instance_id")
        or attempt.get("authority_epoch") != descriptor.get("authority_epoch")
        or attempt.get("pid") != process["pid"]
        or not _same_birth(attempt.get("birth"), process["birth"])
        or payload.get("state") != expected_runtime_state
        or payload.get("instance_id") != descriptor.get("instance_id")
        or payload.get("authority_epoch") != descriptor.get("authority_epoch")
        or payload.get("handoff_id") != handoff.get("handoff_id")
        or payload.get("handoff_lineage") != handoff.get("lineage")
        or payload.get("handoff_attempt_id") != attempt.get("attempt_id")
        or payload.get("pid") != process["pid"]
        or not _same_start_time(payload.get("start_time"), process.get("start_time"))
        or (
            observation.get("peer_pid") is not None
            and (
                observation.get("peer_pid") != process.get("pid")
                or not _same_birth(observation.get("peer_birth"), process.get("birth"))
                or (
                    observation.get("peer_subject") is not None
                    and (
                        not isinstance(observation.get("peer_subject"), str)
                        or not observation.get("peer_subject")
                    )
                )
                or (
                    observation.get("transport") == "windows-pipe"
                    and not isinstance(observation.get("peer_subject"), str)
                )
            )
        )
    )
    if refused:
        return _mark_successor_unknown(record, handoff, attempts, "control_identity_refused")
    next_attempt = dict(attempt)
    next_attempt["state"] = "ready"
    next_attempt["runtime_state"] = expected_runtime_state
    next_attempt["control"] = observation
    next_attempt["control_digest"] = _digest(observation)
    next_handoff = dict(handoff)
    next_handoff["state"] = "ready"
    next_handoff["attempts"] = [*attempts[:-1], next_attempt]
    next_handoff["ready"] = {
        "instance_id": descriptor.get("instance_id"),
        "authority_epoch": descriptor.get("authority_epoch"),
        "pid": process["pid"],
        "birth": process["birth"],
        "control_digest": next_attempt["control_digest"],
    }
    updated = dict(record)
    updated["handoff"] = next_handoff
    return _write_record(home, updated)


def record_successor_ready(
    home: Path, runner: Any, payload: Mapping[str, Any],
    *, control_before: Mapping[str, Any] | None = None,
    control_after: Mapping[str, Any] | None = None,
) -> bool:
    """Commit public READY after the authenticated control response matches the durable attempt."""
    return _record_successor_control(
        home, runner, payload, control_before=control_before, control_after=control_after,
        expected_runtime_state="ready",
    )


def _record_successor_from_control(
    runner: Any, home: Path, *, expected_runtime_state: str,
) -> bool:
    """Perform the real local control roundtrip used by bootstrap.

    A successor handoff is attested against the private ``starting`` descriptor before the
    process publishes public READY.  The default remains ``ready`` for existing SDK/control
    callers that explicitly ask to record a public READY observation.
    """
    home = Path(home).resolve()
    _record, handoff, status = _read_handoff_record(home)
    if status in {*_JOURNAL_READ_FAILURES, "malformed_handoff"}:
        return False
    if status in {"absent", "ordinary"}:
        return True
    if status != "handoff" or not isinstance(handoff, dict):
        return False
    if handoff.get("state") != "released" or not _validate_handoff_integrity(handoff):
        return False
    try:
        from gateway.control_socket import identify_gateway

        control_before = _control_endpoint_identity(home)
        payload = identify_gateway(home)
        control_after = _control_endpoint_identity(home)
    except BaseException:  # health: allow BLE001 -- control roundtrip failure stays UNKNOWN
        return _mark_current_successor_unknown(home, "control_roundtrip_failed")
    if payload is None or control_before is None or control_after is None:
        return _mark_current_successor_unknown(home, "control_roundtrip_unavailable")
    return _record_successor_control(
        home, runner, payload, control_before=control_before, control_after=control_after,
        expected_runtime_state=expected_runtime_state,
    )


def record_successor_ready_from_control(runner: Any, home: Path) -> bool:
    """Perform the public READY control roundtrip for existing callers."""
    return _record_successor_from_control(runner, home, expected_runtime_state="ready")


def adopt_successor_or_refuse(runner: Any, home: Path) -> bool:
    """Bootstrap adapter: ordinary boots pass; an active handoff must authenticate."""
    result = adopt_successor_handoff(runner, home)
    return result is not False or not handoff_requires_successor(home)


def verify_successor_ready(runner: Any, home: Path) -> bool:
    """Bootstrap adapter for the off-loop authenticated private-start roundtrip."""
    return not handoff_requires_successor(home) or _record_successor_from_control(
        runner, home, expected_runtime_state="starting",
    )


def record_restart_native_handoff(runner: Any, custody: Any, context: Any, action: str) -> bool:
    """Bind the source shutdown receipt and durable predecessor journal in one source seam."""
    from gateway.lifecycle_custody import LifecycleNativeSettlement

    receipt = LifecycleNativeSettlement(
        action,
        context,
        runner._shutdown_event,
        context.db_close_succeeded,
        db_close_receipt=context.db_close_receipt,
    )
    recorded = custody.record_native_settlement(action, receipt)
    if action == "restart" and recorded and receipt.ready() is True:
        if not prepare_predecessor_handoff(runner, receipt):
            logger.error("Gateway restart handoff remained UNKNOWN: source proof was not durable")
    return recorded

