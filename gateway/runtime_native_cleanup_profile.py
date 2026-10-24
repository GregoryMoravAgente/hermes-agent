"""Validation helpers for the captured I18 profile cleanup witness.

The profile participants retain one immutable source graph captured before native ACK.  These
helpers keep the retry validation separate from the participant and plan assembly code while
preserving exact object identity checks for profiles, authorities, provider leases, and fences.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from gateway.runtime_custody import NativeCleanupPlan


def _canonical_home_key(home: Any) -> str | None:
    """Return the ownership-safe physical-home key for comparisons.

    Profile lifecycle records retain the spelling captured by the source owner while reserved
    bindings are already canonicalized by ``runtime_ownership``.  Comparing those spellings
    directly rejects the same Windows home when its case differs.  Reuse the existing canonical
    owner boundary so linked/reparse-backed, malformed, and unavailable homes still fail closed.
    """
    if not isinstance(home, (str, Path)):
        return None
    candidate = Path(home)
    if not candidate.is_absolute():
        return None
    try:
        from gateway.runtime_ownership import canonical_home

        return str(canonical_home(candidate))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None


def profile_bindings_match(
    expected: Any, current: Any,
) -> bool:
    """Require one exact reserved-home/descriptor/evidence binding per captured profile."""
    if not isinstance(expected, tuple) or not isinstance(current, tuple):
        return False
    if not expected or len(expected) != len(current):
        return False
    expected_by_home = {}
    for item in expected:
        if type(item) is not tuple or len(item) != 3:
            return False
        home, handle, evidence = item
        key = _canonical_home_key(home)
        if (
            key is None
            or key in expected_by_home
            or handle is None
            or not isinstance(evidence, dict)
        ):
            return False
        expected_by_home[key] = (home, handle, evidence)
    seen = set()
    for item in current:
        if type(item) is not tuple or len(item) != 3:
            return False
        home, handle, evidence = item
        key = _canonical_home_key(home)
        captured = expected_by_home.get(key)
        if (
            key is None
            or key in seen
            or captured is None
            or captured[1] is not handle
        ):
            return False
        if not isinstance(evidence, dict):
            return False
        seen.add(key)
    return seen == set(expected_by_home)


def profile_participants_match_witness(witness: Any, participants: Any) -> bool:
    """Require each captured profile handle to have exactly one participant and native fence."""
    if not isinstance(participants, tuple):
        return False
    if not isinstance(getattr(witness, "profile_bindings", None), tuple):
        return False
    if not _witness_homes_match_bindings(witness):
        return False
    if len(participants) != len(witness.profile_bindings):
        return False
    current = []
    seen = set()
    for item in participants:
        if type(item) is not tuple or len(item) != 3:
            return False
        snapshot, participant, handle = item
        home = getattr(snapshot, "home", None)
        home_key = _canonical_home_key(home)
        if (
            home_key is None
            or participant is None
            or handle is None
            or getattr(snapshot, "handle", None) is not handle
            or home_key in seen
        ):
            return False
        seen.add(home_key)
        current.append((home, handle, getattr(snapshot, "evidence", None)))
    return profile_bindings_match(witness.profile_bindings, tuple(current))


def _witness_homes_match_bindings(witness: Any) -> bool:
    records = getattr(witness, "records", None)
    bindings = getattr(witness, "profile_bindings", None)
    if not isinstance(records, tuple) or not isinstance(bindings, tuple):
        return False
    record_homes = []
    for item in records:
        if type(item) is not tuple or len(item) != 6 or not isinstance(item[0], str):
            return False
        key = _canonical_home_key(item[0])
        if key is None:
            return False
        record_homes.append(key)
    binding_homes = []
    for item in bindings:
        if type(item) is not tuple or len(item) != 3:
            return False
        key = _canonical_home_key(item[0])
        if key is None:
            return False
        binding_homes.append(key)
    return (
        record_homes
        and len(record_homes) == len(set(record_homes))
        and len(binding_homes) == len(set(binding_homes))
        and set(record_homes) == set(binding_homes)
    )


def _source_evidence_valid(witness: Any, coordinator: Any, capability: Any) -> bool:
    try:
        from gateway.runtime_custody import OWNER_SPECS
        from gateway.runtime_native_cleanup_sources import _valid_source_evidence

        return _valid_source_evidence(
            witness.source_evidence,
            source=OWNER_SPECS["I18_multiplex_profile_retirement"].source,
            coordinator=coordinator,
            receipt=capability.closed_receipt,
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def _participant_bound(capability: Any, source_receipt: Any, participant: Any) -> bool:
    plan = getattr(capability, "plan", None)
    return isinstance(plan, NativeCleanupPlan) and any(
        type(entry) is tuple
        and len(entry) == 3
        and entry[1] is source_receipt
        and entry[2] is participant
        for entry in plan.source_participants
    )


def _records_valid(witness: Any, source_profiles: list[dict[str, Any]], home: Any) -> bool:
    custody = witness.custody
    current_records = getattr(custody, "_current", None)
    records = getattr(witness, "records", None)
    if not isinstance(current_records, dict) or not isinstance(records, tuple):
        return False
    source_profiles_by_id = {
        item.get("profile_id"): item for item in source_profiles if isinstance(item, dict)
    }
    if len(source_profiles_by_id) != len(source_profiles):
        return False
    expected_homes: set[str] = set()
    current_by_home: dict[str, Any] = {}
    for current_home, current_record in current_records.items():
        if not isinstance(current_home, str):
            return False
        current_key = _canonical_home_key(current_home)
        if current_key is None or current_key in current_by_home:
            return False
        current_by_home[current_key] = current_record
    for item in records:
        if type(item) is not tuple or len(item) != 6:
            return False
        home_key, record, authority, lease, graph, provider_snapshot = item
        if not isinstance(home_key, str) or not home_key or record is None:
            return False
        canonical_key = _canonical_home_key(home_key)
        if canonical_key is None or canonical_key in expected_homes:
            return False
        expected_homes.add(canonical_key)
        if current_by_home.get(canonical_key) is not record or not _record_identity_valid(
            record, authority, lease, graph, provider_snapshot, witness.close_generation
        ):
            return False
        source_profile = source_profiles_by_id.get(lease.profile_id)
        if not _source_profile_valid(source_profile, record, witness.close_generation):
            return False
    if set(current_by_home) != expected_homes:
        return False
    requested_home = _canonical_home_key(home)
    if requested_home is None:
        return False
    return any(
        _canonical_home_key(item[0]) == requested_home
        and _canonical_home_key(getattr(item[1].lease, "profile_home", None)) == requested_home
        for item in records
        if type(item) is tuple and len(item) == 6
    )


def _record_identity_valid(
    record: Any, authority: Any, lease: Any, graph: Any, provider_snapshot: Any,
    close_generation: Any,
) -> bool:
    return (
        record.authority is authority
        and record.lease is lease
        and record.graph is graph
        and record.close_generation == close_generation
        and record.state in {"closing", "retired_ready"}
        and record.producer is None
        and record.global_provider_prepared is True
        and record.global_provider_snapshot is provider_snapshot
    )


def _source_profile_valid(source_profile: Any, record: Any, close_generation: Any) -> bool:
    if (
        not isinstance(source_profile, dict)
        or not isinstance(source_profile.get("profile_home"), str)
        or not isinstance(getattr(record.lease, "profile_home", None), str)
    ):
        return False
    source_home = _canonical_home_key(source_profile.get("profile_home"))
    record_home = _canonical_home_key(getattr(record.lease, "profile_home", None))
    if source_home is None or source_home != record_home:
        return False
    return all(
        source_profile.get(field) == expected
        for field, expected in (
            ("authority_epoch", record.lease.authority_epoch),
            ("process_instance", record.lease.process_instance),
            ("physical_owner_epoch", record.lease.physical_owner_epoch),
            ("close_generation", close_generation),
            ("registered_witness", record.registered_witness),
        )
    )


def _provider_entries_valid(witness: Any) -> bool:
    provider_custody = witness.provider_custody
    if getattr(provider_custody, "_generation", None) != witness.close_generation:
        return False
    entries = getattr(provider_custody, "_entries", None)
    if not isinstance(entries, dict):
        return False
    for item in witness.provider_entries:
        if type(item) is not tuple or len(item) != 5:
            return False
        entry, agent, authority, manager, lease = item
        if entries.get(id(agent)) is not entry or not _provider_entry_valid(
            entry, agent, authority, manager, lease
        ):
            return False
    return True


def _provider_entry_valid(
    entry: Any, agent: Any, authority: Any, manager: Any, lease: Any,
) -> bool:
    return (
        entry.agent is agent
        and entry.authority is authority
        and entry.manager is manager
        and entry.lease is lease
        and authority is not None
        and manager is not None
        and lease is not None
        and getattr(entry, "status", None) == "committed"
        and getattr(agent, "_memory_manager", None) is manager
    )


def profile_native_cleanup_witness_ready(
    witness: Any, *, coordinator: Any, capability: Any, source_receipt: Any,
    participant: Any, home: Any,
) -> bool:
    """Validate the captured I18 graph without rereading settled profile locks."""
    if not _header_valid(witness, coordinator, capability, source_receipt):
        return False
    if not _participant_bound(capability, source_receipt, participant):
        return False
    source_profiles = witness.source_evidence.get("profiles")
    if not isinstance(source_profiles, (tuple, list)) or not source_profiles:
        return False
    return _source_evidence_valid(witness, coordinator, capability) and _records_valid(
        witness, source_profiles, home
    ) and _provider_entries_valid(witness)


def _header_valid(witness: Any, coordinator: Any, capability: Any, source_receipt: Any) -> bool:
    return (
        witness is not None
        and witness.coordinator is coordinator
        and witness.process_instance == getattr(coordinator, "process_instance", None)
        and witness.authority_epoch == getattr(coordinator, "authority_epoch", None)
        and witness.close_generation == getattr(capability, "close_generation", None)
        and getattr(witness.custody, "runner", None) is getattr(
            witness.provider_custody, "runner", None
        )
        and getattr(source_receipt, "coordinator", None) is coordinator
        and getattr(source_receipt, "process_instance", None) == witness.process_instance
        and getattr(source_receipt, "authority_epoch", None) == witness.authority_epoch
        and getattr(source_receipt, "close_generation", None) == witness.close_generation
    )
