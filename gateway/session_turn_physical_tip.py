"""Authenticated logical-root to physical-compression-tip witnesses.

The API admission remains keyed by the logical root.  This module only reads the existing
compression lineage, route, and store objects to bind the physical writer to that root; it
never creates a row, route, lease, or recovery record.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from typing import Any


@dataclass(frozen=True)
class PhysicalTipWitness:
    logical_root: str
    physical_tip: str
    lineage: tuple[str, ...]
    route: str
    store_path: str
    profile_id: str
    authority_epoch: Any
    process_instance: str
    physical_owner_epoch: str
    binding_identity: tuple[Any, ...]
    row_fingerprints: tuple[tuple[str, str | None, str | None, bool], ...]


def canonical_store_path(authority: Any) -> str | None:
    db = getattr(authority, "db", None)
    path = getattr(db, "db_path", None)
    if path is None or isinstance(path, bool):
        return None
    try:
        return str(Path(path).expanduser().resolve())
    except (OSError, TypeError, ValueError):
        return None


def _row_fingerprint(row: Any, expected_id: str) -> tuple[str, str | None, str | None, bool] | None:
    if not isinstance(row, dict) or row.get("id") != expected_id:
        return None
    parent = row.get("parent_session_id")
    end_reason = row.get("end_reason")
    ended = row.get("ended_at") is not None
    if parent is not None and (type(parent) is not str or not parent):
        return None
    if end_reason is not None and (type(end_reason) is not str or not end_reason):
        return None
    return expected_id, parent, end_reason, ended


def _lineage_rows(db: Any, lineage: tuple[str, ...]) -> tuple[tuple[str, str | None, str | None, bool], ...] | None:
    rows = []
    is_child = getattr(db, "_is_compression_child_row", None)
    for index, session_id in enumerate(lineage):
        row = db.get_session(session_id)
        fingerprint = _row_fingerprint(row, session_id)
        if fingerprint is None:
            return None
        if index < len(lineage) - 1:
            child = db.get_session(lineage[index + 1])
            if row.get("ended_at") is None:
                return None
            if not isinstance(child, dict) or child.get("parent_session_id") != session_id:
                return None
            if row.get("end_reason") == "compression":
                if not callable(is_child) or is_child(child) is not True:
                    return None
            elif row.get("end_reason") == "session_reset":
                try:
                    if json.loads(child.get("model_config") or "{}").get("_reset_from") != session_id:
                        return None
                except (TypeError, ValueError, json.JSONDecodeError):
                    return None
            else:
                return None
        elif row.get("ended_at") is not None or row.get("end_reason") is not None:
            return None
        rows.append(fingerprint)
    return tuple(rows)


def _binding_identity(binding: Any) -> tuple[Any, ...] | None:
    identity = tuple(
        getattr(binding, field, None)
        for field in (
            "admission_id", "execution_generation", "principal_id", "source_identity",
        )
    )
    return None if any(item is None or (type(item) is str and not item) for item in identity) else identity


def _turn_binding_custody_matches(binding: Any) -> bool:
    """Require the canonical retained I04 binding before reading compression state."""
    if type(getattr(binding, "admission_id", None)) is not str or not binding.admission_id:
        return False
    try:
        from gateway.runtime_service_custody import (
            TurnCustodyBinding, _require_authority_parent_identity, _require_binding_shape,
        )
        if not isinstance(binding, TurnCustodyBinding):
            return False
        # This is the existing source-owned I04/coordinator validator.  It accepts a
        # retained finalizing/unknown parent only when the exact handle remains in the
        # coordinator record; scalar binding fields and a DB row are insufficient.
        _require_binding_shape(binding, allow_unknown_parent=True)
        _require_authority_parent_identity(binding)
    except (AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    return True


def _profile_scope_witness(binding: Any) -> str | None:
    """Require the exact retained profile-lock incarnation for this I04 binding.

    The epoch and native owner are resolved by the canonical per-profile coordinator.  A
    missing helper, an old coordinator signature, a foreign lock, or a replacement lock is a
    typed refusal; the physical tip reader never treats a missing method as compatibility.
    """
    coordinator = getattr(binding, "coordinator", None)
    parent = getattr(binding, "parent_handle", None)
    owner_epoch = getattr(parent, "physical_owner_epoch", None)
    if type(owner_epoch) is not str or not owner_epoch:
        return None
    try:
        from gateway.runtime_custody_profiles import profile_scope_valid_for_handle
    except (AttributeError, ImportError):
        return None
    if not callable(profile_scope_valid_for_handle):
        return None
    try:
        if profile_scope_valid_for_handle(coordinator, parent) is not True:
            return None
        profile_scope_valid = getattr(coordinator, "profile_scope_valid", None)
        if not callable(profile_scope_valid):
            return None
        if profile_scope_valid(
            getattr(binding, "profile_id", None),
            getattr(binding, "authority_epoch", None),
            require_serving=False,
            allow_retiring=True,
            physical_owner_epoch=owner_epoch,
        ) is not True:
            return None
    except (
        AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        sqlite3.Error,
    ):
        return None
    return owner_epoch


def _binding_row_matches(db: Any, binding: Any, root: str) -> bool:
    admission_id = getattr(binding, "admission_id", None)
    principal_id = getattr(binding, "principal_id", None)
    source_identity = getattr(binding, "source_identity", None)
    if (
        type(admission_id) is not str or not admission_id
        or type(principal_id) is not str or not principal_id
        or type(source_identity) is not str or not source_identity
    ):
        return False
    try:
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(db, admission_id=admission_id)
    except (ImportError, OSError, KeyError, TypeError, ValueError, sqlite3.Error):
        return False
    return (
        isinstance(row, dict)
        and row.get("admission_id") == admission_id
        and row.get("target_session_id") == root
        and row.get("status") in {"queued", "started", "terminal"}
        and row.get("owner_epoch") == getattr(binding, "authority_epoch", None)
        and row.get("generation") == getattr(binding, "execution_generation", None)
        and row.get("principal_id") == principal_id == source_identity
    )


def _validated_lineage(
    db: Any, authority: Any, binding: Any, profile_id: str, root: str, physical: str,
):
    binding_ok = _turn_binding_custody_matches(binding)
    row_ok = _binding_row_matches(db, binding, root)
    if not binding_ok or not row_ok:
        return None
    # Reset rotations are local-policy lineage, not compression lineage.  The existing
    # persisted local receipt is the source-owned witness for that route; use it before the
    # compression-only projection so a managed worker assigned to a reset child can still
    # prove its exact physical transcript without changing the logical I04 owner.
    local_witness = None
    try:
        from gateway.session_recovery_custody import local_policy_lineage_witness
        local_witness = local_policy_lineage_witness(authority, root)
    except BaseException:  # health: allow BLE001 -- unreadable local-policy lineage stays fail-closed
        local_witness = None
    local_lineage = (
        local_witness is not None
        and getattr(local_witness, "session_id", None) == root
        and getattr(local_witness, "physical_session_id", None) == physical
        and getattr(local_witness, "profile_id", None) == profile_id
        and getattr(local_witness, "authority_epoch", None) == getattr(binding, "authority_epoch", None)
        and getattr(local_witness, "process_instance", None) == getattr(binding, "process_instance", None)
    )
    if local_lineage:
        lineage = tuple(local_witness.lineage)
    else:
        lineage = tuple(db.get_compression_lineage(root))
    if (
        not lineage or lineage[0] != root or lineage[-1] != physical
        or len(set(lineage)) != len(lineage)
    ):
        return None
    if not local_lineage and db.get_compression_tip(root) != physical:
        return None
    from gateway.session_contract import SessionRef
    if (
        not local_lineage
        and authority.physical_target(SessionRef(profile_id, root)) != physical
    ):
        return None
    fingerprints = _lineage_rows(db, lineage)
    return (lineage, fingerprints, local_lineage) if fingerprints is not None else None


def _current_route(authority: Any, root: str, physical: str) -> str | None:
    live = (getattr(authority, "sessions", {}) or {}).get(root)
    route = getattr(live, "route", None)
    store = getattr(getattr(authority, "runner", None), "session_store", None)
    entry = store.lookup_by_session_key(route) if store is not None and isinstance(route, str) else None
    return route if entry is not None and getattr(entry, "session_id", None) == physical else None


def resolve_physical_tip_witness(
    binding: Any, physical_session_id: Any, expected_store_path: str | None = None,
) -> PhysicalTipWitness | None:
    """Read and bind one live physical tip to an existing logical custody binding."""
    root = getattr(binding, "session_id", None)
    authority = getattr(binding, "authority", None)
    profile_id = getattr(binding, "profile_id", None)
    if (
        authority is None or type(root) is not str or not root
        or type(physical_session_id) is not str or not physical_session_id
        or type(profile_id) is not str or not profile_id
        or getattr(authority, "profile_id", None) != profile_id
        or getattr(binding, "authority_epoch", None) != getattr(authority, "epoch", None)
        or getattr(binding, "process_instance", None) != getattr(authority, "instance_id", None)
    ):
        return None
    store_path = canonical_store_path(authority)
    binding_identity = _binding_identity(binding)
    if (
        store_path is None
        or (expected_store_path is not None and expected_store_path != store_path)
        or binding_identity is None
    ):
        return None
    physical_owner_epoch = _profile_scope_witness(binding)
    if physical_owner_epoch is None:
        return None
    db = getattr(authority, "db", None)
    try:
        validated = _validated_lineage(
            db, authority, binding, profile_id, root, physical_session_id,
        )
        if validated is None:
            return None
        lineage, fingerprints, local_lineage = validated
        route = _current_route(authority, root, physical_session_id)
        if route is None:
            return None
        # The lineage rows and route are independently mutable.  Re-read both canonical
        # tip views after those checks so a rotation between the first read and witness
        # publication cannot make an old physical child look current.
        if local_lineage:
            if _current_route(authority, root, physical_session_id) != route:
                return None
        else:
            from gateway.session_contract import SessionRef
            if (
                db.get_compression_tip(root) != physical_session_id
                or authority.physical_target(SessionRef(profile_id, root)) != physical_session_id
                or _current_route(authority, root, physical_session_id) != route
            ):
                return None
        if _profile_scope_witness(binding) != physical_owner_epoch:
            return None
        return PhysicalTipWitness(
            logical_root=root, physical_tip=physical_session_id, lineage=lineage,
            route=route, store_path=store_path, profile_id=profile_id,
            authority_epoch=getattr(binding, "authority_epoch", None),
            process_instance=getattr(binding, "process_instance", None),
            physical_owner_epoch=physical_owner_epoch,
            binding_identity=binding_identity, row_fingerprints=fingerprints,
        )
    except (
        AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        json.JSONDecodeError, sqlite3.Error,
    ):
        return None


def physical_proof_session(binding: Any, proof: Any) -> str | None:
    """Return the proof's authenticated physical session, or refuse it."""
    session_id = getattr(proof, "session_id", None)
    root = getattr(binding, "session_id", None)
    authority = getattr(binding, "authority", None)
    store_path = canonical_store_path(authority)
    if type(session_id) is not str or not session_id or type(root) is not str or not root:
        return None
    if store_path is None or getattr(proof, "store_path", None) != store_path:
        return None
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
        from gateway.session_classic_custody import ClassicWriterBinding
    except (ImportError, AttributeError):
        return None
    if isinstance(binding, TurnCustodyBinding):
        # A logical-root proof is not self-authenticating: compression may have moved the
        # physical writer after the admission was created.  Resolve the current witness even
        # when the proof names the root.
        witness = resolve_physical_tip_witness(binding, session_id, getattr(proof, "store_path", None))
        return witness.physical_tip if witness is not None else None
    if isinstance(binding, ClassicWriterBinding) and session_id == root:
        return root
    return None


def preflight_physical_proof_session(binding: Any, proof: Any) -> str | None:
    """Authenticate a managed proof before the in-memory route CAS.

    ``physical_proof_session`` intentionally requires the live route to already point at the
    proof's physical tip.  Compression publication has the opposite ordering: the durable
    lineage names the new tip while the gateway route still names its parent.  This read-only
    witness validates the same binding/profile/native scope and the canonical lineage without
    accepting a route change as evidence.
    """
    session_id = getattr(proof, "session_id", None)
    root = getattr(binding, "session_id", None)
    authority = getattr(binding, "authority", None)
    profile_id = getattr(binding, "profile_id", None)
    store_path = canonical_store_path(authority)
    if (
        type(session_id) is not str or not session_id
        or type(root) is not str or not root
        or type(profile_id) is not str or not profile_id
        or store_path is None
        or getattr(proof, "store_path", None) != store_path
        or getattr(authority, "profile_id", None) != profile_id
        or getattr(binding, "authority_epoch", None) != getattr(authority, "epoch", None)
        or getattr(binding, "process_instance", None) != getattr(authority, "instance_id", None)
    ):
        return None
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
        from gateway.session_classic_custody import ClassicWriterBinding
    except (ImportError, AttributeError):
        return None
    if isinstance(binding, ClassicWriterBinding):
        return root if session_id == root else None
    if not isinstance(binding, TurnCustodyBinding):
        return None
    if _profile_scope_witness(binding) is None:
        return None
    db = getattr(authority, "db", None)
    try:
        validated = _validated_lineage(db, authority, binding, profile_id, root, session_id)
    except (
        AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        json.JSONDecodeError, sqlite3.Error,
    ):
        return None
    return session_id if validated is not None else None


def retained_physical_proof_session(binding: Any, proof: Any) -> str | None:
    """Validate an exact managed proof after a later compression tip moved.

    The first proof publication remains strict: ``physical_proof_session`` must see the worker's
    own tip.  Its delivery finalizer can run after a descendant has moved the live route, however;
    in that case accept only the same stored proof when its session is still on the authenticated
    current lineage and the current route still resolves to that lineage's live tip.  Siblings,
    resets, copied rows, and replaced routes remain refusals.
    """
    session_id = getattr(proof, "session_id", None)
    root = getattr(binding, "session_id", None)
    authority = getattr(binding, "authority", None)
    store_path = canonical_store_path(authority)
    if (
        type(session_id) is not str or not session_id
        or type(root) is not str or not root
        or store_path is None
        or getattr(proof, "store_path", None) != store_path
    ):
        return None
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
        from gateway.session_contract import SessionRef
    except (ImportError, AttributeError):
        return None
    if not isinstance(binding, TurnCustodyBinding):
        return None
    # Retained proofs may name an older physical child, but they still require the exact
    # retained I04 parent and profile-lock/native-owner incarnation.  Those validators are
    # independent of the current compression tip and must not be bypassed by this fallback.
    if not _turn_binding_custody_matches(binding) or _profile_scope_witness(binding) is None:
        return None
    db = getattr(authority, "db", None)
    if db is None or not _binding_row_matches(db, binding, root):
        return None
    try:
        lineage = tuple(db.get_compression_lineage(root))
        if (
            not lineage
            or session_id not in lineage
            or len(set(lineage)) != len(lineage)
            or _lineage_rows(db, lineage) is None
            or db.get_compression_tip(root) != lineage[-1]
            or authority.physical_target(SessionRef(getattr(binding, "profile_id", None), root))
            != lineage[-1]
            or _current_route(authority, root, lineage[-1]) is None
        ):
            return None
    except (
        AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        json.JSONDecodeError, sqlite3.Error,
    ):
        return None
    return session_id
