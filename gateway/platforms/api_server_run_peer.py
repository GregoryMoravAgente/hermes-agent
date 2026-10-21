"""Authenticated peer admission binding for one API run custody handle."""
from __future__ import annotations

import sqlite3
from typing import Any

from gateway.runtime_custody import CustodyRefusal


def peer_source_kind(authority: Any, record: dict[str, Any]):
    """Return the authenticated producer kind for a peer record.

    The peer Bot Chat is stored as a local GUI session.  Its canonical route is still the
    Bot Chat FIFO, so the storage surface is normalized to ``bot_room`` only after the
    existing Bot Chat lineage lookup proves that exact session is the titled Bot Chat tip.
    A plain GUI session never receives this normalization.
    """
    db = getattr(authority, "db", None)
    if db is None or not isinstance(record, dict):
        return None
    session_id = record.get("session_id")
    session = getattr(db, "get_session", lambda _id: None)(session_id)
    source = session.get("source") if isinstance(session, dict) else None
    if source in {"bot_room", "api_server"}:
        return source
    if source != "gui":
        return None
    if (record.get("_api_producer_phase") != "bot_i02"
            or record.get("_api_producer_source") != "bot_chat"):
        return None
    try:
        from gateway.platforms.api_server_bot_chat import bot_chat_tip
        tip = bot_chat_tip(authority, session_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError,
            ValueError, sqlite3.Error):
        return None
    if not isinstance(tip, dict) or tip.get("id") != session_id:
        return None
    # The mailbox projection may omit both principal fields after it has been published.  When
    # the authenticated actor survives that projection, it must still be the current Bot Chat tip
    # owner; the canonical admission row below remains the source of the producer principal.
    actor_principal = record.get("principal_id")
    return "bot_room" if actor_principal is None or actor_principal == tip.get("user_id") else None


def peer_binding_identity(custody: Any, record: Any, authority: Any):
    if not isinstance(record, dict):
        return None, CustodyRefusal("peer_record_missing", "I03_api_run_direct")
    admission_id = record.get("admission_id")
    retry_id = record.get("retry_admission_id")
    if retry_id is not None and (not isinstance(retry_id, str) or not retry_id):
        return None, CustodyRefusal("peer_admission_missing", "I03_api_run_direct")
    binding_id = retry_id or admission_id
    generation = record.get("retry_execution_generation" if retry_id else "execution_generation")
    if generation is not None and (type(generation) is not int or generation < 0):
        return None, CustodyRefusal("peer_generation_missing", "I03_api_run_direct")
    if generation is None and record.get("status") not in {"queued", "claimed", "canonical"}:
        return None, CustodyRefusal("peer_generation_missing", "I03_api_run_direct")
    if not isinstance(binding_id, str) or not binding_id:
        return None, CustodyRefusal("peer_admission_missing", "I03_api_run_direct")
    if custody.admission_id is not None:
        if binding_id != custody.admission_id:
            return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        if (generation is None
                and record.get("_api_producer_phase") == "bot_i02"
                and record.get("_api_producer_source") == "bot_chat"
                and custody.execution_generation is not None):
            # The Bot delivery projection intentionally omits its private generation fields.  The
            # already-bound custody handle is the same opaque generation witness for replay; only
            # this typed Bot producer may fill the omitted projection field from that witness.
            generation = custody.execution_generation
        elif generation != custody.execution_generation:
            return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
    if record.get("profile_id") != custody.profile_id:
        return None, CustodyRefusal("peer_profile_mismatch", "I03_api_run_direct")
    if record.get("authority_epoch") != custody.authority_epoch:
        return None, CustodyRefusal("peer_epoch_mismatch", "I03_api_run_direct")
    if (getattr(authority, "profile_id", None) != custody.profile_id
            or getattr(authority, "epoch", None) != custody.authority_epoch):
        return None, CustodyRefusal("peer_authority_mismatch", "I03_api_run_direct")
    return (admission_id, binding_id, generation), None


def peer_binding_source(
    custody: Any, record: dict[str, Any], authority: Any, binding_id: str, generation: Any,
    *, expected_parent: Any = None,
):
    db = getattr(authority, "db", None)
    if db is None:
        return None, CustodyRefusal("peer_durable_row_missing", "I03_api_run_direct")
    try:
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(db, admission_id=binding_id)
    except Exception:  # health: allow BLE001 -- unreadable durable ownership stays retained
        return None, CustodyRefusal("peer_durable_row_unavailable", "I03_api_run_direct")
    if not isinstance(row, dict):
        return None, CustodyRefusal("peer_durable_row_missing", "I03_api_run_direct")
    row_generation = row.get("generation")
    claimed_after_receipt = (
        generation is None
        and record.get("status") == "queued"
        and row.get("status") in {"started", "terminal"}
        and type(row_generation) is int
        and row_generation >= 0
    )
    if (
        row.get("admission_id") != binding_id
        or row.get("target_session_id") != record.get("session_id")
        or (row_generation != generation and not claimed_after_receipt)
        or row.get("owner_epoch") != custody.authority_epoch
        or not isinstance(row.get("principal_id"), str)
        or not row.get("principal_id")
    ):
        return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
    source = peer_source_kind(authority, record)
    if source is None:
        return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    principal = record.get("principal_id")
    if source == "bot_room":
        admission_principal = record.get("admission_principal_id")
        if (record.get("_api_producer_phase") != "bot_i02"
                or record.get("_api_producer_source") != "bot_chat"
                or (admission_principal is not None
                    and (not isinstance(admission_principal, str)
                         or not admission_principal
                         or admission_principal != row.get("principal_id")))):
            return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    else:
        if (principal is not None and principal != row.get("principal_id")):
            return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        if any(record.get(key) is not None for key in ("_api_producer_phase", "_api_producer_source")) and (
                record.get("_api_producer_phase") != "api_i02"
                or record.get("_api_producer_source") != "api_server"):
            return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    if expected_parent is None:
        parent, parent_kind = custody._parent_from_authority(authority, binding_id)
    else:
        parent = expected_parent
        parent_kind = getattr(parent, "owner_class", None)
        if parent_kind not in {"I02_api_session_turn", "I04_fifo_turn_execution"}:
            return None, CustodyRefusal("peer_parent_owner_mismatch", "I03_api_run_direct")
    if parent is None:
        return None, CustodyRefusal("peer_parent_unmapped", "I03_api_run_direct")
    return (row, source, parent, parent_kind), None


def peer_binding_replay(custody: Any, record: Any, authority: Any):
    """Re-read an already-bound peer using its same custody object and parent edge."""
    identity, refusal = peer_binding_identity(custody, record, authority)
    if refusal is not None:
        return refusal
    admission_id, binding_id, generation = identity
    if (authority is not getattr(custody, 'admission_authority', None)
            or custody.admission_id != binding_id
            or custody.execution_generation != generation
            or custody.parent_bound is not True):
        return CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
    bound_parent, refusal = custody.bound_parent_handle()
    if refusal is not None or bound_parent is None:
        return refusal or CustodyRefusal("peer_parent_unmapped", "I03_api_run_direct")
    source_data, source_refusal = peer_binding_source(
        custody, record, authority, binding_id, generation,
        expected_parent=bound_parent,
    )
    if source_refusal is None:
        if source_data[2] is not bound_parent:
            return CustodyRefusal("peer_parent_mismatch", "I03_api_run_direct")
        row_generation = source_data[0].get("generation")
        if custody.execution_generation is None and type(row_generation) is int and row_generation >= 0:
            custody.execution_generation = row_generation
        return None
    if source_refusal.code != "peer_parent_unmapped" or record.get('status') not in {
            'settled', 'failed', 'cancelled'}:
        return source_refusal
    return _peer_terminal_replay(
        custody, record, authority, admission_id, binding_id, generation, bound_parent,
    )


def _peer_terminal_replay(custody, record, authority, admission_id, binding_id, generation, parent):
    """Accept terminal replay only with canonical result and the retained parent edge."""
    db = getattr(authority, 'db', None)
    if db is None:
        return CustodyRefusal("peer_durable_row_missing", "I03_api_run_direct")
    try:
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(db, admission_id=binding_id)
        from gateway.session_results import admission_result
        result = admission_result(db, binding_id)
    except (OSError, RuntimeError, TypeError, ValueError, KeyError, sqlite3.Error):
        return CustodyRefusal("peer_terminal_readback_unavailable", "I03_api_run_direct")
    if (not isinstance(row, dict) or not isinstance(result, dict)
            or row.get('admission_id') != binding_id
            or row.get('status') != 'terminal'
            or row.get('generation') != generation
            or row.get('target_session_id') != record.get('session_id')
            or row.get('owner_epoch') != custody.authority_epoch
            or row.get('principal_id') != custody.producer_principal
            or record.get('admission_id') != admission_id):
        return CustodyRefusal("peer_terminal_identity_mismatch", "I03_api_run_direct")
    source = peer_source_kind(authority, record)
    if source is None:
        return CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    admission_principal = record.get("admission_principal_id")
    principal_matches = (
        admission_principal is None or admission_principal == custody.producer_principal
        if source == "bot_room"
        else (
            record.get("principal_id") is None
            or record.get("principal_id") == custody.producer_principal
        )
    )
    if (record.get('profile_id') != custody.profile_id
            or record.get('authority_epoch') != custody.authority_epoch
            or not principal_matches
            or (source == 'bot_room' and (
                record.get('_api_producer_phase') != 'bot_i02'
                or record.get('_api_producer_source') != 'bot_chat'))):
        return CustodyRefusal("peer_terminal_identity_mismatch", "I03_api_run_direct")
    if parent.coordinator_id != custody.handle.coordinator_id:
        return CustodyRefusal("peer_parent_mismatch", "I03_api_run_direct")
    return None
