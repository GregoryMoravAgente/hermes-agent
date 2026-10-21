"""Authenticated peer admission binding for one API run custody handle."""
from __future__ import annotations

from typing import Any

from gateway.runtime_custody import CustodyRefusal


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
    if (custody.admission_id is not None
            and (binding_id != custody.admission_id or generation != custody.execution_generation)):
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
    if (
        row.get("admission_id") != binding_id
        or row.get("target_session_id") != record.get("session_id")
        or row.get("generation") != generation
        or row.get("owner_epoch") != custody.authority_epoch
        or not isinstance(row.get("principal_id"), str)
        or not row.get("principal_id")
    ):
        return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
    session = getattr(db, "get_session", lambda _id: None)(record.get("session_id"))
    source = session.get("source") if isinstance(session, dict) else None
    if source not in {"bot_room", "api_server"}:
        return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    principal = record.get("principal_id")
    if principal is not None and principal != row.get("principal_id"):
        return None, CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
    if source == "bot_room":
        if (record.get("_api_producer_phase") != "bot_i02"
                or record.get("_api_producer_source") != "bot_chat"
                or principal != row.get("principal_id")):
            return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    elif any(record.get(key) is not None for key in ("_api_producer_phase", "_api_producer_source")):
        if (record.get("_api_producer_phase") != "api_i02"
                or record.get("_api_producer_source") != "api_server"):
            return None, CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
    parent, parent_kind = custody._parent_from_authority(authority, binding_id)
    if parent is None:
        return None, CustodyRefusal("peer_parent_unmapped", "I03_api_run_direct")
    return (row, source, parent, parent_kind), None
