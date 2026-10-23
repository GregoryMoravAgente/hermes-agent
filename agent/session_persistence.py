"""Durable transcript persistence for ``AIAgent`` (mixin; MRO-resolved from ``run_agent``): SQLite flush
with intrinsic ``_DB_PERSISTED_MARKER`` dedup, ephemeral-scaffolding filtering, explicit
trajectory export."""
import hashlib

import logging
import re
from contextlib import nullcontext

from typing import Any, Dict, List, Optional, Tuple

from agent.context_compressor import (
    COMPRESSED_SUMMARY_METADATA_KEY,
    _DB_PERSISTED_MARKER,
    ContextCompressor,
    _newest_checkpoint_carrier,
    _strip_persistence_markers,
    drop_shadowed_checkpoints,
    user_originated_turn_view,
)
from agent.lazy_forward import forward as _forward, forward_static as _forward_static
from agent.memory_context import sanitize_context

from agent.tool_dispatch_helpers import _is_multimodal_tool_result, _multimodal_text_summary
from agent.trajectory import save_trajectory as _save_trajectory_to_file
from agent.message_metadata import (
    DB_ROW_SNAPSHOT, MERGED_TURN_PREFIX, REPAIR_BOOKKEEPING_FIELDS, TOOL_CALL_UID, copy_identity_fields,
    tool_call_uid_from_history)
from agent.transcript_repair import sync_flushed_message_markers


logger = logging.getLogger("run_agent")  # origin module's name: log records / caplog filters unchanged

# Flags marking ephemeral recovery scaffolding the loop pops before appending the real response.
# Persistence must skip them or a resumed session replays synthetic turns / breaks prefix-cache reuse.
_EPHEMERAL_SCAFFOLDING_FLAGS = (
    "_empty_recovery_synthetic",
    "_empty_terminal_sentinel",
    "_thinking_prefill",
    "_verification_stop_synthetic",  # verify-on-stop nudge; the assistant candidate itself is NOT synthetic
    "_pre_verify_synthetic",
    "_kanban_stop_synthetic",  # kanban worker stop-guard
    "_dropped_toolcall_nudge",  # internal retry instruction; must not replay as user context
)

_IMAGE_PART_TYPES = {"image", "image_url", "input_image"}
# Reasoning/codex fields are role-gated (assistant-only) inside _insert_message_rows.
_ROW_REASONING_KEYS = ("reasoning", "reasoning_content", "reasoning_details", "codex_reasoning_items", "codex_message_items")
_PERSIST_AFTER_ADMISSION_INTERRUPT = "_persist_after_admission_interrupt"


def _is_ephemeral_scaffolding(msg: Any) -> bool:
    """True when ``msg`` is internal recovery scaffolding that must never reach the durable transcript."""
    return isinstance(msg, dict) and any(msg.get(flag) for flag in _EPHEMERAL_SCAFFOLDING_FLAGS)


# `_DB_PERSISTED_MARKER` (agent.context_compressor) is the intrinsic "already written to SQLite" marker: an
# id(msg) set can alias a freed dict's address onto a new message, a key on the dict cannot. The `_` prefix is
# mandatory (wire sanitizers strip `_` keys). CONTRACT: the marker asserts the dict's CONTENT is durable as
# written — any in-place mutation that must persist MUST pop it (turn_finalizer, context_compressor).


def _safe_session_filename_component(session_id: str) -> str:
    """Path-safe component for a (possibly untrusted ``X-Hermes-Session-Id``) ID: non ``[A-Za-z0-9_-]`` → ``_``,
    capped, plus a content hash when changed so distinct IDs cannot collide."""
    raw = str(session_id or "").strip()
    sanitized = re.sub(r"[^\w-]", "_", raw).strip("._")[:96] or "session"
    if raw and sanitized == raw:
        return sanitized
    return f"{sanitized}_{hashlib.sha256(raw.encode('utf-8', errors='surrogatepass')).hexdigest()[:12]}"


def _override_replaces_content(msg: dict, content: Any, override: Any) -> bool:
    """May the persist override replace ``content``? A plain-text override must not replace native image/audio
    blocks (a list override is the clean multimodal payload and does), nor a message MERGED with a compaction
    summary (overwriting would drop the summary)."""
    return (
        override is not None
        and not msg.get(COMPRESSED_SUMMARY_METADATA_KEY)
        and (not isinstance(content, list) or isinstance(override, list))
    )


def _content_with_turn_override(msg: dict, content: Any, override: Any) -> Any:
    """Replace only the absorbed current turn, leaving unanswered history intact. The join matches
    ``_merge_consecutive_users``; a row that no longer holds the prefix takes the plain override."""
    prefix = msg.get(MERGED_TURN_PREFIX)
    if isinstance(content, str) and isinstance(override, str) and isinstance(prefix, str):
        if content == prefix or content.startswith(prefix + "\n\n"):
            return prefix + "\n\n" + override if override else prefix
    return override


def durable_user_row_content(agent, msg: dict, content: Any, api_content: Any) -> tuple[Any, Any]:
    """``(content, api_content)`` as the current turn's user row is written: the persist override is the
    clean transcript, the live content is what the wire sent — so when they differ and nothing else was
    injected, the live bytes ARE the sidecar. Shared by the flush and the turn-start stamp so the stamp
    matches the row the flush wrote."""
    override = getattr(agent, "_persist_user_message_override", None)
    if _override_replaces_content(msg, content, override):
        if api_content is None and isinstance(content, str) and content != override:
            api_content = content
        content = _content_with_turn_override(msg, content, override)
    return content, api_content


def _summary_display_kind(msg: dict) -> Any:
    """Standalone handoffs are hidden so they never occupy the active user slot in retry/undo dispatch;
    merge-into-tail carriers keep their prior visibility."""
    if (
        msg.get(COMPRESSED_SUMMARY_METADATA_KEY)
        and user_originated_turn_view(msg) is None
        and (
            ContextCompressor.classify_summary_content(msg.get("content")) == "standalone"
            or not msg.get("_compressed_summary_has_user_turn")
        )
    ):
        return "hidden"
    return msg.get("display_kind")


def _durable_content(content: Any) -> Any:
    """Text-only DB projection: multimodal envelopes → summary; part lists keep text, images → ``[screenshot]``."""
    if _is_multimodal_tool_result(content):
        return _multimodal_text_summary(content)
    if not isinstance(content, list):
        return content
    txt = [
        str(p.get("text", "")) if p.get("type") == "text" else _image_marker(p)
        for p in content
        if isinstance(p, dict) and (p.get("type") == "text" or p.get("type") in _IMAGE_PART_TYPES)
    ]
    return "\n".join(txt) if txt else None


def _image_marker(part: dict) -> str:
    """``[screenshot]`` at the image's position; a caller-chosen ``detail`` rides along so a replay
    (``agent.image_replay``) rebuilds the part the live turn sent."""
    url = part.get("image_url")
    detail = part.get("detail") or (url.get("detail") if isinstance(url, dict) else None)
    return f"[screenshot detail={detail}]" if detail in ("low", "high", "auto") else "[screenshot]"


def _persist_lock(agent):
    """Close and turn-start persistence can run on separate CLI threads: one critical section.

    ``__init__`` always creates ``_session_persist_lock``; only ``object.__new__``-built test stubs lack it
    (they run unlocked, matching the historical ``if persist_lock is None`` branch).
    """
    lock = getattr(agent, "_session_persist_lock", None)
    return nullcontext() if lock is None else lock


def adopt_unanswered_turn(history: list[dict[str, Any]], query: Any, agent: Any) -> bool:
    """Re-stage the transcript's unanswered tail row as THIS turn's user message; True when adopted.

    A dispatcher's re-run of a failed delivery turn resumes the DM its first attempt already persisted
    instead of appending it again. Rows loaded from the store are born durable (``_rows_to_conversation``),
    so handing the tail row back as ``agent._pending_cli_user_message`` makes ``_stage_turn_user_message``
    reuse it as this turn's user dict and the flush writes no second row. The one user is the peer-DM
    lane's re-run (``resume_unanswered_turn`` in ``gateway.platforms.api_server``, #115325): in process it
    calls this on the agent it just built for the re-run; under the session authority the admission
    carries ``resume_unanswered`` and ``gateway.run_turn_runner`` calls it on the admitted history
    when its tail is that bare user row (a tool suffix there keeps the completed work in view).
    The owner-side Bot Chat retry (``gateway.session_bot._maybe_retry``) does not adopt: its failed
    turn is already closed by the durable failed-turn boundary, which this function declines by design.

    The DM is not always the literal tail: a turn that died mid-way persisted its tool scaffolding — assistant
    ``tool_calls`` rows and their ``tool`` results — behind the DM before the failure text was built, and the
    dispatcher retries that too. The DM is still unanswered while nothing after it is a plain assistant reply,
    so it is adopted and the failed attempt's scaffolding leaves the in-memory transcript: the re-run starts
    the turn over from the DM (the rows stay in the DB as the record of the failed attempt; the re-run's
    answer lands after them as a valid continuation). Anything else declines — no user row at the tail, or a
    different text there — so a person's deliberate re-send of the same text is never swallowed.
    """
    idx = next((i for i in range(len(history) - 1, -1, -1)
                if isinstance(history[i], dict) and history[i].get("role") == "user"), None)
    if idx is None or history[idx].get("content") != query:
        return False
    if not all(isinstance(row, dict) and (row.get("role") == "tool" or (row.get("role") == "assistant" and row.get("tool_calls")))
               for row in history[idx + 1:]):
        return False
    tail = history[idx]
    del history[idx:]
    tail[_DB_PERSISTED_MARKER] = True
    agent._pending_cli_user_message = tail
    return True


# --- flush phases (module-level so the flush also works bound onto duck-typed agents) ---

def _db_flush_seed_ids(agent) -> set:
    """One-shot ``_flushed_db_message_ids`` seed (same session, after a non-empty flush); the scan translates
    it to markers and the flush clears it."""
    current_session_id = getattr(agent, "session_id", None)
    same_session = getattr(agent, "_flushed_db_message_session_id", None) == current_session_id
    seed_ids = getattr(agent, "_flushed_db_message_ids", None) if same_session and agent._last_flushed_db_idx != 0 else None
    agent._flushed_db_message_session_id = current_session_id
    return seed_ids if isinstance(seed_ids, set) else set()


def _db_flush_scan_start(agent, messages: list[dict]) -> int:
    """Skip the identity-matched, still-marked prefix of the previous flush's snapshot."""
    scan_start = 0
    for prev, cur in zip(getattr(agent, "_db_flush_scan_prefix", None) or (), messages):
        if cur is not prev or not cur.get(_DB_PERSISTED_MARKER):
            break
        scan_start += 1
    return scan_start


def _db_flush_row(agent, msg: dict, is_current_turn_user: bool) -> dict[str, Any]:
    """Build the session-db row for ``msg``, applying the persist override to THIS row only."""
    role = msg.get("role", "unknown")
    content = msg.get("content")
    # api_content sidecar: exact bytes sent to the API when they differ from clean content (replay parity).
    api_content = msg.get("api_content") if isinstance(msg.get("api_content"), str) else None
    timestamp = msg.get("timestamp")
    if is_current_turn_user and role == "user":
        content, api_content = durable_user_row_content(agent, msg, content, api_content)
        ov_timestamp = getattr(agent, "_persist_user_message_timestamp", None)
        timestamp = timestamp if ov_timestamp is None else ov_timestamp
    if api_content == content:
        api_content = None
    # get_messages_as_conversation replays rows through sanitize_context().strip(); capture the sent bytes
    # when they would differ (compared in wire form).
    if (
        api_content is None and role in ("user", "assistant") and isinstance(content, str) and content
        and sanitize_context(content).strip() != content.strip()
    ):
        api_content = content
    # Key order is the divert-JSONL wire order (divert_session_transcript_jsonl).
    row = {
        "role": role, "content": _durable_content(content),
        "tool_name": msg.get("tool_name") or (msg.get("name") if role == "tool" else None),
        "tool_calls": msg["tool_calls"] if isinstance(msg.get("tool_calls"), list) else None,
        "tool_call_id": msg.get("tool_call_id"), "effect_disposition": msg.get("effect_disposition"),
        "token_count": msg.get("token_count"), "finish_reason": msg.get("finish_reason"),
        **{k: msg.get(k) for k in _ROW_REASONING_KEYS},
        "_compressed_summary": bool(msg.get(COMPRESSED_SUMMARY_METADATA_KEY)),
        "timestamp": timestamp, "api_content": api_content,
        "display_kind": _summary_display_kind(msg), "display_metadata": msg.get("display_metadata"),
        # Load-bearing for restart drain-window recovery dedup.
        "platform_message_id": msg.get("platform_message_id") or msg.get("message_id"),
        "observed": bool(msg.get("observed")),
    }
    if isinstance(msg.get("_row_id"), int):
        row["_row_id"] = msg["_row_id"]
    # The merge witness rides on the survivor's row (an owned column: a row-addressed rewrite of the
    # survivor writes it too), so a restart sees which rows the composite folded.
    copy_identity_fields(msg, row)
    if isinstance(msg.get(DB_ROW_SNAPSHOT), str):
        row[DB_ROW_SNAPSHOT] = msg[DB_ROW_SNAPSHOT]
    return row


def _db_flush_collect(agent, messages: list[dict], conversation_history: Optional[list[dict]],
                      replay_history: bool = False):
    """Scan for un-flushed messages; returns ``(rows, msgs)`` to write in one transaction. ``replay_history``
    (session-row heal) writes the history prefix again instead of stamping it as already durable."""
    seed_ids = _db_flush_seed_ids(agent)
    history_ids = {id(item) for item in (conversation_history or []) if isinstance(item, dict)}
    ov_idx = getattr(agent, "_persist_user_message_idx", None)
    # Also match the staged CLI dict by identity — the close safety-net may flush a shortened snapshot whose
    # turn index refers to the full history.
    pending_cli_message = getattr(agent, "_pending_cli_user_message", None)
    batch_rows: list[dict[str, Any]] = []
    batch_msgs: list[dict] = []
    tool_uid_owners: dict = {}  # tool_call_uid_from_history memo; the scanned dicts outlive this loop
    for msg_idx in range(_db_flush_scan_start(agent, messages), len(messages)):
        msg = messages[msg_idx]
        # Append-only flush: a mid-turn persist of scaffolding would commit a synthetic turn the end-of-turn
        # drop cannot un-write. Skip regardless of position.
        if not isinstance(msg, dict) or _is_ephemeral_scaffolding(msg) or msg.get(_DB_PERSISTED_MARKER):
            continue
        # Already durable (history copy or caller-seeded): stamp so future flushes skip it.
        is_history = id(msg) in history_ids
        if (
            (is_history and not replay_history) or id(msg) in seed_ids
        ) and not msg.get(_PERSIST_AFTER_ADMISSION_INTERRUPT):
            msg[_DB_PERSISTED_MARKER] = True
            continue
        if getattr(agent, "_mute_notification_reply", False) and not is_history:
            # Only new rows, never the cached history prefix. Keep evidence/model
            # context intact while transcript pollers omit unsolicited presentation.
            msg["display_kind"] = "hidden"
            msg["display_metadata"] = {**(msg.get("display_metadata") or {}), "notification_category": "diagnostic"}
        if msg.get("role") == "tool" and not msg.get(TOOL_CALL_UID):
            # A result whose call was flushed in an earlier batch: pair it with the uid that assistant row
            # minted (same-batch pairing happens inside the insert).
            if (tool_uid := tool_call_uid_from_history(messages, msg_idx, tool_uid_owners)) is not None:
                msg[TOOL_CALL_UID] = tool_uid
        batch_rows.append(_db_flush_row(agent, msg, ov_idx == msg_idx or msg is pending_cli_message))
        batch_msgs.append(msg)
    return batch_rows, batch_msgs


def _db_flush_write(agent, batch_rows: list[dict[str, Any]], batch_msgs: list[dict], messages: list[dict]) -> None:
    """One transaction for the turn's new rows: on failure nothing lands and no markers are stamped."""
    if not batch_rows:
        return
    agent._session_db.append_messages_batch(
        session_id=agent.session_id, messages=batch_rows,
        compression_lock_holder=getattr(agent, "_active_compression_lock_holder", None),
        turn_lease_holder=getattr(agent, "_active_session_turn_lease_holder", None),
        turn_lease_ttl_seconds=getattr(agent, "_active_session_turn_lease_ttl_seconds", 300.0) or 300.0,
    )
    sync_flushed_message_markers(batch_msgs, batch_rows)
    if _newest_checkpoint_carrier(batch_msgs, "codex_reasoning_items") >= 0:
        # The insert already rewrote the older rows (SessionDB._drop_shadowed_checkpoint_rows); mirror it on
        # the live transcript so forks/compaction built from memory carry one checkpoint too. Markers stay:
        # the rows are durable exactly as the dicts now read.
        drop_shadowed_checkpoints(messages)
    # The managed proof path opts into this capture only for the one canonical writer call.
    # Copy both writer-issued rows and the live message identity after the transaction has committed;
    # the proof helper then reads the actual stored rows back and hashes those values.
    if getattr(agent, "_managed_proof_capture", False):
        agent._managed_proof_batch = tuple(
            (id(message), dict(row), dict(message))
            for row, message in zip(batch_rows, batch_msgs)
        )


def _db_flush_adopt_compression_tip(agent) -> bool:
    """Adopt the live continuation of a compression-closed session. Same-id tip = no continuation; a tip
    whose row is missing or already ended is not adopted either."""
    old_id = agent.session_id
    try:
        tip = agent._session_db.get_compression_tip(old_id)
    except Exception as tip_exc:
        logger.warning("compression tip lookup failed for %s: %s", old_id, tip_exc)
        return False
    if not tip or tip == old_id:
        return False
    try:
        tip_row = agent._session_db.get_session(tip)
    except Exception:
        tip_row = None
    if tip_row is None or tip_row.get("ended_at") is not None:
        return False
    logger.warning("Adopted live compression tip %s for closed session %s; retrying flush once", tip, old_id)
    agent.session_id, agent._flushed_db_message_ids, agent._last_flushed_db_idx = tip, set(), 0
    agent._compression_adoption_failed = False
    return True


def _db_flush_session_row_gone(agent, session_id: Optional[str]) -> bool:
    """True only when ``session_id``'s row is confirmed absent; a failed lookup is not proof."""
    try:
        return agent._session_db.get_session(session_id) is None
    except Exception as lookup_exc:
        logger.warning("session row lookup failed for %s: %s", session_id, lookup_exc)
        return False


def _db_flush_failed(agent, e: Exception, batch_rows: list[dict[str, Any]], adoption_budget: int,
                     messages: list[dict]) -> Optional[str]:
    """Classify a failed flush and name the one retry the caller should take, or None to fail closed.

    ``"adopted"``: a compression-closed session moved onto its live tip. ``"healed"``: the session row was
    deleted under the live agent and has been recreated (unparented for a delegate child whose parent went
    with it); flush markers are reset so the caller replays the full in-memory transcript. Either retry is
    taken at most once (``adoption_budget``)."""
    agent._db_flush_scan_prefix = None  # full re-scan next flush: an exception mid-loop leaves mixed dispositions
    # The only place the SQLite error is visible before it becomes a bare False — classify it so the turn-end
    # explanation names the real cause.
    from hermes_state import StateDbCorruptError, StateDbReplacedError, classify_persistence_error, divert_session_transcript_jsonl
    from hermes_state_errors import CompressionSessionClosedError
    agent._last_persistence_error_cause = classify_persistence_error(e)
    if agent._last_persistence_error_cause == "session_row_missing":
        # The session row was removed under this live agent (`hermes sessions delete`, the Desktop/web
        # delete, bulk prune, a profile-repair move, an in-place store rebuild — none visible to the
        # cached agent, so the cached `_session_db_created` flag is stale and every later append hits
        # the FK). The deletion already erased the session's message rows with it, so the durable
        # transcript is empty: drop the stale flag, reset the flush markers, and replay the FULL
        # in-memory transcript onto the recreated row — not just the current tail (#123583).
        # The FK class also covers the sessions table's own parent/system-prompt FKs, and create is an
        # upsert: only heal when the row is really gone, or the replay would duplicate a live transcript.
        if adoption_budget <= 0 or not _db_flush_session_row_gone(agent, agent.session_id):
            return None
        _strip_persistence_markers(messages)
        # The durable history prefix is gone with the row: keep a replay pending until a write succeeds, so
        # a failed recreate or a failed retry write can't let a later flush stamp that prefix durable.
        agent._session_row_replay_pending = agent.session_id
        agent._flushed_db_message_ids = set()
        agent._last_flushed_db_idx = 0
        agent._session_db_created = False
        agent._ensure_db_session()
        parent_id = agent._parent_session_id
        if not agent._session_db_created and parent_id and _db_flush_session_row_gone(agent, parent_id):
            # Delegate child whose parent was deleted (cascade): the row's own parent FK would reject every
            # recreate. Create it unparented for this call only; the relay and hooks still use the parent id.
            agent._parent_session_id = None
            try:
                agent._ensure_db_session()
            finally:
                agent._parent_session_id = parent_id
        if not agent._session_db_created:
            # Row creation failed too (transient store trouble): don't append into a guaranteed
            # rollback — keep the batch unmarked so the next flush retries the whole thing. That flush
            # recreates the row up front (no FK error, no heal); the pending replay covers the history prefix.
            logger.warning("Session DB row for %s is missing and could not be recreated; will retry next flush",
                           getattr(agent, "session_id", None))
            return None
        logger.warning("Session DB row for %s was removed under the live agent; recreated it and replaying the transcript",
                       getattr(agent, "session_id", None))
        return "healed"
    if isinstance(e, (StateDbReplacedError, StateDbCorruptError)):
        # A replaced/quarantined handle will not take this batch again — keep it on disk.
        try:
            # The CAS digest / adopted row are local repair bookkeeping, not transcript payload.
            divert_session_transcript_jsonl(getattr(agent, "session_id", "") or "",
                                            [{k: v for k, v in r.items() if k not in REPAIR_BOOKKEEPING_FIELDS}
                                             for r in batch_rows])
        except Exception:
            logger.warning("JSONL divert failed after state.db %s for %s",
                           agent._last_persistence_error_cause, getattr(agent, "session_id", None), exc_info=True)
    if isinstance(e, CompressionSessionClosedError):
        # Compression race: another path rotated this session mid-write. Retry exactly once on the live tip; a
        # second closed-parent write fails closed.
        if adoption_budget > 0 and _db_flush_adopt_compression_tip(agent):
            return "adopted"
        agent._compression_adoption_failed = True  # lets the turn explanation name rotation, not full-disk advice
    logger.warning("Session DB append_message failed: %s", e)
    return None


_GATEWAY_WRITER_FALLTHROUGH = object()


def _gateway_writer_reuse_unknown(agent: Any, acp: Any) -> bool:
    """Stop before a second writer when a managed physical witness no longer matches."""
    from agent.session_persistence_proof import mark_gateway_writer_proof_unknown
    mark_gateway_writer_proof_unknown(agent, acp)
    agent._managed_persistence_failed = True
    if acp is not None:
        agent._mark_managed_persistence_unknown(acp)
    return False


def _finish_gateway_writer_request(
    agent: Any, messages: list[dict], conversation_history: Optional[list[dict]],
    requested_binding: Any, explicit_binding: bool, previous_proof: Any, reused: Any,
    acp: Any, flush_managed, flush_plain, staged, forget_proof, remember_proof,
    mark_gateway_writer_proof_unknown, retain_gateway_writer_proof_unknown,
) -> bool:
    from agent.session_persistence_proof import (
        _GATEWAY_WRITER_PREFIX_REPAIR, gateway_writer_proof_extends_prefix,
    )

    continue_with_proof = reused is _GATEWAY_WRITER_PREFIX_REPAIR
    if reused is not None and not continue_with_proof:
        agent._managed_shell_writer_proof = reused
        return True
    if not continue_with_proof:
        forget_proof(agent)
    writer = flush_managed if (staged(messages) or continue_with_proof) else flush_plain
    try:
        proof = writer(agent, messages, conversation_history)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        proof = None
    if proof is None:
        # The authenticated prefix remains available for the same-owner retry, but it never
        # stands in for a terminal suffix that this attempt did not prove.
        agent._managed_shell_writer_proof = previous_proof if continue_with_proof else None
        agent._managed_persistence_failed = True
        mark_gateway_writer_proof_unknown(agent, acp)
        return False
    if continue_with_proof and not gateway_writer_proof_extends_prefix(previous_proof, proof):
        agent._managed_shell_writer_proof = previous_proof
        agent._managed_persistence_failed = True
        mark_gateway_writer_proof_unknown(agent, acp)
        return False
    agent._managed_shell_writer_proof = proof
    remembered = remember_proof(agent, proof, requested_binding, explicit_binding)
    if acp is not None and remembered is not True:
        if continue_with_proof:
            agent._managed_shell_writer_proof = previous_proof
            mark_gateway_writer_proof_unknown(agent, acp)
        else:
            retain_gateway_writer_proof_unknown(
                agent, proof, requested=requested_binding, explicit=explicit_binding,
            )
        agent._managed_persistence_failed = True
        if acp is not None:
            agent._mark_managed_persistence_unknown(acp)
        return False
    if remembered is not True and continue_with_proof:
        agent._managed_shell_writer_proof = previous_proof
        mark_gateway_writer_proof_unknown(agent, acp)
        agent._managed_persistence_failed = True
        return False
    return True


def _flush_gateway_writer_request(
    agent: Any, messages: list[dict], conversation_history: Optional[list[dict]],
    requested_binding: Any, explicit_binding: bool,
    flush_managed, flush_plain, staged, forget_proof, remember_proof, reuse_proof,
):
    """Run the optional direct writer proof without replacing the managed ACP branch."""
    from agent.session_persistence_proof import _GATEWAY_WRITER_UNKNOWN
    from agent.session_persistence_proof import (
        mark_gateway_writer_proof_unknown,
        retain_gateway_writer_proof_unknown,
    )
    if explicit_binding and requested_binding is None:
        forget_proof(agent)
        agent._managed_persistence_failed = True
        return False
    acp = getattr(agent, "_managed_acp_client", None)
    if explicit_binding:
        if acp is not None and acp.has_pending_managed_witness():
            agent._managed_persistence_failed = True
            agent._mark_managed_persistence_unknown(acp)
            return False
        previous_proof = getattr(agent, "_managed_shell_writer_proof", None)
        reused = reuse_proof(
            agent, messages, previous_proof, requested_binding, explicit_binding,
        ) if previous_proof is not None else None
        if reused is _GATEWAY_WRITER_UNKNOWN:
            return _gateway_writer_reuse_unknown(agent, acp)
        return _finish_gateway_writer_request(
            agent, messages, conversation_history, requested_binding, explicit_binding,
            previous_proof, reused, acp, flush_managed, flush_plain, staged, forget_proof,
            remember_proof, mark_gateway_writer_proof_unknown, retain_gateway_writer_proof_unknown,
        )
    if acp is not None and acp.has_pending_managed_witness():
        return _GATEWAY_WRITER_FALLTHROUGH
    previous_proof = getattr(agent, "_managed_shell_writer_proof", None)
    reused = reuse_proof(
        agent, messages, previous_proof, requested_binding, explicit_binding,
    ) if previous_proof is not None else None
    if reused is _GATEWAY_WRITER_UNKNOWN:
        return _gateway_writer_reuse_unknown(agent, acp)
    return _finish_gateway_writer_request(
        agent, messages, conversation_history, requested_binding, explicit_binding,
        previous_proof, reused, acp, flush_managed, flush_plain, staged, forget_proof,
        remember_proof, mark_gateway_writer_proof_unknown, retain_gateway_writer_proof_unknown,
    )



def _retry_managed_post_commit_proof(agent: Any, acp: Any, messages: list[dict]) -> bool:
    """Retry only the retained same-owner receipt without admitting another writer."""
    # The finalizer may have discarded only the one-use request.  A retained
    # managed proof still blocks every fallback writer.  First give the same
    # authenticated owner one strict post-commit witness/readback recovery; this
    # clears only the writer marker and never admits a child or writes another row.
    agent._managed_persistence_failed = True
    if acp is not None:
        parent = getattr(getattr(acp, "_producer_policy", None), "parent", None)
        service = getattr(parent, "_service", None)
        binding = getattr(parent, "turn_binding", None)
        request = (
            (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
            if service is not None and binding is not None else None
        )
        recover = getattr(acp, "_retry_managed_writer_proof", None)
        recovered = False
        if callable(recover) and parent is not None and service is not None and binding is not None and request is not None:
            try:
                recovered = recover(agent, parent, service, binding, request, messages) is True
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                logger.warning("managed post-commit proof recovery refused", exc_info=True)
        if recovered:
            return True
        agent._mark_managed_persistence_unknown(acp)
    return False


def _flush_managed_intermediate_terminal(
    agent: Any, acp: Any, intermediate_fence: Any,
    messages: list[dict], conversation_history: Optional[list[dict]],
) -> bool:
    """Extend the existing child's receipt after the finalizer shapes the canonical tail."""
    from agent.session_persistence_proof import (
        flush_messages_with_managed_current_turn_proof, flush_messages_with_proof,
        managed_current_turn_is_staged, mark_managed_intermediate_writer_fence_unknown,
    )
    terminal_proof = (
        flush_messages_with_managed_current_turn_proof(agent, messages, conversation_history)
        if managed_current_turn_is_staged(messages)
        else flush_messages_with_proof(agent, messages, conversation_history)
    )
    commit_terminal = getattr(acp, "_commit_managed_intermediate_terminal_proof", None)
    if callable(commit_terminal):
        if commit_terminal(agent, intermediate_fence, terminal_proof, messages) is True:
            # A retry may have no new intended rows; the helper then
            # consumed the exact retained terminal proof from the fence.
            agent._managed_shell_writer_proof = (
                terminal_proof
                if terminal_proof is not None
                else getattr(intermediate_fence, "writer_proof", None)
            )
            return True
    mark_managed_intermediate_writer_fence_unknown(
        agent, acp, "managed_intermediate_terminal_without_child",
    )
    agent._managed_persistence_failed = True
    return False


def _flush_pending_managed_child(
    agent: Any, acp: Any, messages: list[dict], conversation_history: Optional[list[dict]],
    gateway_requested: bool, requested_binding: Any, explicit_binding: bool,
) -> bool:
    """Commit a pending child's rows and retain its exact physical handoff evidence."""
    from agent.session_persistence_proof import (
        consume_managed_intermediate_writer_fence, flush_messages_with_managed_current_turn_proof,
        flush_messages_with_proof, forget_gateway_writer_proof, managed_current_turn_is_staged,
        mark_gateway_writer_proof_unknown, mark_managed_intermediate_writer_fence_unknown,
        remember_gateway_writer_proof, retain_gateway_writer_proof_unknown,
        retain_managed_intermediate_writer_fence,
    )
    from agent.session_persistence_proof import SessionDBWriteProof
    physical_witness = None
    proof = None
    previous_proof = getattr(agent, "_managed_shell_writer_proof", None)
    durable_committed = False
    intermediate = (
        not getattr(agent, '_managed_finalize_shell_turn', False)
        and callable(getattr(acp, '_managed_intermediate_message', None))
        and acp._managed_intermediate_message(messages) is not None
    )
    try:
        proof = (
            flush_messages_with_managed_current_turn_proof(agent, messages, conversation_history)
            if managed_current_turn_is_staged(messages)
            else flush_messages_with_proof(agent, messages, conversation_history)
        )
        if not intermediate and type(proof) is SessionDBWriteProof:
            # Shell-hook children use the managed ACP writer's exact SessionDB proof even when
            # this API route has no separate Gateway writer request.  Keep the proof only while
            # an operation is live; the finalizer consumes it through commit_turn_result, which
            # still validates the complete assistant/tool suffix before releasing each sidecar.
            if gateway_requested or bool(getattr(agent, "_shell_hook_custody_operations", None)):
                agent._managed_shell_writer_proof = proof
        commit_writer = (acp._commit_managed_intermediate_writer_proof if intermediate
                         else acp._commit_managed_writer_proof)
        committed = proof is not None and commit_writer(
            agent, proof, messages,
        ) is True
        # The bool writer API is insufficient custody evidence by itself: the
        # exact readback proof must still be attached to this admission's
        # physical receipt before finalization can observe success.
        committed = committed and acp.has_managed_durable_receipt(
            getattr(proof, "session_id", ""),
        )
        durable_committed = committed
        capture_witness = getattr(acp, "capture_managed_durable_receipt_witness", None)
        if committed and callable(capture_witness):
            physical_witness = capture_witness(agent, proof)
            committed = physical_witness is not None
        elif committed and getattr(acp, "_producer_policy", None) is not None:
            # A real managed ACP client must supply the exact receipt/provider witness;
            # a helper-only bool cannot cross the production custody fence.
            committed = False
        if committed and gateway_requested and not intermediate:
            committed = remember_gateway_writer_proof(
                agent, proof, requested_binding, explicit_binding, physical_witness,
            ) is True
        if committed and intermediate:
            # The child keeps its typed durable proof and physical custody. A tool
            # envelope is not the final writer handoff for the enclosing I04 turn.
            forget_gateway_writer_proof(agent)
            committed = retain_managed_intermediate_writer_fence(
                agent, acp, proof, physical_witness,
            ) is True
        elif committed and not intermediate:
            # Consume the prefix only after the next proof is a strict same-receipt
            # SessionDB/readback extension; the final handoff remains below.
            fence_result = consume_managed_intermediate_writer_fence(
                agent, acp, proof, physical_witness, messages,
            )
            if fence_result is False:
                committed = False
                mark_managed_intermediate_writer_fence_unknown(
                    agent, acp, "managed_intermediate_terminal_mismatch",
                )
    except Exception:
        logger.warning("managed ACP durable handoff raised; retaining unknown custody", exc_info=True)
        committed = False
    if not committed:
        retained_proof = proof if type(proof) is SessionDBWriteProof else previous_proof
        retained = retain_gateway_writer_proof_unknown(
            agent, retained_proof, physical_witness,
            requested=requested_binding, explicit=explicit_binding,
        )
        agent._managed_persistence_failed = True
        # A post-commit witness/capture failure keeps the exact durable receipt and
        # physical owner usable for the authenticated same-client proof recovery.  The
        # writer marker remains sticky below, so no later call can create a second
        # writer while that narrow recovery is pending.  Pre-commit failures retain
        # the existing service-level unknown transition.
        if not durable_committed:
            agent._mark_managed_persistence_unknown(acp)
        if not retained:
            # No typed post-effect proof was available to retain.  Keep the
            # owner fail-closed; never turn this into an ordinary writer.
            mark_gateway_writer_proof_unknown(agent, acp)
        return False
    return True


class SessionPersistenceMixin:
    """Session DB flush and trajectory persistence (see module docstring)."""

    def _apply_persist_user_message_override(self, messages: list[dict]) -> None:
        """Rewrite the current-turn user message in place: some paths send an API-only variant that must not
        leak into transcripts or resumed history."""
        idx = getattr(self, "_persist_user_message_idx", None)
        override = getattr(self, "_persist_user_message_override", None)
        timestamp = getattr(self, "_persist_user_message_timestamp", None)
        platform_id = getattr(self, "_persist_user_message_platform_id", None)
        if idx is None or (override is None and timestamp is None and platform_id is None):
            return
        msg = messages[idx] if 0 <= idx < len(messages) else None
        if not (isinstance(msg, dict) and msg.get("role") == "user"):
            return
        if _override_replaces_content(msg, msg.get("content"), override):
            msg["content"] = _content_with_turn_override(msg, msg.get("content"), override)
        if timestamp is not None:
            msg["timestamp"] = timestamp
        if platform_id is not None:  # load-bearing for restart drain-window recovery dedup (has_platform_message_id)
            msg["platform_message_id"] = platform_id

    def _request_gateway_writer_proof(self, *args, **kwargs) -> bool:
        """Request one exact writer proof, preserving the no-argument I04 fallback."""
        from agent.session_persistence_proof import request_gateway_writer_proof
        if "binding" in kwargs:
            return request_gateway_writer_proof(self, kwargs["binding"])
        if args:
            return request_gateway_writer_proof(self, args[0])
        return request_gateway_writer_proof(self)

    def _peek_gateway_writer_proof(self):
        from agent.session_persistence_proof import peek_gateway_writer_proof
        return peek_gateway_writer_proof(self)

    def _ack_gateway_writer_proof(self, handoff) -> bool:
        from agent.session_persistence_proof import acknowledge_gateway_writer_proof
        return acknowledge_gateway_writer_proof(self, handoff)

    def _persist_session(self, messages: list[dict], conversation_history: list[dict] | None = None):
        """Save to SQLite on any exit path. Trailing empty-response scaffolding is dropped from
        the live list; the persist override is applied to the DB row only.

        The persist user-message *override* is NOT applied here — it is resolved inside
        ``_flush_messages_to_session_db`` and written only to the DB row, never mutating the live message
        list used by the API call (#48677 is thus closed for every persist caller, not just this one).
        """
        from agent.agent_runtime_helpers import note_turn_persisted
        with _persist_lock(self):
            # Only the scaffolding goes here. Closing a tool tail this uncovers is the exit
            # owner's job (``_close_transcript_tail``, ``abort_turn_on_interrupt``): only it knows
            # the reason to record.
            self._drop_trailing_empty_response_scaffolding(messages)
            self._session_messages = messages
            persisted = self._flush_messages_to_session_db(messages, conversation_history)
            if persisted is False and getattr(self, "_managed_persistence_failed", False):
                return False
            # Drain async token-accounting deltas at every persist point; cheap no-op when nothing queued.
            if self._session_db is not None:
                self._session_db.flush_token_counts()
            note_turn_persisted(self)
            return persisted

    def _drop_trailing_empty_response_scaffolding(self, messages: list[dict]) -> None:
        """Pop empty-response retry scaffolding from the tail. The
        assistant(tool_calls) / tool rows before it stay: they were saved before the tools ran, so
        dropping them from the live history only makes the model repeat a side effect the durable
        transcript already records."""
        def tail(*keys: str) -> bool:
            return bool(messages) and isinstance(messages[-1], dict) and any(messages[-1].get(k) for k in keys)

        while tail("_empty_recovery_synthetic", "_empty_terminal_sentinel"):
            messages.pop()

    _repair_message_sequence = _forward("agent.agent_runtime_helpers", "repair_message_sequence")

    @staticmethod
    def _mark_managed_persistence_unknown(acp: Any) -> None:
        parent = getattr(getattr(acp, "_producer_policy", None), "parent", None)
        try:
            if parent is not None:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "managed_durable_handoff_failed")
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            # The caller still records the local false/unknown outcome; no permissive retry is allowed.
            logger.warning("managed ACP custody unknown marker failed", exc_info=True)

    def _flush_messages_to_session_db(self, messages: list[dict], conversation_history: Optional[list[dict]] = None):
        """Serialize canonical writes and prove managed ACP results from the same transaction."""
        with _persist_lock(self):
            if getattr(self, "_managed_proof_capture", False):
                return self._flush_messages_to_session_db_unlocked(messages, conversation_history)
            from agent.session_persistence_proof import (
                _GATEWAY_WRITER_PROOF_UNKNOWN,
                _GATEWAY_WRITER_UNKNOWN,
                _requested_gateway_binding,
                forget_gateway_writer_proof,
                flush_messages_with_managed_current_turn_proof,
                flush_messages_with_proof,
                gateway_writer_proof_requested,
                managed_intermediate_writer_fence,
                mark_managed_intermediate_writer_fence_unknown,
                recover_managed_intermediate_writer_fence,
                managed_current_turn_is_staged,
                remember_gateway_writer_proof,
                reuse_gateway_writer_proof,
            )
            acp = getattr(self, "_managed_acp_client", None)
            # Inspect the fence independently of the current client.  A cleared, swapped, or
            # late-cleaned client must make the retained token UNKNOWN, never invisible.
            intermediate_fence = managed_intermediate_writer_fence(self, acp)
            if intermediate_fence is _GATEWAY_WRITER_UNKNOWN:
                self._managed_persistence_failed = True
                mark_managed_intermediate_writer_fence_unknown(
                    self, acp, "managed_intermediate_owner_mismatch", mark_parent=True,
                )
                if acp is not None:
                    self._mark_managed_persistence_unknown(acp)
                return False
            if intermediate_fence is not None and getattr(intermediate_fence, "state", None) == "unknown":
                recovered = False
                if acp is not None:
                    try:
                        recovered = recover_managed_intermediate_writer_fence(self, acp)
                    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                        logger.warning("managed intermediate fence recovery refused", exc_info=True)
                if not recovered:
                    self._managed_persistence_failed = True
                    return False
                intermediate_fence = managed_intermediate_writer_fence(self, acp)
            if getattr(self, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is True and intermediate_fence is None:
                return _retry_managed_post_commit_proof(self, acp, messages)
            gateway_requested = gateway_writer_proof_requested(self)
            requested_binding, explicit_binding = _requested_gateway_binding(self)
            if gateway_requested and intermediate_fence is None:
                gateway_result = _flush_gateway_writer_request(
                    self, messages, conversation_history, requested_binding, explicit_binding,
                    flush_messages_with_managed_current_turn_proof, flush_messages_with_proof,
                    managed_current_turn_is_staged, forget_gateway_writer_proof,
                    remember_gateway_writer_proof, reuse_gateway_writer_proof,
                )
                if gateway_result is not _GATEWAY_WRITER_FALLTHROUGH:
                    return gateway_result
            if acp is not None and acp.has_pending_managed_witness():
                return _flush_pending_managed_child(
                    self, acp, messages, conversation_history,
                    gateway_requested, requested_binding, explicit_binding,
                )
            if intermediate_fence is not None:
                # A finalizer-shaped no-next-child tail still has a supported terminal path:
                # write the exact rows, extend the existing child proof on the same receipt,
                # verify native close, and leave publication to the existing Gateway handoff.
                if getattr(self, "_managed_finalize_shell_turn", False) and acp is not None:
                    return _flush_managed_intermediate_terminal(
                        self, acp, intermediate_fence, messages, conversation_history,
                    )
                # Intermediate tool checkpoints remain canonical SessionDB checkpoints, but
                # they never become final Gateway proof by themselves.
                return self._flush_messages_to_session_db_unlocked(messages, conversation_history)
            return self._flush_messages_to_session_db_unlocked(messages, conversation_history)

    def _flush_messages_to_session_db_with_proof(
        self, messages: list[dict], conversation_history: Optional[list[dict]] = None,
    ):
        """Return a typed committed-row witness for managed custody; preserve the bool API above."""
        from agent.session_persistence_proof import flush_messages_with_proof
        with _persist_lock(self):
            return flush_messages_with_proof(self, messages, conversation_history)

    def _flush_messages_to_session_db_unlocked(
        self, messages: list[dict], conversation_history: Optional[list[dict]] = None, _adoption_budget: int = 1,
    ):
        """Persist un-flushed messages to SQLite. Dedup is the intrinsic ``_DB_PERSISTED_MARKER`` on each written
        dict — not positional slices (drift after sequence repair) nor an ``id(msg)`` set (address reuse). The
        persist override touches the written row only. A compression-closed session adopts its live tip and
        retries exactly once.

        Deduplicates via an intrinsic ``_DB_PERSISTED_MARKER`` stamped on each written message dict, so
        repeated calls (from multiple exit paths) only write truly new messages — preventing the
        duplicate-write bug (#860) without relying on positional slices that can drift after
        message-sequence repair, and without a retained ``id(msg)`` set that CPython could alias onto a
        freed-then-reused address (#50372). The ``_flushed_db_message_ids`` attribute is now only a one-shot
        seed (translated to markers, then cleared each flush), not a persisted set.
        """
        # Persistence-isolated agents (background review fork) share the parent's session_id for cache warmth;
        # a write here would land the curator's turn in the user's real history.
        if getattr(self, "_persist_disabled", False) or not self._session_db:
            return None
        batch_rows: list[dict[str, Any]] = []
        try:
            if not self._session_db_created:  # retry row creation if the earlier attempt failed transiently
                self._ensure_db_session()
            # getattr: object.__new__ test agents flush without running AIAgent init.
            replay = getattr(self, "_session_row_replay_pending", None) == self.session_id
            batch_rows, batch_msgs = _db_flush_collect(self, messages, conversation_history, replay)
            _db_flush_write(self, batch_rows, batch_msgs, messages)
            self._session_row_replay_pending = None
            # Markers are now the sole truth; reset the one-shot seed so no id() outlives this flush.
            self._flushed_db_message_ids = set()
            self._last_flushed_db_idx = len(messages)
            # Snapshot for the bounded scan — only on full success, so a partial list is never treated as settled.
            self._db_flush_scan_prefix = messages[:]
            return True
        except Exception as e:
            retry = _db_flush_failed(self, e, batch_rows, _adoption_budget, messages)
            if retry is None:
                return False
            # After a heal the pending replay re-sends the history prefix instead of stamping it durable.
            return self._flush_messages_to_session_db_unlocked(messages, conversation_history, _adoption_budget=0)

    def _get_messages_up_to_last_assistant(self, messages: list[dict]) -> list[dict]:
        """Messages before the last assistant turn (rollback point for a malformed final answer); all if none."""
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "assistant":
                return messages[:i]
        return messages.copy()

    _format_tools_for_system_message = _forward("agent.system_prompt", "format_tools_for_system_message")
    _convert_to_trajectory_format = _forward("agent.agent_runtime_helpers", "convert_to_trajectory_format")

    def _save_trajectory(self, messages: list[dict[str, Any]], user_query: str, completed: bool):
        """Save conversation trajectory to JSONL file."""
        if not self.save_trajectories:
            return
        trajectory = self._convert_to_trajectory_format(messages, user_query, completed)
        _save_trajectory_to_file(trajectory, self.model, completed)

    _extract_api_error_context = _forward_static("agent.agent_runtime_helpers", "extract_api_error_context")
    _dump_api_request_debug = _forward("agent.agent_runtime_helpers", "dump_api_request_debug")
