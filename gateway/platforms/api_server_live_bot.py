"""I03 custody bridge for API requests handed to a live Bot Chat owner.

The mailbox and FIFO remain owned by their existing producers.  This module only carries the
API run's opaque custody object across that handoff and records the transport receipts at the
API edge; it never discovers a replacement authority or manufactures a peer generation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any, Optional


def reserve_peer_custody(self, profile_id: Optional[str], run_id: str):
    """Reserve I03 before a session-chat route probes or hands to a Bot owner."""
    from gateway.platforms.api_server_run_custody import reserve_legacy
    return reserve_legacy(self, profile_id, run_id)


def _peer_source_entries(adapter, run_id: str, custody):
    """Return the exact peer custody/launch entries, or refuse an ambiguous index."""
    launch = getattr(custody, "source_launch", None)
    if (
        getattr(custody, "run_id", None) != run_id
        or getattr(launch, "owner", None) is not adapter
        or getattr(launch, "run_id", None) != run_id
        or getattr(launch, "custody", None) is not custody
    ):
        return None
    custody_table = getattr(adapter, "_run_custody", None)
    launch_table = getattr(adapter, "_run_launches", None)
    if (
        not isinstance(custody_table, dict)
        or not isinstance(launch_table, dict)
        or custody_table.get(run_id) is not custody
        or launch_table.get(run_id) is not launch
    ):
        return None
    return custody_table, launch_table


def _drop_peer_source_entries(adapter, run_id: str, custody) -> bool:
    """Drop both exact peer indexes after a completed unaccepted/terminal release."""
    entries = _peer_source_entries(adapter, run_id, custody)
    if entries is None:
        return False
    custody_table, launch_table = entries
    custody_table.pop(run_id, None)
    launch_table.pop(run_id, None)
    release_owner = getattr(adapter, "_release_run_owner_if_forgotten", None)
    if callable(release_owner):
        release_owner(run_id)
    return True


def release_peer_custody(self, run_id: str, custody) -> None:
    """Forget a pre-handoff reservation only when it was never accepted or is terminal."""
    if custody is None:
        return
    if (getattr(custody, "unknown_reason", None)
            or getattr(custody, "parent_bound", False)
            or getattr(custody, "status_persisted", False)):
        return
    if custody.released:
        _drop_peer_source_entries(self, run_id, custody)
        return
    if _peer_source_entries(self, run_id, custody) is None:
        return
    refusal = custody.release_unaccepted()
    if refusal is not None:
        custody.mark_unknown("accepted_row_unknown")
        return
    if not _drop_peer_source_entries(self, run_id, custody):
        custody.mark_unknown("peer_source_index_changed")


def _unknown(custody, reason: str):
    if custody is not None:
        custody.mark_unknown(reason)


def _status_projection(custody):
    if custody is None:
        return None
    return custody.mark_status_receipt({
        "committed": True,
        "storage": "in_memory_projection",
        "rowcount": 1,
        "run_id": custody.run_id,
    })


def _finish_projection(custody, *, event_enqueued: bool, stream_closed: bool,
                       transport_retired: bool = False):
    """Close one unkeyed session-chat projection after its status is already ordered."""
    if custody is None or custody.unknown_reason:
        return
    if not custody.worker_submitted:
        _unknown(custody, "peer_worker_submission_missing")
        return
    custody.mark_worker_done()
    if transport_retired:
        custody.mark_transport_retired()
    else:
        custody.mark_event_enqueued(event_enqueued)
        custody.mark_stream_closed(stream_closed)
    custody.mark_control_finalizers()


def _status_proven(custody, refusal) -> bool:
    """A prior owner/transport ambiguity keeps the public terminal event unknown."""
    return refusal is None and not (custody is not None and custody.unknown_reason)


async def admit_to_live_bot_chat(self, session_id: str, message: Any,
                                author: Optional[dict[str, Any]], *, custody=None):
    """Return ``(profile home, receipt)`` for a live owner, carrying *custody* through admission."""
    if not isinstance(message, str):
        return None
    if getattr(self.gateway_runner, "session_authority", None) is not None:
        from gateway.platforms.api_server_bot_chat import admit_peer_turn
        return await admit_peer_turn(
            self.gateway_runner, session_id, message, author, custody=custody)
    db = await self._ensure_session_db_async()
    if db is None:
        return None
    home = Path(db.db_path).parent
    from tools.bot_live_delivery import deliver_to_live_owner, find_canonical_live_owner

    def _admit() -> Optional[dict[str, Any]]:
        try:
            owner = find_canonical_live_owner(home)
        except ValueError:
            return None
        if owner is None or db.get_compression_tip(session_id) != owner["session_id"]:
            return None
        return deliver_to_live_owner(home, owner, message, author=author)

    record = await asyncio.to_thread(_admit)
    if record is not None and custody is not None:
        # The legacy mailbox has no in-process authority handle to bind.  Its generation is useful
        # evidence for a later operator audit, but cannot release this process's opaque handle.
        _unknown(custody, "legacy_peer_owner_unmapped")
    return None if record is None else (home, record)


async def await_live_bot_chat_receipt(self, home: Path, record: dict[str, Any], *, keepalive=None) -> dict[str, Any]:
    """Wait on either the authority FIFO receipt or the legacy owner mailbox."""
    from gateway.platforms.api_server_bot_chat import await_live_delivery
    from tools.bot_mode_dm import _LIVE_WAIT_SECONDS
    from gateway.platforms.api_server import CHAT_COMPLETIONS_SSE_KEEPALIVE_SECONDS
    deadline = time.monotonic() + _LIVE_WAIT_SECONDS
    while record["status"] in ("queued", "claimed"):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        budget = remaining if keepalive is None else min(remaining, CHAT_COMPLETIONS_SSE_KEEPALIVE_SECONDS)
        record = await await_live_delivery(self, home, record, budget)
        if keepalive is not None and record["status"] in ("queued", "claimed"):
            await keepalive()
    return record


async def answer_through_live_bot_chat(self, ctx: dict[str, Any], *, custody=None):
    """Handle the synchronous peer-DM response and finalize its unkeyed custody projection."""
    session_id = ctx["session_id"]
    admitted = await self._admit_to_live_bot_chat(
        session_id, ctx["user_message"], ctx["run_kwargs"].get("turn_author"), custody=custody)
    if admitted is None:
        return None
    admitted_record = admitted[1] if isinstance(admitted[1], dict) else {}
    session_id = admitted_record.get("session_id") or session_id
    # Keep the adapter-owned receipt seam: deployments and tests may extend the waiter
    # without bypassing the authority/mailbox contract implemented by this module.
    record = await self._await_live_bot_chat_receipt(*admitted)
    delivery_id = record["delivery_id"]
    headers = self._session_headers(session_id, ctx["gateway_session_key"])
    if record["status"] == "settled":
        status_refusal = _status_projection(custody)
        if status_refusal is None:
            _finish_projection(custody, event_enqueued=True, stream_closed=True, transport_retired=True)
        else:
            _unknown(custody, getattr(status_refusal, "code", "status_commit_unproven"))
        from aiohttp import web
        return web.json_response(
            {"object": "hermes.session.chat.completion", "session_id": session_id,
             "message": {"role": "assistant", "content": record.get("reply") or ""},
             "usage": {}, "runtime": {}, "delivery_id": delivery_id}, headers=headers)
    if record["status"] in ("queued", "claimed"):
        _unknown(custody, "peer_delivery_pending")
        from aiohttp import web
        return web.json_response(
            {"object": "hermes.session.chat.queued", "session_id": session_id,
             "status": record["status"], "delivery_id": delivery_id}, status=202, headers=headers)
    status_refusal = _status_projection(custody)
    if status_refusal is None:
        _finish_projection(custody, event_enqueued=True, stream_closed=True, transport_retired=True)
    else:
        _unknown(custody, getattr(status_refusal, "code", "status_commit_unproven"))
    from gateway.platforms.api_server import _error_response
    return _error_response(
        record.get("error") or f"Bot Chat delivery {record['status']}", 502,
        code=record.get("reason") or record["status"], headers=headers)


async def stream_through_live_bot_chat(self, request, ctx: dict[str, Any], *, custody=None):
    """SSE peer-DM path with status receipt before its terminal event."""
    from gateway.platforms.api_server import CHAT_COMPLETIONS_SSE_KEEPALIVE_SECONDS, _sse_frame
    from gateway.platforms.api_server import _SessionEventQueue

    session_id = ctx["session_id"]
    admitted = await self._admit_to_live_bot_chat(
        session_id, ctx["user_message"], ctx["run_kwargs"].get("turn_author"), custody=custody)
    if admitted is None:
        return None
    admitted_record = admitted[1] if isinstance(admitted[1], dict) else {}
    session_id = admitted_record.get("session_id") or session_id
    events = _SessionEventQueue(session_id, f"run_{uuid.uuid4().hex}")
    response = await self._prepare_sse_response(request, session_id, ctx["gateway_session_key"])

    async def _write(name: str, payload: dict[str, Any]) -> None:
        name, payload = events.payload(name, payload)
        await response.write(_sse_frame(payload, event=name, ensure_ascii=False))

    async def _keepalive() -> None:
        await response.write(b": keepalive\n\n")

    disconnected = False
    try:
        await _write("run.started", {"user_message": {"role": "user", "content": ctx["user_message"]}, "runtime": {}})
        # The adapter method is the supported extension point for both authority FIFO and
        # legacy mailbox receipts; do not call the module helper behind that seam directly.
        record = await self._await_live_bot_chat_receipt(*admitted, keepalive=_keepalive)
        delivery_id = record["delivery_id"]
        if record["status"] == "settled":
            message_id = f"msg_{uuid.uuid4().hex}"
            await _write("message.started", {"message": {"id": message_id, "role": "assistant"}})
            await _write("assistant.completed", {
                "message_id": message_id, "content": record.get("reply") or "",
                "delivery_id": delivery_id, "runtime": {}})
            status_refusal = _status_projection(custody)
            terminal = "run.completed" if _status_proven(custody, status_refusal) else "run.unknown"
            await _write(terminal, {"message_id": message_id, "delivery_id": delivery_id,
                                    "usage": {}, "runtime": {}})
            if status_refusal is None:
                _finish_projection(custody, event_enqueued=True, stream_closed=True)
            else:
                _unknown(custody, getattr(status_refusal, "code", "status_commit_unproven"))
        elif record["status"] in ("queued", "claimed"):
            _unknown(custody, "peer_delivery_pending")
            await _write("run.queued", {"status": record["status"], "delivery_id": delivery_id})
        else:
            status_refusal = _status_projection(custody)
            terminal = "run.failed" if _status_proven(custody, status_refusal) else "run.unknown"
            await _write(terminal, {"error": record.get("error") or f"Bot Chat delivery {record['status']}",
                                    "code": record.get("reason") or record["status"],
                                    "delivery_id": delivery_id})
            if status_refusal is None:
                _finish_projection(custody, event_enqueued=True, stream_closed=True)
            else:
                _unknown(custody, getattr(status_refusal, "code", "status_commit_unproven"))
        await _write("done", {})
    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
        disconnected = True
    finally:
        if disconnected and custody is not None and not custody.unknown_reason:
            custody.mark_transport_retired()
            custody.mark_control_finalizers()
    return response
