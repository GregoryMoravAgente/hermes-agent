"""Serialized transcript drain phase for SessionStore.

This is the concrete queue/SQLite/readback implementation inherited by SessionTranscriptMixin;
it is not a second writer or a compatibility wrapper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gateway.session_transcript import TranscriptWriteProof

logger = logging.getLogger("gateway.session")


@dataclass
class _TranscriptAppendState:
    session_id: str
    queue_session_id: str
    pending: list[Any]
    msg: dict[str, Any]
    target: dict[str, Any]
    target_proof: Any = None


class SessionTranscriptDrainMixin:
    @staticmethod
    def _transcript_state(session_id: str, pending: list[Any]) -> _TranscriptAppendState:
        return _TranscriptAppendState(
            session_id=session_id, queue_session_id=session_id, pending=pending,
            msg=pending[0], target=pending[-1],
        )

    def _ack_transcript_head(self, state: _TranscriptAppendState) -> bool:
        """Pop the acknowledged head while the per-session retry lock is held."""
        if state.pending and state.pending[0] is state.msg:
            state.pending.pop(0)
        if not state.pending:
            self._dirty_transcripts.pop(state.queue_session_id, None)
            self._transcript_append_failures.pop(state.session_id, None)
            return True
        return False

    def _recover_transcript_fts(
        self, state: _TranscriptAppendState, exc: Exception, spool_exc: Exception | None,
    ) -> tuple[bool, Exception]:
        """Repair FTS once and retry the same row, or return the exception for normal failure handling."""
        if not self._is_fts_corruption_error(exc) or not self._rebuild_fts_once():
            return False, exc
        if spool_exc is not None:
            return True, exc
        try:
            proof = self._append_transcript_message(state.session_id, state.msg)
        except Exception as retry_exc:
            logger.debug("Transcript FTS repair retry failed", exc_info=True)
            return False, retry_exc
        with self._transcript_retry_lock:
            self._ack_transcript_head(state)
        if state.msg is state.target:
            state.target_proof = proof
        return True, exc

    def _handle_transcript_append_error(
        self, state: _TranscriptAppendState, exc: Exception, spool_exc: Exception | None,
    ) -> bool:
        """Handle one failed append; return True only when the outer drain should retry."""
        from gateway.session_transcript import TranscriptCommitUnknown
        from hermes_state import StateDbCorruptError, StateDbReplacedError
        from hermes_state_errors import CompressionSessionClosedError
        if isinstance(exc, (StateDbReplacedError, StateDbCorruptError)):
            self._divert_transcript_after_db_replaced(
                state.session_id, state.queue_session_id, exc,
            )
            return False
        if isinstance(exc, TranscriptCommitUnknown):
            with self._transcript_retry_lock:
                self._ack_transcript_head(state)
            logger.error(
                "Transcript commit readback unavailable for %s row %s; retaining unknown "
                "custody rather than retrying a possibly committed row",
                exc.session_id, exc.row_id,
            )
            return False
        if isinstance(exc, CompressionSessionClosedError):
            owner_key = self._owner_key_for_session_id(state.session_id)
            child_id = self._live_compression_child(state.session_id)
            if child_id:
                if owner_key:
                    self._lazy("_session_owner_hints", dict)[child_id] = owner_key
                try:
                    proof = self._append_transcript_message(child_id, state.msg)
                except Exception as reroute_exc:
                    logger.debug("Transcript compression reroute failed", exc_info=True)
                    exc = reroute_exc
                else:
                    with self._transcript_retry_lock:
                        state.pending = self._migrate_transcript_queue_to_child(
                            state.session_id, state.queue_session_id, child_id, state.pending, state.msg,
                        )
                        state.queue_session_id = child_id
                    if state.msg is state.target:
                        state.target_proof = proof
                    self._publish_transcript_reroute(state.session_id, child_id)
                    if not state.pending:
                        return False
                    state.msg = state.pending[0]
                    state.session_id = child_id
                    return True
            else:
                with self._transcript_retry_lock:
                    self._ack_transcript_head(state)
                logger.error(
                    "Session DB transcript append rejected for compression-ended %s with "
                    "no unique live child; not retrying", state.session_id,
                )
                return False
        retry, exc = self._recover_transcript_fts(state, exc, spool_exc)
        if retry:
            return True
        with self._transcript_retry_lock:
            failures = self._transcript_append_failures.get(state.session_id, 0) + 1
            self._transcript_append_failures[state.session_id] = failures
        if failures >= self._TRANSCRIPT_APPEND_FAILURE_ESCALATION_THRESHOLD:
            spooled = self._spool_stalled_backlog(state.session_id, state.queue_session_id)
            logger.error(
                "Session DB transcript append failed for %s (failure_count=%d, pending=%d, "
                "spooled_to_disk=%d); session is stalled and needs operator attention: %s",
                state.session_id, failures, len(state.pending), spooled, exc,
            )
        else:
            logger.warning(
                "Session DB transcript append failed for %s (failure_count=%d, pending=%d); "
                "will retry: %s", state.session_id, failures, len(state.pending), exc,
            )
        return False

    def _append_to_transcript_serialized(
        self, session_id: str, message: dict[str, Any],
    ) -> TranscriptWriteProof | None:
        """Append one message while preserving FIFO order and exact SQLite readback."""
        with self._transcript_retry_lock:
            state = self._transcript_state(
                session_id, self._enqueue_transcript_message(session_id, message),
            )
        while True:
            spool_exc = None
            try:
                spool_exc = self._drain_spooled_drops(state.session_id)
                if spool_exc is not None:
                    raise spool_exc
                proof = self._append_transcript_message(state.session_id, state.msg)
            except Exception as exc:
                logger.debug("Transcript append failure entered custody classification", exc_info=True)
                if self._handle_transcript_append_error(state, exc, spool_exc):
                    continue
                return state.target_proof
            if state.msg is state.target:
                state.target_proof = proof
            with self._transcript_retry_lock:
                queue_empty = self._ack_transcript_head(state)
                if not queue_empty:
                    state.msg = state.pending[0]
            if queue_empty:
                return state.target_proof
