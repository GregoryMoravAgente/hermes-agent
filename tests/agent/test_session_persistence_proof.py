"""Causal controls for the managed Codex SessionDB proof boundary.

These tests use the real SessionPersistenceMixin and SessionDB.  A legacy bool is
never accepted as a managed custody witness: the proof helper must read the same
row addresses and snapshots back after the one canonical flush.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.message_metadata import DB_ROW_SNAPSHOT
from agent.transcript_repair import transcript_row_snapshot
from agent.session_persistence import SessionPersistenceMixin
from agent.session_persistence_proof import SessionDBWriteProof, flush_messages_with_proof
from hermes_state import SessionDB
from tests.agent.test_api_content_sidecar import _FakeAgent


class _RealPersistenceAgent(SessionPersistenceMixin, _FakeAgent):
    """Real writer with only the normal test-agent construction surface."""

    def __init__(self, db: SessionDB, session_id: str):
        _FakeAgent.__init__(self)
        self._session_db = db
        self.session_id = session_id
        self._session_db_created = True
        self._flushed_db_message_ids = set()
        self._last_flushed_db_idx = 0


def _agent(tmp_path: Path) -> tuple[_RealPersistenceAgent, SessionDB]:
    db = SessionDB(db_path=tmp_path / "state.db")
    session_id = "managed-codex-proof"
    db.create_session(session_id, source="cli")
    return _RealPersistenceAgent(db, session_id), db


def test_managed_proof_reads_writer_stamped_row_after_one_real_flush(tmp_path):
    agent, db = _agent(tmp_path)
    try:
        message = {"role": "assistant", "content": "durable answer"}
        proof = agent._flush_messages_to_session_db_with_proof([message])

        assert isinstance(proof, SessionDBWriteProof)
        assert proof.store_path == str((tmp_path / "state.db").resolve())
        assert len(proof.row_ids) == 1
        assert message["_row_id"] == proof.row_ids[0]
        assert message[DB_ROW_SNAPSHOT] == proof.row_snapshots[0]

        rows = db._read_all(
            "SELECT * FROM messages WHERE session_id = ? AND id = ?",
            (agent.session_id, proof.row_ids[0]),
        )
        row = rows[0]
        assert DB_ROW_SNAPSHOT not in row.keys()
        assert "_compressed_summary" in row.keys()
        assert transcript_row_snapshot(row) == proof.row_snapshots[0]
        assert proof.row_sessions == (agent.session_id,)
        assert proof.row_roles == ("assistant",)
        assert proof.payload_digest
    finally:
        db.close()



def test_managed_proof_reads_structured_tool_call_rows_from_raw_storage_after_one_real_flush(tmp_path):
    agent, db = _agent(tmp_path)
    try:
        message = {
            "role": "assistant",
            "content": "structured durable answer",
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "lookup", "arguments": '{"q":"x"}'},
            }],
        }
        proof = agent._flush_messages_to_session_db_with_proof([message])

        assert isinstance(proof, SessionDBWriteProof)
        assert proof.row_ids == (message["_row_id"],)
        raw = db._read_all(
            "SELECT * FROM messages WHERE session_id = ? AND id = ?",
            (agent.session_id, message["_row_id"]),
        )[0]
        assert "_compressed_summary" in raw.keys()
        assert raw["tool_calls"]
        assert transcript_row_snapshot(raw) == proof.row_snapshots[0]
        assert proof.row_sessions == (agent.session_id,)
        assert proof.row_roles == ("assistant",)
    finally:
        db.close()


def test_managed_proof_refuses_writer_marker_removed_after_real_flush(tmp_path):
    agent, db = _agent(tmp_path)
    message = {"role": "assistant", "content": "marker must remain"}
    original_flush = agent._flush_messages_to_session_db

    def flush_then_strip_marker(messages, conversation_history=None):
        result = original_flush(messages, conversation_history)
        captured = list(agent._managed_proof_batch)
        message_identity, writer_row, writer_message = captured[0]
        writer_message.pop(DB_ROW_SNAPSHOT, None)
        agent._managed_proof_batch = ((message_identity, writer_row, writer_message),)
        return result

    agent._flush_messages_to_session_db = flush_then_strip_marker
    try:
        # The canonical writer really commits the row first; only the captured writer marker
        # is removed before proof consumes the batch.  Proof must fail closed.
        assert agent._flush_messages_to_session_db_with_proof([message]) is None
        assert db._read_all(
            "SELECT * FROM messages WHERE session_id = ? AND id = ?",
            (agent.session_id, message["_row_id"]),
        )
    finally:
        agent._flush_messages_to_session_db = original_flush
        db.close()


def test_managed_proof_requires_every_new_row_after_marked_history(tmp_path):
    agent, db = _agent(tmp_path)
    try:
        existing = {"role": "user", "content": "already durable"}
        assert agent._flush_messages_to_session_db([existing]) is True
        fresh = {"role": "assistant", "content": "second durable answer"}
        proof = agent._flush_messages_to_session_db_with_proof([existing, fresh])
        assert isinstance(proof, SessionDBWriteProof)
        assert len(proof.row_ids) == 1
        assert proof.row_ids[0] == fresh["_row_id"]
        assert proof.row_roles == ("assistant",)
        assert proof.row_sessions == (agent.session_id,)
        assert len(db.get_messages(agent.session_id, include_inactive=True)) == 2
    finally:
        db.close()

def test_managed_proof_refuses_disabled_persistence_without_a_write(tmp_path):
    agent, db = _agent(tmp_path)
    try:
        agent._persist_disabled = True
        message = {"role": "assistant", "content": "must remain unknown"}
        assert agent._flush_messages_to_session_db_with_proof([message]) is None
        assert db.get_messages(agent.session_id, include_inactive=True) == []
        assert "_row_id" not in message
    finally:
        db.close()


def test_managed_proof_does_not_promote_truthy_legacy_result():
    class _TruthyLegacy:
        _persist_disabled = False
        _session_db = object()
        session_id = "s"

        def _flush_messages_to_session_db(self, _messages, _history=None):
            return 1  # A truthy legacy value is not the literal success proof.

    message = {"role": "assistant", "content": "x", "_row_id": 1, DB_ROW_SNAPSHOT: "snapshot"}
    assert flush_messages_with_proof(_TruthyLegacy(), [message]) is None


@pytest.mark.parametrize("writer_result", [None, "partial"], ids=["no-result", "partial-result"])
def test_prefix_repair_writer_failure_retains_prefix_as_unknown(writer_result):
    """A repaired prefix remains retryable; it never substitutes for an unproved suffix."""
    from agent.session_persistence import _finish_gateway_writer_request
    from agent.session_persistence_proof import (
        _GATEWAY_WRITER_PREFIX_REPAIR, mark_gateway_writer_proof_unknown,
    )

    prefix = SessionDBWriteProof(
        "state.db", "session", (1,), ("user-snapshot",), ("session",), ("user",), "prefix",
    )
    result = None if writer_result is None else prefix
    agent = SimpleNamespace(
        _managed_shell_writer_proof=prefix,
        _managed_persistence_failed=False,
    )

    def forbidden_forget(_agent):
        raise AssertionError("validated prefix must remain available for retry")

    def forbidden_plain(*_args):
        raise AssertionError("prefix repair must use the managed writer")

    def remember(*_args):
        return True

    def forbidden_retain(*_args, **_kwargs):
        raise AssertionError("an unproved suffix must not replace the retained prefix")

    assert _finish_gateway_writer_request(
        agent, [], None, object(), True, prefix, _GATEWAY_WRITER_PREFIX_REPAIR, None,
        lambda *_args: result, forbidden_plain, lambda _messages: False,
        forbidden_forget, remember,
        mark_gateway_writer_proof_unknown,
        forbidden_retain,
    ) is False
    assert agent._managed_shell_writer_proof is prefix
    assert agent._classic_writer_proof_unknown is True
    assert agent._managed_persistence_failed is True


def test_prefix_repair_rejects_user_only_suffix():
    from agent.session_persistence_proof import gateway_writer_proof_extends_prefix

    prefix = SessionDBWriteProof(
        "state.db", "session", (1,), ("user-snapshot",), ("session",), ("user",), "prefix",
    )
    user_only_suffix = SessionDBWriteProof(
        "state.db", "session", (1, 2), ("user-snapshot", "next-user-snapshot"),
        ("session", "session"), ("user", "user"), "user-only",
    )
    assistant_suffix = SessionDBWriteProof(
        "state.db", "session", (1, 3), ("user-snapshot", "assistant-snapshot"),
        ("session", "session"), ("user", "assistant"), "assistant-result",
    )

    assert gateway_writer_proof_extends_prefix(prefix, user_only_suffix) is False
    assert gateway_writer_proof_extends_prefix(prefix, assistant_suffix) is True


@pytest.mark.asyncio
async def test_gateway_writer_noop_reuses_exact_proof_only_for_same_turn(tmp_path, monkeypatch):
    """A repeated gateway persist reuses the real readback proof without a second DB write."""
    from gateway.platforms.event import MessageEvent
    from gateway.session_classic_custody import classic_writer_binding_for_event
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        source = fixture.adapter.build_source(
            chat_id="classic-noop@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        )
        session_key = fixture.runner.session_store._generate_session_key(source)
        entry = fixture.runner.session_store.get_or_create_session(source, touch_activity=False)
        event = MessageEvent(text="same", source=source, message_id="classic-noop-1")
        assert fixture.authority._classic_begin(event, session_key, "fifo")
        binding = classic_writer_binding_for_event(event)
        assert binding is not None

        writer = _RealPersistenceAgent(fixture.authority.db, entry.session_id)
        writer._current_turn_id = "classic-noop-turn"
        writer._request_gateway_writer_proof(binding)
        messages = [
            {"role": "user", "content": "same"},
            {"role": "assistant", "content": "durable answer"},
        ]
        assert writer._flush_messages_to_session_db(messages, []) is True
        proof = writer._managed_shell_writer_proof
        assert isinstance(proof, SessionDBWriteProof)
        assert len(fixture.authority.db.get_messages(entry.session_id, include_inactive=True)) == 2

        assert writer._flush_messages_to_session_db(messages, []) is True
        assert writer._managed_shell_writer_proof is proof
        assert len(fixture.authority.db.get_messages(entry.session_id, include_inactive=True)) == 2

        writer._current_turn_id = "classic-noop-next-turn"
        assert writer._flush_messages_to_session_db(messages, []) is False
        assert writer._managed_shell_writer_proof is None
        assert len(fixture.authority.db.get_messages(entry.session_id, include_inactive=True)) == 2
    finally:
        fixture.cleanup()
