"""Managed ACP writer proof for a user row persisted before the final response batch.

The managed turn may write its user row at admission and its assistant/tool suffix later.
The explicit current-turn proof path reads that staged prefix from the real SessionDB before
one final canonical writer batch, then returns the combined ordered witness.
"""

from __future__ import annotations

from pathlib import Path

from agent.context_compressor import _DB_PERSISTED_MARKER
from agent.message_metadata import DB_ROW_SNAPSHOT
from agent.session_persistence import SessionPersistenceMixin
from agent.session_persistence_proof import (
    SessionDBWriteProof,
    flush_messages_with_managed_current_turn_proof,
)
from hermes_state import SessionDB
from tests.agent.test_api_content_sidecar import _FakeAgent


class _RealPersistenceAgent(SessionPersistenceMixin, _FakeAgent):
    """Real SessionDB writer with the normal test-agent construction surface."""

    def __init__(self, db: SessionDB, session_id: str):
        _FakeAgent.__init__(self)
        self._session_db = db
        self.session_id = session_id
        self._session_db_created = True
        self._flushed_db_message_ids = set()
        self._last_flushed_db_idx = 0


class _ManagedProofConsumer:
    """Only the managed handoff seam is observed; proof values come from the real writer."""

    def __init__(self):
        self.pending = False
        self.proofs: list[SessionDBWriteProof] = []

    def has_pending_managed_witness(self):
        return self.pending

    def _commit_managed_writer_proof(self, _agent, proof, _messages):
        assert isinstance(proof, SessionDBWriteProof)
        self.proofs.append(proof)
        self.pending = False
        return True

    def has_managed_durable_receipt(self, session_id):
        return bool(self.proofs and self.proofs[-1].session_id == session_id)


def _agent(tmp_path: Path, session_id: str = "managed-current-turn"):
    db = SessionDB(db_path=tmp_path / f"{session_id}.db")
    db.create_session(session_id, source="cli")
    return _RealPersistenceAgent(db, session_id), db


def test_managed_final_flush_combines_real_early_user_row_and_one_fresh_batch(tmp_path):
    agent, db = _agent(tmp_path)
    consumer = _ManagedProofConsumer()
    agent._managed_acp_client = consumer
    user = {"role": "user", "content": "early admission"}
    assistant = {"role": "assistant", "content": "managed answer"}
    try:
        assert agent._flush_messages_to_session_db([user]) is True
        consumer.pending = True
        assert user[_DB_PERSISTED_MARKER] is True
        assert user["_row_id"] > 0
        assert isinstance(user[DB_ROW_SNAPSHOT], str)

        assert agent._flush_messages_to_session_db([user, assistant]) is True

        proof = consumer.proofs[-1]
        assert proof.row_ids == (user["_row_id"], assistant["_row_id"])
        assert proof.row_roles == ("user", "assistant")
        assert proof.row_sessions == (agent.session_id, agent.session_id)
        assert proof.row_snapshots[0] == user[DB_ROW_SNAPSHOT]
        assert proof.row_snapshots[1] == assistant[DB_ROW_SNAPSHOT]
        assert len(db.get_messages(agent.session_id, include_inactive=True)) == 2
    finally:
        db.close()


def test_managed_current_turn_proof_is_suffix_bounded_across_turns(tmp_path):
    agent, db = _agent(tmp_path, "managed-current-turn-suffix")
    consumer = _ManagedProofConsumer()
    agent._managed_acp_client = consumer
    first_user = {"role": "user", "content": "first"}
    first_assistant = {"role": "assistant", "content": "first answer"}
    second_user = {"role": "user", "content": "second"}
    second_assistant = {"role": "assistant", "content": "second answer"}
    try:
        assert agent._flush_messages_to_session_db([first_user]) is True
        consumer.pending = True
        assert agent._flush_messages_to_session_db([first_user, first_assistant]) is True
        consumer.pending = False
        assert agent._flush_messages_to_session_db([first_user, first_assistant, second_user]) is True
        consumer.pending = True
        assert agent._flush_messages_to_session_db(
            [first_user, first_assistant, second_user, second_assistant]
        ) is True

        assert consumer.proofs[-1].row_ids == (second_user["_row_id"], second_assistant["_row_id"])
        assert consumer.proofs[-1].row_roles == ("user", "assistant")
        assert len(db.get_messages(agent.session_id, include_inactive=True)) == 4
    finally:
        db.close()


def test_managed_current_turn_proof_refuses_missing_or_stale_user_stamp(tmp_path):
    for label in ("missing", "stale"):
        agent, db = _agent(tmp_path, f"managed-current-turn-{label}")
        user = {"role": "user", "content": "early admission"}
        assistant = {"role": "assistant", "content": "must remain unknown"}
        try:
            assert agent._flush_messages_to_session_db([user]) is True
            if label == "missing":
                user.pop(DB_ROW_SNAPSHOT)
            else:
                user[DB_ROW_SNAPSHOT] = "stale-writer-stamp"
            assert flush_messages_with_managed_current_turn_proof(agent, [user, assistant]) is None
            assert "_row_id" not in assistant
            assert len(db.get_messages(agent.session_id, include_inactive=True)) == 1
        finally:
            db.close()


def test_managed_current_turn_proof_refuses_unstaged_old_anchor(tmp_path):
    agent, db = _agent(tmp_path, "managed-current-turn-old-anchor")
    old_user = {"role": "user", "content": "old turn"}
    current_user = {"role": "user", "content": "new turn"}
    assistant = {"role": "assistant", "content": "must remain unknown"}
    try:
        assert agent._flush_messages_to_session_db([old_user]) is True
        current_user.update({
            _DB_PERSISTED_MARKER: True,
            "_row_id": old_user["_row_id"],
            DB_ROW_SNAPSHOT: old_user[DB_ROW_SNAPSHOT],
        })

        assert flush_messages_with_managed_current_turn_proof(
            agent, [current_user, assistant]
        ) is None
        assert "_row_id" not in assistant
        assert len(db.get_messages(agent.session_id, include_inactive=True)) == 1
    finally:
        db.close()


def test_managed_current_turn_proof_refuses_foreign_session_row_marker(tmp_path):
    agent, db = _agent(tmp_path, "managed-current-turn-owner")
    foreign = _RealPersistenceAgent(db, "managed-current-turn-foreign")
    db.create_session(foreign.session_id, source="cli")
    user = {"role": "user", "content": "owner admission"}
    foreign_user = {"role": "user", "content": "foreign admission"}
    assistant = {"role": "assistant", "content": "must remain unknown"}
    try:
        assert agent._flush_messages_to_session_db([user]) is True
        assert foreign._flush_messages_to_session_db([foreign_user]) is True
        user["_row_id"] = foreign_user["_row_id"]
        user[DB_ROW_SNAPSHOT] = foreign_user[DB_ROW_SNAPSHOT]

        assert flush_messages_with_managed_current_turn_proof(agent, [user, assistant]) is None
        assert "_row_id" not in assistant
        assert len(db.get_messages(agent.session_id, include_inactive=True)) == 1
        assert len(db.get_messages(foreign.session_id, include_inactive=True)) == 1
    finally:
        db.close()
