"""Existing claim callers keep the real execution generation and its publication fence."""

from types import SimpleNamespace

import pytest

from gateway.session_authority import LiveSession, SessionAuthority
from gateway.session_contract import SessionRef
from hermes_state import SessionDB
from hermes_state_runtime import admit_session_input, begin_runtime_epoch, claim_session_input


@pytest.mark.parametrize("call_shape", ["positional", "keyword"])
def test_claim_row_keeps_its_execution_stamp_and_generation_fence(tmp_path, call_shape):
    with SessionDB(db_path=tmp_path / "state.db") as db:
        db.create_session("session", source="cli")
        epoch = begin_runtime_epoch(db, instance_id="owner")
        authority = SessionAuthority(
            SimpleNamespace(_draining=False), profile_id="profile", instance_id="owner", db=db, epoch=epoch,
        )
        live = authority.sessions["session"] = LiveSession(None, "route")
        ref = SessionRef("profile", "session")
        admit_session_input(
            db, epoch=epoch, principal_id="person", session_id="session", request_id="request",
            payload={"text": "work"},
        )
        row = claim_session_input(db, epoch=epoch, session_id="session")
        if call_shape == "positional":
            result = authority._stamp_claim(ref, live, row)
        else:
            result = authority._stamp_claim(ref, live, row=row)
        assert result is row
        assert authority.executing["session"] == row["generation"]
        assert live.event_stream.execution == {
            "authority_epoch": epoch, "execution_generation": row["generation"],
            "admission_id": row["admission_id"],
        }
        frames = []
        live.event_stream.observers.add(frames.append)
        authority.publish_execution("session", row["generation"], "message.delta", {"text": "accepted"})
        assert frames[-1]["params"]["payload"]["text"] == "accepted"
        authority.publish_execution("session", row["generation"] + 1, "message.delta", {"text": "stale"})
        assert len(frames) == 1
