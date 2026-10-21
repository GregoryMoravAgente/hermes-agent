"""A real hosted send must reserve custody before its first durable room effect."""

import json
import threading

from tests.gateway.test_hosted_mux_runtime import mux as _mux_fixture

mux = _mux_fixture


def test_coordinator_close_cannot_accept_an_unreserved_room_send(mux, monkeypatch):
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.runtime_custody import DrainReceipt
    from gateway.session_hosted_service import (
        abort_i09_source_close,
        begin_i09_source_close,
    )
    from gateway.session_controls import AuthorityConnection
    from gateway import hosted_room_discussion as discussion

    runner, _homes, _loop, call = mux
    call(recover_gateway_native_sessions(runner))
    authority = runner.session_authority
    service = authority.hosted_room_service
    connection = AuthorityConnection(authority, object(), {"user_id": "synthetic-room-operator"})
    created = call(connection.dispatch({
        "id": 1, "method": "groups.create", "params": {
            "room_id": "close-race-room", "name": "Synthetic close race", "members": [
                {"member_id": "one", "profile": "alpha", "handle": "one"},
                {"member_id": "two", "profile": "beta", "handle": "two"},
            ],
        },
    }))
    assert "result" in created, created

    entered, release = threading.Event(), threading.Event()
    original_validate = discussion.validate_user_payload

    def held_validation(payload):
        # This is after the real authority's last fresh-admission check and before
        # append_user_event; no custody, SQL, task, queue or ready flag is injected.
        entered.set()
        assert release.wait(10), "test did not release the accepted send"
        return original_validate(payload)

    monkeypatch.setattr(discussion, "validate_user_payload", held_validation)
    replies, errors = [], []

    def send():
        try:
            replies.append(call(connection.dispatch({
                "id": 2, "method": "groups.send", "params": {
                    "room_id": "close-race-room", "event_id": "before-close",
                    "payload": {"text": "@one synthetic input", "thread_id": "race-thread"},
                },
            })))
        except BaseException as exc:
            errors.append(exc)

    sender = threading.Thread(target=send)
    close = None
    sender.start()
    try:
        assert entered.wait(10), errors
        before = runner.runtime_custody.status()
        # The shared fixture owns the real profile locks and coordinator but does not run
        # GatewayRunner.start(). Exercise the exact coordinator/source-close transition
        # without pretending that its unstarted runner is a live gateway.
        close = runner.runtime_custody.begin_close(
            owner_id="synthetic-maintenance-owner:i09-source-race", deadline_s=10,
        )
        assert isinstance(close, DrainReceipt), close
        assert begin_i09_source_close(runner, close.close_generation)
        assert service.runtime.preservation_intake_closed(close.close_generation)
        assert any(
            isinstance(record, dict)
            and record.get("owner_class") == "I09_a2a_hosted_kanban"
            for record in authority._preservation_operations.values()
        )
        assert service._preservation_inventory(close.close_generation) is None
        release.set()
        sender.join(10)
        assert not sender.is_alive()
        assert not errors, errors
        assert len(replies) == 1
        accepted = replies[0].get("result", {}).get("accepted") is True
        print("i09-global-close-race: " + json.dumps({
            "accepted_after_close": accepted,
            "owners_before_close": before["active_owner_classes"],
            "owners_after_close": runner.runtime_custody.status()["active_owner_classes"],
            "coordinator_generation": close.close_generation,
        }, sort_keys=True))
        assert not accepted or "I09_a2a_hosted_kanban" in before["active_owner_classes"], (
            "A room send crossed the actual close without reserving I09 custody before its write",
            replies[0], before,
        )
    finally:
        release.set()
        sender.join(10)
        if isinstance(close, DrainReceipt):
            assert runner.runtime_custody.abort_before_handoff(close) is None
            assert abort_i09_source_close(runner, close.close_generation)
