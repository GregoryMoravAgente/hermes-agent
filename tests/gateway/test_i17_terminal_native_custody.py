"""Public I17 hosted-room terminal receipts and authenticated abort controls."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

def test_public_i17_two_profile_terminal_receipts_rebind_same_services(
    started_mux, monkeypatch,
):
    """A real two-profile hosted source stops, proves, and retries the same service objects."""
    from gateway import run_runtime
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.session_authorities import serving_authorities
    from tui_gateway import hosted_tui_custody

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authorities = tuple(serving_authorities(runner))
    assert len(authorities) >= 2
    authorities = authorities[:2]
    services = tuple(authority.hosted_room_service for authority in authorities)
    assert all(service is not None and service.runtime.status()["running"] for service in services)
    original = tuple(
        (
            authority._i17_service_handle,
            service._i17_room_handle,
            service,
            service.runtime,
            authority.db,
            service.member_rpcs,
        )
        for authority, service in zip(authorities, services)
    )
    descriptor = runner.session_runtime_descriptor
    params = {
        "instance_id": descriptor["instance_id"],
        "authority_epoch": descriptor["authority_epoch"],
        "owner_id": "i17-terminal-two-profile",
        "scope": "process",
        "deadline_s": 30,
    }
    wire = call(run_runtime.begin_preservation_drain(runner, params, "fixture-peer"))
    assert wire["status"] in {"draining", "closing"}, wire

    # Intake is fenced, but the enclosing room services are still live.  Their source-owned
    # graph must not certify terminal custody before the real stop/join receipt exists.
    for authority in authorities:
        assert hosted_tui_custody._profile_host_evidence(authority)[0] is False

    prepared = call(run_runtime.prepare_preservation_handoff(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert prepared["status"] == "refused", prepared
    coordinator = runner.runtime_custody
    # I17's real producer graph is sealed independently; another unresolved owner may still make
    # the global handoff refuse, but it must not erase this source's positive terminal evidence.
    assert coordinator.status()["owner_states"]["I17_hosted_tui_services"] == "sealed"
    old_roots = tuple(item[0] for item in original)
    old_children = tuple(item[1] for item in original)
    assert all(coordinator.is_terminal_handle(root) for root in old_roots)
    assert all(coordinator.is_terminal_handle(child) for child in old_children)
    assert all(not service.runtime.status()["running"] for service in services)

    # The exact terminal graph refuses both a missing producer and a replacement service.  Restore
    # each source object before the authenticated same-receipt abort; these are negative controls,
    # not alternate ways to prove a close.
    authority = authorities[0]
    registry = authority._i17_producer_registry
    original_room = registry["room"]
    registry["room"] = {"state": "absent"}
    assert hosted_tui_custody._profile_host_evidence(authority)[0] is False
    registry["room"] = original_room
    original_service = authority.hosted_room_service
    monkeypatch.setattr(authority, "hosted_room_service", object())
    assert hosted_tui_custody._profile_host_evidence(authority)[0] is False
    monkeypatch.setattr(authority, "hosted_room_service", original_service)

    reopened = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert reopened["status"] == "open", reopened
    assert all(service.runtime.status()["running"] for service in services)
    assert all(
        authority._i17_service_handle is not old_root
        and service._i17_room_handle is not old_child
        and service is original_item[2]
        and service.runtime is original_item[3]
        and authority.db is original_item[4]
        and service.member_rpcs is original_item[5]
        for authority, service, original_item, old_root, old_child in zip(
            authorities, services, original, old_roots, old_children,
        )
    )


def test_i17_room_stop_never_certifies_live_or_unknown_runtime(
    tmp_path, monkeypatch,
):
    """A room child remains active/unknown until its real runtime identity is terminal."""
    from contextlib import nullcontext
    from types import SimpleNamespace

    from tests.tui_gateway.test_hosted_tui_custody import _runtime
    from tui_gateway.hosted_room_driver import HostedRoomRuntime
    from tui_gateway import hosted_tui_custody

    _runner, authority, coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    runtime = HostedRoomRuntime(
        db_path=tmp_path / "hosted-room.db",
        rooms=(), rpc=object(), turn_lock=lambda _profile: nullcontext(),
        poll_interval_seconds=0.01, active_poll_interval_seconds=0.01,
    )
    service = SimpleNamespace(runtime=runtime, authority=authority)
    hosted_tui_custody.bind_room_service(service, authority)
    runtime.start()
    try:
        assert hosted_tui_custody.room_service_stop(service, True) is False
        assert coordinator.status()["active_owner_classes"]
    finally:
        runtime.stop(timeout=5)


def test_i17_room_evidence_refuses_detached_or_replaced_source_graph(
    tmp_path, monkeypatch,
):
    """A terminal receipt cannot survive a detached service or replaced runtime graph."""
    from tests.tui_gateway.test_hosted_tui_custody import _runtime
    from tui_gateway.hosted_room_driver import HostedRoomRuntime
    from tui_gateway import hosted_tui_custody

    _runner, authority, _coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    runtime = HostedRoomRuntime(
        db_path=tmp_path / "hosted-room.db",
        rooms=(), rpc=object(), turn_lock=lambda _profile: nullcontext(),
        poll_interval_seconds=0.01, active_poll_interval_seconds=0.01,
    )
    service = SimpleNamespace(runtime=runtime, authority=authority, member_rpcs=object())
    original_db = authority.db
    hosted_tui_custody.bind_room_service(service, authority)
    runtime.start()
    try:
        assert runtime.stop(timeout=5) is True
        assert hosted_tui_custody.room_service_stop(service, True) is True
        producer = authority._i17_producer_registry["room"]
        assert hosted_tui_custody._room_producer_evidence(
            authority, producer, producer["receipt"],
        ) is True
        original_service = authority.hosted_room_service
        authority.hosted_room_service = None
        assert hosted_tui_custody._room_producer_evidence(
            authority, producer, producer["receipt"],
        ) is False
        authority.hosted_room_service = original_service
        original_runtime = service.runtime
        service.runtime = object()
        assert hosted_tui_custody._room_producer_evidence(
            authority, producer, producer["receipt"],
        ) is False
        service.runtime = original_runtime
        authority.db = object()
        assert hosted_tui_custody._room_producer_evidence(
            authority, producer, producer["receipt"],
        ) is False
        authority.db = original_db
    finally:
        authority.hosted_room_service = service
        authority.db = original_db
        runtime.stop(timeout=5)


def test_i17_cold_successor_registry_keeps_intake_closed(
    tmp_path, monkeypatch,
):
    """A no-room rebound registry cannot clear its successor fence during abort."""
    from tests.tui_gateway.test_hosted_tui_custody import _runtime
    from gateway.session_hosted_service import abort_i09_source_close
    from tui_gateway import hosted_tui_custody

    runner, authority, coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    receipt = coordinator.begin_close(owner_id="i17-cold-successor", deadline_s=10)
    hosted_tui_custody._reset_producer_registry(
        authority, coordinator, runner=runner, intake_closed=True,
    )
    registry = authority._i17_producer_registry
    assert abort_i09_source_close(runner, receipt.close_generation) is True
    assert registry["intake_closed"] is True
    assert registry["source_binding"] is authority._i17_rebind_source


@pytest.mark.platforms("windows")
def test_i17_host_evidence_uses_retained_native_identity_after_supervisor_mutation(
    monkeypatch, tmp_path,
):
    """Windows evidence remains tied to the original supervisor/handle, not mutable fields."""
    from tests.tui_gateway.test_hosted_tui_custody import _runtime, _supervisor
    from tui_gateway import hosted_tui_custody

    _runner, authority, _coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-native-retained")
    supervisor.start()
    try:
        # HostSupervisor.shutdown() clears its mutable native fields after acknowledging a clean
        # stop.  Capture the exact source-owned identity at the real shutdown boundary before
        # calling it; host_shutdown then validates the retained object rather than a post-close
        # scalar snapshot.
        hosted_tui_custody._capture_host_shutdown_identity(supervisor)
        receipt = supervisor.shutdown()
        assert hosted_tui_custody.host_shutdown(supervisor, receipt) is True
        supervisor._host_generation += 1
        supervisor._host_boot_id = "foreign-after-close"
        assert hosted_tui_custody._profile_host_evidence(authority)[0] is True
        supervisor._producer_process_handle = object()
        assert hosted_tui_custody._profile_host_evidence(authority)[0] is False
        supervisor._producer_process_handle = None
    finally:
        supervisor.shutdown()


def test_i17_partial_multi_profile_failure_retains_exact_handles(
    started_mux, monkeypatch,
):
    """A late sibling failure leaves the exact stopped graph UNKNOWN, never replaced."""
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.session_authorities import serving_authorities
    from tui_gateway import hosted_tui_custody

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authorities = tuple(serving_authorities(runner))[:2]
    assert len(authorities) == 2
    originals = tuple(
        (authority._i17_service_handle, authority.hosted_room_service,
         authority.hosted_room_service.runtime, authority.db,
         authority.hosted_room_service.member_rpcs,
         authority.hosted_room_service._i17_room_handle)
        for authority in authorities
    )
    coordinator = runner.runtime_custody
    receipt = coordinator.begin_close(owner_id="i17-partial-terminalization", deadline_s=30)
    from gateway.session_hosted_service import begin_i09_source_close
    assert begin_i09_source_close(runner, receipt.close_generation)
    original_mark_terminal = coordinator.mark_terminal
    calls = 0

    def fail_second(handle):
        nonlocal calls
        calls += 1
        if calls == 2:
            return object()
        return original_mark_terminal(handle)

    monkeypatch.setattr(coordinator, "mark_terminal", fail_second)
    try:
        assert call(hosted_tui_custody.terminalize_preservation_rooms(runner, 20)) is False
        assert calls >= 2
        for authority, original in zip(authorities, originals):
            assert authority._i17_service_handle is original[0]
            assert authority.hosted_room_service is original[1]
            assert authority.hosted_room_service.runtime is original[2]
            assert authority.db is original[3]
            assert authority.hosted_room_service.member_rpcs is original[4]
            assert authority.hosted_room_service._i17_room_handle is original[5]
            assert getattr(authority, "_i17_terminalization_partial", ())
    finally:
        # Recover the intentionally UNKNOWN exact child/root so the production fixture can clean
        # up without replacing any object or manufacturing a terminal receipt.
        coordinator.mark_terminal = original_mark_terminal
        for authority in tuple(getattr(runner, "session_authorities", ())):
            service = authority.hosted_room_service
            handle = getattr(service, "_i17_room_handle", None)
            if service is not None and handle is not None and not coordinator.is_terminal_handle(handle):
                coordinator.reconcile_unknown(
                    handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
                    process_instance=handle.process_instance,
                    authority_epoch=handle.authority_epoch,
                )
                assert hosted_tui_custody.room_service_stop(service, True) is True
            root = authority._i17_service_handle
            if not coordinator.is_terminal_handle(root):
                coordinator.reconcile_unknown(
                    root, owner_id=root.owner_id, profile_id=root.profile_id,
                    process_instance=root.process_instance,
                    authority_epoch=root.authority_epoch,
                )
                assert coordinator.mark_terminal(root) is None
            authority._i17_service_closed = True


def test_i17_cold_no_room_abort_reopens_authenticated_registry_for_later_bind(
    tmp_path, monkeypatch,
):
    """A cold prepared successor remains fenced until the authenticated OPEN completion."""
    from tests.tui_gateway.test_hosted_tui_custody import _runtime
    from tui_gateway import hosted_tui_custody
    from tui_gateway.hosted_room_driver import HostedRoomRuntime

    runner, authority, coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    receipt = coordinator.begin_close(owner_id="i17-cold-post-open", deadline_s=30)
    assert hosted_tui_custody.close_profile(authority) is True
    assert hosted_tui_custody.restore_after_abort(runner, receipt) is True
    registry = authority._i17_producer_registry
    assert registry["room"]["state"] == "absent"
    assert registry["intake_closed"] is True
    assert registry["prepared_successor_pending"] is True
    assert coordinator.status()["state"] == "closing"
    from gateway.run_runtime_mcp_abort import _finish_mcp_factories_atomically
    outcome = _finish_mcp_factories_atomically(
        (), coordinator, receipt, runner=runner,
    )
    assert outcome.completed is True
    assert coordinator.status()["state"] == "open"
    assert registry["intake_closed"] is False
    assert registry["prepared_successor_pending"] is False

    runtime = HostedRoomRuntime(
        db_path=tmp_path / "cold-room.db", rooms=(), rpc=object(),
        turn_lock=lambda _profile: nullcontext(),
        poll_interval_seconds=0.01, active_poll_interval_seconds=0.01,
    )
    service = SimpleNamespace(runtime=runtime, authority=authority, member_rpcs=object())
    assert hosted_tui_custody.bind_room_service(service, authority) is not None
    runtime.start()
    try:
        assert runtime.stop(timeout=5) is True
        assert hosted_tui_custody.room_service_stop(service, True) is True
    finally:
        runtime.stop(timeout=5)
