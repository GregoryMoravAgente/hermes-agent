"""Real I17 custody consumers for a hosted compute service.

The tests bind the existing RuntimeCustodyCoordinator to a real SessionAuthorities
registry and a real HostSupervisor child.  They exercise the production custody
adapter around the producer and its shutdown receipt; no local mirror or fabricated
terminal state is used.
"""

from __future__ import annotations

import os
import sys
import threading
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.session_authorities import SessionAuthorities
from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway import hosted_tui_custody
from tui_gateway.host_supervisor import HostSupervisor
from tui_gateway.hosted_room_driver import HostedRoomRuntime


pytestmark = pytest.mark.platforms("posix", "windows")


def _runtime(monkeypatch, tmp_path: Path, *, bind_authority_now: bool = True):
    home = (tmp_path / "hermes-home").resolve()
    home.mkdir()
    process_instance = "i17-test-process"
    authority = SimpleNamespace(
        profile_id=str(home),
        epoch=1,
        instance_id=process_instance,
        retiring=False,
        retired=False,
    )
    authorities = SessionAuthorities(home)
    authorities.add(home, authority)
    runner = SimpleNamespace(
        session_authorities=authorities,
        session_runtime_descriptor={
            "instance_id": process_instance,
            "authority_epoch": 1,
        },
    )
    fence = backend_retirement.RetirementFence()
    coordinator = RuntimeCustodyCoordinator(
        process_instance=process_instance,
        authority_epoch=1,
        fence=fence,
    )
    runner.runtime_custody = coordinator
    monkeypatch.setattr(hosted_tui_custody, "_RUNTIME_RUNNER", None)
    monkeypatch.setattr(hosted_tui_custody, "_RUNTIME_COORDINATOR", None)
    hosted_tui_custody.bind_runtime(runner, coordinator, reserve=bind_authority_now)
    if bind_authority_now:
        hosted_tui_custody.bind_authority(authority, runner=runner, coordinator=coordinator)
    return runner, authority, coordinator, fence, home


def _supervisor(
    monkeypatch, tmp_path: Path, fence, *, rpc_profile: str = "",
    rpc_owner_profile: str = "", rpc_sink=None,
):
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_owner_child.py")
    home = (tmp_path / "hermes-home").resolve()
    home.mkdir(exist_ok=True)
    argv = [str(Path(sys.executable)), str(fixture), "--mode", "normal"]
    if rpc_profile:
        argv.extend(["--rpc-profile", rpc_profile])
    if rpc_owner_profile:
        argv.extend(["--rpc-owner-profile", rpc_owner_profile])
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=argv,
        cwd=root,
        env={"HERMES_HOME": str(home), "PYTHONPATH": str(root)},
        rpc_sink=rpc_sink,
        expected_build_sha="unknown",
        expected_hermes_home=str(home),
        heartbeat_secs=60,
    )
    return supervisor, home


def _frame(tmp_path: Path, home: Path, request_id: str, *, sid: str = "i17-custody-sid") -> dict:
    return {
        "type": "turn.start",
        "sid": sid,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": "i17-custody-session",
        "profile_home": str(home),
        "text": "synthetic hosted turn",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def test_real_host_producer_is_reserved_before_start_and_settled_by_receipt(
    monkeypatch, tmp_path
):
    _runner, authority, coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-real-host")
    root = authority._i17_service_handle
    host = supervisor._i17_host_handle
    assert root is not None and host is not None
    assert coordinator.status()["active_owner_classes"] == ["I17_hosted_tui_services"]

    callback_done = threading.Event()
    callbacks: list[dict] = []

    def callback(frame: dict) -> bool:
        callbacks.append(frame)
        callback_done.set()
        return True

    try:
        supervisor.start()
        hosted_tui_custody.submit_turn(
            supervisor, _frame(tmp_path, home, "i17-real-turn"), callback
        )
        assert callback_done.wait(30), "the real child did not publish a terminal receipt"
        assert callbacks[-1]["request_id"] == "i17-real-turn"
        assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
        receipt = supervisor.shutdown()
        host_settled = hosted_tui_custody.host_shutdown(supervisor, receipt)
        if os.name == "nt":
            assert host_settled
            assert coordinator.status()["active_child_parents"] == []
            assert hosted_tui_custody.close_profile(authority)
            assert coordinator.status()["active_owner_classes"] == []
        else:
            # POSIX has no native owner envelope; process/pipe EOF is deliberately
            # insufficient physical proof and the exact profile remains retained.
            assert host_settled is False
            assert hosted_tui_custody.close_profile(authority) is False
            assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
    finally:
        supervisor.shutdown()


def test_real_child_control_and_rpc_use_authenticated_bridge(monkeypatch, tmp_path):
    """The managed child must admit both a control and an agent RPC through its real bridge."""
    _runner, authority, coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    rpc_frames: list[dict] = []
    supervisor, _ = _supervisor(
        monkeypatch, tmp_path, fence, rpc_profile=str(home), rpc_sink=rpc_frames.append,
    )
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-real-control-rpc")
    root = authority._i17_service_handle
    callback_done = threading.Event()
    callback_frames: list[dict] = []

    def callback(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    try:
        supervisor.start()
        control = hosted_tui_custody.control(
            supervisor, "compute-owner-sid", route_name="session.save",
            payload={"request_id": "i17-child-control"}, wait=True, timeout=30,
        )
        assert isinstance(control, dict)
        hosted_tui_custody.submit_turn(
            supervisor, _frame(tmp_path, home, "i17-child-rpc-turn", sid="compute-owner-sid"), callback,
        )
        assert callback_done.wait(30), f"real child turn did not settle: {callback_frames!r}"
        assert callback_frames[-1].get("type") == "turn.end", (
            f"turn callback failed: type={callback_frames[-1].get('type')!r} "
            f"reason={callback_frames[-1].get('reason')!r} "
            f"message={callback_frames[-1].get('message')!r} "
            f"error={callback_frames[-1].get('error')!r}"
        )
        marker = home / "i17-rpc-dispatch.marker"
        wait_until(marker.exists, timeout=5, what="child RPC dispatch marker")
        wait_until(
            lambda: any(
                frame.get("id") == "i17-child-rpc" for frame in rpc_frames
            ),
            timeout=30,
            what="real child RPC response",
        )
        response = next(frame for frame in rpc_frames if frame.get("id") == "i17-child-rpc")
        assert response.get("jsonrpc") == "2.0"
        assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
    finally:
        supervisor.shutdown()


def test_real_child_foreign_rpc_is_refused_by_parent_bridge(monkeypatch, tmp_path):
    """A real child request cannot redirect its parent-owned bridge to another profile."""
    _runner, authority, coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    rpc_frames: list[dict] = []
    supervisor, _ = _supervisor(
        monkeypatch, tmp_path, fence, rpc_profile=str(home),
        rpc_owner_profile=str(tmp_path / "foreign"), rpc_sink=rpc_frames.append,
    )
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-foreign-rpc")
    root = authority._i17_service_handle
    callback_done = threading.Event()

    try:
        supervisor.start()
        hosted_tui_custody.submit_turn(
            supervisor, _frame(tmp_path, home, "i17-foreign-rpc-turn", sid="compute-owner-sid"),
            lambda _frame: callback_done.set() or True,
        )
        assert callback_done.wait(30), "real child turn did not settle"
        marker = home / "i17-rpc-dispatch.marker"
        wait_until(marker.exists, timeout=5, what="foreign child RPC dispatch marker")
        wait_until(
            lambda: any(frame.get("id") == "i17-child-rpc" for frame in rpc_frames),
            timeout=30,
            what="foreign child RPC refusal",
        )
        response = next(frame for frame in rpc_frames if frame.get("id") == "i17-child-rpc")
        assert response["error"]["message"] == "foreign_child_bridge"
        assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
    finally:
        supervisor.shutdown()


@pytest.mark.parametrize("bridge_mutation", ("stale", "missing"))
def test_real_child_stale_or_missing_bridge_fails_startup(
    monkeypatch, tmp_path, bridge_mutation,
):
    """A managed child cannot start after its authenticated parent bridge is stale or absent."""
    _runner, authority, coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, f"i17-{bridge_mutation}-bridge")
    root = authority._i17_service_handle
    payload = dict(supervisor._i17_child_bridge)
    if bridge_mutation == "stale":
        payload["parent_lease_id"] = "stale-parent-lease"
        supervisor._i17_child_bridge = payload
    else:
        supervisor._i17_child_bridge = None
    try:
        with pytest.raises(RuntimeError, match="startup:i17_child_bridge_mismatch"):
            supervisor.start()
        hosted_tui_custody.host_start_failed(supervisor)
        # The child was actually spawned and failed the authenticated startup handshake.  Its
        # parent reservation therefore stays UNKNOWN until a real owner receipt reconciles it;
        # releasing it here would turn an unproven child boundary into clean custody.
        assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
        assert hosted_tui_custody.close_profile(authority) is False
        assert authority._i17_service_handle is not None
    finally:
        supervisor.shutdown()


def test_incomplete_host_receipt_retains_profile_and_host_custody(
    monkeypatch, tmp_path
):
    _runner, authority, _coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-incomplete-host")
    try:
        assert hosted_tui_custody.host_shutdown(supervisor, {"clean": True}) is False
        assert hosted_tui_custody.close_profile(authority) is False
        assert authority._i17_service_handle is not None
    finally:
        supervisor.shutdown()


def test_real_hosted_room_worker_receipt_is_attached_to_profile_root(monkeypatch, tmp_path):
    _runner, authority, coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    runtime = HostedRoomRuntime(
        db_path=tmp_path / "hosted-room.db",
        rooms=(),
        rpc=object(),
        turn_lock=lambda _profile: nullcontext(),
        poll_interval_seconds=0.01,
        active_poll_interval_seconds=0.01,
    )
    service = SimpleNamespace(runtime=runtime, authority=authority)
    assert hosted_tui_custody.bind_room_service(service, authority) is not None
    runtime.start()
    try:
        assert runtime.status()["running"] is True
        assert runtime.stop(timeout=5) is True
        assert hosted_tui_custody.room_service_stop(service, True) is True
        assert authority._i17_room_terminal_receipt == {
            "stopped": True,
            "running": False,
            "workers_joined": True,
        }
        assert hosted_tui_custody.close_profile(authority) is True
        assert coordinator.status()["active_owner_classes"] == []
    finally:
        runtime.stop(timeout=5)


def test_rpc_admission_uses_profile_root_and_retains_unknown_worker(monkeypatch, tmp_path):
    _runner, authority, coordinator, _fence, home = _runtime(monkeypatch, tmp_path)
    root = authority._i17_service_handle
    custody, reason = hosted_tui_custody.rpc_admission({
        "id": "i17-rpc",
        "method": "profiles.list",
        "params": {"owner": {"profile_id": str(home)}},
    })
    assert reason is None and custody is not None
    assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
    assert hosted_tui_custody.rpc_settle(custody, known=False) is False
    assert coordinator.status()["active_child_parents"] == [root.lease.lease_id]
    assert hosted_tui_custody.close_profile(authority) is False
    foreign, foreign_reason = hosted_tui_custody.rpc_admission({
        "id": "i17-foreign-rpc",
        "method": "profiles.list",
        "params": {"owner": {"profile_id": str(tmp_path / "foreign")}},
    })
    assert foreign is None and foreign_reason == "profile_mismatch"


def test_room_status_read_failure_retains_starting_worker(monkeypatch, tmp_path):
    _runner, authority, _coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)

    def unreadable_status():
        raise RuntimeError("synthetic status read failure")

    service = SimpleNamespace(
        runtime=SimpleNamespace(status=unreadable_status),
        authority=authority,
    )
    hosted_tui_custody.bind_room_service(service, authority)
    hosted_tui_custody.room_service_start_failed(service)
    assert hosted_tui_custody.close_profile(authority) is False


def test_room_stop_status_read_failure_retains_worker(monkeypatch, tmp_path):
    _runner, authority, _coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)

    def unreadable_status():
        raise RuntimeError("synthetic status read failure")

    service = SimpleNamespace(
        runtime=SimpleNamespace(status=unreadable_status),
        authority=authority,
    )
    hosted_tui_custody.bind_room_service(service, authority)
    assert hosted_tui_custody.room_service_stop(service, True) is False
    assert hosted_tui_custody.close_profile(authority) is False


def test_reversible_abort_rebinds_exact_terminal_root_before_new_rpc(monkeypatch, tmp_path):
    runner, authority, coordinator, _fence, home = _runtime(monkeypatch, tmp_path)
    old_root = authority._i17_service_handle
    receipt = coordinator.begin_close(owner_id="i17-abort-rebind")
    assert receipt is not None
    assert hosted_tui_custody.close_profile(authority)
    assert coordinator.is_terminal_handle(old_root, owner_class="I17_hosted_tui_services")
    assert coordinator.abort_before_handoff(receipt) is None

    assert hosted_tui_custody.restore_after_abort(runner)
    new_root = authority._i17_service_handle
    assert new_root is not old_root
    assert old_root.lease.lease_id != new_root.lease.lease_id
    replay = coordinator.rebind_terminal_root(
        old_root, "i17-abort-replay", profile_id=str(home), authority_epoch=authority.epoch,
        owner_class="I17_hosted_tui_services",
    )
    assert getattr(replay, "code", None) == "stale_terminal_root"
    assert coordinator.is_active_handle(
        new_root, owner_class="I17_hosted_tui_services", profile_id=str(home),
        authority_epoch=authority.epoch,
    )
    custody, reason = hosted_tui_custody.rpc_admission({
        "id": "i17-after-abort",
        "method": "profiles.list",
        "params": {"owner": {"profile_id": str(home)}},
    })
    assert custody is not None and reason is None
    assert hosted_tui_custody.rpc_settle(custody, known=True)


def test_cold_profile_root_is_bound_before_close_owner_snapshot(monkeypatch, tmp_path):
    import asyncio

    runner, authority, coordinator, _fence, _home = _runtime(
        monkeypatch, tmp_path, bind_authority_now=False,
    )
    # Drive the same source-owned preparation used by the gateway runtime.  This profile has no
    # room or compute producer yet, but its hosted service is already configured, so preparation
    # must declare the root while the coordinator is OPEN rather than letting owner_evidence try
    # to invent a parent after begin_close.
    authority.hosted_room_service = SimpleNamespace(
        _transport_installed=True,
        runtime=SimpleNamespace(status=lambda: {"running": False, "current_tasks": 0}),
    )
    from gateway.session_hosted_service import ensure_hosted_service
    asyncio.run(ensure_hosted_service(runner))
    root = authority._i17_service_handle
    assert root is not None
    receipt = coordinator.begin_close(owner_id="i17-cold-profile")
    assert getattr(receipt, "state", None) == "closing"
    evidence = hosted_tui_custody.owner_evidence(runner, receipt)
    assert evidence is not None
    assert evidence["profile_roots"] == (str(authority.profile_id),)
    assert evidence["producer_graph"] == ({
        "profile_id": str(authority.profile_id),
        "authority_epoch": authority.epoch,
        "process_instance": coordinator.process_instance,
        "host": "absent",
        "room": "absent",
    },)
    assert coordinator.status()["active_owner_classes"] == []


def test_missing_source_producer_registry_is_unknown(monkeypatch, tmp_path):
    _runner, authority, _coordinator, _fence, _home = _runtime(monkeypatch, tmp_path)
    del authority._i17_producer_registry
    assert hosted_tui_custody.close_profile(authority) is False


def test_child_bridge_positive_and_foreign_identity_refusal(monkeypatch, tmp_path):
    _runner, _authority, _coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-child-bridge")
    payload = dict(supervisor._i17_child_bridge)
    from tui_gateway.i17_child_bridge import install, validate_parent

    assert validate_parent(supervisor, {"i17_child_bridge": payload})
    foreign = dict(payload, profile_id=str(tmp_path / "foreign"))
    assert not validate_parent(supervisor, {"i17_child_bridge": foreign})
    unknown = []
    bridge = install(payload, on_unknown=lambda: unknown.append(True))
    token, reason = bridge.admit({
        "id": "i17-child-rpc", "method": "profiles.list",
        "params": {"owner": {"profile_id": str(home)}},
    })
    assert token is not None and reason is None
    assert bridge.settle(token, known=False) is False
    assert unknown == [True]
