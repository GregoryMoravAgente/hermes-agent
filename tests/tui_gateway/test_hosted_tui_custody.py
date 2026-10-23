"""Real I17 custody consumers for a hosted compute service.

The tests bind the existing RuntimeCustodyCoordinator to a real SessionAuthorities
registry and a real HostSupervisor child.  They exercise the production custody
adapter around the producer and its shutdown receipt; no local mirror or fabricated
terminal state is used.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

import hermes_state
import gateway.runtime_custody as runtime_custody_module
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
from gateway.run_runtime import (
    _retire_profile_authority,
    drain_gateway_runtime,
    initialize_gateway_runtime,
)
from gateway.runtime_ownership import ProfileReleaseOutcome, process_ownership
from gateway.session_authorities import all_authorities
from hermes_cli import backend_retirement
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway import hosted_tui_custody
from tui_gateway.host_supervisor import HostSupervisor
from tui_gateway.hosted_room_driver import HostedRoomRuntime


pytestmark = pytest.mark.platforms("posix", "windows")


# These are test-resource references, never custody state. A resource enters the retained list
# only when source-owned retirement or physical-release proof is UNKNOWN or pending.
_ACTIVE_I17_TEST_RESOURCES: list[dict] = []
_RETAINED_I17_TEST_RESOURCES: list[dict] = []


def _owned_runtime_loop():
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def run_loop():
        asyncio.set_event_loop(loop)
        ready.set()
        loop.run_forever()

    thread = threading.Thread(
        target=run_loop,
        name="i17-hosted-custody-test-loop",
        daemon=True,
    )
    thread.start()
    if not ready.wait(5):
        raise RuntimeError("I17 test runtime loop did not start")

    def call(coroutine, *, timeout=60):
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        try:
            return future.result(timeout)
        except BaseException:
            if not future.done():
                future.cancel()
            raise

    return loop, thread, call


async def _pending_runtime_tasks():
    current = asyncio.current_task()
    return tuple(
        task for task in asyncio.all_tasks()
        if task is not current and not task.done()
    )


def _report_i17_cleanup(node, resource, reasons):
    if node is None or not hasattr(node, "add_report_section"):
        return
    status = {}
    coordinator = resource.get("coordinator")
    if coordinator is not None:
        try:
            status = coordinator.status()
        except BaseException:
            status = {"status_read": "failed"}
    node.add_report_section(
        "teardown",
        "i17-hosted-custody",
        repr({
            "clean_retirement_proven": False,
            "retained_boundary": True,
            "home": str(resource.get("home")),
            "reasons": tuple(reasons),
            "coordinator": status,
        }),
    )


def _retain_i17_resource(resource, node, reasons):
    resource["retained"] = True
    resource["retention_reasons"] = tuple(reasons)
    if resource not in _RETAINED_I17_TEST_RESOURCES:
        _RETAINED_I17_TEST_RESOURCES.append(resource)
    token = resource.get("home_token")
    if token is not None:
        reset_hermes_home_override(token)
        resource["home_token"] = None
    _report_i17_cleanup(node, resource, reasons)


def _stop_owned_runtime_loop(resource):
    call = resource.get("call")
    loop = resource.get("loop")
    thread = resource.get("thread")
    if loop is None or thread is None:
        return True
    if call is not None and call(_pending_runtime_tasks()):
        return False
    loop.call_soon_threadsafe(loop.stop)
    thread.join(60)
    if thread.is_alive():
        return False
    loop.close()
    resource["loop"] = None
    resource["thread"] = None
    return True


def _settle_i17_supervisors(resource):
    reasons = []
    # Complete the real child shutdown where the test did not already consume its receipt. A
    # returned receipt is passed through production host_shutdown; no synthetic receipt is made
    # for a child that never produced one.
    for supervisor in reversed(resource.get("supervisors", ())):
        if getattr(supervisor, "_i17_terminal_receipt", None) is not None:
            continue
        had_effect = getattr(supervisor, "_i17_test_had_host_effect", None)
        if had_effect is None:
            had_effect = hosted_tui_custody._host_effect_started(supervisor)
        if not had_effect:
            try:
                if hosted_tui_custody.host_start_failed(supervisor) is True:
                    continue
            except BaseException as exc:
                reasons.append(f"host_pre_effect:{type(exc).__name__}")
                continue
        receipts = getattr(supervisor, "_i17_test_shutdown_receipts", ())
        receipt = receipts[0] if receipts else None
        try:
            if receipt is None:
                receipt = supervisor.shutdown()
        except BaseException as exc:
            reasons.append(f"host_shutdown:{type(exc).__name__}")
            continue
        if isinstance(receipt, dict):
            try:
                if hosted_tui_custody.host_shutdown(supervisor, receipt) is not True:
                    reasons.append("host_receipt_unproven")
            except BaseException as exc:
                reasons.append(f"host_receipt:{type(exc).__name__}")
    return reasons


def _retire_i17_authorities(resource):
    runner = resource.get("runner")
    call = resource.get("call")
    authorities = tuple(
        authority for authority in resource.get("authorities", ())
        if authority is not None
    )
    reasons = []
    if runner is None or not resource.get("initialized") or call is None:
        return authorities, reasons
    try:
        call(drain_gateway_runtime(runner))
    except BaseException as exc:
        reasons.append(f"drain:{type(exc).__name__}")
    authorities = tuple(
        authority for authority in all_authorities(runner) if authority is not None
    )
    resource["authorities"] = authorities
    # Retirement is the source-owned join: the real authority stays canonical while workers,
    # rooms, finalizers, and I17 source custody settle.
    for authority in authorities:
        try:
            if call(_retire_profile_authority(authority)) is not True:
                reasons.append("authority_retirement_unproven")
        except BaseException as exc:
            reasons.append(f"authority_retirement:{type(exc).__name__}")
    return authorities, reasons


def _close_i17_stores(resource, authorities):
    runner = resource.get("runner")
    reasons = []
    try:
        for authority in authorities:
            db = getattr(authority, "db", None)
            close = getattr(db, "close", None)
            if callable(close):
                close()
        store = getattr(runner, "session_store", None)
        close_all = getattr(store, "close_all_db_handles", None)
        if callable(close_all):
            close_all()
        close_sessions = getattr(runner, "close_all_session_db_handles", None)
        if callable(close_sessions):
            close_sessions()
    except BaseException as exc:
        reasons.append(f"store_close:{type(exc).__name__}")
    return reasons


def _release_i17_home(resource):
    if not resource.get("reserved"):
        return []
    try:
        outcome = process_ownership.release_outcome(resource["home"])
    except BaseException as exc:
        return [f"physical_release:{type(exc).__name__}"]
    if outcome is not ProfileReleaseOutcome.RELEASED:
        return [f"physical_release:{getattr(outcome, 'value', outcome)}"]
    return []


def _cleanup_i17_resource(resource, node):
    if resource.get("cleaned") or resource.get("retained"):
        return

    reasons = _settle_i17_supervisors(resource)
    authorities, retirement_reasons = _retire_i17_authorities(resource)
    reasons.extend(retirement_reasons)
    if reasons:
        _retain_i17_resource(resource, node, reasons)
        return

    # Do not close stores or release the native profile lock while the owned loop could still
    # contain source work. A failed quiescence check retains the exact runtime for retry.
    try:
        quiescent = _stop_owned_runtime_loop(resource)
    except BaseException as exc:
        quiescent = False
        reasons.append(f"runtime_loop:{type(exc).__name__}")
    if not quiescent:
        reasons.append("runtime_loop_not_quiescent")
    if reasons:
        _retain_i17_resource(resource, node, reasons)
        return

    reasons.extend(_close_i17_stores(resource, authorities))
    if reasons:
        _retain_i17_resource(resource, node, reasons)
        return

    reasons.extend(_release_i17_home(resource))
    if reasons:
        _retain_i17_resource(resource, node, reasons)
        return

    token = resource.get("home_token")
    if token is not None:
        reset_hermes_home_override(token)
        resource["home_token"] = None
    runner = resource.get("runner")
    if runner is not None:
        runner.__dict__.pop("_i17_test_call", None)
    resource["cleaned"] = True


@pytest.fixture(autouse=True)
def _i17_runtime_cleanup(request, monkeypatch):
    # Depend on monkeypatch so cleanup runs while the real hosted-TUI globals are still bound.
    start = len(_ACTIVE_I17_TEST_RESOURCES)

    def finalize():
        resources = list(_ACTIVE_I17_TEST_RESOURCES[start:])
        for resource in reversed(resources):
            _cleanup_i17_resource(resource, request.node)
        del _ACTIVE_I17_TEST_RESOURCES[start:]

    request.addfinalizer(finalize)


def _runtime(monkeypatch, tmp_path: Path, *, bind_authority_now: bool = True):
    home = (tmp_path / "hermes-home").resolve()
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr("gateway.run._hermes_home", home)
    monkeypatch.setattr(
        hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH
    )
    monkeypatch.setattr(hosted_tui_custody, "_RUNTIME_RUNNER", None)
    monkeypatch.setattr(hosted_tui_custody, "_RUNTIME_COORDINATOR", None)

    # install_runtime_custody's production class has a definition-time default
    # (fence=hermes_cli.backend_retirement.retirement).  Patching that module global
    # alone cannot inject a fresh fence after runtime_custody imported it.  Keep the
    # canonical coordinator class identity and replace only its constructor default for
    # this fixture.  A per-test subclass would be unsafe: lifecycle_custody is cached
    # after the first bootstrap and would retain the previous subclass as its type check.
    fresh_fence = backend_retirement.RetirementFence()
    canonical_coordinator = runtime_custody_module.RuntimeCustodyCoordinator

    original_coordinator_init = canonical_coordinator.__init__
    missing_fence = object()

    def _fresh_fence_init(
        coordinator,
        *,
        process_instance,
        authority_epoch,
        fence=missing_fence,
        profile_scope=None,
    ):
        original_coordinator_init(
            coordinator,
            process_instance=process_instance,
            authority_epoch=authority_epoch,
            fence=fresh_fence if fence is missing_fence else fence,
            profile_scope=profile_scope,
        )

    monkeypatch.setattr(canonical_coordinator, "__init__", _fresh_fence_init)

    home_token = set_hermes_home_override(home)
    resource = {
        "home": home,
        "home_token": home_token,
        "reserved": False,
        "initialized": False,
        "runner": None,
        "authority": None,
        "authorities": (),
        "coordinator": None,
        "call": None,
        "loop": None,
        "thread": None,
        "supervisors": [],
            "fence": fresh_fence,
            "process_registry": None,
            "process_registry_binding": None,
    }
    _ACTIVE_I17_TEST_RESOURCES.append(resource)
    try:
        process_ownership.reserve([home])
        resource["reserved"] = True
        loop, thread, call = _owned_runtime_loop()
        resource.update(loop=loop, thread=thread, call=call)
        runner = GatewayRunner(GatewayConfig(sessions_dir=home / "sessions"))
        resource["runner"] = runner
        call(initialize_gateway_runtime(runner))
        from tools.process_registry import process_registry

        resource["process_registry"] = process_registry
        resource["process_registry_binding"] = getattr(process_registry, "_runtime_custody", None)
        authority = runner.session_authority
        authorities = tuple(
            authority_item
            for authority_item in getattr(runner, "session_authorities", ())
            if authority_item is not None
        )
        coordinator = runner.runtime_custody
        if authority is None or not authorities or coordinator is None:
            raise RuntimeError("I17 canonical runtime bootstrap was incomplete")
        resource.update(
            authority=authority,
            authorities=authorities,
            coordinator=coordinator,
            initialized=True,
        )
        # This test-only call helper schedules production coroutines on the already-owned loop; it
        # never participates in source identity or custody proof.
        runner._i17_test_call = call
        if bind_authority_now:
            hosted_tui_custody.bind_authority(
                authority, runner=runner, coordinator=coordinator
            )
        return runner, authority, coordinator, fresh_fence, home
    except BaseException:
        _cleanup_i17_resource(resource, None)
        raise


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
        env=synthetic_host_env(home, root),
        rpc_sink=rpc_sink,
        expected_build_sha="unknown",
        expected_hermes_home=str(home),
        heartbeat_secs=60,
    )
    original_shutdown = supervisor.shutdown
    supervisor._i17_test_shutdown_receipts = []
    supervisor._i17_test_had_host_effect = None

    def capture_shutdown(*args, **kwargs):
        if supervisor._i17_test_had_host_effect is None:
            supervisor._i17_test_had_host_effect = hosted_tui_custody._host_effect_started(
                supervisor
            )
        receipt = original_shutdown(*args, **kwargs)
        if not supervisor._i17_test_shutdown_receipts and isinstance(receipt, dict):
            supervisor._i17_test_shutdown_receipts.append(receipt)
        return receipt

    supervisor.shutdown = capture_shutdown
    for resource in reversed(_ACTIVE_I17_TEST_RESOURCES):
        if resource.get("home") == home and not resource.get("retained"):
            resource["supervisors"].append(supervisor)
            break
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
    runner._i17_test_call(ensure_hosted_service(runner))
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


@pytest.mark.parametrize("close_before_release", (False, True))
def test_real_unstarted_host_releases_registered_source_before_or_during_close(
    monkeypatch, tmp_path, close_before_release,
):
    """A genuine no-Popen producer settles its exact reservation even after ingress closes."""
    _runner, authority, coordinator, fence, home = _runtime(monkeypatch, tmp_path)
    supervisor, _ = _supervisor(monkeypatch, tmp_path, fence)
    assert hosted_tui_custody.bind_supervisor(supervisor, home, "i17-real-pre-effect")
    registry = authority._i17_producer_registry
    handle = supervisor._i17_host_handle
    assert registry["host"]["state"] == "registered"
    assert registry["host"]["supervisor"] is supervisor
    assert not hosted_tui_custody._host_effect_started(supervisor)
    if close_before_release:
        receipt = coordinator.begin_close(owner_id="i17-real-pre-effect-close")
        assert getattr(receipt, "state", None) == "closing"
        assert hosted_tui_custody.close_profile(authority) is False
        assert registry["intake_closed"] is True
    assert hosted_tui_custody.host_start_failed(supervisor) is True
    assert registry["host"]["state"] == "absent"
    assert not coordinator.is_active_handle(handle, owner_class="I17_hosted_tui_services")
    assert coordinator.status()["active_child_parents"] == []
    assert hosted_tui_custody.close_profile(authority) is True
    assert coordinator.status()["active_owner_classes"] == []
