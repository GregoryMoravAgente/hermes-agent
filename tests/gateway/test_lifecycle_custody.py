"""Causal I19 lifecycle custody tests.

The tests use the real RuntimeCustodyCoordinator lease objects and a source-bound graph provider.
They keep the exact lease after a refused/failing observation so retry cannot mint a replacement
handle or infer closure from an empty registry.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from gateway.lifecycle_custody import (
    I19_SOURCE,
    LifecycleCustody,
    LifecycleGraphSnapshot,
    LifecycleJoin,
    LifecycleNativeSettlement,
    LifecycleSourceGraph,
    _runner_source_snapshot,
    preservation_owner_evidence,
    record_successor_ready_from_control,
)
from gateway.run_shutdown import GatewayShutdownMixin
from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from gateway.session_authorities import SessionAuthorities
from hermes_cli.backend_retirement import RetirementFence
from hermes_state_registry import SessionDbCloseReceipt


@dataclass
class _GraphState:
    finalizer_ready: bool = True
    native_ready: bool = True
    successor_ready: bool = True
    global_ready: bool = True


def _fixture(*, restart: bool = False):
    state = _GraphState()
    registry = object()
    finalizer = object()
    native = object()
    successor = object()
    global_close = object()

    def snapshot():
        return LifecycleGraphSnapshot(
            source=I19_SOURCE,
            finalizer_registry=LifecycleJoin(
                "finalizer_registry", registry, "test:registry", lambda: True,
            ),
            finalizers=(
                LifecycleJoin("finalizer", finalizer, "test:finalizer", lambda: state.finalizer_ready),
            ),
            native_helpers=(
                LifecycleJoin("native", native, "test:native", lambda: state.native_ready),
            ),
            successors=(
                LifecycleJoin("successor", successor, "test:successor", lambda: state.successor_ready),
            ) if restart else (),
            global_close=LifecycleJoin(
                "global_close", global_close, "test:global", lambda: state.global_ready,
            ),
        )

    coordinator = RuntimeCustodyCoordinator(
        process_instance="i19-test-process", authority_epoch=4, fence=RetirementFence(),
    )
    runner = type("Runner", (), {
        "session_runtime_descriptor": {"instance_id": "i19-test-process", "authority_epoch": 4},
    })()
    custody = LifecycleCustody(
        runner, coordinator,
        source_graph=LifecycleSourceGraph(snapshot),
    )
    runner.lifecycle_custody = custody
    return custody, coordinator, state, (finalizer, native, successor, global_close)


def _strict_fixture(tmp_path):
    """Build the initialized-runtime source graph used by the new causal cases."""
    class _Finalizer:
        state = "terminal"

    class _Authority:
        def __init__(self, finalizer):
            self._finalizer = finalizer

        def external_process_finalizers(self):
            return (self._finalizer,)

    home = Path(tmp_path)
    finalizer = _Finalizer()
    registry = SessionAuthorities(home)
    registry.add(home, _Authority(finalizer))
    event = asyncio.Event()
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i19-strict-process", authority_epoch=5, fence=RetirementFence(),
    )
    runner = type("Runner", (), {
        "session_runtime_descriptor": {
            "instance_id": "i19-strict-process", "authority_epoch": 5,
        },
        "session_authorities": registry,
        "_shutdown_event": event,
        "_stop_task": None,
        "_restart_task": None,
    })()
    custody = LifecycleCustody(runner, coordinator)
    runner.lifecycle_custody = custody
    return custody, coordinator, runner, registry, event


def _source_close_context(runner, *, succeeded=True, receipt=True):
    """Use the same source-issued StopContext contract as actual ``_stop_impl``."""
    context = GatewayShutdownMixin._StopContext(deferred_count=lambda: 0)
    context.db_close_succeeded = succeeded
    context.db_close_receipt = (
        SessionDbCloseReceipt(
            "test:close", attempted=1, succeeded=1 if succeeded else 0,
            failed=0 if succeeded else 1,
        )
        if receipt else None
    )
    runner._lifecycle_stop_context = context
    runner._lifecycle_stop_context_token = context.source_token
    return context


def test_active_late_and_quiescent_registered_graph_keep_exact_lease():
    custody, coordinator, _state, objects = _fixture()
    lease = custody.admit("stop", owner_id="stop-1")
    assert lease.owner_id == "stop-1"
    late = custody.admit("idle", owner_id="idle-late")
    assert isinstance(late, CustodyRefusal) and late.code == "lifecycle_owned"
    evidence = lease.complete()
    assert evidence.owner_id == "stop-1"
    assert lease.state == "terminal"
    assert custody.status()["active"] == {}
    assert isinstance(lease.complete(), CustodyRefusal)
    assert coordinator.status()["active_owner_classes"] == []
    assert objects[0] is not None


def test_failed_finalizer_keeps_same_handle_for_retry():
    custody, _coordinator, state, _objects = _fixture()
    state.finalizer_ready = False
    lease = custody.admit("stop", owner_id="stop-retry")
    refused = lease.complete()
    assert isinstance(refused, CustodyRefusal)
    assert lease.state == "unknown"
    original_handle = lease.handle
    state.finalizer_ready = True
    evidence = lease.retry()
    assert evidence.owner_id == "stop-retry"
    assert lease.handle is original_handle
    assert lease.state == "terminal"


def test_refused_admission_when_runtime_close_is_already_owned():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i19-close-process", authority_epoch=4, fence=RetirementFence(),
    )
    close = coordinator.begin_close(owner_id="preservation-close")
    assert close.state == "closing"
    runner = type("Runner", (), {
        "session_runtime_descriptor": {"instance_id": "i19-close-process", "authority_epoch": 4},
    })()
    custody = LifecycleCustody(runner, coordinator, source_graph=LifecycleSourceGraph(lambda: None))
    refused = custody.admit("stop", owner_id="late-stop")
    assert isinstance(refused, CustodyRefusal) and refused.code == "runtime_draining"
    assert coordinator.abort_before_handoff(close) is None


def test_restart_requires_exact_successor_join_and_can_publish_owner_evidence():
    custody, coordinator, _state, _objects = _fixture(restart=True)
    lease = custody.admit("restart", owner_id="restart-1")
    assert lease.complete().owner_id == "restart-1"
    custody.record_global_close(object())
    close = coordinator.begin_close(owner_id="preservation-close")
    evidence = preservation_owner_evidence(
        custody.runner,
        process_instance="i19-test-process",
        authority_epoch=4,
        close_generation=close.close_generation,
        owner_id="i19-test-process:I19_gateway_lifecycle",
    )
    assert evidence is not None
    assert evidence["source"] == I19_SOURCE
    assert evidence["cross_process_proof"] is True
    assert evidence["lifecycle_receipts"][0]["successor_ids"]
    assert coordinator.status()["active_owner_classes"] == []
    assert coordinator.abort_before_handoff(close) is None


def test_quiescent_registered_graph_uses_real_authority_registry_and_stop_task(tmp_path):
    class _Finalizer:
        state = "terminal"

    class _Authority:
        def __init__(self, finalizer):
            self._finalizer = finalizer

        def external_process_finalizers(self):
            return (self._finalizer,)

    loop = asyncio.new_event_loop()
    try:
        finalizer = _Finalizer()
        registry = SessionAuthorities(Path(tmp_path))
        registry.add(Path(tmp_path), _Authority(finalizer))
        stop_task = loop.create_future()
        stop_task.set_result(None)
        coordinator = RuntimeCustodyCoordinator(
            process_instance="i19-real-source", authority_epoch=5, fence=RetirementFence(),
        )
        runner = type("Runner", (), {
            "session_runtime_descriptor": {"instance_id": "i19-real-source", "authority_epoch": 5},
            "session_authorities": registry,
            "_shutdown_event": asyncio.Event(),
            "_stop_task": stop_task,
            "_restart_task": None,
        })()
        custody = LifecycleCustody(runner, coordinator)
        runner.lifecycle_custody = custody
        lease = custody.admit("stop", owner_id="real-source-stop")
        runner._shutdown_event.set()
        close_context = _source_close_context(runner)
        assert custody.record_global_close(
            close_context, close_generation=lease.close_generation,
        ) is True
        assert custody.record_native_settlement(
            "stop",
            LifecycleNativeSettlement(
                "stop", close_context, runner._shutdown_event, True,
                db_close_receipt=close_context.db_close_receipt,
            ),
        ) is True
        evidence = lease.complete()
        assert evidence.finalizer_ids == (id(finalizer),)
        assert evidence.native_ids
        snapshot = _runner_source_snapshot(runner, custody)
        assert snapshot.finalizer_registry.identity is registry
        assert snapshot.global_close.identity is custody._global_close_receipt
    finally:
        loop.close()


def test_restart_refuses_until_successor_join_is_observed():
    custody, _coordinator, state, _objects = _fixture(restart=True)
    state.successor_ready = False
    lease = custody.admit("restart", owner_id="restart-late")
    refused = lease.complete()
    assert isinstance(refused, CustodyRefusal)
    assert lease.state == "unknown"
    state.successor_ready = True
    assert lease.retry().owner_id == "restart-late"


def test_external_drain_transition_uses_registered_source_without_global_close():
    custody, coordinator, _state, _objects = _fixture()
    lease = custody.admit("external-drain", owner_id="drain-1")
    assert lease.complete_external_transition() is True
    assert lease.state == "terminal"
    assert coordinator.status()["active_owner_classes"] == []


def test_idle_public_entry_refuses_while_stop_lease_is_active():
    custody, _coordinator, _state, _objects = _fixture()
    runner = custody.runner
    runner._running = True
    runner._draining = False
    runner._exit_reason = None
    runner.stop = lambda: None
    runner.session_runtime_descriptor.update(state="ready", capabilities=["session"])
    assert custody.admit("stop", owner_id="already-stopping")

    from gateway.run_idle_exit import _begin_idle_exit

    _begin_idle_exit(runner, 30.0)
    assert runner._draining is False
    assert not hasattr(runner, "_idle_exit_stop_task")


def test_external_drain_public_entry_refuses_while_stop_lease_is_active():
    custody, _coordinator, _state, _objects = _fixture()
    runner = custody.runner
    runner._running = True
    runner._draining = False
    runner._external_drain_active = False
    runner._active_work_count = lambda: 0
    runner._update_runtime_status = lambda *_args, **_kwargs: None
    assert custody.admit("stop", owner_id="already-stopping")

    from gateway.run_shutdown import GatewayShutdownMixin

    GatewayShutdownMixin._enter_external_drain(runner)
    assert runner._external_drain_active is False


def test_live_maintenance_close_proves_registered_zero_work_and_rejects_stale_graph(tmp_path):
    custody, coordinator, runner, registry, _event = _strict_fixture(tmp_path)
    close = coordinator.begin_close(owner_id="preservation-close")
    assert close.state == "closing"
    assert custody.begin_maintenance_close(close.close_generation) is None

    evidence = preservation_owner_evidence(
        runner,
        process_instance="i19-strict-process",
        authority_epoch=5,
        close_generation=close.close_generation,
        owner_id="i19-strict-process:I19_gateway_lifecycle",
    )
    assert evidence is not None
    assert evidence["no_work"] is True
    assert evidence["lifecycle_receipts"] == []
    assert evidence["global_close_identity"] is None

    late = custody.admit("stop", owner_id="late-stop")
    assert isinstance(late, CustodyRefusal) and late.code == "runtime_draining"

    # A same-registry authority whose source finalizer graph changed is stale too; the zero-work
    # branch cannot turn that changed producer set into an absence receipt.
    authority = next(iter(registry))
    original_finalizer = authority.external_process_finalizers()[0]
    authority._finalizer = SimpleNamespace(state="terminal")
    assert preservation_owner_evidence(
        runner,
        process_instance="i19-strict-process",
        authority_epoch=5,
        close_generation=close.close_generation,
        owner_id="i19-strict-process:I19_gateway_lifecycle",
    ) is None
    authority._finalizer = original_finalizer

    # Replacing the registry with a same-shaped object is stale source state, not an empty
    # absence proof. The close must remain tied to the original registered graph.
    runner.session_authorities = SessionAuthorities(Path(tmp_path))
    assert preservation_owner_evidence(
        runner,
        process_instance="i19-strict-process",
        authority_epoch=5,
        close_generation=close.close_generation,
        owner_id="i19-strict-process:I19_gateway_lifecycle",
    ) is None
    assert custody.abort_maintenance_close(close.close_generation) is True
    assert coordinator.abort_before_handoff(close) is None


def test_completed_stop_task_is_not_native_settlement_and_keeps_same_handle(tmp_path):
    custody, _coordinator, runner, _registry, event = _strict_fixture(tmp_path)
    loop = asyncio.new_event_loop()
    try:
        stop_task = loop.create_future()
        stop_task.set_result(None)
        runner._stop_task = stop_task
        event.set()
        lease = custody.admit("stop", owner_id="stop-native-missing")
        original_handle = lease.handle
        close_context = _source_close_context(runner)
        assert custody.record_global_close(
            close_context, close_generation=lease.close_generation,
        ) is True
        refused = lease.complete()
        assert isinstance(refused, CustodyRefusal)
        assert refused.code == "native_join_missing"
        assert lease.state == "unknown"
        assert lease.handle is original_handle
    finally:
        loop.close()


def test_failed_db_close_cannot_publish_global_or_native_settlement(tmp_path):
    custody, _coordinator, _runner, _registry, event = _strict_fixture(tmp_path)
    event.set()
    lease = custody.admit("stop", owner_id="stop-db-close-failed")
    failed_context = _source_close_context(_runner, succeeded=False)
    assert custody.record_global_close(
        failed_context, close_generation=lease.close_generation,
    ) is False
    assert custody.record_native_settlement(
        "stop",
        LifecycleNativeSettlement(
            "stop", failed_context, event, False,
            db_close_receipt=failed_context.db_close_receipt,
        ),
    ) is True
    refused = lease.complete()
    assert isinstance(refused, CustodyRefusal)
    assert refused.code == "global_close_unavailable"
    assert lease.state == "unknown"


def test_missing_checked_db_close_receipt_cannot_publish_global_boundary(tmp_path):
    custody, _coordinator, _runner, _registry, event = _strict_fixture(tmp_path)
    event.set()
    lease = custody.admit("stop", owner_id="stop-db-close-receipt-missing")
    missing_context = _source_close_context(_runner, receipt=False)
    assert custody.record_global_close(
        missing_context, close_generation=lease.close_generation,
    ) is False
    native = LifecycleNativeSettlement("stop", missing_context, event, True)
    assert native.ready() is False
    assert custody.record_native_settlement("stop", native) is True
    refused = lease.complete()
    assert isinstance(refused, CustodyRefusal)
    assert refused.code == "global_close_unavailable"
    assert lease.state == "unknown"


def test_receipt_shaped_foreign_stop_context_cannot_publish_global_boundary(tmp_path):
    custody, _coordinator, runner, _registry, event = _strict_fixture(tmp_path)
    event.set()
    lease = custody.admit("stop", owner_id="stop-foreign-context")
    source_context = _source_close_context(runner)
    foreign_context = SimpleNamespace(
        db_close_succeeded=True,
        db_close_receipt=source_context.db_close_receipt,
    )
    assert custody.record_global_close(
        foreign_context, close_generation=lease.close_generation,
    ) is False
    assert custody.record_native_settlement(
        "stop",
        LifecycleNativeSettlement(
            "stop", foreign_context, event, True,
            db_close_receipt=foreign_context.db_close_receipt,
        ),
    ) is False


def test_db_close_receipt_rechecks_the_source_context_on_retry(tmp_path):
    custody, _coordinator, _runner, _registry, event = _strict_fixture(tmp_path)
    event.set()
    lease = custody.admit("stop", owner_id="stop-db-close-mutated")
    close_context = _source_close_context(_runner)
    native_context = close_context
    assert custody.record_global_close(
        close_context, close_generation=lease.close_generation,
    ) is True
    assert custody.record_native_settlement(
        "stop",
        LifecycleNativeSettlement(
            "stop", native_context, event, True,
            db_close_receipt=native_context.db_close_receipt,
        ),
    ) is True
    close_context.db_close_succeeded = False
    refused = lease.complete()
    assert isinstance(refused, CustodyRefusal)
    assert refused.code == "global_close_unavailable"
    assert lease.state == "unknown"


def test_restart_task_and_helper_need_actual_successor_ready_receipt(tmp_path, monkeypatch):
    custody, _coordinator, runner, _registry, event = _strict_fixture(tmp_path)
    loop = asyncio.new_event_loop()
    try:
        restart_task = loop.create_future()
        restart_task.set_result(None)
        runner._restart_task = restart_task
        event.set()
        lease = custody.admit("restart", owner_id="restart-successor")
        original_handle = lease.handle
        helper = SimpleNamespace(poll=lambda: 0)
        assert custody.record_restart_helper(helper) is True
        close_context = _source_close_context(runner)
        assert custody.record_global_close(
            close_context, close_generation=lease.close_generation,
        ) is True
        assert custody.record_native_settlement(
            "restart",
            LifecycleNativeSettlement(
                "restart", close_context, event, True,
                db_close_receipt=close_context.db_close_receipt,
            ),
        ) is True

        refused = lease.complete()
        assert isinstance(refused, CustodyRefusal)
        assert refused.code == "successor_unavailable"
        assert lease.state == "unknown"
        assert lease.handle is original_handle

        payload = {
            "instance_id": "i19-successor-process",
            "authority_epoch": 6,
            "state": "ready",
            "capabilities": ["session-authority-v1"],
        }
        same_epoch = dict(payload, authority_epoch=5)
        monkeypatch.setattr("gateway.control_socket.identify_gateway", lambda _home: same_epoch)
        assert record_successor_ready_from_control(runner, Path(tmp_path)) is False
        monkeypatch.setattr("gateway.control_socket.identify_gateway", lambda _home: payload)
        monkeypatch.setattr("gateway.control_socket.resolve_client_socket_path", lambda _home: "/tmp/i19.sock")
        assert record_successor_ready_from_control(runner, Path(tmp_path)) is True
        evidence = lease.retry()
        assert evidence.owner_id == "restart-successor"
        assert evidence.successor_ids
        assert lease.handle is original_handle
        assert lease.state == "terminal"
    finally:
        loop.close()

