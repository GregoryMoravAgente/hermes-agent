"""Causal tests for the AGC1663 preservation close protocol.

These tests exercise the exact RetirementFence lease objects through the proposed runtime
coordinator. They do not treat an empty registry, a timeout, a PID, or a task completion as a
whole-runtime verdict.
"""
from __future__ import annotations

from dataclasses import replace

from gateway.runtime_custody import OWNER_CLASSES, CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


def _coordinator() -> RuntimeCustodyCoordinator:
    return RuntimeCustodyCoordinator(
        process_instance="runtime-test-instance",
        authority_epoch=7,
        fence=RetirementFence(),
    )


def _seal_all(coordinator: RuntimeCustodyCoordinator, receipt) -> None:
    for owner_class in OWNER_CLASSES:
        assert coordinator.seal_owner(
            owner_class,
            {
                "source": next(item.source for item in coordinator.owner_manifest if item.owner_class == owner_class),
                "process_instance": receipt.process_instance,
                "authority_epoch": receipt.authority_epoch,
                "close_generation": receipt.close_generation,
                "owner_id": f"owner-{owner_class}",
                "observed": True,
                "cross_process_proof": True,
                "os_lock_owner": True,
            },
        ) is None


def test_unwired_source_class_is_visible_and_blocks_handoff():
    coordinator = _coordinator()
    close = coordinator.begin_close(owner_id="unwired-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    status = coordinator.status()
    assert len(status["unwired_owner_classes"]) == len(OWNER_CLASSES)
    verdict = coordinator.prepare_handoff(close)
    assert isinstance(verdict, CustodyRefusal) and verdict.code == "unknown_owner"
    assert verdict.owner_class == OWNER_CLASSES[0]
    assert coordinator.abort_before_handoff(close) is None


def test_cross_process_owner_without_real_lock_proof_is_refused():
    coordinator = _coordinator()
    close = coordinator.begin_close(owner_id="cross-process-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    spec = next(item for item in coordinator.owner_manifest if item.owner_class == "I03_api_run_direct")
    refused = coordinator.seal_owner(
        spec.owner_class,
        {
            "source": spec.source,
            "process_instance": close.process_instance,
            "authority_epoch": close.authority_epoch,
            "close_generation": close.close_generation,
            "owner_id": "direct-run-1",
            "observed": True,
        },
    )
    assert isinstance(refused, CustodyRefusal) and refused.code == "cross_process_unproven"
    assert coordinator.status()["unknown_owner_classes"] == ["I03_api_run_direct"]
    assert coordinator.abort_before_handoff(close) is None


def test_close_race_refuses_new_root_but_keeps_accepted_parent_and_child():
    coordinator = _coordinator()
    parent = coordinator.reserve_root("I01_native_inbound", "inbound-1", profile_id="p", authority_epoch=7)
    assert not isinstance(parent, CustodyRefusal)
    close = coordinator.begin_close(owner_id="native-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    late = coordinator.reserve_root("I02_api_session_turn", "late-1", profile_id="p", authority_epoch=7)
    assert isinstance(late, CustodyRefusal) and late.code == "runtime_draining"
    child = coordinator.reserve_root("I08_goals_loops_heartbeat", "child-1", profile_id="p", authority_epoch=7, parent=parent)
    assert not isinstance(child, CustodyRefusal)
    assert coordinator.mark_terminal(child) is None
    assert coordinator.mark_terminal(parent) is None
    assert coordinator.abort_before_handoff(close) is None
    reopened = coordinator.reserve_root("I02_api_session_turn", "after-abort", profile_id="p", authority_epoch=7)
    assert not isinstance(reopened, CustodyRefusal)
    assert coordinator.mark_terminal(reopened) is None


def test_unknown_owner_refuses_and_abort_restores_admission_without_shutdown():
    coordinator = _coordinator()
    close = coordinator.begin_close(owner_id="unknown-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    _seal_all(coordinator, close)
    assert coordinator.mark_owner_unknown("I15_process_registry_terminal", "read_failed")
    verdict = coordinator.prepare_handoff(close)
    assert isinstance(verdict, CustodyRefusal) and verdict.code == "unknown_owner"
    assert coordinator.abort_before_handoff(close) is None
    admitted = coordinator.reserve_root("I03_api_run_direct", "still-live", profile_id="p", authority_epoch=7)
    assert not isinstance(admitted, CustodyRefusal)
    assert coordinator.mark_terminal(admitted) is None


def test_finalizer_and_partial_child_keep_custody_pending():
    coordinator = _coordinator()
    parent = coordinator.reserve_root("I15_process_registry_terminal", "process-1", profile_id="p", authority_epoch=7)
    assert not isinstance(parent, CustodyRefusal)
    close = coordinator.begin_close(owner_id="finalizer-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    _seal_all(coordinator, close)
    assert coordinator.mark_finalizing(parent, "reader-and-reaper-1") is None
    verdict = coordinator.prepare_handoff(close)
    assert isinstance(verdict, CustodyRefusal) and verdict.code == "custody_pending"
    assert coordinator.abort_before_handoff(close) is None
    assert coordinator.mark_terminal(parent) is None


def test_unadopted_foreign_lease_blocks_commit_and_never_gets_released():
    fence = RetirementFence()
    foreign = fence.acquire(owner_kind="foreign-rpc", owner_id="foreign-1", profile_id="p")
    assert foreign is not None
    coordinator = RuntimeCustodyCoordinator(process_instance="runtime-test-instance", authority_epoch=7, fence=fence)
    close = coordinator.begin_close(owner_id="handoff-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    _seal_all(coordinator, close)
    verdict = coordinator.prepare_handoff(close)
    assert isinstance(verdict, CustodyRefusal) and verdict.code == "foreign_or_unmapped_lease"
    assert coordinator.abort_before_handoff(close) is None
    assert fence.release(foreign) is True


def test_handoff_receipt_stays_closed_until_exact_native_ack():
    coordinator = _coordinator()
    close = coordinator.begin_close(owner_id="native-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    _seal_all(coordinator, close)
    handoff = coordinator.prepare_handoff(close)
    assert not isinstance(handoff, CustodyRefusal)
    late = coordinator.reserve_root("I01_native_inbound", "late-2", profile_id="p", authority_epoch=7)
    assert isinstance(late, CustodyRefusal) and late.code == "runtime_draining"
    forged = replace(handoff, receipt_id="different-receipt")
    assert isinstance(coordinator.acknowledge_native(forged), CustodyRefusal)
    assert coordinator.state == "handoff_pending"
    done = coordinator.acknowledge_native(handoff)
    assert not isinstance(done, CustodyRefusal) and done.state == "closed"
