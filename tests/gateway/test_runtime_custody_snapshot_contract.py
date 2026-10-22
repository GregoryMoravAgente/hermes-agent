"""Snapshot boundary contract for the preservation coordinator.

The production snapshot reader already requires an outer owner/drain lease. This test-only contract
keeps that requirement explicit: a database snapshot is evidence under custody, never the owner.
"""
from __future__ import annotations

from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
from tools.process_registry import ProcessRegistry
from tools.process_registry_custody import ForegroundParentBinding


def test_runtime_snapshot_requires_close_owner_before_handoff():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="snapshot-test-instance",
        authority_epoch=3,
        fence=RetirementFence(),
    )
    close = coordinator.begin_close(owner_id="snapshot-owner", deadline_s=30)
    assert not isinstance(close, CustodyRefusal)
    # The actual hermes_state_runtime.snapshot_runtime_admission_workers call is made by the
    # gateway integration only after this exact close receipt exists. A source-only snapshot
    # without this receipt cannot be treated as complete ownership evidence.
    assert coordinator.status()["state"] == "closing"
    assert coordinator.abort_before_handoff(close) is None


def test_unknown_process_lease_reconciles_only_with_exact_owner_identity():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="snapshot-process-instance",
        authority_epoch=4,
        fence=RetirementFence(),
    )
    handle = coordinator.reserve_root(
        "I15_process_registry_terminal",
        "snapshot-process-owner",
        profile_id="profile-a",
        authority_epoch=4,
    )
    assert not isinstance(handle, CustodyRefusal)
    coordinator.mark_unknown(handle, "partial_launch")
    assert isinstance(
        coordinator.reconcile_unknown(
            handle,
            owner_id="snapshot-process-owner",
            profile_id="profile-b",
            process_instance="snapshot-process-instance",
            authority_epoch=4,
        ),
        CustodyRefusal,
    )
    assert coordinator.reconcile_unknown(
        handle,
        owner_id="snapshot-process-owner",
        profile_id="profile-a",
        process_instance="snapshot-process-instance",
        authority_epoch=4,
    ) is None
    assert coordinator.status()["active_owner_classes"] == ["I15_process_registry_terminal"]


def test_owner_evidence_stays_unknown_while_exact_foreground_record_is_retained(tmp_path):
    coordinator = RuntimeCustodyCoordinator(
        process_instance="snapshot-foreground-instance", authority_epoch=5,
        fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    binding = ForegroundParentBinding("session", "session-key", "tool-call", "turn", 2)
    custody = registry.reserve_foreground_custody(
        profile_id=str(tmp_path), binding=binding,
    )
    transaction = type("ReservedTransaction", (), {"custody": custody})()
    assert registry.retain_foreground_custody(transaction, binding=binding) is True
    assert registry.foreground_binding_active(
        session_id="session", session_key="session-key", turn_id="turn",
        run_generation=2, tool_call_id="tool-call",
    ) is True
    assert registry.foreground_delivery_token(
        session_id="session", session_key="session-key", turn_id="turn",
        run_generation=2, tool_call_id="tool-call",
    )
    assert registry.preservation_owner_evidence(
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=0,
        owner_id=f"{coordinator.process_instance}:I15_process_registry_terminal",
        profile_id=str(tmp_path),
    ) is None
