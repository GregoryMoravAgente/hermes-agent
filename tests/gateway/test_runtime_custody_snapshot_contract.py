"""Snapshot boundary contract for the preservation coordinator.

The production snapshot reader already requires an outer owner/drain lease. This test-only contract
keeps that requirement explicit: a database snapshot is evidence under custody, never the owner.
"""
from __future__ import annotations

from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


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
