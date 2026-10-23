"""Causal custody controls for the persistent Codex service lease."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.runtime_service_custody import (
    ServiceCustodyError,
    TurnCustodyBinding,
    reserve_service,
)
from hermes_cli.backend_retirement import RetirementFence


def _coordinator_and_parent():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="process-1", authority_epoch=7, fence=RetirementFence(),
    )
    parent = coordinator.reserve_root(
        "I04_fifo_turn_execution", "claim:admission-1",
        profile_id="profile-1", authority_epoch=7,
    )
    return coordinator, parent


def _binding(coordinator, parent, authority=None):
    authority = authority or SimpleNamespace(
        profile_id="profile-1", epoch=7,
        _preservation_coordinator=lambda: coordinator,
        _preservation_children={"admission-1": parent},
    )
    return TurnCustodyBinding(
        authority=authority, coordinator=coordinator, parent_handle=parent,
        admission_id="admission-1", execution_generation=3, session_id="session-1",
        profile_id="profile-1", authority_epoch=7, process_instance="process-1",
    )


def test_parent_terminalization_waits_for_active_child():
    coordinator, parent = _coordinator_and_parent()
    child = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1",
        profile_id="profile-1", authority_epoch=7, parent=parent,
    )
    pending = coordinator.mark_terminal(parent)
    assert pending.code == "custody_pending"
    assert coordinator.mark_terminal(child) is None
    assert coordinator.mark_terminal(parent) is None


@pytest.mark.asyncio
async def test_service_request_unknown_retains_exact_lease(tmp_path, monkeypatch):
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = fixture.binding
        service = reserve_service(binding, "service:session-1", producer_key="codex")
        service.begin_request(binding)
        service.finish_request(binding, durable=False)
        status = fixture.binding.coordinator.status()
        assert service.handle.lease.lease_id in status["active_child_parents"]
        assert "I16_turn_sidecars" in status["active_owner_classes"]
        with pytest.raises(ServiceCustodyError, match="custody_unknown"):
            service.begin_request(binding)
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_copied_parent_handle_cannot_bind_a_service(tmp_path, monkeypatch):
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = fixture.binding
        copied_parent = replace(binding.parent_handle)
        copied_binding = replace(binding, parent_handle=copied_parent)
        baseline = binding.coordinator.status()
        assert copied_binding is not binding
        assert copied_binding == binding
        assert copied_binding.parent_handle is not binding.parent_handle
        assert copied_binding.parent_handle == binding.parent_handle
        native_parent = fixture.authority._preservation_handles[binding.admission_id]
        assert native_parent.owner_class == "I01_native_inbound"
        assert binding.parent_handle.owner_class == "I04_fifo_turn_execution"
        assert binding.coordinator._parent(binding.parent_handle) is binding.parent_handle.lease
        assert binding.parent_handle.lease.parent_lease_id == native_parent.lease.lease_id
        assert set(baseline["active_owner_classes"]) == {
            "I01_native_inbound", "I04_fifo_turn_execution",
        }
        assert "I16_turn_sidecars" not in baseline["active_owner_classes"]

        with pytest.raises(ServiceCustodyError, match="preservation_owner_mismatch"):
            reserve_service(copied_binding, "service:session-1")
        assert binding.coordinator.status() == baseline
        assert "I16_turn_sidecars" not in binding.coordinator.status()["active_owner_classes"]
    finally:
        fixture.cleanup()
