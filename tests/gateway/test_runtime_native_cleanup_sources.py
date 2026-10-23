"""Causal controls for the live native cleanup producer.

The positive cases use real profile lock descriptors and the real cron jobs-lock source registry.
They do not turn a copied receipt, a task state, or an empty collection into native zero work.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import (
    CustodyRefusal,
    NativeCleanupPlan,
    NativeCleanupZeroWorkReceipt,
)
from gateway.runtime_native_cleanup_sources import (
    CronNativeCleanupParticipant,
    ProviderNativeCleanupParticipant,
    ProviderNativeCleanupReceipt,
    ProfileNativeCleanupParticipant,
    build_native_cleanup_plan,
    _cron_participant,
)


def _real_capability(coordinator, handoff, source_receipt, participant, fences):
    plan = NativeCleanupPlan(
        process_instance=handoff.process_instance,
        authority_epoch=handoff.authority_epoch,
        close_generation=handoff.close_generation,
        source_participants=((source_receipt, source_receipt, participant),),
        native_fences=tuple(fences),
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    result = coordinator.acknowledge_native(handoff)
    assert not isinstance(result, CustodyRefusal)
    assert result.cleanup == "pending"
    return result.capability


def _bound_pre_ack_capability(coordinator, handoff, source_receipt, participant, fences):
    plan = NativeCleanupPlan(
        process_instance=handoff.process_instance,
        authority_epoch=handoff.authority_epoch,
        close_generation=handoff.close_generation,
        source_participants=((source_receipt, source_receipt, participant),),
        native_fences=tuple(fences),
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    capability = coordinator.native_cleanup_capability(handoff)
    assert not isinstance(capability, CustodyRefusal)
    assert coordinator.state == "handoff_pending"
    return capability


def test_bootstrap_binds_native_plan_provider_after_real_source_graph(started_mux):
    runner, _homes, _loop, _call = started_mux
    provider = getattr(runner, "_preservation_native_cleanup_plan", None)
    assert callable(provider)
    assert getattr(provider, "func", None).__name__ == "build_native_cleanup_plan"
    assert runner.runtime_custody._cron_source_registry is not None
    assert runner.runtime_custody._i16_source_registry is not None


def test_profile_participant_closes_exact_real_lock_and_rejects_replacement(tmp_path):
    from gateway.runtime_ownership import ProfileOwnership, canonical_home
    from gateway.runtime_native_cleanup_sources import (
        ProfileNativeCleanupReceipt,
    )
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    home = (tmp_path / "profile").resolve()
    owner = ProfileOwnership()
    owner.reserve([home])
    coordinator, handoff = _coordinator()
    try:
        evidence = owner.reservation_evidence(home)
        assert evidence is not None
        handle = owner._owner_record_bindings[canonical_home(home)][0]
        source_receipt = ProfileNativeCleanupReceipt(
            coordinator=coordinator,
            owner=owner,
            home=canonical_home(home),
            handle=handle,
            evidence=evidence,
            process_instance=coordinator.process_instance,
            authority_epoch=coordinator.authority_epoch,
            close_generation=handoff.close_generation,
        )
        participant = ProfileNativeCleanupParticipant(source_receipt)
        capability = _real_capability(coordinator, handoff, source_receipt, participant, (handle,))
        with pytest.raises(RuntimeError, match="fence_(missing|payload_mismatch)"):
            participant.finish_native_cleanup(
                coordinator=coordinator,
                capability=capability,
                source_receipt=source_receipt,
                native_fences=(),
                deadline=5.0,
            )
        assert owner.owns(home)
        assert not handle.closed
        with pytest.raises(RuntimeError, match="capability_foreign"):
            participant.finish_native_cleanup(
                coordinator=coordinator,
                capability=replace(capability),
                source_receipt=source_receipt,
                native_fences=(handle,),
                deadline=5.0,
            )
        assert owner.owns(home)
        with pytest.raises(RuntimeError, match="deadline_expired"):
            participant.finish_native_cleanup(
                coordinator=coordinator,
                capability=capability,
                source_receipt=source_receipt,
                native_fences=(handle,),
                deadline=0.0,
            )
        assert owner.owns(home)
        completion = participant.finish_native_cleanup(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            native_fences=(handle,),
            deadline=5.0,
        )
        assert completion.native_fence is handle
        assert owner.homes == ()

        owner.reserve([home])
        with pytest.raises(RuntimeError, match="owner_replaced"):
            participant.finish_native_cleanup(
                coordinator=coordinator,
                capability=capability,
                source_receipt=source_receipt,
                native_fences=(handle,),
                deadline=5.0,
            )
        assert owner.owns(home)
    finally:
        owner.close()


def test_profile_participant_refuses_pre_ack_without_closing_real_lock(tmp_path):
    from gateway.runtime_ownership import ProfileOwnership, canonical_home
    from gateway.runtime_native_cleanup_sources import ProfileNativeCleanupReceipt
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    home = (tmp_path / "pre-ack-profile").resolve()
    owner = ProfileOwnership()
    owner.reserve([home])
    coordinator, handoff = _coordinator()
    try:
        evidence = owner.reservation_evidence(home)
        handle = owner._owner_record_bindings[canonical_home(home)][0]
        source_receipt = ProfileNativeCleanupReceipt(
            coordinator=coordinator,
            owner=owner,
            home=canonical_home(home),
            handle=handle,
            evidence=evidence,
            process_instance=handoff.process_instance,
            authority_epoch=handoff.authority_epoch,
            close_generation=handoff.close_generation,
        )
        participant = ProfileNativeCleanupParticipant(source_receipt)
        capability = _bound_pre_ack_capability(
            coordinator, handoff, source_receipt, participant, (handle,),
        )
        with pytest.raises(RuntimeError, match="coordinator_not_closed"):
            participant.finish_native_cleanup(
                coordinator=coordinator,
                capability=capability,
                source_receipt=source_receipt,
                native_fences=(handle,),
                deadline=5.0,
            )
        assert coordinator.state == "handoff_pending"
        assert owner.owns(home)
        assert not handle.closed
    finally:
        owner.close()


def _real_cron_fixture(tmp_path):
    from gateway.runtime_cron_source_registry import CronSourceRegistry
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    coordinator, handoff = _coordinator()
    runner = SimpleNamespace()
    registry = CronSourceRegistry(coordinator)
    home = (tmp_path / "cron-profile").resolve()
    registry._runner = runner
    registry.homes = (("default", home),)
    # The participant tests the real lock/fence transition.  The producer graph itself is already
    # covered by test_cron_source_registry; keep this isolated fixture focused on the coordinator
    # capability and native fence ordering.
    registry._validate_graph_locked = lambda: True
    registry._evidence_graph_ready_locked = lambda: True
    registry._profile_graph_digest_locked = lambda: "real-cron-test-graph"
    coordinator._cron_source_registry = registry
    assert registry.begin_close(handoff.close_generation)
    return runner, coordinator, registry, handoff


def test_cron_participant_closes_real_jobs_fence(tmp_path):
    runner, coordinator, registry, handoff = _real_cron_fixture(tmp_path)
    source_receipt, participant, fences = _cron_participant(runner, coordinator, handoff)
    assert isinstance(participant, CronNativeCleanupParticipant)
    capability = _real_capability(coordinator, handoff, source_receipt, participant, fences)
    with pytest.raises(RuntimeError, match="fence_(missing|payload_mismatch)"):
        participant.finish_native_cleanup(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            native_fences=(),
            deadline=5.0,
        )
    assert registry.state == "closing"
    assert all(not fence.fd.closed for fence in fences)
    completion = participant.finish_native_cleanup(
        coordinator=coordinator,
        capability=capability,
        source_receipt=source_receipt,
        native_fences=fences,
        deadline=5.0,
    )
    assert completion.native_fence in fences
    assert registry.state == "closed"
    assert all(fence.fd.closed for fence in fences)


def test_cron_participant_refuses_pre_ack_without_closing_real_fences(tmp_path):
    runner, coordinator, registry, handoff = _real_cron_fixture(tmp_path)
    source_receipt, participant, fences = _cron_participant(runner, coordinator, handoff)
    assert isinstance(participant, CronNativeCleanupParticipant)
    capability = _bound_pre_ack_capability(
        coordinator, handoff, source_receipt, participant, fences,
    )
    with pytest.raises(RuntimeError, match="coordinator_not_closed"):
        participant.finish_native_cleanup(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            native_fences=fences,
            deadline=5.0,
        )
    assert coordinator.state == "handoff_pending"
    assert registry.state == "closing"
    assert all(not fence.fd.closed for fence in fences)


@pytest.mark.asyncio
async def test_provider_participant_refuses_pre_ack_without_committing_exact_lease():
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    class _Custody:
        def __init__(self):
            self.calls = 0

        async def commit_native_entry(self, _generation, _entry):
            self.calls += 1
            return True

    coordinator, handoff = _coordinator()
    custody = _Custody()
    lease = object()
    entry = SimpleNamespace(status="prepared", lease=lease)
    source_receipt = ProviderNativeCleanupReceipt(
        coordinator=coordinator,
        custody=custody,
        entry=entry,
        lease=lease,
        process_instance=handoff.process_instance,
        authority_epoch=handoff.authority_epoch,
        close_generation=handoff.close_generation,
    )
    participant = ProviderNativeCleanupParticipant(source_receipt)
    capability = _bound_pre_ack_capability(
        coordinator, handoff, source_receipt, participant, (lease,),
    )
    with pytest.raises(RuntimeError, match="coordinator_not_closed"):
        await participant.finish_native_cleanup(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            native_fences=(lease,),
            deadline=5.0,
        )
    assert coordinator.state == "handoff_pending"
    assert custody.calls == 0
    assert source_receipt.lease is lease


def test_cron_failure_retains_same_fence_for_same_receipt_retry(tmp_path, monkeypatch):
    runner, coordinator, registry, handoff = _real_cron_fixture(tmp_path)
    source_receipt, participant, fences = _cron_participant(runner, coordinator, handoff)
    capability = _real_capability(coordinator, handoff, source_receipt, participant, fences)
    original = registry._release_fence
    calls = []

    def fail_once(fence):
        if not calls:
            calls.append(fence)
            return False
        return original(fence)

    monkeypatch.setattr(registry, "_release_fence", fail_once)
    with pytest.raises(RuntimeError, match="cleanup_pending"):
        participant.finish_native_cleanup(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            native_fences=fences,
            deadline=5.0,
        )
    assert registry.state == "handoff_pending"
    retained = tuple(registry._fences.values())
    assert retained
    assert all(any(item is captured for captured in fences) for item in retained)

    monkeypatch.setattr(registry, "_release_fence", original)
    completion = participant.finish_native_cleanup(
        coordinator=coordinator,
        capability=capability,
        source_receipt=source_receipt,
        native_fences=fences,
        deadline=5.0,
    )
    assert completion.source_receipt is source_receipt
    assert registry.state == "closed"


def test_plan_refuses_before_profile_release_when_i17_or_i18_source_is_missing(tmp_path):
    from gateway.runtime_ownership import ProfileOwnership

    owner = ProfileOwnership()
    home = (tmp_path / "profile").resolve()
    owner.reserve([home])
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    coordinator, handoff = _coordinator()
    runner = SimpleNamespace(runtime_custody=coordinator, session_runtime_descriptor={})
    try:
        assert build_native_cleanup_plan(runner, coordinator, handoff) is None
        assert owner.owns(home)
    finally:
        owner.close()


def test_closed_retry_reuses_coordinator_owned_plan_identity():
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    coordinator, handoff = _coordinator()
    zero = NativeCleanupZeroWorkReceipt(
        source="source-owned",
        process_instance=handoff.process_instance,
        authority_epoch=handoff.authority_epoch,
        close_generation=handoff.close_generation,
        owner_id="source-owner",
    )
    plan = NativeCleanupPlan(
        handoff.process_instance,
        handoff.authority_epoch,
        handoff.close_generation,
        zero_work=zero,
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    result = coordinator.acknowledge_native(handoff)
    assert result.cleanup == "complete"
    observed = build_native_cleanup_plan(
        SimpleNamespace(), coordinator, result.closed_receipt,
    )
    assert observed is plan


def test_handoff_retry_reuses_bound_plan_identity_before_ack():
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    coordinator, handoff = _coordinator()
    zero = NativeCleanupZeroWorkReceipt(
        source="source-owned",
        process_instance=handoff.process_instance,
        authority_epoch=handoff.authority_epoch,
        close_generation=handoff.close_generation,
        owner_id="source-owner",
    )
    plan = NativeCleanupPlan(
        handoff.process_instance,
        handoff.authority_epoch,
        handoff.close_generation,
        zero_work=zero,
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    observed = build_native_cleanup_plan(SimpleNamespace(), coordinator, handoff)
    assert observed is plan


def test_public_route_settles_real_profile_participant_with_closed_capability(tmp_path):
    from gateway.run_runtime_native_cleanup import acknowledge_native_cleanup
    from gateway.runtime_ownership import ProfileOwnership, canonical_home
    from gateway.runtime_native_cleanup_sources import ProfileNativeCleanupReceipt
    from tests.gateway.test_preservation_native_cleanup import _coordinator

    home = (tmp_path / "public-profile").resolve()
    owner = ProfileOwnership()
    owner.reserve([home])
    coordinator, handoff = _coordinator()
    try:
        evidence = owner.reservation_evidence(home)
        handle = owner._owner_record_bindings[canonical_home(home)][0]
        source_checks = [False]
        source_receipt = ProfileNativeCleanupReceipt(
            coordinator=coordinator,
            owner=owner,
            home=canonical_home(home),
            handle=handle,
            evidence=evidence,
            process_instance=handoff.process_instance,
            authority_epoch=handoff.authority_epoch,
            close_generation=handoff.close_generation,
            source_ready=lambda: source_checks[0],
        )
        participant = ProfileNativeCleanupParticipant(source_receipt)
        plan = NativeCleanupPlan(
            handoff.process_instance,
            handoff.authority_epoch,
            handoff.close_generation,
            source_participants=((source_receipt, source_receipt, participant),),
            native_fences=(handle,),
        )
        runner = SimpleNamespace(_preservation_native_cleanup_plan=lambda _c, _r: plan)
        result = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
        assert result.cleanup == "unknown"
        assert owner.owns(home)
        source_checks[0] = True
        result = asyncio.run(acknowledge_native_cleanup(runner, coordinator, result.closed_receipt))
        assert result.cleanup == "complete"
        assert owner.homes == ()
    finally:
        owner.close()
