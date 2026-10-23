"""Causal tests for the committed native preservation cleanup capability."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
import threading
import time

from gateway.run_runtime_native_cleanup import acknowledge_native_cleanup
from gateway.runtime_custody import (
    CustodyRefusal,
    NativeCleanupCompletionReceipt,
    NativeCleanupPlan,
    NativeCleanupZeroWorkReceipt,
    RuntimeCustodyCoordinator,
    OWNER_CLASSES,
)
from hermes_cli.backend_retirement import RetirementFence


def _coordinator(deadline_s=30.0):
    coordinator = RuntimeCustodyCoordinator(
        process_instance="native-cleanup-test-instance",
        authority_epoch=11,
        fence=RetirementFence(),
    )
    close = coordinator.begin_close(owner_id="cleanup-owner", deadline_s=deadline_s)
    assert not isinstance(close, CustodyRefusal)
    for spec in coordinator.owner_manifest:
        assert coordinator.seal_owner(
            spec.owner_class,
            {
                "source": spec.source,
                "process_instance": close.process_instance,
                "authority_epoch": close.authority_epoch,
                "close_generation": close.close_generation,
                "owner_id": f"owner-{spec.owner_class}",
                "observed": True,
                "cross_process_proof": True,
                "os_lock_owner": True,
            },
        ) is None
    handoff = coordinator.prepare_handoff(close)
    assert not isinstance(handoff, CustodyRefusal)
    return coordinator, handoff


class _Participant:
    def __init__(self, coordinator, source_receipt, fence, *, failures=0, reentrant=False, delay=0):
        self.coordinator = coordinator
        self.source_receipt = source_receipt
        self.fence = fence
        self.failures = failures
        self.reentrant = reentrant
        self.delay = delay
        self.calls = 0
        self.lock_probe = None

    def finish_native_cleanup(self, *, coordinator, capability, source_receipt, native_fences, deadline):
        self.calls += 1
        if self.delay:
            time.sleep(self.delay)  # health: allow HX013 -- delay is the causal late-deadline boundary under test
        assert coordinator is self.coordinator
        assert source_receipt is self.source_receipt
        assert self.fence in native_fences
        if self.reentrant:
            result = []
            thread = threading.Thread(target=lambda: result.append(coordinator.status()))
            thread.start()
            thread.join(timeout=0.5)
            self.lock_probe = bool(result)
            assert self.lock_probe is True
        if self.calls <= self.failures:
            raise RuntimeError("source cleanup did not settle")
        return NativeCleanupCompletionReceipt(
            coordinator=coordinator,
            capability=capability,
            participant=self,
            source_receipt=self.source_receipt,
            native_fence=self.fence,
            process_instance=coordinator.process_instance,
            authority_epoch=coordinator.authority_epoch,
            close_generation=capability.close_generation,
        )


def _plan(coordinator, handoff, participants=()):
    fences = tuple(item.fence for item in participants)
    return NativeCleanupPlan(
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=handoff.close_generation,
        source_participants=tuple(
            (object(), item.source_receipt, item) for item in participants
        ),
        native_fences=fences,
    )


def test_acknowledgement_commits_closed_but_source_cleanup_is_pending_then_completes():
    coordinator, handoff = _coordinator()
    source_receipt = object()
    participant = _Participant(coordinator, source_receipt, object(), reentrant=True)
    plan = _plan(coordinator, handoff, (participant,))
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    ack = coordinator.acknowledge_native(handoff)
    assert ack.cleanup == "pending"
    assert coordinator.state == "closed"
    started, entries = coordinator.begin_native_cleanup(ack.capability)
    assert started.cleanup == "pending" and entries == plan.source_participants
    completion = participant.finish_native_cleanup(
        coordinator=coordinator,
        capability=ack.capability,
        source_receipt=source_receipt,
        native_fences=plan.native_fences,
        deadline=5.0,
    )
    done = coordinator.finish_native_cleanup(ack.capability, (completion,))
    assert done.cleanup == "complete"
    assert participant.lock_probe is True
    assert coordinator.status()["native_cleanup"] == "complete"


def test_zero_work_requires_source_owned_receipt_and_completes_without_release():
    coordinator, handoff = _coordinator()
    zero = NativeCleanupZeroWorkReceipt(
        source="source-owned-registry",
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=handoff.close_generation,
        owner_id="source-owner",
    )
    plan = NativeCleanupPlan(
        coordinator.process_instance,
        coordinator.authority_epoch,
        handoff.close_generation,
        zero_work=zero,
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    done = coordinator.acknowledge_native(handoff)
    assert done.cleanup == "complete"
    assert coordinator.state == "closed"
    assert coordinator._fence._committed is True


def test_failed_cleanup_retains_same_participant_for_same_receipt_retry():
    coordinator, handoff = _coordinator()
    source_receipt = object()
    participant = _Participant(coordinator, source_receipt, object(), failures=1)
    plan = _plan(coordinator, handoff, (participant,))
    runner = SimpleNamespace(_preservation_native_cleanup_plan=lambda _c, _r: plan)
    first = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
    assert first.cleanup == "unknown"
    assert coordinator.status()["native_cleanup_pending_participants"] == 1
    retry = asyncio.run(acknowledge_native_cleanup(runner, coordinator, first.closed_receipt))
    assert retry.cleanup == "complete"
    assert participant.calls == 2


def test_partial_cleanup_keeps_unresolved_source_and_retries_only_it():
    coordinator, handoff = _coordinator()
    first_receipt, second_receipt = object(), object()
    first = _Participant(coordinator, first_receipt, object())
    second = _Participant(coordinator, second_receipt, object(), failures=1)
    plan = _plan(coordinator, handoff, (first, second))
    runner = SimpleNamespace(_preservation_native_cleanup_plan=lambda _c, _r: plan)
    result = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
    assert result.cleanup == "unknown"
    assert first.calls == 1 and second.calls == 1
    assert coordinator.status()["native_cleanup_pending_participants"] == 1
    result = asyncio.run(acknowledge_native_cleanup(runner, coordinator, result.closed_receipt))
    assert result.cleanup == "complete"
    assert first.calls == 1 and second.calls == 2


def test_replaced_plan_and_stale_receipts_refuse_without_adopting_new_source():
    coordinator, handoff = _coordinator()
    source_receipt = object()
    participant = _Participant(coordinator, source_receipt, object(), failures=1)
    plan = _plan(coordinator, handoff, (participant,))
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    ack = coordinator.acknowledge_native(handoff)
    assert ack.cleanup == "pending"
    replacement = _plan(coordinator, handoff, (participant,))
    assert coordinator.bind_native_cleanup_plan(ack.closed_receipt, replacement)
    assert isinstance(
        coordinator.acknowledge_native(replace(ack.closed_receipt, receipt_id="stale")),
        CustodyRefusal,
    )
    assert coordinator.status()["native_cleanup_pending_participants"] == 1


def test_public_retry_marks_replaced_source_unknown_and_keeps_original_participant():
    coordinator, handoff = _coordinator()
    source_receipt = object()
    participant = _Participant(coordinator, source_receipt, object(), failures=3)
    plan = _plan(coordinator, handoff, (participant,))
    replacement = _plan(coordinator, handoff, (participant,))
    calls = [0]

    def provider(_coordinator, _receipt):
        calls[0] += 1
        return plan if calls[0] == 1 else replacement

    runner = SimpleNamespace(_preservation_native_cleanup_plan=provider)
    first = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
    assert first.cleanup == "unknown"
    second = asyncio.run(acknowledge_native_cleanup(runner, coordinator, first.closed_receipt))
    assert second.cleanup == "unknown"
    assert second.reason == "native_cleanup_plan_replaced"
    assert coordinator.status()["native_cleanup_pending_participants"] == 1


def test_shared_close_deadline_keeps_late_source_completion_unknown():
    coordinator, handoff = _coordinator(deadline_s=0.2)
    source_receipt = object()
    participant = _Participant(coordinator, source_receipt, object(), delay=0.3)
    plan = _plan(coordinator, handoff, (participant,))
    runner = SimpleNamespace(_preservation_native_cleanup_plan=lambda _c, _r: plan)
    result = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
    assert result.cleanup == "unknown"
    assert result.reason == "native_cleanup_deadline"
    assert coordinator.status()["native_cleanup_pending_participants"] == 1


def test_public_path_requires_plan_and_does_not_invent_empty_work():
    coordinator, handoff = _coordinator()
    runner = SimpleNamespace(_preservation_native_cleanup_plan=lambda _c, _r: None)
    refused = asyncio.run(acknowledge_native_cleanup(runner, coordinator, handoff))
    assert isinstance(refused, CustodyRefusal)
    assert refused.code == "native_cleanup_missing"
    assert coordinator.state == "handoff_pending"


def test_acknowledged_close_cannot_be_reopened_or_late_root_admitted():
    coordinator, handoff = _coordinator()
    zero = NativeCleanupZeroWorkReceipt(
        "source", coordinator.process_instance, coordinator.authority_epoch,
        handoff.close_generation, "owner",
    )
    plan = NativeCleanupPlan(
        coordinator.process_instance, coordinator.authority_epoch, handoff.close_generation,
        zero_work=zero,
    )
    assert coordinator.bind_native_cleanup_plan(handoff, plan) is None
    done = coordinator.acknowledge_native(handoff)
    assert done.cleanup == "complete"
    late = coordinator.reserve_root("I01_native_inbound", "late", profile_id="p", authority_epoch=11)
    assert isinstance(late, CustodyRefusal) and late.code == "runtime_draining"
    assert isinstance(coordinator.abort_before_handoff(handoff), CustodyRefusal)


def test_public_gateway_route_binds_api_factory_before_native_ack(started_mux, monkeypatch):
    """Exercise the real READY/startup route before the native ACK boundary."""
    from gateway import run_runtime
    from gateway.run_runtime import recover_gateway_native_sessions

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    adapters = run_runtime._api_server_adapters(runner)
    assert adapters
    assert all(getattr(adapter, "_api_run_factory", None) is not None for adapter in adapters)
    descriptor = runner.session_runtime_descriptor
    params = {
        "instance_id": descriptor["instance_id"],
        "authority_epoch": descriptor["authority_epoch"],
        "owner_id": "native-public-api-factory",
        "scope": "process",
        "deadline_s": 30,
    }
    wire = call(run_runtime.begin_preservation_drain(runner, params, "fixture-peer"))
    assert wire["status"] == "closing", wire
    # Bootstrap now installs the real hosted room. Keep this fixture's missing-native-receipt
    # boundary explicit instead of expecting a successfully stopped I17 source to be UNKNOWN.
    authority = runner.session_authority
    service = authority.hosted_room_service
    retained_root = authority._i17_service_handle
    stop_attempts = []

    def refuse_hosted_native_stop(*, timeout):
        stop_attempts.append(timeout)
        return False

    with monkeypatch.context() as missing_native:
        missing_native.setattr(service, "stop", refuse_hosted_native_stop)
        prepared = call(run_runtime.prepare_preservation_handoff(
            runner, {"receipt": wire}, "fixture-peer",
        ))
    assert stop_attempts
    assert authority.hosted_room_service is service
    assert authority._i17_service_handle is retained_root
    if prepared.get("status") != "handoff_pending":
        # The public fixture intentionally boots the API factory but not every independent
        # cron/process/host source.  Keep this as a causal boundary check: I03 is sealed from
        # the real factory, while the missing source receipts refuse handoff and remain abortable.
        states = runner.runtime_custody.status().get("owner_states", {})
        assert states["I03_api_run_direct"] == "sealed"
        assert prepared["status"] == "refused"
        assert prepared["reason"] == "unknown_owner"
        assert call(run_runtime.abort_preservation_drain(
            runner, {"receipt": wire}, "fixture-peer",
        ))["status"] == "open"
        assert states["I12_cron_fire"] == "unwired"
        assert states["I15_process_registry_terminal"] == "unknown"
        assert states["I17_hosted_tui_services"] == "unknown"
        return
    zero = NativeCleanupZeroWorkReceipt(
        source="public-api-factory",
        process_instance=wire["process_instance"],
        authority_epoch=wire["authority_epoch"],
        close_generation=wire["close_generation"],
        owner_id="native-public-api-factory",
    )
    plan = NativeCleanupPlan(
        wire["process_instance"], wire["authority_epoch"], wire["close_generation"],
        zero_work=zero,
    )
    monkeypatch.setattr(runner, "_preservation_native_cleanup_plan", lambda _c, _r: plan)
    closed = call(run_runtime.acknowledge_preservation_native(
        runner, {"receipt": prepared}, "fixture-peer",
    ))
    assert closed["status"] == "closed", closed
    assert closed["cleanup"] == "complete", closed

