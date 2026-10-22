"""Scoped ledger contracts for persistent Codex service parent edges.

The first coordinator tests exercise the transition API.  The admission/root reuse test
also consumes the actual Codex session/client and typed SessionDB proof path; native Windows,
provider, and installed acceptance remain separate proof boundaries.
"""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.runtime_service_custody import (
    ServiceCustodyError,
    admission_binding,
    reserve_service,
)
from hermes_cli.backend_retirement import RetirementFence


def _coordinator():
    return RuntimeCustodyCoordinator(
        process_instance="process-1", authority_epoch=7, fence=RetirementFence(),
    )


def _parent(coordinator, admission_id):
    return coordinator.reserve_root(
        "I04_fifo_turn_execution", f"claim:{admission_id}",
        profile_id="profile-1", authority_epoch=7,
    )


async def real_runtime_binding(tmp_path, monkeypatch, *, include_probe=True):
    """Build one actual claimed SessionAuthority admission for custody-boundary tests."""
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.preservation_source_close import source_handoff_context
    from gateway.runtime_ownership import process_ownership
    from gateway.session_ingress_context import native_callback
    from hermes_constants import (
        get_hermes_home,
        reset_hermes_home_override,
        set_hermes_home_override,
    )
    import hermes_state
    from hermes_state_runtime import get_session_admission
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

    home = (tmp_path / "profile").resolve()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    # tests/conftest.py pins DEFAULT_DB_PATH to its per-test home. Close that escape hatch so the
    # supported context-local home override can select this fixture's reserved profile DB.
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
    home_token = set_hermes_home_override(home)
    reserved = False
    runner = None
    authority = None
    handed_off = False

    def close_resources():
        try:
            if runner is not None:
                store = getattr(runner, "session_store", None)
                if store is not None:
                    store.close_all_db_handles()
                runner.close_all_session_db_handles()
        finally:
            if reserved:
                process_ownership.close()
            reset_hermes_home_override(home_token)

    try:
        process_ownership.reserve([home])
        reserved = True
        runner = GatewayRunner(GatewayConfig(sessions_dir=home / "sessions"))
        adapter = WhatsAppAdapter(
            PlatformConfig(enabled=True, extra={"session_path": str(home / "wa")})
        )
        adapter.gateway_runner = runner
        runner.adapters = {Platform.WHATSAPP: adapter}
        await initialize_gateway_runtime(runner)
        runner._wire_adapter_handlers(adapter)
        authority = runner.session_authority
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        )
        # Keep the source-owned scheduler fence for every caller; the unrelated probe is
        # optional because the I02 readiness helper must inspect its own current row.
        authority._schedule = lambda _ref: None

        async def admit_native_with_source_handoff(native_event):
            owner = runner._transport_owner(native_event.source)
            assert owner is not None and owner[0] is adapter
            source_adapter, source_profile = owner
            source_close = runner._preservation_source_close
            token = source_close.admit_source(
                source_adapter, native_event, profile=source_profile, event=native_event,
                phase="native-fixture",
            )
            completed = False
            try:
                with source_handoff_context((token,)):
                    with native_callback(
                        runner, native_event, get_hermes_home(), source_profile,
                        source_token=token,
                    ):
                        receipt = await authority.admit_native(native_event)
                        source_close.mark_producer_done(token)
                        completed = True
                        return receipt
            except BaseException:  # health: allow BLE001 -- cancellation retains the real source lease
                if not completed:
                    source_close.mark_unknown(token, "native_fixture_admission_failed")
                raise
        probe_binding = None
        if include_probe:
            # Keep one independently claimed producer edge for the legacy custody
            # oracles that inspect an active binding before they exercise a service.
            # The actual Codex controls below use the primary source and run their
            # admissions through the real drain, so this probe never settles a
            # positive result or stands in for the managed turn.
            probe_source = adapter.build_source(
                chat_id="agc1723-probe@g.us",
                user_id="15551234567@s.whatsapp.net",
                chat_type="group",
            )

        async def claim(request_id):
            # Use the real native producer: it reserves/binds the I01 parent around the durable
            # admission before the FIFO claim can reserve its exact I04 child.
            native_event = MessageEvent(text=request_id, source=source, message_id=request_id)
            receipt = await admit_native_with_source_handoff(native_event)
            assert receipt.status == "queued"
            ref = receipt.ref
            live = authority.sessions[ref.session_id]
            row, _first = await authority._claim_next(ref, live)
            assert isinstance(row, dict) and row["status"] == "started"
            row = get_session_admission(authority.db, admission_id=row["admission_id"])
            assert row is not None
            assert row["target_session_id"] == ref.session_id
            assert row["payload"]["native_text_v1"]["producer_process_mode"] == "managed_turn"
            assert isinstance(row["principal_id"], str) and row["principal_id"].startswith("messaging:")
            assert row["owner_epoch"] == authority.epoch
            assert row["generation"] == live.event_stream.execution["execution_generation"]
            assert live.event_stream.execution["admission_id"] == row["admission_id"]
            assert authority.executing[ref.session_id] == row["generation"]
            assert authority._preservation_children[row["admission_id"]].owner_class == (
                "I04_fifo_turn_execution"
            )
            binding = admission_binding(authority, ref, row)
            return binding

        if include_probe:
            probe_event = MessageEvent(
                text="fixture-probe", source=probe_source, message_id="fixture-probe",
            )
            probe_receipt = await admit_native_with_source_handoff(probe_event)
            assert probe_receipt.status == "queued"
            probe_ref = probe_receipt.ref
            probe_live = authority.sessions[probe_ref.session_id]
            probe_row, _probe_first = await authority._claim_next(probe_ref, probe_live)
            assert isinstance(probe_row, dict) and probe_row["status"] == "started"
            probe_row = get_session_admission(authority.db, admission_id=probe_row["admission_id"])
            assert probe_row is not None
            probe_binding = admission_binding(authority, probe_ref, probe_row)

        async def run_real_admission(
            session, writer, text, *, fail_delivery=False, use_native_waiter=True,
        ):
            """Run one admission through the actual handler, drain, settlement and finalizer."""
            from agent.codex_runtime import run_codex_app_server_turn
            from gateway.runtime_service_custody import current_turn_binding
            from gateway.session_results import execution_result

            native_event = MessageEvent(text=text, source=source, message_id=text)
            receipt = await admit_native_with_source_handoff(native_event)
            assert receipt.status == "queued"
            # The native waiter is optional: finalizer-only controls use it to avoid a
            # platform send, while the pre-effect retry control reaches deliver_settled.
            if use_native_waiter:
                authority.native_waiters.add(receipt.admission_id)
                authority.waiters[receipt.admission_id] = asyncio.get_running_loop().create_future()
            captured_binding = {}
            original_handler = runner._handle_message
            finalizer_state = {"calls": 0}
            restore_finalizer = None

            async def handle_message(event):
                nonlocal restore_finalizer
                binding = current_turn_binding()
                assert binding is not None
                captured_binding["binding"] = binding
                writer.session_id = binding.session_id
                writer._codex_session = session
                writer._codex_session_prompt = None
                writer._codex_session_model_provider = None
                messages = authority.db.get_messages_as_conversation(
                    binding.session_id, include_row_ids=True,
                )
                messages.append({"role": "user", "content": event.text})
                result = run_codex_app_server_turn(
                    writer, user_message=event.text, original_user_message=event.text,
                    messages=messages, effective_task_id=binding.session_id,
                )
                assert type(result.get("agent_persisted")) is bool
                captured = execution_result.get()
                assert captured is not None
                captured["result"] = result
                if fail_delivery:
                    service = session._custody.service
                    assert service is not None
                    restore_finalizer = service.complete_delivery

                    def fail_delivery_once(_binding, _request):
                        finalizer_state["calls"] += 1
                        if finalizer_state["calls"] == 1:
                            raise ServiceCustodyError("delivery_finalize_failed")
                        return restore_finalizer(_binding, _request)

                    service.complete_delivery = fail_delivery_once
                return result["final_response"]

            runner._handle_message = handle_message
            try:
                await authority._drain(receipt.ref)
            finally:
                runner._handle_message = original_handler
                if restore_finalizer is not None:
                    session._custody.service.complete_delivery = restore_finalizer
            binding = captured_binding["binding"]
            custody = session._custody
            assert custody is not None and custody.request is not None
            assert custody.service is not None and custody.service.physical is not None
            fixture.binding = binding
            fixture.ref = receipt.ref
            fixture.finalizer_calls = finalizer_state["calls"]
            return binding, custody.request, custody.service.physical

        fixture = SimpleNamespace(
            runner=runner, authority=authority, adapter=adapter, live=None, ref=None, claim=claim,
            run_real_admission=run_real_admission, cleanup=close_resources, binding=probe_binding,
            admit_native_with_source_handoff=admit_native_with_source_handoff,
            finalizer_calls=0,
        )
        handed_off = True
        return fixture
    finally:
        if not handed_off:
            close_resources()


def test_request_terminalization_removes_both_parent_edges_atomically():
    coordinator = _coordinator()
    i04_parent = _parent(coordinator, "admission-1")
    service = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1",
        profile_id="profile-1", authority_epoch=7,
    )
    request = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1:request:admission-1",
        profile_id="profile-1", authority_epoch=7, parent=service,
    )

    assert coordinator.bind_secondary_parent(request, i04_parent) is None
    assert coordinator.mark_terminal(i04_parent).code == "custody_pending"
    assert coordinator.mark_terminal(request).code == "secondary_parent_required"
    assert coordinator.mark_terminal_with_secondary_parents(
        request, (i04_parent,)
    ) is None
    assert coordinator.mark_terminal(i04_parent) is None
    assert coordinator.mark_terminal(service) is None


def test_secondary_binding_rejects_copied_foreign_and_stale_parent_handles():
    coordinator = _coordinator()
    i04_parent = _parent(coordinator, "admission-1")
    service = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1",
        profile_id="profile-1", authority_epoch=7,
    )
    request = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1:request:admission-1",
        profile_id="profile-1", authority_epoch=7, parent=service,
    )
    copied = replace(i04_parent, owner_id="claim:copied")
    foreign = _parent(_coordinator(), "foreign")

    assert (
        coordinator.bind_secondary_parent(request, copied).code
        == "secondary_parent_mismatch"
    )
    assert (
        coordinator.bind_secondary_parent(request, foreign).code
        == "secondary_parent_mismatch"
    )
    assert coordinator.mark_terminal(i04_parent) is None
    assert (
        coordinator.bind_secondary_parent(request, i04_parent).code
        == "secondary_parent_mismatch"
    )


def test_unknown_secondary_child_keeps_parent_and_service_root_live():
    coordinator = _coordinator()
    i04_parent = _parent(coordinator, "admission-1")
    service = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1",
        profile_id="profile-1", authority_epoch=7,
    )
    request = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1:request:admission-1",
        profile_id="profile-1", authority_epoch=7, parent=service,
    )

    assert coordinator.bind_secondary_parent(request, i04_parent) is None
    assert coordinator.mark_unknown(request, "join_failed").code == "join_failed"
    assert coordinator.mark_terminal(i04_parent).code == "custody_pending"
    assert coordinator.mark_terminal_with_secondary_parents(
        request, (i04_parent,)
    ).code == "custody_unknown"
    status = coordinator.status()
    assert i04_parent.lease.lease_id in status["active_secondary_child_parents"]
    assert service.lease.lease_id in status["active_child_parents"]


def test_atomic_request_refuses_stale_secondary_before_accepting_child():
    coordinator = _coordinator()
    i04_parent = _parent(coordinator, "admission-1")
    service = coordinator.reserve_root(
        "I16_turn_sidecars", "service:session-1",
        profile_id="profile-1", authority_epoch=7,
    )
    copied = replace(i04_parent, owner_id="claim:copied")

    refusal = coordinator.reserve_root_with_secondary_parent(
        "I16_turn_sidecars", "service:session-1:request:admission-1",
        profile_id="profile-1", authority_epoch=7,
        parent=service, secondary_parent=copied,
    )

    assert refusal.code == "secondary_parent_mismatch"
    status = coordinator.status()
    assert status["active_owner_classes"].count("I16_turn_sidecars") == 1
    assert status["active_child_parents"] == []
    assert status["active_secondary_child_parents"] == []


def _physical_close_diagnostic(physical):
    """Read retained failure evidence without advancing any reconciliation state."""
    transaction = physical.transaction
    job = getattr(transaction, "job", None)
    return {
        "physical_state": physical.state,
        "unknown_reason": physical.unknown_reason,
        "process_returncode": getattr(physical.process, "returncode", None),
        "eof_streams": sorted(physical.eof_streams),
        "joined_streams": sorted(physical.joined_streams),
        "completed_requests": len(physical.completed_requests),
        "delivery_requests": len(physical.delivery_requests),
        "durable_proofs": len(physical.durable_proofs),
        "transaction_state": getattr(transaction, "state", None),
        "scope_observation": getattr(transaction, "scope_observation", None),
        "native_empty_observed": getattr(transaction, "native_empty_observed", None),
        "process_handle_closed": getattr(transaction, "process_handle_closed", None),
        "job_handle_closed": getattr(transaction, "job_handle_closed", None),
        "job_active_processes": job.active_processes() if job is not None else None,
    }


@pytest.mark.asyncio
async def test_service_lease_reuses_one_root_across_two_exact_admissions(tmp_path, monkeypatch):
    import json
    from agent.transports.codex_app_server import CodexAppServerTransportError
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.agent.transports.test_codex_physical_custody import (
        _RealProofAgent,
        _complete_real_admission,
        _portable_codex_launcher,
    )

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request1, physical = await _complete_real_admission(
            session, fixture, writer, "two-admission-a",
        )
        service = session._custody.service
        root = service.handle

        request2, physical2 = await _complete_real_admission(
            session, fixture, writer, "two-admission-b",
        )
        assert service.handle is root
        assert request1 is not request2
        assert request2.lease.parent_lease_id == root.lease.lease_id
        assert physical2 is physical
        assert writer.flush_count == 2
        assert service.requests == {}
        try:
            session.close()
        except CodexAppServerTransportError:
            print("native-physical-close-diagnostic: " + json.dumps(
                _physical_close_diagnostic(physical), sort_keys=True,
            ))
            raise
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_stale_live_generation_refuses_before_service_reservation(tmp_path, monkeypatch):
    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim("stale-generation")
        fixture.binding = binding
        coordinator = binding.coordinator
        native_parent = fixture.authority._preservation_handles[binding.admission_id]
        fifo_child = fixture.authority._preservation_children[binding.admission_id]
        baseline = coordinator.status()
        assert native_parent.owner_class == "I01_native_inbound"
        assert fifo_child is binding.parent_handle
        assert fifo_child.owner_class == "I04_fifo_turn_execution"
        assert coordinator._parent(fifo_child) is fifo_child.lease
        assert fifo_child.lease.parent_lease_id == native_parent.lease.lease_id
        assert set(baseline["active_owner_classes"]) == {
            "I01_native_inbound", "I04_fifo_turn_execution",
        }
        assert "I16_turn_sidecars" not in baseline["active_owner_classes"]
        stale = replace(
            binding,
            execution_generation=binding.execution_generation + 1,
        )
        with pytest.raises(ServiceCustodyError, match="preservation_owner_mismatch"):
            reserve_service(stale, "service:session-1")
        assert coordinator.status() == baseline
        assert "I16_turn_sidecars" not in coordinator.status()["active_owner_classes"]
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_foreign_profile_refuses_before_service_reservation(tmp_path, monkeypatch):
    from gateway.session_contract import SessionRef
    from hermes_state_runtime import get_session_admission

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim("foreign-profile")
        fixture.binding = binding
        row = get_session_admission(
            fixture.authority.db, admission_id=binding.admission_id,
        )
        assert row is not None
        coordinator = binding.coordinator
        native_parent = fixture.authority._preservation_handles[binding.admission_id]
        fifo_child = fixture.authority._preservation_children[binding.admission_id]
        baseline = coordinator.status()
        assert native_parent.owner_class == "I01_native_inbound"
        assert fifo_child is binding.parent_handle
        assert fifo_child.owner_class == "I04_fifo_turn_execution"
        assert coordinator._parent(fifo_child) is fifo_child.lease
        assert fifo_child.lease.parent_lease_id == native_parent.lease.lease_id
        assert set(baseline["active_owner_classes"]) == {
            "I01_native_inbound", "I04_fifo_turn_execution",
        }
        assert "I16_turn_sidecars" not in baseline["active_owner_classes"]
        foreign_ref = SessionRef(
            binding.profile_id + "-foreign", binding.session_id,
        )
        with pytest.raises(ServiceCustodyError, match="preservation_owner_mismatch"):
            admission_binding(fixture.authority, foreign_ref, row)
        assert coordinator.status() == baseline
        assert "I16_turn_sidecars" not in coordinator.status()["active_owner_classes"]
    finally:
        fixture.cleanup()
