"""Real admission controls for the tri-state classic callback requirement."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from gateway.session_classic_delivery import (
    CLASSIC_CALLBACK_NOT_REQUIRED,
    CLASSIC_CALLBACK_UNKNOWN,
    classic_callback_requirement,
)
from gateway.session_api_turn import admit_api_turn
from hermes_state_runtime import admit_session_input, claim_session_input, settle_session_input
from tests.gateway.test_kanban_finite_admission import kanban_owner as kanban_owner  # noqa: PLC0414 -- pytest discovers this genuine module fixture through the explicit self-alias


def _terminal_row(owner, *, session_id, request_id, payload):
    owner.db.create_session(session_id, "api_server")
    queued = admit_session_input(
        owner.db,
        epoch=owner.epoch,
        principal_id="api",
        session_id=session_id,
        request_id=request_id,
        payload=payload,
    )
    started = claim_session_input(
        owner.db, epoch=owner.epoch, session_id=session_id, head=queued["admission_id"],
    )
    assert isinstance(started, dict)
    settle_session_input(
        owner.db,
        epoch=owner.epoch,
        admission_id=queued["admission_id"],
        generation=started["generation"],
        outcome="completed",
    )
    return queued


def test_managed_api_producer_is_not_required_to_supply_classic_callback(api, owner):
    _adapter, ref, queued = admit_api_turn(
        api,
        session_id="api-managed",
        request_id="api-managed-request",
        user_message="managed API producer",
        conversation_history=[],
    )
    started = claim_session_input(
        owner.db, epoch=owner.epoch, session_id=ref.session_id, head=queued["admission_id"],
    )
    assert isinstance(started, dict)
    settle_session_input(
        owner.db,
        epoch=owner.epoch,
        admission_id=queued["admission_id"],
        generation=started["generation"],
        outcome="completed",
    )

    assert classic_callback_requirement(
        owner, queued["admission_id"]
    ) == CLASSIC_CALLBACK_NOT_REQUIRED


def test_real_api_producer_before_terminal_readback_stays_unknown(api, owner):
    _adapter, _ref, queued = admit_api_turn(
        api,
        session_id="api-pending",
        request_id="api-pending-request",
        user_message="pending API producer",
        conversation_history=[],
    )

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


def test_missing_classic_bridge_stays_unknown_for_native_classic_row(owner):
    queued = _terminal_row(
        owner,
        session_id="classic-unmapped",
        request_id="classic-unmapped-request",
        payload={"native_text_v1": {"producer_process_mode": "classic_busy"}},
    )

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


@pytest.mark.asyncio
async def test_real_preclaim_cancelled_automation_has_typed_no_execution_witness(kanban_owner, monkeypatch):
    from gateway.session_automation import callback_record_for_automation
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from hermes_state_runtime import get_session_admission

    owner = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(owner.connection, owner.params)
        authority = owner.authority
        admission_id = receipt["receipt"]["admission_id"]
        queued = get_session_admission(authority.db, admission_id=admission_id)
        assert queued["status"] == "queued" and queued["generation"] is None
        handle = authority._preservation_handles[admission_id]
        real_release = authority._preservation_release_terminal
        observed = {}

        def observe_release(current_id):
            row = get_session_admission(authority.db, admission_id=current_id)
            observed["requirement"] = classic_callback_requirement(authority, current_id)
            observed["record"] = callback_record_for_automation(
                authority, row, current_id, handle)
            return real_release(current_id)

        monkeypatch.setattr(authority, "_preservation_release_terminal", observe_release)
        cancelled = await authority.cancel_queued(
            owner.connection.actor,
            SessionRef(authority.profile_id, queued["target_session_id"]),
            admission_id,
        )
        assert cancelled.status == "terminal" and cancelled.outcome == "cancelled"
        assert observed["requirement"] == CLASSIC_CALLBACK_NOT_REQUIRED
        assert observed["record"]["handle"] is handle
        assert observed["record"]["preclaim_cancelled"] is True
        assert get_session_admission(authority.db, admission_id=admission_id)["generation"] is None
        # Once the exact root is consumed, the empty map is deliberately unknown again.
        assert classic_callback_requirement(authority, admission_id) == CLASSIC_CALLBACK_UNKNOWN
    finally:
        await owner.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", ["binding", "descriptor"])
async def test_preclaim_cancelled_automation_missing_or_foreign_witness_stays_unknown(
        kanban_owner, monkeypatch, broken):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from hermes_state_runtime import get_session_admission

    owner = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(owner.connection, owner.params)
        authority = owner.authority
        admission_id = receipt["receipt"]["admission_id"]
        queued = get_session_admission(authority.db, admission_id=admission_id)
        binding = authority._preservation_automation_bindings[admission_id]
        if broken == "binding":
            authority._preservation_automation_bindings.pop(admission_id)
        else:
            binding["descriptor"] = dict(binding["descriptor"], source_identity="foreign-source")
        real_release = authority._preservation_release_terminal
        observed = {}

        def observe_release(current_id):
            observed["requirement"] = classic_callback_requirement(authority, current_id)
            return real_release(current_id)

        monkeypatch.setattr(authority, "_preservation_release_terminal", observe_release)
        cancelled = await authority.cancel_queued(
            owner.connection.actor,
            SessionRef(authority.profile_id, queued["target_session_id"]),
            admission_id,
        )
        assert cancelled.status == "terminal" and cancelled.outcome == "cancelled"
        assert observed["requirement"] == CLASSIC_CALLBACK_UNKNOWN
    finally:
        await owner.close()


def test_terminal_admission_without_source_owned_owner_stays_unknown(owner):
    queued = _terminal_row(
        owner,
        session_id="ownerless-terminal",
        request_id="ownerless-terminal-request",
        payload={"native_text_v1": {"producer_process_mode": "managed_turn"}},
    )

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


@pytest.mark.asyncio
async def test_real_heartbeat_producer_has_typed_callback_free_terminal_witness(tmp_path, monkeypatch):
    from hermes_state_runtime import claim_session_input, list_session_admissions, settle_session_input
    from tests.gateway.test_heartbeat_execution_ownership import _real_goal_owner
    from gateway.session_ingress import admit_preserved_automation

    runner, adapter, authority, _key, entry, event, handoff = await _real_goal_owner(
        tmp_path, monkeypatch, seed_native=True,
    )
    try:
        await admit_preserved_automation(authority, adapter, event, handoff)
        [queued] = [
            row for row in list_session_admissions(
                authority.db, session_id=authority.logical_owner(entry.session_id), pending_only=False,
            ) if row["request_id"] == handoff.request_id
        ]
        for prior in list_session_admissions(
            authority.db, session_id=authority.logical_owner(entry.session_id), pending_only=True,
        ):
            if prior["admission_id"] == queued["admission_id"]:
                break
            started = claim_session_input(
                authority.db, epoch=authority.epoch,
                session_id=prior["target_session_id"], head=prior["admission_id"],
            )
            assert isinstance(started, dict)
            settle_session_input(
                authority.db, epoch=authority.epoch, admission_id=prior["admission_id"],
                generation=started["generation"], outcome="completed",
            )
        started = claim_session_input(
            authority.db, epoch=authority.epoch, session_id=queued["target_session_id"],
            head=queued["admission_id"],
        )
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=queued["admission_id"],
            generation=started["generation"], outcome="completed",
        )
        assert classic_callback_requirement(authority, queued["admission_id"]) == CLASSIC_CALLBACK_NOT_REQUIRED
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_real_plugin_producer_requires_its_typed_terminal_row(tmp_path, monkeypatch):
    from hermes_state_runtime import claim_session_input, get_session_admission, list_session_admissions, settle_session_input
    from tests.gateway.test_plugin_origin_injection_authority import _plugin_custody_route

    runner, authority, adapter, _source, key, session_id = await _plugin_custody_route(
        monkeypatch, human=True,
    )
    try:
        assert await runner._admit_plugin_injection(
            adapter, authority.sessions[session_id].source, session_key=key,
            session_id=session_id, content="callback requirement producer", plugin_id="desk",
        )
        rows = [
            row for row in list_session_admissions(authority.db, session_id=session_id, pending_only=False)
            if row["payload"].get("text") == "callback requirement producer"
        ]
        assert len(rows) == 1
        queued = rows[0]
        for prior in list_session_admissions(authority.db, session_id=session_id, pending_only=True):
            if prior["admission_id"] == queued["admission_id"]:
                break
            started = claim_session_input(
                authority.db, epoch=authority.epoch, session_id=session_id,
                head=prior["admission_id"],
            )
            assert isinstance(started, dict)
            settle_session_input(
                authority.db, epoch=authority.epoch, admission_id=prior["admission_id"],
                generation=started["generation"], outcome="completed",
            )
        started = claim_session_input(
            authority.db, epoch=authority.epoch, session_id=session_id, head=queued["admission_id"],
        )
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=queued["admission_id"],
            generation=started["generation"], outcome="completed",
        )
        assert get_session_admission(authority.db, admission_id=queued["admission_id"])["status"] == "terminal"
        assert classic_callback_requirement(authority, queued["admission_id"]) == CLASSIC_CALLBACK_NOT_REQUIRED
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_real_kanban_producer_has_exact_automation_descriptor(kanban_owner):
    from hermes_state_runtime import claim_session_input, get_session_admission, settle_session_input
    from gateway.session_kanban import run_task

    fixture = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(fixture.connection, fixture.params)
        queued = get_session_admission(
            fixture.authority.db, admission_id=receipt["receipt"]["admission_id"],
        )
        started = claim_session_input(
            fixture.authority.db, epoch=fixture.authority.epoch,
            session_id=queued["target_session_id"], head=queued["admission_id"],
        )
        settle_session_input(
            fixture.authority.db, epoch=fixture.authority.epoch,
            admission_id=queued["admission_id"], generation=started["generation"],
            outcome="completed",
        )
        assert classic_callback_requirement(
            fixture.authority, queued["admission_id"],
        ) == CLASSIC_CALLBACK_NOT_REQUIRED
    finally:
        await fixture.close()


def _settled_real_api_candidate(api, owner, suffix):
    _adapter, ref, queued = admit_api_turn(
        api,
        session_id=f"api-marker-{suffix}",
        request_id=f"api-marker-request-{suffix}",
        user_message="marker signal candidate",
        conversation_history=[],
    )
    started = claim_session_input(
        owner.db, epoch=owner.epoch, session_id=ref.session_id, head=queued["admission_id"],
    )
    assert isinstance(started, dict)
    settle_session_input(
        owner.db,
        epoch=owner.epoch,
        admission_id=queued["admission_id"],
        generation=started["generation"],
        outcome="completed",
    )
    return queued


def test_held_turn_marker_keeps_valid_api_candidate_unknown(api, owner):
    queued = _settled_real_api_candidate(api, owner, "held")
    owner.held_turn_markers = {queued["admission_id"]: object()}

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


def test_marker_watcher_keeps_valid_api_candidate_unknown(api, owner):
    queued = _settled_real_api_candidate(api, owner, "watcher")
    owner._preservation_marker_watchers = {queued["admission_id"]: object()}

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


def test_authenticated_marker_adopter_keeps_valid_api_candidate_unknown(api, owner):
    queued = _settled_real_api_candidate(api, owner, "adopter")
    owner.marker_adopters = {
        queued["admission_id"]: SimpleNamespace(_classic_custody_authority=owner),
    }

    assert classic_callback_requirement(owner, queued["admission_id"]) == CLASSIC_CALLBACK_UNKNOWN


@pytest.mark.asyncio
async def test_terminal_automation_witness_does_not_reopen_admission(kanban_owner):
    from gateway.session_automation import callback_record_for_automation
    from gateway.session_kanban import run_task
    from hermes_state_runtime import get_session_admission

    fixture = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(fixture.connection, fixture.params)
        authority = fixture.authority
        admission_id = receipt["receipt"]["admission_id"]
        queued = get_session_admission(authority.db, admission_id=admission_id)
        handle = authority._preservation_handles[admission_id]
        started = claim_session_input(
            authority.db, epoch=authority.epoch,
            session_id=queued["target_session_id"], head=admission_id,
        )
        assert authority._preservation_automation_row_matches(started, handle)
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=admission_id,
            generation=started["generation"], outcome="completed",
        )
        terminal = get_session_admission(authority.db, admission_id=admission_id)
        # The admission/recovery contract remains closed to settled work. Only the
        # callback's read-only witness can recognize the authenticated terminal row.
        assert not authority._preservation_automation_row_matches(terminal, handle)
        assert authority._preservation_automation_row_matches(
            terminal, handle, allow_terminal=True,
        )
        assert callback_record_for_automation(
            authority, terminal, admission_id, handle,
        )["handle"] is handle
        for field, value in (
            ("owner_epoch", authority.epoch + 1), ("generation", 0),
            ("request_id", "foreign-request"), ("principal_id", "foreign-principal"),
            ("target_session_id", "foreign-session"), ("outcome", "unsettled"),
        ):
            foreign = {**terminal, field: value}
            assert not authority._preservation_automation_row_matches(
                foreign, handle, allow_terminal=True,
            )
            assert callback_record_for_automation(
                authority, foreign, admission_id, handle,
            ) is None
    finally:
        await fixture.close()


def _bridge_handoff_case():
    from gateway.session_classic_custody import (
        ClassicExecutionEventBridge, ClassicExecutionReceipt,
    )

    identity = {
        "owner_class": "I06_busy_followup_delivery",
        "profile_id": "profile-r9",
        "profile_home": "/tmp/profile-r9",
        "process_instance": "process-r9",
        "authority_epoch": 4,
        "session_id": "session-r9",
        "principal_id": "messaging:r9",
        "request_id": "request-r9",
        "source_identity": "source-r9",
        "producer": "fifo",
        "occurrence_id": "occurrence-r9",
        "session_key": "discord|r9|dm",
    }
    authority = SimpleNamespace(_preservation_operations={})
    owner = SimpleNamespace()
    execution = SimpleNamespace()
    record = {
        "admission_id": "admission-r9",
        "generation": 2,
        "classic_identity": dict(identity),
    }
    authority._preservation_operations["classic-r9"] = record
    owner._classic_custody_authority = authority
    owner._classic_custody_key = "classic-r9"
    bridge = ClassicExecutionEventBridge(
        owner_event=owner,
        execution_event=execution,
        admission_id="admission-r9",
        generation=2,
        session_id=identity["session_id"],
        principal_id=identity["principal_id"],
        request_id=identity["request_id"],
        occurrence_id=identity["occurrence_id"],
        operation_key="classic-r9",
    )
    owner._classic_execution_bridge = bridge
    execution._classic_execution_bridge = bridge
    execution._classic_execution_owner_event = owner
    receipt = ClassicExecutionReceipt(
        session_id=identity["session_id"],
        session_key=identity["session_key"],
        identity=tuple(identity[key] for key in (
            "owner_class", "profile_id", "profile_home", "process_instance",
            "authority_epoch", "session_id", "principal_id", "request_id",
            "source_identity", "producer", "occurrence_id",
        )),
    )
    return owner, execution, bridge, receipt


def test_classic_receipt_handoff_requires_exact_bridge_event_and_generation():
    from gateway.session_classic_custody import handoff_classic_execution_receipt

    owner, execution, bridge, receipt = _bridge_handoff_case()
    execution._classic_execution_receipt = receipt
    assert handoff_classic_execution_receipt(owner, execution) is True
    assert owner._classic_execution_receipt is receipt

    foreign = SimpleNamespace(
        _classic_execution_owner_event=owner,
        _classic_execution_receipt=receipt,
    )
    assert handoff_classic_execution_receipt(owner, foreign) is False

    bridge.generation = 3
    assert handoff_classic_execution_receipt(owner, execution) is False

    bridge.generation = 2
    terminal = SimpleNamespace(_classic_execution_receipt=receipt)
    execution._classic_terminal_event = terminal
    assert handoff_classic_execution_receipt(owner, terminal) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", [None, "binding", "descriptor"])
async def test_terminal_automation_retention_never_releases_unmapped_terminal(
        kanban_owner, monkeypatch, broken):
    from gateway.session_kanban import run_task
    from hermes_state_runtime import (
        claim_session_input,
        get_session_admission,
        settle_session_input,
    )

    fixture = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(fixture.connection, fixture.params)
        authority = fixture.authority
        admission_id = receipt["receipt"]["admission_id"]
        queued = get_session_admission(authority.db, admission_id=admission_id)
        handle = authority._preservation_handles[admission_id]
        started = claim_session_input(
            authority.db, epoch=authority.epoch,
            session_id=queued["target_session_id"], head=admission_id,
        )
        assert isinstance(started, dict)
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=admission_id,
            generation=started["generation"], outcome="completed",
        )
        terminal = get_session_admission(authority.db, admission_id=admission_id)
        released = []
        monkeypatch.setattr(
            authority, "_preservation_release_unaccepted",
            lambda current: released.append(current),
        )
        if broken == "binding":
            authority._preservation_automation_bindings[admission_id] = {"handle": object()}
        elif broken == "descriptor":
            monkeypatch.setattr(
                authority,
                "_preservation_automation_row_descriptor",
                lambda _row: None,
            )
        handles_before = dict(authority._preservation_handles)
        bindings_before = dict(authority._preservation_automation_bindings)
        pending_before = dict(authority._preservation_pending_roots)

        authority._preservation_bind_or_release(terminal, handle)
        assert released == []
        if broken is None:
            assert authority._preservation_handles[admission_id] is handle
            assert authority._preservation_automation_bindings[admission_id]["handle"] is handle
        else:
            assert authority._preservation_handles == handles_before
            assert authority._preservation_automation_bindings == bindings_before
            assert authority._preservation_pending_roots == pending_before
    finally:
        await fixture.close()


def test_classic_receipt_handoff_rejects_nested_foreign_receipt():
    from gateway.session_classic_custody import handoff_classic_execution_receipt

    owner, execution, _bridge, receipt = _bridge_handoff_case()
    foreign_terminal = SimpleNamespace(_classic_execution_receipt=receipt)
    execution._classic_terminal_event = foreign_terminal
    assert handoff_classic_execution_receipt(owner, execution) is False
    assert not hasattr(owner, "_classic_execution_receipt")


def test_classic_receipt_handoff_requires_reference_operation_key():
    from gateway.session_classic_custody import handoff_classic_execution_receipt

    owner, execution, bridge, receipt = _bridge_handoff_case()
    execution._classic_execution_receipt = receipt
    bridge.operation_key = "foreign-operation"
    assert handoff_classic_execution_receipt(owner, execution) is False
    assert not hasattr(owner, "_classic_execution_receipt")


@pytest.mark.asyncio
async def test_terminal_automation_retention_rejects_conflicting_handle_map(
        kanban_owner, monkeypatch):
    from gateway.session_kanban import run_task
    from hermes_state_runtime import (
        claim_session_input,
        get_session_admission,
        settle_session_input,
    )

    fixture = kanban_owner({
        "model": {"default": "loop-model", "provider": "custom"},
        "platform_toolsets": {"cli": ["terminal"]},
    })
    try:
        receipt = await run_task(fixture.connection, fixture.params)
        authority = fixture.authority
        admission_id = receipt["receipt"]["admission_id"]
        queued = get_session_admission(authority.db, admission_id=admission_id)
        handle = authority._preservation_handles[admission_id]
        started = claim_session_input(
            authority.db, epoch=authority.epoch,
            session_id=queued["target_session_id"], head=admission_id,
        )
        assert isinstance(started, dict)
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=admission_id,
            generation=started["generation"], outcome="completed",
        )
        terminal = get_session_admission(authority.db, admission_id=admission_id)
        released = []
        monkeypatch.setattr(
            authority, "_preservation_release_unaccepted",
            lambda current: released.append(current),
        )
        foreign = object()
        authority._preservation_handles[admission_id] = foreign
        handles_before = dict(authority._preservation_handles)
        bindings_before = dict(authority._preservation_automation_bindings)
        pending_before = dict(authority._preservation_pending_roots)

        authority._preservation_bind_or_release(terminal, handle)
        assert released == []
        assert authority._preservation_handles == handles_before
        assert authority._preservation_automation_bindings == bindings_before
        assert authority._preservation_pending_roots == pending_before
    finally:
        await fixture.close()


@asynccontextmanager
async def _real_automation_delivery_case(tmp_path, monkeypatch, *, sidecar=False):
    """Drain the seeded native row through the real child/reader/finalizer before automation."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from gateway.platforms.event import MessageEvent
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_ingress import PreservedAutomationHandoff, admit_preserved_automation
    from hermes_state_runtime import claim_session_input, list_session_admissions, settle_session_input
    from tests.agent.transports.test_codex_physical_custody import (
        _RealProofAgent, _complete_real_admission, _portable_codex_launcher,
    )
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request, physical = await _complete_real_admission(
            session, fixture, writer, "seed-native-parent",
        )
        assert physical is not None
        assert not session._custody.service.request_is_active(request)

        runner, adapter, authority = fixture.runner, fixture.adapter, fixture.authority
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        key = runner.session_store._generate_session_key(source)
        entry = runner.session_store.lookup_by_session_key(key)
        assert entry is not None
        event = MessageEvent(
            text="automation-after-real-parent", source=source, internal=True,
            message_id="automation-after-real-parent",
            metadata={"gateway_session_key": key, "gateway_session_id": entry.session_id},
        )
        session_id = authority.logical_owner(entry.session_id)
        source_identity = automation_source_identity(event, key)
        handle = authority._preservation_automation_root(
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=source_identity,
            owner_class="I08_goals_loops_heartbeat",
        )
        handoff = PreservedAutomationHandoff(
            authority=authority, handle=handle, owner_class="I08_goals_loops_heartbeat",
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=source_identity,
        )
        await admit_preserved_automation(authority, adapter, event, handoff)
        rows = list_session_admissions(
            authority.db, session_id=session_id, pending_only=False,
        )
        row = next(candidate for candidate in rows if candidate["request_id"] == handoff.request_id)
        admission_id = row["admission_id"]
        if sidecar:
            from gateway.runtime_service_custody import admission_binding, reserve_service
            from gateway.session_contract import SessionRef

            ref = SessionRef(authority.profile_id, row["target_session_id"])
            live = authority.sessions[ref.session_id]
            started, _first = await authority._claim_next(ref, live)
            assert isinstance(started, dict) and started["status"] == "started"
            binding = admission_binding(authority, ref, started)
            service = reserve_service(binding, "automation-sidecar", producer_key="provider")
            request = service.begin_request(binding)
            authority._preservation_bind_sidecar(admission_id, binding.parent_handle, service, request)
            authority._preservation_release_sidecar(admission_id, request, durable=True)
            row = started
        else:
            started = claim_session_input(
                authority.db, epoch=authority.epoch,
                session_id=row["target_session_id"], head=admission_id,
            )
            assert isinstance(started, dict) and started["status"] == "started"
            row = started
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=admission_id,
            generation=row["generation"], outcome="completed",
        )

        from gateway.platforms.base import SendResult

        async def send(*_args, **_kwargs):
            return SendResult(success=True, message_id="automation-loopback-1")

        monkeypatch.setattr(adapter, "send", send)
        authority.pending_deliveries[admission_id] = (
            adapter, event, key, "automation transport reply", None,
        )
        yield runner, authority, admission_id
    finally:
        if (session is not None and session._client is not None
                and session._client._proc.poll() is None):
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_waiter_completion_without_physical_receipt_keeps_automation_root(tmp_path, monkeypatch):
    """A resolved waiter cannot authorize release when the source reports no delivery."""
    from gateway.session_ingress import DeliveryReceipt, finish_preservation_delivery

    async with _real_automation_delivery_case(tmp_path, monkeypatch) as (
        runner, authority, admission_id,
    ):
        try:
            result = await finish_preservation_delivery(
                authority,
                admission_id,
                DeliveryReceipt(
                    admission_id, attempted=False, delivered=False,
                    reason="no_delivery", effect_state="none",
                ),
                True,
            )
            assert result is False
            assert admission_id in authority._preservation_handles
            assert "terminal_delivery_receipt" not in authority._preservation_automation_bindings[
                admission_id
            ]
        finally:
            runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_actual_automation_transport_receipt_releases_exact_root(tmp_path, monkeypatch):
    """The real deliver_settled adapter path, not waiter state, supplies the positive receipt."""
    from gateway.session_ingress import deliver_settled, finish_preservation_delivery

    async with _real_automation_delivery_case(tmp_path, monkeypatch) as (
        runner, authority, admission_id,
    ):
        try:
            delivery = await deliver_settled(authority, admission_id)
            assert delivery.admission_id == admission_id
            assert delivery.attempted is True and delivery.delivered is True
            assert delivery.effect_state == "delivered"
            assert await finish_preservation_delivery(authority, admission_id, delivery, False) is True
            assert admission_id not in authority._preservation_handles
        finally:
            runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_actual_sidecar_finalizer_without_physical_receipt_keeps_root(tmp_path, monkeypatch):
    """A delivered-looking transport cannot bypass the source sidecar's missing physical receipt."""
    from gateway.session_ingress import deliver_settled, finish_preservation_delivery

    async with _real_automation_delivery_case(
        tmp_path, monkeypatch, sidecar=True,
    ) as (runner, authority, admission_id):
        try:
            delivery = await deliver_settled(authority, admission_id)
            assert delivery.delivered is True
            result = await finish_preservation_delivery(authority, admission_id, delivery, True)
            assert result is False
            assert admission_id in authority._preservation_handles
            sidecars = [
                entry for entry in authority._preservation_sidecars.values()
                if entry["admission_id"] == admission_id
            ]
            assert len(sidecars) == 1 and sidecars[0]["terminal"] is False
        finally:
            runner.session_store.close_all_db_handles()


@asynccontextmanager
async def _real_i02_automation_after_real_parent(tmp_path, monkeypatch):
    """Keep a real completed native parent live while I02 consumes its next FIFO row."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from gateway.platforms.event import MessageEvent
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_ingress import PreservedAutomationHandoff
    from tests.agent.transports.test_codex_physical_custody import (
        _RealProofAgent, _complete_real_admission, _portable_codex_launcher,
    )
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request, physical = await _complete_real_admission(
            session, fixture, writer, "i02-native-parent",
        )
        assert physical is not None
        assert not session._custody.service.request_is_active(request)

        runner, adapter, authority = fixture.runner, fixture.adapter, fixture.authority
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        key = runner.session_store._generate_session_key(source)
        entry = runner.session_store.lookup_by_session_key(key)
        assert entry is not None
        event = MessageEvent(
            text="i02-after-real-parent", source=source, internal=True,
            message_id="i02-after-real-parent",
            metadata={
                "gateway_session_key": key,
                "gateway_session_id": entry.session_id,
            },
        )
        session_id = authority.logical_owner(entry.session_id)
        source_identity = automation_source_identity(event, key)
        handle = authority._preservation_automation_root(
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=source_identity,
            owner_class="I08_goals_loops_heartbeat",
        )
        handoff = PreservedAutomationHandoff(
            authority=authority, handle=handle, owner_class="I08_goals_loops_heartbeat",
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=source_identity,
        )
        yield runner, adapter, authority, entry, event, handoff
    finally:
        if (session is not None and session._client is not None
                and session._client._proc.poll() is None):
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [None, "stale_generation", "foreign_request"])
async def test_i02_readiness_rereads_exact_automation_terminal_row(
        tmp_path, monkeypatch, mutation,
):
    """I02 may ignore only the current row for the exact retained automation handle."""
    from gateway.session_ingress import admit_preserved_automation
    from hermes_state_runtime import (
        claim_session_input, get_session_admission, list_session_admissions, settle_session_input,
    )
    async with _real_i02_automation_after_real_parent(
            tmp_path, monkeypatch,
    ) as (runner, adapter, authority, entry, event, handoff):
        try:
            await admit_preserved_automation(authority, adapter, event, handoff)
            [queued] = [
                row for row in list_session_admissions(
                    authority.db,
                    session_id=authority.logical_owner(entry.session_id), pending_only=False,
                ) if row["request_id"] == handoff.request_id
            ]
            started = claim_session_input(
                authority.db, epoch=authority.epoch,
                session_id=queued["target_session_id"], head=queued["admission_id"],
            )
            assert isinstance(started, dict) and started["status"] == "started"
            settle_session_input(
                authority.db, epoch=authority.epoch, admission_id=queued["admission_id"],
                generation=started["generation"], outcome="completed",
            )
            terminal = get_session_admission(authority.db, admission_id=queued["admission_id"])
            handle = authority._preservation_handles[queued["admission_id"]]
            assert authority._preservation_retain_terminal_automation(terminal, handle) is True
            binding = authority._preservation_automation_bindings[queued["admission_id"]]
            assert binding["terminal_pending"] is True
            if mutation == "stale_generation":
                binding["row"] = {**binding["row"], "generation": terminal["generation"] + 1}
            elif mutation == "foreign_request":
                binding["row"] = {**binding["row"], "request_id": "foreign-automation-request"}
            ready = authority.preservation_owner_ready("I02_api_session_turn")
            assert ready is (mutation is None)
            if mutation is not None:
                assert authority._preservation_handles[queued["admission_id"]] is handle
        finally:
            runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_real_parent_reader_result_finalizer_precedes_automation_fifo_admission(
        tmp_path, monkeypatch,
):
    """A real child/readback/finalizer closes before the next automation FIFO row is admitted."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from gateway.platforms.event import MessageEvent
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_ingress import PreservedAutomationHandoff, admit_preserved_automation
    from hermes_state_runtime import list_session_admissions
    from tests.agent.transports.test_codex_physical_custody import (
        _RealProofAgent, _complete_real_admission, _portable_codex_launcher,
    )
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request, physical = await _complete_real_admission(
            session, fixture, writer, "real-parent-complete",
        )
        assert physical is not None
        assert not session._custody.service.request_is_active(request)
        authority, adapter, runner = fixture.authority, fixture.adapter, fixture.runner
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        key = runner.session_store._generate_session_key(source)
        entry = runner.session_store.lookup_by_session_key(key)
        assert entry is not None
        event = MessageEvent(
            text="automation-after-real-parent", source=source, internal=True,
            message_id="automation-after-real-parent",
            metadata={"gateway_session_key": key, "gateway_session_id": entry.session_id},
        )
        session_id = authority.logical_owner(entry.session_id)
        handle = authority._preservation_automation_root(
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=automation_source_identity(event, key),
            owner_class="I08_goals_loops_heartbeat",
        )
        handoff = PreservedAutomationHandoff(
            authority=authority, handle=handle, owner_class="I08_goals_loops_heartbeat",
            request_id=event.message_id, principal_id="automation:" + key,
            session_id=session_id, source_identity=automation_source_identity(event, key),
        )
        receipt = await admit_preserved_automation(authority, adapter, event, handoff)
        assert receipt.status == "queued"
        rows = list_session_admissions(authority.db, session_id=session_id, pending_only=False)
        prior = next(row for row in rows if row["request_id"] == "real-parent-complete")
        followup = next(row for row in rows if row["request_id"] == event.message_id)
        assert prior["status"] == "terminal"
        assert followup["status"] == "queued"
        assert followup["seq"] > prior["seq"]
    finally:
        if (session is not None and session._client is not None
                and session._client._proc.poll() is None):
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_unaccepted_busy_return_keeps_source_unknown_without_producer_done(
        tmp_path, monkeypatch,
):
    """A real adapter's busy refusal cannot close a source lease as if FIFO accepted it."""
    import asyncio
    from tests.gateway.test_heartbeat_execution_ownership import _real_goal_owner

    runner, adapter, _authority, key, _entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch, seed_native=True,
    )
    try:
        adapter._active_sessions[key] = asyncio.Event()
        event.message_id = "unaccepted-busy-return"
        producer_done = []
        unknown = []

        async def refuse(_event, _session_key):
            return None

        monkeypatch.setattr(adapter, "_handle_message_while_active", refuse)
        monkeypatch.setattr(
            adapter, "_preservation_source_producer_done", lambda item: producer_done.append(item),
        )
        monkeypatch.setattr(
            adapter, "_preservation_source_unknown", lambda item, reason: unknown.append(reason),
        )
        await adapter.handle_message(event)
        assert event._gateway_accepted is False
        assert producer_done == []
        assert unknown == ["busy_dispatch_not_accepted"]
    finally:
        runner.session_store.close_all_db_handles()
