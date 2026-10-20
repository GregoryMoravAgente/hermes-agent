"""Real Gateway transcript/delivery witnesses for classic custody.

These controls use the shared SessionAuthority fixture and the real GatewayRunner/SessionDB helper.
They deliberately exercise row-addressed readback and the existing delivery ledger; text equality
and caller booleans are only negative inputs and never terminal proofs.
"""

from dataclasses import replace

import pytest


def _request_gateway_writer_handoff(writer, binding=None):
    return writer._request_gateway_writer_proof(binding) if binding is not None else writer._request_gateway_writer_proof()


def _gateway_writer_handoff_valid(handoff, writer, binding=None):
    from agent.session_persistence_proof import GatewayWriterProofHandoff

    return (
        type(handoff) is GatewayWriterProofHandoff
        and writer._peek_gateway_writer_proof() is handoff
        and (binding is None or handoff.binding is binding)
    )


def _capture_gateway_writer_handoff(writer, binding=None, *, owner_event=None):
    handoff = writer._peek_gateway_writer_proof()
    if handoff is None or (binding is not None and handoff.binding is not binding):
        return None
    if owner_event is not None:
        owner_event._classic_writer_handoff = handoff
    assert writer._ack_gateway_writer_proof(handoff) is True
    return handoff


def _source_and_entry(owner):
    from gateway.config import Platform
    from gateway.session import SessionSource

    source = SessionSource(
        platform=Platform.DISCORD, chat_id="writer-chat", chat_type="dm", user_id="writer-user",
    )
    store = owner.runner.session_store
    entry = store.get_or_create_session(source, touch_activity=False)
    return source, entry


def _restore_production_scheduler(authority, monkeypatch):
    """Use SessionAuthority's real drain scheduler; the shared fixture disables it for lower seams."""
    scheduler = getattr(type(authority), "_schedule", None)
    assert scheduler is not None
    monkeypatch.setattr(authority, "_schedule", scheduler.__get__(authority, type(authority)))
    assert getattr(authority._schedule, "__self__", None) is authority


def _install_custody(owner):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id,
        authority_epoch=owner.epoch,
        fence=RetirementFence(),
    )


def _configure_whatsapp_home(fixture, source):
    from gateway.config import HomeChannel, Platform, PlatformConfig

    fixture.runner.config.platforms[Platform.WHATSAPP] = PlatformConfig(
        enabled=True,
        home_channel=HomeChannel(platform=Platform.WHATSAPP, chat_id=source.chat_id, name="fixture"),
    )


def test_real_transcript_rows_have_distinct_readback_proofs(owner):
    _source, entry = _source_and_entry(owner)
    store = owner.runner.session_store
    first = store.append_to_transcript(
        entry.session_id,
        {"role": "user", "content": "identical", "message_id": "writer-1", "timestamp": 1.0},
    )
    second = store.append_to_transcript(
        entry.session_id,
        {"role": "user", "content": "identical", "message_id": "writer-2", "timestamp": 2.0},
    )

    assert first is not None and second is not None
    assert first.session_id == second.session_id == entry.session_id
    assert first.row_id != second.row_id
    assert first.role == second.role == "user"
    assert first.row_snapshot and second.row_snapshot

    rows = store._db_for_session_id(entry.session_id)._read_all(
        "SELECT id, session_id, role FROM messages WHERE session_id = ? AND id IN (?, ?)",
        (entry.session_id, first.row_id, second.row_id),
    )
    assert {int(row["id"]) for row in rows} == {first.row_id, second.row_id}


def test_transcript_skip_and_unavailable_store_remain_unproven(owner, monkeypatch):
    _source, entry = _source_and_entry(owner)
    store = owner.runner.session_store
    message = {"role": "assistant", "content": "deferred", "timestamp": 3.0}
    assert store.append_to_transcript(entry.session_id, message, skip_db=True) is None

    monkeypatch.setattr(store, "_db_for_session_id", lambda _sid: None)
    assert store.append_to_transcript(entry.session_id, message) is None
    assert entry.session_id in store._dirty_transcripts


def test_delivery_readback_and_foreign_false_ack_do_not_terminalize(owner, monkeypatch):
    from gateway import delivery_ledger

    monkeypatch.setattr(delivery_ledger, "_db_path", lambda: owner.db.db_path)
    session_key = "discord|writer-chat|dm"
    content = "durable reply"
    obligation_id = delivery_ledger.compute_obligation_id(session_key, "writer-3", content)
    delivery_ledger.record_obligation(
        obligation_id=obligation_id, session_key=session_key, platform="discord",
        chat_id="writer-chat", thread_id=None, content=content, adapter_profile="default",
    )
    delivery_ledger.mark_attempting(obligation_id)
    assert delivery_ledger.read_obligation(
        obligation_id, session_key=session_key, platform="discord", chat_id="writer-chat",
        thread_id=None, content=content, expected_state="delivered",
    ) is None
    delivery_ledger.mark_delivered(obligation_id)
    proof = delivery_ledger.read_obligation(
        obligation_id, session_key=session_key, platform="discord", chat_id="writer-chat",
        thread_id=None, content=content, expected_state="delivered",
    )
    assert proof is not None
    assert delivery_ledger.read_obligation(
        obligation_id, session_key=session_key, platform="discord", chat_id="foreign-chat",
        thread_id=None, content=content, expected_state="delivered",
    ) is None


def test_real_classic_receipt_requires_exact_rows_and_delivery(owner, monkeypatch):
    from gateway import delivery_ledger
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource, build_session_key
    from gateway.config import Platform
    from gateway.session_classic_custody import (
        _classic_delivery_proof_valid, _classic_receipt_terminal,
        _classic_transcript_rows_valid, build_classic_execution_receipt,
    )

    _install_custody(owner)
    source = SessionSource(
        platform=Platform.DISCORD, chat_id="classic-writer-chat", chat_type="dm", user_id="writer-user",
    )
    session_key = build_session_key(source)
    store = owner.runner.session_store
    entry = store.get_or_create_session(source, touch_activity=False)
    event = MessageEvent(text="same", source=source, message_id="classic-writer-4")
    assert owner._classic_begin(event, session_key, "fifo")

    user_proof = store.append_to_transcript(
        entry.session_id,
        {"role": "user", "content": "same", "message_id": event.message_id, "timestamp": 4.0},
    )
    result_proof = store.append_to_transcript(
        entry.session_id,
        {"role": "assistant", "content": "durable reply", "timestamp": 5.0},
    )
    assert user_proof is not None and result_proof is not None

    monkeypatch.setattr(delivery_ledger, "_db_path", lambda: owner.db.db_path)
    obligation_id = delivery_ledger.compute_obligation_id(session_key, event.message_id, "durable reply")
    delivery_ledger.record_obligation(
        obligation_id=obligation_id, session_key=session_key, platform="discord",
        chat_id=source.chat_id, thread_id=None, content="durable reply", adapter_profile="default",
    )
    delivery_ledger.mark_attempting(obligation_id)
    delivery_ledger.mark_delivered(obligation_id)
    delivery_proof = delivery_ledger.read_obligation(
        obligation_id, session_key=session_key, platform="discord", chat_id=source.chat_id,
        thread_id=None, content="durable reply", expected_state="delivered",
    )
    assert delivery_proof is not None

    event._classic_transcript_write_proofs = (user_proof, result_proof)
    event._classic_agent_persisted = False
    event._classic_delivery_proof = delivery_proof
    assert _classic_transcript_rows_valid((user_proof, result_proof), entry.session_id)
    assert _classic_delivery_proof_valid(delivery_proof, session_key)
    receipt = build_classic_execution_receipt(owner.runner, event, entry, {})
    assert receipt is not None
    assert _classic_receipt_terminal(receipt, owner._preservation_operations[event._classic_custody_key]["classic_identity"])

    foreign = replace(receipt, delivery_proof=replace(delivery_proof, session_key="foreign-session"))
    assert not _classic_receipt_terminal(
        foreign, owner._preservation_operations[event._classic_custody_key]["classic_identity"],
    )
    owner._classic_finish(
        event, success=True, receipt=receipt,
    )
    assert event._classic_custody_key not in owner._preservation_operations


def _real_session_writer(db, session_id):
    """Use the production SessionPersistenceMixin against the real fixture SessionDB."""
    import threading

    from agent.session_persistence import SessionPersistenceMixin

    writer = SessionPersistenceMixin()
    writer._session_db = db
    writer._session_db_created = True
    writer._session_persist_lock = threading.RLock()
    writer._flushed_db_message_ids = set()
    writer._last_flushed_db_idx = 0
    writer.session_id = session_id
    writer._persist_user_message_idx = None
    writer._persist_user_message_override = None
    writer._persist_user_message_timestamp = None
    return writer


@pytest.mark.asyncio
async def test_real_direct_classic_writer_proof_reaches_sessiondb_and_delivery(tmp_path, monkeypatch):
    """A real I06 root uses one SessionDB writer, exact readback, and the delivery ledger."""
    from gateway import delivery_ledger
    from gateway.platforms.event import MessageEvent
    from gateway.session import build_session_key
    from gateway.session_classic_custody import (
        _classic_receipt_terminal, build_classic_execution_receipt,
        classic_writer_binding_for_event,
    )
    from agent.session_persistence_proof import publish_gateway_writer_proof
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        source = fixture.adapter.build_source(
            chat_id="direct-writer@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        )
        session_key = fixture.runner.session_store._generate_session_key(source)
        entry = fixture.runner.session_store.get_or_create_session(source, touch_activity=False)
        event = MessageEvent(text="same", source=source, message_id="direct-writer-1")
        assert fixture.authority._classic_begin(event, session_key, "fifo")
        binding = classic_writer_binding_for_event(event)
        assert binding is not None

        writer = _real_session_writer(fixture.authority.db, entry.session_id)
        writer._current_turn_id = "direct-turn-1"
        _request_gateway_writer_handoff(writer, binding)
        messages = [
            {"role": "user", "content": "same"},
            {"role": "assistant", "content": "durable reply"},
        ]
        assert writer._flush_messages_to_session_db(messages, []) is True
        proof = writer._managed_shell_writer_proof
        assert proof is not None
        handoff = publish_gateway_writer_proof(writer, messages, proof, "direct-turn-1")
        assert handoff is not None
        assert _gateway_writer_handoff_valid(handoff, writer, binding)
        assert _capture_gateway_writer_handoff(writer, binding, owner_event=event) is handoff
        assert writer._peek_gateway_writer_proof() is None

        monkeypatch.setattr(delivery_ledger, "_db_path", lambda: fixture.authority.db.db_path)
        obligation_id = delivery_ledger.compute_obligation_id(session_key, event.message_id, "durable reply")
        delivery_ledger.record_obligation(
            obligation_id=obligation_id, session_key=session_key, platform="whatsapp",
            chat_id=source.chat_id, thread_id=None, content="durable reply", adapter_profile="default",
        )
        delivery_ledger.mark_attempting(obligation_id)
        delivery_ledger.mark_delivered(obligation_id)
        delivery_proof = delivery_ledger.read_obligation(
            obligation_id, session_key=session_key, platform="whatsapp", chat_id=source.chat_id,
            thread_id=None, content="durable reply", expected_state="delivered",
        )
        assert delivery_proof is not None
        event._classic_writer_handoff = handoff
        event._classic_delivery_proof = delivery_proof
        receipt = build_classic_execution_receipt(fixture.runner, event, entry, {})
        assert receipt is not None
        identity = fixture.authority._preservation_operations[event._classic_custody_key]["classic_identity"]
        assert _classic_receipt_terminal(receipt, identity)
        fixture.authority._classic_finish(event, success=True, receipt=receipt)
        assert event._classic_custody_key not in fixture.authority._preservation_operations
    finally:
        fixture.cleanup()


def test_classic_writer_proof_refuses_absent_or_foreign_owner_and_keeps_retry(owner):
    """Explicit absence, stale record, and foreign bindings remain unknown/retryable."""
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource, build_session_key
    from gateway.session_classic_custody import classic_writer_binding_for_event, classic_writer_binding_valid

    _install_custody(owner)
    source = SessionSource(
        platform=Platform.DISCORD, chat_id="negative-writer-chat", chat_type="dm", user_id="writer-user",
    )
    key = build_session_key(source)
    owner.runner.session_store.get_or_create_session(source, touch_activity=False)
    event = MessageEvent(text="same", source=source, message_id="negative-writer-1")
    assert owner._classic_begin(event, key, "fifo")
    binding = classic_writer_binding_for_event(event)
    assert binding is not None
    foreign = replace(binding, owner_id="foreign-owner")
    assert not classic_writer_binding_valid(foreign, expected_session_id=binding.session_id)

    idless = MessageEvent(
        text="same", source=SessionSource(
            platform=Platform.DISCORD, chat_id=None, chat_type="dm", user_id=None,
        ), message_id=None,
    )
    assert owner._classic_begin(idless, key, "fifo") is False

    owner._classic_finish(event, success=False, reason="writer_proof_missing")
    assert event._classic_custody_key in owner._preservation_operations
    assert owner._classic_begin(event, key, "fifo") is True


@pytest.mark.asyncio
async def test_nested_i04_classic_event_uses_existing_managed_writer_binding(tmp_path, monkeypatch):
    """A classic child of a real FIFO claim uses the existing I04 ContextVar, never I06 as a second root."""
    from gateway.platforms.event import MessageEvent
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from gateway.session_classic_custody import classic_writer_binding_for_event
    from agent.session_persistence_proof import publish_gateway_writer_proof
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim("nested-classic-writer")
        authority = fixture.authority
        source = authority.sessions[binding.session_id].source
        session_key = fixture.runner.session_store._generate_session_key(source)
        event = MessageEvent(text="nested", source=source, message_id="nested-classic-writer")
        assert authority._classic_begin(event, session_key, "fifo")
        record = authority._preservation_operations[event._classic_custody_key]
        assert record["parent_handle"] is binding.parent_handle
        assert classic_writer_binding_for_event(event) is None

        refused_writer = _real_session_writer(authority.db, binding.session_id)
        refused_writer._current_turn_id = "nested-explicit-none"
        refusal_token = bind_turn(binding)
        try:
            refused_writer._request_gateway_writer_proof(binding=None)
            assert refused_writer._flush_messages_to_session_db(
                [{"role": "user", "content": "refused"}, {"role": "assistant", "content": "refused"}],
                [],
            ) is False
            assert refused_writer._peek_gateway_writer_proof() is None
        finally:
            reset_turn(refusal_token)

        writer = _real_session_writer(authority.db, binding.session_id)
        writer._current_turn_id = "nested-turn-1"
        token = bind_turn(binding)
        try:
            _request_gateway_writer_handoff(writer)
            messages = [
                {"role": "user", "content": "nested"},
                {"role": "assistant", "content": "nested reply"},
            ]
            assert writer._flush_messages_to_session_db(messages, []) is True
            handoff = publish_gateway_writer_proof(
                writer, messages, writer._managed_shell_writer_proof, "nested-turn-1",
            )
            assert handoff is not None and handoff.binding is binding
            assert _gateway_writer_handoff_valid(handoff, writer)
            assert _capture_gateway_writer_handoff(writer, owner_event=event) is handoff
        finally:
            reset_turn(token)
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_gateway_runner_agent_sessiondb_delivery_has_no_private_proof_projection(
    tmp_path, monkeypatch,
):
    """Run the public adapter path with a real AIAgent and synthetic HTTP model.

    The transport is bounded to the in-process fake LLM and a loopback adapter send.  The
    GatewayRunner, SessionAuthority admission, SessionDB writer, delivery ledger and
    post-delivery callback remain production implementations.  The assertion deliberately
    inspects only the durable row/ledger witnesses and confirms that the private writer handoff
    never enters the agent result or the persisted message JSON.
    """
    import asyncio

    from gateway import delivery_ledger
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    authority = fixture.authority
    _restore_production_scheduler(authority, monkeypatch)
    sent: list[str] = []
    finished: list[tuple[object, bool, object | None]] = []
    original_classic_finish = authority._classic_finish

    def _observe_classic_finish(event, *, success, reason='join_failed', receipt=None):
        finished.append((event, success, receipt))
        return original_classic_finish(event, success=success, reason=reason, receipt=receipt)

    monkeypatch.setattr(authority, "_classic_finish", _observe_classic_finish)
    admission_trace: list[tuple[str, str | None, object | None]] = []
    restored_owner_trace: list[tuple[object, object | None]] = []

    original_classic_admitted = authority._classic_admission_committed
    original_schedule_admitted = authority._schedule_admitted
    original_owner_for_admission = authority._classic_execution_owner_for_admission
    original_handle_message = fixture.runner._handle_message

    def _record_classic_admitted(event, row):
        admission_trace.append(("owner", row.get("admission_id"), event))
        return original_classic_admitted(event, row)

    def _record_schedule_admitted(ref, receipt=None):
        admission_trace.append(("schedule", None, None))
        return original_schedule_admitted(ref, receipt)

    def _record_owner_for_admission(row):
        return original_owner_for_admission(row)

    async def _record_restored_owner(event):
        restored_owner_trace.append((event, getattr(event, "_classic_execution_owner_event", None)))
        return await original_handle_message(event)

    monkeypatch.setattr(authority, "_classic_admission_committed", _record_classic_admitted)
    monkeypatch.setattr(authority, "_schedule_admitted", _record_schedule_admitted)
    monkeypatch.setattr(authority, "_classic_execution_owner_for_admission", _record_owner_for_admission)
    monkeypatch.setattr(fixture.runner, "_handle_message", _record_restored_owner)

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id="real-route-reply-1")

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai", "api_key": "fixture-key",
            "base_url": llm.base_url, "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})

    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    session_key = fixture.runner.session_store._generate_session_key(source)
    entry = fixture.runner.session_store.get_or_create_session(source, touch_activity=False)
    event = MessageEvent(text="real public turn", source=source, message_id="real-route-1")
    try:
        with FakeLLMServer([Text("real gateway reply")]) as llm:
            await adapter.handle_message(event)
            task = adapter._session_tasks.get(session_key)
            assert task is not None
            await asyncio.wait_for(task, 30)

        owner_positions = [index for index, item in enumerate(admission_trace) if item[0] == "owner"]
        schedule_positions = [index for index, item in enumerate(admission_trace) if item[0] == "schedule"]
        assert owner_positions and schedule_positions
        assert owner_positions[0] < schedule_positions[0]
        assert any(item[2] is event for item in admission_trace if item[0] == "owner")
        assert any(owner is event for _restored, owner in restored_owner_trace)
        assert sent == ["real gateway reply"]
        rows = fixture.authority.db.get_messages(entry.session_id)
        assert any(row.get("role") == "user" and row.get("content") == "real public turn" for row in rows)
        assert any(row.get("role") == "assistant" and row.get("content") == "real gateway reply" for row in rows)
        assert all("_classic_writer_proof" not in row for row in rows)

        monkeypatch.setattr(delivery_ledger, "_db_path", lambda: fixture.authority.db.db_path)
        obligation_id = delivery_ledger.compute_obligation_id(
            session_key, event.message_id, "real gateway reply",
        )
        delivery = delivery_ledger.read_obligation(
            obligation_id,
            session_key=session_key,
            platform="whatsapp",
            chat_id=source.chat_id,
            thread_id=None,
            content="real gateway reply",
            expected_state="delivered",
        )
        assert delivery is not None
        assert not hasattr(event, "_classic_writer_proof")
        assert not hasattr(event, "_classic_writer_handoff")
        assert any(observed is event and success is True and receipt is not None
                   for observed, success, receipt in finished)
        assert not authority._preservation_operations
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_public_delivery_failure_keeps_exact_owner_for_retry(tmp_path, monkeypatch):
    """A real public send failure retains the original occurrence for the same-owner retry."""
    import asyncio

    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    authority = fixture.authority
    _restore_production_scheduler(authority, monkeypatch)
    finished: list[tuple[object, bool, object | None]] = []
    original_classic_finish = authority._classic_finish

    def _observe_classic_finish(event, *, success, reason='join_failed', receipt=None):
        finished.append((event, success, receipt))
        return original_classic_finish(event, success=success, reason=reason, receipt=receipt)

    monkeypatch.setattr(authority, "_classic_finish", _observe_classic_finish)
    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    session_key = fixture.runner.session_store._generate_session_key(source)
    event = MessageEvent(text="retryable public turn", source=source, message_id="retryable-public-1")
    sends = []

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sends.append(len(sends))
        return SendResult(success=False, message_id="failed-public-reply",
                          raw_response={"partial_overflow": True})

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda: {"provider": "openai", "api_key": "fixture-key",
                 "base_url": llm.base_url, "api_mode": "chat_completions"},
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})

    try:
        with FakeLLMServer([Text("retryable public reply")]) as llm:
            await adapter.handle_message(event)
            task = adapter._session_tasks.get(session_key)
            assert task is not None
            await asyncio.wait_for(task, 30)
        key = getattr(event, "_classic_custody_key", None)
        assert sends == [0]
        assert isinstance(key, str) and key in authority._preservation_operations
        record = authority._preservation_operations[key]
        assert record["producer_object"] is event
        assert record["unknown"] is True
        assert any(observed is event and success is False and receipt is None
                   for observed, success, receipt in finished)
        assert authority._classic_begin(event, session_key, "fifo") is True
        assert event._classic_custody_key == key
        assert authority._preservation_operations[key]["unknown"] is False
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_classic_fifo_keeps_two_occurrence_identities_and_terminal_receipts(
    tmp_path, monkeypatch,
):
    """Two real inbound messages retain FIFO occurrence identity through delivery and finish."""
    import asyncio

    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    authority = fixture.authority
    _restore_production_scheduler(authority, monkeypatch)
    sent: list[str] = []
    finished: list[tuple[object, bool]] = []
    begins: list[tuple[object, str | None]] = []
    first_processed = asyncio.Event()
    release_first = asyncio.Event()
    second_finished = asyncio.Event()
    original_classic_finish = authority._classic_finish
    original_classic_begin = authority._classic_begin
    original_handle_message_with_agent = fixture.runner._handle_message_with_agent

    def _observe_classic_finish(event, *, success, reason='join_failed', receipt=None):
        finished.append((event, success))
        if event is second and success:
            second_finished.set()
        return original_classic_finish(event, success=success, reason=reason, receipt=receipt)

    def _observe_classic_begin(event, session_key, producer, **kwargs):
        result = original_classic_begin(event, session_key, producer, **kwargs)
        if result:
            begins.append((event, getattr(event, "_classic_custody_key", None)))
        return result

    async def _hold_after_real_turn(*args, **kwargs):
        result = await original_handle_message_with_agent(*args, **kwargs)
        first_processed.set()
        await release_first.wait()
        return result

    monkeypatch.setattr(authority, "_classic_finish", _observe_classic_finish)
    monkeypatch.setattr(authority, "_classic_begin", _observe_classic_begin)
    monkeypatch.setattr(fixture.runner, "_handle_message_with_agent", _hold_after_real_turn)

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id=f"fifo-reply-{len(sent)}")

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda: {"provider": "openai", "api_key": "fixture-key",
                 "base_url": llm.base_url, "api_mode": "chat_completions"},
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})

    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    first = MessageEvent(text="fifo first", source=source, message_id="fifo-inbound-1")
    second = MessageEvent(text="fifo second", source=source, message_id="fifo-inbound-2")
    try:
        with FakeLLMServer([Text("fifo reply 1"), Text("fifo reply 2")]) as llm:
            first_call = asyncio.create_task(adapter.handle_message(first))
            await asyncio.wait_for(first_processed.wait(), 30)
            second_call = asyncio.create_task(adapter.handle_message(second))
            await asyncio.wait_for(second_call, 10)
            assert second._gateway_accepted is True
            release_first.set()
            await asyncio.wait_for(first_call, 30)
            await asyncio.wait_for(second_finished.wait(), 30)

        assert sent == ["fifo reply 1", "fifo reply 2"]
        assert [event for event, _key in begins[:2]] == [first, second]
        assert begins[0][1] is not None and begins[1][1] is not None
        assert begins[0][1] != begins[1][1]
        assert [event for event, success in finished if success] == [first, second]
        assert not authority._preservation_operations
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_classic_owner_loss_stays_unknown_until_same_owner_retry(tmp_path, monkeypatch):
    """A real admitted event loses its source owner and cannot close a successor implicitly."""
    import asyncio

    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    authority = fixture.authority
    _restore_production_scheduler(authority, monkeypatch)
    original_owner_for_admission = authority._classic_execution_owner_for_admission
    lost = {"done": False, "event": None, "key": None}

    def _lose_owner(row):
        owner, error = original_owner_for_admission(row)
        if owner is not None and not lost["done"]:
            key = getattr(owner, "_classic_custody_key", None)
            record = authority._preservation_operations.get(key)
            if isinstance(record, dict):
                lost.update(done=True, event=owner, key=key)
                authority._preservation_operation_unknown(key, "classic_owner_unknown")
                return None, "classic_owner_unknown"
        return owner, error

    monkeypatch.setattr(authority, "_classic_execution_owner_for_admission", _lose_owner)
    sent: list[str] = []

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id="owner-loss-reply")

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda: {"provider": "openai", "api_key": "fixture-key",
                 "base_url": llm.base_url, "api_mode": "chat_completions"},
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})

    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    session_key = fixture.runner.session_store._generate_session_key(source)
    event = MessageEvent(text="owner loss", source=source, message_id="owner-loss-inbound")
    try:
        with FakeLLMServer([Text("owner loss reply")]) as llm:
            await adapter.handle_message(event)
            task = adapter._session_tasks.get(session_key)
            assert task is not None
            await asyncio.wait_for(task, 30)

        assert lost["done"] is True
        assert isinstance(lost["key"], str)
        record = authority._preservation_operations[lost["key"]]
        assert record["unknown"] is True
        assert record["producer_object"] is lost["event"]
        assert record["classic_identity"]["producer"] == "running-slot"
        assert sent == []
        assert authority._classic_begin(lost["event"], session_key, "fifo") is True
        assert authority._preservation_operations[lost["key"]]["unknown"] is False
        assert authority._preservation_operations[lost["key"]]["classic_identity"]["producer"] == "running-slot"
        assert lost["event"]._classic_custody_key == lost["key"]
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_nested_i04_gateway_route_keeps_authoritative_binding_and_delivery(
    tmp_path, monkeypatch,
):
    """Consume a real FIFO/I04 admission through the public GatewayRunner route.

    ``bind_turn`` carries the actual admission binding into the task context; it is not a
    fabricated parent or an I06 helper.  The response still travels through the real AIAgent,
    SessionDB and adapter delivery path, and the exact admission row remains the authority for
    the writer handoff.
    """
    import asyncio

    from gateway import delivery_ledger
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    binding = await fixture.claim("nested-public-route")
    _restore_production_scheduler(fixture.authority, monkeypatch)
    sent: list[str] = []

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id="nested-public-reply-1")

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "openai", "api_key": "fixture-key",
            "base_url": llm.base_url, "api_mode": "chat_completions",
        },
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})

    source = fixture.authority.sessions[binding.session_id].source
    _configure_whatsapp_home(fixture, source)
    session_key = fixture.runner.session_store._generate_session_key(source)
    entry = fixture.runner.session_store.lookup_by_session_key(session_key)
    assert entry is not None and entry.session_id == binding.session_id
    event = MessageEvent(text="nested public turn", source=source, message_id="nested-public-1")
    token = bind_turn(binding)
    try:
        with FakeLLMServer([Text("nested public reply")]) as llm:
            await adapter.handle_message(event)
            task = adapter._session_tasks.get(session_key)
            assert task is not None
            await asyncio.wait_for(task, 30)
        assert sent == ["nested public reply"]
        rows = fixture.authority.db.get_messages(entry.session_id)
        assert any(row.get("role") == "assistant" and row.get("content") == "nested public reply" for row in rows)
        assert all("_classic_writer_proof" not in row for row in rows)
        monkeypatch.setattr(delivery_ledger, "_db_path", lambda: fixture.authority.db.db_path)
        obligation_id = delivery_ledger.compute_obligation_id(
            session_key, event.message_id, "nested public reply",
        )
        delivery = delivery_ledger.read_obligation(
            obligation_id,
            session_key=session_key,
            platform="whatsapp",
            chat_id=source.chat_id,
            thread_id=None,
            content="nested public reply",
            expected_state="delivered",
        )
        assert delivery is not None
        assert binding.session_id == entry.session_id
        assert type(binding.execution_generation) is int and binding.execution_generation >= 1
    finally:
        reset_turn(token)
        fixture.cleanup()
