"""Native queued-row links retain the accepted classic parent without minting a second root."""

from dataclasses import replace
import pytest


def _parented_event(fixture, binding, request_id):
    from gateway.platforms.event import MessageEvent

    authority = fixture.authority
    source = authority.sessions[binding.session_id].source
    session_key = fixture.runner.session_store._generate_session_key(source)
    event = MessageEvent(text=request_id, source=source, message_id=request_id)
    assert authority._classic_begin(event, session_key, "fifo")
    key = event._classic_custody_key
    record = authority._preservation_operations[key]
    assert record["parent_handle"] is binding.parent_handle
    return event, key, record


def _restore_production_scheduler(authority, monkeypatch):
    scheduler = getattr(type(authority), "_schedule", None)
    assert scheduler is not None
    monkeypatch.setattr(authority, "_schedule", scheduler.__get__(authority, type(authority)))


def _configure_whatsapp_home(fixture, source):
    from gateway.config import HomeChannel, Platform, PlatformConfig

    fixture.runner.config.platforms[Platform.WHATSAPP] = PlatformConfig(
        enabled=True,
        home_channel=HomeChannel(platform=Platform.WHATSAPP, chat_id=source.chat_id, name="fixture"),
    )


@pytest.mark.asyncio
async def test_real_parented_native_admission_binds_exact_queued_row(tmp_path, monkeypatch):
    """A real native child links to the existing I04 parent before its own claim."""
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim("native-link-parent")
        authority = fixture.authority
        event, key, record = _parented_event(fixture, binding, "native-link-child")

        receipt = await authority.admit_native(event)
        row = get_session_admission(authority.db, admission_id=receipt.admission_id)

        assert receipt.status == "queued"
        assert row is not None
        assert row["admission_id"] == receipt.admission_id
        assert row["status"] == "queued"
        assert row["generation"] is None
        assert row["owner_epoch"] == authority.epoch
        assert row["target_session_id"] == record["session_id"]
        assert row["principal_id"] == record["principal_id"]
        assert row["request_id"] == record["request_id"]
        assert row["payload"]["native_text_v1"]["producer_process_mode"] == "managed_turn"
        assert "parent_handle" not in row["payload"]
        assert "_classic_custody_key" not in row["payload"]
        assert record["admission_id"] == receipt.admission_id
        assert record["parent_handle"] is binding.parent_handle
        assert record["handle"].owner_class == "I06_busy_followup_delivery"
        assert authority._preservation_children[binding.admission_id] is binding.parent_handle
        assert receipt.admission_id not in authority._preservation_children
        assert authority._preservation_operations[key]["unknown"] is False
        reread = authority._preservation_operation_row(
            key, expected_parent=binding.parent_handle)
        assert reread is not None
        assert reread["admission_id"] == receipt.admission_id
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_classic_busy_parented_native_admission_uses_direct_parent(tmp_path, monkeypatch):
    """The public first classic row keeps classic_busy while its genuine I04 claim runs."""
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        authority = fixture.authority
        adapter = fixture.runner.adapters[Platform.WHATSAPP]
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        )
        session_key = fixture.runner.session_store._generate_session_key(source)
        first = MessageEvent(text="classic-direct-parent", source=source, message_id="classic-direct-parent")
        assert authority._classic_begin(first, session_key, "running-slot")
        first_receipt = await authority.admit_native(first)
        first_row = get_session_admission(authority.db, admission_id=first_receipt.admission_id)
        assert first_row["payload"]["native_text_v1"]["producer_process_mode"] == "classic_busy"
        live = authority.sessions[first_receipt.ref.session_id]
        claimed, _first = await authority._claim_next(first_receipt.ref, live)
        assert claimed["status"] == "started"
        claimed = get_session_admission(authority.db, admission_id=claimed["admission_id"])
        from gateway.runtime_service_custody import admission_binding
        binding = admission_binding(authority, first_receipt.ref, claimed)
        event, key, record = _parented_event(fixture, binding, "classic-direct-child")

        receipt = await authority.admit_native(event)
        row = get_session_admission(authority.db, admission_id=receipt.admission_id)
        assert row["payload"]["native_text_v1"]["producer_process_mode"] == "managed_turn"
        assert record["parent_handle"] is binding.parent_handle
        assert authority._preservation_operation_row(
            key, expected_parent=binding.parent_handle,
        )["admission_id"] == receipt.admission_id
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control",
    ("missing_parent", "foreign_parent", "stale_parent", "current_claim_mismatch"),
)
async def test_parented_native_admission_rejects_invalid_parent_or_live_i04(
    tmp_path, monkeypatch, control,
):
    """Missing, copied, stale, and mismatched current I04 links remain fail-closed."""
    from hermes_state_runtime import RuntimeStoreError
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    record = None
    original_parent = None
    live = None
    original_execution = None
    try:
        binding = await fixture.claim("native-link-negative-" + control)
        authority = fixture.authority
        event, _key, record = _parented_event(
            fixture, binding, "native-link-negative-child-" + control)
        original_parent = record["parent_handle"]
        live = authority.sessions[binding.session_id]
        if control == "missing_parent":
            record["parent_handle"] = None
        elif control == "foreign_parent":
            record["parent_handle"] = replace(original_parent, profile_id="foreign-profile")
        elif control == "stale_parent":
            record["parent_handle"] = replace(original_parent, owner_id="claim:stale-parent")
        else:
            original_execution = dict(live.event_stream.execution)
            live.event_stream.execution["admission_id"] = "foreign-live-admission"

        with pytest.raises(RuntimeStoreError, match="preservation_owner_unmapped"):
            await authority.admit_native(event)
        assert record["unknown"] is True
    finally:
        if record is not None:
            record["parent_handle"] = original_parent
        if original_execution is not None and live is not None:
            live.event_stream.execution.clear()
            live.event_stream.execution.update(original_execution)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("control", ("foreign_direct_parent", "stale_direct_parent"))
async def test_classic_busy_parented_native_admission_rejects_direct_parent_tamper(
    tmp_path, monkeypatch, control,
):
    """A classic_busy parent marker cannot authorize a copied or stale I04 handle."""
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import RuntimeStoreError, get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    record = None
    original_parent = None
    try:
        authority = fixture.authority
        adapter = fixture.runner.adapters[Platform.WHATSAPP]
        source = adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        )
        session_key = fixture.runner.session_store._generate_session_key(source)
        first = MessageEvent(text="classic-direct-negative-parent", source=source,
                             message_id="classic-direct-negative-parent")
        assert authority._classic_begin(first, session_key, "running-slot")
        first_receipt = await authority.admit_native(first)
        first_row = get_session_admission(authority.db, admission_id=first_receipt.admission_id)
        assert first_row["payload"]["native_text_v1"]["producer_process_mode"] == "classic_busy"
        live = authority.sessions[first_receipt.ref.session_id]
        claimed, _first = await authority._claim_next(first_receipt.ref, live)
        assert claimed["status"] == "started"
        claimed = get_session_admission(authority.db, admission_id=claimed["admission_id"])
        from gateway.runtime_service_custody import admission_binding
        binding = admission_binding(authority, first_receipt.ref, claimed)
        event, _key, record = _parented_event(fixture, binding, "classic-direct-negative-child-" + control)
        original_parent = record["parent_handle"]
        record["parent_handle"] = (
            replace(original_parent, profile_id="foreign-profile")
            if control == "foreign_direct_parent"
            else replace(original_parent, owner_id="claim:stale-direct-parent")
        )

        with pytest.raises(RuntimeStoreError, match="preservation_owner_unmapped"):
            await authority.admit_native(event)
        assert record["unknown"] is True
    finally:
        if record is not None:
            record["parent_handle"] = original_parent
        fixture.cleanup()


async def _run_real_authority_callback_case(tmp_path, monkeypatch, control=None):
    """Drive the public I04/I06 delivery consumer with the real SessionDB graph."""
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
    sent = []
    consumer_results = []
    callback_observations = []
    consumer_done = asyncio.Event()
    second_finished = asyncio.Event()
    first_processed = asyncio.Event()
    release_first = asyncio.Event()
    original_finish = authority._classic_finish
    from gateway import session_classic_delivery
    from gateway import delivery_ledger

    original_consumer = session_classic_delivery.fire_authority_post_delivery_callback
    original_read_obligation = delivery_ledger.read_obligation
    mutated = False
    fire_calls = 0
    diagnostic = []
    settlement_observations = []
    finish_state_by_admission = {}
    observed_operation_key = None

    original_unknown = authority._preservation_operation_unknown
    original_release_terminal = authority._preservation_release_terminal

    def _observe_release_terminal(admission_id):
        children = getattr(authority, "_preservation_children", None)
        records = getattr(authority, "_preservation_operations", None)
        matching = [
            (key, record)
            for key, record in (records.items() if isinstance(records, dict) else ())
            if isinstance(record, dict)
            and record.get("owner_class") == "I06_busy_followup_delivery"
            and record.get("admission_id") == admission_id
        ]
        settlement_observations.append({
            "kind": "release_before",
            "admission_id_present": isinstance(admission_id, str) and bool(admission_id),
            "record_present_before": len(matching) == 1,
            "child_present_before": (
                isinstance(children, dict) and admission_id in children
            ),
            "finish_state": finish_state_by_admission.get(admission_id),
        })
        result = original_release_terminal(admission_id)
        children_after = getattr(authority, "_preservation_children", None)
        settlement_observations.append({
            "kind": "release_after",
            "admission_id_present": isinstance(admission_id, str) and bool(admission_id),
            "child_present_after": (
                isinstance(children_after, dict) and admission_id in children_after
            ),
        })
        return result

    def _observe_unknown(key, reason):
        if key == observed_operation_key:
            diagnostic.append({
                "kind": "unknown",
                "operation": "parented_child",
                "reason": str(reason),
            })
        return original_unknown(key, reason)

    def _safe_bool(callback):
        try:
            return bool(callback())
        except Exception:
            return False

    def _safe_value(callback, default=None):
        try:
            return callback()
        except Exception:
            return default

    def _read_parent_row(authority_arg, parent_admission_id):
        if not isinstance(parent_admission_id, str) or not parent_admission_id:
            return None
        try:
            from hermes_state_runtime import get_session_admission

            row = get_session_admission(authority_arg.db, admission_id=parent_admission_id)
        except Exception:
            return None
        return row if isinstance(row, dict) else None

    def _first_false(checks):
        return next((name for name, value in checks if not value), None)

    def _receipt_snapshot(authority_arg, record, parent):
        from gateway.session_classic_custody import (
            _CLASSIC_IDENTITY_FIELDS,
            _classic_writer_binding_valid,
            _classic_writer_proof_valid,
        )

        identity = record.get("classic_identity") if isinstance(record, dict) else None
        receipt = _safe_value(lambda: session_classic_delivery._classic_receipt_for_event(
            record.get("producer_object"),
        ))
        binding = getattr(receipt, "writer_binding", None)
        identity_present = isinstance(identity, dict)
        receipt_present = receipt is not None
        identity_matches = (
            identity_present
            and receipt_present
            and _safe_bool(lambda: receipt.identity == tuple(
                identity.get(field) for field in _CLASSIC_IDENTITY_FIELDS
            ))
        )
        session_id_matches = (
            identity_present
            and receipt_present
            and _safe_bool(lambda: receipt.session_id == identity.get("session_id"))
        )
        session_key_matches = (
            identity_present
            and receipt_present
            and _safe_bool(lambda: receipt.session_key == identity.get("session_key"))
        )
        writer_proof_valid = (
            receipt_present
            and _safe_bool(lambda: _classic_writer_proof_valid(
                receipt.writer_proof, receipt.session_id,
            ))
        )
        writer_binding_valid = (
            identity_present
            and binding is not None
            and _safe_bool(lambda: _classic_writer_binding_valid(
                binding, identity, record, allow_terminal=True,
            ))
        )
        snapshot = {
            "child_receipt_present": receipt_present,
            "child_receipt_identity_matches": identity_matches,
            "child_receipt_session_id_matches": session_id_matches,
            "child_receipt_session_key_matches": session_key_matches,
            "child_writer_proof_valid": writer_proof_valid,
            "child_writer_binding_present": binding is not None,
            "child_writer_binding_authority_matches": (
                getattr(binding, "authority", None) is authority_arg
            ),
            "child_writer_binding_parent_matches": (
                getattr(binding, "parent_handle", None) is parent
            ),
            "child_writer_binding_admission_matches": (
                getattr(binding, "admission_id", None) == record.get("admission_id")
            ),
            "child_writer_binding_valid": writer_binding_valid,
        }
        checks = [
            ("identity_present", identity_present),
            ("receipt_present", receipt_present),
            ("receipt_identity_matches", identity_matches),
            ("receipt_session_id_matches", session_id_matches),
            ("receipt_session_key_matches", session_key_matches),
            ("writer_proof_valid", writer_proof_valid),
            (
                "writer_binding_authority_matches",
                getattr(binding, "authority", None) is authority_arg,
            ),
            ("writer_binding_parent_matches", getattr(binding, "parent_handle", None) is parent),
            (
                "writer_binding_admission_matches",
                getattr(binding, "admission_id", None) == record.get("admission_id"),
            ),
            ("writer_binding_valid", writer_binding_valid),
        ]
        return snapshot, checks

    def _custody_edge_snapshot(authority_arg, parent, parent_admission_id):
        children = getattr(authority_arg, "_preservation_children", None)
        children_identity = (
            isinstance(children, dict)
            and children.get(parent_admission_id) is parent
        )
        coordinator = _safe_value(lambda: authority_arg._preservation_coordinator())
        coordinator_edge = (
            coordinator is not None
            and _safe_bool(lambda: coordinator._parent(parent) is not None)
        )
        return {
            "children_map_present": isinstance(children, dict),
            "children_map_parent_identity": children_identity,
            "coordinator_present": coordinator is not None,
            "coordinator_parent_edge": coordinator_edge,
        }

    def _finish_snapshot(
        event, *, operation_key=None, success=None, reason=None, receipt=None, phase,
    ):
        """Return sanitized settlement evidence without changing any custody state."""
        from gateway.session_classic_custody import (
            _CLASSIC_IDENTITY_FIELDS,
            classic_execution_owner_for_event,
        )

        owner = classic_execution_owner_for_event(event)
        bridge = getattr(owner, "_classic_execution_bridge", None)
        execution = getattr(bridge, "execution_event", None)
        event_key = getattr(event, "_classic_custody_key", None)
        owner_key = getattr(owner, "_classic_custody_key", None)
        key = operation_key if isinstance(operation_key, str) else owner_key or event_key
        records = getattr(authority, "_preservation_operations", None)
        record = records.get(key) if isinstance(records, dict) and isinstance(key, str) else None
        identity = record.get("classic_identity") if isinstance(record, dict) else None
        current_receipt = _safe_value(lambda: session_classic_delivery._classic_receipt_for_event(owner))
        observed_receipt = receipt if receipt is not None else current_receipt
        receipt_identity = getattr(observed_receipt, "identity", None)
        expected_identity = (
            tuple(identity.get(field) for field in _CLASSIC_IDENTITY_FIELDS)
            if isinstance(identity, dict) else None
        )
        proof = getattr(observed_receipt, "delivery_proof", None)
        source_parent = record.get("parent_handle") if isinstance(record, dict) else None
        writer_binding = getattr(observed_receipt, "writer_binding", None)
        writer_parent = getattr(writer_binding, "parent_handle", None)
        coordinator = _safe_value(lambda: authority._preservation_coordinator())
        children = getattr(authority, "_preservation_children", None)
        source_owner_id = getattr(source_parent, "owner_id", None)
        writer_owner_id = getattr(writer_parent, "owner_id", None)
        source_admission_id = (
            source_owner_id[len("claim:"):]
            if isinstance(source_owner_id, str) and source_owner_id.startswith("claim:")
            else None
        )
        writer_admission_id = (
            writer_owner_id[len("claim:"):]
            if isinstance(writer_owner_id, str) and writer_owner_id.startswith("claim:")
            else None
        )
        source_parent_edge = _safe_bool(
            lambda: coordinator is not None and coordinator._parent(source_parent) is not None,
        )
        writer_parent_edge = _safe_bool(
            lambda: coordinator is not None and coordinator._parent(writer_parent) is not None,
        )
        checks = [
            ("record_present", isinstance(record, dict)),
            ("record_unknown_false", isinstance(record, dict) and record.get("unknown") is not True),
            ("record_state_present", isinstance(record, dict) and record.get("state") is not None),
            ("record_producer_is_owner", (
                isinstance(record, dict) and record.get("producer_object") is owner
            )),
            ("record_occurrence_present", isinstance(identity, dict) and bool(identity.get("occurrence_id"))),
            ("bridge_occurrence_matches_record", (
                bridge is not None
                and isinstance(identity, dict)
                and getattr(bridge, "occurrence_id", None) == identity.get("occurrence_id")
            )),
            ("receipt_present", observed_receipt is not None),
            ("receipt_identity_matches_record", receipt_identity == expected_identity),
            ("receipt_session_id_matches_record", (
                observed_receipt is not None
                and getattr(observed_receipt, "session_id", None) == (
                    identity.get("session_id") if isinstance(identity, dict) else None
                )
            )),
            ("receipt_session_key_matches_record", (
                observed_receipt is not None
                and getattr(observed_receipt, "session_key", None) == (
                    identity.get("session_key") if isinstance(identity, dict) else None
                )
            )),
            ("delivery_proof_present", proof is not None),
            ("delivery_proof_delivered", getattr(proof, "state", None) == "delivered"),
            ("writer_binding_present", writer_binding is not None),
            ("writer_binding_admission_matches_record", (
                writer_binding is not None
                and getattr(writer_binding, "admission_id", None) == (
                    record.get("admission_id") if isinstance(record, dict) else None
                )
            )),
            ("source_parent_present", source_parent is not None),
            ("source_parent_coordinator_edge", source_parent_edge),
            ("writer_parent_present", writer_parent is not None),
            ("writer_parent_coordinator_edge", writer_parent_edge),
            ("coordinator_present", coordinator is not None),
        ]
        return {
            "kind": phase,
            "finish_success": success,
            "finish_reason": str(reason) if reason is not None else None,
            "event_is_owner": event is owner,
            "event_key_present": isinstance(event_key, str),
            "owner_key_present": isinstance(owner_key, str),
            "event_owner_key_same": event_key == owner_key,
            "bridge_present": bridge is not None,
            "bridge_owner_event_same": getattr(bridge, "owner_event", None) is owner,
            "bridge_execution_event_same": getattr(bridge, "execution_event", None) is execution,
            "record_producer_is_owner": (
                isinstance(record, dict) and record.get("producer_object") is owner
            ),
            "record_producer_is_event": (
                isinstance(record, dict) and record.get("producer_object") is event
            ),
            "bridge_occurrence_matches_record": (
                bridge is not None
                and isinstance(identity, dict)
                and getattr(bridge, "occurrence_id", None) == identity.get("occurrence_id")
            ),
            "record_present": isinstance(record, dict),
            "record_owner_class": record.get("owner_class") if isinstance(record, dict) else None,
            "record_unknown": record.get("unknown") if isinstance(record, dict) else None,
            "record_state": record.get("state") if isinstance(record, dict) else None,
            "record_admission_present": bool(record.get("admission_id")) if isinstance(record, dict) else False,
            "record_occurrence_present": isinstance(identity, dict) and bool(identity.get("occurrence_id")),
            "record_request_present": isinstance(identity, dict) and bool(identity.get("request_id")),
            "record_session_id_present": isinstance(identity, dict) and bool(identity.get("session_id")),
            "record_session_key_present": isinstance(identity, dict) and bool(identity.get("session_key")),
            "receipt_present": observed_receipt is not None,
            "receipt_identity_present": isinstance(receipt_identity, tuple) and bool(receipt_identity),
            "receipt_identity_matches_record": receipt_identity == expected_identity,
            "receipt_session_id_present": bool(getattr(observed_receipt, "session_id", None)),
            "receipt_session_id_matches_record": (
                observed_receipt is not None
                and getattr(observed_receipt, "session_id", None) == (
                    identity.get("session_id") if isinstance(identity, dict) else None
                )
            ),
            "receipt_session_key_present": bool(getattr(observed_receipt, "session_key", None)),
            "receipt_session_key_matches_record": (
                observed_receipt is not None
                and getattr(observed_receipt, "session_key", None) == (
                    identity.get("session_key") if isinstance(identity, dict) else None
                )
            ),
            "delivery_proof_present": proof is not None,
            "delivery_proof_type": type(proof).__name__ if proof is not None else None,
            "delivery_proof_state": getattr(proof, "state", None),
            "delivery_proof_obligation_present": bool(getattr(proof, "obligation_id", None)),
            "delivery_proof_session_key_matches_receipt": (
                proof is not None
                and getattr(proof, "session_key", None) == getattr(observed_receipt, "session_key", None)
            ),
            "writer_binding_present": writer_binding is not None,
            "writer_binding_admission_matches_record": (
                writer_binding is not None
                and getattr(writer_binding, "admission_id", None) == (
                    record.get("admission_id") if isinstance(record, dict) else None
                )
            ),
            "source_parent_present": source_parent is not None,
            "source_parent_owner_class": getattr(source_parent, "owner_class", None),
            "source_parent_owner_id_present": bool(source_owner_id),
            "source_parent_admission_present": bool(source_admission_id),
            "writer_parent_present": writer_parent is not None,
            "writer_parent_owner_class": getattr(writer_parent, "owner_class", None),
            "writer_parent_owner_id_present": bool(writer_owner_id),
            "writer_parent_admission_present": bool(writer_admission_id),
            "source_parent_is_writer_parent": source_parent is writer_parent,
            "source_parent_children_identity": (
                isinstance(children, dict) and children.get(source_admission_id) is source_parent
            ),
            "writer_parent_children_identity": (
                isinstance(children, dict) and children.get(writer_admission_id) is writer_parent
            ),
            "source_parent_coordinator_edge": source_parent_edge,
            "writer_parent_coordinator_edge": writer_parent_edge,
            "coordinator_present": coordinator is not None,
            "coordinator_children_map_present": isinstance(children, dict),
            "first_false_predicate": _first_false(checks),
        }

    def _obligation_snapshot(phase, args, kwargs, proof=None):
        obligation_id = args[0] if args else kwargs.get("obligation_id")
        session_key = kwargs.get("session_key")
        content = kwargs.get("content")
        if content is None and len(args) > 5:
            content = args[5]
        return {
            "kind": phase,
            "obligation_id_present": bool(obligation_id),
            "session_key_present": bool(session_key),
            "content_length": len(str(content)) if content is not None else None,
            "content_is_second_synthetic_reply": str(content) == "authority reply 2",
            "expected_state": kwargs.get("expected_state"),
            "proof_present": proof is not None,
            "proof_type": type(proof).__name__ if proof is not None else None,
            "proof_obligation_id_present": bool(getattr(proof, "obligation_id", None)),
            "proof_session_key_present": bool(getattr(proof, "session_key", None)),
            "proof_session_key_matches_request": (
                proof is not None and getattr(proof, "session_key", None) == session_key
            ),
            "proof_state": getattr(proof, "state", None),
        }

    def _observe_read_obligation(*args, **kwargs):
        before = _obligation_snapshot("delivery_ledger_read_before", args, kwargs)
        diagnostic.append(before)
        try:
            proof = original_read_obligation(*args, **kwargs)
        except BaseException as exc:
            after = _obligation_snapshot("delivery_ledger_read_after", args, kwargs)
            after["exception_type"] = type(exc).__name__
            diagnostic.append(after)
            raise
        diagnostic.append(_obligation_snapshot(
            "delivery_ledger_read_after", args, kwargs, proof,
        ))
        return proof

    def _capture_parent_proof(authority_arg, record):
        from gateway.session_classic_custody import (
            _classic_admission_row_matches,
            classic_direct_route_marker,
            classic_row_is_direct,
        )

        parent = record.get("parent_handle") if isinstance(record, dict) else None
        parent_owner_id = getattr(parent, "owner_id", None)
        owner_id_is_str = type(parent_owner_id) is str
        owner_id_is_claim = owner_id_is_str and parent_owner_id.startswith("claim:")
        parent_admission_id = (
            parent_owner_id[len("claim:"):] if owner_id_is_claim else None
        )
        parent_row = _read_parent_row(authority_arg, parent_admission_id)
        records = getattr(authority_arg, "_preservation_operations", None)
        candidates = [
            (key, candidate)
            for key, candidate in (records.items() if isinstance(records, dict) else ())
            if isinstance(candidate, dict)
            and candidate.get("owner_class") == "I06_busy_followup_delivery"
            and candidate.get("admission_id") == parent_admission_id
            and candidate.get("parent_handle") is None
        ]
        parent_key, parent_record = (
            candidates[0] if len(candidates) == 1 else (None, None)
        )
        parent_owner = (
            parent_record.get("producer_object") if parent_record is not None else None
        )
        row_status = parent_row.get("status") if parent_row is not None else None
        row_outcome = parent_row.get("outcome") if parent_row is not None else None
        row_generation = parent_row.get("generation") if parent_row is not None else None
        row_direct = _safe_bool(lambda: classic_row_is_direct(parent_row))
        row_identity = (
            None
            if parent_record is None
            else _safe_bool(lambda: _classic_admission_row_matches(
                authority_arg,
                parent_record,
                parent_row,
                allow_terminal=row_status == "terminal",
            ))
        )
        route_marker = (
            _safe_value(lambda: classic_direct_route_marker(authority_arg, parent_owner))
            if parent_owner is not None
            else None
        )
        checks = [
            ("parent_handle_present", parent is not None),
            ("parent_owner_class", getattr(parent, "owner_class", None) == "I04_fifo_turn_execution"),
            ("parent_owner_id_type", owner_id_is_str),
            ("parent_owner_id_prefix", owner_id_is_str and parent_owner_id.startswith("claim:")),
            ("parent_admission_present", bool(parent_admission_id)),
            ("parent_row_present", parent_row is not None),
            ("parent_record_count_at_most_one", len(candidates) <= 1),
            ("parent_row_direct_mode", row_direct),
            (
                "parent_row_admission_matches",
                parent_row is not None and parent_row.get("admission_id") == parent_admission_id,
            ),
            (
                "parent_row_session_matches",
                parent_row is not None and parent_row.get("target_session_id") == record.get("session_id"),
            ),
            (
                "parent_row_principal_matches",
                parent_row is not None and parent_row.get("principal_id") == record.get("principal_id"),
            ),
            (
                "parent_row_epoch_matches",
                parent_row is not None and parent_row.get("owner_epoch") == authority_arg.epoch,
            ),
            ("parent_row_status_allowed", row_status in {"started", "terminal"}),
            ("parent_row_generation_integer", type(row_generation) is int),
            ("parent_row_generation_positive", (
                type(row_generation) is int and row_generation >= 1
            )),
            (
                "parent_row_request_id_type",
                parent_row is not None and type(parent_row.get("request_id")) is str,
            ),
            (
                "parent_row_request_id_nonempty",
                parent_row is not None and bool(parent_row.get("request_id")),
            ),
        ]
        if parent_record is None:
            checks.extend([
                ("parent_row_terminal", row_status == "terminal"),
                ("parent_row_completed", row_outcome == "completed"),
            ])
        else:
            checks.extend([
                ("parent_owner_present", parent_owner is not None),
                (
                    "parent_owner_authority_matches",
                    getattr(parent_owner, "_classic_custody_authority", None) is authority_arg,
                ),
                (
                    "parent_owner_key_matches",
                    getattr(parent_owner, "_classic_custody_key", None) == parent_key,
                ),
                ("parent_record_unknown", parent_record.get("unknown") is not True),
                ("parent_direct_route_marker", route_marker == "classic_busy"),
                (
                    "parent_record_generation_matches",
                    parent_record.get("generation") == row_generation,
                ),
                ("parent_row_identity_matches", row_identity),
            ])
        child_receipt, _child_checks = _receipt_snapshot(authority_arg, record, parent)
        custody_edge = _custody_edge_snapshot(
            authority_arg, parent, parent_admission_id,
        )
        return {
            "kind": "parent_proof",
            "parent_record_present": parent_record is not None,
            "parent_record_count": len(candidates),
            "parent_record_unknown": (
                parent_record.get("unknown") is True if parent_record is not None else None
            ),
            "parent_record_state": (
                parent_record.get("state") if parent_record is not None else None
            ),
            "parent_row_status": row_status,
            "parent_row_outcome": row_outcome,
            "parent_row_direct_mode": _safe_value(
                lambda: parent_row["payload"]["native_text_v1"]["producer_process_mode"],
            ) if isinstance(parent_row, dict) else None,
            "parent_row_identity_matches": row_identity,
            "parent_direct_route_marker": route_marker,
            "first_false_predicate": _first_false(checks),
            "_terminal_eligible": parent_record is None and _first_false(checks) is None,
            **child_receipt,
            **custody_edge,
        }

    def _capture_terminal_receipt(authority_arg, record, parent, parent_admission_id):
        snapshot, checks = _receipt_snapshot(authority_arg, record, parent)
        custody_edge = _custody_edge_snapshot(
            authority_arg, parent, parent_admission_id,
        )
        checks.extend([
            ("children_map_present", custody_edge["children_map_present"]),
            ("children_map_parent_identity", custody_edge["children_map_parent_identity"]),
            ("coordinator_present", custody_edge["coordinator_present"]),
            ("coordinator_parent_edge", custody_edge["coordinator_parent_edge"]),
        ])
        return {
            "kind": "terminal_parent_receipt",
            **snapshot,
            **custody_edge,
            "first_false_predicate": _first_false(checks),
        }

    original_parent_proof = session_classic_delivery.classic_parent_admission_proof
    original_terminal_parent_receipt = (
        session_classic_delivery._classic_terminal_parent_receipt_valid
    )

    def _observe_parent_proof(authority_arg, record):
        observation = _safe_value(
            lambda: _capture_parent_proof(authority_arg, record),
            {"kind": "parent_proof", "telemetry_error": True},
        )
        diagnostic.append(observation)
        try:
            result = original_parent_proof(authority_arg, record)
        except BaseException as exc:
            observation["validator_exception"] = type(exc).__name__
            raise
        observation["validator_result"] = result is not None
        if observation.pop("_terminal_eligible", False) and observation.get("first_false_predicate") is None:
            terminal = next(
                (
                    item for item in reversed(diagnostic)
                    if item.get("kind") == "terminal_parent_receipt"
                ),
                None,
            )
            if terminal is not None and terminal.get("first_false_predicate") is not None:
                observation["first_false_predicate"] = (
                    "terminal_parent_receipt." + terminal["first_false_predicate"]
                )
            elif result is None:
                observation["first_false_predicate"] = "unmodeled_validator_result"
        else:
            observation.pop("_terminal_eligible", None)
        return result

    def _observe_terminal_parent_receipt(authority_arg, record, parent, parent_admission_id):
        observation = _safe_value(
            lambda: _capture_terminal_receipt(
                authority_arg, record, parent, parent_admission_id,
            ),
            {"kind": "terminal_parent_receipt", "telemetry_error": True},
        )
        diagnostic.append(observation)
        try:
            result = original_terminal_parent_receipt(
                authority_arg, record, parent, parent_admission_id,
            )
        except BaseException as exc:
            observation["validator_exception"] = type(exc).__name__
            raise
        observation["validator_result"] = result is True
        return result

    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    first = MessageEvent(text="authority first", source=source, message_id="authority-inbound-1")
    second = MessageEvent(text="authority second", source=source, message_id="authority-inbound-2")
    third = MessageEvent(text="authority third", source=source, message_id="authority-inbound-3")
    # These controls belong to this real callback case; keep their state in this helper
    # rather than a neighboring fixture so closures cannot resolve an unrelated local.
    callback_fault = asyncio.Event()
    callback_task_done = asyncio.Event()
    callback_task_errors = []
    cleanup_after_callback_fault = asyncio.Event()
    third_queue_observed = asyncio.Event()
    third_queue_observation = {}
    cleanup_observation = {}
    third_model_started = asyncio.Event()
    third_delivery_started = asyncio.Event()
    third_task = None
    adapter_shutdown = False

    async def _shutdown_fault_driver():
        nonlocal adapter_shutdown
        if (
            control in {"base_callback_cancel", "base_callback_failure"}
            and not adapter_shutdown
        ):
            adapter_shutdown = True
            await adapter.cancel_background_tasks()

    def _observe_finish(event, *, success, reason="join_failed", receipt=None):
        finish_owner = getattr(event, "_classic_execution_owner_event", event)
        operation_key = (
            getattr(finish_owner, "_classic_custody_key", None)
            or getattr(event, "_classic_custody_key", None)
        )
        before = _finish_snapshot(
            event,
            operation_key=operation_key,
            success=success,
            reason=reason,
            receipt=receipt,
            phase="classic_finish_before",
        )
        tracked = before.get("record_owner_class") == "I06_busy_followup_delivery"
        finish_record = (
            authority._preservation_operations.get(operation_key)
            if isinstance(operation_key, str)
            else None
        )
        finish_admission_id = (
            finish_record.get("admission_id")
            if isinstance(finish_record, dict)
            else None
        )
        finish_bridge = getattr(finish_owner, "_classic_execution_bridge", None)
        finish_is_parented = (
            isinstance(finish_record, dict)
            and finish_record.get("parent_handle") is not None
        )
        callback_pending_before = bool(
            getattr(finish_bridge, "post_delivery_callback_pending", False)
        )
        callback_executed_before = bool(
            getattr(finish_bridge, "post_delivery_callback_executed", False)
        )
        if tracked:
            diagnostic.append(before)
        try:
            result = original_finish(event, success=success, reason=reason, receipt=receipt)
        except BaseException as exc:
            if tracked:
                after = _finish_snapshot(
                    event,
                    operation_key=operation_key,
                    success=success,
                    reason=reason,
                    receipt=receipt,
                    phase="classic_finish_after",
                )
                after["exception_type"] = type(exc).__name__
                diagnostic.append(after)
            raise
        if tracked:
            after = _finish_snapshot(
                event,
                operation_key=operation_key,
                success=success,
                reason=reason,
                receipt=receipt,
                phase="classic_finish_after",
            )
            after["original_result_type"] = type(result).__name__
            diagnostic.append(after)
            if finish_is_parented:
                finish_state = {
                    "kind": "finish",
                    "sequence": len(settlement_observations),
                    "admission_id_present": (
                        isinstance(finish_admission_id, str) and bool(finish_admission_id)
                    ),
                    "callback_pending_before": callback_pending_before,
                    "callback_executed_before": callback_executed_before,
                    "writer_parent_children_identity_before": before.get(
                        "writer_parent_children_identity"
                    ),
                    "writer_parent_coordinator_edge_before": before.get(
                        "writer_parent_coordinator_edge"
                    ),
                    "record_present_after": (
                        isinstance(operation_key, str)
                        and operation_key in authority._preservation_operations
                    ),
                }
                finish_state_by_admission[finish_admission_id] = finish_state
                settlement_observations.append(finish_state)
        owner = getattr(event, "_classic_execution_owner_event", event)
        if success and (event is second or owner is second):
            second_finished.set()
        return result

    original_handle = fixture.runner._handle_message_with_agent

    async def _hold_first(*args, **kwargs):
        candidate_event = args[0] if args else kwargs.get("event")
        if candidate_event is third:
            third_model_started.set()
        result = await original_handle(*args, **kwargs)
        if not first_processed.is_set():
            first_processed.set()
            await release_first.wait()
        return result

    def _retired_i04_handle(owner_id):
        from gateway.runtime_custody import CustodyRefusal

        coordinator = authority._preservation_coordinator()
        handle = coordinator.reserve_root(
            "I04_fifo_turn_execution", owner_id,
            profile_id=authority.profile_id, authority_epoch=authority.epoch,
        )
        assert not isinstance(handle, CustodyRefusal)
        assert coordinator.mark_terminal(handle) is None
        return handle

    def _replace_receipt_writer(owner, binding):
        receipt = session_classic_delivery._classic_receipt_for_event(owner)
        assert receipt is not None
        replacement = replace(receipt, writer_binding=binding)
        bridge = getattr(owner, "_classic_execution_bridge", None)
        execution_event = getattr(bridge, "execution_event", None)
        terminal_event = getattr(execution_event, "_classic_terminal_event", None)
        for holder in (owner, execution_event, terminal_event):
            if getattr(holder, "_classic_execution_receipt", None) is receipt:
                holder._classic_execution_receipt = replacement

    def _apply_control(control, owner, record, bridge, callback_session_key):
        """Apply one named negative-control mutation through a closed dispatch table."""
        def _foreign_callback():
            generation = getattr(bridge.execution_event, "_hermes_run_generation", None)

            async def _foreign_callback_body():
                return None

            adapter._post_delivery_callbacks[callback_session_key] = (
                generation, _foreign_callback_body,
            )

        def _generic_callback_failure():
            generation = getattr(bridge.execution_event, "_hermes_run_generation", None)

            async def _generic_callback_body():
                raise AssertionError("fixture generic callback failure")

            adapter.register_post_delivery_callback(
                callback_session_key, _generic_callback_body, generation=generation,
            )

        def _writer_parent_control(name):
            receipt = session_classic_delivery._classic_receipt_for_event(owner)
            binding = getattr(receipt, "writer_binding", None)
            assert binding is not None
            if name == "missing_execution_writer_parent":
                writer_parent = None
            elif name == "foreign_execution_writer_parent":
                writer_parent = replace(
                    binding.parent_handle,
                    owner_id=f"claim:foreign-writer-{record['admission_id']}",
                )
            elif name == "retired_execution_writer_parent":
                writer_parent = _retired_i04_handle(
                    f"claim:retired-writer-{record['admission_id']}",
                )
            else:
                writer_parent = record["parent_handle"]
            _replace_receipt_writer(owner, replace(binding, parent_handle=writer_parent))

        actions = {
            "wrong_typed_bridge": lambda: setattr(owner, "_classic_execution_bridge", object()),
            "stale_native_generation": lambda: setattr(bridge, "generation", bridge.generation + 1),
            "foreign_callback": _foreign_callback,
            "generic_callback_failure": _generic_callback_failure,
            "missing_source_parent": lambda: record.__setitem__("parent_handle", None),
            "foreign_source_parent": lambda: record.__setitem__(
                "parent_handle", replace(record["parent_handle"], profile_id="foreign-profile"),
            ),
            "retired_source_parent": lambda: record.__setitem__(
                "parent_handle", _retired_i04_handle(
                    f"claim:retired-source-{record['admission_id']}"
                ),
            ),
            "missing_execution_writer_parent": lambda: _writer_parent_control(
                "missing_execution_writer_parent"
            ),
            "foreign_execution_writer_parent": lambda: _writer_parent_control(
                "foreign_execution_writer_parent"
            ),
            "retired_execution_writer_parent": lambda: _writer_parent_control(
                "retired_execution_writer_parent"
            ),
            "wrong_execution_writer_parent": lambda: _writer_parent_control(
                "wrong_execution_writer_parent"
            ),
            "delivery_failure": lambda: None,
        }
        action = actions.get(control)
        if action is None:
            return False
        action()
        return True

    async def _observe_consumer(*args, **kwargs):
        nonlocal mutated, observed_operation_key
        from gateway.session_classic_custody import classic_execution_owner_for_event

        event = args[2] if len(args) > 2 else kwargs.get("event")
        owner = classic_execution_owner_for_event(event)
        key = getattr(owner, "_classic_custody_key", None)
        record = authority._preservation_operations.get(key)
        parented = isinstance(record, dict) and record.get("parent_handle") is not None
        callback_session_key = args[3] if len(args) > 3 else kwargs.get("session_key")
        callback_entry = adapter._post_delivery_callbacks.get(callback_session_key)
        registered = callback_entry is not None
        marker_event = kwargs.get("marker_event")
        marker_released = getattr(marker_event, "_gateway_marker_released", None)
        bridge = getattr(owner, "_classic_execution_bridge", None)
        execution_event = getattr(bridge, "execution_event", None)
        current_generation = getattr(execution_event, "_hermes_run_generation", None)
        callback_generation = (
            callback_entry[0]
            if isinstance(callback_entry, tuple) and len(callback_entry) == 2
            else None
        )
        if parented:
            observed_operation_key = key
            diagnostic.append({
                "kind": "consumer_entry",
                "marker_release_set": (
                    isinstance(marker_released, asyncio.Event) and marker_released.is_set()
                ),
                "adopter_present": (
                    getattr(authority, "marker_adopters", {}).get(record.get("admission_id"))
                    is not None
                ),
                "adopter_is_marker_event": (
                    getattr(authority, "marker_adopters", {}).get(record.get("admission_id"))
                    is marker_event
                ),
                "callback_entry_present": callback_entry is not None,
                "callback_generation_current": (
                    callback_entry is not None
                    and type(callback_generation) is int
                    and type(current_generation) is int
                    and callback_generation == current_generation
                ),
            })
        fire_before = fire_calls
        if control is not None and not mutated and parented:
            bridge = getattr(owner, "_classic_execution_bridge", None)
            mutated = _apply_control(control, owner, record, bridge, callback_session_key)
        try:
            result = await original_consumer(*args, **kwargs)
            if parented:
                consumer_results.append(result)
                callback_observations.append({
                    "registered": registered,
                    "fired": fire_calls > fire_before,
                    "fire_delta": fire_calls - fire_before,
                    "popped": callback_session_key not in adapter._post_delivery_callbacks,
                })
                diagnostic.append({
                    "kind": "consumer_exit",
                    "result": result,
                    "fire_delta": fire_calls - fire_before,
                    "callback_entry_present_after": (
                        callback_session_key in adapter._post_delivery_callbacks
                    ),
                })
            return result
        finally:
            if parented and (control is None or mutated):
                consumer_done.set()

    original_send_final_text = adapter._send_final_text

    async def _observe_send_final_text(*args, **kwargs):
        candidate_event = args[0] if args else kwargs.get("event")
        if candidate_event is third:
            third_delivery_started.set()
        return await original_send_final_text(*args, **kwargs)

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        if control == "delivery_failure" and "authority reply 2" in str(content):
            return SendResult(
                success=False,
                message_id="authority-failed",
                error="fixture_delivery_failure",
                raw_response={"failed": True},
            )
        return SendResult(success=True, message_id=f"authority-reply-{len(sent)}")

    original_fire = adapter._fire_post_delivery_callback
    original_fire_occurrence = adapter._fire_post_delivery_callback_occurrence

    def _consume_task_exception(task):
        try:
            error = task.exception()
        except BaseException as exc:  # health: allow BLE001 -- task cancellation is retained for explicit exception assertions after cleanup
            error = exc
        if error is not None:
            callback_task_errors.append(error)
        callback_task_done.set()

    async def _observe_fire(*args, **kwargs):
        nonlocal fire_calls
        fire_calls += 1
        return await original_fire(*args, **kwargs)

    async def _observe_fire_occurrence(*args, **kwargs):
        nonlocal third_task
        handle = args[2] if len(args) > 2 else kwargs.get("handle")
        consume_generic = bool(kwargs.get("consume_generic", False))
        owner = getattr(handle, "_owner_event", None)
        result = await original_fire_occurrence(*args, **kwargs)
        if (
            control in {"base_callback_cancel", "base_callback_failure"}
            and not consume_generic
            and getattr(owner, "message_id", None) == "authority-inbound-2"
        ):
            # The occurrence was consumed through the real registry before this fault is raised.
            # Admit a distinct real follow-up while the faulting occurrence still owns the session;
            # its queue/authority snapshot is captured before cleanup reconciliation begins.
            if third_task is None:
                third_task = asyncio.create_task(adapter.handle_message(third))
                await third_task
                third_records = [
                    (key, record)
                    for key, record in authority._preservation_operations.items()
                    if record.get("request_id") == "authority-inbound-3"
                ]
                second_records = [
                    (key, record)
                    for key, record in authority._preservation_operations.items()
                    if record.get("request_id") == "authority-inbound-2"
                ]
                third_key, third_record = (third_records[0] if len(third_records) == 1 else (None, {}))
                second_key, second_record = (second_records[0] if len(second_records) == 1 else (None, {}))
                third_identity = third_record.get("classic_identity")
                second_identity = second_record.get("classic_identity")
                third_queue_observation.update({
                    "accepted": third._gateway_accepted is True,
                    "pending_is_third": adapter._pending_messages.get(session_key) is third,
                    "record_count": len(third_records),
                    "queued_state": third_record.get("state") == "queued",
                    "handle_present": third_record.get("handle") is not None,
                    "owner_class": third_record.get("owner_class"),
                    "unknown": third_record.get("unknown"),
                    "operation_key": third_key,
                    "admission_id": third_record.get("admission_id"),
                    "event_key_matches": getattr(third, "_classic_custody_key", None) == third_key,
                    "identity_request_id": (
                        third_identity.get("request_id")
                        if isinstance(third_identity, dict) else None
                    ),
                    "identity_occurrence_id": (
                        third_identity.get("occurrence_id")
                        if isinstance(third_identity, dict) else None
                    ),
                    "different_operation_key": third_key is not None and third_key != second_key,
                    "different_identity": (
                        isinstance(third_identity, dict)
                        and isinstance(second_identity, dict)
                        and third_identity.get("request_id") != second_identity.get("request_id")
                        and third_identity.get("occurrence_id") != second_identity.get("occurrence_id")
                        and third_identity.get("source_identity") != second_identity.get("source_identity")
                    ),
                })
                third_queue_observed.set()
            callback_fault.set()
            current_task = asyncio.current_task()
            if current_task is not None:
                current_task.add_done_callback(_consume_task_exception)
            if control == "base_callback_cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("fixture post-delivery callback failure")
        return result

    original_finish_session_task = adapter._finish_session_task

    def _observe_finish_session_task(*args, **kwargs):
        if callback_fault.is_set():
            cleanup_observation.update({
                "third_model_started_before_reconciliation": third_model_started.is_set(),
                "third_delivery_started_before_reconciliation": third_delivery_started.is_set(),
            })
        try:
            return original_finish_session_task(*args, **kwargs)
        finally:
            if callback_fault.is_set():
                cleanup_after_callback_fault.set()

    monkeypatch.setattr(authority, "_classic_finish", _observe_finish)
    monkeypatch.setattr(authority, "_preservation_release_terminal", _observe_release_terminal)
    monkeypatch.setattr(authority, "_preservation_operation_unknown", _observe_unknown)
    monkeypatch.setattr(fixture.runner, "_handle_message_with_agent", _hold_first)
    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.setattr(adapter, "_send_final_text", _observe_send_final_text)
    monkeypatch.setattr(adapter, "_fire_post_delivery_callback", _observe_fire)
    monkeypatch.setattr(adapter, "_fire_post_delivery_callback_occurrence", _observe_fire_occurrence)
    monkeypatch.setattr(adapter, "_finish_session_task", _observe_finish_session_task)
    monkeypatch.setattr(
        session_classic_delivery,
        "classic_parent_admission_proof",
        _observe_parent_proof,
    )
    monkeypatch.setattr(
        session_classic_delivery,
        "_classic_terminal_parent_receipt_valid",
        _observe_terminal_parent_receipt,
    )
    monkeypatch.setattr(
        "gateway.session_classic_delivery.fire_authority_post_delivery_callback", _observe_consumer,
    )
    monkeypatch.setattr(delivery_ledger, "read_obligation", _observe_read_obligation)
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

    session_key = fixture.runner.session_store._generate_session_key(source)
    try:
        responses = [Text("authority reply 1"), Text("authority reply 2")]
        with FakeLLMServer(responses) as llm:
            first_call = asyncio.create_task(adapter.handle_message(first))
            await asyncio.wait_for(first_processed.wait(), 30)
            second_call = asyncio.create_task(adapter.handle_message(second))
            await asyncio.wait_for(second_call, 10)
            assert second._gateway_accepted is True
            release_first.set()
            await asyncio.wait_for(first_call, 30)
            await asyncio.wait_for(consumer_done.wait(), 30)
            if control is None:
                await asyncio.wait_for(second_finished.wait(), 30)
            elif control in {"base_callback_cancel", "base_callback_failure"}:
                await asyncio.wait_for(callback_fault.wait(), 30)
                await asyncio.wait_for(third_queue_observed.wait(), 30)
                await asyncio.wait_for(cleanup_after_callback_fault.wait(), 30)
                await asyncio.wait_for(callback_task_done.wait(), 30)
                expected_error = (
                    asyncio.CancelledError
                    if control == "base_callback_cancel"
                    else RuntimeError
                )
                assert any(isinstance(error, expected_error) for error in callback_task_errors)
                assert third_task is not None and third_task.done()
                assert third_queue_observation["accepted"] is True
                assert third_queue_observation["pending_is_third"] is True
                assert third_queue_observation["record_count"] == 1
                assert third_queue_observation["queued_state"] is True
                assert third_queue_observation["handle_present"] is True
                assert third_queue_observation["owner_class"] == "I06_busy_followup_delivery"
                assert third_queue_observation["unknown"] is False
                assert third_queue_observation["event_key_matches"] is True
                assert third_queue_observation["different_operation_key"] is True
                assert third_queue_observation["different_identity"] is True
                assert third_queue_observation["identity_request_id"] == "authority-inbound-3"
                assert third_queue_observation["identity_occurrence_id"] == "authority-inbound-3"
                assert cleanup_observation["third_model_started_before_reconciliation"] is False
                assert cleanup_observation["third_delivery_started_before_reconciliation"] is False
                settlement_observations.append({
                    "kind": "callback_fault_cleanup",
                    "task_done": callback_task_done.is_set(),
                    "error_type": type(callback_task_errors[0]).__name__,
                    "third_queue": dict(third_queue_observation),
                    "third_model_started_before_reconciliation": cleanup_observation[
                        "third_model_started_before_reconciliation"
                    ],
                    "third_delivery_started_before_reconciliation": cleanup_observation[
                        "third_delivery_started_before_reconciliation"
                    ],
                })
                await _shutdown_fault_driver()
        return (
            fixture, authority, adapter, sent, consumer_results, callback_observations,
            mutated, session_key, settlement_observations,
        )
    except BaseException:
        await _shutdown_fault_driver()
        fixture.cleanup()
        raise
    finally:
        await _shutdown_fault_driver()
        if control is None:
            print(f"native-link callback diagnostic: {diagnostic!r}")
        else:
            print(f"native-link settlement diagnostic[{control}]: {diagnostic!r}")


@pytest.mark.asyncio
async def test_real_authority_consumer_keeps_typed_parented_callback(tmp_path, monkeypatch):
    fixture = None
    try:
        fixture, authority, _adapter, sent, results, observations, _mutated, _session_key, settlement = await _run_real_authority_callback_case(
            tmp_path, monkeypatch,
        )
        assert sent == ["authority reply 1", "authority reply 2"]
        assert results == [True]
        assert observations == [{"registered": True, "fired": True, "fire_delta": 1, "popped": True}]
        assert not authority._preservation_operations
        finishes = [item for item in settlement if item.get("kind") == "finish"]
        releases = [item for item in settlement if item.get("kind") == "release_before"]
        releases_after = [item for item in settlement if item.get("kind") == "release_after"]
        assert finishes
        assert any(
            item["callback_pending_before"]
            and item["callback_executed_before"]
            and item["writer_parent_children_identity_before"]
            and item["writer_parent_coordinator_edge_before"]
            for item in finishes
        )
        assert any(
            item["child_present_before"]
            and isinstance(item.get("finish_state"), dict)
            and item["finish_state"].get("callback_pending_before")
            and item["finish_state"].get("callback_executed_before")
            and item["finish_state"].get("writer_parent_coordinator_edge_before")
            for item in releases
        )
        assert any(not item["child_present_after"] for item in releases_after)
    finally:
        if fixture is not None:
            fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control",
    ("wrong_typed_bridge", "stale_native_generation", "foreign_callback", "delivery_failure"),
)
async def test_real_authority_consumer_rejects_invalid_callback_proof(
    tmp_path, monkeypatch, control,
):
    fixture = None
    try:
        fixture, authority, adapter, _sent, results, observations, mutated, session_key, _settlement = await _run_real_authority_callback_case(
            tmp_path, monkeypatch, control,
        )
        assert mutated is True
        assert results == [False]
        assert observations == [{"registered": True, "fired": False, "fire_delta": 0, "popped": False}]
        if control == "delivery_failure":
            assert sum("authority reply 2" in content for content in _sent) >= 2
        records = [
            record for record in authority._preservation_operations.values()
            if record.get("request_id") == "authority-inbound-2"
        ]
        assert len(records) == 1
        assert records[0]["unknown"] is True
        assert session_key in adapter._post_delivery_callbacks
    finally:
        if fixture is not None:
            fixture.cleanup()


@pytest.mark.asyncio
async def test_real_authority_consumer_refuses_generic_callback_exception(
    tmp_path, monkeypatch,
):
    fixture = None
    try:
        fixture, authority, adapter, _sent, results, observations, mutated, session_key, _settlement = await _run_real_authority_callback_case(
            tmp_path, monkeypatch, "generic_callback_failure",
        )
        assert mutated is True
        assert results == [False]
        assert observations == [{"registered": True, "fired": True, "fire_delta": 1, "popped": True}]
        records = [
            record for record in authority._preservation_operations.values()
            if record.get("request_id") == "authority-inbound-2"
        ]
        assert len(records) == 1
        assert records[0]["unknown"] is True
        assert session_key not in adapter._post_delivery_callbacks
    finally:
        if fixture is not None:
            fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control",
    (
        "missing_source_parent", "foreign_source_parent", "retired_source_parent",
        "missing_execution_writer_parent", "foreign_execution_writer_parent",
        "retired_execution_writer_parent", "wrong_execution_writer_parent",
    ),
)
async def test_real_authority_consumer_rejects_invalid_parent_witness(
    tmp_path, monkeypatch, control,
):
    fixture = None
    try:
        fixture, authority, adapter, _sent, results, observations, mutated, session_key, _settlement = await _run_real_authority_callback_case(
            tmp_path, monkeypatch, control,
        )
        assert mutated is True
        assert results == [False]
        assert observations == [{"registered": True, "fired": False, "fire_delta": 0, "popped": False}]
        records = [
            record for record in authority._preservation_operations.values()
            if record.get("request_id") == "authority-inbound-2"
        ]
        assert len(records) == 1
        assert records[0]["unknown"] is True
        assert session_key in adapter._post_delivery_callbacks
    finally:
        if fixture is not None:
            fixture.cleanup()




@pytest.mark.asyncio
@pytest.mark.parametrize("control", ("base_callback_cancel", "base_callback_failure"))
async def test_real_base_finalizer_preserves_callback_fault_and_retains_pending_fifo(
    tmp_path, monkeypatch, control,
):
    """A real occurrence fault stays UNKNOWN while the real pending follow-up remains blocked."""
    fixture = None
    try:
        fixture, authority, _adapter, _sent, _results, _observations, _mutated, _session_key, settlement = await _run_real_authority_callback_case(
            tmp_path, monkeypatch, control,
        )
        records = [
            record for record in authority._preservation_operations.values()
            if record.get("request_id") == "authority-inbound-2"
        ]
        assert len(records) == 1
        assert records[0]["unknown"] is True
        cleanup = [item for item in settlement if item.get("kind") == "callback_fault_cleanup"]
        assert len(cleanup) == 1
        assert cleanup[0]["task_done"] is True
        assert cleanup[0]["third_model_started_before_reconciliation"] is False
        assert cleanup[0]["third_delivery_started_before_reconciliation"] is False
        queue = cleanup[0]["third_queue"]
        assert queue["accepted"] is True
        assert queue["pending_is_third"] is True
        assert queue["record_count"] == 1
        assert queue["queued_state"] is True
        assert queue["handle_present"] is True
        assert queue["owner_class"] == "I06_busy_followup_delivery"
        assert queue["unknown"] is False
        assert queue["event_key_matches"] is True
        assert queue["different_operation_key"] is True
        assert queue["different_identity"] is True
        assert queue["identity_request_id"] == "authority-inbound-3"
        assert queue["identity_occurrence_id"] == "authority-inbound-3"
    finally:
        if fixture is not None:
            fixture.cleanup()
