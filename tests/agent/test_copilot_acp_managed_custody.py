"""Managed ACP custody controls across the real admission, child, and SessionDB writer seams.

The child is an actual ACP-shaped subprocess and the writer is the normal SessionPersistenceMixin
against the authority's real SessionDB.  The test stops at the durable-writer fence: delivery and
native close remain the existing Gateway finalizer's responsibility, so a child response cannot
silently release the retained service lease.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from agent.copilot_acp_client import CopilotACPClient
from gateway.runtime_service_custody import reserve_service
from gateway.runtime_service_operations import ProducerPolicy, parent_for_managed_service, provider_process_kwargs
from tests.agent.transports.test_codex_physical_custody import _RealProofAgent
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


_ACP_SCRIPT = (
    "import json,sys\n"
    "for line in sys.stdin:\n"
    " m=json.loads(line); method=m.get('method'); rid=m.get('id')\n"
    " if method == 'initialize': out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
    " elif method == 'session/new': out={'jsonrpc':'2.0','id':rid,'result':{'sessionId':'provider-acp-session'}}\n"
    " elif method == 'session/prompt':\n"
    "  sid=m['params']['sessionId']; note={'jsonrpc':'2.0','method':'session/update','params':{'sessionId':sid,'update':{'sessionUpdate':'agent_message_chunk','content':{'text':'managed answer'}}}}; print(json.dumps(note),flush=True); out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
    " else: out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
    " print(json.dumps(out),flush=True)\n"
)

_EARLY_EXIT_ACP_SCRIPT = (
    "import json,sys\n"
    "for line in sys.stdin:\n"
    " m=json.loads(line); method=m.get('method'); rid=m.get('id')\n"
    " if method == 'initialize': out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
    " elif method == 'session/new':\n"
    "  out={'jsonrpc':'2.0','id':rid,'result':{'sessionId':'provider-acp-session'}}; print(json.dumps(out),flush=True); raise SystemExit(7)\n"
    " else: out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
    " print(json.dumps(out),flush=True)\n"
)


def test_process_route_marker_is_authoritative_and_unknown_is_rejected():
    """The persisted producer marker selects managed custody; absent legacy rows never grant it."""
    from gateway.runtime_service_custody import ServiceCustodyError
    from gateway.session_ingress import _managed_process_route

    assert _managed_process_route({
        "payload": {"api_turn_v1": {"producer_process_mode": "managed_turn"}}
    }) is True
    # Missing/legacy rows remain the direct route; an external producer still reaches its
    # existing preservation_owner_unmapped fence instead of receiving managed ownership.
    assert _managed_process_route({"payload": {"api_turn_v1": {}}}) is False
    with pytest.raises(ServiceCustodyError, match="producer_policy_invalid"):
        _managed_process_route({
            "payload": {"api_turn_v1": {"producer_process_mode": "client_selected"}}
        })


@pytest.mark.asyncio
async def test_managed_acp_response_requires_real_sessiondb_proof_before_delivery(tmp_path, monkeypatch):
    """A real ACP child may stage only after the canonical writer readback, retaining physical custody."""
    from gateway.runtime_service_custody import bind_turn, reset_turn

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    client = None
    binding_token = None
    try:
        binding = await fixture.claim("managed-acp-proof")
        service = reserve_service(binding, "service:managed-acp")
        parent = parent_for_managed_service(binding, service, operation_id=binding.admission_id)
        request = service.begin_request(binding)
        fixture.authority._preservation_bind_sidecar(
            binding.admission_id, binding.parent_handle, service, request,
        )

        writer = _RealProofAgent(fixture.authority.db, binding.session_id)
        policy = ProducerPolicy("managed_turn", parent, True, False, False)
        writer._external_process_policy = policy
        kwargs = provider_process_kwargs(
            writer, SimpleNamespace(auth_type="external_process"), {"base_url": "acp://managed"},
        )
        assert kwargs["producer_policy"] is policy
        client = CopilotACPClient(
            command=sys.executable, args=["-c", _ACP_SCRIPT],
            acp_cwd=str(tmp_path), producer_policy=policy,
        )

        text, _reasoning = client._run_prompt("answer", timeout_seconds=5)
        assert text == "managed answer"
        assert parent._physical is not None
        receipt = parent._physical
        assert receipt.state in {"process_bound", "readers_active"}
        assert service.durable_requests == set()
        assert client.has_pending_managed_witness()
        witness = next(
            item for item in client._pending_managed_witnesses
            if item.get("provider_session_id") == "provider-acp-session"
        )
        assert witness["provider_session_id"] == "provider-acp-session"
        assert witness["hermes_session_id"] == binding.session_id
        assert witness["provider_session_id"] != witness["hermes_session_id"]

        writer._managed_acp_client = client
        writer._current_turn_id = "managed-acp-turn"
        binding_token = bind_turn(binding)
        assert writer._request_gateway_writer_proof() is True
        user_message = {"role": "user", "content": "answer"}
        message = {"role": "assistant", "content": text}
        # The canonical writer must commit the current user row and assistant result in
        # one batch so the durable proof has an actual admission anchor and ordered suffix.
        assert writer._flush_messages_to_session_db([user_message, message]) is True
        assert user_message["_row_id"] > 0
        assert message["_row_id"] > user_message["_row_id"]
        rows = fixture.authority.db.get_messages(binding.session_id)
        assert [(row["role"], row.get("_row_id") or row.get("id")) for row in rows[-2:]] == [
            ("user", user_message["_row_id"]), ("assistant", message["_row_id"]),
        ]
        assert service.durable_requests == {binding.admission_id}
        assert parent._physical is receipt
        assert receipt.completed_requests == set()
        assert receipt.durable_proofs
        proof = writer._managed_shell_writer_proof
        assert proof is not None
        physical_witness = writer.__dict__["_classic_writer_proof_physical_witness"]
        assert physical_witness.client is client
        assert physical_witness.parent is parent
        assert physical_witness.service is service
        assert physical_witness.request is service.requests[binding.admission_id]
        assert physical_witness.binding is binding
        assert physical_witness.receipt is receipt
        assert physical_witness.durable_proof is receipt.managed_witness["durable_proof"]

        row_count = len(rows)
        assert writer._flush_messages_to_session_db([user_message, message]) is True
        assert writer._managed_shell_writer_proof is proof
        assert len(fixture.authority.db.get_messages(binding.session_id)) == row_count

        # Replacing the receipt-local provider witness must stop before a second writer and
        # retain the original managed owner as unknown for its same-handle retry.
        receipt.managed_witness = {**receipt.managed_witness, "provider_session_id": "replaced"}
        assert writer._flush_messages_to_session_db([user_message, message]) is False
        assert len(fixture.authority.db.get_messages(binding.session_id)) == row_count
        assert writer._managed_persistence_failed is True
        assert parent.state == "unknown"
    finally:
        if binding_token is not None:
            reset_turn(binding_token)
        if client is not None:
            client.close()
        fixture.cleanup()


async def _managed_acp_replay_case(tmp_path, monkeypatch, *, prime=True):
    """Create one real managed ACP/SessionDB writer case for mutation controls."""
    from gateway.runtime_service_custody import bind_turn

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    binding_token = None
    client = None
    try:
        binding = await fixture.claim("managed-acp-replay-matrix")
        service = reserve_service(binding, "service:managed-acp-replay")
        parent = parent_for_managed_service(binding, service, operation_id=binding.admission_id)
        request = service.begin_request(binding)
        fixture.authority._preservation_bind_sidecar(
            binding.admission_id, binding.parent_handle, service, request,
        )

        writer = _RealProofAgent(fixture.authority.db, binding.session_id)
        policy = ProducerPolicy("managed_turn", parent, True, False, False)
        writer._external_process_policy = policy
        kwargs = provider_process_kwargs(
            writer, SimpleNamespace(auth_type="external_process"), {"base_url": "acp://managed"},
        )
        assert kwargs["producer_policy"] is policy
        assert parent._owner_agent is writer
        client = CopilotACPClient(
            command=sys.executable, args=["-c", _ACP_SCRIPT],
            acp_cwd=str(tmp_path), producer_policy=policy,
        )
        text, _reasoning = client._run_prompt("matrix answer", timeout_seconds=5)
        assert text == "managed answer"
        writer._managed_acp_client = client
        writer._current_turn_id = "managed-acp-replay-turn"
        binding_token = bind_turn(binding)
        assert writer._request_gateway_writer_proof() is True
        user_message = {"role": "user", "content": "matrix answer"}
        result_message = {"role": "assistant", "content": text}
        messages = [user_message, result_message]
        initial_row_count = len(fixture.authority.db.get_messages(
            binding.session_id, include_inactive=True,
        ))
        proof = None
        physical_witness = None
        if prime:
            assert writer._flush_messages_to_session_db(messages) is True
            proof = writer._managed_shell_writer_proof
            physical_witness = writer.__dict__["_classic_writer_proof_physical_witness"]
            assert proof is not None
            assert physical_witness.client is client
            assert physical_witness.writer_proof is proof
            assert physical_witness.durable_proof is parent._physical.managed_witness["durable_proof"]
        return {
            "fixture": fixture,
            "binding": binding,
            "binding_token": binding_token,
            "client": client,
            "writer": writer,
            "parent": parent,
            "service": service,
            "request": request,
            "receipt": parent._physical,
            "witness": parent._physical.managed_witness,
            "messages": messages,
            "proof": proof,
            "physical_witness": physical_witness,
            "initial_row_count": initial_row_count,
            "row_count": len(fixture.authority.db.get_messages(binding.session_id, include_inactive=True)),
        }
    except (AssertionError, AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        if binding_token is not None:
            from gateway.runtime_service_custody import reset_turn
            reset_turn(binding_token)
        if client is not None:
            client.close()
        fixture.cleanup()
        raise


async def _managed_acp_failure_recovery_case(tmp_path, monkeypatch):
    """Create an owner-bound managed client whose real child exits before its prompt."""
    from gateway.runtime_service_custody import bind_turn

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    binding_token = None
    try:
        binding = await fixture.claim("managed-acp-recovery")
        service = reserve_service(binding, "service:managed-acp-recovery")
        parent = parent_for_managed_service(binding, service, operation_id=binding.admission_id)
        request = service.begin_request(binding)
        fixture.authority._preservation_bind_sidecar(
            binding.admission_id, binding.parent_handle, service, request,
        )
        writer = _RealProofAgent(fixture.authority.db, binding.session_id)
        policy = ProducerPolicy("managed_turn", parent, True, False, False)
        writer._external_process_policy = policy
        kwargs = provider_process_kwargs(
            writer, SimpleNamespace(auth_type="external_process"), {"base_url": "acp://managed"},
        )
        assert kwargs["producer_policy"] is policy
        assert parent._owner_agent is writer
        client = CopilotACPClient(
            command=sys.executable, args=["-c", _EARLY_EXIT_ACP_SCRIPT],
            acp_cwd=str(tmp_path), producer_policy=policy,
        )
        writer._managed_acp_client = client
        writer._current_turn_id = "managed-acp-recovery-turn"
        binding_token = bind_turn(binding)
        assert writer._request_gateway_writer_proof() is True
        return {
            "fixture": fixture, "binding": binding, "binding_token": binding_token,
            "client": client, "writer": writer, "parent": parent, "service": service,
            "request": request,
        }
    except (AssertionError, AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        if binding_token is not None:
            from gateway.runtime_service_custody import reset_turn
            reset_turn(binding_token)
        fixture.cleanup()
        raise


@pytest.mark.asyncio
async def test_managed_acp_same_owner_process_retry_retires_stale_writer_proof(tmp_path, monkeypatch):
    """A real failed child may recover its exact receipt before a new writer is admitted."""
    ctx = await _managed_acp_failure_recovery_case(tmp_path, monkeypatch)
    writer = ctx["writer"]
    client = ctx["client"]
    parent = ctx["parent"]
    service = ctx["service"]
    binding = ctx["binding"]
    try:
        with pytest.raises((OSError, RuntimeError, TimeoutError)):
            client._run_prompt("recovery answer", timeout_seconds=5)
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt.state == "unknown"
        assert receipt.process is not None
        assert parent.state == "unknown"
        assert client.has_pending_managed_witness()
        assert any(item.get("receipt") is receipt and item.get("_failure_reason")
                   for item in client._pending_managed_witnesses)

        messages = [
            {"role": "user", "content": "recovery answer"},
            {"role": "assistant", "content": "failed child answer"},
        ]
        assert writer._flush_messages_to_session_db(messages) is False
        retained = writer._managed_shell_writer_proof
        assert retained is not None
        assert writer.__dict__["_classic_writer_proof_unknown"] is True
        handle = receipt.handle

        assert client._retry_managed_process_failure(parent, service, binding, ctx["request"]) is True
        assert parent._physical is receipt and service.physical is receipt
        assert receipt.handle is handle
        assert receipt.state == "reserved" and receipt.retry_ready is True
        assert receipt.process is None
        assert getattr(writer, "_managed_shell_writer_proof", None) is None
        assert writer.__dict__.get("_classic_writer_proof_unknown") is None
        assert writer._managed_persistence_failed is False

        client._acp_args = ["-c", _ACP_SCRIPT]
        writer._current_turn_id = "managed-acp-recovered-turn"
        assert writer._request_gateway_writer_proof() is True
        text, _reasoning = client._run_prompt("recovered answer", timeout_seconds=5)
        assert text == "managed answer"
        recovered_messages = [
            {"role": "user", "content": "recovered answer"},
            {"role": "assistant", "content": text},
        ]
        assert writer._flush_messages_to_session_db(recovered_messages) is True
        assert writer._managed_shell_writer_proof is not retained
        assert parent._physical is receipt
        assert receipt.managed_client is client
        assert receipt.managed_witness["provider_session_id"] == "provider-acp-session"
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        client.close()
        ctx["fixture"].cleanup()


def _managed_acp_mutate(ctx, mutation):
    """Apply one real receipt/binding identity mutation and return its exact restoration."""
    receipt = ctx["receipt"]
    parent = ctx["parent"]
    service = ctx["service"]
    witness = ctx["witness"]
    admission_id = ctx["binding"].admission_id
    if mutation == "clear_physical_receipt":
        previous = parent._physical
        parent._physical = None
        return lambda: setattr(parent, "_physical", previous)
    if mutation == "replace_physical_receipt":
        previous = parent._physical
        parent._physical = object()
        return lambda: setattr(parent, "_physical", previous)
    if mutation == "replace_service_physical":
        previous = service.physical
        service.physical = object()
        return lambda: setattr(service, "physical", previous)
    if mutation == "clear_client":
        previous = receipt.managed_client
        receipt.managed_client = None
        return lambda: setattr(receipt, "managed_client", previous)
    if mutation == "clear_managed_witness":
        previous = receipt.managed_witness
        receipt.managed_witness = None
        return lambda: setattr(receipt, "managed_witness", previous)
    if mutation == "replace_provider":
        previous = witness["provider_session_id"]
        witness["provider_session_id"] = "replacement-provider"
        return lambda: witness.__setitem__("provider_session_id", previous)
    if mutation == "replace_durable_proof":
        previous = witness["durable_proof"]
        witness["durable_proof"] = object()
        return lambda: witness.__setitem__("durable_proof", previous)
    if mutation == "replace_binding_request":
        previous = service.requests[admission_id]
        service.requests[admission_id] = object()
        return lambda: service.requests.__setitem__(admission_id, previous)
    if mutation == "replace_admission":
        previous = witness["admission_id"]
        witness["admission_id"] = "replacement-admission"
        return lambda: witness.__setitem__("admission_id", previous)
    if mutation == "replace_generation":
        previous = witness["execution_generation"]
        witness["execution_generation"] = previous + 1
        return lambda: witness.__setitem__("execution_generation", previous)
    if mutation == "replace_owner_identity":
        previous = parent._owner_agent
        parent._owner_agent = object()
        return lambda: setattr(parent, "_owner_agent", previous)
    raise AssertionError(f"unknown managed ACP mutation: {mutation}")


def _exercise_changed_continuation(ctx, writer, control):
    """Apply one changed-turn control without a long name-based branch in the test."""
    def changed_turn():
        original = writer._current_turn_id
        writer._current_turn_id = "managed-acp-next-turn"
        try:
            assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        finally:
            writer._current_turn_id = original

    def request_reset():
        assert writer._request_gateway_writer_proof() is False

    def pending_witness():
        original_pending = tuple(ctx["client"]._pending_managed_witnesses)
        ctx["client"]._pending_managed_witnesses.append({
            "provider_session_id": "late-provider", "_failure_reason": "",
        })
        try:
            assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        finally:
            ctx["client"]._pending_managed_witnesses.clear()
            ctx["client"]._pending_managed_witnesses.extend(original_pending)

    def new_rows():
        fresh = {"role": "assistant", "content": "uncommitted replacement"}
        ctx["messages"].append(fresh)
        try:
            assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        finally:
            ctx["messages"].pop()

    def writer_error():
        writer._managed_persistence_failed = True
        assert writer._flush_messages_to_session_db(ctx["messages"]) is False

    actions = {
        "changed_turn": changed_turn,
        "request_reset": request_reset,
        "pending_witness": pending_witness,
        "new_rows": new_rows,
        "writer_error": writer_error,
    }
    actions[control]()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        "clear_physical_receipt", "replace_physical_receipt", "replace_service_physical",
        "clear_client", "clear_managed_witness", "replace_provider", "replace_durable_proof",
        "replace_binding_request", "replace_admission", "replace_generation", "replace_owner_identity",
    ),
)
async def test_managed_acp_replay_retains_unknown_owner_without_a_second_writer(
    tmp_path, monkeypatch, mutation,
):
    """Every receipt/provider/binding mutation refuses before publication and preserves the token."""
    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch)
    restore = _managed_acp_mutate(ctx, mutation)
    try:
        writer = ctx["writer"]
        assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == ctx["row_count"]
        assert writer._managed_shell_writer_proof is ctx["proof"]
        assert writer.__dict__["_classic_writer_proof_physical_witness"] is ctx["physical_witness"]
        assert ctx["parent"].state == "unknown"
    finally:
        restore()
    # A later call sees the sticky unknown marker and cannot fall through to a fresh writer.
    try:
        assert ctx["writer"]._flush_messages_to_session_db(ctx["messages"]) is False
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == ctx["row_count"]
        assert ctx["writer"]._managed_shell_writer_proof is ctx["proof"]
        # Finalizer-style request discard must not open the plain fallback writer while the
        # original managed owner remains unknown.
        from agent.session_persistence_proof import discard_gateway_writer_proof
        discard_gateway_writer_proof(ctx["writer"])
        assert ctx["writer"]._flush_messages_to_session_db(ctx["messages"]) is False
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == ctx["row_count"]
        assert ctx["writer"]._managed_shell_writer_proof is ctx["proof"]
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        ctx["client"].close()
        ctx["fixture"].cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("capture", "remember"))
async def test_managed_acp_post_effect_failure_retains_exact_unknown_token(
    tmp_path, monkeypatch, failure,
):
    """A failed post-commit handoff retains the proof/token and blocks a second writer."""
    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch, prime=False)
    writer = ctx["writer"]
    if failure == "capture":
        monkeypatch.setattr(
            ctx["client"], "capture_managed_durable_receipt_witness",
            lambda *_args: None,
        )
    else:
        import agent.session_persistence_proof as proof_module

        monkeypatch.setattr(proof_module, "remember_gateway_writer_proof", lambda *_args: False)
    try:
        assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        rows_after_failure = len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        ))
        assert rows_after_failure > ctx["initial_row_count"]
        proof = writer._managed_shell_writer_proof
        assert proof is not None
        assert getattr(writer, "_classic_writer_proof_unknown", False) is True
        if failure == "remember":
            assert writer.__dict__["_classic_writer_proof_physical_witness"] is not None
        recovered = writer._flush_messages_to_session_db(ctx["messages"])
        # A retained receipt witness may now recover through the authenticated same-owner
        # path.  A capture failure without a retained witness remains sticky unknown.
        assert recovered is (failure == "remember")
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == rows_after_failure
        assert writer._managed_shell_writer_proof is proof
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        ctx["client"].close()
        ctx["fixture"].cleanup()


@pytest.mark.asyncio
async def test_managed_acp_postcommit_same_owner_recovery_reuses_exact_proof_without_write(
    tmp_path, monkeypatch,
):
    """A transient post-commit handoff failure recovers the same proof and receipt only."""
    import agent.session_persistence_proof as proof_module

    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch, prime=False)
    writer = ctx["writer"]
    client = ctx["client"]
    messages = ctx["messages"]
    calls = 0
    original_remember = proof_module.remember_gateway_writer_proof

    def fail_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return False
        return original_remember(*args)

    monkeypatch.setattr(proof_module, "remember_gateway_writer_proof", fail_once)
    try:
        assert writer._flush_messages_to_session_db(messages) is False
        proof = writer._managed_shell_writer_proof
        receipt = ctx["receipt"]
        row_count = len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        ))
        assert proof is not None
        assert calls == 1
        assert writer.__dict__["_classic_writer_proof_unknown"] is True
        assert writer.__dict__["_classic_writer_proof_physical_witness"] is not None
        assert ctx["service"].unknown is False
        assert ctx["parent"].state not in {"unknown", "terminal"}

        # This is the real retry entrypoint used by the next canonical flush.  It must
        # recapture and revalidate the original receipt, then reuse the same writer proof.
        assert writer._flush_messages_to_session_db(messages) is True
        assert writer._managed_shell_writer_proof is proof
        assert writer.__dict__.get("_classic_writer_proof_unknown") is None
        assert writer._managed_persistence_failed is False
        assert ctx["service"].unknown is False
        assert receipt.durable_proofs
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == row_count

        assert writer._flush_messages_to_session_db(messages) is True
        assert writer._managed_shell_writer_proof is proof
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == row_count
        from agent.session_persistence_proof import publish_gateway_writer_proof

        handoff = publish_gateway_writer_proof(
            writer, messages, proof, writer._current_turn_id,
        )
        assert handoff is not None
        assert handoff.writer_proof.row_ids == proof.row_ids
        assert handoff.writer_proof.result_row_ids == proof.row_ids[1:]
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        client.close()
        ctx["fixture"].cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("stale_provider", "copied_witness"))
async def test_managed_acp_postcommit_recovery_rejects_stale_or_copied_witness(
    tmp_path, monkeypatch, mutation,
):
    """Recovery remains sticky unknown when the retained receipt witness changes."""
    import agent.session_persistence_proof as proof_module

    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch, prime=False)
    writer = ctx["writer"]
    client = ctx["client"]
    original_remember = proof_module.remember_gateway_writer_proof
    monkeypatch.setattr(proof_module, "remember_gateway_writer_proof", lambda *_args: False)
    try:
        assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        proof = writer._managed_shell_writer_proof
        receipt = ctx["receipt"]
        row_count = len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        ))
        retained_witness = writer.__dict__["_classic_writer_proof_physical_witness"]
        assert proof is not None and retained_witness is not None
        if mutation == "stale_provider":
            receipt.managed_witness["provider_session_id"] = "stale-provider"
        else:
            receipt.managed_witness = dict(receipt.managed_witness)

        assert writer._flush_messages_to_session_db(ctx["messages"]) is False
        assert writer._managed_shell_writer_proof is proof
        assert writer.__dict__["_classic_writer_proof_unknown"] is True
        assert ctx["parent"].state == "unknown"
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == row_count
    finally:
        monkeypatch.setattr(proof_module, "remember_gateway_writer_proof", original_remember)
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        client.close()
        ctx["fixture"].cleanup()


@pytest.mark.asyncio
async def test_managed_acp_unchanged_replay_is_idempotent_without_a_second_db_write(tmp_path, monkeypatch):
    """An unchanged managed receipt reuses the exact proof and raw row count."""
    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch)
    try:
        assert ctx["writer"]._flush_messages_to_session_db(ctx["messages"]) is True
        assert ctx["writer"]._managed_shell_writer_proof is ctx["proof"]
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == ctx["row_count"]
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        ctx["client"].close()
        ctx["fixture"].cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("control", ("changed_turn", "request_reset", "pending_witness", "new_rows", "writer_error"))
async def test_managed_acp_changed_continuation_fails_closed_without_old_publication(
    tmp_path, monkeypatch, control,
):
    """Turn/request/pending/new-row/error changes never re-adopt the old managed publication."""
    ctx = await _managed_acp_replay_case(tmp_path, monkeypatch)
    writer = ctx["writer"]
    try:
        _exercise_changed_continuation(ctx, writer, control)
        assert len(ctx["fixture"].authority.db.get_messages(
            ctx["binding"].session_id, include_inactive=True,
        )) == ctx["row_count"]
        assert writer._managed_shell_writer_proof is ctx["proof"]
    finally:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(ctx["binding_token"])
        ctx["client"].close()
        ctx["fixture"].cleanup()
