"""Actual ACP result delivery retains the exact API response owner until settlement."""

import asyncio
from dataclasses import replace
import json

import pytest

from tests.agent import test_copilot_acp_managed_production as controls


production_runtime = controls.production_runtime


@pytest.mark.asyncio
async def test_public_acp_delivery_rejects_replaced_pending_response_owner(
    production_runtime, monkeypatch,
):
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms import api_server_run_completion as completions

    runtime = production_runtime
    observed = []
    original = completions.can_stage_api_sidecar_delivery

    def observe_preflight(authority, admission_id, receipt):
        accepted = original(authority, admission_id, receipt)
        assert accepted is True
        completion = completions.completion_for(authority, admission_id)
        waiter = authority.waiters[admission_id]
        assert completion.response_waiter is waiter
        assert completion.response_task is completion.custody.worker_task
        assert waiter.done() is False
        assert completion.receipt is None

        # These synchronous probes do not finalize a service or acknowledge a result.
        # Restore the original source objects before the real finalizer continues.
        foreign_waiter = asyncio.get_running_loop().create_future()
        authority.waiters[admission_id] = foreign_waiter
        try:
            assert original(authority, admission_id, receipt) is False
        finally:
            authority.waiters[admission_id] = waiter
            foreign_waiter.cancel()
        assert original(authority, admission_id, replace(
            receipt, admission_id="foreign-admission",
        )) is False
        registry = authority._api_finalizer_completions
        assert registry.pop(admission_id) is completion
        try:
            assert original(authority, admission_id, receipt) is False
        finally:
            registry[admission_id] = completion
        assert original(authority, admission_id, receipt) is True
        observed.append(completion)
        return accepted

    monkeypatch.setattr(completions, "can_stage_api_sidecar_delivery", observe_preflight)
    async with TestClient(TestServer(controls._api_app(runtime.adapter, runtime.runner))) as client:
        await controls._create_session(client, controls.HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{controls.HERMES_SESSION_ID}/chat",
            json={"message": controls.FIRST_PROMPT, "provider": "copilot-acp", "model": controls.MODEL},
            headers={"Authorization": f"Bearer {controls.API_KEY}",
                     "X-Hermes-Session-Key": controls.HERMES_SESSION_KEY},
        )
        body = await response.text()
        assert response.status == 200, body
        assert json.loads(body)["message"]["content"] == controls.FIRST_REPLY

    assert len(observed) == 1
    completion = observed[0]
    assert completion.response_waiter.done() is True
    assert completion.receipt.state == "complete"
    assert completion.receipt.waiter_delivered is True
    assert completion.custody.released is True
    assert completion.custody.unknown_reason is None


@pytest.mark.asyncio
async def test_failed_acp_result_refuses_replaced_parent_and_unknown_receipt(
    production_runtime, tmp_path, monkeypatch,
):
    from gateway.platforms import api_server_run_completion as completions

    # Exercise the actual public ACP failure with every original custody assertion.
    await controls.test_public_child_failure_rejects_copied_prefix_alias_without_new_child(
        production_runtime, tmp_path, monkeypatch,
    )
    authority = production_runtime.authority
    [completion] = list(authority._api_finalizer_completions.values())
    custody = completion.custody
    original_receipt = completion.receipt
    assert original_receipt.state == "unknown"
    assert custody.finalizer_completion_receipt is original_receipt
    assert completion.future.result() is original_receipt
    released_before = custody.released
    assert released_before is True
    def read_result():
        return completions.retained_failed_api_result(custody, active_run_id=None)

    failed_result = read_result()
    assert failed_result is not None
    assert failed_result[0]["failed"] is True
    assert read_result() == failed_result

    # Synchronous read-only refusal probes restore the actual source objects before
    # any finalizer or release can run. They do not reconcile UNKNOWN custody.
    handles = authority._preservation_handles
    parent = handles[completion.admission_id]
    assert parent is completion.parent
    handles[completion.admission_id] = replace(parent)
    try:
        assert read_result() is None
    finally:
        handles[completion.admission_id] = parent
    assert handles.pop(completion.admission_id) is parent
    try:
        assert read_result() is None
    finally:
        handles[completion.admission_id] = parent
    completion._receipt = replace(original_receipt)
    try:
        assert read_result() is None
    finally:
        completion._receipt = original_receipt
    assert read_result() == failed_result
    assert completion.receipt is original_receipt
    assert custody.finalizer_completion_receipt is original_receipt
    assert custody.released is released_before
    assert custody.coordinator.is_active_handle(
        parent, owner_class="I02_api_session_turn",
        profile_id=custody.profile_id, authority_epoch=custody.authority_epoch,
    )
    assert custody.unknown_reason
