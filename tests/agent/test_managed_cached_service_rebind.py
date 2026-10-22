"""Real managed-service close guards for cached ACP client reuse."""

from __future__ import annotations

import sys

import pytest
from aiohttp.test_utils import TestClient, TestServer

from agent.copilot_acp_client import CopilotACPClient
from gateway.runtime_service_operations import close_after_finalizers
from tests.agent.test_copilot_acp_failure_retention import _HOLD_CHILD, _managed_client
from tests.agent.test_copilot_acp_managed_production import (
    FIRST_PROMPT, FIRST_REPLY, HERMES_SESSION_ID, _api_app, _cached_agent, _chat,
    _create_session, production_runtime as _production_runtime,
)


production_runtime = _production_runtime


@pytest.mark.asyncio
async def test_busy_managed_service_cannot_be_retired_by_cached_client_close(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "cached-service-close-busy",
    )
    try:
        client._refresh_closed_state()
        assert client.is_closed is False
        assert service.requests.get(binding.admission_id) is request
        assert close_after_finalizers(parent) is False
        assert parent.state == "request_reserved"
        assert service.requests.get(binding.admission_id) is request
        client.close()
        assert parent.state == "request_reserved"
        assert service.requests.get(binding.admission_id) is request
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_stale_managed_client_cannot_retire_replaced_owner(tmp_path, monkeypatch):
    """An old cached client cannot terminalize the exact service now owned by its replacement."""
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "cached-service-stale-owner",
    )
    replacement = None
    try:
        replacement = CopilotACPClient(
            command=sys.executable, args=["-c", _HOLD_CHILD], acp_cwd=str(tmp_path),
            producer_policy=client._producer_policy,
        )
        assert parent._catalog_client is replacement
        client.close()
        assert client.is_closed is True
        assert parent._catalog_client is replacement
        assert parent.state == "request_reserved"
        assert service.requests.get(binding.admission_id) is request
        assert close_after_finalizers(parent) is False
        assert parent.state == "request_reserved"
        assert service.requests.get(binding.admission_id) is request
    finally:
        if replacement is not None:
            replacement.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_settled_managed_delivery_refreshes_cached_client(production_runtime):
    """A real public turn keeps its cached client open only after every lease map is clear."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, FIRST_PROMPT)
        assert response.status == 200, body
        assert body["message"]["content"] == FIRST_REPLY, body
        cached = _cached_agent(runtime.runner, HERMES_SESSION_ID)
        policy_parent = getattr(getattr(cached, "_external_process_policy", None), "parent", None)
        assert policy_parent is not None
        service = policy_parent._service
        assert policy_parent.state == "request_reserved"
        assert policy_parent._physical is None
        assert service.physical is None
        assert service.requests == {}
        assert service.durable_requests == set()
        assert service.delivery_unknown_reasons == {}
        assert service.delivery_terminalized == set()
        assert service.unknown is False
        actual_client = policy_parent._catalog_client
        assert policy_parent._owner_agent is cached
        assert actual_client is cached._managed_acp_client
        assert actual_client._producer_policy.parent is policy_parent
        assert actual_client.is_closed is False
