"""Public API retired-session fences, including the admission-time retirement race.

The production fixture runs the real GatewayRunner/APIServerAdapter path.  Only the provider
boundary is the repository's schema-valid ``AcpFake`` child, exactly as in the existing managed
ACP consumer.  These tests keep the retired-session checks at both the HTTP preflight and the
canonical SessionDB admission transaction.
"""
from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from gateway.session_api_turn import _admit_target
from hermes_state_runtime import RuntimeStoreError, list_session_admissions
from tests.agent.test_copilot_acp_managed_production import (
    API_KEY,
    HERMES_SESSION_ID,
    _api_app,
    _create_session,
    production_runtime as _production_runtime,
)


production_runtime = _production_runtime


@pytest.mark.asyncio
async def test_public_chat_refuses_retired_target_before_body_and_provider_effects(production_runtime):
    """The public route rejects the durable retired row before parsing or running a turn."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        retired = await client.patch(
            f"/api/sessions/{HERMES_SESSION_ID}",
            json={"end_reason": "test-retired-target"},
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        assert retired.status == 200, await retired.text()
        row_before = runtime.authority.db.get_session(HERMES_SESSION_ID)
        assert row_before and row_before["ended_at"] is not None
        admissions_before = list_session_admissions(
            runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False,
        )
        messages_before = runtime.authority.db.get_messages(HERMES_SESSION_ID)
        settings_before = runtime.authority.db.get_meta(
            f"gateway.api.settings.v1.{HERMES_SESSION_ID}"
        )
        spawns_before = len(runtime.fake.events("spawn"))

        # Invalid JSON proves the retirement fence runs before body parsing; it must still return
        # the canonical retired-target refusal.  No provider child or admission may be created.
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            data=b"not-json",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "X-Hermes-Session-Key": "hermes-api-key-retired-target-20261019",
                "Content-Type": "application/json",
            },
        )
        body = await response.json()

    assert response.status == 409, body
    assert body["error"]["code"] == "session_retired", body
    assert len(runtime.fake.events("spawn")) == spawns_before
    assert list_session_admissions(
        runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False,
    ) == admissions_before
    assert runtime.authority.db.get_messages(HERMES_SESSION_ID) == messages_before
    assert runtime.authority.db.get_meta(
        f"gateway.api.settings.v1.{HERMES_SESSION_ID}"
    ) == settings_before
    row_after = runtime.authority.db.get_session(HERMES_SESSION_ID)
    assert row_after["ended_at"] == row_before["ended_at"]
    assert row_after["end_reason"] == "test-retired-target"


@pytest.mark.asyncio
async def test_admission_transaction_refuses_target_retired_after_api_preflight(production_runtime):
    """The final real SQLite admission transaction closes retirement after a live preflight."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)

    # This is the API preflight's durable read.  Retire in a separate committed transaction before
    # calling the exact admission callback, reproducing the only interleaving the callback must fence.
    preflight = runtime.authority.db.get_session(HERMES_SESSION_ID)
    assert preflight and preflight["ended_at"] is None
    runtime.authority.db.end_session(HERMES_SESSION_ID, "race-retired-target")
    spawns_before = len(runtime.fake.events("spawn"))
    settings_key = f"gateway.api.settings.v1.{HERMES_SESSION_ID}"

    with pytest.raises(RuntimeStoreError, match="admission_conflict"):
        runtime.authority.db._execute_write(
            lambda conn: _admit_target(
                HERMES_SESSION_ID,
                "race-request-retired-target-20261019",
                {"requested_model": "fake-model-managed-acp"},
                conn,
                client_key=False,
            )
        )

    assert len(runtime.fake.events("spawn")) == spawns_before
    assert list_session_admissions(
        runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False,
    ) == []
    assert runtime.authority.db.get_meta(settings_key) is None
    retired = runtime.authority.db.get_session(HERMES_SESSION_ID)
    assert retired and retired["ended_at"] is not None
    assert retired["end_reason"] == "race-retired-target"
