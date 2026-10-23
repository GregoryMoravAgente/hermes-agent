"""Public direct-I04 writer admission controls.

The positive case uses the canonical API admission and authority drain, real AIAgent,
and the loopback FakeLLM.  The model returns ordinary text, so no CUA owner or sidecar
is created; the test observes the exact claimed binding and the source-owned physical-tip
precheck before the typed SessionDB writer request.  The adjacent managed CUA suite retains
the physical-tip, stale, foreign, reader, and late-finalizer controls.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
async def test_public_direct_i04_text_turn_publishes_durable_writer_result(
    runtime_owned_api_owner, monkeypatch, tmp_path, compressed,
):
    """A real direct I04 API admission drains and delivers without a CUA sidecar."""
    from gateway import run_turn_runner, session_turn_physical_tip
    from gateway.session_api_turn import admit_api_turn
    from gateway.runtime_service_custody import current_turn_binding
    from hermes_state_runtime import RuntimeStoreError, get_session_admission
    from hermes_state import SessionDB
    from gateway.runtime_service_custody import ServiceCustodyError
    from gateway.session_results import admission_result
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text

    api, authority = runtime_owned_api_owner
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"terminal"})
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model",
    )
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None),
    )
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)

    request_trace: list[tuple[object, object, bool, object]] = []
    phase_trace: list[str] = []
    refusals: list[str] = []
    foreign_db = SessionDB(tmp_path / "foreign-store.db")
    original_request = run_turn_runner._request_classic_writer_proof

    def observe_request(agent, ctx, requested, binding=None):
        phase_trace.append("request")
        request_trace.append((agent, ctx, requested, binding))
        if requested and binding is not None:
            original_db, original_session = agent._session_db, agent.session_id
            try:
                agent._session_db = None
                with pytest.raises(RuntimeStoreError) as missing_db:
                    run_turn_runner._validate_direct_i04_writer_context(agent, binding)
                assert missing_db.value.reason == "preservation_owner_unmapped"
                refusals.append("missing-db")
                agent._session_db = foreign_db
                with pytest.raises(RuntimeStoreError) as foreign_store:
                    run_turn_runner._validate_direct_i04_writer_context(agent, binding)
                assert foreign_store.value.reason == "preservation_owner_unmapped"
                refusals.append("foreign-store")
                agent._session_db = original_db
                agent.session_id = "foreign-physical-session"
                with pytest.raises(RuntimeStoreError) as foreign_tip:
                    run_turn_runner._validate_direct_i04_writer_context(agent, binding)
                assert foreign_tip.value.reason == "preservation_owner_unmapped"
                refusals.append("foreign-tip")
            finally:
                agent._session_db, agent.session_id = original_db, original_session
        return original_request(agent, ctx, requested, binding)

    monkeypatch.setattr(run_turn_runner, "_request_classic_writer_proof", observe_request)
    tip_trace: list[tuple[object, str, str | None, object]] = []
    original_resolve_tip = session_turn_physical_tip.resolve_physical_tip_witness

    def observe_tip(binding, session_id, expected_store_path=None):
        witness = original_resolve_tip(binding, session_id, expected_store_path)
        phase_trace.append("tip")
        tip_trace.append((binding, session_id, expected_store_path, witness))
        return witness

    monkeypatch.setattr(
        session_turn_physical_tip, "resolve_physical_tip_witness", observe_tip,
    )
    claimed_holder: dict[str, object] = {}
    binding_holder: dict[str, object] = {}
    sent: list[str] = []
    original_handler = authority.runner._handle_message

    async def observe_claimed_handler(event):
        binding = current_turn_binding()
        assert binding is not None
        claimed = get_session_admission(
            authority.db, admission_id=admission_row["admission_id"],
        )
        assert claimed is not None and claimed["status"] == "started"
        assert claimed["generation"] == binding.execution_generation
        binding_holder["binding"] = binding
        claimed_holder["row"] = claimed
        response = await original_handler(event)
        sent.append(str(response))
        return response

    with FakeLLMServer([Text("direct I04 durable reply")]) as llm:
        monkeypatch.setattr(
            "gateway.run._resolve_runtime_agent_kwargs",
            lambda: {
                "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
                "api_mode": "chat_completions",
            },
        )
        _api, ref, admission_row = admit_api_turn(
            api,
            session_id="direct-i04-session",
            request_id="direct-i04-request",
            user_message="ordinary direct turn",
            conversation_history=[],
        )
        physical_session_id = admission_row["target_session_id"]
        if compressed:
            from tests.gateway.test_compression_tip_writer_binding import _publish_real_compression
            physical_session_id = "direct-i04-compressed-child"
            _publish_real_compression(
                authority, ref.session_id, physical_session_id, "real compression summary",
            )
            route = authority.sessions[ref.session_id].route
            assert authority.runner.session_store.advance_compression_session(
                route, ref.session_id, physical_session_id,
            )
        authority.runner._handle_message = observe_claimed_handler
        try:
            result = await asyncio.wait_for(authority._drain(ref), 30)
        finally:
            authority.runner._handle_message = original_handler
            foreign_db.close()

    assert result is None or isinstance(result, str)
    binding = binding_holder["binding"]
    claimed = claimed_holder["row"]
    assert request_trace
    requested_agent, _ctx, requested, requested_binding = request_trace[-1]
    assert requested_agent is not None
    assert requested is True
    assert requested_binding is binding
    assert requested_binding.session_id == admission_row["target_session_id"]
    assert requested_agent.session_id == physical_session_id
    assert refusals == ["missing-db", "foreign-store", "foreign-tip"]
    pre_effect_tips = [item for item in tip_trace if item[0] is binding]
    assert pre_effect_tips and pre_effect_tips[0][3] is not None
    assert pre_effect_tips[0][1] == requested_agent.session_id
    assert pre_effect_tips[0][3].logical_root == requested_binding.session_id
    assert pre_effect_tips[0][3].physical_tip == physical_session_id
    assert isinstance(pre_effect_tips[0][2], str) and pre_effect_tips[0][2]
    assert pre_effect_tips[0][2] == session_turn_physical_tip.canonical_store_path(authority)
    assert phase_trace.index("tip") < phase_trace.index("request")
    terminal = get_session_admission(authority.db, admission_id=admission_row["admission_id"])
    assert terminal is not None and terminal["status"] == "terminal"
    assert terminal["generation"] == claimed["generation"]
    saved = admission_result(authority.db, admission_row["admission_id"])
    assert isinstance(saved, dict)
    assert isinstance(saved.get("result"), dict)
    assert saved["result"].get("final_response") == "direct I04 durable reply"
    assert sent == ["direct I04 durable reply"]
    rows = authority.db.get_messages(physical_session_id)
    assert any(
        row.get("role") == "user" and row.get("content") == "ordinary direct turn"
        for row in rows
    )
    assert any(
        row.get("role") == "assistant" and row.get("content") == "direct I04 durable reply"
        for row in rows
    )
    assert all("_classic_writer_proof" not in row for row in rows)
    assert admission_row["admission_id"] not in authority.pending_results
    assert admission_row["admission_id"] not in authority.pending_deliveries
    assert not authority._preservation_operations
    assert not any(
        getattr(parent, "_cua_receipt", None) is not None
        for parent in authority.external_process_finalizers()
    )

    # The exact genuine binding becomes stale after canonical settlement; no synthetic binding.
    with pytest.raises(ServiceCustodyError) as stale:
        run_turn_runner._validate_direct_i04_writer_context(requested_agent, binding)
    assert str(stale.value) == "preservation_owner_mismatch"
