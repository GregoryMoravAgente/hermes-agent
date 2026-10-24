"""Shared real-I04 setup for managed startup/cancellation tests."""

async def real_managed_claim(tmp_path, monkeypatch, request_id, *, include_worker_runtime=False):
    """Return a real claimed row and its authenticated managed-turn binding."""
    from gateway.session_contract import SessionRef
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(
        tmp_path, monkeypatch, include_probe=False,
        include_worker_runtime=include_worker_runtime,
    )
    binding = await fixture.claim(request_id)
    authority = fixture.authority
    row = get_session_admission(authority.db, admission_id=binding.admission_id)
    assert row is not None
    ref = SessionRef(binding.profile_id, binding.session_id)
    live = authority.sessions[binding.session_id]
    return fixture, binding, authority, ref, row, live


async def real_managed_queue(tmp_path, monkeypatch, request_id, *, include_worker_runtime=False):
    """Queue a real native admission so the production drain performs the claim/settlement."""
    import os
    from gateway.platforms.event import MessageEvent
    from gateway.session_contract import Principal, Submission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(
        tmp_path, monkeypatch, include_probe=False,
        include_worker_runtime=include_worker_runtime,
    )
    if include_worker_runtime:
        from gateway.session_local import create_local_session

        local_principal = f"uid:{os.getuid()}"
        actor = Principal(
            local_principal, fixture.authority.profile_id,
            frozenset({'session:create', 'session:submit', 'session:control'}),
            'managed-worker-fixture',
        )
        ref = create_local_session(fixture.authority, actor, {
            'request_id': f'worker-session-{request_id}', 'source': 'cli',
            'cwd': str(tmp_path), 'model': 'm', 'toolsets': [],
        })
        receipt = await fixture.authority.submit(
            actor, Submission(request_id, ref, {'text': request_id}, 'queue'),
        )
        assert receipt.status == 'queued'
        return fixture, fixture.authority, receipt
    source = fixture.adapter.build_source(
        chat_id="15551234567@s.whatsapp.net",
        user_id="15551234567@s.whatsapp.net",
        chat_type="dm",
    )
    event = MessageEvent(text=request_id, source=source, message_id=request_id)
    receipt = await fixture.admit_native_with_source_handoff(event)
    assert receipt.status == "queued"
    return fixture, fixture.authority, receipt
