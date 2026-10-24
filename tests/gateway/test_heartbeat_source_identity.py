"""Strict heartbeat source identities are retained across the I08 pending-root boundary."""

from dataclasses import replace
import json
from pathlib import Path

import pytest


def _wire_local_heartbeat_owner(owner, tmp_path, monkeypatch, request_id):
    from gateway.config import Platform
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_contract import Principal
    from gateway.session_local import LocalSessionAdapter, create_local_session
    from hermes_cli.backend_retirement import RetirementFence

    monkeypatch.setenv('HERMES_HOME', str(Path(owner.db.db_path).parent))
    adapter = LocalSessionAdapter(owner)
    owner.runner.adapters = {Platform.LOCAL: adapter}
    owner.runner._adapter_for_source = lambda _source: adapter
    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id,
        authority_epoch=owner.epoch,
        fence=RetirementFence(),
    )
    owner._schedule = lambda _ref: None
    actor = Principal(
        'heartbeat-source-test', owner.profile_id,
        frozenset({'session:create', 'session:read', 'session:submit'}), 'fixture',
    )
    ref = create_local_session(owner, actor, {
        'request_id': request_id,
        'source': 'cli',
        'model': 'fixture',
        'toolsets': [],
        'cwd': str(owner.db.db_path.parent),
    })
    live = owner.sessions[ref.session_id]
    return adapter, ref, live


def _reserve_heartbeat_root(owner, ref, live, request_id):
    from gateway.platforms.event import MessageEvent
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_goal_tick_custody import reserve_tick

    event = MessageEvent(
        text='heartbeat source identity',
        source=live.source,
        internal=True,
        message_id=request_id,
        metadata={
            'gateway_session_key': live.route,
            'gateway_session_id': ref.session_id,
        },
    )
    source_identity = automation_source_identity(event, live.route)
    tick = reserve_tick(
        owner,
        request_id=request_id,
        principal_id='automation:' + live.route,
        session_id=owner.logical_owner(ref.session_id),
        source_identity=source_identity,
        kind='heartbeat',
        watcher_key=live.route,
    )
    assert tick is not None and tick.handle is not None
    return event, source_identity, tick.handle


def _finish_root(owner, handle, request_id):
    coordinator = owner._preservation_coordinator()
    coordinator.mark_terminal(handle)
    owner._preservation_forget_pending_root(handle)
    owner._preservation_automation_roots.pop(request_id, None)


@pytest.mark.asyncio
async def test_heartbeat_admission_binds_event_source_identity(owner, tmp_path, monkeypatch):
    """The public heartbeat producer publishes the same identity the authority recomputes."""
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from gateway.session_automation_custody import automation_source_identity
    from hermes_cli.heartbeat import HeartbeatState, save_heartbeat

    session_request_id = 'heartbeat-source-session'
    adapter, ref, live = _wire_local_heartbeat_owner(
        owner, tmp_path, monkeypatch, session_request_id,
    )
    request_id = json.dumps(['heartbeat', ref.session_id, 1.0, 1], separators=(',', ':'))
    save_heartbeat(ref.session_id, HeartbeatState(
        prompt='heartbeat source identity', interval_seconds=60, created_at=1,
    ))
    captured = {}

    async def observe(_adapter, event, identity, *, _preservation_handle=None):
        captured.update(event=event, identity=identity, handle=_preservation_handle)

    monkeypatch.setattr(owner, 'admit_automation', observe)
    await admit_heartbeat(owner.runner, adapter, live.source, ref.session_id, live.route)

    event = captured['event']
    handle = captured['handle']
    assert captured['identity'] == request_id
    assert handle is not None
    pending = owner._preservation_pending_roots[id(handle)]
    assert pending['handle'] is handle
    assert pending['source_identity'] == automation_source_identity(event, live.route)
    _finish_root(owner, handle, request_id)


@pytest.mark.asyncio
async def test_heartbeat_pending_source_identity_tamper_refuses_without_release(
        owner, tmp_path, monkeypatch,
):
    from hermes_state_runtime import RuntimeStoreError

    request_id = json.dumps(['heartbeat', 'heartbeat-tamper', 1, 1], separators=(',', ':'))
    _adapter, ref, live = _wire_local_heartbeat_owner(owner, tmp_path, monkeypatch, request_id)
    _event, source_identity, handle = _reserve_heartbeat_root(owner, ref, live, request_id)
    pending = owner._preservation_pending_roots[id(handle)]
    pending['source_identity'] = 'foreign-source-identity'
    try:
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            owner._preservation_pending_root(
                request_id,
                handle,
                principal_id='automation:' + live.route,
                session_id=owner.logical_owner(ref.session_id),
                source_identity=source_identity,
            )
        record = owner._preservation_coordinator()._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == 'accepted'
        assert owner._preservation_pending_roots[id(handle)]['handle'] is handle
    finally:
        _finish_root(owner, handle, request_id)


@pytest.mark.asyncio
async def test_heartbeat_pending_source_identity_rejects_copied_and_missing_root(
        owner, tmp_path, monkeypatch,
):
    from hermes_state_runtime import RuntimeStoreError

    request_id = json.dumps(['heartbeat', 'heartbeat-missing', 1, 1], separators=(',', ':'))
    _adapter, ref, live = _wire_local_heartbeat_owner(owner, tmp_path, monkeypatch, request_id)
    _event, source_identity, handle = _reserve_heartbeat_root(owner, ref, live, request_id)
    copied = replace(handle, owner_id='copied-heartbeat-root')
    try:
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            owner._preservation_pending_root(
                request_id,
                copied,
                principal_id='automation:' + live.route,
                session_id=owner.logical_owner(ref.session_id),
                source_identity=source_identity,
            )
        owner._preservation_pending_roots.pop(id(handle), None)
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            owner._preservation_pending_root(
                request_id,
                handle,
                principal_id='automation:' + live.route,
                session_id=owner.logical_owner(ref.session_id),
                source_identity=source_identity,
            )
        record = owner._preservation_coordinator()._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == 'accepted'
    finally:
        _finish_root(owner, handle, request_id)
