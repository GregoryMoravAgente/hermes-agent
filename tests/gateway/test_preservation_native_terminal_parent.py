"""Terminal I04 parents admit only children reserved against their live generation."""

from dataclasses import replace

import pytest

from tests.gateway.test_preservation_native_admission_link import (
    _admit_with_native_callback, _parented_event,
)


@pytest.mark.asyncio
@pytest.mark.parametrize('variant', [
    'completed', 'failed', 'detached_edge', 'copied_parent', 'stale_generation',
])
async def test_delayed_native_child_requires_completed_terminal_parent_and_live_edge(
    tmp_path, monkeypatch, variant,
):
    """A delayed I06 binds after a completed I04, but never borrows a failed turn."""
    from hermes_state_runtime import RuntimeStoreError, get_session_admission, settle_session_input
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('delayed-parent')
        authority = fixture.authority
        event, key, record = _parented_event(fixture, binding, 'delayed-child')
        parent = binding.parent_handle
        settled = settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=binding.admission_id,
            generation=binding.execution_generation,
            outcome='failed' if variant == 'failed' else 'completed',
        )
        assert settled['status'] == 'terminal'
        live = authority.sessions[binding.session_id]
        with live.event_stream.lock:
            live.event_stream.execution.clear()
        authority.executing.pop(binding.session_id, None)
        coordinator = authority._preservation_coordinator()
        assert coordinator.mark_terminal(parent).code == 'custody_pending'
        if variant == 'detached_edge':
            coordinator._children_by_parent[parent.lease.lease_id].discard(
                record['handle'].lease.lease_id)
        elif variant == 'copied_parent':
            record['parent_handle'] = replace(parent)
        elif variant == 'stale_generation':
            authority.db._execute_write(lambda conn: conn.execute(
                'UPDATE session_admissions SET generation=? WHERE admission_id=?',
                (binding.execution_generation + 1, binding.admission_id)))
        if variant == 'completed':
            receipt = await _admit_with_native_callback(fixture, event)
            row = get_session_admission(authority.db, admission_id=receipt.admission_id)
            assert row['status'] == 'queued'
            assert record['admission_id'] == receipt.admission_id
            assert record['unknown'] is False
            assert authority._preservation_children[binding.admission_id] is parent
        else:
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                await _admit_with_native_callback(fixture, event)
            assert record['unknown'] is True
    finally:
        if 'record' in locals() and variant == 'detached_edge':
            coordinator._children_by_parent.setdefault(parent.lease.lease_id, set()).add(
                record['handle'].lease.lease_id)
        elif 'record' in locals() and variant == 'copied_parent':
            record['parent_handle'] = parent
        fixture.cleanup()


@pytest.mark.asyncio
async def test_terminal_parent_cannot_reserve_a_new_classic_child(tmp_path, monkeypatch):
    """Only a child reserved while I04 was live may bind after its completion."""
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import settle_session_input
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('terminal-before-child')
        authority = fixture.authority
        source = authority.sessions[binding.session_id].source
        session_key = fixture.runner.session_store._generate_session_key(source)
        settled = settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=binding.admission_id,
            generation=binding.execution_generation, outcome='completed',
        )
        assert settled['status'] == 'terminal'
        event = MessageEvent(text='too-late', source=source, message_id='too-late')
        keys_before = set(authority._preservation_operations)
        assert authority._classic_begin(
            event, session_key, 'fifo', parent=binding.parent_handle) is False
        assert set(authority._preservation_operations) == keys_before
        assert not hasattr(event, '_classic_custody_key')
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_classic_child_refuses_when_parent_row_read_fails(tmp_path, monkeypatch):
    """A store failure cannot turn an unverified I04 parent into an I06 lease."""
    import sqlite3

    from gateway.platforms.event import MessageEvent
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('parent-read-failure')
        authority = fixture.authority
        source = authority.sessions[binding.session_id].source
        session_key = fixture.runner.session_store._generate_session_key(source)
        event = MessageEvent(text='refuse', source=source, message_id='refuse')
        keys_before = set(authority._preservation_operations)

        def failed_read(*args, **kwargs):
            raise sqlite3.OperationalError('synthetic read failure')

        monkeypatch.setattr(
            'gateway.session_classic_parent_generation.get_session_admission', failed_read,
        )
        assert authority._classic_begin(
            event, session_key, 'fifo', parent=binding.parent_handle) is False
        assert set(authority._preservation_operations) == keys_before
        assert not hasattr(event, '_classic_custody_key')
    finally:
        fixture.cleanup()
