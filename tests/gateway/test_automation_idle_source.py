"""Causal coverage for the source-owned I07 idle/zero-work close witness."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3

import pytest

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


async def _admit_real_i07_terminal(runner, authority, tmp_path, monkeypatch):
    """Create one real automation admission and settle its durable terminal result."""
    from gateway.platforms.event import MessageEvent
    from gateway.session_contract import Principal
    from gateway.session_local import _bypass_policy, create_local_session
    from hermes_state_runtime import claim_session_input, list_session_admissions, settle_session_input

    # The explicit bypass policy keeps this synthetic fixture independent of profile credentials;
    # the admission/descriptor/claim/result path remains the real source-owned route.
    from hermes_cli import tools_config
    monkeypatch.setattr(tools_config, '_xai_credentials_present', lambda: False)
    actor = Principal(
        'i07-fixture', authority.profile_id,
        frozenset({'session:create', 'session:submit', 'session:read'}), 'fixture',
    )
    policy = _bypass_policy(
        {'source': 'gui', 'cwd': str(tmp_path), 'model': 'fixture',
         'provider': 'custom', 'toolsets': []},
        private_secrets={},
    )
    ref = create_local_session(
        authority, actor,
        {'request_id': 'i07-active-session', 'source': 'gui', 'cwd': str(tmp_path),
         'model': 'fixture', 'toolsets': []},
        trusted_policy=policy,
    )
    live = authority.sessions[ref.session_id]
    route = runner.session_store._generate_session_key(live.source)
    entry = runner.session_store.lookup_by_session_key(route)
    assert entry is not None
    event = MessageEvent(
        text='i07 active terminal', source=live.source, internal=True,
        message_id='i07-active-terminal',
        metadata={'gateway_session_key': route, 'gateway_session_id': entry.session_id},
    )
    receipt = await authority.admit_automation(
        runner._adapter_for_source(live.source), event, event.message_id,
    )
    rows = list_session_admissions(
        authority.db, session_id=ref.session_id, pending_only=True,
    )
    row = next(row for row in rows if row['admission_id'] == receipt.admission_id)
    started = claim_session_input(
        authority.db, epoch=authority.epoch, session_id=ref.session_id,
        head=row['admission_id'],
    )
    assert isinstance(started, dict) and started['status'] == 'started'
    settle_session_input(
        authority.db, epoch=authority.epoch, admission_id=row['admission_id'],
        generation=started['generation'], outcome='completed',
        result={'final_response': 'i07 fixture result', 'messages': []},
    )
    authority._preservation_finish_automation(
        row['admission_id'], finalizer_id='i07-fixture-finalizer',
    )
    assert not authority._preservation_handles
    assert not authority._preservation_automation_bindings
    return row['admission_id']


async def _real_pending_i08_route_wake(tmp_path, monkeypatch):
    """Build one real local route and its source-owned I08 pending root."""
    from gateway.platforms.event import MessageEvent
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_contract import Principal
    from gateway.session_goal_tick_custody import reserve_tick
    from gateway.session_local import _bypass_policy, create_local_session
    from hermes_cli import tools_config

    monkeypatch.setattr(tools_config, '_xai_credentials_present', lambda: False)
    runner, entries = await _boot(tmp_path, monkeypatch)
    authority = runner.session_authorities.launch
    actor = Principal(
        'i08-fixture', authority.profile_id,
        frozenset({'session:create', 'session:submit', 'session:read'}), 'fixture',
    )
    policy = _bypass_policy(
        {'source': 'gui', 'cwd': str(tmp_path), 'model': 'fixture',
         'provider': 'custom', 'toolsets': []},
        private_secrets={},
    )
    ref = create_local_session(
        authority, actor,
        {'request_id': 'i08-pending-session', 'source': 'gui', 'cwd': str(tmp_path),
         'model': 'fixture', 'toolsets': []},
        trusted_policy=policy,
    )
    live = authority.sessions[ref.session_id]
    entry = runner.session_store.lookup_by_session_key(live.route)
    assert entry is not None
    source_event = MessageEvent(
        text='', source=live.source, internal=True, message_id='i08-source-identity',
    )
    request_id = 'i08-pending-root'
    tick = reserve_tick(
        authority,
        request_id=request_id,
        principal_id='automation:' + entry.session_key,
        session_id=authority.logical_owner(entry.session_id),
        source_identity=automation_source_identity(source_event, live.route),
        kind='loop',
        watcher_key=entry.session_key,
    )
    assert tick is not None and tick.handle is not None
    return runner, entries, authority, live, tick.handle, request_id


def _forget_pending_i08_route(authority, handle, request_id):
    coordinator = authority._preservation_coordinator()
    if coordinator is not None:
        coordinator.mark_terminal(handle)
    authority._preservation_forget_pending_root(handle)
    authority._preservation_automation_roots.pop(request_id, None)


@pytest.mark.asyncio
async def test_two_profile_idle_automation_source_is_wired_at_close(tmp_path, monkeypatch):
    """A real multiplexed boot must close an untouched I07 source without fake traffic."""
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        launch = runner.session_authorities.launch
        secondary = runner.session_authorities.for_home(entries[1][1])
        assert secondary is not None
        assert secondary.epoch != launch.epoch

        coordinator = runner.runtime_custody
        receipt = coordinator.begin_close(owner_id="i07-idle-source")
        assert receipt.state == "closing"
        assert launch.preservation_owner_ready("I07_automation_route_wakes") is True
        assert secondary.preservation_owner_ready("I07_automation_route_wakes") is True

        from gateway.session_automation_custody import preservation_owner_evidence

        for authority in (launch, secondary):
            evidence = preservation_owner_evidence(
                authority,
                process_instance=receipt.process_instance,
                authority_epoch=receipt.authority_epoch,
                close_generation=receipt.close_generation,
                owner_id="i07-idle-source",
            )
            assert evidence["source_snapshot"]["mode"] == "zero_work"
            assert evidence["source_snapshot"]["zero_work"] is True
            assert evidence["source_snapshot"]["authority_epoch"] == authority.epoch

        from gateway.run_runtime import _seal_integrated_preservation_owners

        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        assert coordinator.status()["owner_states"]["I07_automation_route_wakes"] == "sealed"
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_route_wake_reuses_exact_pending_request_identity(tmp_path, monkeypatch):
    """A retained source handle uses its reservation identity, not a new route digest."""
    from gateway.session_automation import admit_route_wake

    runner, entries, authority, live, handle, request_id = await _real_pending_i08_route_wake(
        tmp_path, monkeypatch,
    )
    seen = []

    async def capture(_adapter, _event, identity, **_kwargs):
        seen.append(identity)

    monkeypatch.setattr(authority, 'admit_automation', capture)
    try:
        assert await admit_route_wake(
            runner, runner._adapter_for_source(live.source), live.source,
            'loop tick', 'loop-tick', _preservation_handle=handle,
        ) is True
        assert seen == [request_id]
    finally:
        _forget_pending_i08_route(authority, handle, request_id)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_route_wake_refuses_tampered_pending_request_id_without_release(tmp_path, monkeypatch):
    from gateway.session_automation import admit_route_wake
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    runner, entries, authority, live, handle, request_id = await _real_pending_i08_route_wake(
        tmp_path, monkeypatch,
    )
    pending = authority._preservation_pending_roots[id(handle)]
    entry = runner.session_store.lookup_by_session_key(live.route)
    assert entry is not None
    before = list_session_admissions(authority.db, session_id=entry.session_id, pending_only=False)
    pending['request_id'] = 'i08-forged-request'
    try:
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            await admit_route_wake(
                runner, runner._adapter_for_source(live.source), live.source,
                'loop tick', 'loop-tick', _preservation_handle=handle,
            )
        assert list_session_admissions(
            authority.db, session_id=entry.session_id, pending_only=False,
        ) == before
        assert authority._preservation_automation_roots[request_id] is handle
        assert authority._preservation_coordinator()._records[handle.lease.lease_id].state == 'accepted'
    finally:
        pending['request_id'] = request_id
        _forget_pending_i08_route(authority, handle, request_id)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_route_wake_refuses_tampered_pending_root_without_release(tmp_path, monkeypatch):
    from gateway.session_automation import admit_route_wake
    from hermes_state_runtime import RuntimeStoreError

    runner, entries, authority, live, handle, request_id = await _real_pending_i08_route_wake(
        tmp_path, monkeypatch,
    )
    pending = authority._preservation_pending_roots[id(handle)]
    pending['handle'] = object()
    try:
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            await admit_route_wake(
                runner, runner._adapter_for_source(live.source), live.source,
                'loop tick', 'loop-tick', _preservation_handle=handle,
            )
        assert authority._preservation_pending_roots[id(handle)]['handle'] is not handle
        assert authority._preservation_coordinator()._records[handle.lease.lease_id].state == 'accepted'
    finally:
        pending['handle'] = handle
        _forget_pending_i08_route(authority, handle, request_id)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_route_wake_refuses_missing_pending_root_without_effect(tmp_path, monkeypatch):
    from gateway.session_automation import admit_route_wake
    from hermes_state_runtime import RuntimeStoreError
    from hermes_state_runtime import list_session_admissions

    runner, entries, authority, live, handle, request_id = await _real_pending_i08_route_wake(
        tmp_path, monkeypatch,
    )
    authority._preservation_pending_roots.pop(id(handle), None)
    try:
        entry = runner.session_store.lookup_by_session_key(live.route)
        assert entry is not None
        before = list_session_admissions(authority.db, session_id=entry.session_id, pending_only=False)
        with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
            await admit_route_wake(
                runner, runner._adapter_for_source(live.source), live.source,
                'loop tick', 'loop-tick', _preservation_handle=handle,
            )
        after = list_session_admissions(authority.db, session_id=entry.session_id, pending_only=False)
        assert after == before
        assert authority._preservation_coordinator()._records[handle.lease.lease_id].state == 'accepted'
    finally:
        _forget_pending_i08_route(authority, handle, request_id)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_idle_source_refuses_replaced_index_and_read_failure(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        receipt = coordinator.begin_close(owner_id="i07-idle-source-negative")
        original = authority._preservation_automation_roots
        authority._preservation_automation_roots = {}
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
        authority._preservation_automation_roots = original

        original_profile_id = authority.profile_id
        authority.profile_id = f"{original_profile_id}-foreign"
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
        authority.profile_id = original_profile_id
        original_epoch = authority.epoch
        authority.epoch = original_epoch + 1
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
        authority.epoch = original_epoch

        @contextmanager
        def failing_read():
            raise sqlite3.OperationalError("synthetic source read failure")
            yield

        monkeypatch.setattr(authority.db, "_read_ctx", failing_read)
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_active_source_seals_only_from_real_terminal_receipt(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        admission_id = await _admit_real_i07_terminal(runner, authority, tmp_path, monkeypatch)
        receipt = coordinator.begin_close(owner_id='i07-active-source')
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is True

        from gateway.session_automation_custody import preservation_owner_evidence
        evidence = preservation_owner_evidence(
            authority,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id='i07-active-source',
        )
        snapshot = evidence['source_snapshot']
        assert snapshot['mode'] == 'active_terminal'
        assert snapshot['terminal_receipts'][0]['admission_id'] == admission_id
        assert snapshot['terminal_receipts'][0]['result_present'] is True
        assert snapshot['profile_id'] == authority.profile_id
        assert snapshot['authority_epoch'] == authority.epoch
        assert snapshot['process_instance'] == coordinator.process_instance
        assert isinstance(snapshot['physical_owner_epoch'], str)
        assert snapshot['close_generation'] == receipt.close_generation
        from gateway.run_runtime import _seal_integrated_preservation_owners
        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        assert coordinator.status()['owner_states']['I07_automation_route_wakes'] == 'sealed'

        # The outer receipt is part of the same identity tuple; a copied process or close
        # generation cannot authorize the source even after its real terminal row exists.
        assert preservation_owner_evidence(
            authority, process_instance='foreign-process',
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id='i07-active-source',
        ) is None
        assert preservation_owner_evidence(
            authority, process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation + 1,
            owner_id='i07-active-source',
        ) is None
    finally:
        if receipt is not None and runner.runtime_custody.status()['state'] == 'closing':
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_active_seen_bit_and_replaced_graph_remain_unknown(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        authority._route_wake_inbound_seen = True
        receipt = coordinator.begin_close(owner_id='i07-active-bit-only')
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is False
        from gateway.session_automation_custody import preservation_owner_evidence
        assert preservation_owner_evidence(
            authority,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id='i07-active-bit-only',
        ) is None
        assert coordinator.status()['owner_states']['I07_automation_route_wakes'] == 'unwired'
    finally:
        authority._route_wake_inbound_seen = False
        if receipt is not None and runner.runtime_custody.status()['state'] == 'closing':
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_seen_only_secondary_refuses_after_valid_terminal_source(
        tmp_path, monkeypatch,
):
    """A valid terminal source on one profile cannot bless a seen bit on another profile."""
    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        launch = runner.session_authorities.launch
        secondary = runner.session_authorities.for_home(entries[1][1])
        assert secondary is not None
        await _admit_real_i07_terminal(runner, launch, tmp_path, monkeypatch)
        assert launch.preservation_owner_ready('I07_automation_route_wakes') is True

        secondary._route_wake_inbound_seen = True
        assert secondary.preservation_owner_ready('I07_automation_route_wakes') is False
    finally:
        if secondary is not None:
            secondary._route_wake_inbound_seen = False
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_active_terminal_refuses_replaced_source_index_and_owner_read(
        tmp_path, monkeypatch,
):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    original_roots = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        await _admit_real_i07_terminal(runner, authority, tmp_path, monkeypatch)
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is True
        original_roots = authority._preservation_automation_roots
        authority._preservation_automation_roots = {}
        receipt = coordinator.begin_close(owner_id='i07-active-index')
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is False
        authority._preservation_automation_roots = original_roots

        from gateway.runtime_ownership import process_ownership
        monkeypatch.setattr(process_ownership, 'reservation_evidence', lambda _home: None)
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is False
    finally:
        if original_roots is not None:
            authority._preservation_automation_roots = original_roots
        if receipt is not None and runner.runtime_custody.status()['state'] == 'closing':
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_idle_source_refuses_released_and_reacquired_native_owner(
        tmp_path, monkeypatch,
):
    """A close witness keeps the owner bound at graph registration, not the latest lock."""
    from gateway.runtime_ownership import process_ownership

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        home = authority.profile_id
        bound = process_ownership.reservation_evidence(home)
        assert isinstance(bound, dict)
        bound_epoch = bound['owner_epoch']

        receipt = coordinator.begin_close(owner_id='i07-native-owner-replacement')
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is True

        assert process_ownership.release(home) is True
        process_ownership.reserve([home])
        replacement = process_ownership.reservation_evidence(home)
        assert isinstance(replacement, dict)
        assert replacement['owner_epoch'] != bound_epoch
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is False
    finally:
        if receipt is not None and runner.runtime_custody.status()['state'] == 'closing':
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_i07_case_alias_keeps_physical_owner_and_rejects_replacement(
        tmp_path, monkeypatch,
):
    """A case-only NTFS alias is physical evidence; a new owner incarnation is not."""
    from gateway.runtime_ownership import canonical_home, process_ownership
    from gateway.run_runtime import _strict_physical_owner_bindings
    from gateway.session_automation_custody import _i07_native_reservation

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        home = Path(authority.profile_id)
        alias = Path("".join(char.swapcase() if char.isalpha() else char for char in str(home)))
        assert str(alias) != str(home)
        assert canonical_home(alias) == canonical_home(home)

        evidence = process_ownership.reservation_evidence(alias)
        assert evidence is not None
        assert evidence["profile_home"] == str(canonical_home(home))
        assert _strict_physical_owner_bindings(
            [evidence], [("default", alias)],
        ) == ((str(canonical_home(home)), evidence["owner_epoch"], evidence["native_descriptor"]),)
        assert _i07_native_reservation(alias) is not None

        receipt = coordinator.begin_close(owner_id="i07-windows-case-alias")
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is True
        bound_epoch = evidence["owner_epoch"]

        assert process_ownership.release(alias) is True
        process_ownership.reserve([alias])
        replacement = process_ownership.reservation_evidence(home)
        assert replacement is not None
        assert replacement["owner_epoch"] != bound_epoch
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_post_fence_root_refuses_before_payload_or_row(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        with authority.db._read_ctx() as conn:
            before = conn.execute("SELECT COUNT(*) FROM session_admissions").fetchone()[0]
        receipt = coordinator.begin_close(owner_id="i07-idle-source-fence")
        from hermes_state_runtime import RuntimeStoreError

        with pytest.raises(RuntimeStoreError, match="runtime_draining"):
            authority._preservation_automation_root(
                request_id="late-i07-root",
                principal_id="automation:late",
                session_id="late-session",
                source_identity="late-source",
            )
        with authority.db._read_ctx() as conn:
            after = conn.execute("SELECT COUNT(*) FROM session_admissions").fetchone()[0]
        assert after == before
        assert authority._route_wake_inbound_seen is False
        assert authority._preservation_automation_roots == {}
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i07_preclose_accepted_root_and_unknown_retain_same_handle(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    handle = None
    request_id = "preclose-i07-root"
    try:
        authority = runner.session_authorities.launch
        coordinator = runner.runtime_custody
        handle = authority._preservation_automation_root(
            request_id=request_id,
            principal_id="automation:preclose",
            session_id="preclose-session",
            source_identity="preclose-source",
        )
        assert handle.owner_class == "I07_automation_route_wakes"
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
        receipt = coordinator.begin_close(owner_id="i07-preclose-root")
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
        assert coordinator.mark_unknown(handle, "read_failed").code == "read_failed"
        record = coordinator._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == "unknown"
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        if handle is not None:
            assert coordinator.reconcile_unknown(
                handle,
                owner_id=handle.owner_id,
                profile_id=authority.profile_id,
                process_instance=authority.instance_id,
                authority_epoch=authority.epoch,
            ) is None
            assert coordinator.mark_terminal(handle) is None
            authority._preservation_forget_pending_root(handle)
            assert authority._preservation_automation_roots.pop(request_id, None) is handle
        assert await _close_boot(runner, entries) is True
