"""Real source-close and abort controls for I09/I17 preservation ownership."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from tests.gateway.test_hosted_mux_runtime import mux as _hosted_mux_fixture


def test_booted_i09_source_catalog_observes_real_producer_graph(started_mux, monkeypatch):
    from gateway.runtime_custody import DrainReceipt
    from gateway.session_hosted_service import (
        _i09_source_catalog,
        abort_i09_source_close,
        await_i09_source_close,
        begin_i09_source_close,
        preservation_i09_owner_evidence,
    )
    import inspect

    runner, _homes, _loop, call = started_mux
    authority = runner.session_authority

    async def invoke(function, args, kwargs):
        result = function(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    def sync(function, *args, **kwargs):
        return call(invoke(function, args, kwargs))

    catalog = sync(_i09_source_catalog, runner)
    assert catalog is not None and not catalog.no_producer
    assert len(catalog.rows) == len(runner.session_authorities)
    row = next(item for item in catalog.rows if item['authority'] is authority)
    assert row['profile_id'] == authority.profile_id
    assert row['authority_epoch'] == authority.epoch
    assert row['i17_registry'] is authority._i17_producer_registry
    assert row['room_factory']['authority'] is authority
    assert row['room_factory']['runner'] is runner
    assert row['control_present'] is True
    assert row['service_present'] is True
    assert type(row['physical_owner_epoch']) is str and row['physical_owner_epoch']
    assert row['active_i09_operations'] == 0

    coordinator = runner.runtime_custody
    receipt = sync(coordinator.begin_close, owner_id='i09-booted-source-test', deadline_s=10)
    assert isinstance(receipt, DrainReceipt), receipt
    control = runner.session_control_server
    try:
        assert sync(begin_i09_source_close, runner, receipt.close_generation)
        assert sync(await_i09_source_close, runner, receipt.close_generation, 10)
        closed_catalog = sync(_i09_source_catalog, runner)
        assert closed_catalog is not None and not closed_catalog.no_producer
        assert all(
            row['i17_registry']['intake_closed'] is True
            for row in closed_catalog.rows
        )
        evidence = sync(preservation_i09_owner_evidence,
            authority,
            process_instance=authority.instance_id,
            authority_epoch=authority.epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{authority.instance_id}:I09_a2a_hosted_kanban',
        )
        assert evidence is not None
        assert evidence.get('configured_absent') is not True
        assert evidence['source_catalog']
        assert evidence['source_catalog_digest'] == closed_catalog.digest

        # A booted producer graph without the canonical control source is incomplete evidence.
        monkeypatch.setattr(runner, 'session_control_server', None, raising=False)
        assert sync(preservation_i09_owner_evidence,
            authority,
            process_instance=authority.instance_id,
            authority_epoch=authority.epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{authority.instance_id}:I09_a2a_hosted_kanban',
        ) is None
        monkeypatch.setattr(runner, 'session_control_server', control, raising=False)
    finally:
        monkeypatch.setattr(runner, 'session_control_server', control, raising=False)
        assert sync(abort_i09_source_close, runner, receipt.close_generation)
        reopened_catalog = sync(_i09_source_catalog, runner)
        assert reopened_catalog is not None and not reopened_catalog.no_producer
        assert all(
            row['i17_registry']['intake_closed'] is False
            for row in reopened_catalog.rows
        )
        assert sync(coordinator.abort_before_handoff, receipt) is None


@pytest.mark.asyncio
async def test_init_only_i09_absence_closes_intake_before_configured_absence(
    tmp_path, monkeypatch,
):
    from gateway.session_hosted_service import (
        _i09_source_catalog,
        abort_i09_source_close,
        await_i09_source_close,
        begin_i09_source_close,
        ensure_hosted_service,
        preservation_i09_owner_evidence,
    )
    from hermes_state_runtime import RuntimeStoreError
    from tests.gateway.conftest import runtime_owned_api

    async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
        runner = adapter.gateway_runner
        authority = runner.session_authority
        catalog = _i09_source_catalog(runner)
        assert catalog is not None and catalog.no_producer
        assert authority._i17_producer_registry['intake_closed'] is False

        receipt = runner.runtime_custody.begin_close(
            owner_id='i09-init-only-absence', deadline_s=10,
        )
        assert receipt is not None
        assert begin_i09_source_close(runner, receipt.close_generation)
        assert authority._i17_producer_registry['intake_closed'] is True
        assert await await_i09_source_close(runner, receipt.close_generation, 10)
        evidence = preservation_i09_owner_evidence(
            authority,
            process_instance=authority.instance_id,
            authority_epoch=authority.epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{authority.instance_id}:I09_a2a_hosted_kanban',
        )
        assert evidence is not None and evidence['configured_absent'] is True
        with pytest.raises(RuntimeStoreError, match='hosted_source_closed'):
            await ensure_hosted_service(runner)
        assert getattr(authority, 'hosted_room_service', None) is None
        assert abort_i09_source_close(runner, receipt.close_generation)
        assert authority._i17_producer_registry['intake_closed'] is False
        assert runner.runtime_custody.abort_before_handoff(receipt) is None


def test_legacy_source_abort_retains_generation_and_token(tmp_path):
    from gateway.preservation_source_close import (
        SourceCloseContract,
        abort_source_close,
        begin_source_close,
        ensure_source_close,
        register_source_adapter,
        source_admit,
    )

    class Adapter:
        platform = SimpleNamespace(value='synthetic-source')

        def __init__(self, runner):
            self.gateway_runner = runner
            self._preservation_resume_source = lambda _generation: False

        def handle_message(self, _event):
            return None

        def _preservation_source_contract(self):
            return SourceCloseContract.for_adapter(self, name='synthetic-source', supported=True)

    runner = SimpleNamespace()
    adapter = Adapter(runner)
    register_source_adapter(runner, adapter, profile='default')
    token = source_admit(
        adapter, marker='accepted-before-close', event=SimpleNamespace(), profile='default'
    )
    assert token is not None
    registry = ensure_source_close(runner)
    assert begin_source_close(runner, 31)
    assert not abort_source_close(runner, 31)
    assert registry.state == 'closing'
    assert registry.close_generation == 31
    assert token in registry.active_leases
    assert token.close_generation == 31
    assert token.identity[-1] == 31

    def resume_source(generation):
        if getattr(adapter, '_preservation_source_close_generation', None) != generation:
            return False
        adapter._preservation_source_close_generation = None
        return True

    adapter._preservation_resume_source = resume_source
    assert abort_source_close(runner, 31)
    assert registry.state == 'open'
    assert registry.close_generation is None
    assert token in registry.active_leases
    assert token.close_generation is None
    assert token.identity[-1] is None


def test_hosted_abort_recloses_all_real_services_after_partial_failure(
    request, monkeypatch,
):
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.session_authorities import serving_authorities
    from gateway.session_hosted_service import (
        abort_i09_source_close,
        begin_i09_source_close,
    )
    from gateway.runtime_custody import DrainReceipt

    runner, _homes, _loop, call = request.getfixturevalue('_hosted_mux_fixture')
    call(recover_gateway_native_sessions(runner))
    services = tuple(
        getattr(authority, 'hosted_room_service', None)
        for authority in serving_authorities(runner)
    )
    assert len(services) >= 2 and all(service is not None for service in services)
    coordinator = runner.runtime_custody
    receipt = coordinator.begin_close(owner_id='i09-hosted-abort-test', deadline_s=10)
    assert isinstance(receipt, DrainReceipt), receipt
    generation = receipt.close_generation
    assert begin_i09_source_close(runner, generation)
    second = services[1]
    original_abort = second.abort_preservation_close
    monkeypatch.setattr(second, 'abort_preservation_close', lambda _generation: False)
    try:
        assert not abort_i09_source_close(runner, generation)
        assert all(
            service.runtime.status()['preservation_close_generation'] == generation
            for service in services
        )
    finally:
        monkeypatch.setattr(second, 'abort_preservation_close', original_abort)
        assert abort_i09_source_close(runner, generation)
        assert all(not service.runtime.status()['intake_closed'] for service in services)
        assert coordinator.abort_before_handoff(receipt) is None


def test_global_close_abort_entrypoint_retains_receipt_on_hosted_source_failure(
    started_mux, monkeypatch,
):
    """The public close/abort path keeps the same coordinator receipt across a failed source."""
    from gateway.run_runtime import (
        abort_preservation_drain,
        begin_preservation_drain,
        recover_gateway_native_sessions,
    )
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    descriptor = runner.session_runtime_descriptor
    params = {
        'instance_id': descriptor['instance_id'],
        'authority_epoch': descriptor['authority_epoch'],
        'owner_id': 'i09-global-entrypoint',
        'scope': 'process',
        'deadline_s': 10,
    }
    wire = call(begin_preservation_drain(runner, params, 'fixture-peer'))
    assert wire['status'] == 'closing'
    generation = wire['close_generation']
    services = tuple(
        getattr(authority, 'hosted_room_service', None)
        for authority in serving_authorities(runner)
    )
    assert len(services) >= 2 and all(service is not None for service in services)
    second = services[1]
    original_abort = second.abort_preservation_close
    monkeypatch.setattr(second, 'abort_preservation_close', lambda _generation: False)
    try:
        refused = call(abort_preservation_drain(
            runner, {'receipt': wire}, 'fixture-peer',
        ))
        assert refused == {'status': 'refused', 'reason': 'hosted_source_resume_conflict'}
        assert runner.runtime_custody.state == 'closing'
        assert runner.runtime_custody.status()['close_generation'] == generation
    finally:
        monkeypatch.setattr(second, 'abort_preservation_close', original_abort)
    reopened = call(abort_preservation_drain(
        runner, {'receipt': wire}, 'fixture-peer',
    ))
    assert reopened['status'] == 'open'


def test_global_close_abort_entrypoint_retains_receipt_on_legacy_resume_failure(
    started_mux, monkeypatch,
):
    """A real registered adapter resume failure blocks the global abort before I09/I17 reopen."""
    from gateway.run_runtime import (
        abort_preservation_drain,
        begin_preservation_drain,
        recover_gateway_native_sessions,
    )

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    registry = runner._preservation_source_close
    registrations = tuple(registry.registrations)
    assert registrations
    adapter = registrations[0].adapter
    original_resume = adapter._preservation_await_source_resume
    descriptor = runner.session_runtime_descriptor
    params = {
        'instance_id': descriptor['instance_id'],
        'authority_epoch': descriptor['authority_epoch'],
        'owner_id': 'i09-legacy-entrypoint',
        'scope': 'process',
        'deadline_s': 10,
    }
    wire = call(begin_preservation_drain(runner, params, 'fixture-peer'))
    assert wire['status'] == 'closing'
    try:
        monkeypatch.setattr(
            adapter, '_preservation_await_source_resume',
            lambda _generation, _timeout: False,
        )
        refused = call(abort_preservation_drain(
            runner, {'receipt': wire}, 'fixture-peer',
        ))
        assert refused == {'status': 'refused', 'reason': 'source_resume_pending'}
        assert runner.runtime_custody.state == 'closing'
        assert runner.runtime_custody.status()['close_generation'] == wire['close_generation']
    finally:
        monkeypatch.setattr(adapter, '_preservation_await_source_resume', original_resume)
    reopened = call(abort_preservation_drain(
        runner, {'receipt': wire}, 'fixture-peer',
    ))
    assert reopened['status'] == 'open'


@pytest.mark.asyncio
async def test_mcp_factory_abort_requires_exact_source_receipt(tmp_path, monkeypatch):
    """A copied MCP close receipt cannot reopen a profile source."""
    from gateway.runtime_sidecar_remote import RemoteSourceError
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        factory = fixture.authority._mcp_source_factory
        factory.observe_config({})
        coordinator = fixture.runner.runtime_custody
        coordinator_receipt = coordinator.begin_close(
            owner_id='mcp-exact-receipt-test', deadline_s=10,
        )
        assert coordinator_receipt.state == 'closing'
        receipt = factory.close_ingress(coordinator_receipt.close_generation)
        assert factory.ingress == 'closed'
        copied = replace(receipt)
        with pytest.raises(RemoteSourceError) as excinfo:
            factory.abort_ingress(copied)
        assert getattr(excinfo.value, 'code', None) == 'mcp_close_receipt_mismatch'
        assert factory.ingress == 'closed'

        assert factory.abort_ingress(receipt)
        assert factory.ingress == 'open_pending'
        assert factory.reclose_ingress(receipt)
        assert factory.ingress == 'closed'
        assert factory.abort_ingress(receipt)
        with pytest.raises(RemoteSourceError) as excinfo:
            factory.finish_abort(
                receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
            )
        assert getattr(excinfo.value, 'code', None) == 'mcp_source_finalization_unprepared'
        assert factory.ingress == 'open_pending'
        assert factory.prepare_finish_abort(
            receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
        )
        assert coordinator.abort_before_handoff(coordinator_receipt) is None
        assert factory.finish_abort(
            receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
        )
        assert factory.ingress == 'open'
        assert factory.close_generation is None
        assert factory.close_receipt is None
    finally:
        fixture.cleanup()


def test_global_close_abort_recloses_mcp_factories_after_partial_failure(
    started_mux, monkeypatch,
):
    """A failed profile resume keeps every MCP factory and the coordinator CLOSING."""
    from gateway.run_runtime import (
        abort_preservation_drain,
        begin_preservation_drain,
        recover_gateway_native_sessions,
    )
    from gateway.runtime_sidecar_factory import observe_mcp_config
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authorities = tuple(serving_authorities(runner))
    assert len(authorities) >= 2
    for authority in authorities:
        observe_mcp_config(runner, authority.profile_id, {})
    factories = tuple(authority._mcp_source_factory for authority in authorities)
    assert all(factory is not None for factory in factories)
    descriptor = runner.session_runtime_descriptor
    params = {
        'instance_id': descriptor['instance_id'],
        'authority_epoch': descriptor['authority_epoch'],
        'owner_id': 'mcp-partial-abort',
        'scope': 'process',
        'deadline_s': 10,
    }
    wire = call(begin_preservation_drain(runner, params, 'fixture-peer'))
    assert wire['status'] == 'closing'
    generation = wire['close_generation']
    assert all(factory.ingress == 'closed' for factory in factories)

    second = factories[1]
    original_abort = type(second).abort_ingress

    def fail_second(factory, receipt):
        if factory is second:
            return False
        return original_abort(factory, receipt)

    monkeypatch.setattr(type(second), 'abort_ingress', fail_second)
    try:
        refused = call(abort_preservation_drain(
            runner, {'receipt': wire}, 'fixture-peer',
        ))
        assert refused == {'status': 'refused', 'reason': 'mcp_source_resume_conflict'}
        assert runner.runtime_custody.state == 'closing'
        assert runner.runtime_custody.status()['close_generation'] == generation
        assert all(factory.ingress == 'closed' for factory in factories)
    finally:
        monkeypatch.setattr(type(second), 'abort_ingress', original_abort)

    reopened = call(abort_preservation_drain(
        runner, {'receipt': wire}, 'fixture-peer',
    ))
    assert reopened['status'] == 'open'
    assert all(factory.ingress == 'open' for factory in factories)
    assert all(factory.close_receipt is None for factory in factories)


def test_global_close_abort_retries_after_mcp_finalization_epoch_failure(
    started_mux, monkeypatch,
):
    """A failed finalization keeps the public close CLOSING until the same receipt retries."""
    from gateway.run_runtime import (
        abort_preservation_drain,
        begin_preservation_drain,
        recover_gateway_native_sessions,
    )
    from gateway.runtime_sidecar_remote import RemoteSourceError
    from gateway.runtime_sidecar_factory import observe_mcp_config
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authorities = tuple(serving_authorities(runner))
    assert len(authorities) >= 2
    for authority in authorities:
        observe_mcp_config(runner, authority.profile_id, {})
    factories = tuple(authority._mcp_source_factory for authority in authorities)
    assert all(factory is not None for factory in factories)
    descriptor = runner.session_runtime_descriptor
    wire = call(begin_preservation_drain(
        runner,
        {
            'instance_id': descriptor['instance_id'],
            'authority_epoch': descriptor['authority_epoch'],
            'owner_id': 'mcp-finalization-epoch',
            'scope': 'process',
            'deadline_s': 10,
        },
        'fixture-peer',
    ))
    assert wire['status'] == 'closing'
    generation = wire['close_generation']
    assert all(factory.ingress == 'closed' for factory in factories)

    target = factories[1]
    original_identity = type(target)._close_identity
    calls = {'count': 0}

    def fail_during_prepare(factory):
        if factory is target:
            calls['count'] += 1
            if calls['count'] >= 2:
                raise RemoteSourceError('physical_owner_unknown')
        return original_identity(factory)

    monkeypatch.setattr(type(target), '_close_identity', fail_during_prepare)
    try:
        refused = call(abort_preservation_drain(
            runner, {'receipt': wire}, 'fixture-peer',
        ))
        assert refused == {
            'status': 'refused', 'reason': 'mcp_source_finalization_pending',
        }
        assert runner.runtime_custody.state == 'closing'
        assert runner.runtime_custody.status()['close_generation'] == generation
        assert all(factory.ingress == 'closed' for factory in factories)
        assert all(factory.close_receipt is not None for factory in factories)
        assert calls['count'] == 2
    finally:
        monkeypatch.setattr(type(target), '_close_identity', original_identity)

    reopened = call(abort_preservation_drain(
        runner, {'receipt': wire}, 'fixture-peer',
    ))
    assert reopened['status'] == 'open'
    assert all(factory.ingress == 'open' for factory in factories)
    assert all(factory.close_receipt is None for factory in factories)


def test_global_close_abort_retries_accepted_mcp_route_after_finalization_failure(
    started_mux, monkeypatch, require_mcp_2_sdk,
):
    """An accepted SDK route stays owned across a failed public abort and same-receipt retry."""
    pytest.importorskip('mcp.client.streamable_http')
    from gateway.run_runtime import (
        abort_preservation_drain,
        begin_preservation_drain,
        recover_gateway_native_sessions,
    )
    from gateway import runtime_sidecar_factory
    from gateway.runtime_sidecar_remote import RemoteSourceError, RemoteSourceReceipt
    from tools import mcp_tool_discovery, mcp_tool_lifecycle
    from tools.mcp_tool_common import _core
    from tests.gateway.test_runtime_sidecar_remote_consumers import _LoopbackMCP
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authority = next(iter(serving_authorities(runner)))
    factory = authority._mcp_source_factory
    assert factory is not None
    loopback = _LoopbackMCP()
    url = loopback.start()
    route_name = 'accepted'
    registry_scope = _core._mcp_registry_scope()
    observed = {}
    finish_results = []
    original_bind = RemoteSourceReceipt._bind_transport_scope_session
    original_finish = runtime_sidecar_factory.finish_remote_owner

    def observe_bind(receipt, scope):
        result = original_bind(receipt, scope)
        observed.update(
            receipt=receipt,
            owner=receipt._owner,
            scope=scope,
            server=receipt._server,
            task=scope.task,
        )
        return result

    def observe_finish(owner, *, task=None):
        result = original_finish(owner, task=task)
        if owner is observed.get('owner'):
            finish_results.append(result)
        return result

    monkeypatch.setattr(RemoteSourceReceipt, '_bind_transport_scope_session', observe_bind)
    monkeypatch.setattr(runtime_sidecar_factory, 'finish_remote_owner', observe_finish)
    remote_receipt = None
    try:
        names = mcp_tool_discovery.register_mcp_servers(
            {route_name: {'url': url, 'connect_timeout': 5}},
        )
        assert f'mcp__{route_name}__loopback_probe' in names
        remote_receipt = observed.get('receipt')
        server = observed.get('server')
        source_task = observed.get('task')
        assert remote_receipt is not None and remote_receipt.accepted is True
        assert remote_receipt.session is not None
        assert server is not None and source_task is server._task
        assert remote_receipt.task is source_task
        assert not source_task.done()

        descriptor = runner.session_runtime_descriptor
        wire = call(begin_preservation_drain(
            runner,
            {
                'instance_id': descriptor['instance_id'],
                'authority_epoch': descriptor['authority_epoch'],
                'owner_id': 'mcp-accepted-finalization-epoch',
                'scope': 'process',
                'deadline_s': 10,
            },
            'fixture-peer',
        ))
        assert wire['status'] == 'closing'
        assert factory.ingress == 'closed'
        original_identity = type(factory)._close_identity
        calls = {'count': 0}

        def fail_during_prepare(source_factory):
            if source_factory is factory:
                calls['count'] += 1
                if calls['count'] >= 2:
                    raise RemoteSourceError('physical_owner_unknown')
            return original_identity(source_factory)

        monkeypatch.setattr(type(factory), '_close_identity', fail_during_prepare)
        try:
            refused = call(abort_preservation_drain(
                runner, {'receipt': wire}, 'fixture-peer',
            ))
            assert refused == {
                'status': 'refused', 'reason': 'mcp_source_finalization_pending',
            }
            assert runner.runtime_custody.state == 'closing'
            assert factory.ingress == 'closed'
            assert factory.close_receipt is not None
            assert remote_receipt.state == 'accepted'
        finally:
            monkeypatch.setattr(type(factory), '_close_identity', original_identity)

        reopened = call(abort_preservation_drain(
            runner, {'receipt': wire}, 'fixture-peer',
        ))
        assert reopened['status'] == 'open'
        assert factory.ingress == 'open'
        assert factory.close_receipt is None
        assert remote_receipt.state == 'accepted'
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(
            scope=registry_scope, names={route_name}, timeout=15,
        )
        if remote_receipt is not None:
            async def retire_scope():
                return finish_results[-1] if finish_results else False

            assert call(retire_scope()) is True
            assert remote_receipt.state == 'closed'
        loopback.close()


def test_global_close_mcp_unknown_does_not_leave_factory_closed(started_mux):
    """A configured but unaccepted route rolls back the factory with the coordinator."""
    from gateway.run_runtime import begin_preservation_drain, recover_gateway_native_sessions
    from gateway.runtime_sidecar_factory import observe_mcp_config

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authority = runner.session_authority
    factory = authority._mcp_source_factory
    observe_mcp_config(
        runner, authority.profile_id,
        {'configured': {'url': 'http://127.0.0.1:1/mcp'}},
    )
    descriptor = runner.session_runtime_descriptor
    refused = call(begin_preservation_drain(
        runner,
        {
            'instance_id': descriptor['instance_id'],
            'authority_epoch': descriptor['authority_epoch'],
            'owner_id': 'mcp-unknown-begin',
            'scope': 'process',
            'deadline_s': 10,
        },
        'fixture-peer',
    ))
    assert refused == {'status': 'refused', 'reason': 'mcp_source_close_unknown'}
    assert runner.runtime_custody.state == 'open'
    assert factory.ingress == 'open'
    assert factory.close_receipt is None
