"""Missing canonical producer registries cannot prove recovery-source absence."""

import pytest
from queue import Queue
import threading
from types import SimpleNamespace

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


@pytest.mark.asyncio
@pytest.mark.parametrize('attribute', ['completion_queue', 'pending_watchers', '_lock'])
async def test_notification_source_refuses_missing_registry(tmp_path, monkeypatch, attribute):
    from gateway.run_notifications import i10_source_snapshot

    runner, entries = await _boot(tmp_path, monkeypatch)
    registry = runner._profile_epoch_test_process_registry
    try:
        with monkeypatch.context() as missing:
            missing.delattr(registry, attribute)
            with pytest.raises(RuntimeError, match='registry'):
                i10_source_snapshot(runner.session_authorities.launch)
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_adapter_source_refuses_missing_inbound_registry(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        with monkeypatch.context() as missing:
            missing.delattr(runner, 'adapters')
            with pytest.raises(RuntimeError, match='registry'):
                runner.i10_source_snapshot(runner.session_authorities.launch)
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
@pytest.mark.parametrize('attribute,factory', [
    ('completion_queue', Queue), ('pending_watchers', list), ('_lock', threading.Lock),
    ('_running', dict), ('_finished', dict),
])
async def test_notification_source_refuses_replaced_index(
    tmp_path, monkeypatch, attribute, factory,
):
    from gateway.run_notifications import i10_source_snapshot

    runner, entries = await _boot(tmp_path, monkeypatch)
    registry = runner._profile_epoch_test_process_registry
    try:
        with monkeypatch.context() as replaced:
            replaced.setattr(registry, attribute, factory())
            with pytest.raises(RuntimeError, match='registry'):
                i10_source_snapshot(runner.session_authorities.launch)
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_notification_source_identity_is_bound_without_a_test_marker(tmp_path, monkeypatch):
    from gateway.run_notifications import i10_source_snapshot
    from tools import process_registry as module

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        with monkeypatch.context() as replaced:
            replaced.delattr(runner, '_profile_epoch_test_process_registry')
            replaced.setattr(module, 'process_registry', module.ProcessRegistry())
            with pytest.raises(RuntimeError, match='registry'):
                i10_source_snapshot(runner.session_authorities.launch)
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
@pytest.mark.parametrize('task', [None, object(), SimpleNamespace(done=lambda: 'unknown')])
async def test_notification_source_refuses_unknown_batch_task(tmp_path, monkeypatch, task):
    from gateway.run_notifications import i10_source_snapshot

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        with monkeypatch.context() as malformed:
            malformed.setitem(runner._completion_notification_batch_tasks, ('agent:main:readback',), task)
            with pytest.raises(RuntimeError, match='registry'):
                i10_source_snapshot(runner.session_authorities.launch)
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_notification_source_refuses_watcher_without_identity(tmp_path, monkeypatch):
    from gateway.run_notifications import i10_source_snapshot

    runner, entries = await _boot(tmp_path, monkeypatch)
    watchers = runner._profile_epoch_test_process_registry.pending_watchers
    try:
        watchers.append({})
        with pytest.raises(RuntimeError, match='registry'):
            i10_source_snapshot(runner.session_authorities.launch)
    finally:
        watchers.clear()
        assert await _close_boot(runner, entries) is True
