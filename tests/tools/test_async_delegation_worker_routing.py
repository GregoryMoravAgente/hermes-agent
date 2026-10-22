"""Worker routing never falls through to raw canonical SQLite."""
from concurrent.futures import ThreadPoolExecutor
import os
import queue
import sqlite3
import time

import pytest

from agent import runtime_session_store
from tools import async_delegation as ledger


class Store:
    def __init__(self):
        from gateway.status import get_process_start_time
        birth = get_process_start_time(os.getpid())
        assert type(birth) is int
        self.scope = {
            'pid': os.getpid(), 'birth': birth, 'session_id': 'parent', 'profile_id': 'profile',
            'execution_id': 'worker-routing', 'generation': 0, 'epoch': 1,
        }
        self.calls = []

    def _apply(self, operation, payload):
        self.calls.append((operation, payload))
        if operation == 'delegation.read':
            return {'value': {'delegation_id': payload['delegation_id'], 'result': {'summary': 'done'}}}
        return {'value': True}


def test_worker_ledger_paths_are_closed_and_process_scoped(monkeypatch):
    from tools import async_delegation_worker as worker
    monkeypatch.setattr(runtime_session_store, '_worker_process', True)
    store = Store()
    monkeypatch.setattr(worker, '_store', None)
    worker.bind_worker_delegation_store(store)
    def forbidden(*args, **kwargs):
        pytest.fail('worker opened canonical SQLite')
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    birth = store.scope['birth']
    record = {
        'delegation_id': 'deleg_owned', 'parent_session_id': 'parent', 'goal': 'inert',
        'dispatched_at': time.time(), 'session_key': '', 'origin_session_id': '', 'origin_ui_session_id': '',
        'execution': {
            'handoff_id': 'deleg_owned', 'state': 'prepared', 'accepted': 'unknown',
            'worker_started': False, 'worker_exited': False, 'owner_pid': os.getpid(),
            'owner_started_at': birth, 'owner_execution_id': store.scope['execution_id'],
            'owner_scope': {key: store.scope[key] for key in ('profile_id', 'session_id', 'generation', 'epoch')},
            'error': None, 'exception': None,
        },
    }
    event = dict(record, type='async_delegation', status='completed', completed_at=time.time())
    def exercise():
        ledger._persist_dispatch(record)
        ledger.record_unit_child('deleg_owned', {'task_index': 0, 'summary': 'partial'})
        ledger._persist_completion(event, {'summary': 'done'})
        assert ledger.claim_completion_delivery('deleg_owned', 'token')
        assert ledger.defer_completion_delivery('deleg_owned', 'token')
        assert ledger.release_completion_delivery('deleg_owned', 'token')
        assert ledger.drop_completion_delivery('deleg_owned', 'token')
        assert ledger.complete_completion_delivery('deleg_owned', 'token')
        assert ledger.get_durable_delegation('deleg_owned')
        assert ledger.restore_undelivered_completions(queue.Queue()) == 0
        with pytest.raises(runtime_session_store.WorkerPersistenceError, match='owner_only'):
            ledger.mark_completion_delivered('deleg_owned')
        with pytest.raises(runtime_session_store.WorkerPersistenceError, match='owner_only'):
            ledger.recover_abandoned_delegations()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(exercise).result()
    assert {name for name, _ in store.calls} == {'delegation.' + name for name in
        ('dispatch', 'child', 'complete', 'claim', 'defer', 'release', 'drop', 'ack', 'read')}
    assert store.calls[0][1]['task'] == {'goal': 'inert'}


def test_unbound_worker_refuses_before_persistence_and_dispatch_state(monkeypatch):
    from tools import async_delegation_worker as worker
    monkeypatch.setattr(runtime_session_store, '_worker_process', True)
    monkeypatch.setattr(worker, '_store', None)
    monkeypatch.setattr(ledger, '_records', {})
    with pytest.raises(runtime_session_store.WorkerPersistenceError, match='worker_delegation_ledger_unbound'):
        ledger.dispatch_async_delegation(goal='inert', context=None, toolsets=None, role='leaf', model=None,
            session_key='', parent_session_id='parent', runner=lambda: {'summary': 'not run'})
    assert ledger.active_count() == 0
