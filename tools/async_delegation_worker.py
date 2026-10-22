"""Process-bound worker ledger routing through the existing durable outbox.

Binding is private runtime wiring, not model tool surface or a storage fallback.
Fresh executor/helper threads retain the registered interpreter's owner boundary.
"""
import os
import threading

from agent.runtime_session_store import WorkerPersistenceError, call_off_event_loop, is_worker_process

_store = None
_lock = threading.Lock()


def bind_worker_delegation_store(store):
    global _store
    if not is_worker_process() or store.scope['pid'] != os.getpid():
        raise WorkerPersistenceError('permission_denied')
    with _lock:
        if _store is not None and _store is not store:
            raise WorkerPersistenceError('worker_delegation_already_bound')
        _store = store


def worker_ledger():
    if not is_worker_process():
        return None
    with _lock:
        if _store is None or _store.scope['pid'] != os.getpid():
            raise WorkerPersistenceError('worker_delegation_ledger_unbound')
        return WorkerDelegationLedger(_store)


def require_ledger_owner():
    if is_worker_process():
        raise WorkerPersistenceError('worker_delegation_owner_only')


class WorkerDelegationLedger:
    def __init__(self, store):
        self.store = store

    def dispatch(self, record):
        if record.get('parent_session_id') != self.store.scope['session_id']:
            raise WorkerPersistenceError('permission_denied')
        from tools.async_delegation import _ROUTING_KEYS
        if any(record.get(key) for key in _ROUTING_KEYS):
            raise WorkerPersistenceError('unsupported_producer')
        from gateway.status import get_process_start_time
        owner_started_at = get_process_start_time(self.store.scope['pid'])
        if type(owner_started_at) is not int or owner_started_at < 0:
            raise WorkerPersistenceError('worker_birth_unavailable')
        execution = dict(record['execution'])
        execution.update(
            owner_execution_id=self.store.scope['execution_id'], owner_pid=self.store.scope['pid'],
            owner_started_at=owner_started_at, owner_scope={
                key: self.store.scope[key] for key in ('profile_id', 'session_id', 'generation', 'epoch')})
        record['execution'] = execution
        # The tool callback can run on Relay's event loop; keep the accepted
        # execution identity while moving the journal write off that loop.
        return call_off_event_loop(self.store._apply, 'delegation.dispatch', {
            key: record.get(key, '') for key in ('delegation_id', 'parent_session_id', 'session_key',
                'origin_ui_session_id', 'origin_session_id', 'dispatched_at')} |
            {'execution': record['execution']} | {'task': {
            key: record[key] for key in ('goal', 'goals', 'context', 'toolsets', 'role', 'model', 'is_batch', 'task_indexes')
            if key in record}})['value']

    def complete(self, event, result):
        return self.store._apply('delegation.complete', {'event': event, 'result': result})['value']

    def execution(self, delegation_id, execution):
        return self.store._apply('delegation.execution', {
            'delegation_id': delegation_id, 'execution': execution})['value']

    def retry_pending_settlement(self):
        """Replay the exact RuntimeSessionStore journal entry, never allocate a new sequence."""
        return self.store.retry_pending()

    def child(self, delegation_id, entry):
        return self.store._apply('delegation.child', {'delegation_id': delegation_id, 'entry': entry})['value']

    def read(self, delegation_id):
        return self.store._apply('delegation.read', {'delegation_id': delegation_id})['value']

    def claim(self, delegation_id, claim_id):
        return self.store._apply('delegation.claim', {'delegation_id': delegation_id, 'claim_id': claim_id})['value']

    def ack(self, delegation_id, claim_id):
        return self.store._apply('delegation.ack', {'delegation_id': delegation_id, 'claim_id': claim_id})['value']

    def defer(self, delegation_id, claim_id):
        return self.store._apply('delegation.defer', {'delegation_id': delegation_id, 'claim_id': claim_id})['value']

    def release(self, delegation_id, claim_id):
        return self.store._apply('delegation.release', {'delegation_id': delegation_id, 'claim_id': claim_id})['value']

    def drop(self, delegation_id, claim_id):
        return self.store._apply('delegation.drop', {'delegation_id': delegation_id, 'claim_id': claim_id})['value']

    def unscheduled(self, delegation_id):
        return call_off_event_loop(self.store._apply, 'delegation.unscheduled', {'delegation_id': delegation_id})['value']
