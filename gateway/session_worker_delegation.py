"""Closed delegation-ledger operations in the existing worker receipt transaction.

The caller must supply the verified registration's execution/PID/birth, never
operation payload identity. No handler opens a database or commits independently.
"""
from functools import partial
import json
import math
import time

from hermes_state_runtime import RuntimeStoreError

_TASK_KEYS = {'goal', 'goals', 'context', 'toolsets', 'role', 'model', 'is_batch', 'task_indexes'}
_ROUTE = {'session_key': 'origin_session', 'parent_session_id': 'parent_session_id',
          'origin_ui_session_id': 'origin_ui_session_id', 'origin_session_id': 'origin_session_id'}
_EVENT_KEYS = _TASK_KEYS | set(_ROUTE) | {
    'type', 'delegation_id', 'status', 'completed_at', 'dispatched_at', 'summary', 'error',
    'api_calls', 'duration_seconds', 'total_duration_seconds', 'results', 'live_transcripts',
    'group', 'exit_reason', 'stalled_after_quiet_seconds', 'stall_threshold_seconds',
    'stall_phase', 'stall_grace_seconds'}
_EXECUTION_STATES = {
    'prepared', 'queued', 'started', 'runner_exited', 'unknown',
    'cancelled_before_start', 'rejected_before_enqueue',
}
_EXECUTION_ACCEPTANCE = {'yes', 'no', 'unknown'}
_EXECUTION_KEYS = {
    'handoff_id', 'state', 'accepted', 'worker_started', 'worker_exited',
    'owner_pid', 'owner_started_at', 'owner_execution_id', 'owner_scope', 'error', 'exception',
}


def _shape(payload, keys):
    if not isinstance(payload, dict) or set(payload) != set(keys):
        raise RuntimeStoreError('invalid_params')


def _text(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise RuntimeStoreError('invalid_params')


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise RuntimeStoreError('invalid_params')


def _execution(value, delegation_id):
    if not isinstance(value, dict) or set(value) - _EXECUTION_KEYS:
        raise RuntimeStoreError('invalid_params')
    state = value.get('state')
    if value.get('handoff_id') != delegation_id or state not in _EXECUTION_STATES:
        raise RuntimeStoreError('invalid_params')
    if value.get('accepted') not in _EXECUTION_ACCEPTANCE:
        raise RuntimeStoreError('invalid_params')
    if type(value.get('worker_started')) is not bool or type(value.get('worker_exited')) is not bool:
        raise RuntimeStoreError('invalid_params')
    if type(value.get('owner_pid')) is not int or value['owner_pid'] <= 0:
        raise RuntimeStoreError('invalid_params')
    if value.get('owner_started_at') is not None:
        _number(value['owner_started_at'])
    _text(value.get('owner_execution_id'))
    scope = value.get('owner_scope')
    if scope is not None:
        if (not isinstance(scope, dict) or set(scope) != {'profile_id', 'session_id', 'generation', 'epoch'}
                or not isinstance(scope.get('profile_id'), str) or not scope['profile_id']
                or not isinstance(scope.get('session_id'), str) or not scope['session_id']
                or type(scope.get('generation')) is not int or type(scope.get('epoch')) is not int
                or scope['generation'] < 0 or scope['epoch'] < 1):
            raise RuntimeStoreError('invalid_params')
    for key in ('error', 'exception'):
        if value.get(key) is not None:
            if not isinstance(value[key], dict):
                raise RuntimeStoreError('invalid_params')
            try:
                encoded = json.dumps(value[key], separators=(',', ':'), allow_nan=False)
            except (TypeError, ValueError):
                raise RuntimeStoreError('invalid_params')
            if len(encoded) > 2048:
                raise RuntimeStoreError('invalid_params')
    started, exited, accepted = value['worker_started'], value['worker_exited'], value['accepted']
    if state == 'prepared' and (accepted != 'unknown' or started or exited):
        raise RuntimeStoreError('invalid_params')
    if state == 'queued' and (accepted != 'yes' or started or exited):
        raise RuntimeStoreError('invalid_params')
    if state == 'started' and (accepted != 'yes' or not started or exited):
        raise RuntimeStoreError('invalid_params')
    if state == 'runner_exited' and (accepted != 'yes' or not started or not exited):
        raise RuntimeStoreError('invalid_params')
    if state == 'cancelled_before_start' and (accepted != 'yes' or started or not exited):
        raise RuntimeStoreError('invalid_params')
    if state == 'rejected_before_enqueue' and (accepted != 'no' or started or not exited):
        raise RuntimeStoreError('invalid_params')


def _owned(conn, session_id, execution_id, delegation_id):
    _text(delegation_id)
    row = conn.execute('SELECT * FROM async_delegations WHERE delegation_id=?', (delegation_id,)).fetchone()
    if row is None:
        raise RuntimeStoreError('not_found')
    if row['parent_session_id'] != session_id or row['owner_execution_id'] != execution_id:
        raise RuntimeStoreError('permission_denied')
    return row


def _dispatch(db, conn, session_id, payload, *, execution_id, worker_pid, worker_birth, owner_profile_id=None, owner_generation=None, owner_epoch=None):
    _shape(payload, {'delegation_id', 'task', 'dispatched_at', 'execution', *_ROUTE})
    _text(payload['delegation_id'])
    _number(payload['dispatched_at'])
    from gateway.session_worker import _canonical_worker_birth
    owner_started_at = _canonical_worker_birth(worker_pid, worker_birth)
    _execution(payload['execution'], payload['delegation_id'])
    execution = payload['execution']
    expected_scope = {'profile_id': owner_profile_id, 'session_id': session_id,
                      'generation': owner_generation, 'epoch': owner_epoch}
    if (execution['owner_execution_id'] != execution_id
            or execution['owner_pid'] != worker_pid
            or execution.get('owner_started_at') != owner_started_at
            or (owner_profile_id is not None and execution.get('owner_scope') != expected_scope)):
        raise RuntimeStoreError('permission_denied')
    if not isinstance(payload['task'], dict) or set(payload['task']) - _TASK_KEYS:
        raise RuntimeStoreError('invalid_params')
    session = conn.execute('SELECT source,session_key FROM sessions WHERE id=?', (session_id,)).fetchone()
    # Only compute registrations are currently enabled. Native/child origins need
    # an owner-reserved source envelope, not worker-selected transport identities.
    if session is None or session['source'] not in ('cli', 'tui', 'gui', 'tool', 'oneshot'):
        raise RuntimeStoreError('unsupported_producer')
    if (payload['parent_session_id'] != session_id
            or payload['session_key'] != (session['session_key'] or '')
            or payload['origin_ui_session_id'] not in ('', session_id)
            or payload['origin_session_id'] not in ('', session_id)):
        raise RuntimeStoreError('permission_denied')
    old = conn.execute('SELECT 1 FROM async_delegations WHERE delegation_id=?', (payload['delegation_id'],)).fetchone()
    if old is not None:
        _owned(conn, session_id, execution_id, payload['delegation_id'])
        # Exact network retries are resolved by worker_receipts before this handler.
        raise RuntimeStoreError('admission_conflict')
    now = time.time()
    conn.execute('''INSERT INTO async_delegations
        (delegation_id,origin_session,origin_ui_session_id,parent_session_id,state,dispatched_at,
         updated_at,owner_pid,owner_started_at,task_json,origin_session_id,owner_execution_id,execution_json)
        VALUES(?,?,?,?,'running',?,?,?,?,?,?,?,?)''',
        (payload['delegation_id'], payload['session_key'], payload['origin_ui_session_id'], session_id,
         payload['dispatched_at'], now, worker_pid, owner_started_at, json.dumps(payload['task']),
         payload['origin_session_id'], execution_id, json.dumps(payload['execution'], sort_keys=True)))
    return {'value': None}


def _execution_update(db, conn, session_id, payload, *, execution_id, worker_pid, worker_birth, owner_profile_id=None, owner_generation=None, owner_epoch=None):
    _shape(payload, {'delegation_id', 'execution'})
    row = _owned(conn, session_id, execution_id, payload['delegation_id'])
    incoming = payload['execution']
    _execution(incoming, payload['delegation_id'])
    expected_scope = {'profile_id': owner_profile_id, 'session_id': session_id,
                      'generation': owner_generation, 'epoch': owner_epoch}
    if (incoming['owner_execution_id'] != execution_id
            or incoming['owner_pid'] != worker_pid
            or incoming.get('owner_started_at') != row['owner_started_at']
            or (owner_profile_id is not None and incoming.get('owner_scope') != expected_scope)
            or row['owner_pid'] != worker_pid
            or row['owner_execution_id'] != execution_id):
        raise RuntimeStoreError('permission_denied')
    current = json.loads(row['execution_json'] or '{}')
    if not isinstance(current, dict) or current.get('handoff_id') != payload['delegation_id']:
        raise RuntimeStoreError('execution_unknown')
    if (current.get('worker_started') and not incoming['worker_started']) or (
            current.get('worker_exited') and not incoming['worker_exited']):
        raise RuntimeStoreError('stale_execution')
    transitions = {
        'prepared': {'prepared', 'queued', 'unknown', 'rejected_before_enqueue'},
        'queued': {'queued', 'started', 'unknown', 'cancelled_before_start'},
        'started': {'started', 'runner_exited', 'unknown'},
        'unknown': {'unknown', 'started', 'runner_exited', 'cancelled_before_start', 'rejected_before_enqueue'},
        'runner_exited': {'runner_exited'},
        'cancelled_before_start': {'cancelled_before_start'},
        'rejected_before_enqueue': {'rejected_before_enqueue'},
    }
    if incoming['state'] not in transitions.get(current.get('state'), set()):
        raise RuntimeStoreError('stale_execution')
    conn.execute('UPDATE async_delegations SET execution_json=?,updated_at=? WHERE delegation_id=?',
                 (json.dumps(incoming, sort_keys=True), time.time(), payload['delegation_id']))
    return {'value': True}


def _complete(db, conn, session_id, payload, *, execution_id):
    _shape(payload, {'event', 'result'})
    event = payload['event']
    if (not isinstance(event, dict) or set(event) - _EVENT_KEYS
            or not isinstance(payload['result'], dict) or event.get('type') != 'async_delegation'
            or event.get('status') not in ('completed', 'success', 'error', 'interrupted', 'stalled', 'unknown')):
        raise RuntimeStoreError('invalid_params')
    row = _owned(conn, session_id, execution_id, event.get('delegation_id'))
    if any(event.get(key, '') != (row[column] or '') for key, column in _ROUTE.items()):
        raise RuntimeStoreError('permission_denied')
    _number(event.get('completed_at'))
    if row['state'] != 'running':
        same_event = json.loads(row['event_json']) if row['event_json'] else None
        same_result = json.loads(row['result_json']) if row['result_json'] else None
        if same_event == event and same_result == payload['result']:
            return {'value': None}
        raise RuntimeStoreError('admission_conflict')
    conn.execute('''UPDATE async_delegations SET state=?,completed_at=?,updated_at=?,event_json=?,result_json=?
        WHERE delegation_id=?''', (event['status'], event['completed_at'], time.time(),
        json.dumps(event), json.dumps(payload['result']), event['delegation_id']))
    return {'value': None}


def _child(db, conn, session_id, payload, *, execution_id):
    _shape(payload, {'delegation_id', 'entry'})
    row = _owned(conn, session_id, execution_id, payload['delegation_id'])
    entry = payload['entry']
    if not isinstance(entry, dict) or type(entry.get('task_index')) is not int or entry['task_index'] < 0:
        raise RuntimeStoreError('invalid_params')
    if row['state'] != 'running':
        return {'value': None}
    previous = json.loads(row['result_json'] or '{}')
    results = [item for item in previous.get('results', []) if item.get('task_index') != entry['task_index']]
    results.append(entry)
    conn.execute('UPDATE async_delegations SET result_json=?,updated_at=? WHERE delegation_id=?',
        (json.dumps({'results': results, 'partial': True}), time.time(), payload['delegation_id']))
    return {'value': None}


def _read(db, conn, session_id, payload, *, execution_id):
    _shape(payload, {'delegation_id'})
    row = _owned(conn, session_id, execution_id, payload['delegation_id'])
    return {'value': {key: row[key] for key in ('delegation_id', 'origin_session', 'state', 'dispatched_at',
        'completed_at', 'delivery_state', 'delivery_attempts', 'origin_session_id')} |
        {'result': json.loads(row['result_json']) if row['result_json'] else None,
         'execution': json.loads(row['execution_json']) if row['execution_json'] else None}}


def _claim(db, conn, session_id, payload, *, execution_id):
    _shape(payload, {'delegation_id', 'claim_id'})
    row = _owned(conn, session_id, execution_id, payload['delegation_id'])
    _text(payload['claim_id'])
    now = time.time()
    if row['state'] in ('running', 'finalizing'):
        return {'value': False}
    changed = conn.execute('''UPDATE async_delegations SET delivery_claim=?,delivery_claimed_at=?,
        delivery_attempts=delivery_attempts+1,updated_at=? WHERE delegation_id=? AND delivery_state='pending'
        AND (delivery_claim IS NULL OR delivery_claimed_at < ?)''',
        (payload['claim_id'], now, now, payload['delegation_id'], now - 300)).rowcount
    return {'value': changed == 1}


_DELIVERY_UPDATES = {
    'ack': "delivery_state='delivered',delivered_at=:now",
    'drop': "delivery_state='dropped'",
    'defer': 'delivery_attempts=MAX(0,delivery_attempts-1)',
    'release': "delivery_state=CASE WHEN delivery_attempts>=:cap THEN 'dropped' ELSE delivery_state END",
}


def _delivery(db, conn, session_id, payload, *, execution_id, action):
    from tools.async_delegation import _MAX_DELIVERY_ATTEMPTS
    _shape(payload, {'delegation_id', 'claim_id'})
    _owned(conn, session_id, execution_id, payload['delegation_id'])
    _text(payload['claim_id'])
    # Only constant, closed statement fragments enter this query.
    changed = conn.execute('UPDATE async_delegations SET ' + _DELIVERY_UPDATES[action] +
        ",updated_at=:now,delivery_claim=NULL,delivery_claimed_at=NULL WHERE delegation_id=:delegation_id "
        "AND delivery_state='pending' AND delivery_claim=:claim_id",
        payload | {'now': time.time(), 'cap': _MAX_DELIVERY_ATTEMPTS}).rowcount
    return {'value': changed == 1}


def _unscheduled(db, conn, session_id, payload, *, execution_id):
    _shape(payload, {'delegation_id'})
    row = _owned(conn, session_id, execution_id, payload['delegation_id'])
    if row['state'] != 'running' or row['result_json'] is not None:
        raise RuntimeStoreError('admission_conflict')
    conn.execute('DELETE FROM async_delegations WHERE delegation_id=?', (payload['delegation_id'],))
    return {'value': None}


def rebind_adopted_async_delegations(db, *, execution_id, session_id, generation, epoch,
                                      profile_id, worker_pid, worker_birth):
    """Rehome only a dead owner's scoped async rows after authenticated worker adoption."""
    _text(execution_id)
    _text(session_id)
    _text(profile_id)
    if type(generation) is not int or generation < 0 or type(epoch) is not int or epoch < 1:
        raise RuntimeStoreError('invalid_params')
    if type(worker_pid) is not int or worker_pid <= 0:
        raise RuntimeStoreError('invalid_params')
    _number(worker_birth)
    from gateway.session_worker import _canonical_worker_birth
    worker_birth = _canonical_worker_birth(worker_pid, worker_birth)
    expected_scope = {'profile_id': profile_id, 'session_id': session_id,
                      'generation': generation, 'epoch': epoch}

    def write(conn):
        owner = conn.execute(
            'SELECT session_id,generation,owner_epoch,status FROM worker_executions WHERE execution_id=?',
            (execution_id,)).fetchone()
        if (owner is None or owner['session_id'] != session_id or owner['generation'] != generation
                or owner['owner_epoch'] != epoch or owner['status'] != 'running'):
            raise RuntimeStoreError('stale_generation')
        rows = conn.execute(
            "SELECT delegation_id,owner_execution_id,owner_pid,owner_started_at,execution_json FROM async_delegations "
            "WHERE parent_session_id=? AND owner_execution_id=? AND state IN ('running','finalizing')",
            (session_id, execution_id)).fetchall()
        rebound = 0
        from gateway.status import _pid_exists, get_process_start_time, start_time_fingerprints_match
        for row in rows:
            try:
                claim = json.loads(row['execution_json'] or '{}')
            except (TypeError, ValueError):
                raise RuntimeStoreError('execution_unknown')
            if not isinstance(claim, dict):
                raise RuntimeStoreError('execution_unknown')
            _execution(claim, row['delegation_id'])
            old_scope = claim.get('owner_scope')
            if (row['owner_execution_id'] != execution_id
                    or claim.get('owner_execution_id') != execution_id
                    or claim.get('owner_pid') != row['owner_pid']
                    or claim.get('owner_started_at') != row['owner_started_at']):
                raise RuntimeStoreError('execution_unknown')
            old_pid, old_birth = claim.get('owner_pid'), claim.get('owner_started_at')
            if type(old_pid) is not int or old_pid <= 0 or type(old_birth) is not int:
                raise RuntimeStoreError('execution_unknown')
            # A lost worker.adopt reply may retry after the exact row was already
            # rebound.  Requiring every row/claim identity to match makes this
            # branch idempotent without accepting a foreign or stale owner.
            if (old_scope == expected_scope and old_pid == worker_pid and old_birth == worker_birth):
                continue
            if (not isinstance(old_scope, dict)
                    or old_scope.get('profile_id') != profile_id
                    or old_scope.get('session_id') != session_id
                    or old_scope.get('generation') != generation
                    or type(old_scope.get('epoch')) is not int
                    or old_scope['epoch'] >= epoch):
                raise RuntimeStoreError('execution_unknown')
            try:
                old_alive = _pid_exists(old_pid)
                observed = get_process_start_time(old_pid)
            except (OSError, RuntimeError, TypeError, ValueError):
                raise RuntimeStoreError('execution_unknown')
            if old_alive is None or (old_alive and observed is None):
                raise RuntimeStoreError('execution_unknown')
            if old_alive and start_time_fingerprints_match(old_birth, observed):
                raise RuntimeStoreError('owner_still_live')
            updated = dict(claim, owner_pid=worker_pid, owner_started_at=worker_birth,
                           owner_scope=expected_scope)
            changed = conn.execute(
                'UPDATE async_delegations SET owner_pid=?,owner_started_at=?,execution_json=?,updated_at=? '
                'WHERE delegation_id=? AND owner_execution_id=? AND owner_pid=? AND owner_started_at=?',
                (worker_pid, worker_birth, json.dumps(updated, sort_keys=True), time.time(),
                 row['delegation_id'], execution_id, old_pid, old_birth)).rowcount
            if changed != 1:
                raise RuntimeStoreError('execution_unknown')
            rebound += 1
        return rebound

    return db._execute_write(write)


def worker_delegation_handlers(execution_id, worker_pid, worker_birth, *, profile_id=None, session_id=None, generation=None, epoch=None):
    """Register these handlers only after gateway worker identity verification."""
    _text(execution_id)
    if type(worker_pid) is not int or worker_pid <= 0:
        raise RuntimeStoreError('invalid_params')
    _number(worker_birth)
    scoped = profile_id is not None
    if scoped and (not isinstance(profile_id, str) or not profile_id or type(generation) is not int
                   or type(epoch) is not int or epoch < 1):
        raise RuntimeStoreError('invalid_params')
    handlers = {name: partial(fn, execution_id=execution_id) for name, fn in {
        'complete': _complete, 'child': _child, 'read': _read, 'claim': _claim,
        'execution': partial(_execution_update, worker_pid=worker_pid, worker_birth=worker_birth,
                             owner_profile_id=profile_id, owner_generation=generation, owner_epoch=epoch),
        'unscheduled': _unscheduled}.items()}
    if scoped:
        handlers['dispatch'] = partial(_dispatch, execution_id=execution_id, worker_pid=worker_pid, worker_birth=worker_birth,
                                       owner_profile_id=profile_id, owner_generation=generation, owner_epoch=epoch)
    else:
        def legacy_dispatch(db, conn, sid, payload):
            if 'execution' not in payload:
                from gateway.status import get_process_start_time
                payload = dict(payload, execution={
                    'handoff_id': payload.get('delegation_id'), 'state': 'prepared', 'accepted': 'unknown',
                    'worker_started': False, 'worker_exited': False, 'owner_pid': worker_pid,
                    'owner_started_at': get_process_start_time(worker_pid), 'owner_execution_id': execution_id,
                    'error': None, 'exception': None})
            return _dispatch(db, conn, sid, payload, execution_id=execution_id, worker_pid=worker_pid,
                             worker_birth=worker_birth)
        handlers['dispatch'] = legacy_dispatch
    handlers.update({name: partial(_delivery, execution_id=execution_id, action=name) for name in _DELIVERY_UPDATES})
    return {'delegation.' + name: handler for name, handler in handlers.items()}


def persist_worker_delegation(db, *, epoch, profile_id=None, execution_id, session_id, generation,
                              sequence, operation, payload, worker_pid, worker_birth):
    """Closed ledger sibling of mutate_worker_execution, sharing its receipt stream.

    Gateway calls this ONLY after its ordinary worker claim verification. The
    verified process identity is deliberately separate from the operation payload.
    """
    from hermes_state_runtime import apply_worker_receipt
    handlers = worker_delegation_handlers(execution_id, worker_pid, worker_birth,
                                           profile_id=profile_id, session_id=session_id,
                                           generation=generation, epoch=epoch)
    return apply_worker_receipt(db, epoch=epoch, execution_id=execution_id, session_id=session_id,
                                generation=generation, sequence=sequence, operation=operation,
                                payload=payload, handlers=handlers)


async def worker_delegation_request(connection, ref, params):
    """worker.persist delegation-family hook; all authentication stays mandatory."""
    import asyncio
    from gateway.session_worker import _SCOPE, _claim, _verify
    if set(params) != _SCOPE | {'epoch', 'sequence', 'operation', 'payload'}:
        raise RuntimeStoreError('invalid_params')
    claim = _claim(connection, ref, params)
    authority = connection.authority
    if 'worker:adopt' not in connection.actor.capabilities:
        raise RuntimeStoreError('permission_denied')
    _verify(connection, ref, params, claim)

    def persist_and_publish():
        result = persist_worker_delegation(authority.db,
            **{key: params[key] for key in ('epoch', 'execution_id', 'session_id', 'generation',
                                           'sequence', 'operation', 'payload')},
            profile_id=params['profile_id'], worker_pid=params['pid'], worker_birth=params['birth'])
        if params['operation'] == 'delegation.complete':
            with authority.db._read_ctx() as conn:
                row = _owned(conn, ref.session_id, params['execution_id'], params['payload']['event']['delegation_id'])
                event = json.loads(row['event_json']) if row['delivery_state'] == 'pending' else None
            if event is not None:
                from tools.process_registry import process_registry
                # Queueing after commit can repeat after a lost ACK. Existing
                # token-guarded claims arbitrate delivery; never ACK at publication.
                process_registry.completion_queue.put(event | {'restored': True})
        return result
    return await asyncio.to_thread(persist_and_publish)
