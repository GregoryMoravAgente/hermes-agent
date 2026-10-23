"""Execution-scoped worker operations on the ordinary authenticated authority.

Only explicit local compute registrations are enabled here. A delegated child is reserved by
the owner on its parent's receipt stream (gateway/session_worker_child.py), never registered.
"""
import asyncio
import hmac
import os

from gateway.session_admission import admission_fingerprint
from hermes_state_runtime import (
    RuntimeStoreError, _secret_digest, adopt_worker_execution,
    mutate_worker_execution, register_worker_execution,
)

_SCOPE = {'profile_id', 'session_id', 'execution_id', 'generation', 'pid', 'birth', 'secret'}


def _canonical_worker_birth(pid, supplied):
    """Normalize legacy/raw worker input to the canonical integer fingerprint."""
    from gateway.status import get_process_start_time
    try:
        canonical = get_process_start_time(pid)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        canonical = None
    if type(canonical) is not int or canonical < 0:
        raise RuntimeStoreError('worker_not_live')
    if type(supplied) is int:
        if supplied != canonical:
            raise RuntimeStoreError('permission_denied')
        return canonical
    if type(supplied) is float:
        # Existing launchers/tests can still present a raw process birth at the
        # boundary; it is never persisted or used as the internal fence. Use the
        # same kernel-stable read as the worker across macOS clock steps.
        import psutil
        from hermes_cli.process_identity import process_birth
        try:
            if process_birth(psutil.Process(pid)) == supplied:
                return canonical
        except psutil.Error as exc:
            raise RuntimeStoreError('worker_not_live') from exc
        raise RuntimeStoreError('permission_denied')
    raise RuntimeStoreError('invalid_params')


def _claim(connection, ref, params):
    import psutil
    authority = connection.authority
    if params.get('profile_id') != authority.profile_id or connection.actor.profile_id != authority.profile_id:
        raise RuntimeStoreError('profile_mismatch')
    if not connection.actor.capabilities:
        raise RuntimeStoreError('permission_denied')
    pid, birth = params.get('pid'), params.get('birth')
    secret = params.get('secret')
    if (type(pid) is not int or pid <= 0 or pid == os.getpid()
            or type(birth) not in (int, float) or not isinstance(secret, str) or not 16 <= len(secret) <= 256):
        raise RuntimeStoreError('invalid_params')
    try:
        process = psutil.Process(pid)
        canonical_birth = _canonical_worker_birth(pid, birth)
        if (not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
                or process.username() != psutil.Process().username()):
            raise RuntimeStoreError('permission_denied')
    except psutil.Error as exc:
        raise RuntimeStoreError('worker_not_live') from exc
    # Reuse the durable adoption digest as the producer claim. Changing any
    # assignment, principal, profile, PID birth or secret cannot adopt it.
    # The digest sees the canonical integer, so raw and canonical handshakes
    # identify the same worker without persisting a float birth.
    from hermes_state_worker_compression import worker_claim_origin
    canonical_params = dict(params, birth=canonical_birth)
    origin = worker_claim_origin(authority.db, canonical_params)
    return admission_fingerprint(canonical_target=origin, payload={
        key: canonical_params[key] for key in _SCOPE} | {'session_id': origin, 'principal': connection.actor.subject})


def _verify(connection, ref, params, claim):
    from hermes_state_worker_compression import worker_retry_target
    authority = connection.authority
    target = worker_retry_target(authority.db, params)
    with authority.db._read_ctx() as conn:
        row = conn.execute('SELECT * FROM worker_executions WHERE execution_id=?',
                           (params['execution_id'],)).fetchone()
        if row is None:
            raise RuntimeStoreError('not_found')
        if row['session_id'] != target:
            raise RuntimeStoreError('permission_denied')
        if type(params['generation']) is not int or row['generation'] != params['generation']:
            raise RuntimeStoreError('stale_generation')
        if not hmac.compare_digest(row['adoption_digest'], _secret_digest(claim)):
            raise RuntimeStoreError('permission_denied')


async def worker_request(connection, ref, params, *, operation):
    from gateway.session_authorities import require_serving
    require_serving(connection.authority)  # adopt/persist would write with the released epoch
    if operation == "persist" and isinstance(params.get("operation"), str) and params["operation"].startswith("delegation."):
        from gateway.session_worker_delegation import worker_delegation_request
        return await worker_delegation_request(connection, ref, params)
    if operation == "persist" and params.get("operation") == "child.reserve":
        from gateway.session_worker_child import worker_child_request
        return await worker_child_request(connection, ref, params)
    extra = {'register': {'kind'}, 'adopt': set(), 'persist': {'epoch', 'sequence', 'operation', 'payload'}}[operation]
    if set(params) != _SCOPE | extra:
        raise RuntimeStoreError('invalid_params')
    claim = _claim(connection, ref, params)
    authority = connection.authority
    if operation == 'register':
        authority._require_admission_open()
    scope = {key: params[key] for key in ('execution_id', 'session_id', 'generation')}
    if operation == 'register':
        # Only a worker-adoption ticket (what a launched worker carries): an interactive client
        # must not park a 'registered' row that nothing reaps (the FIFO would pause until restart).
        authority.authorize(connection.actor, ref, 'worker:adopt')
        if params['kind'] != 'compute':
            raise RuntimeStoreError('unsupported_producer')
        return await asyncio.to_thread(register_worker_execution, authority.db, epoch=authority.epoch,
            **scope, kind='compute', adoption_secret=claim, require_idle=True)
    if 'worker:adopt' not in connection.actor.capabilities:
        raise RuntimeStoreError('permission_denied')
    _verify(connection, ref, params, claim)
    if operation == 'adopt':
        result = await asyncio.to_thread(adopt_worker_execution, authority.db, epoch=authority.epoch,
                                         **scope, adoption_secret=claim)
        from gateway.session_worker_delegation import rebind_adopted_async_delegations
        await asyncio.to_thread(
            rebind_adopted_async_delegations, authority.db, execution_id=params['execution_id'],
            session_id=params['session_id'], generation=params['generation'], epoch=authority.epoch,
            profile_id=authority.profile_id, worker_pid=params['pid'],
            worker_birth=_canonical_worker_birth(params['pid'], params['birth']))
        return result
    result = await asyncio.to_thread(mutate_worker_execution, authority.db, epoch=params['epoch'],
        **scope, sequence=params['sequence'], operation=params['operation'], payload=params['payload'])
    if params['operation'] == 'execution.finish':
        # Queued input parked behind this worker; nothing else would restart its FIFO.
        authority.wake_after_worker(params['session_id'])
    return result
