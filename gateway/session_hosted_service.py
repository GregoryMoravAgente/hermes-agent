"""Authority-bound hosted service; no legacy session server is constructed."""
import asyncio
from contextlib import nullcontext
from pathlib import Path
import time
from typing import Any

from gateway.session_contract import Principal
from gateway.session_authorities import active_authority, all_authorities, owner_scope, serving_authorities
from gateway.session_hosted_controls import HostedControls
from hermes_state_runtime import RuntimeStoreError, _epoch
from tui_gateway.hosted_room_service import HostedRoomService

_OWNER = 'gateway.hosted.owner.v1:'


class CanonicalHostedRoomService(HostedControls, HostedRoomService):
    def __init__(self, authority, loop):
        self.authority, self.loop = authority, loop
        self.member_rpcs = {}
        super().__init__(None, db_path=authority.db.db_path)

    def begin_preservation_close(self, close_generation: int) -> bool:
        """Fence new hosted-room intake while accepted room work drains."""
        if not getattr(self, '_transport_installed', False):
            return False
        return self.runtime.begin_preservation_close(close_generation)

    def abort_preservation_close(self, close_generation: int) -> bool:
        """Reopen the same hosted intake generation before handoff is committed."""
        return self.runtime.abort_preservation_close(close_generation)

    @staticmethod
    def _pending_callbacks(rpc: Any) -> int:
        """Count callbacks whose exact hosted admission has not been acknowledged.

        Hosted member RPCs retain the callback until the callback consumer has returned and
        the operation finalizer has released the same durable admission.  Reading this map is
        therefore stronger than treating a terminal driver row as an acknowledged delivery.
        The owner RPC protects its map with ``_lock``; the in-process RPC is only touched by the
        authority loop, so its snapshot is taken directly.
        """
        callbacks = getattr(rpc, 'callbacks', None)
        if not isinstance(callbacks, dict):
            return 0
        lock = getattr(rpc, '_lock', None)
        if lock is None:
            return len(callbacks)
        with lock:
            return len(callbacks)

    def _preservation_pending_acks(self) -> int:
        """Observe local callback and remote run receipts owned by this room service."""
        pending = 0
        rpcs = [self]
        rpcs.extend(getattr(self, 'member_rpcs', {}).values())
        for rpc in rpcs:
            pending += self._pending_callbacks(rpc)
        # A peer client keeps the exact run receipt until its terminal status is observed.  A
        # durable terminal room task can outlive that in-memory receipt, so this remains a
        # conservative close fence: an unobserved peer ACK always blocks handoff.
        for client in getattr(self, 'peer_clients', {}).values():
            runs = getattr(client, '_runs', None)
            if not isinstance(runs, dict):
                continue
            terminal = getattr(client, '_terminal_receipts', ())
            pending += sum(key not in terminal for key in runs)
        return pending

    def _preservation_active_operations(self) -> int:
        """Count canonical I09 handles that still need their task/no-task finalizer."""
        records = getattr(getattr(self, 'authority', None), '_preservation_operations', None)
        if not isinstance(records, dict):
            return 0
        return sum(
            isinstance(record, dict)
            and record.get('owner_class') == 'I09_a2a_hosted_kanban'
            for record in records.values()
        )

    def _preservation_inventory(self, close_generation: int) -> dict[str, Any] | None:
        if not getattr(self, '_transport_installed', False):
            return None
        runtime = self.runtime.status()
        if (runtime.get('preservation_close_generation') != close_generation
                or runtime.get('intake_closed') is not True
                or runtime.get('stopping') is True
                or runtime.get('running') is not True
                or any(runtime.get(name) for name in (
                    'current_tasks', 'room_threads', 'leased_rooms', 'blocked_rooms',
                    'ambiguous_rooms', 'unavailable_route_retries',
                    'preservation_close_scan_rooms'))):
            return None
        with self._policy_lock:
            if self._pending_actions or self._link_load_error:
                return None
            route_statuses = tuple(sorted(
                (room_id, member_id, self._peer_route_status.get((room_id, member_id), 'ready'))
                for room_id, member_id in self.peer_routes))
        if any(status != 'ready' for _room_id, _member_id, status in route_statuses):
            return None
        from gateway import hosted_rooms
        from gateway import hosted_room_driver as driver
        limit = hosted_rooms.MAX_ROOM_LIST_LIMIT
        rooms, offset = [], 0
        while True:
            page = hosted_rooms.list_rooms(
                self.db_path, include_disbanded=True, limit=limit, offset=offset)
            rooms.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        local_gateway_id = hosted_rooms.local_authority_gateway_id()
        rooms = [room for room in rooms
                 if str(room.get('authority_gateway_id')) == local_gateway_id]
        terminal_tasks = 0
        active_tasks = 0
        for room in rooms:
            room_id = str(room['room_id'])
            tasks = driver.list_tasks(self.db_path, room_id=room_id)
            for task in tasks:
                status = task['status']
                if status not in driver.TERMINAL_STATUSES:
                    active_tasks += 1
                    continue
                if not self.policy_checkpoint.publication_exists(
                        room_id=room_id, task_id=task['identity'].task_id,
                        status=status, execution_generation=int(task['execution_generation'] or 0)):
                    return None
                terminal_tasks += 1
        pending_acks = self._preservation_pending_acks()
        active_operations = self._preservation_active_operations()
        if active_tasks or pending_acks or active_operations:
            return None
        return {
            'registered_rooms': tuple(sorted(str(room['room_id']) for room in rooms)),
            'registered_peer_routes': route_statuses,
            'terminal_tasks_published': terminal_tasks,
            'active_tasks': active_tasks,
            'pending_acks': pending_acks,
            'active_operations': active_operations,
            'worker_process_generation': runtime['process_generation'],
            'intake_closed': True,
            'close_generation': close_generation,
        }

    def await_preservation_close(self, close_generation: int, timeout: float) -> bool:
        """Wait for the hosted producer, rooms, leases and terminal publications to quiesce."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            if self._preservation_inventory(close_generation) is not None:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.05, remaining))

    def preservation_owner_evidence(
        self, *, process_instance: str, authority_epoch: int,
        close_generation: int, owner_id: str,
    ) -> dict[str, Any] | None:
        inventory = self._preservation_inventory(close_generation)
        if inventory is None:
            return None
        from gateway.runtime_custody import OWNER_SPECS
        return {
            'source': OWNER_SPECS['I09_a2a_hosted_kanban'].source,
            'process_instance': process_instance,
            'authority_epoch': authority_epoch,
            'close_generation': close_generation,
            'owner_id': owner_id,
            'observed': True,
            **inventory,
        }

    def _make_rpc(self, server):
        # Member-specific canonical transports retain exact durable history. They
        # intentionally use the runtime's receipt-capable (non-legacy) recovery path.
        return self

    def profile_homes(self):
        from gateway.session_hosted_members import member_roster
        return member_roster(self.authority)[0]

    def local_profiles(self):
        return tuple(self.profile_homes())

    def room_profiles(self, room):
        """A saved room keeps every local member it seated: unserving one bot must not stop the
        others. That member's own turns still fail (``_resolve_member_transport`` and ``attest``
        need a live home), and a new group cannot seat it (``local_profiles``)."""
        seated = {m.get('profile') for m in room.get('members') or ()
                  if isinstance(m, dict) and isinstance(m.get('profile'), str)
                  and (m.get('target') or {}).get('kind', 'local') == 'local'}
        return tuple(sorted(set(self.local_profiles()) | seated))

    def attest(self, selector, operation, params):
        from dataclasses import asdict
        from gateway.hosted_room_driver import list_tasks
        if set(selector) != {'room_id', 'member_id', 'profile'}:
            raise RuntimeStoreError('invalid_params')
        room_id, member, profile = (selector[k] for k in ('room_id', 'member_id', 'profile'))
        owner = self._owner(room_id)
        self._owned_authority(room_id)
        room = self._room(room_id)
        if not any(m['member_id'] == member and m['profile'] == profile
                   and m.get('target', {}).get('kind', 'local') == 'local' for m in room['members']):
            raise RuntimeStoreError('permission_denied')
        target_home = self.profile_homes().get(profile)
        if target_home is None or params.get('_target_home') != str(target_home):
            raise RuntimeStoreError('permission_denied')
        result = {'owner': owner, 'target_home': str(target_home)}
        if operation in {'submit', 'execute', 'attachment'}:
            matches = [t for t in list_tasks(self.db_path, room_id=room_id)
                       if asdict(t['identity']) == params.get('task')
                       and t['execution_generation'] == params.get('execution_generation')
                       and t['status'] == 'running'
                       and t['payload'].get('target_member_id', t['payload']['target_profile']) == member
                       and t['payload']['target_profile'] == profile
                       and (operation == 'execute' or (
                           t['payload']['prompt'] == params.get('prompt')
                           and t['payload'].get('attachments', []) == (params.get('attachments') or [])))]
            if len(matches) != 1:
                raise RuntimeStoreError('permission_denied')
            payload = matches[0]['payload']
            if operation == 'attachment':
                # Bytes only: the prompt/manifest already attested for the submit would
                # otherwise compete with the chunk for the bounded response line.
                from gateway.session_hosted_transport import source_attachment_chunk
                result.update(source_attachment_chunk(self, member, room_id, payload.get('attachments', []), params))
            else:
                from gateway.session_hosted_transport import source_attachment_digests
                manifest = payload.get('attachments', [])
                result.update(prompt=payload['prompt'], attachments=manifest,
                              attachment_digests=source_attachment_digests(self, member, room_id, manifest))
        return result


    def bindings(self):
        with self.authority.db._read_ctx() as conn:
            owned = {r[0][len(_OWNER):] for r in conn.execute(
                'SELECT key FROM state_meta WHERE key LIKE ?', (_OWNER + '%',))}
        return tuple(b for b in super().bindings() if b.room_id in owned)

    def _turn_lock(self, profile):
        return nullcontext()

    def authorize_room(self, actor_subject, room_id, *, create=False, conn=None):
        """Check (or, with ``create``, claim) room ownership. A ``conn`` runs the claim inside
        the caller's room-creation transaction, so owner and room commit or roll back together."""
        from gateway.hosted_rooms_common import IDENTIFIER_RE
        if (not isinstance(actor_subject, str) or not actor_subject
                or not isinstance(room_id, str) or len(room_id) > 128
                or not IDENTIFIER_RE.fullmatch(room_id)):
            raise RuntimeStoreError('invalid_params')
        def write(conn):
            _epoch(conn, self.authority.epoch)
            key = _OWNER + room_id
            row = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
            if row is None and create:
                historical = conn.execute(
                    'SELECT 1 FROM hosted_rooms WHERE room_id=? UNION ALL '
                    'SELECT 1 FROM hosted_room_retired_ids WHERE room_id=?',
                    (room_id, room_id)).fetchone()
                if historical is None:
                    conn.execute('INSERT INTO state_meta(key,value) VALUES(?,?)', (key, actor_subject))
                    return True
            if row is None or row[0] != actor_subject:
                raise RuntimeStoreError('permission_denied')
            return True
        return write(conn) if conn is not None else self.authority.db._execute_write(write)

    def _owner(self, room_id):
        with self.authority.db._read_ctx() as conn:
            row = conn.execute('SELECT value FROM state_meta WHERE key=?', (_OWNER + room_id,)).fetchone()
        if row is None:
            raise RuntimeStoreError('permission_denied')
        return row[0]

    def _resolve_member_transport(self, binding, task):
        if self._member_is_peer(binding.room_id, str(task['payload'].get('target_member_id') or task['payload'].get('target_profile'))):
            return super()._resolve_member_transport(binding, task)
        from gateway.session_hosted_rpc import HostedRoomAuthorityRPC
        payload = task['payload']
        member = str(payload.get('target_member_id') or payload.get('target_profile'))
        profile = payload['target_profile']
        owner = self._owner(binding.room_id)
        home = self.profile_homes().get(profile)
        if home is None:
            raise RuntimeStoreError('permission_denied')
        key = binding.room_id, member, profile, owner, str(home)
        if key not in self.member_rpcs:
            if home != Path(self.authority.profile_id):
                from gateway.session_hosted_transport import HostedRoomOwnerRPC
                self.member_rpcs[key] = HostedRoomOwnerRPC(home=home,
                    source_home=self.authority.profile_id, room_id=binding.room_id,
                    member_id=member, profile=profile)
                return self.member_rpcs[key]
            def authorized(conn, operation, identity, generation):
                # Admission checks must share the FIFO writer's snapshot. Opening
                # another transaction here would reintroduce the revocation race.
                from gateway.hosted_rooms import _room_from_row
                from gateway.hosted_room_driver import _task_from_row
                _epoch(conn, self.authority.epoch)
                owned = conn.execute('SELECT value FROM state_meta WHERE key=?',
                                     (_OWNER + binding.room_id,)).fetchone()
                if owned is None or owned[0] != owner:
                    return False
                stored = conn.execute('SELECT * FROM hosted_rooms WHERE room_id=?',
                                      (binding.room_id,)).fetchone()
                if stored is None or stored['disbanded_at'] is not None:
                    return False
                room = _room_from_row(stored)
                if (room['authority_gateway_id'], room['authority_epoch']) != (binding.gateway_id, binding.authority_epoch):
                    return False
                members = room['members']
                if not any(m.get('member_id') == member and m.get('profile') == profile
                           and (m.get('target') is None or (
                               isinstance(m.get('target'), dict)
                               and m['target'].get('kind', 'local') == 'local'))
                           for m in members):
                    return False
                if self.profile_homes().get(profile) != home:
                    return False
                if identity is not None:
                    stored = conn.execute('SELECT * FROM hosted_room_driver_tasks WHERE room_id=? AND task_id=?',
                                          (binding.room_id, identity.task_id)).fetchone()
                    if stored is None:
                        return False
                    current = _task_from_row(stored)
                    return (current['identity'] == identity and current['execution_generation'] == generation
                            and current['payload'].get('target_profile') == profile
                            and current['payload'].get('target_member_id', profile) == member
                            and current['status'] in ({'running'} if operation in {'submit', 'execute'}
                                                       else {'running', 'stopping'}))
                return True
            def authorize(operation, identity, generation):
                with self.authority.db._read_ctx() as conn:
                    return authorized(conn, operation, identity, generation)
            def authorize_write(conn, identity, generation):
                # Raise to refuse rather than return False: a guard that only returns False
                # is ignored wherever the admission hook signals refusal by raising.
                if not authorized(conn, 'submit', identity, generation):
                    raise RuntimeStoreError('permission_denied')
                return True
            principal = Principal(owner, self.authority.profile_id,
                frozenset({'session:create', 'session:read', 'session:submit', 'session:control', 'session:approve'}),
                'hosted:' + binding.room_id + ':' + member)
            self.member_rpcs[key] = HostedRoomAuthorityRPC(self.authority, self.loop,
                room_id=binding.room_id, member_id=member, profile=profile, principal=principal, authorize=authorize,
                authorize_write=authorize_write)
        return self.member_rpcs[key]

    def check_admission(self, ref, row):
        """Reconstruct the private producer from durable task state before claim."""
        import json
        from gateway.hosted_room_driver import TaskIdentity, list_tasks
        from gateway.session_hosted_attachments import committed_submission_payload
        from tui_gateway.hosted_room_driver import HostedRoomBinding
        try:
            if not row['request_id'].startswith('hosted:'):
                raise ValueError('not a hosted admission')
            identity, generation = json.loads(row['request_id'][7:])
            identity = TaskIdentity(**identity)
            if type(generation) is not int or generation < 1:
                raise ValueError('invalid generation')
            owner = self._owner(identity.room_id)
            if owner != row['principal_id']:
                raise ValueError('foreign owner')
            room = self._room(identity.room_id)
            task = next(t for t in list_tasks(self.db_path, room_id=identity.room_id)
                        if t['identity'] == identity and t['execution_generation'] == generation)
            rpc = self._resolve_member_transport(HostedRoomBinding(identity.room_id,
                room['authority_gateway_id'], room['authority_epoch']), task)
            if (getattr(rpc, 'ref', None) != ref or task['status'] != 'running'
                    or row['payload'] != committed_submission_payload(rpc, task['payload']['prompt'], task['payload'].get('attachments'))
                    or rpc.authorizer('execute', identity, generation) is not True):
                raise ValueError('changed hosted binding')
            return task
        except (ValueError, TypeError, KeyError, StopIteration) as exc:
            raise RuntimeStoreError('permission_denied') from exc

    def approve(self, *, session_id, request_id, choice):
        rpc = next((r for r in self.member_rpcs.values() if r.ref.session_id == session_id), None)
        if rpc is None:
            raise RuntimeStoreError('permission_denied')
        return rpc.approve(session_id=session_id, request_id=request_id, choice=choice)


async def ensure_hosted_service(runner):
    """Prepare every transport before readiness can release any coordinator."""
    active = active_authority(runner)
    if active is None:
        raise RuntimeStoreError('profile_mismatch')
    coordinator = getattr(runner, 'runtime_custody', None)
    for authority in serving_authorities(runner):
        # This is the source-owned declaration that the profile will host TUI/room work.  Reserve
        # its exact I17 root before constructing the service or installing its transport, while a
        # reversible preservation close can still observe a real parent for a cold profile.
        if coordinator is not None:
            from tui_gateway.hosted_tui_custody import bind_authority
            bind_authority(authority, runner=runner, coordinator=coordinator)
        await _ensure_hosted_service(runner, authority)
    start_ready_hosted_services(runner)
    return active.hosted_room_service


async def _ensure_hosted_service(runner, authority):
    with owner_scope(authority):
        service = getattr(authority, 'hosted_room_service', None)
        if service is None:
            loop = asyncio.get_running_loop()
            service = await asyncio.to_thread(CanonicalHostedRoomService, authority, loop)
            authority.hosted_room_service = service
        if not getattr(service, '_transport_installed', False):
            from gateway.session_hosted_transport import install_hosted_transport
            install_hosted_transport(runner.session_control_server, authority, asyncio.get_running_loop(),
                                     attest=service.attest)
            service._transport_installed = True


def start_ready_hosted_services(runner):
    """Start only a fully prepared served set behind the published ready gate."""
    if (getattr(runner, 'session_runtime_descriptor', {}).get('state') != 'ready'
            or getattr(runner, '_draining', False)):
        return
    services = [getattr(authority, 'hosted_room_service', None) for authority in serving_authorities(runner)]
    if any(service is None or not getattr(service, '_transport_installed', False) for service in services):
        return
    for service in services:
        # start() only releases its thread; preparation and disk access happened above.
        from tui_gateway.hosted_tui_custody import bind_room_service, room_service_start_failed
        bind_room_service(service, service.authority)
        try:
            service.start()
        except BaseException:
            room_service_start_failed(service)
            raise
        service._i17_room_started = True


async def stop_hosted_service(runner, timeout=5):
    loop = asyncio.get_running_loop()
    deadline, stopped = loop.time() + max(0, timeout), True
    for authority in all_authorities(runner):
        service = getattr(authority, 'hosted_room_service', None)
        if service is not None:
            result = await asyncio.to_thread(service.stop, timeout=max(0, deadline - loop.time()))
            from tui_gateway.hosted_tui_custody import room_service_stop
            stopped = room_service_stop(service, result) and stopped
    return stopped


def _i09_service_pairs(runner):
    authorities = tuple(serving_authorities(runner))
    pairs = tuple((authority, getattr(authority, 'hosted_room_service', None))
                  for authority in authorities)
    if pairs and all(service is None for _authority, service in pairs):
        return ()
    if any(service is None for _authority, service in pairs):
        return None
    return pairs


def begin_i09_source_close(runner, close_generation: int) -> bool:
    """Close every configured hosted producer at the coordinator's exact generation."""
    pairs = _i09_service_pairs(runner)
    if pairs is None:
        return False
    begun = []
    for _authority, service in pairs:
        begin = getattr(service, 'begin_preservation_close', None)
        if not callable(begin) or not begin(close_generation):
            for previous in begun:
                previous.abort_preservation_close(close_generation)
            return False
        begun.append(service)
    return True


def abort_i09_source_close(runner, close_generation: int) -> bool:
    pairs = _i09_service_pairs(runner)
    if pairs is None:
        return False
    result = True
    for _authority, service in pairs:
        abort = getattr(service, 'abort_preservation_close', None)
        result = bool(callable(abort) and abort(close_generation)) and result
    return result


async def await_i09_source_close(runner, close_generation: int, timeout: float) -> bool:
    pairs = _i09_service_pairs(runner)
    if pairs is None:
        return False
    deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout))
    for _authority, service in pairs:
        wait = getattr(service, 'await_preservation_close', None)
        if not callable(wait):
            return False
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0 or not await asyncio.to_thread(wait, close_generation, remaining):
            return False
    return True


def preservation_i09_owner_ready(authority, custody: dict[str, Any]) -> bool:
    service = getattr(authority, 'hosted_room_service', None)
    generation = custody.get('close_generation') if isinstance(custody, dict) else None
    if service is None or type(generation) is not int:
        return False
    evidence = service.preservation_owner_evidence(
        process_instance=authority.instance_id, authority_epoch=authority.epoch,
        close_generation=generation, owner_id=f'{authority.instance_id}:I09_a2a_hosted_kanban')
    return evidence is not None


def preservation_i09_owner_evidence(
    authority, *, process_instance: str, authority_epoch: int,
    close_generation: int, owner_id: str,
) -> dict[str, Any] | None:
    service = getattr(authority, 'hosted_room_service', None)
    if service is None:
        return None
    return service.preservation_owner_evidence(
        process_instance=process_instance, authority_epoch=authority_epoch,
        close_generation=close_generation, owner_id=owner_id)
