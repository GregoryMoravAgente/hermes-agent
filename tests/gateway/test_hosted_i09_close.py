"""Configured hosted services prove an exact no-work I09 close from their source graph."""


def test_configured_hosted_service_seals_exact_no_work_generation(hosted_owner):
    from gateway import hosted_rooms
    from gateway.session_hosted_service import CanonicalHostedRoomService

    authority, loop, _principal, _agent = hosted_owner
    service = CanonicalHostedRoomService(authority, loop)
    authority.hosted_room_service = service
    service._transport_installed = True
    hosted_rooms.create_room(
        service.db_path, room_id='idle-room', name='Idle room',
        authority_gateway_id=hosted_rooms.local_authority_gateway_id(),
        members=[{'member_id': 'default', 'profile': 'default', 'handle': 'idle'}])
    service.start()
    try:
        assert service.begin_preservation_close(7)
        assert service.preservation_owner_evidence(
            process_instance=authority.instance_id, authority_epoch=authority.epoch,
            close_generation=6, owner_id='stale') is None
        assert service.await_preservation_close(7, 5)
        evidence = service.preservation_owner_evidence(
            process_instance=authority.instance_id, authority_epoch=authority.epoch,
            close_generation=7, owner_id='first:I09_a2a_hosted_kanban')
        assert evidence is not None
        assert evidence['registered_rooms'] == ('idle-room',)
        assert evidence['registered_peer_routes'] == ()
        assert evidence['terminal_tasks_published'] == 0
        assert evidence['active_tasks'] == 0
        assert evidence['pending_acks'] == 0
        assert evidence['worker_process_generation'] == service.runtime.process_generation
    finally:
        assert service.abort_preservation_close(7)
        assert service.stop(timeout=5)


def test_configured_hosted_service_close_waits_for_real_room_worker(hosted_owner, monkeypatch):
    """The close fence waits for an accepted room task in the canonical worker."""
    import asyncio
    from pathlib import Path
    import threading

    from gateway import session_finite
    from gateway.session_authority import SessionAuthority
    from gateway.session_hosted_service import CanonicalHostedRoomService

    authority, loop, principal, _agent = hosted_owner
    service = CanonicalHostedRoomService(authority, loop)
    authority.hosted_room_service = service
    service._transport_installed = True
    monkeypatch.setattr(
        service, 'profile_homes',
        lambda: {'default': Path(authority.profile_id), 'other': Path(authority.profile_id)})
    service.authorize_room(principal.subject, 'busy-room', create=True)
    service.create_room(
        room_id='busy-room', name='Busy room',
        members=[
            {'member_id': 'default', 'profile': 'default', 'handle': 'default'},
            {'member_id': 'member', 'profile': 'other', 'handle': 'member'},
        ])

    started = threading.Event()
    release = asyncio.Event()

    async def held_provider(_authority, _ref, _row):
        started.set()
        await release.wait()
        return 'worker reply'

    monkeypatch.setattr(session_finite, 'execute_finite_admission', held_provider)
    monkeypatch.setattr(authority, '_schedule', SessionAuthority._schedule.__get__(authority))
    service.start()
    closing = False
    try:
        sent = service.send(
            room_id='busy-room', event_id='input',
            payload={'text': '@default hello', 'thread_id': 'thread'})
        assert sent['event_id'] == 'input'
        assert started.wait(10), f"status={service.runtime.status()}"

        # The real room producer must retain its exact source-owned handle while the
        # worker is still running.  This observes the admission record created by
        # ``send``; it does not seed a custody map or manufacture a task.
        input_records = [
            record for record in authority._preservation_operations.values()
            if record.get('owner_class') == 'I09_a2a_hosted_kanban'
            and record.get('operation') == 'hosted-room-input'
        ]
        assert len(input_records) == 1
        input_record = input_records[0]
        retained_handle = input_record['handle']
        assert input_record['request_id'] == 'input'
        assert input_record['source_identity'].find('"event_id":"input"') >= 0
        assert service._preservation_active_operations() >= 1

        assert service.begin_preservation_close(13)
        closing = True
        status = service.runtime.status()
        assert status['current_tasks'] or status['room_threads'], status
        assert service._preservation_inventory(13) is None

        loop.call_soon_threadsafe(release.set)
        assert service.await_preservation_close(13, 10)
        evidence = service.preservation_owner_evidence(
            process_instance=authority.instance_id, authority_epoch=authority.epoch,
            close_generation=13, owner_id='first:I09_a2a_hosted_kanban')
        assert evidence is not None
        assert evidence['active_tasks'] == 0
        assert evidence['pending_acks'] == 0
        assert evidence['active_operations'] == 0
        assert evidence['terminal_tasks_published'] >= 1
        assert all(
            record.get('handle') is not retained_handle
            for record in authority._preservation_operations.values()
            if record.get('owner_class') == 'I09_a2a_hosted_kanban'
        )
    finally:
        loop.call_soon_threadsafe(release.set)
        if closing:
            assert service.abort_preservation_close(13)
        assert service.stop(timeout=10)


def test_configured_hosted_service_close_waits_for_real_callback_ack(hosted_owner, monkeypatch):
    """A terminal room callback remains a close fence until the consumer returns."""
    from pathlib import Path
    import threading

    from gateway import session_finite
    from gateway.session_authority import SessionAuthority
    from gateway.session_hosted_service import CanonicalHostedRoomService

    authority, loop, principal, _agent = hosted_owner
    service = CanonicalHostedRoomService(authority, loop)
    authority.hosted_room_service = service
    service._transport_installed = True
    monkeypatch.setattr(
        service, 'profile_homes',
        lambda: {'default': Path(authority.profile_id), 'other': Path(authority.profile_id)})
    service.authorize_room(principal.subject, 'ack-room', create=True)
    service.create_room(
        room_id='ack-room', name='Ack room',
        members=[
            {'member_id': 'default', 'profile': 'default', 'handle': 'default'},
            {'member_id': 'member', 'profile': 'other', 'handle': 'member'},
        ])

    callback_entered = threading.Event()
    callback_release = threading.Event()

    async def immediate_provider(_authority, _ref, _row):
        return 'ack reply'

    monkeypatch.setattr(session_finite, 'execute_finite_admission', immediate_provider)
    monkeypatch.setattr(authority, '_schedule', SessionAuthority._schedule.__get__(authority))
    original_on_terminal = service.runtime._on_terminal

    def held_callback(binding, attempt, receipt):
        callback_entered.set()
        assert callback_release.wait(10), 'test did not release the terminal callback'
        original_on_terminal(binding, attempt, receipt)

    monkeypatch.setattr(service.runtime, '_on_terminal', held_callback)
    service.start()
    closing = False
    try:
        sent = service.send(
            room_id='ack-room', event_id='input',
            payload={'text': '@default hello', 'thread_id': 'thread'})
        assert sent['event_id'] == 'input'
        assert callback_entered.wait(10), service.runtime.status()
        ack_input_records = [
            record for record in authority._preservation_operations.values()
            if record.get('owner_class') == 'I09_a2a_hosted_kanban'
            and record.get('operation') == 'hosted-room-input'
        ]
        assert len(ack_input_records) == 1
        assert service._preservation_active_operations() >= 1

        assert service.begin_preservation_close(17)
        closing = True
        assert service._preservation_pending_acks() == 1
        assert service._preservation_inventory(17) is None

        callback_release.set()
        assert service.await_preservation_close(17, 10)
        evidence = service.preservation_owner_evidence(
            process_instance=authority.instance_id, authority_epoch=authority.epoch,
            close_generation=17, owner_id='first:I09_a2a_hosted_kanban')
        assert evidence is not None
        assert evidence['active_tasks'] == 0
        assert evidence['pending_acks'] == 0
        assert evidence['active_operations'] == 0
        assert evidence['terminal_tasks_published'] >= 1
    finally:
        callback_release.set()
        if closing:
            assert service.abort_preservation_close(17)
        assert service.stop(timeout=10)
