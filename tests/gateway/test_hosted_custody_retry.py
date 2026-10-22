"""Managed hosted members retain one I09 operation across callback failure and retry."""

import pytest


def test_managed_hosted_callback_failure_keeps_exact_operation_for_retry(hosted_owner):
    from gateway.hosted_room_driver import TaskIdentity
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_hosted_rpc import HostedRoomAuthorityRPC
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import claim_session_input, list_session_admissions, settle_session_input

    authority, loop, principal, _ = hosted_owner
    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    rpc = HostedRoomAuthorityRPC(
        authority, loop, room_id='room', member_id='member', profile='default',
        principal=principal, authorize=lambda *_args: True,
    )
    coords = {'profile': 'default', 'source': 'bot_room'}
    sid = rpc.create(**coords, title='Group: room')['session_id']
    task = TaskIdentity('room', 'task', 'thread', 'turn')
    args = {
        **coords, 'session_id': sid, 'prompt': 'input', 'task': task,
        'execution_generation': 7,
    }

    first = rpc.submit(**args, on_terminal=lambda _receipt: (_ for _ in ()).throw(RuntimeError('ack')))
    [operation_key] = list(authority._preservation_operations)
    operation = authority._preservation_operations[operation_key]
    retained_handle = operation['handle']
    assert operation['owner_class'] == 'I09_a2a_hosted_kanban'
    identity = operation['source_identity']
    assert '"service":"hosted_room_member.v1"' in identity
    assert '"execution_generation":7' in identity
    assert operation['request_id'] == 'hosted:[{"room_id":"room","task_id":"task","thread_id":"thread","turn_id":"turn"},7]'

    row = claim_session_input(authority.db, epoch=authority.epoch, session_id=sid)
    settle_session_input(
        authority.db, epoch=authority.epoch, admission_id=row['admission_id'],
        generation=row['generation'], outcome='completed',
        result={'result': {'final_response': 'reply'}, 'usage': {}},
    )
    with pytest.raises(RuntimeError, match='ack'):
        rpc.history(**coords, session_id=sid)
    assert operation_key in authority._preservation_operations
    assert authority._preservation_operations[operation_key]['unknown'] is True
    assert authority._preservation_operations[operation_key]['handle'] == retained_handle
    assert row['admission_id'] in rpc.callbacks

    delivered = []
    retried = rpc.submit(**args, on_terminal=delivered.append)
    assert retried['admission_id'] == first['admission_id']
    assert delivered == [{
        'status': 'settled', 'text': 'reply', 'message_id': row['admission_id'],
        'settlement_id': row['admission_id'], 'task_id': 'task', 'execution_generation': 7,
    }]
    assert list(authority._preservation_operations) == []
    assert rpc.callbacks == {}
    assert [item['admission_id'] for item in list_session_admissions(
        authority.db, session_id=sid, pending_only=False)] == [row['admission_id']]
