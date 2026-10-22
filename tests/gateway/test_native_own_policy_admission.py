"""Real Yuanbao own-policy admission and restart/replacement controls.

The fixture starts the configured adapter before READY and only replaces the
outbound websocket/sign-token transport with an owned loopback. The controls
therefore exercise the source token, native callback, authority admission and
durable route together; no test mints a token or a terminal authorization flag.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
import json

import pytest

from gateway.config import Platform
import gateway.session_envelope as _session_envelope
from gateway.session_envelope import check_native_route, snapshot_native
from gateway.session_ingress import executing_admission
from hermes_state_runtime import RuntimeStoreError
from plugins.platforms.yuanbao import yuanbao_proto

_ORIGINAL_PREPARE_NATIVE = _session_envelope.prepare_native


@pytest.fixture(autouse=True)
def _restore_test_seams():
    yield
    _session_envelope.prepare_native = _ORIGINAL_PREPARE_NATIVE


async def _wait_until(predicate, *, timeout=20.0, what='condition'):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f'timed out waiting for {what}')
        await asyncio.sleep(0.01)


def _push_data(sender: str, message_id: str, body: list[dict], group_code: str = '') -> bytes:
    return yuanbao_proto._encode_parts([
        (2, 's', sender),
        (6, 's', group_code),
        (12, 's', message_id),
        (13, 'b', body),
    ])


def _push(sender: str, message_id: str, text: str = 'native policy turn') -> bytes:
    data = _push_data(sender, message_id, [
        {'msg_type': 'TIMTextElem', 'msg_content': {'text': text}},
    ])
    decoded = yuanbao_proto.decode_inbound_push(data)
    assert decoded and decoded['from_account'] == sender and decoded['msg_id'] == message_id
    return yuanbao_proto.encode_conn_msg_full(
        cmd_type=yuanbao_proto.CMD_TYPE['Push'], cmd='push', seq_no=1,
        msg_id='', module='', data=data, need_ack=True,
    )


async def _deliver(connection, sender, message_id, text='native policy turn'):
    raw = _push(sender, message_id, text)
    data = yuanbao_proto.decode_conn_msg(raw)['data']
    await connection._handle_frame(raw)
    connection._flush_inbound_buffer(connection._extract_sender_key(data))


def _group_push(sender: str, message_id: str, group_code: str, bot_id: str) -> bytes:
    data = _push_data(sender, message_id, [
        {'msg_type': 'TIMTextElem', 'msg_content': {'text': 'native group policy turn'}},
        {'msg_type': 'TIMCustomElem', 'msg_content': {'data': json.dumps({
            'elem_type': 1002, 'user_id': bot_id, 'text': '@synthetic-yuanbao-bot',
        })}},
    ], group_code)
    decoded = yuanbao_proto.decode_inbound_push(data)
    assert decoded and decoded['from_account'] == sender and decoded['group_code'] == group_code
    return yuanbao_proto.encode_conn_msg_full(
        cmd_type=yuanbao_proto.CMD_TYPE['Push'], cmd='push', seq_no=1,
        msg_id='', module='', data=data, need_ack=True,
    )


async def _deliver_group(connection, sender: str, message_id: str, group_code: str, bot_id: str):
    raw = _group_push(sender, message_id, group_code, bot_id)
    data = yuanbao_proto.decode_conn_msg(raw)['data']
    await connection._handle_frame(raw)
    connection._flush_inbound_buffer(connection._extract_sender_key(data))


def _unknown_tokens(adapter, prior_ids: set[int]) -> list[object]:
    return [
        token
        for token in adapter._preservation_source_close.active_leases
        if id(token) not in prior_ids and token.state == "unknown"
    ]


def _prepare_observation() -> dict[str, object]:
    return {
        "entered": 0,
        "stage": None,
        "reason": None,
        "field": None,
        "returned_payloads": [],
        "handoff_present": False,
    }


def _record_prepare_error(observation: dict[str, object], error: RuntimeStoreError) -> None:
    observation["reason"] = error.reason
    observation["field"] = getattr(error, "field", None)


@pytest.mark.asyncio
async def test_yuanbao_own_policy_positive_uses_real_handoff_and_descriptor(started_yuanbao):
    runner, _adapter, _loop, call, connection = started_yuanbao
    payloads: list[object] = []
    phases: list[bool] = []
    events: list[object] = []

    async def install_observers():
        original_prepare_native = _session_envelope.prepare_native
        original_runner_handler = runner._handle_message

        async def capture_prepare(owner, event):
            payload = await original_prepare_native(owner, event)
            payloads.append(payload)
            return payload

        async def observe_runner(event):
            phases.append(bool(executing_admission.get()))
            events.append(event)
            return await original_runner_handler(event)

        _session_envelope.prepare_native = capture_prepare
        runner._handle_message = observe_runner

    call(install_observers())
    call(_deliver(
        connection,
        "synthetic-yuanbao-user",
        "r19-positive",
        "positive",
    ))
    call(
        _wait_until(
            lambda: bool(payloads)
            and events
            and False in phases
            and True in phases,
            what="the admitted payload and both fresh/replay handler phases",
        )
    )

    assert len(payloads) == 1
    assert phases[0] is False
    assert True in phases
    assert events

    envelope = payloads[0]['native_text_v1']
    assert envelope['reauthorize'] == 'own_policy'
    assert envelope['source']['user_id'] == 'synthetic-yuanbao-user'
    with pytest.raises(RuntimeStoreError) as direct_refused:
        snapshot_native(runner, events[0])
    assert direct_refused.value.reason == 'permission_denied'



@pytest.mark.asyncio
async def test_yuanbao_own_policy_rechecks_revocation_after_source_handoff(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    prior_ids = {id(token) for token in adapter._preservation_source_close.active_leases}
    observation = _prepare_observation()
    phases: list[bool] = []

    async def install_revocation():
        from gateway.preservation_source_close import current_source_handoff

        original_prepare_native = _session_envelope.prepare_native
        original_runner_handler = runner._handle_message
        original_allow_from = list(adapter._access_policy._allow_from)

        async def capture_prepare(owner, event):
            observation["entered"] = int(observation["entered"]) + 1
            observation["stage"] = "prepare_native.entered"
            observation["handoff_present"] = bool(current_source_handoff())
            adapter._access_policy._allow_from.clear()
            try:
                payload = await original_prepare_native(owner, event)
            except RuntimeStoreError as error:
                _record_prepare_error(observation, error)
                observation["stage"] = "prepare_native.refused"
                raise
            else:
                observation["returned_payloads"].append(payload)
                observation["stage"] = "prepare_native.returned"
                return payload
            finally:
                adapter._access_policy._allow_from[:] = original_allow_from

        async def observe_runner(event):
            phases.append(bool(executing_admission.get()))
            return await original_runner_handler(event)

        _session_envelope.prepare_native = capture_prepare
        runner._handle_message = observe_runner

    call(install_revocation())
    call(_deliver(
        connection,
        "synthetic-yuanbao-user",
        "r19-revocation",
        "revoked",
    ))
    call(
        _wait_until(
            lambda: int(observation["entered"]) == 1 and observation["reason"] is not None,
            what="the real prepare_native revocation refusal",
        )
    )
    call(
        _wait_until(
            lambda: bool(_unknown_tokens(adapter, prior_ids)),
            what="the retained UNKNOWN revocation token",
        )
    )

    assert observation["stage"] == "prepare_native.refused"
    assert observation["reason"] == "permission_denied"
    assert observation["field"] == "user_not_authorized"
    assert observation["handoff_present"] is True
    assert observation["returned_payloads"] == []
    assert phases and phases[0] is False
    assert True not in phases
    assert len(_unknown_tokens(adapter, prior_ids)) == 1



@pytest.mark.asyncio
async def test_yuanbao_own_policy_rejects_unauthorized_sender_before_agent(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    events = []
    prior_tokens = {id(token) for token in adapter._preservation_source_close.active_leases}

    async def install():
        original_handle = runner._handle_message

        async def observe(event):
            events.append(event)
            return await original_handle(event)

        runner._handle_message = observe

    call(install())
    call(_deliver(connection, 'unauthorized-yuanbao-user', 'r16-unauthorized'))
    call(_wait_until(
        lambda: any(
            id(token) not in prior_tokens and token.state == 'unknown'
            for token in adapter._preservation_source_close.active_leases
        ),
        what='unauthorized source custody',
    ))
    assert events == []
    new_unknown = tuple(
        token for token in adapter._preservation_source_close.active_leases
        if id(token) not in prior_tokens and token.state == 'unknown'
    )
    assert new_unknown
    assert all(
        adapter._preservation_source_close._unknown_sources.get(token) == 'yuanbao_pipeline_no_dispatch'
        for token in new_unknown
    )
    assert not any(
        token.event is not None and getattr(token.event.source, 'user_id', None) == 'unauthorized-yuanbao-user'
        for token in adapter._preservation_source_close.active_leases
    )


@pytest.mark.asyncio
async def test_yuanbao_restart_route_uses_fresh_live_policy(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    events, payloads = [], []

    async def install():
        import gateway.session_envelope as envelope

        original_prepare = envelope.prepare_native
        original_handle = runner._handle_message

        async def capture_prepare(owner, event):
            payload = await original_prepare(owner, event)
            payloads.append(payload)
            return payload

        async def observe(event):
            events.append(event)
            return await original_handle(event)

        envelope.prepare_native = capture_prepare
        runner._handle_message = observe

    call(install())
    call(_deliver(connection, 'synthetic-yuanbao-user', 'r16-restart'))
    call(_wait_until(lambda: bool(payloads), what='restart payload'))

    def _route_entry():
        route = payloads[0]['native_text_v1']['route']
        return route, runner.session_store.lookup_by_session_key(route)

    def _canonical_restart_payload():
        _, entry = _route_entry()
        if entry is None:
            return False
        from hermes_state_runtime import list_session_admissions
        return any(
            row.get('payload') == payloads[0]
            for row in list_session_admissions(
                runner.session_authority.db,
                session_id=entry.session_id,
                pending_only=False,
            )
        )

    call(_wait_until(_canonical_restart_payload, what='durable restart admission'))
    route, entry = _route_entry()
    source = events[0].source

    async def check_allowed():
        return await check_native_route(runner, payloads[0], entry.session_id, source, adapter)

    checked_source, checked_route = call(check_allowed())
    assert checked_source.user_id == 'synthetic-yuanbao-user'
    assert checked_route == route

    async def check_revoked():
        adapter._access_policy._allow_from.clear()
        try:
            await check_native_route(runner, payloads[0], entry.session_id, source, adapter)
        finally:
            adapter._access_policy._allow_from[:] = ['synthetic-yuanbao-user']

    with pytest.raises(RuntimeStoreError) as refused:
        call(check_revoked())
    assert refused.value.reason == 'permission_denied'


@pytest.mark.asyncio
async def test_yuanbao_restart_rejects_copied_source_and_replaced_adapter(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    events, payloads = [], []

    async def install():
        import gateway.session_envelope as envelope

        original_prepare = envelope.prepare_native
        original_handle = runner._handle_message

        async def capture_prepare(owner, event):
            payload = await original_prepare(owner, event)
            payloads.append(payload)
            return payload

        async def observe(event):
            events.append(event)
            return await original_handle(event)

        envelope.prepare_native = capture_prepare
        runner._handle_message = observe

    call(install())
    call(_deliver(connection, 'synthetic-yuanbao-user', 'r16-replaced'))
    call(_wait_until(lambda: bool(payloads), what='replacement payload'))
    route = payloads[0]['native_text_v1']['route']
    entry = runner.session_store.lookup_by_session_key(route)
    assert entry is not None

    copied = deepcopy(payloads[0])
    copied['native_text_v1']['source']['user_id'] = 'copied-yuanbao-user'

    async def check_copied():
        await check_native_route(runner, copied, entry.session_id, events[0].source, adapter)

    with pytest.raises(RuntimeStoreError):
        call(check_copied())

    forged_descriptor = deepcopy(payloads[0])
    forged_descriptor['native_text_v1']['event']['message_id'] = 'forged-descriptor'

    async def check_forged_descriptor():
        await check_native_route(
            runner, forged_descriptor, entry.session_id, events[0].source, adapter,
        )

    with pytest.raises(RuntimeStoreError):
        call(check_forged_descriptor())

    stale = deepcopy(payloads[0])
    stale['native_text_v1']['source']['profile'] = 'stale-profile'

    async def check_stale():
        await check_native_route(runner, stale, entry.session_id, events[0].source, adapter)

    with pytest.raises(RuntimeStoreError):
        call(check_stale())

    async def check_replaced():
        from plugins.platforms.yuanbao.adapter import YuanbaoAdapter

        replacement = YuanbaoAdapter(adapter.config)
        runner.adapters[Platform.YUANBAO] = replacement
        try:
            await check_native_route(runner, payloads[0], entry.session_id, events[0].source, adapter)
        finally:
            runner.adapters[Platform.YUANBAO] = adapter

    with pytest.raises(RuntimeStoreError):
        call(check_replaced())


@pytest.mark.asyncio
async def test_yuanbao_group_policy_rechecks_revocation_after_restart(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    events, payloads = [], []

    async def install():
        import gateway.session_envelope as envelope

        original_prepare = envelope.prepare_native
        original_handle = runner._handle_message

        async def capture_prepare(owner, event):
            payload = await original_prepare(owner, event)
            payloads.append(payload)
            return payload

        async def observe(event):
            events.append(event)
            return await original_handle(event)

        envelope.prepare_native = capture_prepare
        runner._handle_message = observe

    call(install())
    call(_deliver_group(
        connection, 'synthetic-yuanbao-group-user', 'r17-group-restart',
        'synthetic-group', 'synthetic-yuanbao-bot',
    ))
    def _canonical_group_payload():
        if not payloads:
            return False
        route = payloads[0]['native_text_v1']['route']
        entry = runner.session_store.lookup_by_session_key(route)
        if entry is None:
            return False
        from hermes_state_runtime import list_session_admissions
        return any(
            row.get('payload') == payloads[0]
            for row in list_session_admissions(
                runner.session_authority.db,
                session_id=entry.session_id,
                pending_only=False,
            )
        )

    call(_wait_until(_canonical_group_payload, what='group native payload'))
    assert events and events[0].source.chat_type == 'group'

    route = payloads[0]['native_text_v1']['route']
    entry = runner.session_store.lookup_by_session_key(route)
    assert entry is not None
    source = events[0].source

    async def check_allowed():
        return await check_native_route(runner, payloads[0], entry.session_id, source, adapter)

    checked_source, checked_route = call(check_allowed())
    assert checked_source.chat_id == 'group:synthetic-group'
    assert checked_route == route

    async def check_revoked():
        adapter._access_policy._group_allow_from.clear()
        try:
            await check_native_route(runner, payloads[0], entry.session_id, source, adapter)
        finally:
            adapter._access_policy._group_allow_from[:] = ['synthetic-group']

    with pytest.raises(RuntimeStoreError) as refused:
        call(check_revoked())
    assert refused.value.reason == 'permission_denied'


@pytest.mark.asyncio
async def test_yuanbao_handoff_rejects_active_same_instance_handler_mutation(started_yuanbao):
    runner, adapter, _loop, call, connection = started_yuanbao
    prior_ids = {id(token) for token in adapter._preservation_source_close.active_leases}
    observation = _prepare_observation()
    phases: list[bool] = []

    async def install_mutation():
        from gateway.preservation_source_close import current_source_handoff

        original_prepare_native = _session_envelope.prepare_native
        original_runner_handler = runner._handle_message

        async def capture_prepare(owner, event):
            observation["entered"] = int(observation["entered"]) + 1
            observation["stage"] = "prepare_native.entered"
            observation["handoff_present"] = bool(current_source_handoff())
            original_adapter_handler = adapter.handle_message

            async def replaced_adapter_handler(replaced_event):
                return await original_adapter_handler(replaced_event)

            adapter.handle_message = replaced_adapter_handler
            try:
                payload = await original_prepare_native(owner, event)
            except RuntimeStoreError as error:
                _record_prepare_error(observation, error)
                observation["stage"] = "prepare_native.refused"
                raise
            else:
                observation["returned_payloads"].append(payload)
                observation["stage"] = "prepare_native.returned"
                return payload
            finally:
                adapter.handle_message = original_adapter_handler

        async def observe_runner(event):
            phases.append(bool(executing_admission.get()))
            return await original_runner_handler(event)

        _session_envelope.prepare_native = capture_prepare
        runner._handle_message = observe_runner

    call(install_mutation())
    call(_deliver(
        connection,
        "synthetic-yuanbao-user",
        "r19-mutation",
        "mutated",
    ))
    call(
        _wait_until(
            lambda: int(observation["entered"]) == 1 and observation["reason"] is not None,
            what="the real prepare_native handler-mutation refusal",
        )
    )
    call(
        _wait_until(
            lambda: bool(_unknown_tokens(adapter, prior_ids)),
            what="the retained UNKNOWN mutation token",
        )
    )

    assert observation["stage"] == "prepare_native.refused"
    assert observation["reason"] == "permission_denied"
    assert observation["field"] == "source_handoff"
    assert observation["handoff_present"] is True
    assert observation["returned_payloads"] == []
    assert phases and phases[0] is False
    assert True not in phases
    assert len(_unknown_tokens(adapter, prior_ids)) == 1
    assert callable(adapter.handle_message)

