"""The FIFO-close retry watcher preserves exact I07/I06 custody identities."""

from copy import copy
import asyncio

import pytest

pytest_plugins = ['tests.gateway.test_session_bot_retry']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'tamper', [
        'none', 'binding', 'root_handle', 'child_operation', 'missing_child',
        'watcher_tuple', 'extra_child',
    ],
)
async def test_fifo_close_callback_rejects_stale_or_replaced_custody_and_accepts_exact(
    bot, monkeypatch, tamper,
):
    """A real Bot admission's late callback cannot release a copied/replaced custody edge."""
    from hermes_state_runtime import get_session_admission, settle_session_input
    from tests.gateway.test_bot_delivery_custody import _install_custody, _start_held_bot_admission
    from tests.gateway.test_session_bot_retry import _settle_quietly

    _install_custody(bot.authority)
    held = await _start_held_bot_admission(
        bot, monkeypatch, ('f' if tamper == 'binding' else 'e') * 32,
    )
    authority = held['authority']
    admission_id = held['receipt']['admission_id']
    binding = authority._preservation_automation_bindings[admission_id]
    root = held['root']
    child_key = held['child_key']
    child = held['record']
    child_admission_id = child['admission_id']
    child_row = get_session_admission(authority.db, admission_id=child_admission_id)
    assert child_row['status'] == 'started'

    release_calls = []
    operation_calls = []
    release = authority._preservation_release_terminal
    operation_terminal = authority._preservation_operation_terminal
    monkeypatch.setattr(
        authority, '_preservation_release_terminal',
        lambda current: (release_calls.append(current), release(current))[1],
    )
    monkeypatch.setattr(
        authority, '_preservation_operation_terminal',
        lambda key, finalizer: (operation_calls.append((key, finalizer)), operation_terminal(key, finalizer))[1],
    )
    if tamper == 'none':
        # Keep the real custody graph and callback validation in play, but leave the held
        # worker's cleanup to its fixture rather than racing a second terminalizer against it.
        monkeypatch.setattr(
            authority, '_preservation_release_terminal',
            lambda current: (release_calls.append(current), True)[1],
        )
        monkeypatch.setattr(
            authority, '_preservation_operation_terminal',
            lambda key, finalizer: (operation_calls.append((key, finalizer)), True)[1],
        )

    # Replace only the callback trigger with a real loop Future.  The I07 root, I06 child,
    # coordinator edge, and durable row remain from the real Bot admission; the Future merely
    # lets this test fire the source-owned callback without completing the held worker first.
    delivery_id = binding['delivery_id']
    previous_watcher = authority._bot_reply_watchers.get(delivery_id)
    previous_waiter = authority.waiters.get(admission_id)
    callback_trigger = asyncio.get_running_loop().create_future()
    authority.waiters[admission_id] = callback_trigger
    authority._bot_reply_watchers[delivery_id] = (admission_id, callback_trigger, callback_trigger)
    assert authority._preservation_watch_fifo_close(admission_id) is True
    settled = settle_session_input(
        authority.db,
        epoch=authority.epoch,
        admission_id=child_admission_id,
        generation=child_row['generation'],
        outcome='completed',
    )
    assert settled['status'] == 'terminal'

    replacement = None

    def replace_binding():
        authority._preservation_automation_bindings[admission_id] = dict(binding)

    def replace_root_handle():
        binding['handle'] = copy(root)

    def replace_child_operation():
        authority._preservation_operations[child_key] = dict(child)

    def replace_watcher_tuple():
        nonlocal replacement
        replacement = asyncio.get_running_loop().create_future()
        authority._bot_reply_watchers[delivery_id] = (
            admission_id, replacement, replacement,
        )
        authority.waiters[admission_id] = replacement

    def add_extra_child():
        authority._preservation_operations['extra-child'] = dict(child)

    def remove_child_operation():
        authority._preservation_operations.pop(child_key)

    {
        'none': lambda: None,
        'binding': replace_binding,
        'root_handle': replace_root_handle,
        'child_operation': replace_child_operation,
        'watcher_tuple': replace_watcher_tuple,
        'extra_child': add_extra_child,
        'missing_child': remove_child_operation,
    }[tamper]()

    try:
        callback_trigger.set_result(None)
        await asyncio.sleep(0)
        if tamper == 'none':
            assert release_calls == [admission_id]
            assert operation_calls == [(child_key, 'fifo-close:' + admission_id)]
        else:
            assert release_calls == []
            assert operation_calls == []
        assert authority._preservation_handles.get(admission_id) is root
        if tamper == 'missing_child':
            assert child_key not in authority._preservation_operations
        else:
            assert authority._preservation_operations.get(child_key) is not None
    finally:
        # Restore the exact objects before the fixture's normal cleanup path so this negative test
        # never relies on dropping custody to terminate its real worker.
        authority._preservation_automation_bindings[admission_id] = binding
        binding['handle'] = root
        authority._preservation_operations[child_key] = child
        if tamper == 'watcher_tuple':
            replacement.cancel()
        authority._preservation_operations.pop('extra-child', None)
        if previous_watcher is None:
            authority._bot_reply_watchers.pop(delivery_id, None)
        else:
            authority._bot_reply_watchers[delivery_id] = previous_watcher
        if previous_waiter is None:
            authority.waiters.pop(admission_id, None)
        else:
            authority.waiters[admission_id] = previous_waiter
        held['finish'].set()
        await _settle_quietly(bot)


@pytest.mark.asyncio
async def test_fifo_close_pending_child_rearms_without_poisoning_parent(bot, monkeypatch):
    """A real held Bot child keeps the I07 root retryable while its FIFO lease is pending."""
    from hermes_state_runtime import get_session_admission, settle_session_input
    from tests.gateway.test_bot_delivery_custody import _install_custody, _start_held_bot_admission
    from tests.gateway.test_session_bot_retry import _settle_quietly

    _install_custody(bot.authority)
    held = await _start_held_bot_admission(bot, monkeypatch, 'b' * 32)
    authority = held['authority']
    admission_id = held['receipt']['admission_id']
    root = held['root']
    coordinator = held['coordinator']
    try:
        child_key = held['child_key']
        child = held['record']
        child_row = get_session_admission(authority.db, admission_id=child['admission_id'])
        settled = settle_session_input(
            authority.db,
            epoch=authority.epoch,
            admission_id=child['admission_id'],
            generation=child_row['generation'],
            outcome='completed',
        )
        assert settled['status'] == 'terminal'
        result = authority._preservation_release_terminal(admission_id)
        assert result is False
        assert authority._preservation_operations.get(child_key) is child
        assert authority._preservation_handles.get(admission_id) is root
        record = coordinator._records[root.lease.lease_id]
        assert record.handle is root and record.state != 'unknown'
    finally:
        held['finish'].set()
    await _settle_quietly(bot)


@pytest.mark.asyncio
async def test_fifo_close_survives_normal_watch_reply_cleanup(bot, monkeypatch):
    """The source watcher's cleanup may remove its map entry before FIFO retry runs."""
    from gateway import session_bot
    from hermes_state_runtime import get_session_admission, settle_session_input
    from tests.gateway.test_bot_delivery_custody import _install_custody, _start_held_bot_admission
    from tests.gateway.test_session_bot_retry import _settle_quietly

    async def wait_for_reply(_authority, _home, _key, future):
        await future

    monkeypatch.setattr(session_bot, '_record_reply', wait_for_reply)
    _install_custody(bot.authority)
    held = await _start_held_bot_admission(bot, monkeypatch, 'c' * 32)
    authority = held['authority']
    admission_id = held['receipt']['admission_id']
    binding = authority._preservation_automation_bindings[admission_id]
    delivery_id = held['receipt']['delivery_id']
    # The legacy fixture's I07 descriptor omits the mailbox projection key; bind the real
    # delivery identity before exercising the production reply watcher so the source map and
    # preservation seam refer to the same authenticated delivery.
    binding['delivery_id'] = delivery_id
    binding['descriptor'] = {**binding['descriptor'], 'delivery_id': delivery_id}
    watcher = authority._bot_reply_watchers[delivery_id]
    assert watcher[0] == admission_id

    release_calls = []
    operation_calls = []
    monkeypatch.setattr(
        authority, '_preservation_release_terminal',
        lambda current: (release_calls.append(current), True)[1],
    )
    monkeypatch.setattr(
        authority, '_preservation_operation_terminal',
        lambda key, finalizer: (operation_calls.append((key, finalizer)), True)[1],
    )

    child = held['record']
    child_row = get_session_admission(authority.db, admission_id=child['admission_id'])
    settled = settle_session_input(
        authority.db,
        epoch=authority.epoch,
        admission_id=child['admission_id'],
        generation=child_row['generation'],
        outcome='completed',
    )
    assert settled['status'] == 'terminal'
    assert authority._preservation_watch_fifo_close(admission_id) is True

    watcher[2].set_result(None)
    from tests.waiting import async_wait_until
    await async_wait_until(
        lambda: bool(release_calls),
        timeout=5,
        what='FIFO release after normal reply-watcher cleanup',
    )
    assert admission_id in release_calls
    assert (held['child_key'], 'fifo-close:' + admission_id) in operation_calls
    assert delivery_id not in authority._bot_reply_watchers
    assert authority._preservation_handles.get(admission_id) is held['root']
    held['finish'].set()
    await _settle_quietly(bot)


@pytest.mark.asyncio
async def test_fifo_close_terminalization_retires_exact_automation_child_index(bot, monkeypatch):
    """A proven terminal child retires only its matching automation-child index."""
    from hermes_state_runtime import get_session_admission, settle_session_input
    from tests.gateway.test_bot_delivery_custody import _start_held_bot_admission
    from tests.gateway.test_session_bot_retry import _settle_quietly

    held = await _start_held_bot_admission(bot, monkeypatch, 'd' * 32)
    authority = held['authority']
    admission_id = held['receipt']['admission_id']
    child_key = held['child_key']
    child = held['record']
    try:
        child_row = get_session_admission(authority.db, admission_id=child['admission_id'])
        assert child_row['status'] == 'started'
        settled = settle_session_input(
            authority.db,
            epoch=authority.epoch,
            admission_id=child['admission_id'],
            generation=child_row['generation'],
            outcome='completed',
        )
        assert settled['status'] == 'terminal'
        assert authority._preservation_operation_terminal(
            child_key, 'fifo-close:' + admission_id,
        ) is True
        assert child_key not in authority._preservation_operations
        assert authority._preservation_automation_children.get(admission_id) is None
        assert authority._preservation_handles.get(admission_id) is held['root']
    finally:
        held['finish'].set()
    await _settle_quietly(bot)
