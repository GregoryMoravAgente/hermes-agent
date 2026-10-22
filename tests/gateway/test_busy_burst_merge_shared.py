"""A busy same-sender burst is one authority admission, as main merged it into one turn (N54).

Main merged a busy photo burst into the pending slot and debounced queue-mode split text from one
sender. Under the session authority every busy input was admitted on its own, so a thought split
over two messages (or a photo burst) ran as separate turns. Real GatewayRunner, DiscordAdapter,
SessionAuthority and drain over SQLite; the running user turn is a stand-in holding the slot.
"""
import asyncio
import threading
import time

import pytest

from tests.waiting import async_wait_until

PNG = b'\x89PNG\r\n\x1a\n' + b'\x00' * 24


async def _real_discord_runtime(tmp_path, monkeypatch):
    """Build the supported profile-owned runtime used by the burst producer."""
    from types import SimpleNamespace

    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    import hermes_state
    from plugins.platforms.discord.adapter import DiscordAdapter

    home = (tmp_path / 'profile').resolve()
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42,43')
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', hermes_state._IMPORT_DEFAULT_DB_PATH)
    home_token = set_hermes_home_override(home)
    reserved = False
    runner = None

    def cleanup():
        try:
            if runner is not None:
                store = getattr(runner, 'session_store', None)
                if store is not None:
                    store.close_all_db_handles()
                runner.close_all_session_db_handles()
        finally:
            if reserved:
                process_ownership.close()
            reset_hermes_home_override(home_token)

    try:
        process_ownership.reserve([home])
        reserved = True
        runner = GatewayRunner(
            GatewayConfig(sessions_dir=home / 'sessions', group_sessions_per_user=False)
        )
        adapter = DiscordAdapter(
            PlatformConfig(
                enabled=True,
                token='fixture-token',
                typing_indicator=False,
                extra={'group_sessions_per_user': False},
            )
        )
        adapter.gateway_runner = runner
        runner.adapters = {Platform.DISCORD: adapter}
        await initialize_gateway_runtime(runner)
        runner._wire_adapter_handlers(adapter, busy_text_mode='queue')
        return SimpleNamespace(
            runner=runner, adapter=adapter, authority=runner.session_authority, cleanup=cleanup,
        )
    except BaseException:
        cleanup()
        raise

# (case, follow-ups as (sender, text, photo?), expected admitted (text, media count) after 'A')
CASES = [
    ('split-text', [('42', 'first half', False), ('42', 'second half', False)],
     [('first half\nsecond half', 0)]),
    ('photo-burst', [('42', 'look', True), ('42', '', True)], [('look', 2)]),
    # Different senders never merge, and arrival order holds.
    ('two-senders', [('42', 'mine', False), ('43', 'theirs', False)], [('mine', 0), ('theirs', 0)]),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('case,follow_ups,expected', CASES, ids=[c[0] for c in CASES])
async def test_busy_burst_is_admitted_once(monkeypatch, tmp_path, case, follow_ups, expected):
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.session_ingress_burst import holds_busy_burst
    from hermes_state_runtime import list_session_admissions

    from tests.fakes import fake_llm_provider
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home

    loop = asyncio.get_running_loop()
    running, finish = asyncio.Event(), asyncio.Event()
    provider_finish = threading.Event()
    executed = []
    executed_lock = threading.Lock()
    stream_delay = 0.001
    stream_deadline = [None]

    class _EventClock:
        def __init__(self):
            self._real = time
            self._stream_sleep_calls = 0

        def sleep(self, seconds):
            if seconds != stream_delay:
                return self._real.sleep(seconds)
            self._stream_sleep_calls += 1
            deadline = stream_deadline[0]
            if deadline is None:
                return self._real.sleep(seconds)
            remaining = deadline - self._real.monotonic()
            if remaining <= 0:
                raise TimeoutError('busy burst provider stream exceeded 10s deadline')
            # FakeLLMServer sleeps before writing each content chunk. The first
            # call precedes the first chunk; the second follows that flushed
            # chunk, so this is the first safe point to wake the busy producer.
            if self._stream_sleep_calls == 2:
                loop.call_soon_threadsafe(running.set)
            if self._stream_sleep_calls >= 2:
                if not provider_finish.wait(remaining):
                    raise TimeoutError('busy burst provider stream exceeded 10s deadline')
                return
            return self._real.sleep(min(seconds, remaining))

        def __getattr__(self, name):
            return getattr(self._real, name)

    # Keep FakeLLMServer's actual HTTP/SSE renderer. Only its fixture-local
    # pacing clock is event-controlled; the original provider deadline remains
    # one absolute ten-second budget and the real finish event releases it.
    monkeypatch.setattr(fake_llm_provider, 'time', _EventClock())

    def responder(record):
        body = record.get('body') if type(record) is dict else None
        messages = body.get('messages') if type(body) is dict else None
        message = '<unknown>'
        for item in reversed(messages if type(messages) is list else []):
            if type(item) is dict and item.get('role') == 'user':
                content = item.get('content')
                message = content if type(content) is str else '<multimodal>'
                break
        with executed_lock:
            executed.append(message)
        if message == 'A':
            # The clock publishes running only after FakeLLMServer has flushed
            # the first real SSE content chunk; its second pacing call then
            # waits on the original provider-finish event.
            stream_deadline[0] = time.monotonic() + 10
            return Text('x' * 80, chunk_chars=1, delay_per_chunk=stream_delay)
        return Text('ok')

    llm = FakeLLMServer(responder)
    llm.start()
    try:
        write_hermes_home(
            (tmp_path / 'profile').resolve(), llm.base_url,
            extra_config='display:\n  busy_input_mode: queue\n  busy_text_mode: queue\n',
        )
        fixture = await _real_discord_runtime(tmp_path, monkeypatch)
    except BaseException:
        provider_finish.set()
        finish.set()
        llm.stop()
        raise
    runner, adapter, authority = fixture.runner, fixture.adapter, fixture.authority
    try:
        async def send(chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id='sent')
        monkeypatch.setattr(adapter, 'send', send)
        monkeypatch.setattr('hermes_cli.tools_config._get_platform_tools', lambda *_: {'terminal'})
        adapter.gateway_runner = runner

        def event(sender, text, photo, n):
            source = adapter.build_source(chat_id='room', chat_type='group', user_id=sender)
            if not photo:
                return MessageEvent(text=text, source=source, message_id=f'm{n}')
            path = tmp_path / f'p{n}.png'
            path.write_bytes(PNG + bytes([n]))
            return MessageEvent(text=text, source=source, message_id=f'm{n}', message_type=MessageType.PHOTO,
                                media_urls=[str(path)], media_types=['image/png'])

        await adapter.handle_message(event('42', 'A', False, 0))
        await asyncio.wait_for(running.wait(), 10)
        sent = [event(*spec, n) for n, spec in enumerate(follow_ups, 1)]
        key = adapter._event_session_key(sent[0])
        callers = []
        for e in sent:  # in arrival order: each lands while the earlier one is still held
            callers.append(asyncio.create_task(adapter.handle_message(e)))
            async with asyncio.timeout(5):
                while not (holds_busy_burst(adapter, key) or e._gateway_accepted):
                    await asyncio.sleep(0)
        # Each caller returns only once its input is durably committed (committed before ACK).
        await asyncio.wait_for(asyncio.gather(*callers), 10)
        assert all(e._gateway_accepted is True for e in sent), [e._gateway_accepted for e in sent]
        owner = runner.session_store.peek_session_id(runner.session_store._generate_session_key(sent[0].source))
        rows = list_session_admissions(authority.db, session_id=owner, pending_only=False)
        assert [(r['payload']['text'], len(r['payload']['native_text_v1'].get('media', [])))
                for r in rows[1:]] == expected
        finish.set()
        provider_finish.set()
        await async_wait_until(lambda: len(executed) >= 1 + len(expected), timeout=10, what='every merged turn executed')
    finally:
        finish.set()
        provider_finish.set()
        try:
            fixture.cleanup()
        finally:
            llm.stop()
