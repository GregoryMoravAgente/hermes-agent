"""A busy same-sender burst is one authority admission, as main merged it into one turn (N54).

Main merged a busy photo burst into the pending slot and debounced queue-mode split text from one
sender. Under the session authority every busy input was admitted on its own, so a thought split
over two messages (or a photo burst) ran as separate turns. Real GatewayRunner, DiscordAdapter,
SessionAuthority and drain over SQLite; the running user turn is a stand-in holding the slot.
"""
import asyncio

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
    from gateway.session_results import execution_result
    from hermes_state_runtime import list_session_admissions

    fixture = await _real_discord_runtime(tmp_path, monkeypatch)
    runner, adapter, authority = fixture.runner, fixture.adapter, fixture.authority
    try:
        async def send(chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id='sent')
        monkeypatch.setattr(adapter, 'send', send)
        adapter.gateway_runner = runner
        running, finish = asyncio.Event(), asyncio.Event()
        executed = []

        async def turn(event, source, quick_key, run_generation):
            executed.append(event.text)
            if event.text == 'A':
                running.set()
                await finish.wait()
            captured = execution_result.get()
            if captured is not None:
                captured['result'] = {'final_response': 'ok', 'messages': []}
            return 'ok'
        monkeypatch.setattr(runner, '_handle_message_with_agent', turn)

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
        await async_wait_until(lambda: len(executed) >= 1 + len(expected), timeout=10, what='every merged turn executed')
    finally:
        fixture.cleanup()
