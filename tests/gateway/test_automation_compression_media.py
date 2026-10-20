"""Producer completions and heartbeats keep entering a messaging conversation after compression
moved its route to a child and after a prior input's retained media was released."""
import asyncio

import pytest


async def _discord_owner(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import SessionAuthority
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    import hermes_state
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setattr(SessionAuthority, '_schedule', lambda self, ref: None)
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42')
    home = (tmp_path / 'profile').resolve()
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setattr('gateway.run._hermes_home', home)
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
        runner = GatewayRunner(GatewayConfig(sessions_dir=home / 'sessions'))
        adapter = DiscordAdapter(
            PlatformConfig(enabled=True, token='fixture-token', typing_indicator=False)
        )
        adapter.gateway_runner = runner
        runner.adapters = {Platform.DISCORD: adapter}
        await initialize_gateway_runtime(runner)
        runner._wire_adapter_handlers(adapter)
        return SimpleNamespace(
            runner=runner,
            adapter=adapter,
            authority=runner.session_authority,
            source=adapter.build_source(chat_id='42', chat_type='dm', user_id='42'),
            cleanup=cleanup,
        )
    except BaseException:
        cleanup()
        raise


async def _admit_human(runner, authority, event):
    from gateway.session_ingress_context import native_callback
    from hermes_constants import get_hermes_home
    with native_callback(runner, event, get_hermes_home()):
        return await authority.admit_native(event)


@pytest.mark.asyncio
async def test_completion_after_compression_admits_to_the_logical_root(tmp_path, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import list_session_admissions
    from tests.gateway.test_completion_admission import pending

    fixture = await _discord_owner(tmp_path, monkeypatch)
    runner, _, authority, source = fixture.runner, fixture.adapter, fixture.authority, fixture.source
    try:
        receipt = await _admit_human(runner, authority, MessageEvent(text='human', source=source, message_id='human'))
        key = runner.session_store._generate_session_key(source)
        root = receipt.ref.session_id
        db = authority.db
        db.append_message(root, 'user', 'human')
        child = root + '-child'
        db.publish_compression_child(parent_session_id=root, child_session_id=child, source='discord',
                                     messages=[{'role': 'user', 'content': 'summary'}], require_compression_lease=False)
        assert runner.session_store.advance_compression_session(key, root, child) is not None
        event = dict(pending(key, 'after-compression'), parent_session_id=child)
        assert await runner._deliver_async_delegation_group([event]) is True
        rows = list_session_admissions(db, session_id=root, pending_only=False)
        assert next(r['request_id'] for r in rows) == 'human' and len(rows) == 2, rows
        assert 'after-compression' in rows[-1]['payload']['text']
        assert list_session_admissions(db, session_id=child, pending_only=False) == []
        # A lost-ACK redelivery is the same admission, found under the root's ledger.
        from gateway.session_automation import completion_admission
        assert completion_admission(runner, event)['admission_id'] == rows[-1]['admission_id']
        await asyncio.sleep(0)
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_text_completion_after_a_released_photo_input_is_admitted(tmp_path, monkeypatch):
    from gateway.platforms.base import get_image_cache_dir
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.session_ingress_media import release_admission_media
    from hermes_state_runtime import claim_session_input, list_session_admissions, settle_session_input
    from tests.gateway.test_completion_admission import pending

    fixture = await _discord_owner(tmp_path, monkeypatch)
    runner, _, authority, source = fixture.runner, fixture.adapter, fixture.authority, fixture.source
    try:
        photo = get_image_cache_dir() / 'photo.png'
        photo.write_bytes(b'\x89PNG\r\n\x1a\n' + b'\x00' * 32)
        receipt = await _admit_human(runner, authority, MessageEvent(
            text='look', source=source, message_id='photo', message_type=MessageType.PHOTO,
            media_urls=[str(photo)], media_types=['image/png']))
        sid = receipt.ref.session_id
        started = claim_session_input(authority.db, epoch=authority.epoch, session_id=sid)
        settle_session_input(authority.db, epoch=authority.epoch, admission_id=started['admission_id'],
                             generation=started['generation'], outcome='completed')
        assert release_admission_media(authority.db, started['admission_id']) == 1  # what _drain does
        key = runner.session_store._generate_session_key(source)
        assert await runner._deliver_async_delegation_group([dict(pending(key, 'after-photo'), parent_session_id=sid)])
        rows = list_session_admissions(authority.db, session_id=sid, pending_only=False)
        assert len(rows) == 2 and 'after-photo' in rows[-1]['payload']['text'], rows
        assert 'media' not in rows[-1]['payload']['native_text_v1']
        await asyncio.sleep(0)
    finally:
        fixture.cleanup()
