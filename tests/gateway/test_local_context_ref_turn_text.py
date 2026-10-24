"""``@`` references reach the model on every local route now that the classic CLI leaves them to the
gateway: its API-only text (voice prefix, model-switch notes), managed workers (``--safe-mode`` /
``--ignore-user-config``), and a refused injection's explanation.
"""
import asyncio
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway import session_policy
from gateway.config import GatewayConfig, Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from hermes_cli.cli_render import _prepend_note_to_message


@pytest.fixture
def project(tmp_path, monkeypatch):
    project, daemon = tmp_path / 'project', tmp_path / 'daemon'
    for folder, body in ((project, 'PROJECT-NOTES'), (daemon, 'DECOY-NOTES')):
        folder.mkdir()
        (folder / 'notes.txt').write_text(body, encoding='utf-8')
    for name in ('a', 'b', 'c'):  # each under the 50% budget of 1000 tokens, together over it
        (project / f'{name}.txt').write_text(name * 1800, encoding='utf-8')
    monkeypatch.setenv('TERMINAL_CWD', str(daemon))
    monkeypatch.setattr(session_policy, 'policy_for_source', lambda runner, source: SimpleNamespace(
        cwd=str(project)) if source.platform == Platform.LOCAL else None)
    return project


def _runner(monkeypatch, context_length):
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(platforms={})
    runner.adapters = {}

    async def _context_length(_source, _session_key):
        return context_length

    monkeypatch.setattr(runner, '_inbound_model_context_length', _context_length, raising=False)
    return runner


def _event(text):
    return MessageEvent(text=text, source=SessionSource(platform=Platform.LOCAL, chat_id='local-chat'),
                        message_id='adm-1')


async def _prepare(runner, event):
    return await runner._prepare_inbound_message_text(
        event=event, source=event.source, history=[], session_key='key')


# The classic CLI's API-only prefixes: a voice turn, and the one-shot /model and /reload-skills notes
# (hermes_cli/cli_chat_turn_mixin.py puts them in front through _prepend_note_to_message).
_API_ONLY_PREFIXES = {
    'voice': lambda text: f'[voice] {text}',
    'model': lambda text: _prepend_note_to_message(
        text, '[Note: model was just switched from a to b via x. Adjust your self-identification accordingly.]'),
    'reload-skills': lambda text: _prepend_note_to_message(
        text, '[USER INITIATED SKILLS RELOAD:\n\nAdded Skills:\n    - demo: a demo skill\n\n'
              'Use skills_list to see the updated catalog.]'),
}


@pytest.mark.asyncio
@pytest.mark.parametrize('route', sorted(_API_ONLY_PREFIXES))
async def test_cli_api_text_reaches_the_model_expanded_and_the_stored_row_keeps_the_content(project, monkeypatch, route):
    from gateway.session_cli_turn import cli_turn_scope
    from gateway.run_turn_runner import TurnRunner
    from gateway.turn_context import TurnContext
    from tests.gateway.test_local_viewer_events_submit import _RecordingAgent, _gateway_runner

    typed = await _prepare(_runner(monkeypatch, 128_000), _event('read @file:notes.txt'))
    api_text = _API_ONLY_PREFIXES[route]('read @file:notes.txt')
    event = _event('read @file:notes.txt')
    with cli_turn_scope('adm-1', {'api_text': api_text}):
        prepared = await _prepare(_runner(monkeypatch, 128_000), event)
        # The stored row is exactly what a typed turn stores (the file once, no prefix or note), so a
        # later turn that reads the row's content without the api_content sidecar still has the file.
        assert prepared == typed and 'PROJECT-NOTES' in prepared
        monkeypatch.setattr(session_policy, 'policy_for_source', lambda runner, source: None)  # the turn's own read
        ctx = TurnContext(
            source=event.source, message=prepared, history=[], session_id='sid', session_key='key',
            user_config={}, inbound_message_id='adm-1', AIAgent=_RecordingAgent,
            resolve_display_setting=lambda *_a: False, _run_still_current=lambda: True,
            _hooks_ref=SimpleNamespace(loaded_hooks=False))
        # The turn runs on an executor thread with a copy of this context, as run_sync does.
        await asyncio.to_thread(TurnRunner(_gateway_runner(), ctx).run_sync)
    [(message, kwargs)] = ctx.agent_holder[0].calls
    assert message.startswith(api_text) and message.count('PROJECT-NOTES') == 1
    assert 'DECOY-NOTES' not in message
    assert kwargs['persist_user_message'] == prepared


@pytest.mark.asyncio
async def test_a_refused_local_turn_answers_with_its_refusal(project, monkeypatch):
    event = _event('compare @file:a.txt @file:b.txt @file:c.txt')
    assert await _prepare(_runner(monkeypatch, 1000), event) is None
    assert 'refused' in event._gateway_turn_refusal

    # The prepare step hands that refusal back as the turn's reply instead of dropping the turn.
    runner = _runner(monkeypatch, 1000)

    async def _none(*_args, **_kwargs):
        return None

    async def _open(*_args):
        return False, False

    runner._hmwa_open_session = _open
    runner._set_session_env = lambda context: []
    runner._hmwa_acquire_turn_lease = runner._mark_durable_active_turn = _none
    runner._hmwa_first_contact_notes = _none
    runner._voice_channel_sidecar_note = lambda *_args: None
    runner.session_store = object()
    runner._async_session_store = SimpleNamespace(_store=runner.session_store, load_transcript=lambda _sid: _history())
    runner._hmwa_run_session_hygiene = lambda *args: _history()
    monkeypatch.setattr('gateway.run_turn_prepare.build_session_context', lambda *_args: None)
    entry = SimpleNamespace(yolo=None, session_id='sid', session_key='key')
    reply, _tokens = await runner._hmwa_prepare_turn(_event(event.text), event.source, entry, 'key', 'key', 1)
    assert isinstance(reply, str) and 'refused' in reply


async def _history():
    return []


async def _managed(tmp_path, monkeypatch, text):
    """Prepare a real I04 claim before exercising the managed-text path."""
    from gateway import session_managed_worker as managed
    from gateway.session_local import _bypass_policy
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    binding = await fixture.claim(text)
    authority = fixture.authority
    ref = SimpleNamespace(session_id=binding.session_id, profile_id=binding.profile_id)
    row = get_session_admission(authority.db, admission_id=binding.admission_id)
    assert row is not None
    policy = _bypass_policy({'cwd': str(tmp_path), 'model': 'fixture', 'provider': 'custom',
                             'base_url': 'http://127.0.0.1:9/v1', 'safe_mode': True}, private_secrets={})
    monkeypatch.setattr('gateway.session_policy.launch_key', lambda *args: None)

    async def _context_length(model, **kwargs):
        assert kwargs['api_key'] == ''  # never a launch secret for a metadata probe
        return 1000

    monkeypatch.setattr('agent.model_metadata.get_model_context_length_async', _context_length)
    sent, spawned = [], []
    child = """import json, os, psutil, sys
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
out.write((json.dumps({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(),
                       'ancestors': [p.pid for p in me.parents()]}) + '\\n').encode())
for _line in sys.stdin.buffer:
    out.write(b'{\"type\": \"bogus\"}\\n')
    sys.stdin.buffer.read()
    break
"""

    class Child(subprocess.Popen):
        def __init__(self, args, **kwargs):
            process = super().__init__([sys.executable, '-c', child], **kwargs)
            spawned.append(process)

    class RecordingWorker(managed.ManagedWorker):
        def send(self, frame):
            sent.append(frame)
            return super().send(frame)

    monkeypatch.setattr(managed, '_worker_env', lambda authority: None)
    monkeypatch.setattr(managed, 'ManagedWorker', RecordingWorker)
    monkeypatch.setattr(managed.subprocess, 'Popen', Child)
    monkeypatch.setattr('gateway.session_managed_worker_log.worker_log_path', lambda profile: None)
    run = managed.execute_managed(authority, ref, row, policy, binding)
    return run, authority, sent, spawned, fixture, binding


@pytest.mark.asyncio
async def test_a_managed_worker_runs_the_expanded_text(tmp_path, monkeypatch):
    from gateway.session_managed_worker import ManagedExecutionUnknown
    (tmp_path / 'notes.txt').write_text('PROJECT-NOTES', encoding='utf-8')
    run, _authority, sent, _spawned, fixture, _binding = await _managed(
        tmp_path, monkeypatch, 'read @file:notes.txt'
    )
    try:
        with pytest.raises(ManagedExecutionUnknown):
            await run
    finally:
        fixture.cleanup()
    [frame] = sent
    assert frame['text'].startswith('read @file:notes.txt') and 'PROJECT-NOTES' in frame['text']


@pytest.mark.asyncio
async def test_a_managed_turn_with_refused_references_answers_without_spawning(tmp_path, monkeypatch):
    for name in ('a', 'b', 'c'):
        (tmp_path / f'{name}.txt').write_text(name * 1800, encoding='utf-8')
    run, authority, sent, spawned, fixture, binding = await _managed(
        tmp_path, monkeypatch, '@file:a.txt @file:b.txt @file:c.txt'
    )
    try:
        reply = await run
        assert 'refused' in reply and not spawned and not sent
        assert authority.pending_results[binding.admission_id]['result']['final_response'] == reply
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_a_bypass_session_expands_built_in_references_only(tmp_path, monkeypatch):
    from agent.context_references import (ContextCompletionItem, ContextReferenceProvider,
                                          _context_reference_providers, register_context_reference_provider)

    class Probe(ContextReferenceProvider):
        prefix, description = 'probe', 'probe provider'

        async def autocomplete(self, query, *, limit=10):
            return [ContextCompletionItem(text=query)]

        async def expand(self, target):
            return 'PLUGIN-RAN'

    from gateway.session_managed_worker import ManagedExecutionUnknown
    (tmp_path / 'notes.txt').write_text('PROJECT-NOTES', encoding='utf-8')
    register_context_reference_provider(Probe())
    try:
        run, _authority, sent, _spawned, fixture, _binding = await _managed(
            tmp_path, monkeypatch, '@probe:x @file:notes.txt'
        )
        try:
            with pytest.raises(ManagedExecutionUnknown):
                await run
        finally:
            fixture.cleanup()
    finally:
        _context_reference_providers.pop('probe', None)
    assert 'PROJECT-NOTES' in sent[0]['text'] and 'PLUGIN-RAN' not in sent[0]['text']
