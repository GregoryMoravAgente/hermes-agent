"""Ordinary daemon + local authenticated clients + actual terminal completion."""
import asyncio
from copy import copy
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading

from automation_peer import Model
from local_recovery_probe import daemon, websocket, rpc
from tests.waiting import async_wait_until


def _preflight(base):
    """Exercise the real I10 classifier against owned objects before any effect."""
    from types import SimpleNamespace

    from gateway.config import GatewayConfig, Platform
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_ownership import process_ownership
    from gateway.session import SessionSource, SessionStore
    from gateway.session_authority import SessionAuthority
    from gateway.session_recovery_custody import (
        _is_i10_local_automation, ensure_recovery_root,
    )
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state import SessionDB
    from hermes_state_runtime import (
        RuntimeStoreError, admit_session_input, begin_runtime_epoch, get_session_admission,
    )

    home = (base / 'state').resolve()
    user = (base / 'user').resolve()
    home.mkdir(parents=True)
    user.mkdir(parents=True)
    os.environ.update(HERMES_HOME=str(home), HOME=str(user), USERPROFILE=str(user))
    db = SessionDB(home / 'state.db')
    store = SessionStore(home / 'sessions', GatewayConfig())
    store._db = db
    source = SessionSource(
        platform=Platform.LOCAL, chat_id='preflight-chat', user_id='preflight-user', profile='default',
    )
    entry = store.get_or_create_session(source)
    instance_id = 'preflight-owner'
    process_ownership.reserve([home])
    try:
        epoch = begin_runtime_epoch(db, instance_id=instance_id)
        runner = SimpleNamespace(_draining=False, session_store=store, adapters={})
        authority = SessionAuthority(
            runner, profile_id=str(home), instance_id=instance_id, db=db, epoch=epoch,
        )
        coordinator = RuntimeCustodyCoordinator(
            process_instance=instance_id, authority_epoch=epoch, fence=RetirementFence(),
        )
        runner.runtime_custody = coordinator
        row = admit_session_input(
            db, epoch=epoch, principal_id='automation:' + entry.session_key,
            session_id=entry.session_id, request_id='preflight-local-automation',
            payload={'local_automation_v1': {
                'route': entry.session_key, 'source': 'gui',
                'event': {'message_id': 'preflight-local-automation'},
            }},
        )
        admission_id = row['admission_id']
        handle = ensure_recovery_root(authority, row, consumer='startup')
        assert _is_i10_local_automation(authority, row) is True
        binding = authority._i10_recovery_bindings[admission_id]
        baseline_handles = dict(authority._preservation_handles)
        baseline_bindings = dict(authority._i10_recovery_bindings)

        cases = (
            ('copied_opaque_handle',
             lambda: authority._preservation_handles.__setitem__(admission_id, copy(handle)),
             lambda: authority._preservation_handles.__setitem__(admission_id, handle),
             'preservation_owner_unmapped'),
            ('missing_binding',
             lambda: authority._i10_recovery_bindings.pop(admission_id),
             lambda: authority._i10_recovery_bindings.__setitem__(admission_id, binding),
             'preservation_owner_unmapped'),
            ('missing_current_parent',
             lambda: authority._preservation_handles.pop(admission_id),
             lambda: authority._preservation_handles.__setitem__(admission_id, handle),
             'preservation_owner_unmapped'),
            ('stale_profile',
             lambda: setattr(authority, 'profile_id', str(home) + '-foreign'),
             lambda: setattr(authority, 'profile_id', str(home)),
             'preservation_owner_unmapped'),
            ('stale_epoch',
             lambda: setattr(authority, 'epoch', epoch + 1),
             lambda: setattr(authority, 'epoch', epoch),
             'preservation_owner_unmapped'),
            ('unknown_coordinator_record',
             lambda: coordinator.mark_unknown(handle, 'unmapped_writer'),
             lambda: coordinator.reconcile_unknown(
                 handle, owner_id=handle.owner_id, profile_id=authority.profile_id,
                 process_instance=authority.instance_id, authority_epoch=authority.epoch,
             ),
             'preservation_owner_unmapped'),
            ('foreign_physical_owner',
             lambda: process_ownership.release(home),
             lambda: process_ownership.reserve([home]),
             'foreign_custody'),
        )
        for label, install, restore, reason in cases:
            install()
            try:
                try:
                    _is_i10_local_automation(authority, row)
                except RuntimeStoreError as exc:
                    assert exc.reason == reason, (label, exc.reason)
                else:
                    raise AssertionError(f'{label} was accepted')
            finally:
                restore()
            assert _is_i10_local_automation(authority, row) is True, label

        mismatched_row = dict(row)
        mismatched_row['generation'] = (row.get('generation') or 0) + 1
        try:
            _is_i10_local_automation(authority, mismatched_row)
        except RuntimeStoreError as exc:
            assert exc.reason == 'preservation_owner_unmapped'
        else:
            raise AssertionError('full canonical row mismatch was accepted')
        assert _is_i10_local_automation(authority, row) is True
        assert authority._preservation_handles == baseline_handles
        assert authority._i10_recovery_bindings == baseline_bindings
        current = get_session_admission(db, admission_id=admission_id)
        assert current == row and current['status'] == 'queued'
        record = coordinator._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == 'accepted'

        from gateway import session_local_recovery
        original_restore = session_local_recovery.restore_local_session
        original_generic_recover = authority._preservation_recover_automation
        restore_calls = []
        generic_recover_calls = []

        def observe_restore(owner, session_id):
            restore_calls.append((owner, session_id))
            return original_restore(owner, session_id)

        def observe_generic_recover(row_arg, receipt=None):
            generic_recover_calls.append(row_arg.get('admission_id') if isinstance(row_arg, dict) else None)
            return original_generic_recover(row_arg, receipt)

        session_local_recovery.restore_local_session = observe_restore
        authority._preservation_recover_automation = observe_generic_recover
        try:
            def install_row_generation_mismatch():
                db._write_sql(
                    'UPDATE session_admissions SET generation=? WHERE admission_id=?',
                    ((row.get('generation') or 0) + 1, admission_id),
                )

            def restore_row_generation():
                db._write_sql(
                    'UPDATE session_admissions SET generation=? WHERE admission_id=?',
                    (row.get('generation'), admission_id),
                )

            public_cases = cases + (
                ('full_canonical_row_mismatch', install_row_generation_mismatch,
                 restore_row_generation, 'preservation_owner_unmapped'),
            )
            for label, install, restore, reason in public_cases:
                install()
                before_sessions = dict(authority.sessions)
                before_handles = dict(authority._preservation_handles)
                before_bindings = dict(authority._i10_recovery_bindings)
                before_restore_calls = len(restore_calls)
                before_generic_calls = len(generic_recover_calls)
                try:
                    session_local_recovery.restore_pending_local_automation(authority, [entry.session_id])
                    results = asyncio.run(authority.recover_native_sessions(
                        [(entry.session_id, source, None)]))
                    assert results.get(entry.session_id) == reason, (label, results)
                    assert len(restore_calls) == before_restore_calls, label
                    assert len(generic_recover_calls) == before_generic_calls, label
                    assert authority.sessions == before_sessions, label
                    assert authority._preservation_handles == before_handles, label
                    assert authority._i10_recovery_bindings == before_bindings, label
                    current = get_session_admission(db, admission_id=admission_id)
                    assert current['status'] == 'queued', (label, current)
                finally:
                    restore()
                assert _is_i10_local_automation(authority, row) is True, label

            def install_missing_both():
                authority._i10_recovery_bindings.pop(admission_id)
                authority._preservation_handles.pop(admission_id)

            def restore_missing_both():
                authority._i10_recovery_bindings[admission_id] = binding
                authority._preservation_handles[admission_id] = handle

            install_missing_both()
            before_sessions = dict(authority.sessions)
            before_restore_calls = len(restore_calls)
            before_generic_calls = len(generic_recover_calls)
            try:
                session_local_recovery.restore_pending_local_automation(authority, [entry.session_id])
                results = asyncio.run(authority.recover_native_sessions(
                    [(entry.session_id, source, None)]))
                assert results.get(entry.session_id) == 'preservation_owner_unmapped', results
                assert len(restore_calls) == before_restore_calls
                assert len(generic_recover_calls) == before_generic_calls
                assert authority.sessions == before_sessions
                current = get_session_admission(db, admission_id=admission_id)
                assert current == row and current['status'] == 'queued'
            finally:
                restore_missing_both()
            assert authority._preservation_handles[admission_id] is handle
            assert authority._i10_recovery_bindings[admission_id] is binding
            assert _is_i10_local_automation(authority, row) is True
        finally:
            session_local_recovery.restore_local_session = original_restore
            authority._preservation_recover_automation = original_generic_recover

        return {
            'cases': [case[0] for case in cases] + ['full_canonical_row_mismatch'],
            'refused_before_effect': True,
            'restored_handle_identity': True,
            'restored_binding_identity': True,
            'queued_row_unchanged': True,
            'public_cases': [case[0] for case in public_cases],
            'public_refused_before_restore': True,
            'public_refused_before_generic_recover': True,
            'missing_both_restored_exactly': True,
        }
    finally:
        db.close()
        process_ownership.release(home)


def probe(base, source):
    if source == 'preflight':
        return _preflight(base)
    restart = source == 'restart'
    watch = source == 'watch'
    source = 'gui' if restart or watch else source
    root = Path(__file__).resolve().parents[3]
    home, user = base / 'state', base / 'user'
    home.mkdir(); user.mkdir()
    model = ThreadingHTTPServer(('127.0.0.1', 0), Model)
    model.requests = []
    model.watch = watch
    model.blocked, model.release = threading.Event(), threading.Event()
    model.gate = home / 'terminal-gate'
    threading.Thread(target=model.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{model.server_port}/v1'
    cfg = {'gateway': {'multiplex_profiles': False},
        'model': {'provider': 'custom', 'default': 'local-fixture', 'base_url': url},
        'terminal': {'cwd': str(home)}, 'approvals': {'mode': 'off'},
        'auxiliary': {'title_generation': {'enabled': False}}}
    (home / 'config.yaml').write_text(json.dumps(cfg))
    env = {k: os.environ[k] for k in ('PATH', 'LANG', 'TZ', 'TIRITH_ENABLED') if k in os.environ}
    env.update(HOME=str(user), USERPROFILE=str(user), HERMES_HOME=str(home), PYTHONPATH=str(root),
        OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL=url, PYTHONUNBUFFERED='1')

    def rows():
        with sqlite3.connect(f'file:{home / "state.db"}?mode=ro', uri=True) as db:
            db.row_factory = sqlite3.Row
            return [dict(r) for r in db.execute('SELECT * FROM session_admissions ORDER BY seq')]

    async def wait(predicate):
        await async_wait_until(predicate, timeout=30)

    def notes():
        with sqlite3.connect(f'file:{home / "state.db"}?mode=ro', uri=True) as db:
            return db.execute("SELECT display_kind, json_extract(display_metadata, '$.display_text') FROM messages"
                              " WHERE role='user' AND content LIKE ?", ('%REAL_TERMINAL_COMPLETION%',)).fetchall()

    def completion_turns():
        # Model calls whose newest user input is the completion: an automation turn for it.
        return [r for r in model.requests if r.get('messages') and 'REAL_TERMINAL_COMPLETION' in str(next(
            (m.get('content', '') for m in reversed(r['messages']) if m['role'] == 'user'), ''))]

    async def unviewed(desc, sid):
        # No viewer is left (#134889 LC-7): the completion is committed behind the running turn and the
        # human follower, in FIFO order, and when it reaches the head it is written into the session as a
        # note (no model call) instead of an automation turn; the next turn sees it.
        await wait(lambda: any(r['principal_id'].startswith('automation:') for r in rows()))
        automatic = [r for r in rows() if r['principal_id'].startswith('automation:')]
        assert len(automatic) == 1 and automatic[0]['status'] == 'queued', automatic
        assert json.loads(automatic[0]['request_id'])[0] == ('watch_match' if watch else 'completion'), automatic
        assert automatic[0]['seq'] > next(r for r in rows() if r['request_id'] == 'follower')['seq']
        assert not notes()
        model.release.set()
        await wait(lambda: all(r['status'] == 'terminal' for r in rows()))
        await async_wait_until(notes, timeout=120)
        await asyncio.sleep(8)  # health: allow HX013 -- proves no automation turn or second note arrives
        assert completion_turns() == [], 'an unviewed completion started a model turn'
        assert all(r['outcome'] == 'completed' for r in rows()), rows()
        found = notes()
        if watch:  # a watch match is a notice, not the async-result card
            assert len(found) == 1 and found[0][0] == 'internal_notification', found
        else:
            assert len(found) == 1 and found[0][0] == 'process_complete', found
            assert str(found[0][1]).startswith('Background Process Finished: '), found
        async with websocket(home, desc) as ws:
            assert 'result' in await rpc(ws, 'session.resume', session_id=sid)
            assert 'result' in await rpc(ws, 'prompt.submit', session_id=sid, input_id='after', text='AFTER_NOTE')
            await wait(lambda: next((r['status'] for r in rows() if r['request_id'] == 'after'), '') == 'terminal')
        users = [str(m.get('content')) for m in model.requests[-1]['messages'] if m['role'] == 'user']
        assert 'AFTER_NOTE' in users[-1] and any('REAL_TERMINAL_COMPLETION' in u for u in users), users
        assert 'HUMAN_FIFO_FOLLOWER' in str(users), users
        prefixes = {json.dumps([m for m in r['messages'] if m['role'] in ('system', 'developer')], sort_keys=True)
                    for r in model.requests if r.get('messages')}
        assert len(prefixes) == 1, 'the note changed the cached system prefix'
        return {'source': source, 'ledger': rows(), 'note': found, 'observer_closed_before_completion': True}

    async def run(desc):
        async with websocket(home, desc) as ws:
            result = await rpc(ws, 'session.create', request_id='automation', source=source,
                cwd=str(home), toolsets=['terminal'])
            assert 'result' in result, result
            sid = result['result']['session_id']
            async def submit(identity, text):
                reply = await rpc(ws, 'prompt.submit', session_id=sid, input_id=identity, text=text)
                assert 'result' in reply, reply
            await submit('spawn', 'SPAWN_AUTOMATION')
            await wait(lambda: rows()[0]['status'] == 'terminal')
            assert 'REAL_TERMINAL_COMPLETION' in json.dumps(model.requests)
            if restart:
                model.gate.touch()
                await wait(lambda: any(r['principal_id'].startswith('automation:') for r in rows()))
                assert rows()[-1]['status'] == 'queued', rows()
                return {'session_id': sid, 'committed': rows()}
            await submit('held', 'HOLD_AUTOMATION')
            assert await asyncio.to_thread(model.blocked.wait, 20)
            await submit('follower', 'HUMAN_FIFO_FOLLOWER')
        # Last observer closes while owner remains in a model call.
        model.gate.touch()
        return await unviewed(desc, sid)

    try:
        with daemon(root, home, env, barrier=restart, fixture='local_automation_daemon.py') as (proc, desc):
            try:
                receipt = asyncio.run(run(desc))
                receipt['daemon_pid'] = proc.pid
                if not restart:
                    print(json.dumps(receipt))
            except BaseException:
                print((home / ('first.log' if restart else 'restart.log')).read_text()[-15000:], file=sys.stderr)
                raise
        if restart:
            pids = [receipt['daemon_pid']]
            epochs = [desc['authority_epoch']]
            async def recovered(desc):
                await wait(lambda: all(r['status'] == 'terminal' for r in rows()))
                # Leave ordinary startup/watchers running before checking zero replay.
                await asyncio.sleep(8)  # health: allow HX013 -- proves recovery replays nothing in the window
                assert len(rows()) == len(receipt['committed']), rows()
                assert rows()[-1]['outcome'] == 'completed', rows()
                async with websocket(home, desc) as ws:
                    result = await rpc(ws, 'session.resume', session_id=receipt['session_id'])
                    assert 'result' in result, result
            for _ in range(2):
                with daemon(root, home, env, barrier=False) as (proc, desc):
                    pids.append(proc.pid); epochs.append(desc['authority_epoch'])
                    asyncio.run(recovered(desc))
            texts = [next((m.get('content', '') for m in reversed(r['messages']) if m['role'] == 'user'), '')
                     for r in model.requests if r.get('messages')]
            # The committed completion outlived two owners; with no viewer it ran once, as one note.
            assert completion_turns() == [], texts
            assert len(notes()) == 1, notes()
            assert len(set(pids)) == 3 and epochs == sorted(set(epochs))
            print(json.dumps({'pids': pids, 'epochs': epochs, 'ledger': rows(),
                              'model_inputs': texts, 'lost_producer_ack': True}))
    finally:
        model.gate.touch(); model.release.set()
        model.shutdown(); model.server_close()


if __name__ == '__main__':
    result = probe(Path(sys.argv[1]), sys.argv[2])
    if sys.argv[2] == 'preflight':
        print(json.dumps(result))
