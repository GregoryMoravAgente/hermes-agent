"""A managed worker's agent gets a RuntimeSessionStore as its session_db. Every SessionDB method
the agent core calls must exist on the store, accept the same keywords, and answer like the
owner's SessionDB (Adolanium reg-1 was one missing keyword; the sweep found two missing reads:
the failed-turn boundary's tail role and in-place compaction's held-row liveness)."""
import ast
import inspect
from pathlib import Path
import re

import pytest

from agent.runtime_session_store import RuntimeSessionStore, WorkerPersistenceError
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

ROOT = Path(__file__).resolve().parents[2]
# The agent core: everything a worker's AIAgent runs (tools open their own handles, or refuse).
CORE = [ROOT / 'run_agent.py', *sorted((ROOT / 'agent').glob('*.py'))]


def _core_session_db_calls():
    """Names called on the agent's store: ``x._session_db`` / ``x.db`` / ``session_db``, and a local
    ``db`` where the module binds it from ``_session_db`` (a module opening its own SessionDB is not)."""
    names = set()
    for path in CORE:
        text = path.read_text(encoding='utf-8')
        local_db = re.search(r'\bdb\b[^=\n]*=\s*getattr\([^)\n]*_session_db', text) is not None
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                owner = node.func.value
                if ((isinstance(owner, ast.Attribute) and owner.attr in ('_session_db', 'db'))
                        or (isinstance(owner, ast.Name) and (owner.id == 'session_db' or (owner.id == 'db' and local_db)))):
                    names.add(node.func.attr)
    return sorted(n for n in names if hasattr(SessionDB, n))


@pytest.mark.parametrize('name', _core_session_db_calls())
def test_store_offers_every_core_session_db_call_with_its_keywords(name):
    assert hasattr(RuntimeSessionStore, name), f'RuntimeSessionStore lacks {name}'
    store = inspect.signature(getattr(RuntimeSessionStore, name)).parameters
    if any(p.kind is p.VAR_KEYWORD for p in store.values()):
        return
    owner = inspect.signature(getattr(SessionDB, name)).parameters
    missing = [k for k, p in owner.items() if k not in store and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)]
    assert missing == [], f'RuntimeSessionStore.{name} rejects {missing}'


@pytest.mark.parametrize('content', [
    None, b'\x00synthetic\xff', 17, 'literal',
    {'text': ['synthetic', {'nested': True}]},
    '\x00json:literal-prefix',
])
def test_committed_content_codec_preserves_values_without_opening_owner_store(content, monkeypatch):
    encoded = SessionDB._encode_content(content)
    monkeypatch.setattr(
        SessionDB, '__init__',
        lambda *_args, **_kwargs: pytest.fail('a worker codec must not open the owner database'),
    )
    assert RuntimeSessionStore._decode_content(encoded) == content


def test_committed_content_codec_preserves_a_malformed_legacy_marker():
    content = SessionDB._CONTENT_JSON_PREFIX + 'not-json'
    assert RuntimeSessionStore._decode_content(content) == content


def test_tail_reads_answer_like_the_owner(tmp_path):
    db = SessionDB(tmp_path / 'state.db')
    try:
        db.create_session('s', 'cli')
        db.append_messages_batch('s', [{'role': 'user', 'content': 'q'}, {'role': 'assistant', 'content': 'a'},
                                       {'role': 'user', 'content': 'failed turn'}])
        db.create_session('other', 'cli')
        db.append_messages_batch('other', [{'role': 'assistant', 'content': 'x'}])
        rows = [r[0] for r in db._read_all('SELECT id FROM messages ORDER BY id')]
        epoch = begin_runtime_epoch(db, instance_id='fixture')
        scope = dict(epoch=epoch, execution_id='w', session_id='s', generation=0)
        register_worker_execution(db, **scope, kind='compute', adoption_secret='secret')
        store = RuntimeSessionStore(lambda method, **p: mutate_worker_execution(db, **p), scope, tmp_path / 'outbox')
        try:
            assert store.latest_conversation_role('s') == db.latest_conversation_role('s') == 'user'
            for row_id in [*rows, 10**6]:  # another transcript's row is not this session's
                assert store.get_message_role('s', row_id) == db.get_message_role('s', row_id)
        finally:
            store.close()
    finally:
        db.close()


def test_committed_row_read_is_owner_scoped_and_retries_the_same_receipt(tmp_path):
    db = SessionDB(tmp_path / 'state.db')
    try:
        db.create_session('s', 'cli')
        db.create_session('other', 'cli')
        db.append_messages_batch('s', [{'role': 'user', 'content': 'owned'}])
        db.append_messages_batch('other', [{'role': 'user', 'content': 'foreign'}])
        owned_id = db._read_one("SELECT id FROM messages WHERE session_id='s'")[0]
        foreign_id = db._read_one("SELECT id FROM messages WHERE session_id='other'")[0]
        blob = b'\x00committed\xff'
        db._write_sql('UPDATE messages SET display_identity=? WHERE id=?', (blob, owned_id))
        bulk_before = db._read_one("SELECT max(id) FROM messages WHERE session_id='s'")[0]
        db.append_messages_batch(
            's', [{'role': 'assistant', 'content': f'bulk-{index}'} for index in range(901)],
        )
        bulk_ids = [row[0] for row in db._read_all(
            'SELECT id FROM messages WHERE session_id=? AND id>? ORDER BY id',
            ('s', bulk_before),
        )]
        assert len(bulk_ids) == 901
        epoch = begin_runtime_epoch(db, instance_id='fixture-read')
        scope = dict(epoch=epoch, execution_id='read-worker', session_id='s', generation=0)
        register_worker_execution(db, **scope, kind='compute', adoption_secret='secret')
        dropped = False

        def rpc(_method, **params):
            nonlocal dropped
            result = mutate_worker_execution(db, **params)
            if params['operation'] == 'session.committed_rows' and not dropped:
                dropped = True
                raise TimeoutError('read response lost after owner commit')
            return result

        store = RuntimeSessionStore(rpc, scope, tmp_path / 'outbox')
        try:
            sql = 'SELECT * FROM messages WHERE session_id = ? AND id IN (?)'
            with pytest.raises(TimeoutError, match='response lost'):
                store._read_all(sql, ('s', owned_id))
            pending_sequence = store.journal['pending'][0]['sequence']
            [receipt] = store.retry_pending()
            assert receipt['value']['rows'][0]['id'] == owned_id
            assert store.journal['pending'] == []
            assert db._read_one(
                'SELECT COUNT(*) FROM worker_receipts WHERE execution_id=? AND sequence=?',
                ('read-worker', pending_sequence),
            )[0] == 1
            owned = store._read_all(sql, ('s', owned_id))
            assert owned[0]['display_identity'] == blob
            bulk_sql = (
                'SELECT * FROM messages WHERE session_id = ? AND id IN ('
                + ','.join('?' for _ in bulk_ids) + ')'
            )
            bulk_rows = store._read_all(bulk_sql, ('s', *bulk_ids))
            assert [row['id'] for row in bulk_rows] == bulk_ids
            duplicate_sql = 'SELECT * FROM messages WHERE session_id = ? AND id IN (?,?)'
            assert [row['id'] for row in store._read_all(
                duplicate_sql, ('s', owned_id, owned_id),
            )] == [owned_id]
            assert store._read_all(sql, ('s', foreign_id)) == []
            with pytest.raises(WorkerPersistenceError, match='permission_denied'):
                store._read_all(sql, ('other', foreign_id))
        finally:
            store.close()
    finally:
        db.close()
