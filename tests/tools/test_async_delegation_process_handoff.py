"""Process-separated owned delegation controls over the real worker RPC boundary.

These controls deliberately use the gateway's authenticated WorkerRPC and the existing
RuntimeSessionStore journal.  A test-only response drop is inserted after the real server
has committed so retry proof cannot be satisfied by a fake store or a second ledger.
"""

from contextlib import closing
import asyncio
import json
import os
from pathlib import Path
import queue
import sqlite3
import time

import pytest


_SCOPE_SECRET = "private-r6-delegation-worker-secret"


def _worker_phase(home, session_id, session_key, outbox, phase, result_queue):
    """Run one physical worker owner; called by a real multiprocessing child."""
    from agent.runtime_session_store import RuntimeSessionStore, WorkerPersistenceError, WorkerRPC
    from gateway.status import get_process_start_time
    from tools.async_delegation_worker import WorkerDelegationLedger

    base_rpc = WorkerRPC(home)
    pid = os.getpid()
    birth = get_process_start_time(pid)
    assert type(birth) is int
    scope = {
        "profile_id": str(home), "session_id": session_id, "execution_id": "r6-delegation-worker",
        "generation": 0, "pid": pid, "birth": birth, "secret": _SCOPE_SECRET,
    }
    if phase == "initial":
        registered = base_rpc("worker.register", **scope, kind="compute")
        scope["epoch"] = registered["owner_epoch"]

        class DropOneResponse:
            def __init__(self, inner):
                self.inner, self.dropped = inner, False

            def __call__(self, method, **params):
                response = self.inner(method, **params)
                if method == "worker.persist" and not self.dropped:
                    self.dropped = True
                    raise TimeoutError("response lost after owner commit")
                return response

        rpc = DropOneResponse(base_rpc)
        store = RuntimeSessionStore(rpc, scope, outbox)
        try:
            ledger = WorkerDelegationLedger(store)
            delegation_id = "r6-process-response-loss"
            record = {
                "delegation_id": delegation_id, "parent_session_id": session_id,
                "session_key": session_key, "origin_ui_session_id": "", "origin_session_id": "",
                "dispatched_at": time.time(), "goal": "process separated handoff",
                "execution": {
                    "handoff_id": delegation_id, "state": "prepared", "accepted": "unknown",
                    "worker_started": False, "worker_exited": False, "owner_pid": pid,
                    "owner_started_at": birth, "owner_execution_id": scope["execution_id"],
                    "owner_scope": {key: scope[key] for key in ("profile_id", "session_id", "generation", "epoch")},
                    "error": None, "exception": None,
                },
            }
            with pytest.raises(TimeoutError, match="response lost"):
                ledger.dispatch(record)
            result_queue.put({
                "phase": phase, "delegation_id": delegation_id,
                "pending": len(store.journal["pending"]),
                "sequence": store.journal["pending"][0]["sequence"],
                "scope": dict(scope),
            })
        finally:
            store._outbox_owner.close()
        return

    # The new physical process first proves a wrong profile is refused, then uses the
    # authenticated worker.adopt result to move the exact pending journal to this PID/birth.
    wrong = dict(scope, profile_id=str(Path(home) / "foreign"))
    try:
        base_rpc("worker.adopt", **{key: wrong[key] for key in wrong if key != "epoch"})
    except WorkerPersistenceError as exc:
        result_queue.put({"phase": "wrong-owner", "error": str(exc)})
    else:
        result_queue.put({"phase": "wrong-owner", "error": "unexpected acceptance"})
    class DropAdoptResponse:
        def __init__(self, inner):
            self.inner, self.dropped = inner, False

        def __call__(self, method, **params):
            response = self.inner(method, **params)
            if method == "worker.adopt" and not self.dropped:
                self.dropped = True
                raise TimeoutError("adopt response lost after owner commit")
            return response

    with pytest.raises(TimeoutError, match="adopt response lost"):
        DropAdoptResponse(base_rpc)("worker.adopt", **{key: scope[key] for key in scope if key != "epoch"})
    adopted = base_rpc("worker.adopt", **{key: scope[key] for key in scope if key != "epoch"})
    scope["epoch"] = adopted["owner_epoch"]
    store = RuntimeSessionStore(base_rpc, scope, outbox, adopted=adopted)
    try:
        ledger = WorkerDelegationLedger(store)
        delegation_id = "r6-process-response-loss"
        execution = {
            "handoff_id": delegation_id, "state": "queued", "accepted": "yes",
            "worker_started": False, "worker_exited": False, "owner_pid": pid,
            "owner_started_at": birth, "owner_execution_id": scope["execution_id"],
            "owner_scope": {key: scope[key] for key in ("profile_id", "session_id", "generation", "epoch")},
            "error": None, "exception": None,
        }
        ledger.execution(delegation_id, execution)
        execution["state"] = "started"
        execution["worker_started"] = True
        ledger.execution(delegation_id, execution)
        execution["state"] = "runner_exited"
        execution["worker_exited"] = True
        ledger.execution(delegation_id, execution)
        event = {
            "type": "async_delegation", "delegation_id": delegation_id,
            "parent_session_id": session_id, "session_key": session_key,
            "origin_ui_session_id": "", "origin_session_id": "", "status": "completed",
            "completed_at": time.time(), "summary": "replayed once",
        }
        ledger.complete(event, {"summary": "replayed once"})
        readback = ledger.read(delegation_id)
        result_queue.put({
            "phase": phase, "pending": len(store.journal["pending"]),
            "scope": dict(store.scope), "readback": readback, "adopt_retry": True,
        })
    finally:
        store._outbox_owner.close()


def _child_result(process, result_queue):
    process.join(60)
    assert process.exitcode == 0
    values = []
    while True:
        try:
            values.append(result_queue.get_nowait())
        except queue.Empty:
            return values


@pytest.mark.platforms("linux")
def test_process_separated_worker_rpc_response_loss_restart_and_wrong_owner(tmp_path):
    """A committed dispatch survives a lost RPC reply and one authenticated adoption."""
    from tests.gateway.fixtures.local_recovery_probe import daemon, rpc, websocket

    root = Path(__file__).resolve().parents[2]
    home, user = tmp_path / "state", tmp_path / "user"
    home.mkdir(mode=0o700)
    user.mkdir()
    (home / "config.yaml").write_text(json.dumps({
        "gateway": {"multiplex_profiles": False},
        "model": {"provider": "custom", "default": "unused", "base_url": "http://127.0.0.1:9/v1"},
        "auxiliary": {"title_generation": {"enabled": False}},
        "platform_toolsets": {"cli": []},
    }))
    env = {key: os.environ[key] for key in ("PATH", "LANG", "TZ", "TIRITH_ENABLED") if key in os.environ}
    env.update(HOME=str(user), USERPROFILE=str(user), HERMES_HOME=str(home), PYTHONPATH=str(root))
    context = __import__("multiprocessing").get_context("spawn")
    results = context.Queue()

    async def create(desc):
        async with websocket(home, desc) as ws:
            response = await rpc(ws, "session.create", request_id="r6-process", source="cli", model="unused", toolsets=[])
            assert "result" in response, response
            return response["result"]["session_id"]

    def session_key(session_id):
        with closing(sqlite3.connect(home / "state.db")) as db:
            return db.execute("SELECT session_key FROM sessions WHERE id=?", (session_id,)).fetchone()[0]

    with daemon(root, home, env, barrier=False) as (_, desc):
        session_id = asyncio.run(create(desc))
        key = session_key(session_id)
        first = context.Process(target=_worker_phase, args=(home, session_id, key, tmp_path / "outbox", "initial", results))
        first.start()
        first_rows = _child_result(first, results)
        assert first_rows and first_rows[0]["pending"] == 1
        assert first_rows[0]["sequence"] == 1

    with daemon(root, home, env, barrier=False):
        second = context.Process(target=_worker_phase, args=(home, session_id, key, tmp_path / "outbox", "adopt", results))
        second.start()
        second_rows = _child_result(second, results)

    wrong = next(row for row in second_rows if row["phase"] == "wrong-owner")
    assert wrong["error"] in {"profile_mismatch", "permission_denied"}
    settled = next(row for row in second_rows if row["phase"] == "adopt")
    assert settled["pending"] == 0
    assert settled["scope"]["profile_id"] == str(home)
    assert settled["scope"]["session_id"] == session_id
    assert settled["scope"]["generation"] == 0
    assert settled["scope"]["epoch"] > first_rows[0]["scope"]["epoch"]
    assert settled["scope"]["pid"] != first_rows[0]["scope"]["pid"]
    assert settled["adopt_retry"] is True
    assert settled["readback"]["state"] == "completed"


def test_postcommit_queue_failure_retries_same_worker_receipt(tmp_path, monkeypatch):
    """The real authenticated bridge keeps one receipt when queue publication fails."""
    from types import SimpleNamespace
    from gateway.session_contract import SessionRef
    from gateway.status import get_process_start_time
    from gateway.session_worker import _claim, worker_request
    from hermes_state import SessionDB

    async def worker_delegation_request(connection, ref, params):
        # Preserve the historical test oracle name while entering the authenticated
        # worker_request(operation='persist') boundary.
        return await worker_request(connection, ref, params, operation="persist")
    from hermes_state_runtime import begin_runtime_epoch, register_worker_execution
    from tools.async_delegation import _initialize_schema
    from tools.process_registry import process_registry

    class FailingQueue:
        def __init__(self):
            self.calls = 0
            self.events = []
        def put(self, _event):
            self.calls += 1
            if self.calls == 1:
                raise OSError("completion queue unavailable after commit")
            self.events.append(_event)

    queue_impl = FailingQueue()
    monkeypatch.setattr(process_registry, "completion_queue", queue_impl)
    db = SessionDB(tmp_path / "state.db")
    worker = __import__("subprocess").Popen([os.sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        db.create_session("parent", source="cli")
        epoch = begin_runtime_epoch(db, instance_id="owner")
        with closing(sqlite3.connect(tmp_path / "state.db")) as conn:
            _initialize_schema(conn)
        authority = SimpleNamespace(db=db, epoch=epoch, profile_id="profile", _require_admission_open=lambda: None)
        actor = SimpleNamespace(profile_id="profile", subject="alice", capabilities={"worker:adopt"})
        connection = SimpleNamespace(authority=authority, actor=actor)
        ref = SessionRef("profile", "parent")
        birth = get_process_start_time(worker.pid)
        assert type(birth) is int
        scope = dict(profile_id="profile", session_id="parent", execution_id="worker", generation=0,
                     pid=worker.pid, birth=birth, secret=_SCOPE_SECRET)
        claim = _claim(connection, ref, scope)
        register_worker_execution(db, epoch=epoch, execution_id="worker", session_id="parent", generation=0,
                                  kind="compute", adoption_secret=claim)
        owner_started_at = get_process_start_time(worker.pid)
        dispatch = dict(delegation_id="queue-fault", task={"goal": "inert"}, dispatched_at=time.time(),
                        parent_session_id="parent", session_key="", origin_ui_session_id="", origin_session_id="",
                        execution={
                            "handoff_id": "queue-fault", "state": "prepared", "accepted": "unknown",
                            "worker_started": False, "worker_exited": False, "owner_pid": worker.pid,
                            "owner_started_at": owner_started_at, "owner_execution_id": "worker",
                            "owner_scope": {"profile_id": "profile", "session_id": "parent",
                                            "generation": 0, "epoch": epoch},
                            "error": None, "exception": None,
                        })
        params = scope | dict(epoch=epoch, sequence=1, operation="delegation.dispatch", payload=dispatch)
        asyncio.run(worker_delegation_request(connection, ref, params))
        event = dict(type="async_delegation", delegation_id="queue-fault", parent_session_id="parent",
                     session_key="", origin_ui_session_id="", origin_session_id="", status="completed",
                     completed_at=time.time(), summary="durable")
        complete = scope | dict(epoch=epoch, sequence=2, operation="delegation.complete",
                                 payload={"event": event, "result": {"summary": "durable"}})
        with pytest.raises(OSError, match="queue unavailable"):
            asyncio.run(worker_delegation_request(connection, ref, complete))
        with db._read_ctx() as conn:
            row = conn.execute("SELECT state,event_json FROM async_delegations WHERE delegation_id=?", ("queue-fault",)).fetchone()
            assert row[0] == "completed" and json.loads(row[1]) == event
            execution_row = conn.execute("SELECT execution_json FROM async_delegations WHERE delegation_id=?", ("queue-fault",)).fetchone()
            assert json.loads(execution_row[0])["owner_scope"] == {"profile_id": "profile", "session_id": "parent",
                                                                     "generation": 0, "epoch": epoch}
        assert asyncio.run(worker_delegation_request(connection, ref, complete)) == {"value": None}
        assert queue_impl.events == [event | {"restored": True}]
        with db._read_ctx() as conn:
            assert conn.execute("SELECT COUNT(*) FROM worker_receipts WHERE sequence=2").fetchone()[0] == 1
    finally:
        worker.terminate()
        worker.wait(timeout=5)
        db.close()


def test_durable_prune_never_evicts_accepted_unknown_or_pending(tmp_path, monkeypatch):
    from hermes_state import SessionDB
    from tools import async_delegation as ad

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    db = SessionDB(tmp_path / "state.db")
    try:
        accepted_claim = {
            "handoff_id": "accepted", "state": "runner_exited", "accepted": "yes",
            "worker_started": True, "worker_exited": True, "owner_pid": 321,
            "owner_started_at": 4, "owner_execution_id": "owner",
            "owner_scope": {"profile_id": "p", "session_id": "s", "generation": 0, "epoch": 1},
            "error": None, "exception": None,
        }
        db._execute_write(lambda conn: conn.executemany(
            "INSERT INTO async_delegations(delegation_id,origin_session,parent_session_id,state,dispatched_at,updated_at,delivery_state,owner_pid,owner_started_at,owner_execution_id,execution_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("unknown", "s", "s", "completed", 1, 1, "pending", None, None, None,
                 json.dumps({"accepted": "unknown", "worker_exited": False, "state": "unknown"})),
                ("accepted", "s", "s", "completed", 1, 1, "delivered", 321, 4, "owner",
                 json.dumps(accepted_claim)),
                ("missing-proof", "s", "s", "completed", 1, 1, "delivered", 322, 5, "owner-2",
                 json.dumps({"handoff_id": "missing-proof", "accepted": "yes",
                             "worker_exited": True, "state": "runner_exited"})),
                ("missing-started", "s", "s", "completed", 1, 1, "delivered", 323, 6, "owner-3",
                 json.dumps({"handoff_id": "missing-started", "accepted": "yes",
                             "worker_exited": True, "state": "runner_exited", "owner_pid": 323,
                             "owner_started_at": 6, "owner_execution_id": "owner-3",
                             "owner_scope": {"profile_id": "p", "session_id": "s", "generation": 0, "epoch": 1},
                             "error": None, "exception": None})),
                ("false-started", "s", "s", "completed", 1, 1, "delivered", 324, 7, "owner-4",
                 json.dumps({"handoff_id": "false-started", "accepted": "yes",
                             "worker_started": False, "worker_exited": True, "state": "runner_exited",
                             "owner_pid": 324, "owner_started_at": 7, "owner_execution_id": "owner-4",
                             "owner_scope": {"profile_id": "p", "session_id": "s", "generation": 0, "epoch": 1},
                             "error": None, "exception": None})),
            ]))
        monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 0)
        ad._prune_durable_records()
        with db._read_ctx() as conn:
            rows = {row[0] for row in conn.execute("SELECT delegation_id FROM async_delegations")}
        assert "unknown" in rows
        assert "missing-proof" in rows
        assert "missing-started" in rows
        assert "false-started" in rows
        assert "accepted" not in rows
    finally:
        db.close()
