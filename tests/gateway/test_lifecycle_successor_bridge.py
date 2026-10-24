"""Cross-process lifecycle handoff proofs over real profile locks and local control transport."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


_CHILD = r'''
import asyncio
import json
import os
from pathlib import Path
import sys
from threading import Event
from types import SimpleNamespace

home = Path(os.environ["HERMES_HOME"]).resolve()
mode = sys.argv[1]
epoch = int(sys.argv[2])

from gateway.lifecycle_custody import LifecycleNativeSettlement
from gateway.lifecycle_ledger import record_startup
from gateway.runtime_ownership import process_ownership
from hermes_state_registry import SessionDbCloseReceipt
from hermes_state_runtime import begin_runtime_epoch


class _Authority:
    def __init__(self, value):
        self.epoch = value
        self.profile_id = str(home)


class _Runner:
    pass


runner = _Runner()
runner.config = SimpleNamespace(multiplex_profiles=False, _runtime_profile_homes=None)
runner.session_runtime_descriptor = {
    "instance_id": f"{mode}-{os.getpid()}", "authority_epoch": epoch,
}
runner.session_authorities = SimpleNamespace(for_home=lambda _home: _Authority(epoch))


class _RuntimeDb:
    def __init__(self, path):
        import sqlite3
        self.connection = sqlite3.connect(path)

    def _execute_write(self, operation):
        result = operation(self.connection)
        self.connection.commit()
        return result


home.mkdir(parents=True, exist_ok=True)
runtime_db = _RuntimeDb(home / "state.db")
runtime_db.connection.execute(
    "CREATE TABLE IF NOT EXISTS runtime_epoch "
    "(singleton INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, instance_id TEXT NOT NULL)"
)
runtime_db.connection.commit()


def _emit(value):
    print(value, flush=True)


def _state():
    return json.loads((home / "state" / "gateway.lifecycle.json").read_text())


process_ownership.reserve([home])
record_startup(home)
runner.session_runtime_descriptor["instance_id"] = (
    f"predecessor-{os.getpid()}" if mode.startswith("a-")
    else f"successor-{mode}-{os.getpid()}"
)
runner.session_runtime_descriptor["served_profiles"] = [
    {"profile_id": str(home), "home": str(home)},
]
assert begin_runtime_epoch(
    runtime_db, instance_id=runner.session_runtime_descriptor["instance_id"]
) == epoch

if mode.startswith("a-"):
    if mode == "a-before-prepared":
        _emit("before-prepared")
        os._exit(0)
    from gateway.lifecycle_handoff import prepare_predecessor_handoff, finalize_predecessor_release

    shutdown_event = Event()
    shutdown_event.set()
    native = LifecycleNativeSettlement(
        "restart", object(), shutdown_event, True,
        db_close_receipt=SessionDbCloseReceipt("test", 0, 0),
    )
    prepared = prepare_predecessor_handoff(runner, native)
    _emit(f"prepared={prepared}")
    if mode == "a-clean":
        summary = process_ownership.close()
        released = finalize_predecessor_release(home, summary, pid_removed=True)
        _emit(f"released={released}")
    else:
        # An abrupt process death after durable preparation leaves recovery to the next owner.
        process_ownership.close()
    if mode == "a-prepared":
        os._exit(0)
    raise SystemExit(0)


from gateway.lifecycle_handoff import (
    adopt_successor_handoff, handoff_requires_successor,
    record_successor_ready, record_successor_ready_from_control,
)

adopted = adopt_successor_handoff(runner, home)
_emit(f"adopted={adopted}")
_emit(f"requires={handoff_requires_successor(home)}")

if mode == "b-retry-first":
    process_ownership.close()
    os._exit(0)

if adopted is True and mode in {"b-ready", "b-forged", "b-wrong-served", "b-foreign-peer", "b-stale-owner", "b-replaced-control", "b-replaced-after-identify"}:
    async def _roundtrip():
        from gateway.control_socket import GatewayControlServer
        from gateway.lifecycle_handoff import _control_endpoint_identity
        from gateway.status import _build_pid_record

        payload = _build_pid_record()
        payload.update({
            "state": "ready",
            "instance_id": runner.session_runtime_descriptor["instance_id"],
            "authority_epoch": epoch,
            "handoff_id": runner.session_runtime_descriptor["handoff_id"],
            "handoff_lineage": (
                "forged-lineage" if mode == "b-forged"
                else runner.session_runtime_descriptor["handoff_lineage"]
            ),
            "handoff_attempt_id": runner.session_runtime_descriptor["handoff_attempt_id"],
            "served_profiles": runner.session_runtime_descriptor["served_profiles"],
            "capabilities": ["session-authority-v1"],
        })
        if mode == "b-wrong-served":
            payload["served_profiles"] = [{"profile_id": str(home / "wrong"), "home": str(home / "wrong")}]
        if mode == "b-foreign-peer":
            payload["pid"] = os.getppid()
        if mode == "b-stale-owner":
            lock_path = home / "gateway.lock"
            lock_record = json.loads(lock_path.read_text())
            lock_record["owner_epoch"] = "stale-owner-epoch"
            lock_path.write_text(json.dumps(lock_record))
        server = GatewayControlServer(home, verb_handlers={"identify": lambda: payload})
        if not await server.start():
            raise RuntimeError("control server did not start")
        try:
            if mode == "b-replaced-after-identify":
                import gateway.control_socket as control_socket

                loop = asyncio.get_running_loop()
                identify_calls = 0
                original_identify = control_socket._identify_gateway_with_control_socket_identity

                async def replace_after_middle_identify():
                    nonlocal server
                    await server.stop()
                    server = GatewayControlServer(home, verb_handlers={"identify": lambda: payload})
                    if not await server.start():
                        raise RuntimeError("replacement control server did not start")

                def identify_with_replacement(*args, **kwargs):
                    nonlocal identify_calls
                    response = original_identify(*args, **kwargs)
                    identify_calls += 1
                    if identify_calls == 2:
                        replacement = asyncio.run_coroutine_threadsafe(
                            replace_after_middle_identify(), loop,
                        )
                        replacement.result(timeout=5)
                    return response

                control_socket._identify_gateway_with_control_socket_identity = identify_with_replacement
                try:
                    ready = await asyncio.to_thread(record_successor_ready_from_control, runner, home)
                finally:
                    control_socket._identify_gateway_with_control_socket_identity = original_identify
                _emit(f"identify_calls={identify_calls}")
                assert identify_calls == 3, identify_calls
            elif mode != "b-replaced-control":
                ready = await asyncio.to_thread(record_successor_ready_from_control, runner, home)
            else:
                before = await asyncio.to_thread(_control_endpoint_identity, home)
                await server.stop()
                server = GatewayControlServer(home, verb_handlers={"identify": lambda: payload})
                if not await server.start():
                    raise RuntimeError("replacement control server did not start")
                after = await asyncio.to_thread(_control_endpoint_identity, home)
                ready = record_successor_ready(
                    home, runner, payload, control_before=before, control_after=after,
                )
            _emit(f"ready={ready}")
        finally:
            await server.stop()

    asyncio.run(_roundtrip())

_emit(json.dumps(_state(), sort_keys=True))
runtime_db.connection.close()
process_ownership.close()
'''


def _run_child(home: Path, mode: str, epoch: int) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["HERMES_HOME"] = str(home)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    return subprocess.run(
        [sys.executable, "-c", _CHILD, mode, str(epoch)],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    )


def _journal(home: Path) -> dict:
    return json.loads((home / "state" / "gateway.lifecycle.json").read_text())


def test_two_process_clean_handoff_requires_real_control_ready(tmp_path):
    home = tmp_path / "home"
    _run_child(home, "a-clean", 1)
    result = _run_child(home, "b-ready", 2)
    journal = _journal(home)["handoff"]
    assert "ready=True" in result.stdout
    assert journal["state"] == "ready"
    assert journal["lineage"] == journal["attempts"][0]["lineage"]
    assert journal["release"]["mode"] == "self"
    assert all(value == "released" for value in journal["release"]["outcomes"].values())
    assert journal["attempts"][0]["state"] == "ready"
    assert journal["attempts"][0]["authority_epoch"] == 2
    assert journal["ready"]["control_digest"] == journal["attempts"][0]["control_digest"]


def test_successor_refuses_replaced_predecessor_record_witness(tmp_path):
    """The top-level diagnostic copy cannot replace the signed lineage witness set."""
    home = tmp_path / "home"
    _run_child(home, "a-clean", 1)
    journal = _journal(home)
    records = journal["handoff"]["predecessor_records"]
    assert records
    records[0] = {"home": records[0]["home"], "record": {"hermes_home": str(home)}}
    (home / "state" / "gateway.lifecycle.json").write_text(
        json.dumps(journal), encoding="utf-8",
    )
    result = _run_child(home, "b-ready", 2)
    assert "adopted=False" in result.stdout
    assert _journal(home)["handoff"]["state"] == "released"


def test_successor_refuses_forged_control_lineage(tmp_path):
    home = tmp_path / "home"
    _run_child(home, "a-clean", 1)
    result = _run_child(home, "b-forged", 2)
    journal = _journal(home)["handoff"]
    assert "ready=False" in result.stdout
    assert journal["state"] == "unknown"
    assert journal["attempts"][-1]["state"] == "unknown"


def test_control_peer_subject_is_bound_to_pre_post_endpoint_identity():
    from gateway.lifecycle_handoff import _control_endpoint_key

    before = {
        "transport": "windows-pipe", "endpoint": r"\\.\pipe\hermes", "peer_pid": 41,
        "peer_birth": 1001.0, "peer_subject": "sid:S-1-5-21-owner",
    }
    after = dict(before, peer_subject="sid:S-1-5-21-foreign")
    assert _control_endpoint_key(before) != _control_endpoint_key(after)


def test_kill_before_prepared_does_not_mint_successor_handoff(tmp_path):
    home = tmp_path / "home"
    _run_child(home, "a-before-prepared", 1)
    result = _run_child(home, "b-ready", 2)
    journal = _journal(home)
    assert "adopted=None" in result.stdout
    assert "requires=False" in result.stdout
    assert "handoff" not in journal


def test_prepared_recovery_and_failed_successor_retry_reuse_lineage(tmp_path):
    home = tmp_path / "home"
    _run_child(home, "a-prepared", 1)
    first = _run_child(home, "b-retry-first", 2)
    assert "adopted=True" in first.stdout
    second = _run_child(home, "b-retry-second", 3)
    journal = _journal(home)["handoff"]
    assert "adopted=True" in second.stdout
    assert journal["release"]["mode"] == "recovery-after-death"
    assert journal["state"] == "released"
    assert len(journal["attempts"]) == 2
    assert journal["attempts"][0]["state"] == "failed"
    assert journal["attempts"][1]["state"] == "starting"
    assert journal["attempts"][0]["lineage"] == journal["attempts"][1]["lineage"]
    assert journal["attempts"][1]["authority_epoch"] > journal["attempts"][0]["authority_epoch"]


@pytest.mark.parametrize("mode", ["b-wrong-served", "b-foreign-peer", "b-stale-owner", "b-replaced-control", "b-replaced-after-identify"])
def test_successor_refuses_changed_runtime_owner_or_control_identity(tmp_path, mode):
    home = tmp_path / "home"
    _run_child(home, "a-clean", 1)
    result = _run_child(home, mode, 2)
    journal = _journal(home)["handoff"]
    assert "ready=False" in result.stdout
    assert journal["state"] == "unknown"
    assert journal["attempts"][-1]["state"] == "unknown"
