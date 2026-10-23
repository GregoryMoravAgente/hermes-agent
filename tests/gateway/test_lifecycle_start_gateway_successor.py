"""Behavioral lifecycle proof through two real ``start_gateway`` processes.

This complements the focused handoff tests with the production bootstrap and shutdown path.  Each
child is a fresh interpreter running the real gateway; the parent only uses the existing local
control client to observe startup and request the planned stop.  No runner, close receipt, or READY
payload is manufactured by the test.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from gateway.control_socket import identify_gateway, pause_gateway_for_update
from tests.waiting import wait_until


_REAL_GATEWAY_CHILD = r'''
import asyncio
import os
from pathlib import Path

from gateway.config import GatewayConfig
from gateway.run_bootstrap import start_gateway

home = Path(os.environ["HERMES_HOME"]).resolve()
config = GatewayConfig(
    platforms={},
    sessions_dir=home / "sessions",
    multiplex_profiles=False,
    loop_watchdog=False,
    on_all_adapters_down="stay_alive",
)
asyncio.run(start_gateway(config=config, replace=False, verbosity=None, force=False, idle_exit=False))
'''


def _launch_gateway(home: Path, label: str) -> tuple[subprocess.Popen[str], Path, Path]:
    log_dir = home / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{label}.stdout"
    stderr_path = log_dir / f"{label}.stderr"
    env = os.environ.copy()
    env.update({"HERMES_HOME": str(home), "PYTHONPATH": str(Path(__file__).resolve().parents[2])})
    stdout = stdout_path.open("w", encoding="utf-8")
    stderr = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, "-c", _REAL_GATEWAY_CHILD],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        stdout=stdout,
        stderr=stderr,
        text=True,
    )
    stdout.close()
    stderr.close()
    return process, stdout_path, stderr_path


def _read_journal(home: Path) -> dict | None:
    try:
        return json.loads((home / "state" / "gateway.lifecycle.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _handoff_state(home: Path, state: str) -> dict | None:
    record = _read_journal(home)
    handoff = record.get("handoff") if isinstance(record, dict) else None
    return handoff if isinstance(handoff, dict) and handoff.get("state") == state else None


def _wait_for_process_exit(process: subprocess.Popen[str], *, what: str) -> int:
    wait_until(lambda: process.poll() is not None, timeout=120, what=what)
    assert process.returncode is not None
    return process.returncode


def _stop_if_running(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _assert_exit_75(process: subprocess.Popen[str], stderr_path: Path, *, what: str) -> None:
    code = _wait_for_process_exit(process, what=what)
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")
    assert code == 75, f"{what} exited {code}; stderr={stderr[-4000:]}"


def test_real_start_gateway_two_process_handoff_releases_and_reaches_ready(tmp_path: Path) -> None:
    """The production shutdown receipt gates the next real gateway's authenticated READY."""
    home = (tmp_path / "hermes-home").resolve()
    predecessor = successor = None
    try:
        predecessor, _predecessor_stdout, predecessor_stderr = _launch_gateway(home, "predecessor")
        predecessor_identity = wait_until(
            lambda: identify_gateway(home, timeout=1.0),
            timeout=90,
            what="real predecessor control identify",
        )
        pause = wait_until(
            lambda: pause_gateway_for_update(home, timeout=2.0),
            timeout=30,
            what="real predecessor planned stop acknowledgement",
        )
        assert pause.get("pausing") is True
        _assert_exit_75(predecessor, predecessor_stderr, what="real predecessor")

        released = wait_until(
            lambda: _handoff_state(home, "released"),
            timeout=15,
            what="durable predecessor release",
        )
        assert released["predecessor"]["pid"] == predecessor_identity["pid"]
        assert released["release"]["pid_removed"] is True
        assert all(value == "released" for value in released["release"]["outcomes"].values())
        assert (home / "state.db").is_file()
        # These digests are emitted by the production shutdown settlement and are the durable
        # checked-close/native witnesses consumed by the successor, not test-supplied receipt data.
        assert all(
            isinstance(released["proofs"].get(key), str)
            and len(released["proofs"][key]) == 64
            and set(released["proofs"][key]) != {"0"}
            for key in ("close_digest", "native_digest")
        )

        successor, _successor_stdout, successor_stderr = _launch_gateway(home, "successor")
        successor_identity = wait_until(
            lambda: _identify_successor(home, released["lineage"]),
            timeout=90,
            what="real successor control identify",
        )
        assert successor_identity["handoff_lineage"] == released["lineage"]
        assert successor_identity["authority_epoch"] > released["predecessor"]["authority_epoch"]

        ready = wait_until(
            lambda: _handoff_state(home, "ready"),
            timeout=90,
            what="durable successor control READY",
        )
        attempt = ready["attempts"][-1]
        assert attempt["state"] == "ready"
        assert attempt["pid"] == successor_identity["pid"]
        assert ready["ready"]["control_digest"] == attempt["control_digest"]
        assert ready["release"]["outcomes"] == released["release"]["outcomes"]

        successor_pause = wait_until(
            lambda: pause_gateway_for_update(home, timeout=2.0),
            timeout=30,
            what="real successor planned stop acknowledgement",
        )
        assert successor_pause.get("pausing") is True
        _assert_exit_75(successor, successor_stderr, what="real successor")
    finally:
        if successor is not None:
            _stop_if_running(successor)
        if predecessor is not None:
            _stop_if_running(predecessor)


def _identify_successor(home: Path, lineage: str) -> dict | None:
    identity = identify_gateway(home, timeout=1.0)
    return identity if identity and identity.get("handoff_lineage") == lineage else None


@pytest.mark.asyncio
async def test_successor_attestation_refusal_never_publishes_public_ready(monkeypatch, tmp_path):
    """The production bootstrap gate refuses before the public descriptor is mutated."""
    from gateway import lifecycle_handoff, run_bootstrap, run_runtime

    calls = []
    runner = type("Runner", (), {"session_runtime_descriptor": {"state": "starting"}})()

    def refuse(_runner, _home):
        calls.append(("attest", _runner.session_runtime_descriptor["state"]))
        return False

    def publish(_runner):
        calls.append(("publish", _runner.session_runtime_descriptor["state"]))
        _runner.session_runtime_descriptor["state"] = "ready"

    monkeypatch.setattr(lifecycle_handoff, "verify_successor_ready", refuse)
    monkeypatch.setattr(run_runtime, "publish_gateway_runtime_ready", publish)
    with pytest.raises(RuntimeError, match="successor READY control roundtrip"):
        await run_bootstrap._attest_successor_before_public_ready(runner, tmp_path)

    assert calls == [("attest", "starting")]
    assert runner.session_runtime_descriptor["state"] == "starting"
