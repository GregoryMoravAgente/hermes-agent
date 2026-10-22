"""Private real-worker producer fixture for AGC-1663.

This child deliberately keeps the production ``ComputeHost._run_real_turn`` ->
``server._run_prompt_submit`` -> ``session_lifecycle._start_session_work`` path.
The only synthetic seam is Hermes' existing ``HERMES_ISO_CERTIFY_SYNTH_TURN``
AIAgent factory; its ``run_conversation`` is wrapped only to hold, remove the
real session record, and raise after a parent-owned release marker. No producer
receipt or terminal frame is fabricated by this fixture.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import threading
import time

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
_WIRE_STDOUT = sys.stdout

from tests.waiting import wait_until
_SID = "real-production-worker-sid"
_SESSION_KEY = "real-production-worker-session"


def _write(path: str, value: str) -> None:
    Path(path).write_text(value, encoding="utf-8")


class _InertLease:
    """Only prevents a test child from claiming a real cross-process slot."""

    lease_id = "synthetic-child-inert-lease"
    released = False
    enabled = False

    def release(self) -> None:
        self.released = True


def _install_neutral_side_effects(server) -> None:
    """Remove unrelated profile/notification work while preserving turn admission and worker start."""
    server._wire_callbacks = lambda _sid: None
    server._sync_agent_model_with_config = lambda _sid, _session: None
    server._sync_agent_compression_with_config = lambda _sid, _session: None
    server._sync_agent_fallback_with_config = lambda _sid, _session: None
    server._sync_bot_capabilities = lambda _sid, _session: None
    server._apply_pending_model_switch = lambda _sid, _session: None
    server._session_cwd = lambda session: str(session.get("cwd") or _REPO_ROOT)
    server._register_session_cwd = lambda _session: None
    server._tts_stream_begin = lambda: None
    server._sync_session_key_after_compress = lambda *args, **kwargs: None
    server._get_usage = lambda _agent: {}
    server._ensure_session_db_row = lambda _session: True
    # These are lifecycle side services, not the turn/worker path under test.
    server._wire_session_agent = lambda _sid, _key, _agent: False
    server._start_session_services = lambda _sid, _key, _session: None
    server._schedule_mcp_late_refresh = lambda _sid, _agent: None
    server._hydrate_session_cwd = lambda *args, **kwargs: None
    server._session_todo_state = lambda _session: None
    server._notify_session_boundary = lambda *args, **kwargs: []


def _install_submit_observer(server, args: argparse.Namespace) -> None:
    """Observe the real submit result and the real worker's natural thread exit."""
    original_submit = server._run_prompt_submit

    def observed_submit(*submit_args, **submit_kwargs):
        result = original_submit(*submit_args, **submit_kwargs)
        session = submit_args[2]
        _write(args.submit_return, json.dumps({"accepted": bool(result)}, sort_keys=True))
        run_thread = session.get("_run_thread")
        if run_thread is not None and hasattr(run_thread, "join"):
            def mark_after_worker_exit() -> None:
                run_thread.join()
                _write(args.worker_exited, "exited")

            threading.Thread(
                target=mark_after_worker_exit,
                name="real-worker-exit-observer",
                daemon=True,
            ).start()
        return result

    # This is an observer around the production function, not a replacement producer.
    server._run_prompt_submit = observed_submit


def _install_held_real_agent(server, args: argparse.Namespace) -> None:
    """Wrap the existing synthetic AIAgent's provider call; worker ownership stays production-owned."""
    from tui_gateway.synthetic_turn import SyntheticHeavyAgent

    def held_run(self, message, **_kwargs):
        _write(args.worker_started, "started")
        popped = server._pop_session_by_id(_SID)
        _write(
            args.session_removed,
            "removed" if popped is not None and popped.get("agent") is self else "unexpected",
        )
        wait_until(
            lambda: Path(args.worker_release).exists(),
            timeout=30,
            what="real synthetic worker release",
        )
        # Raising here is the provider/agent failure. The production prompt worker catches it,
        # performs its normal turn cleanup, and the production ComputeHost joins that same thread.
        raise RuntimeError("real synthetic provider failure after session removal")

    SyntheticHeavyAgent.run_conversation = held_run


def _session_record(agent, *, closing: bool = False) -> dict:
    return {
        "agent": agent,
        "agent_error": "synthetic agent unavailable" if agent is None else None,
        "session_key": _SESSION_KEY,
        "profile_home": None,
        "history": [],
        "history_lock": threading.RLock(),
        "history_version": 0,
        "running": False,
        "attached_images": [],
        "image_counter": 0,
        "cols": 80,
        "slash_worker": None,
        "show_reasoning": False,
        "tool_progress_mode": "all",
        "inflight_turn": None,
        "cwd": str(_REPO_ROOT),
        "source": "desktop",
        "transport": None,
        "active_session_lease": _InertLease(),
        "_closing": closing,
        "created_at": time.time(),
        "last_active": time.time(),
    }


def _install_no_worker_session(server, mode: str) -> None:
    agent = None
    if mode == "closing":
        # The real production factory still builds the existing synthetic AIAgent; only the
        # lifecycle record is marked closing, so _admit_prompt_turn returns False before spawning.
        agent = server._make_agent(_SID, _SESSION_KEY, session_id=_SESSION_KEY)
    server._sessions[_SID] = _session_record(agent, closing=mode == "closing")


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("real-worker", "no-agent", "closing"), required=True)
    parser.add_argument("--worker-release", required=True)
    parser.add_argument("--worker-started", required=True)
    parser.add_argument("--worker-exited", required=True)
    parser.add_argument("--session-removed", required=True)
    parser.add_argument("--submit-return", required=True)
    args = parser.parse_args(argv)

    # This existing source-owned seam builds SyntheticHeavyAgent from server._make_agent;
    # no network/provider credential is consulted.
    os.environ["HERMES_ISO_CERTIFY_SYNTH_TURN"] = "1"
    from tui_gateway import compute_host, server
    _install_neutral_side_effects(server)
    _install_submit_observer(server, args)
    if args.mode == "real-worker":
        _install_held_real_agent(server, args)
    else:
        _install_no_worker_session(server, args.mode)

    os.environ["HERMES_COMPUTE_HOST_CHILD"] = "1"
    compute_host.run_host(stdout=_WIRE_STDOUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
