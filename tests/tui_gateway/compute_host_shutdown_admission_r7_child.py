"""Real compute-host child for the shutdown-wins producer settlement race.

The fixture only gates the real ``ComputeHost._handle_turn_start`` entry.  The
production reader, executor admission, terminal reply and private completion
pipe remain in use; the gate exists to make the shutdown interleaving repeatable.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import threading
import types


_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
_WIRE_STDOUT = sys.stdout

from tests.waiting import wait_until
from tui_gateway import compute_host, server
from tui_gateway.compute_host import ComputeHost


_SID = "r7-shutdown-sid"
_SESSION_KEY = "r7-shutdown-session"


def _agent() -> types.SimpleNamespace:
    def run_conversation(prompt, *, conversation_history=None, stream_callback=None, **_kwargs):
        if stream_callback is not None:
            stream_callback("synthetic terminal")
        return {
            "final_response": "synthetic terminal",
            "messages": [
                *(conversation_history or []),
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": "synthetic terminal"},
            ],
        }

    return types.SimpleNamespace(
        session_id=_SESSION_KEY,
        run_conversation=run_conversation,
        clear_interrupt=lambda: None,
        hard_interrupt=lambda *args, **kwargs: None,
    )


def _session() -> dict:
    return {
        "agent": _agent(),
        "session_key": _SESSION_KEY,
        # The child must retain the actual profile scope from its inherited
        # synthetic environment; an empty value redirects the registry to cwd.
        "profile_home": os.environ["HERMES_HOME"],
        "history": [],
        "history_lock": threading.Lock(),
        "history_version": 0,
        "running": False,
        "attached_images": [],
        "image_counter": 0,
        "cols": 80,
        "slash_worker": None,
        "show_reasoning": False,
        "tool_progress_mode": "all",
        "inflight_turn": None,
        "active_session_lease": None,
    }


def _install_neutral_turn_seams() -> None:
    server._wire_callbacks = lambda _sid: None
    server._sync_agent_model_with_config = lambda _sid, _session: None
    server._session_cwd = lambda session: str(session.get("cwd") or _REPO_ROOT)
    server._register_session_cwd = lambda _session: None
    server._tts_stream_begin = lambda: None
    server._sync_session_key_after_compress = lambda *args, **kwargs: None
    server._get_usage = lambda _agent: {}


def _install_delayed_turn(paths: argparse.Namespace) -> None:
    original_turn_start = ComputeHost._handle_turn_start

    def delayed_turn_start(self: ComputeHost, frame: dict) -> None:
        Path(paths.turn_entered).write_text("entered", encoding="utf-8")
        wait_until(
            lambda: (
                Path(paths.allow_turn).exists()
                or bool(getattr(self, "_turn_admission_expired", False))
            ),
            timeout=30,
            what="test-owned delayed turn admission release or expired shutdown",
        )
        if getattr(self, "_turn_admission_expired", False):
            # The test-owned gate is an admitted, unresolved handler. A real
            # shutdown boundary may stop waiting here, but it must not fabricate
            # a terminal frame or release the parent-owned lease.
            return
        original_turn_start(self, frame)

    ComputeHost._handle_turn_start = delayed_turn_start

    if paths.gate_before_admission:
        original_handle_frame = ComputeHost.handle_frame

        def delayed_handle_frame(self: ComputeHost, frame: dict) -> None:
            if frame.get("type") == "turn.start":
                Path(paths.frame_received).write_text("received", encoding="utf-8")
                wait_until(
                    lambda: (
                        Path(paths.allow_frame).exists()
                        or self._shutdown_requested.is_set()
                    ),
                    timeout=30,
                    what="test-owned pre-admission frame release or shutdown",
                )
                if self._shutdown_requested.is_set():
                    # This frame has not crossed the production admission
                    # boundary. Let the reader finish without claiming work or
                    # publishing an unauthenticated refusal.
                    return
            original_handle_frame(self, frame)

        ComputeHost.handle_frame = delayed_handle_frame

    original_shutdown = ComputeHost.shutdown

    def observed_shutdown(self: ComputeHost, *args, **kwargs) -> None:
        Path(paths.shutdown_entered).write_text("entered", encoding="utf-8")
        if paths.shutdown_wait is not None:
            kwargs = {**kwargs, "wait": float(paths.shutdown_wait)}
        original_shutdown(self, *args, **kwargs)
        if paths.admission_expired and getattr(self, "_turn_admission_expired", False):
            Path(paths.admission_expired).write_text("expired", encoding="utf-8")

    ComputeHost.shutdown = observed_shutdown


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turn-entered", required=True)
    parser.add_argument("--allow-turn", required=True)
    parser.add_argument("--shutdown-entered", required=True)
    parser.add_argument("--frame-received")
    parser.add_argument("--allow-frame")
    parser.add_argument("--gate-before-admission", action="store_true")
    parser.add_argument("--shutdown-wait", type=float)
    parser.add_argument("--admission-expired")
    args = parser.parse_args(argv)

    _install_neutral_turn_seams()
    server._sessions[_SID] = _session()
    _install_delayed_turn(args)
    os.environ["HERMES_COMPUTE_HOST_CHILD"] = "1"
    compute_host.run_host(stdout=_WIRE_STDOUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
