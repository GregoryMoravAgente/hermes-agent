"""Real ComputeHost child that gates stdin after source-bound hello."""

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
_WIRE_STDIN = getattr(sys.stdin, "buffer", sys.stdin)

from tests.waiting import wait_until
from tui_gateway import compute_host, server



_SID = "pipe-backpressure-sid"
_SESSION_KEY = "pipe-backpressure-session"


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
        # Keep the synthetic session bound to the same profile home that the
        # parent supplied through ``synthetic_host_env``.  An empty value is
        # interpreted as the current working directory by active_sessions.
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


class _GatedInputProxy:
    """Fixture-only fallback for source revisions without ``_iter_control_lines``."""

    def __init__(self, stream, ready: Path, release: Path, finished: Path) -> None:
        self._stream = stream
        self._ready = ready
        self._release = release
        self._finished = finished

    def fileno(self):
        return self._stream.fileno()

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __iter__(self):
        try:
            self._ready.write_text("reader-ready", encoding="utf-8")
            wait_until(
                self._release.exists,
                timeout=30,
                what="test-owned legacy stdin reader release",
            )
            yield from self._stream
        finally:
            self._finished.write_text("reader-finished", encoding="utf-8")


def _install_gated_control_reader(ready: Path, release: Path, finished: Path):
    """Gate the fixture while delegating the real pipe read to production."""
    original_reader = getattr(compute_host, "_iter_control_lines", None)
    if not callable(original_reader):
        # The pre-stop-aware source iterates stdin directly. Keep that exact
        # production path and gate only the test-owned stream proxy.
        return _GatedInputProxy(_WIRE_STDIN, ready, release, finished)

    def gated_reader(stream, stop):
        try:
            ready.write_text("reader-ready", encoding="utf-8")
            wait_until(
                lambda: release.exists() or stop.is_set(),
                timeout=30,
                what="test-owned real stdin reader release",
            )
            if stop.is_set():
                return
            yield from original_reader(stream, stop)
        finally:
            finished.write_text("reader-finished", encoding="utf-8")

    compute_host._iter_control_lines = gated_reader
    return _WIRE_STDIN


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--reader-finished", required=True)
    args = parser.parse_args(argv)
    _install_neutral_turn_seams()
    server._sessions[_SID] = _session()
    stdin = _install_gated_control_reader(
        Path(args.ready), Path(args.release), Path(args.reader_finished),
    )
    compute_host.run_host(stdin=stdin, stdout=_WIRE_STDOUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
