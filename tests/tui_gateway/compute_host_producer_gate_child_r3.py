"""Private child fixture for the producer-completion gate tests.

The parent starts this file through HostSupervisor. The fixture imports the production
ComputeHost and calls production ``compute_host.run_host(stdout=...)`` after capturing
the real wire stream. It only installs the same neutral synthetic-agent seams used
by ``test_compute_host_turn_protocol.py`` so the real server turn pipeline can
finish without a provider, network, or credential.

In descendant mode the wrapper still calls the production ``ComputeHost._run_real_turn``.
It starts one test-owned helper that inherits stdout, waits until the real turn has
emitted its terminal, writes one adversarial matching frame, and stays alive until the
parent releases it. That matching frame is deliberately not the normal producer path.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import types


# This fixture is launched as a script. Put the checkout root on sys.path before
# either the descendant branch or production imports so the bounded wait helper is
# available even when the host process disappears before its normal branch starts.
_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

# Preserve the real HostSupervisor stdout pipe before importing tui_gateway.server;
# that module intentionally redirects process stdout to stderr for its own logging.
_WIRE_STDOUT = sys.stdout

from tests.waiting import wait_until


class _ShutdownAckGate:
    """Hold the real shutdown ACK write at a test-owned barrier."""

    def __init__(self, stream, ready: str, release: str) -> None:
        self._stream = stream
        self._ready = Path(ready)
        self._release = Path(release)
        self._armed = False

    def write(self, data: str) -> int:
        if not self._armed:
            try:
                frame = json.loads(data.strip())
            except (TypeError, json.JSONDecodeError):
                frame = None
            if isinstance(frame, dict) and frame.get("type") == "shutdown.ack":
                self._ready.write_text("ready", encoding="utf-8")
                wait_until(
                    self._release.exists,
                    timeout=30,
                    what="test-owned shutdown ACK release",
                )
                self._armed = True
        return self._stream.write(data)

    def flush(self) -> None:
        self._stream.flush()


def _descendant_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--descendant", action="store_true")
    parser.add_argument("--release", required=True)
    parser.add_argument("--terminal-signal", required=True)
    parser.add_argument("--ready", required=True)
    parser.add_argument("--exit-ack", required=True)
    parser.add_argument("--payload", required=True)
    parser.add_argument("--duplicate-go", default="")
    parser.add_argument("--duplicate-written", default="")
    args = parser.parse_args(argv)
    release = Path(args.release)
    terminal_signal = Path(args.terminal_signal)
    ready = Path(args.ready)
    exit_ack = Path(args.exit_ack)
    payload = json.loads(args.payload)
    try:
        # This helper is outside the host's process group. Its own deadline is the
        # last-resort leak bound if the parent host disappears before test cleanup.
        if args.duplicate_go:
            duplicate_go = Path(args.duplicate_go)
            duplicate_written = Path(args.duplicate_written)
            wait_until(
                lambda: release.exists() or duplicate_go.exists(),
                timeout=30,
                what="duplicate writer release or parent permission",
            )
            if release.exists() and not duplicate_go.exists():
                return 0
            if duplicate_go.exists() and not duplicate_written.exists():
                try:
                    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
                    sys.stdout.flush()
                except Exception:
                    pass
                duplicate_written.write_text("written", encoding="utf-8")
        else:
            wait_until(
                lambda: release.exists() or terminal_signal.exists(),
                timeout=30,
                what="descendant release or real terminal signal",
            )
            if release.exists() and not terminal_signal.exists():
                return 0
            if not release.exists() and not ready.exists():
                try:
                    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
                    sys.stdout.flush()
                except Exception:
                    pass
                ready.write_text("ready", encoding="utf-8")
        wait_until(
            release.exists,
            timeout=30,
            what="descendant release marker",
        )
        return 0
    finally:
        # Always leave a finite cleanup receipt, including when either wait deadline
        # expires because the parent has vanished.
        exit_ack.write_text("exiting", encoding="utf-8")


if "--descendant" in sys.argv:
    raise SystemExit(_descendant_main(sys.argv[1:]))

from tui_gateway import compute_host, server, session_lifecycle
from tui_gateway.compute_host import ComputeHost

_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


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
        "profile_home": "",
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
        # Let the real child admission path claim and release its temporary slot;
        # an inert object would make teardown call a nonexistent release method.
        "active_session_lease": None,
    }


def _install_neutral_turn_seams() -> None:
    """Mirror the neutral seams from test_compute_host_turn_protocol.py."""
    server._wire_callbacks = lambda _sid: None
    server._sync_agent_model_with_config = lambda _sid, _session: None
    server._session_cwd = lambda session: str(session.get("cwd") or _REPO_ROOT)
    server._register_session_cwd = lambda _session: None
    server._tts_stream_begin = lambda: None
    server._sync_session_key_after_compress = lambda *args, **kwargs: None
    server._get_usage = lambda _agent: {}


def _terminal_payload(host: ComputeHost, frame: dict) -> dict:
    return {
        "type": "turn.end",
        "sid": str(frame.get("sid") or ""),
        "request_id": str(frame.get("request_id") or ""),
        "turn_id": str(frame.get("turn_id") or frame.get("request_id") or ""),
        "session_key": _SESSION_KEY,
        "profile_home": str(frame.get("profile_home") or ""),
        "host_boot_id": host._boot_id,
        "origin_session_key": _SESSION_KEY,
    }


def _install_early_terminal(paths: argparse.Namespace) -> None:
    original = ComputeHost._run_real_turn

    def run_with_early_terminal(self: ComputeHost, frame: dict) -> None:
        self.emit(_terminal_payload(self, frame))
        Path(paths.early_written).write_text("early", encoding="utf-8")
        wait_until(
            lambda: Path(paths.early_go).exists(),
            timeout=30,
            what="parent permission after early terminal",
        )
        original(self, frame)

    ComputeHost._run_real_turn = run_with_early_terminal


def _install_retry_completion(paths: argparse.Namespace) -> None:
    original = ComputeHost._run_real_turn
    original_emit = ComputeHost.emit
    last_terminal: dict | None = None

    def capture_terminal(self: ComputeHost, frame: dict) -> None:
        nonlocal last_terminal
        if frame.get("type") in {"turn.end", "turn.error"}:
            last_terminal = dict(frame)
        original_emit(self, frame)

    ComputeHost.emit = capture_terminal

    def run_with_retry_completion(self: ComputeHost, frame: dict) -> None:
        original(self, frame)
        Path(paths.real_terminal_returned).write_text("production-returned", encoding="utf-8")
        wait_until(
            lambda: Path(paths.retry_go).exists(),
            timeout=30,
            what="parent retry permission",
        )
        assert last_terminal is not None
        self._emit_producer_completion(dict(last_terminal))

    ComputeHost._run_real_turn = run_with_retry_completion


def _install_inherited_writer(paths: argparse.Namespace) -> None:
    original = ComputeHost._run_real_turn

    def run_with_inherited_writer(self: ComputeHost, frame: dict) -> None:
        payload = _terminal_payload(self, frame)
        descendant_args = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--descendant",
            "--release",
            paths.descendant_release,
            "--terminal-signal",
            paths.real_terminal_returned,
            "--ready",
            paths.descendant_ready,
            "--exit-ack",
            paths.descendant_exit_ack,
            "--payload",
            json.dumps(payload, separators=(",", ":")),
        ]
        if paths.duplicate_go:
            descendant_args.extend(("--duplicate-go", paths.duplicate_go,
                                    "--duplicate-written", paths.duplicate_written))
        descendant = subprocess.Popen(
            descendant_args,
            stdin=subprocess.DEVNULL,
            # stdout is intentionally inherited from the real compute host. This is
            # the physical descendant-writer counterexample under test.
            stderr=subprocess.DEVNULL,
        )

        def reap_descendant() -> None:
            try:
                returncode = descendant.wait(timeout=75)
            except subprocess.TimeoutExpired:
                Path(paths.descendant_reap_error).write_text(
                    "timeout", encoding="utf-8"
                )
            except Exception as exc:
                Path(paths.descendant_reap_error).write_text(
                    f"{type(exc).__name__}: {exc}", encoding="utf-8"
                )
            else:
                Path(paths.descendant_reaped).write_text(
                    str(returncode), encoding="utf-8"
                )

        # Keep this non-daemon: the real host must retain the exact Popen owner
        # until it has reaped the inherited writer or recorded a bounded failure.
        threading.Thread(
            target=reap_descendant,
            name="compute-owner-descendant-reaper",
            daemon=False,
        ).start()
        Path(paths.descendant_pid).write_text(str(descendant.pid), encoding="utf-8")
        # The production method emits turn.end before returning. Only then may the
        # inherited writer emit its matching frame.
        original(self, frame)
        Path(paths.real_terminal_returned).write_text("production-returned", encoding="utf-8")

    ComputeHost._run_real_turn = run_with_inherited_writer


def _install_held_exception(paths: argparse.Namespace) -> None:
    """Leave a real producer worker alive so the bounded exception path fail-closes."""
    def run_with_held_exception(*args, **_kwargs) -> None:
        session = args[2]
        release = Path(paths.worker_release)

        def wait_for_release() -> None:
            wait_until(
                release.exists,
                timeout=30,
                what="held producer worker release",
            )

        worker = threading.Thread(
            target=wait_for_release,
            name="compute-owner-held-producer",
            daemon=False,
        )
        worker.start()
        session["_run_thread"] = worker
        raise RuntimeError("held producer test failure")

    server._run_prompt_submit = run_with_held_exception


def _install_removed_session_held_exception(paths: argparse.Namespace) -> None:
    """Pop the accepted session through the real lifecycle while its worker is live."""
    def run_with_removed_session(*args, **_kwargs) -> None:
        session = args[2]
        release = Path(paths.worker_release)

        def wait_for_release() -> None:
            try:
                wait_until(
                    release.exists,
                    timeout=30,
                    what="removed-session worker release",
                )
            finally:
                Path(paths.worker_finished).write_text("finished", encoding="utf-8")

        worker = threading.Thread(
            target=wait_for_release,
            name="compute-owner-removed-session-worker",
            daemon=False,
        )
        session["_run_thread"] = worker
        worker.start()
        Path(paths.worker_started).write_text("started", encoding="utf-8")
        popped = server._pop_session_by_id(_SID)
        Path(paths.session_removed).write_text(
            "removed" if popped is session else "unexpected", encoding="utf-8"
        )
        raise RuntimeError("removed session while producer worker is live")

    server._run_prompt_submit = run_with_removed_session


def _configure_normal(_args: argparse.Namespace) -> None:
    return None


def _configure_early(args: argparse.Namespace) -> None:
    if not args.early_written or not args.early_go:
        raise SystemExit("early mode requires --early-written and --early-go")
    _install_early_terminal(args)


def _configure_retry(args: argparse.Namespace) -> None:
    if not args.retry_go or not args.real_terminal_returned:
        raise SystemExit("retry mode requires --retry-go and --real-terminal-returned")
    _install_retry_completion(args)


def _configure_late(args: argparse.Namespace) -> None:
    required = (
        args.descendant_release,
        args.descendant_ready,
        args.descendant_exit_ack,
        args.descendant_pid,
        args.descendant_reaped,
        args.descendant_reap_error,
        args.real_terminal_returned,
    )
    if not all(required):
        raise SystemExit("late mode requires all synchronization paths")
    _install_inherited_writer(args)


def _configure_held_exception(args: argparse.Namespace) -> None:
    if not args.worker_release:
        raise SystemExit("held-exception mode requires --worker-release")
    _install_held_exception(args)


def _configure_removed_session_held_exception(args: argparse.Namespace) -> None:
    required = (args.worker_release, args.worker_started, args.worker_finished, args.session_removed)
    if not all(required):
        raise SystemExit(
            "removed-session-held-exception mode requires worker and session synchronization paths"
        )
    _install_removed_session_held_exception(args)


_MODE_CONFIGURERS = {
    "normal": _configure_normal,
    "early": _configure_early,
    "late": _configure_late,
    "retry": _configure_retry,
    "held-exception": _configure_held_exception,
    "removed-session-held-exception": _configure_removed_session_held_exception,
}


def _configure_mode(args: argparse.Namespace) -> None:
    _MODE_CONFIGURERS[args.mode](args)


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=(
            "normal", "early", "late", "retry", "held-exception",
            "removed-session-held-exception",
        ), required=True)
    parser.add_argument("--worker-release", default="")
    parser.add_argument("--worker-started", default="")
    parser.add_argument("--worker-finished", default="")
    parser.add_argument("--session-removed", default="")
    parser.add_argument("--descendant-release", default="")
    parser.add_argument("--descendant-ready", default="")
    parser.add_argument("--descendant-exit-ack", default="")
    parser.add_argument("--descendant-pid", default="")
    parser.add_argument("--descendant-reaped", default="")
    parser.add_argument("--descendant-reap-error", default="")
    parser.add_argument("--real-terminal-returned", default="")
    parser.add_argument("--early-written", default="")
    parser.add_argument("--early-go", default="")
    parser.add_argument("--retry-go", default="")
    parser.add_argument("--duplicate-go", default="")
    parser.add_argument("--duplicate-written", default="")
    parser.add_argument("--shutdown-ack-ready", default="")
    parser.add_argument("--shutdown-ack-release", default="")
    args = parser.parse_args(argv)

    _install_neutral_turn_seams()
    server._sessions[_SID] = _session()
    _configure_mode(args)
    if bool(args.shutdown_ack_ready) != bool(args.shutdown_ack_release):
        raise SystemExit("shutdown ACK gate requires both synchronization paths")
    stdout = _WIRE_STDOUT
    if args.shutdown_ack_ready:
        stdout = _ShutdownAckGate(
            _WIRE_STDOUT, args.shutdown_ack_ready, args.shutdown_ack_release,
        )
    os.environ["HERMES_COMPUTE_HOST_CHILD"] = "1"
    # main() only parses argv then calls run_host() without an explicit stream. Pass
    # the captured pipe explicitly so server's logging redirect cannot move the wire.
    compute_host.run_host(stdout=stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
