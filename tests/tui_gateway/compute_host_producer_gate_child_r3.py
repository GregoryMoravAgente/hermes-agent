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
_WIRE_STDIN = getattr(sys.stdin, "buffer", sys.stdin)

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


class _FinishedInputProxy:
    """Fixture-only fallback for source revisions that iterate stdin directly."""

    def __init__(self, stream, finished: Path) -> None:
        self._stream = stream
        self._finished = finished

    def fileno(self):
        return self._stream.fileno()

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __iter__(self):
        try:
            yield from self._stream
        finally:
            self._finished.write_text("reader-finished", encoding="utf-8")


def _install_reader_finished_marker(finished: str, stream):
    """Mark fixture cleanup while delegating the real production pipe reader."""
    original_reader = getattr(compute_host, "_iter_control_lines", None)
    if not callable(original_reader):
        # Older source iterates stdin directly; delegate to that real stream
        # through a fixture-only proxy and do not add a production hook.
        return _FinishedInputProxy(stream, Path(finished))

    def marked_reader(stream, stop):
        try:
            yield from original_reader(stream, stop)
        finally:
            Path(finished).write_text("reader-finished", encoding="utf-8")

    compute_host._iter_control_lines = marked_reader
    return stream


def _install_shutdown_boundary_probe(args: argparse.Namespace) -> None:
    """Observe real run_host ownership while the production drain is gated."""
    required = (
        args.shutdown_drain_ready, args.shutdown_drain_release,
        args.shutdown_explicit_drain_returned, args.shutdown_fallback_entered,
        args.shutdown_fallback_shutdown, args.shutdown_unregister,
        args.shutdown_join_returned, args.shutdown_join_release,
    )
    if not all(required):
        raise SystemExit(
            "shutdown-boundary mode requires all synchronization paths"
        )

    original_drain = ComputeHost._drain_and_flush
    drain_lock = threading.Lock()
    drain_calls = 0

    def gated_drain(self: ComputeHost, *drain_args, **drain_kwargs):
        nonlocal drain_calls
        with drain_lock:
            drain_calls += 1
            call_number = drain_calls
        if call_number == 1:
            Path(args.shutdown_drain_ready).write_text("entered", encoding="utf-8")
            wait_until(
                lambda: Path(args.shutdown_drain_release).exists(),
                timeout=30,
                what="test-owned explicit drain release",
            )
            result = original_drain(self, *drain_args, **drain_kwargs)
            Path(args.shutdown_explicit_drain_returned).write_text(
                "returned", encoding="utf-8"
            )
            return result
        Path(args.shutdown_fallback_entered).write_text(
            json.dumps({"thread": threading.current_thread().name}),
            encoding="utf-8",
        )
        return original_drain(self, *drain_args, **drain_kwargs)

    ComputeHost._drain_and_flush = gated_drain

    original_shutdown = ComputeHost.shutdown

    def observed_shutdown(self: ComputeHost, *shutdown_args, **shutdown_kwargs):
        Path(args.shutdown_fallback_shutdown).write_text(
            json.dumps({"thread": threading.current_thread().name}),
            encoding="utf-8",
        )
        return original_shutdown(self, *shutdown_args, **shutdown_kwargs)

    ComputeHost.shutdown = observed_shutdown

    original_unregister = server.unregister_live_transport

    def observed_unregister(transport):
        Path(args.shutdown_unregister).write_text("unregistered", encoding="utf-8")
        return original_unregister(transport)

    server.unregister_live_transport = observed_unregister

    original_join = threading.Thread.join

    def observed_join(thread, timeout=None):
        if (
            thread.name == "compute-host-lifecycle-writer"
            and timeout is None
            and args.shutdown_writer_join_entered
        ):
            # This is emitted at the real deferred finalizer's writer.join(None)
            # entry, before that join can be released by the ACK gate.
            Path(args.shutdown_writer_join_entered).write_text(
                json.dumps({"thread": threading.current_thread().name, "timeout": timeout}),
                encoding="utf-8",
            )
        result = original_join(thread, timeout)
        if (
            thread.name == "compute-host-control-reader"
            and timeout is None
            and args.shutdown_reader_settled
        ):
            # This follows the exact reader.join(None) used by the deferred
            # finalizer, rather than the fixture's generator-finally marker.
            Path(args.shutdown_reader_settled).write_text(
                json.dumps({"thread": threading.current_thread().name, "joined": True}),
                encoding="utf-8",
            )
        if thread.name == "compute-host-control-reader" and timeout == 2.0:
            Path(args.shutdown_join_returned).write_text(
                json.dumps({"alive": thread.is_alive(), "timeout": timeout}),
                encoding="utf-8",
            )
            if args.shutdown_reader_finishes_first:
                # Use the saved original join directly so this control proves the
                # reader really died while the ACK writer remained gated. The
                # existing join-release barrier stays after that real thread exit.
                original_join(thread)
                if args.shutdown_reader_fully_returned:
                    Path(args.shutdown_reader_fully_returned).write_text(
                        json.dumps({
                            "thread": threading.current_thread().name,
                            "alive": thread.is_alive(),
                            "joined": True,
                        }),
                        encoding="utf-8",
                    )
            wait_until(
                lambda: Path(args.shutdown_join_release).exists(),
                timeout=30,
                what="test-owned reader-join boundary release",
            )
        return result

    threading.Thread.join = observed_join


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


def _install_completion_backpressure(paths: argparse.Namespace) -> None:
    """Fill the real inherited completion pipe during the producer write."""
    original_write = compute_host._write_completion_bytes
    write_count = 0

    def blocked_write(fd: int, data: bytes, **kwargs) -> None:
        nonlocal write_count
        write_count += 1
        # Startup is the first record. Fill only the actual turn completion write,
        # after the parent has authenticated startup and intentionally paused its
        # completion reader. Every padding unit is the exact source-bound completion
        # record, newline framed and within the production record budget; no malformed
        # partial line can swallow the later real record.
        if write_count == 2:
            os.set_blocking(fd, False)
            padding_count = 0
            try:
                while True:
                    try:
                        written = os.write(fd, data)
                    except BlockingIOError:
                        break
                    if written != len(data):
                        raise OSError("completion padding made a short framed write")
                    padding_count += 1
            finally:
                os.set_blocking(fd, True)
            Path(paths.completion_filled).write_text(str(padding_count), encoding="utf-8")
            wait_until(
                lambda: Path(paths.completion_release).exists(),
                timeout=30,
                what="test-owned completion pipe release",
            )
        return original_write(fd, data, **kwargs)

    compute_host._write_completion_bytes = blocked_write
    original_close = ComputeHost.close

    def observed_close(self: ComputeHost, *args, **kwargs) -> bool:
        result = original_close(self, *args, **kwargs)
        result_path = Path(paths.close_result)
        if not result_path.exists():
            result_path.write_text(
                json.dumps({
                    "result": bool(result),
                    "completion_closed": self._completion_closed.is_set(),
                    "completion_fd_present": self._completion_fd is not None,
                }, separators=(",", ":")),
                encoding="utf-8",
            )
        return result

    ComputeHost.close = observed_close
    original_shutdown = ComputeHost.shutdown

    def bounded_shutdown(self: ComputeHost, *args, **kwargs) -> None:
        return original_shutdown(
            self, *args, **{**kwargs, "wait": float(paths.shutdown_wait)},
        )

    ComputeHost.shutdown = bounded_shutdown


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


def _configure_completion_backpressure(args: argparse.Namespace) -> None:
    required = (args.completion_filled, args.completion_release, args.close_result)
    if not all(required):
        raise SystemExit(
            "completion-backpressure mode requires fill, release, and close-result paths"
        )
    _install_completion_backpressure(args)


_MODE_CONFIGURERS = {
    "normal": _configure_normal,
    "early": _configure_early,
    "late": _configure_late,
    "retry": _configure_retry,
    "held-exception": _configure_held_exception,
    "removed-session-held-exception": _configure_removed_session_held_exception,
    "completion-backpressure": _configure_completion_backpressure,
}


def _configure_mode(args: argparse.Namespace) -> None:
    _MODE_CONFIGURERS[args.mode](args)


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=(
            "normal", "early", "late", "retry", "held-exception",
            "removed-session-held-exception", "completion-backpressure",
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
    parser.add_argument("--shutdown-drain-ready", default="")
    parser.add_argument("--shutdown-drain-release", default="")
    parser.add_argument("--shutdown-explicit-drain-returned", default="")
    parser.add_argument("--shutdown-fallback-entered", default="")
    parser.add_argument("--shutdown-fallback-shutdown", default="")
    parser.add_argument("--shutdown-unregister", default="")
    parser.add_argument("--shutdown-join-returned", default="")
    parser.add_argument("--shutdown-join-release", default="")
    parser.add_argument("--shutdown-reader-settled", default="")
    parser.add_argument("--shutdown-writer-join-entered", default="")
    parser.add_argument("--shutdown-reader-finishes-first", action="store_true")
    parser.add_argument("--shutdown-reader-fully-returned", default="")
    parser.add_argument("--completion-filled", default="")
    parser.add_argument("--reader-finished", default="")
    parser.add_argument("--completion-release", default="")
    parser.add_argument("--close-result", default="")
    parser.add_argument("--shutdown-wait", type=float, default=0.25)
    args = parser.parse_args(argv)

    _install_neutral_turn_seams()
    server._sessions[_SID] = _session()
    _configure_mode(args)
    if args.shutdown_drain_ready:
        _install_shutdown_boundary_probe(args)
    if bool(args.shutdown_ack_ready) != bool(args.shutdown_ack_release):
        raise SystemExit("shutdown ACK gate requires both synchronization paths")
    stdout = _WIRE_STDOUT
    if args.shutdown_ack_ready:
        stdout = _ShutdownAckGate(
            _WIRE_STDOUT, args.shutdown_ack_ready, args.shutdown_ack_release,
        )
    os.environ["HERMES_COMPUTE_HOST_CHILD"] = "1"
    # main() only parses argv then calls run_host() without an explicit stream. Pass
    # the captured pipes explicitly so server's logging redirect cannot move the wire.
    stdin = _WIRE_STDIN
    if args.reader_finished:
        stdin = _install_reader_finished_marker(args.reader_finished, stdin)
    compute_host.run_host(stdin=stdin, stdout=stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
