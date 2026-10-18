"""Real-child failure injection for the bounded producer watcher tests.

This wrapper delegates to the accepted R3a child fixture, so it still runs the real
ComputeHost/run_host/session pipeline and the real removed-session worker. It only
injects one deterministic failure at the production watcher boundary:
Thread.start for the named watcher, or the watcher's session cleanup call.
"""

from __future__ import annotations

from pathlib import Path
import sys
import threading

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

# The imported child captures the real HostSupervisor stdout before server redirects
# process stdout to stderr. No alternate host or hand-built terminal is introduced.
from tests.tui_gateway import compute_host_producer_gate_child_r3 as real_child

def _remove_option(argv: list[str], option: str) -> tuple[str, str]:
    values = list(argv)
    try:
        index = values.index(option)
        value = values[index + 1]
    except (ValueError, IndexError):
        raise SystemExit(f"failure child requires {option} VALUE") from None
    del values[index:index + 2]
    return value, values


def _mark(path: str) -> None:
    Path(path).write_text("injected", encoding="utf-8")


def _install_failure(kind: str, seen_path: str) -> None:
    if kind == "thread-start":
        original_start = threading.Thread.start

        def fail_watcher_start(thread: threading.Thread, *args, **kwargs):
            if thread.name == "compute-host-exception-worker":
                _mark(seen_path)
                raise RuntimeError("test-owned watcher thread start failure")
            return original_start(thread, *args, **kwargs)

        threading.Thread.start = fail_watcher_start
        return
    if kind == "cleanup":
        original_clear = real_child.server._clear_inflight_turn

        def fail_watcher_cleanup(session: dict, *args, **kwargs):
            if threading.current_thread().name == "compute-host-exception-worker":
                _mark(seen_path)
                raise RuntimeError("test-owned watcher cleanup failure")
            return original_clear(session, *args, **kwargs)

        real_child.server._clear_inflight_turn = fail_watcher_cleanup
        return
    raise SystemExit(f"unknown failure kind: {kind}")


def main(argv: list[str]) -> int:
    kind, remaining = _remove_option(argv, "--failure")
    seen_path, remaining = _remove_option(remaining, "--failure-seen")
    _install_failure(kind, seen_path)
    return real_child._main(remaining)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
