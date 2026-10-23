"""Lifecycle-control boundaries for the gateway shutdown mixin.

Keeping the reversible drain watcher and restart entrypoint in this topical sibling keeps the
large shutdown phase module below its ratcheted file-size cap.  The methods still run on the same
``GatewayRunner`` instance and call the existing shutdown phases through its MRO.
"""
from __future__ import annotations

import asyncio
import logging
from contextvars import Context


logger = logging.getLogger("gateway.run")


# Windows has no bash/setsid chain: a tiny detached Python watcher waits for the gateway PID to
# exit (bounded), then spawns ``hermes gateway restart``.  It is kept with the lifecycle entrypoint
# seam so the large shutdown phase module stays below its ratcheted file-size cap.
WINDOWS_RESTART_WATCHER = """
import os, subprocess, sys, time
from hermes_cli._subprocess_compat import windows_detach_flags_without_breakaway
pid = int(sys.argv[1])
expected_start = int(sys.argv[2])
expected_birth = float(sys.argv[3])
restart_after_s = float(sys.argv[4])
cmd = sys.argv[5:]
deadline = time.monotonic() + restart_after_s

def _same_process(p):
    try:
        from gateway.status import get_process_start_time
        from hermes_cli.process_identity import _process_create_time
        current_start = get_process_start_time(int(p))
        current_birth = _process_create_time(int(p))
        return (
            current_start is not None and current_start == expected_start
            and current_birth is not None and current_birth == expected_birth
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        return False

def _process_alive(p):
    # On Windows, os.kill(pid, 0) is NOT a no-op — it maps to
    # GenerateConsoleCtrlEvent(0, pid) (bpo-14484). Use the
    # Win32 handle-based existence check instead.
    if os.name == 'nt':
        import ctypes
        k32 = ctypes.windll.kernel32
        k32.OpenProcess.restype = ctypes.c_void_p
        k32.WaitForSingleObject.restype = ctypes.c_uint
        k32.GetLastError.restype = ctypes.c_uint
        h = k32.OpenProcess(0x1000 | 0x100000, False, int(p))
        if not h:
            return False
        try:
            return k32.WaitForSingleObject(h, 0) == 0x102
        finally:
            k32.CloseHandle(h)
    try:
        os.kill(int(p), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False

while time.monotonic() < deadline:
    if not _process_alive(pid):
        break
    # A live PID with a different start fingerprint is a recycled process.  Do
    # not launch a restart into an identity we did not capture.
    if not _same_process(pid):
        sys.exit(0)
    time.sleep(0.2)
subprocess.Popen(
    cmd,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    creationflags=windows_detach_flags_without_breakaway(),
)
""".strip()


class GatewayLifecycleMixin:
    """Restart, external-drain and lifecycle-custody entrypoints."""

    def _enter_external_drain(self) -> None:
        """Begin external drain: refuse NEW turns (in-flight ones are NOT interrupted). Idempotent."""
        from gateway.runtime_custody import CustodyRefusal

        if self._external_drain_active:
            _lifecycle_custody = getattr(self, "lifecycle_custody", None)
            _lifecycle = (
                _lifecycle_custody.active_lease("external-drain")
                if _lifecycle_custody is not None else None
            )
            if _lifecycle is not None and _lifecycle.state == "unknown":
                _result = _lifecycle.complete_external_transition()
                if isinstance(_result, CustodyRefusal):
                    logger.warning("External drain lifecycle retry remains unresolved: %s", _result.code)
            return
        from gateway.drain_control import admit_external_drain

        _lifecycle = admit_external_drain(self)
        if isinstance(_lifecycle, CustodyRefusal):
            logger.warning("External drain refused by lifecycle custody: %s", _lifecycle.code)
            return
        self._external_drain_active = True
        logger.info(
            "External drain ENGAGED (.drain_request.json present) — refusing "
            "new turns; %d in-flight turn(s) will finish. Process stays up.", self._active_work_count(),
        )
        # Persist "draining" so /api/status tracks it; active_agents is read-merged, only state changes.
        self._update_runtime_status("draining")
        if _lifecycle is not None:
            _result = _lifecycle.complete_external_transition()
            if isinstance(_result, CustodyRefusal):
                logger.warning("External drain lifecycle receipt remained unresolved: %s", _result.code)

    def _exit_external_drain(self) -> None:
        """Cancel external drain: re-accept new turns. Idempotent; never resurrects a stopping gateway."""
        from gateway.runtime_custody import CustodyRefusal

        if not self._external_drain_active:
            return
        _lifecycle_custody = getattr(self, "lifecycle_custody", None)
        _lifecycle = (
            _lifecycle_custody.active_lease("external-drain")
            if _lifecycle_custody is not None else None
        )
        if _lifecycle is not None:
            _result = _lifecycle.complete_external_transition()
            if isinstance(_result, CustodyRefusal):
                logger.warning("External drain release refused by lifecycle custody: %s", _result.code)
                return
        self._external_drain_active = False
        if self._draining or not self._running:
            logger.info(
                "External drain marker cleared during shutdown — not reverting "
                "to running (shutdown takes precedence)."
            )
            return
        logger.info(
            "External drain RELEASED (.drain_request.json removed) — "
            "re-accepting new turns; gateway_state -> %s.", self._serving_state(),
        )
        self._update_runtime_status(self._serving_state())

    async def _drain_control_watcher(self, interval: float = 1.0) -> None:
        """Poll ``.drain_request.json`` at 1s: present -> enter drain, absent -> exit; a stale epoch = absent."""
        from gateway.drain_control import drain_requested

        while self._running:
            try:
                # Off-thread: a synchronous marker read at 1s cadence can stall 30s+ under host I/O
                # pressure and take every platform heartbeat down.
                if await asyncio.to_thread(drain_requested):
                    self._enter_external_drain()
                    # API and cron work live outside messaging's _running_agents map; refresh the
                    # aggregate while an external caller polls this reversible drain state.
                    self._persist_active_agents()
                else:
                    self._exit_external_drain()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # health: allow BLE001 -- watcher errors keep the marker fail-closed
                logger.debug("Drain-control watcher tick error: %s", exc, exc_info=True)
            await asyncio.sleep(interval)

    def request_restart(self, *, detached: bool = False, via_service: bool = False) -> bool:
        if self._restart_task_started:
            return False
        from gateway.lifecycle_custody import admit_lifecycle
        from gateway.runtime_custody import CustodyRefusal

        _lifecycle = admit_lifecycle(self, "restart", owner_id="gateway:restart")
        if isinstance(_lifecycle, CustodyRefusal):
            logger.warning("Gateway restart refused by lifecycle custody: %s", _lifecycle.code)
            return False
        self._restart_task_started = True
        self._restart_requested = True
        self._restart_detached = detached
        self._restart_via_service = via_service
        # Refuse new turns; keep ``_running`` True so the active turn can still deliver its final response.
        self._draining = True
        from gateway.drain_control import open_restart_wait

        open_restart_wait(self)  # clients may still answer a turn parked on approval/clarify (#139041)
        # The restart's after-turn wait is a drain window too: pollers of GET /v1/runs/{id} must see
        # the boundary from the moment new turns are refused, not only once stop() begins (#115133).
        self._mark_api_runs_shutdown_requested()

        async def _run_restart() -> None:
            await self._await_active_work_before_restart()
            # Detached helper only AFTER the after-turn wait, or its drain_timeout+5 deadline fires mid-turn.
            if detached:
                from gateway.run_shutdown import _log_suppressed

                with _log_suppressed(logging.ERROR, "Failed to launch detached gateway restart helper: %s"):
                    await self._launch_detached_restart_command()
            await asyncio.sleep(0.05)
            await self.stop(restart=True, detached_restart=detached, service_restart=via_service)

        # NOT in _background_tasks: _stop_impl cancels those, which would cancel _run_restart while it
        # awaits _stop_task and skip _shutdown_event.set() / _exit_code = 75 (#12875). We still hold a strong reference in
        # self._restart_task: a bare asyncio.create_task() keeps only a weak reference, so the event loop
        # may garbage-collect a still-pending task mid-flight. The cancel loop in _stop_impl explicitly
        # skips _restart_task for the same reason it skips _stop_task.
        # Empty Context: /restart is handled inside the requester's profile scope, and a copied context
        # would run the HOST restart as that profile (watcher HERMES_HOME, stop()'s flushes).
        self._restart_task = Context().run(lambda: asyncio.create_task(_run_restart()))
        if _lifecycle is not None:
            def _finish_restart(_task):
                _result = _lifecycle.complete()
                if isinstance(_result, CustodyRefusal):
                    logger.warning("Gateway restart lifecycle receipt remained unresolved: %s", _result.code)

            self._restart_task.add_done_callback(_finish_restart)
        return True

