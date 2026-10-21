"""Topical startup/adoption lifecycle for the WhatsApp bridge adapter."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
import subprocess
import threading


class _BridgeLogOpenState:
    """Own one synchronous log-open worker and any handle it produces."""

    def __init__(self):
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._started = False
        self._closed = False
        self._handle = None

    def start(self) -> bool:
        with self._lock:
            if self._closed:
                self._done.set()
                return False
            self._started = True
            return True

    def bind(self, handle) -> bool:
        with self._lock:
            if not self._closed:
                self._handle = handle
                return True
        with suppress(Exception):
            handle.close()
        return False

    def request_close(self) -> None:
        with self._lock:
            self._closed = True
            handle = self._handle
            self._handle = None
            if not self._started:
                self._done.set()
        if handle is not None:
            with suppress(Exception):
                handle.close()

    def take(self, expected):
        with self._lock:
            if self._closed or self._handle is not expected:
                return None
            self._handle = None
            return expected

    def mark_done(self) -> None:
        self._done.set()

    def done(self) -> bool:
        return self._done.is_set()


def _open_bridge_log_file(path: Path):
    """Open one bridge log handle in the tracked worker's synchronous boundary."""
    return open(path, "a", encoding="utf-8")


def _open_bridge_log_file_owned(path: Path, state: _BridgeLogOpenState):
    """Close a late file in the worker when its asyncio wrapper is already gone."""
    if not state.start():
        return None
    try:
        handle = _open_bridge_log_file(path)
        if state.bind(handle):
            return handle
        return None
    finally:
        state.mark_done()


def _raise_bridge_log_cancellation(opening, shield_error, prior_cancellation):
    """Preserve the caller cancellation or recover the reason from the inner task."""
    if prior_cancellation is not None:
        raise prior_cancellation from shield_error
    try:
        opening.result()
    except asyncio.CancelledError as inner_cancellation:
        raise inner_cancellation from shield_error
    raise shield_error


async def _await_bridge_log_open(opening, state):
    """Drain one shielded open while retaining cancellation ownership."""
    cancellation = None
    while True:
        try:
            handle = await asyncio.shield(opening)
        except asyncio.CancelledError as exc:
            state.request_close()
            if opening.cancelled():
                _raise_bridge_log_cancellation(opening, exc, cancellation)
            cancellation = cancellation or exc
            continue
        except BaseException as worker_exc:
            if cancellation is not None:
                raise cancellation from worker_exc
            raise
        else:
            return handle, cancellation


class WhatsAppBridgeConnectMixin:
    async def _prepare_bridge_port(self, *, secondary: bool, prior_bridge_is_ours: bool) -> bool:
        """Reap only an authenticated stale bridge and fail closed on an unknown holder."""
        from . import adapter as _adapter

        logger = _adapter.logger
        if not secondary or prior_bridge_is_ours:
            _adapter._kill_stale_bridge_by_pidfile(self._session_path)
        if not secondary:
            if self._bridge_probe_timed_out:
                # No /health answer is no ownership evidence (another profile's busy bridge looks the same), and
                # our own stale bridge was just reaped by its pidfile: a holder that remains is left running.
                await asyncio.sleep(1)
                try:
                    held = bool(_adapter._port_listener_pids(self._bridge_port))
                except (OSError, subprocess.SubprocessError, ValueError) as exc:
                    logger.warning(
                        "[%s] Could not inspect bridge listeners; retaining fail-closed port hold: %s",
                        self.name, exc, exc_info=True,
                    )
                    held = True  # listeners could not be listed: still no evidence to kill on
                if held:
                    self._set_fatal_error(
                        "whatsapp_bridge_unresponsive",
                        f"Port {self._bridge_port} is held by a process that did not answer /health in time, so "
                        f"Hermes cannot tell whose WhatsApp bridge it is; nothing on port {self._bridge_port} was "
                        "stopped. Stop that process, or give this profile its own port via "
                        "platforms.whatsapp.extra.bridge_port.",
                        retryable=True,
                    )
                    return False
            _adapter._kill_port_process(self._bridge_port)
        if not secondary or prior_bridge_is_ours:
            await asyncio.sleep(1)
        return True

    def _bridge_start_mode(self) -> tuple[bool, bool] | None:
        secondary = bool(getattr(self, "_runtime_status_platform_key", ""))
        prior_bridge_is_ours = False
        if not secondary:
            return secondary, prior_bridge_is_ours
        from .bridge_ownership import secondary_bridge_port, check_secondary_ownership
        try:
            self._bridge_port = secondary_bridge_port(self._profile_home, self.config.extra.get("bridge_port"))
            prior_bridge_is_ours = check_secondary_ownership(self._session_path, self._bridge_port) == "ours"
        except (OSError, ValueError) as exc:
            self._set_fatal_error(
                "whatsapp_bridge_conflict",
                f"{exc}. Set platforms.whatsapp.extra.bridge_port to a distinct free port; "
                "or stop the process holding it.", retryable=False)
            return None
        return secondary, prior_bridge_is_ours

    def _close_late_bridge_log(self, handle) -> None:
        if handle is getattr(self, "_bridge_log_fh", None):
            self._bridge_log_fh = None
        with suppress(Exception):
            handle.close()

    def _bridge_log_open_finished(self, task) -> None:
        state = getattr(self, "_bridge_log_open_state", None)
        if state is None:
            return
        if task.cancelled():
            state.request_close()
            return
        # This callback receives our completed asyncio.Task. The awaiting
        # connect path owns its original error; inspecting it does not swallow it.
        if task.exception() is not None:
            state.request_close()
            return
        if (
            getattr(self, "_bridge_log_open_close_requested", False)
            or getattr(self, "_bridge_log_open_task", None) is not task
        ):
            state.request_close()

    def _request_bridge_log_open_close(self) -> None:
        self._bridge_log_open_close_requested = True
        state = getattr(self, "_bridge_log_open_state", None)
        if state is not None:
            state.request_close()
        task = getattr(self, "_bridge_log_open_task", None)
        if task is not None and task.done():
            self._bridge_log_open_finished(task)

    async def _open_bridge_log(self, path: Path):
        active = getattr(self, "_bridge_log_open_task", None)
        if active is not None and not active.done():
            raise RuntimeError("WhatsApp bridge log open is already in progress")
        prior_state = getattr(self, "_bridge_log_open_state", None)
        if prior_state is not None:
            if not prior_state.done():
                raise RuntimeError("WhatsApp bridge log worker is still finishing")
            self._bridge_log_open_state = None
        self._bridge_log_open_close_requested = False
        state = _BridgeLogOpenState()
        self._bridge_log_open_state = state
        opening = asyncio.create_task(
            asyncio.to_thread(_open_bridge_log_file_owned, path, state),
            name="whatsapp-bridge-log-open",
        )
        self._bridge_log_open_task = opening
        opening.add_done_callback(self._bridge_log_open_finished)
        published = False
        try:
            handle, cancellation = await _await_bridge_log_open(opening, state)
            close_requested = (
                getattr(self, "_bridge_log_open_close_requested", False)
                or getattr(self, "_shutting_down", False)
            )
            if handle is None or cancellation is not None or close_requested:
                self._bridge_log_open_close_requested = True
                state.request_close()
                if cancellation is not None:
                    raise cancellation
                raise RuntimeError("WhatsApp bridge log closed before publication")
            owned = state.take(handle)
            if owned is None:
                state.request_close()
                self._close_late_bridge_log(handle)
                raise RuntimeError("WhatsApp bridge log ownership was lost before publication")
            if getattr(self, "_bridge_log_fh", None) is not owned:
                self._bridge_log_fh = owned
            self._bridge_log_open_state = None
            published = True
            return owned
        finally:
            if not published:
                state.request_close()
                if state.done() and getattr(self, "_bridge_log_open_state", None) is state:
                    self._bridge_log_open_state = None
            if getattr(self, "_bridge_log_open_task", None) is opening:
                self._bridge_log_open_task = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Start (or adopt) the Node.js bridge and wait for it to be ready."""
        from . import adapter as _adapter

        # Keep the adapter facade lookup: existing callers/tests replace this symbol
        # on ``adapter`` to control the selected Hermes Node installation.
        find_node_executable = _adapter.find_node_executable
        _wenv = _adapter._wenv
        _write_bridge_pidfile = _adapter._write_bridge_pidfile
        logger = _adapter.logger
        from hermes_cli._subprocess_compat import windows_detach_popen_kwargs
        if find_node_executable("node") is None:
            import pm

            try:
                await asyncio.to_thread(pm.ensure, "node")
            except pm.InstallError as exc:
                self._set_fatal_error("whatsapp_node_missing", str(exc), retryable=False)
                return False
        mode = self._bridge_start_mode()
        if mode is None:
            return False
        secondary, prior_bridge_is_ours = mode
        if not await asyncio.to_thread(self._preflight):
            return False
        bridge_path = Path(self._bridge_script)
        lock_acquired = False
        try:
            if not self._acquire_platform_lock('whatsapp-session', str(self._session_path), 'WhatsApp session'):
                return False
            lock_acquired = True
        except (OSError, RuntimeError, ValueError) as exc:
            # A lock probe can be unavailable on a damaged/shared profile. Keep the
            # historical non-fatal path, but retain a traceback and do not hide
            # programming errors behind a blanket catch.
            logger.warning(
                "[%s] Could not acquire session lock (non-fatal): %s",
                self.name, exc, exc_info=True,
            )
        try:
            if not self._ensure_bridge_deps(bridge_path.parent):
                return False
            self._session_path.mkdir(parents=True, exist_ok=True)
            # A secondary adopts or reaps only a bridge its own pidfile identifies (crash restart);
            # the default keeps its historical adopt-or-clear-the-port path.
            if (not secondary or prior_bridge_is_ours) and await self._reuse_running_bridge(bridge_path):
                return True
            if self._foreign_bridge_session:
                # The port is served by another profile's bridge. Never adopt
                # it and never kill it: it belongs to a live session.
                foreign = self._foreign_bridge_session
                self._set_fatal_error(
                    "whatsapp_bridge_foreign_session",
                    f"Port {self._bridge_port} is already served by a WhatsApp bridge for a different session "
                    f"({foreign}). Give this profile its own port via platforms.whatsapp.extra.bridge_port "
                    f"(one distinct bridge_port per profile); nothing on port {self._bridge_port} was stopped.",
                    retryable=False,
                )
                return False
            if not await self._prepare_bridge_port(secondary=secondary, prior_bridge_is_ours=prior_bridge_is_ours):
                return False
            # Bridge output goes to a log file so QR codes, errors, and reconnection messages survive for troubleshooting.
            self._bridge_log = self._session_path.parent / "bridge.log"
            bridge_log_fh = await self._open_bridge_log(self._bridge_log)
            node = find_node_executable("node")
            if node is None:
                raise RuntimeError("Node.js is no longer available; run `hermes pm install`")
            self._bridge_process = subprocess.Popen(  # noqa: ASYNC220 -- persistent bridge; callers use the Popen object (pid, poll)
                [node, str(bridge_path), "--port", str(self._bridge_port), "--session", str(self._session_path),
                 "--mode", _wenv("WHATSAPP_MODE", "self-chat"), "--managed"], stdin=subprocess.PIPE, stdout=bridge_log_fh, stderr=bridge_log_fh, env=self._bridge_env(), **windows_detach_popen_kwargs())
            authority, startup_mode = self._send_bridge_bootstrap(self._bridge_process)
            _write_bridge_pidfile(self._session_path, self._bridge_process.pid, self._bridge_port)
            if not await self._wait_for_bridge():
                return False
            self._attach_to_bridge(self._bridge_process, start_polling=False)
            if not await self._prepare_spawned_bridge_startup(authority, startup_mode):
                self._set_fatal_error(
                    "whatsapp_bridge_adoption_failed",
                    "WhatsApp bridge owner adoption could not be authenticated; no queue polling started.",
                    retryable=True,
                )
                shutting_down = self._shutting_down
                try:
                    await self.disconnect()
                finally:
                    self._shutting_down = shutting_down
                return False
            self._mark_connected()
            self._start_bridge_polling()
            print(f"[{self.name}] Bridge started on port {self._bridge_port}")
            self._wire_plugin_handlers(None)
            return True
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            logger.error("[%s] Failed to start bridge: %s", self.name, exc, exc_info=True)
            return False
        finally:
            if not self._running:
                if lock_acquired:
                    self._release_platform_lock()
                self._close_bridge_log()
