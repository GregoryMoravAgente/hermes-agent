"""Recoverable per-path SessionDB handle caches for the gateway."""

from __future__ import annotations

import contextlib
import threading
import time
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

_INITIAL_RETRY_DELAY_SECONDS = 1.0
_MAX_RETRY_DELAY_SECONDS = 60.0


@dataclass
class _Unavailable:
    failures: int = 0
    next_retry_at: float = 0.0
    in_flight: bool = False
    error: str = ""


class _HealthSource:
    """Weak-keyable identity for one cache's health entries."""


_health_lock = threading.Lock()
_health_states: weakref.WeakKeyDictionary[_HealthSource, dict[Path, str]]
_health_states = weakref.WeakKeyDictionary()


def _publish_health(source: _HealthSource, path: Path, state: str) -> None:
    """Publish one privacy-safe aggregate (no paths, no errors) across all live caches."""
    with _health_lock:
        _health_states.setdefault(source, {})[path] = state
        all_states = {value for item in _health_states.values() for value in item.values()}
        aggregate = next((s for s in ("retrying", "unavailable") if s in all_states), "ok")
    try:
        from gateway.status import publish_runtime_status
        publish_runtime_status(session_store={"status": aggregate})
    except Exception:
        pass  # Runtime health is diagnostic only; persistence must not depend on it.


def _registry_owned(handle: Any) -> bool:
    """True while the process-wide registry owns *handle*; an ``AsyncSessionDB`` (the only handle
    type with a ``_db``) is judged by the SessionDB it wraps. ``close_all`` / ``close_all_under``
    clear the flag when they tear the generation down."""
    inner = getattr(handle, "_db", handle)
    return getattr(inner, "_shared_registry_owned", None) is True


class RecoverableHandleCache:
    """Cache handles by path while allowing failed opens to heal in-process.

    Opens run OUTSIDE ``lock`` (single-flight per path via ``in_flight``); ``close_all`` bumps
    ``_generation`` so a later-completing open is rejected instead of resurrecting the cache.
    """

    def __init__(
        self, *, handles: dict[Path, Any] | None = None, lock: threading.Lock | None = None,
        clock: Callable[[], float] = time.monotonic,
        initial_retry_delay: float = _INITIAL_RETRY_DELAY_SECONDS,
        max_retry_delay: float = _MAX_RETRY_DELAY_SECONDS,
    ) -> None:
        self.handles = handles if handles is not None else {}
        self.lock = lock if lock is not None else threading.Lock()
        self._clock = clock
        self._initial_retry_delay = max(0.0, float(initial_retry_delay))
        self._max_retry_delay = max(self._initial_retry_delay, float(max_retry_delay))
        self._unavailable: dict[Path, _Unavailable] = {}
        # Paths whose cached handle the registry owned when it was cached: once the registry tears
        # that generation down (profile unserve/delete), the entry is dead and must be reopened.
        self._registry_backed: set[Path] = set()
        self._health_source = _HealthSource()
        self._generation = 0
        self._close_rejected: Callable[[Any], None] | None = None
        self._close_rejected_checked = False
        # Openers admitted before a checked close remain in flight after the cache generation is
        # bumped.  Keep their exact identity until the stale result is closed, so a receipt cannot
        # report an empty sweep while a physical handle is still being constructed.
        self._closing_inflight: dict[
            tuple[int, int], tuple[Path, _Unavailable, Callable[[Any], Any], bool]
        ] = {}
        # A stale opener can itself fail its close callback.  Keep that exact handle for the next
        # checked retry without overwriting a newer cached handle for the same path.
        self._failed_handles: dict[int, tuple[Path, Any]] = {}

    def _is_stale(self, path: Path, unavailable: _Unavailable, generation: int) -> bool:
        """Caller holds ``lock``: True when close_all ran or the slot was replaced mid-open."""
        return generation != self._generation or self._unavailable.get(path) is not unavailable

    def _settle_closing_inflight(self, unavailable: _Unavailable, generation: int) -> bool:
        """Forget one opener counted by a checked close once its stale result settles."""
        with self.lock:
            return self._closing_inflight.pop((id(unavailable), generation), None) is not None

    def _retain_failed_handle(self, path: Path, handle: Any) -> None:
        with self.lock:
            self._failed_handles[id(handle)] = (path, handle)

    def _finish_stale_open(
        self, path: Path, handle: Any,
        closing: tuple[Path, _Unavailable, Callable[[Any], Any], bool] | None,
    ) -> None:
        """Settle a stale opener using the close mode admitted for its generation."""
        if closing is None:
            close_rejected = self._close_rejected
            close_rejected_checked = self._close_rejected_checked
        else:
            close_rejected, close_rejected_checked = closing[2], closing[3]
        if close_rejected_checked:
            # A checked sweep that ended with an opener still in flight cannot publish a
            # physical receipt from this worker thread.  Retain the exact stale handle for
            # the next checked sweep; otherwise a later compatibility close could make the
            # pending handle disappear and an empty receipt would falsely verify custody.
            self._retain_failed_handle(path, handle)
            return
        if close_rejected is not None:
            with contextlib.suppress(Exception):
                close_rejected(handle)

    def get(
        self, path: Path, opener: Callable[[], Any], *, raise_on_error: bool = False,
        on_recovered: Callable[[], None] | None = None,
        non_cacheable: Callable[[Exception], bool] | None = None,
    ) -> Any:
        """Return a cached handle or make one bounded, single-flight open attempt; None while
        a retry is in flight or backing off.  ``non_cacheable`` exceptions (e.g. a live-system
        guard) are re-raised without recording a failure so the next call retries at once."""
        path = Path(path)
        with self.lock:
            if path in self.handles:
                handle = self.handles[path]
                if path not in self._registry_backed or _registry_owned(handle):
                    return handle
                # The registry force-closed this generation. Serving it would let its self-heal reopen
                # a writer the registry cannot see (a second writer beside the next ``acquire``), or keep
                # raising StateDbReplacedError after a delete + recreate: reopen through the registry.
                del self.handles[path]
                self._registry_backed.discard(path)
            unavailable = self._unavailable.setdefault(path, _Unavailable())
            if unavailable.in_flight or self._clock() < unavailable.next_retry_at:
                return None
            unavailable.in_flight = True
            was_unavailable = unavailable.failures > 0
            generation = self._generation
        if was_unavailable:
            _publish_health(self._health_source, path, "retrying")

        try:
            handle = opener()
        except Exception as exc:
            uncacheable = non_cacheable is not None and non_cacheable(exc)
            with self.lock:
                stale = self._is_stale(path, unavailable, generation)
                if uncacheable:
                    if not stale:
                        self._unavailable.pop(path, None)
                    else:
                        self._closing_inflight.pop((id(unavailable), generation), None)
                    raise
                if not stale:
                    unavailable.failures += 1
                    unavailable.error = f"{type(exc).__name__}: {exc}"
                    backoff = self._initial_retry_delay * (2 ** min(unavailable.failures - 1, 30))
                    unavailable.next_retry_at = self._clock() + min(backoff, self._max_retry_delay)
                    unavailable.in_flight = False
            if stale:
                self._settle_closing_inflight(unavailable, generation)
            if not stale:
                _publish_health(self._health_source, path, "unavailable")
            if raise_on_error:
                raise
            return None

        with self.lock:
            stale = self._is_stale(path, unavailable, generation)
            if not stale:
                self.handles[path] = handle
                if _registry_owned(handle):
                    self._registry_backed.add(path)
                else:
                    self._registry_backed.discard(path)
                self._unavailable.pop(path, None)
            if stale:
                closing = self._closing_inflight.pop((id(unavailable), generation), None)
            else:
                closing = None
        if stale:
            self._finish_stale_open(path, handle, closing)
            return None
        _publish_health(self._health_source, path, "ok")
        if was_unavailable and on_recovered is not None:
            on_recovered()
        return handle

    def last_error(self, path: Path) -> str:
        """The exception the latest failed open of *path* raised (``''`` when none is recorded)."""
        target = Path(path).resolve()
        with self.lock:
            return next((u.error for key, u in self._unavailable.items() if key.resolve() == target), "")

    def close_all(self, close: Callable[[Any], None]) -> None:
        """Drain cached handles under the lock and close them outside it."""
        with self.lock:
            old_generation = self._generation
            self._generation += 1
            self._close_rejected = close
            self._close_rejected_checked = False
            for path, unavailable in self._unavailable.items():
                if unavailable.in_flight:
                    self._closing_inflight.setdefault(
                        (id(unavailable), old_generation),
                        (path, unavailable, close, False),
                    )
            handles = list(self.handles.values())
            paths = set(self.handles) | set(self._unavailable)
            self.handles.clear()
            self._registry_backed.clear()
            self._unavailable.clear()
        for handle in handles:
            with contextlib.suppress(Exception):
                close(handle)
        with _health_lock:
            states = _health_states.get(self._health_source, {})
            for path in paths:
                states.pop(path, None)

    def close_all_checked(self, close: Callable[[Any], bool]):
        """Drain handles and return a typed receipt for lifecycle shutdown.

        The compatibility ``close_all`` intentionally suppresses callback failures.  Lifecycle
        custody must retain those failures, so its callback returns literal ``True`` per handle.
        """
        from hermes_state_registry import SessionDbCloseReceipt

        with self.lock:
            old_generation = self._generation
            self._generation += 1
            self._close_rejected = close
            self._close_rejected_checked = True
            handles = list(self.handles.items())
            failed_handles = list(self._failed_handles.values())
            self._failed_handles.clear()
            for path, unavailable in self._unavailable.items():
                if unavailable.in_flight:
                    self._closing_inflight.setdefault(
                        (id(unavailable), old_generation),
                        (path, unavailable, close, True),
                    )
            paths = set(self.handles) | set(self._unavailable)
            self.handles.clear()
            self._registry_backed.clear()
            self._unavailable.clear()
        failed = 0
        attempted_handles = handles + failed_handles
        for path, handle in attempted_handles:
            try:
                closed = close(handle) is True
            except Exception:  # health: allow BLE001 -- checked close failures stay in the receipt
                closed = False
            if not closed:
                failed += 1
                with self.lock:
                    if id(handle) in self._failed_handles:
                        continue
                    if any(existing is handle for existing in self.handles.values()):
                        continue
                    self.handles[path] = handle
                    # The handle may have come from a registry-backed cache entry.  Its exact
                    # ownership marker remains meaningful while the checked retry is pending.
                    if _registry_owned(handle):
                        self._registry_backed.add(path)
            else:
                with self.lock:
                    self._failed_handles.pop(id(handle), None)
        with self.lock:
            pending = len(self._closing_inflight)
        with _health_lock:
            states = _health_states.get(self._health_source, {})
            for path in paths:
                states.pop(path, None)
        return SessionDbCloseReceipt(
            source="gateway.session_db_recovery:RecoverableHandleCache",
            attempted=len(attempted_handles),
            succeeded=len(attempted_handles) - failed,
            failed=failed,
            pending=pending,
        )
