"""The gateway-owned source graph and cross-process admission fence for cron.

Cron has several ingress paths (the in-process ticker, the provider, the dashboard, the API
adapter and the manual CLI).  They all write the same jobs/executions stores, so an empty local
callback map cannot prove that a second process is not about to claim work.  This module keeps the
exact producer graph beside the existing :class:`RuntimeCustodyCoordinator` and retains one real
``cron/.jobs.lock`` descriptor per served profile during a preservation close.

It is deliberately a source registry, not another ledger.  Durable execution rows remain owned by
``cron.executions`` and lifecycle admission remains owned by ``RuntimeCustodyCoordinator``.
"""
from __future__ import annotations

from dataclasses import dataclass
import contextlib
import hashlib
import importlib
import json
import logging
import os
from pathlib import Path
import sys
import stat
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# These are the production entrypoints that can create a cron claim, row, handoff or child.  The
# map is closed-world: a missing or replaced callable leaves the source graph unknown.  The values
# are intentionally import paths rather than caller-supplied labels.
CRON_PRODUCER_ENTRYPOINTS = {
    "provider_claim": ("cron.scheduler_provider", "CronScheduler.claim_fire"),
    "ticker": ("cron.scheduler_provider", "InProcessCronScheduler._start_multiplex"),
    "builtin_dispatch": ("cron.scheduler", "_submit_with_guard"),
    "manual_cli": ("tools.cronjob_tools", "_claim_for_manual_run"),
    "dashboard": ("hermes_cli.web_server_cron", "_fire_cron_job_for_profile"),
    "api_ingress": ("gateway.platforms.api_server_cron_fire", "_handle_authenticated_fire"),
    "api_pending": ("gateway.platforms.api_server", "_reserve_pending_api_work"),
    "worker": ("cron.scheduler", "_launch_external_cron_worker"),
    "script_process": ("cron.scheduler_script", "_run_job_script"),
    "worker_reaper": (
        "cron.scheduler_detached_worker", "reap_terminal_worker_in_background",
    ),
}
CRON_PRODUCER_KEYS = tuple(CRON_PRODUCER_ENTRYPOINTS)


class CronSourceGraphError(RuntimeError):
    """The exact cron producer graph cannot authenticate an admission."""


@dataclass(frozen=True)
class CronProducerBinding:
    key: str
    entrypoint: Any
    module: str
    qualname: str


@dataclass(frozen=True)
class CronJobsFence:
    home: Path
    path: Path
    fd: Any
    inode: tuple[int, int] | None


@dataclass(frozen=True)
class CronCloseOutcome:
    """Truthful result of releasing the cron source fence.

    A failed release retains the exact descriptor and generation.  Callers that already committed
    a coordinator phase can report the pending outcome instead of claiming that the source is still
    in the pre-commit state.
    """

    status: str
    generation: int | None
    pending_homes: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"opened", "closed"}

    @property
    def pending(self) -> bool:
        return self.status.endswith("cleanup_pending")


@dataclass(frozen=True)
class CronAbortCapability:
    """Exact cron source participant for one coordinator-issued reversible abort."""

    registry: Any
    coordinator: Any
    receipt: Any
    generation: int
    graph_digest: str
    homes: tuple[tuple[str, Path], ...]
    authorities: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class CronAuthorityBinding:
    """The actual authority/DB pair that bootstrapped one served profile."""

    profile_id: str
    epoch: int
    authority: Any
    database: Any
    database_path: Path


class CronAdmissionLease:
    """One strict profile lock held across all effects of one cron admission.

    The old admission boundary returned ``True`` after briefly taking and releasing ``.jobs.lock``.
    That made a close race possible between the check and the SQLite/Popen effect.  This lease keeps
    the existing jobs lock and registry admission witness alive until the producer has completed
    its effect.  It is re-entrant so existing nested jobs-store helpers can safely participate.
    """

    def __init__(
        self, registry: CronSourceRegistry | None, source_key: str,
        home: Path | None, parent: Any = None,
    ) -> None:
        self.registry = registry
        self.source_key = source_key
        self.home = home
        self.parent = parent
        self.lease_id = id(self)
        self._store_context = None
        self._lock_context = None
        self._entered = 0
        self._released = False

    @property
    def active(self) -> bool:
        return self._entered > 0 and not self._released

    def __enter__(self) -> CronAdmissionLease:
        if self._released:
            raise CronSourceGraphError("cron_admission_lease_released")
        if self._entered:
            self._entered += 1
            return self
        if self.registry is None:
            from cron import jobs

            self._store_context = (
                jobs.use_cron_store(self.home) if self.home is not None else contextlib.nullcontext()
            )
            self._store_context.__enter__()
            try:
                self._lock_context = jobs._jobs_lock(strict=True)
                self._lock_context.__enter__()
            except BaseException:
                self._store_context.__exit__(*sys.exc_info())
                self._store_context = None
                raise
        else:
            self.registry._enter_lease(self)
        self._entered = 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._entered > 1:
            self._entered -= 1
            return
        if not self._entered:
            return
        self._entered = 0
        if self.registry is None:
            try:
                if self._lock_context is not None:
                    self._lock_context.__exit__(exc_type, exc_value, traceback)
            finally:
                if self._store_context is not None:
                    self._store_context.__exit__(exc_type, exc_value, traceback)
        else:
            self.registry._leave_lease(self)
        self._released = True


def _resolve_entrypoint(module_name: str, qualname: str) -> Any:
    value = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    return value


def _canonical_home(home: Any) -> Path:
    return Path(home).expanduser().resolve()


def _profile_homes(runner: Any) -> tuple[tuple[str, Path], ...]:
    from gateway.run_runtime import reserved_profile_homes

    return tuple(
        (str(name or "default"), _canonical_home(home))
        for name, home in reserved_profile_homes(runner)
    )


def _authority_snapshot(
    runner: Any,
) -> tuple[tuple[tuple[str, int], ...], tuple[CronAuthorityBinding, ...]] | None:
    try:
        from gateway.session_authorities import all_authorities

        authorities = tuple(all_authorities(runner))
        result: list[tuple[str, int]] = []
        bindings: list[CronAuthorityBinding] = []
        for authority in authorities:
            profile_id = getattr(authority, "profile_id", None)
            epoch = getattr(authority, "epoch", None)
            database = getattr(authority, "db", None)
            database_path = getattr(database, "db_path", None)
            if (
                not isinstance(profile_id, str)
                or type(epoch) is not int
                or epoch < 1
                or database is None
                or database_path is None
            ):
                return None
            canonical_profile = _canonical_home(profile_id).as_posix()
            canonical_db_path = _canonical_home(database_path)
            result.append((canonical_profile, epoch))
            bindings.append(CronAuthorityBinding(
                canonical_profile, epoch, authority, database, canonical_db_path,
            ))
        ordered = tuple(sorted(zip(result, bindings), key=lambda item: item[0]))
        return tuple(item[0] for item in ordered), tuple(item[1] for item in ordered)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return None


class CronSourceRegistry:
    """Exact cron producer graph attached to one runtime coordinator."""

    def __init__(self, coordinator: Any):
        self.coordinator = coordinator
        self.producer_bindings: dict[str, CronProducerBinding] = {}
        self.missing: dict[str, str] = {}
        self.homes: tuple[tuple[str, Path], ...] = ()
        self.authorities: tuple[tuple[str, int], ...] | None = None
        self._authority_bindings: tuple[CronAuthorityBinding, ...] = ()
        self.provider: Any = None
        self.ticker_thread: Any = None
        self.api_adapters: tuple[Any, ...] = ()
        self.bootstrapped = False
        self._state = "open"
        self._close_generation: int | None = None
        self._fences: dict[Path, CronJobsFence] = {}
        # Physical profile ownership is captured when the runtime source graph is bootstrapped.
        # A later owner_epoch/descriptor replacement must not be adopted merely because a fresh
        # reservation currently looks healthy; that would let a close prove a different native
        # owner than the one that created the graph.
        self._native_bindings: dict[Path, tuple[Any, ...]] = {}
        self._lock = threading.RLock()
        self._lease_condition = threading.Condition(self._lock)
        self._active_leases: dict[int, CronAdmissionLease] = {}
        self._runner: Any = None

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def close_generation(self) -> int | None:
        with self._lock:
            return self._close_generation

    def _resolve_current(self, key: str) -> Any:
        module_name, qualname = CRON_PRODUCER_ENTRYPOINTS[key]
        try:
            return _resolve_entrypoint(module_name, qualname)
        except (AttributeError, ImportError, OSError, TypeError, ValueError):
            return None

    @staticmethod
    def _native_binding(evidence: dict[str, Any] | None) -> tuple[Any, ...] | None:
        if not isinstance(evidence, dict):
            return None
        physical = evidence.get("physical_binding")
        record = evidence.get("record_binding")
        if not isinstance(physical, tuple) or len(physical) != 3:
            return None
        if not isinstance(record, tuple) or len(record) != 5:
            return None
        return physical, record

    def _capture_native_bindings_locked(
        self, homes: tuple[tuple[str, Path], ...], *, initial: bool,
    ) -> None:
        for _name, home in homes:
            current = self._native_binding(self._profile_native_evidence(home))
            previous = self._native_bindings.get(home)
            if initial:
                if current is None:
                    self.missing[f"native:{home}"] = "native_owner_unavailable"
                else:
                    self._native_bindings[home] = current
                continue
            if previous is None:
                if current is None:
                    self.missing[f"native:{home}"] = "native_owner_unavailable"
                else:
                    # A hot-served profile has no prior graph incarnation; bind it exactly once.
                    self._native_bindings[home] = current
            elif current is None or current != previous:
                self.missing[f"native:{home}"] = "native_owner_replaced"

    def bootstrap(self, runner: Any) -> None:
        """Capture the immutable callable graph and current served profile authorities."""
        with self._lock:
            self._runner = runner
            self.homes = _profile_homes(runner)
            authority_snapshot = _authority_snapshot(runner)
            self.authorities = authority_snapshot[0] if authority_snapshot is not None else None
            self._authority_bindings = authority_snapshot[1] if authority_snapshot is not None else ()
            self.missing.clear()
            self._native_bindings.clear()
            self._capture_native_bindings_locked(self.homes, initial=True)
        for key in CRON_PRODUCER_KEYS:
            entrypoint = self._resolve_current(key)
            if not callable(entrypoint):
                with self._lock:
                    self.missing[key] = "entrypoint_unavailable"
                continue
            with self._lock:
                old = self.producer_bindings.get(key)
                if old is not None and old.entrypoint is not entrypoint:
                    self.missing[key] = "entrypoint_replaced"
                    continue
                self.producer_bindings[key] = CronProducerBinding(
                    key, entrypoint, str(getattr(entrypoint, "__module__", "")),
                    str(getattr(entrypoint, "__qualname__", "")),
                )
        with self._lock:
            self.bootstrapped = True

    def refresh(self, runner: Any) -> None:
        """Refresh only additive profile/provider identity; callable replacement is unknown."""
        with self._lock:
            self._runner = runner
            self.homes = _profile_homes(runner)
            authority_snapshot = _authority_snapshot(runner)
            self.authorities = authority_snapshot[0] if authority_snapshot is not None else None
            self._authority_bindings = authority_snapshot[1] if authority_snapshot is not None else ()
            self._capture_native_bindings_locked(self.homes, initial=False)
            for key, binding in tuple(self.producer_bindings.items()):
                current = self._resolve_current(key)
                if current is not binding.entrypoint:
                    self.missing[key] = "entrypoint_replaced"

    def bind_scheduler(self, provider: Any, ticker_thread: Any = None) -> None:
        with self._lock:
            if self.provider is not None and self.provider is not provider:
                self.missing["provider"] = "provider_replaced"
                return
            self.provider = provider
            if ticker_thread is not None:
                if self.ticker_thread is not None and self.ticker_thread is not ticker_thread:
                    self.missing["ticker"] = "ticker_replaced"
                    return
                self.ticker_thread = ticker_thread

    def bind_api_adapters(self, adapters: Any) -> None:
        if isinstance(adapters, dict):
            values = tuple(value for value in adapters.values() if value is not None)
        else:
            try:
                values = tuple(value for value in adapters if value is not None)
            except TypeError:
                values = ()
        with self._lock:
            if self.api_adapters and len(values) != len(self.api_adapters):
                self.missing["api_adapters"] = "api_adapter_replaced"
                return
            if self.api_adapters and any(
                all(value is not previous for previous in self.api_adapters) for value in values
            ):
                self.missing["api_adapters"] = "api_adapter_replaced"
                return
            self.api_adapters = values

    def _graph_ready_locked(self) -> bool:
        return bool(
            self.bootstrapped
            and not self.missing
            and tuple(self.producer_bindings) == CRON_PRODUCER_KEYS
            and self.homes
            and self.authorities is not None
        )

    def _evidence_graph_ready_locked(self) -> bool:
        return self._graph_ready_locked() and self.provider is not None and self.ticker_thread is not None

    def _validate_graph_locked(self) -> bool:
        """Re-read the source graph and real authorities at the proof boundary."""
        if not self._graph_ready_locked() or self._runner is None:
            return False
        for key, binding in self.producer_bindings.items():
            if self._resolve_current(key) is not binding.entrypoint:
                return False
        snapshot = _authority_snapshot(self._runner)
        if snapshot is None or snapshot[0] != self.authorities:
            return False
        if len(snapshot[1]) != len(self._authority_bindings):
            return False
        for current, captured in zip(snapshot[1], self._authority_bindings):
            if (
                current.profile_id != captured.profile_id
                or current.epoch != captured.epoch
                or current.database_path != captured.database_path
                or current.authority is not captured.authority
                or current.database is not captured.database
            ):
                return False
        for _name, home in self.homes:
            current = self._native_binding(self._profile_native_evidence(home))
            if current is None or current != self._native_bindings.get(home):
                return False
        current_adapters = _api_adapters(self._runner)
        if isinstance(current_adapters, dict):
            current_adapters = tuple(value for value in current_adapters.values() if value is not None)
        else:
            try:
                current_adapters = tuple(value for value in current_adapters if value is not None)
            except TypeError:
                current_adapters = ()
        if len(current_adapters) != len(self.api_adapters) or any(
            current is not captured
            for current, captured in zip(current_adapters, self.api_adapters)
        ):
            return False
        return True

    def _enter_lease(self, lease: CronAdmissionLease) -> None:
        from cron import jobs

        with self._lock:
            self._validate_admission_locked(lease.source_key, lease.home, lease.parent)
            store_context = (
                jobs.use_cron_store(lease.home)
                if lease.home is not None else contextlib.nullcontext()
            )
            store_context.__enter__()
            try:
                lock_context = jobs._jobs_lock(strict=True)
                lock_context.__enter__()
            except BaseException:
                store_context.__exit__(*sys.exc_info())
                raise
            lease._store_context = store_context
            lease._lock_context = lock_context
            self._active_leases[lease.lease_id] = lease

    def _leave_lease(self, lease: CronAdmissionLease) -> None:
        try:
            try:
                if lease._lock_context is not None:
                    lease._lock_context.__exit__(None, None, None)
            finally:
                if lease._store_context is not None:
                    lease._store_context.__exit__(None, None, None)
        finally:
            with self._lock:
                self._active_leases.pop(lease.lease_id, None)
                self._lease_condition.notify_all()

    def _validate_admission_locked(self, source_key: str, home: Path | None, parent: Any) -> None:
        if source_key not in CRON_PRODUCER_KEYS and source_key != "execution_row":
            raise CronSourceGraphError("cron_source_unregistered")
        if not self._validate_graph_locked():
            raise CronSourceGraphError("cron_source_graph_incomplete")
        if source_key in CRON_PRODUCER_KEYS:
            binding = self.producer_bindings.get(source_key)
            if binding is None or self._resolve_current(source_key) is not binding.entrypoint:
                raise CronSourceGraphError("cron_source_replaced")
        target = _canonical_home(home) if home is not None else None
        if target is not None and target not in {item for _name, item in self.homes}:
            raise CronSourceGraphError("cron_profile_unregistered")
        if self._state != "open":
            continuation = self._parent_is_active_locked(parent)
            allowed = {"builtin_dispatch", "worker", "script_process", "worker_reaper"}
            if not continuation or source_key not in allowed:
                raise CronSourceGraphError("cron_admission_closed")

    @staticmethod
    def _release_fence(fence: CronJobsFence) -> bool:
        from cron import jobs

        checked = getattr(jobs, "_release_flock_checked", None)
        if callable(checked):
            return bool(checked(fence.fd))
        jobs._release_flock(fence.fd)
        return True

    def _wait_for_active_leases_locked(self, deadline: float | None) -> bool:
        while self._active_leases:
            if deadline is None:
                self._lease_condition.wait()
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self._lease_condition.wait(timeout=remaining)
        return True

    def _release_candidate_or_retain(
        self, fence: CronJobsFence, retained: list[CronJobsFence],
    ) -> None:
        try:
            if not self._release_fence(fence):
                retained.append(fence)
        except (OSError, ValueError):
            retained.append(fence)

    def begin_close(self, generation: int, *, deadline: float | None = None) -> bool:
        """Retain every profile's real jobs lock for the coordinator's close generation.

        ``deadline`` is an absolute monotonic deadline supplied by the owning runtime close.  The
        synchronous form remains for source/tests that already run off the gateway loop; the public
        async close uses :meth:`begin_close_async` so this wait never blocks that loop.
        """
        if type(generation) is not int or generation < 1:
            return False
        from cron import jobs

        acquired: list[CronJobsFence] = []
        with self._lock:
            if self._state != "open" or not self._validate_graph_locked():
                return False
            # Close is linearized before waiting for already accepted leases.  A producer that
            # won the admission gate before this transition is allowed to finish its durable or
            # native effect; no producer can enter after the transition unless it is the exact
            # continuation of that accepted custody.
            self._state = "closing"
            self._close_generation = generation
            if not self._wait_for_active_leases_locked(deadline):
                self._state = "open"
                self._close_generation = None
                return False
            homes = tuple(sorted({home for _name, home in self.homes}, key=lambda item: os.path.normcase(str(item))))
        try:
            for home in homes:
                if deadline is not None and deadline - time.monotonic() <= 0:
                    raise CronSourceGraphError("cron_jobs_lock_deadline_expired")
                cron_dir = home / "cron"
                cron_dir.mkdir(parents=True, exist_ok=True)
                path = cron_dir / ".jobs.lock"
                flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                fd_number = os.open(path, flags, 0o600)
                fd = os.fdopen(fd_number, "a+", encoding="utf-8")
                candidate = CronJobsFence(home, path, fd, None)
                timeout = jobs._JOBS_LOCK_TIMEOUT_SECONDS
                if deadline is not None:
                    timeout = max(0.0, min(timeout, deadline - time.monotonic()))
                result = jobs._acquire_flock(fd, timeout)
                if result is not True:
                    fd.close()
                    raise CronSourceGraphError("cron_jobs_lock_unavailable")
                try:
                    stat = os.fstat(fd.fileno())
                    inode = (int(stat.st_dev), int(stat.st_ino))
                except (OSError, ValueError):
                    self._release_candidate_or_retain(candidate, acquired)
                    raise CronSourceGraphError("cron_jobs_lock_identity_unknown")
                acquired.append(CronJobsFence(home, path, fd, inode))
        except (OSError, RuntimeError, TypeError, ValueError, CronSourceGraphError):
            remaining_fences: list[CronJobsFence] = []
            for fence in reversed(acquired):
                try:
                    if not self._release_fence(fence):
                        remaining_fences.append(fence)
                except (OSError, ValueError):
                    remaining_fences.append(fence)
                    logger.error("Cron jobs fence release failed for %s", fence.home, exc_info=True)
            with self._lock:
                if remaining_fences:
                    # Never discard a partial native/OS fence after a cleanup failure.  Keep the
                    # exact descriptor and generation retryable through abort_close_outcome().
                    self._fences = {fence.home: fence for fence in remaining_fences}
                else:
                    self._fences = {}
                    self._state = "open"
                    self._close_generation = None
            return False
        with self._lock:
            if self._state != "closing" or self._close_generation != generation:
                remaining_fences: list[CronJobsFence] = []
                for fence in reversed(acquired):
                    try:
                        if not self._release_fence(fence):
                            remaining_fences.append(fence)
                    except (OSError, ValueError):
                        remaining_fences.append(fence)
                self._fences = {fence.home: fence for fence in remaining_fences}
                if not remaining_fences:
                    self._state = "open"
                    self._close_generation = None
                return False
            self._fences = {fence.home: fence for fence in acquired}
            self._close_generation = generation
            self._state = "closing"
        return True

    async def begin_close_async(self, generation: int, *, deadline: float) -> bool:
        """Run the bounded close barrier without blocking the gateway event loop.

        The source lease may be held by a coroutine on that same loop.  The synchronous barrier is
        therefore delegated to a worker thread, where its condition wait can be notified by the
        coroutine's eventual lease release.  The one absolute deadline also covers every profile
        jobs-lock acquisition performed by that worker.
        """
        import asyncio

        return await asyncio.to_thread(self.begin_close, generation, deadline=deadline)

    def _release_all_locked(self) -> bool:
        fences = tuple(self._fences.values())
        released = True
        for fence in reversed(fences):
            try:
                if not self._release_fence(fence):
                    released = False
                    continue
                self._fences.pop(fence.home, None)
            except (OSError, ValueError):
                released = False
                logger.error("Cron jobs fence release failed for %s", fence.home, exc_info=True)
        return released

    def abort_close(self, generation: int) -> bool:
        return self.abort_close_outcome(generation).succeeded

    def abort_close_outcome(self, generation: int) -> CronCloseOutcome:
        with self._lock:
            if self._state != "closing" or self._close_generation != generation:
                return CronCloseOutcome("refused", self._close_generation)
            if not self._release_all_locked():
                return CronCloseOutcome(
                    "closing_cleanup_pending", generation,
                    tuple(str(home) for home in self._fences),
                )
            self._state = "open"
            self._close_generation = None
            return CronCloseOutcome("opened", generation)

    def mark_handoff_pending(self, generation: int) -> bool:
        with self._lock:
            if self._state != "closing" or self._close_generation != generation:
                return False
            self._state = "handoff_pending"
            return True

    def acknowledge_native(self, generation: int) -> bool:
        return self.acknowledge_native_outcome(generation).succeeded

    def acknowledge_native_outcome(self, generation: int) -> CronCloseOutcome:
        with self._lock:
            if self._state != "handoff_pending" or self._close_generation != generation:
                return CronCloseOutcome("refused", self._close_generation)
            if not self._release_all_locked():
                return CronCloseOutcome(
                    "handoff_cleanup_pending", generation,
                    tuple(str(home) for home in self._fences),
                )
            self._state = "closed"
            return CronCloseOutcome("closed", generation)

    @staticmethod
    def _fence_matches(fence: CronJobsFence) -> bool:
        try:
            if getattr(fence.fd, "closed", False):
                return False
            stat = os.fstat(fence.fd.fileno())
            return fence.inode == (int(stat.st_dev), int(stat.st_ino)) and path_same_inode(
                fence.path, fence.fd.fileno())
        except (OSError, ValueError):
            return False

    def _profile_native_evidence(self, home: Path) -> dict[str, Any] | None:
        try:
            from gateway.runtime_ownership import process_ownership

            evidence = process_ownership.reservation_evidence(home)
            return evidence if isinstance(evidence, dict) else None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def _ledger_quiet(self, home: Path) -> dict[str, Any] | None:
        try:
            from cron.executions import list_executions
            from cron.scheduler import _wedged_inflight_keys, is_job_running
            from cron.scheduler_provider import _profile_cron_scope
            from hermes_constants import hermes_home_key

            with _profile_cron_scope(home):
                # A complete paged read is required; an empty page is positive only after the
                # first query, never because a local callback index happened to be empty.
                rows: list[dict[str, Any]] = []
                before = None
                while True:
                    page = list_executions(limit=500, before_claimed_at=before)
                    if not page:
                        break
                    if any(not isinstance(item, dict) for item in page):
                        return False
                    rows.extend(page)
                    if len(page) < 500:
                        break
                    next_before = page[-1].get("claimed_at")
                    if not isinstance(next_before, str) or next_before == before:
                        return False
                    before = next_before
                terminal_rows = 0
                for row in rows:
                    status = row.get("status")
                    if status not in {"completed", "failed"}:
                        return None
                    if (
                        not isinstance(row.get("finished_at"), str)
                        or not isinstance(row.get("process_id"), str)
                        or type(row.get("pid")) is not int
                        or row.get("pid") <= 0
                        or bool(row.get("handoff_pending"))
                        or row.get("delivery_outcome") == "queued"
                    ):
                        return None
                    if is_job_running(str(row.get("job_id")), home=home):
                        return None
                    terminal_rows += 1
                # ``get_wedged_job_ids`` intentionally projects away the profile home for
                # host-wide shutdown metrics.  A profile's idle proof must not be poisoned by a
                # wedged claim belonging to a different served home with the same job id.
                local_home_key = hermes_home_key(home)
                if any(
                    len(key) >= 2 and key[0] == local_home_key
                    for key in _wedged_inflight_keys()
                ):
                    return None
                workers = home / "cron" / "external-workers"
                if workers.exists() and any(workers.iterdir()):
                    return None
            return {"rows": len(rows), "terminal_rows": terminal_rows, "empty": not rows}
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def _profile_graph_digest_locked(self) -> str:
        payload = {
            "homes": [(name, str(home)) for name, home in self.homes],
            "authorities": self.authorities,
            "authority_bindings": [
                (binding.profile_id, binding.epoch, str(binding.database_path),
                 id(binding.authority), id(binding.database))
                for binding in self._authority_bindings
            ],
            "native_bindings": [
                (str(home), binding)
                for home, binding in sorted(self._native_bindings.items())
            ],
            "producers": [
                (key, binding.module, binding.qualname)
                for key, binding in self.producer_bindings.items()
            ],
            "provider": (type(self.provider).__module__, type(self.provider).__qualname__),
            "ticker": (type(self.ticker_thread).__module__, type(self.ticker_thread).__qualname__),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()

    def _parent_is_active_locked(self, parent: Any) -> bool:
        if parent is None or getattr(parent, "coordinator", None) is not self.coordinator:
            return False
        handle = getattr(parent, "handle", None)
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        records = getattr(self.coordinator, "_records", None)
        record = records.get(lease_id) if isinstance(records, dict) and lease_id else None
        return bool(record is not None and record.handle is handle and record.state in {"accepted", "finalizing"})

    def source_owner_evidence(
        self, owner_class: str, *, process_instance: str, authority_epoch: int,
        close_generation: int, owner_id: str,
    ) -> dict[str, Any] | None:
        from gateway.runtime_custody import OWNER_SPECS

        with self._lock:
            if (
                not self._evidence_graph_ready_locked()
                or self._state != "closing"
                or self._close_generation != close_generation
                or process_instance != getattr(self.coordinator, "process_instance", None)
                or authority_epoch != getattr(self.coordinator, "authority_epoch", None)
                or owner_class not in {"I12_cron_fire", "I13_external_cron_worker", "I14_api_detached_cron"}
                or type(owner_id) is not str or not owner_id
            ):
                return None
            if not self._validate_graph_locked():
                return None
            homes = tuple(self.homes)
            authorities = self.authorities
            fences = tuple(self._fences)
            graph_digest = self._profile_graph_digest_locked()
        if authorities is None or len(authorities) != len(homes):
            return None
        profile_rows: list[dict[str, Any]] = []
        for _name, home in homes:
            with self._lock:
                fence = self._fences.get(home)
            if fence is None or home not in fences or not self._fence_matches(fence):
                return None
            native = self._profile_native_evidence(home)
            with self._lock:
                bound_native = self._native_bindings.get(home)
            if (
                native is None
                or native.get("os_lock_owner") is not True
                or self._native_binding(native) != bound_native
            ):
                return None
            ledger = self._ledger_quiet(home)
            if ledger is None:
                return None
            profile_rows.append({
                "home": str(home),
                "native_owner_epoch": native.get("owner_epoch"),
                "jobs_lock_path": str(home / "cron" / ".jobs.lock"),
                "jobs_lock_inode": fence.inode,
                "authority_epoch": next(
                    (epoch for profile, epoch in authorities if profile == home.as_posix()), None),
                "authority_binding": next(
                    (
                        {
                            "authority_id": id(binding.authority),
                            "database_id": id(binding.database),
                            "database_path": str(binding.database_path),
                        }
                        for binding in self._authority_bindings
                        if binding.profile_id == home.as_posix()
                    ),
                    None,
                ),
                "ledger": ledger,
                "cross_process_proof": True,
            })
        return {
            "source": OWNER_SPECS[owner_class].source,
            "source_graph": "gateway/runtime_cron_source_registry.py:CRON_PRODUCER_ENTRYPOINTS",
            "process_instance": process_instance,
            "authority_epoch": authority_epoch,
            "close_generation": close_generation,
            "owner_id": owner_id,
            "observed": True,
            "cross_process_proof": True,
            "cross_process_proof_source": (
                "cron_source_graph+strict_jobs_lock+profile_owner+durable_ledger"
            ),
            "registered_producers": CRON_PRODUCER_KEYS,
            "producer_entrypoints": tuple(
                (key, binding.module, binding.qualname)
                for key, binding in self.producer_bindings.items()
            ),
            "graph_digest": graph_digest,
            "profiles": tuple(profile_rows),
            "profile_count": len(profile_rows),
        }

    def source_owner_evidence_and_seal(self, **kwargs: Any) -> tuple[dict[str, Any] | None, Any]:
        evidence = self.source_owner_evidence(**kwargs)
        if evidence is None:
            return None, None
        refusal = self.coordinator.seal_owner(kwargs["owner_class"], evidence)
        if refusal is not None:
            return None, refusal
        return evidence, None

    def admit(self, source_key: str, *, home: Any = None, parent: Any = None) -> CronAdmissionLease:
        """Return a lease held across the producer's durable/native effects."""
        target = _canonical_home(home) if home is not None else None
        return CronAdmissionLease(self, source_key, target, parent)


def path_same_inode(path: Path, fd: int) -> bool:
    try:
        stat_path = os.lstat(path)
        stat_fd = os.fstat(fd)
        if not stat.S_ISREG(stat_path.st_mode) or not stat.S_ISREG(stat_fd.st_mode):
            return False
        return (int(stat_path.st_dev), int(stat_path.st_ino)) == (
            int(stat_fd.st_dev), int(stat_fd.st_ino)
        )
    except (OSError, ValueError):
        return False


def ensure_cron_source_registry(coordinator: Any) -> CronSourceRegistry:
    registry = getattr(coordinator, "_cron_source_registry", None)
    if isinstance(registry, CronSourceRegistry) and registry.coordinator is coordinator:
        return registry
    registry = CronSourceRegistry(coordinator)
    coordinator._cron_source_registry = registry
    return registry


def bootstrap_cron_source_registry(runner: Any, coordinator: Any) -> CronSourceRegistry:
    registry = ensure_cron_source_registry(coordinator)
    registry.bootstrap(runner)
    registry.bind_api_adapters(_api_adapters(runner))
    return registry


def refresh_cron_source_registry(runner: Any, coordinator: Any) -> CronSourceRegistry:
    registry = ensure_cron_source_registry(coordinator)
    registry.refresh(runner)
    registry.bind_api_adapters(_api_adapters(runner))
    return registry


def bind_cron_scheduler_runtime(runner: Any, coordinator: Any, provider: Any, ticker: Any) -> None:
    registry = ensure_cron_source_registry(coordinator)
    registry.bind_scheduler(provider, ticker)
    registry.bind_api_adapters(_api_adapters(runner))


def _api_adapters(runner: Any) -> Any:
    try:
        from gateway.run_runtime import _api_server_adapters

        return _api_server_adapters(runner)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return getattr(runner, "adapters", None)


def admit_cron_source(
    source_key: str, *, home: Any = None, parent: Any = None,
) -> CronAdmissionLease:
    """Common fail-closed boundary used by all claim/row/dispatch producers."""
    try:
        gateway_run = sys.modules.get("gateway.run")
        runner_ref = getattr(gateway_run, "_gateway_runner_ref", None)
        runner = runner_ref() if callable(runner_ref) else None
        coordinator = getattr(runner, "runtime_custody", None) if runner is not None else None
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        coordinator = None
    if coordinator is None:
        target = _canonical_home(home) if home is not None else None
        return CronAdmissionLease(None, source_key, target, parent)
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        raise CronSourceGraphError("cron_source_registry_unavailable")
    return registry.admit(source_key, home=home, parent=parent)


def cron_owner_evidence(owner_class: str, **kwargs: Any) -> dict[str, Any] | None:
    registry = getattr(kwargs.pop("coordinator", None), "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return None
    return registry.source_owner_evidence(owner_class, **kwargs)


def seal_cron_owner(coordinator: Any, owner_class: str, **kwargs: Any) -> tuple[dict[str, Any] | None, Any]:
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return None, None
    return registry.source_owner_evidence_and_seal(owner_class=owner_class, **kwargs)


def begin_cron_close(coordinator: Any, generation: int) -> bool:
    registry = getattr(coordinator, "_cron_source_registry", None)
    return isinstance(registry, CronSourceRegistry) and registry.begin_close(generation)


async def begin_cron_close_async(
    coordinator: Any, generation: int, *, deadline: float,
) -> bool:
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return False
    return await registry.begin_close_async(generation, deadline=deadline)


def abort_cron_close(coordinator: Any, generation: int) -> bool:
    registry = getattr(coordinator, "_cron_source_registry", None)
    return isinstance(registry, CronSourceRegistry) and registry.abort_close(generation)


def abort_cron_close_outcome(coordinator: Any, generation: int) -> CronCloseOutcome:
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return CronCloseOutcome("refused", None)
    return registry.abort_close_outcome(generation)


def mark_cron_handoff_pending(coordinator: Any, generation: int) -> bool:
    registry = getattr(coordinator, "_cron_source_registry", None)
    return isinstance(registry, CronSourceRegistry) and registry.mark_handoff_pending(generation)


def acknowledge_cron_native(coordinator: Any, generation: int) -> bool:
    registry = getattr(coordinator, "_cron_source_registry", None)
    return isinstance(registry, CronSourceRegistry) and registry.acknowledge_native(generation)


def acknowledge_cron_native_outcome(coordinator: Any, generation: int) -> CronCloseOutcome:
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return CronCloseOutcome("refused", None)
    return registry.acknowledge_native_outcome(generation)


def _same_cron_receipt(left, right) -> bool:
    return all(
        getattr(left, field, None) == getattr(right, field, None)
        for field in ("receipt_id", "process_instance", "authority_epoch", "close_generation")
    ) and left is not None and right is not None


def prepare_cron_abort(coordinator: Any, receipt: Any) -> CronAbortCapability | None:
    """Capture the immutable cron graph while its native fences remain CLOSING."""
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return None
    existing = getattr(registry, "_post_open_capability", None)
    if isinstance(existing, CronAbortCapability):
        return existing if _same_cron_receipt(existing.receipt, receipt) else None
    if (
        getattr(coordinator, "state", None) != "closing"
        or not callable(getattr(coordinator, "_check_close_identity", None))
        or not coordinator._check_close_identity(receipt)
    ):
        return None
    with registry._lock:
        if (
            registry._state != "closing"
            or registry._close_generation != receipt.close_generation
            or not registry._validate_graph_locked()
        ):
            return None
        digest = registry._profile_graph_digest_locked()
        capability = CronAbortCapability(
            registry=registry,
            coordinator=coordinator,
            receipt=receipt,
            generation=receipt.close_generation,
            graph_digest=digest,
            homes=registry.homes,
            authorities=registry.authorities or (),
        )
        registry._post_open_capability = capability
        return capability


def cron_abort_participant(capability: CronAbortCapability):
    return (capability.registry, capability.receipt, capability)


def validate_cron_abort_participant(coordinator: Any, transition: Any, entry: Any) -> bool:
    if type(entry) is not tuple or len(entry) != 3:
        return False
    registry, receipt, capability = entry
    if (
        not isinstance(capability, CronAbortCapability)
        or capability.registry is not registry
        or capability.receipt is not receipt
        or capability.coordinator is not coordinator
        or not _same_cron_receipt(receipt, getattr(transition, "receipt", None))
        or capability.generation != getattr(transition, "close_generation", None)
        or getattr(registry, "_post_open_capability", None) is not capability
    ):
        return False
    with registry._lock:
        if registry.homes != capability.homes or registry.authorities != capability.authorities:
            return False
        if registry._state not in {"closing", "open"}:
            return False
        if registry._state == "closing":
            if registry._close_generation != capability.generation:
                return False
            return registry._validate_graph_locked() and registry._profile_graph_digest_locked() == capability.graph_digest
        return True


def complete_cron_abort(capability: CronAbortCapability, transition: Any) -> bool:
    """Release the exact cron fences only after coordinator OPEN; retain pending on failure."""
    if not isinstance(capability, CronAbortCapability):
        return False
    coordinator = capability.coordinator
    if getattr(coordinator, "state", None) != "open":
        return False
    if not callable(getattr(coordinator, "check_post_open_transition", None)):
        return False
    if not coordinator.check_post_open_transition(transition):
        return False
    registry = capability.registry
    with registry._lock:
        if registry._state == "open" and registry._close_generation is None:
            return True
        if registry._state != "closing" or registry._close_generation != capability.generation:
            return False
    outcome = registry.abort_close_outcome(capability.generation)
    return outcome.status == "opened"


def clear_cron_abort(capability: CronAbortCapability, transition: Any) -> bool:
    if not isinstance(capability, CronAbortCapability):
        return False
    registry = capability.registry
    if getattr(registry, "_post_open_capability", None) is not capability:
        return False
    completed = getattr(capability.coordinator, "completed_post_open_transition_for", None)
    if not callable(completed) or completed(capability.receipt) is None:
        return False
    registry._post_open_capability = None
    return True
