"""Global reversible memory-provider custody for the profile lifecycle source.

The profile lifecycle source captures the exact cached agent objects before a process-wide close.
This module owns the reversible manager leases for that snapshot.  It deliberately talks only to
the public agent facade; a missing facade is reported as an UNKNOWN capability boundary rather
than being converted into a successful global preparation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import logging
from typing import Any


logger = logging.getLogger(__name__)


@dataclass
class _Preparation:
    agent: Any
    authority: Any
    manager: Any
    lease: Any = None
    report: Any = None
    task: asyncio.Task | None = None
    status: str = "unprepared"
    reason: str = "preparation_in_progress"


@dataclass(frozen=True)
class GlobalProviderOutcome:
    """The result of one reversible global provider boundary."""

    ready: bool
    reason: str


class GlobalProviderCustody:
    """Retain exact agent leases across prepare, abort and native commit retries."""

    def __init__(self, runner: Any):
        self.runner = runner
        self._generation: int | None = None
        self._entries: dict[int, _Preparation] = {}

    def _custody(self):
        return getattr(self.runner, "_profile_lifecycle_custody", None)

    def _agents(self, generation: int):
        custody = self._custody()
        if custody is None:
            return None
        return custody.global_provider_agents(generation)

    def _snapshot_valid(self, generation: int) -> bool:
        custody = self._custody()
        return custody is not None and custody.global_provider_snapshots_valid(generation)

    def _begin_generation(self, generation: int) -> bool:
        if self._generation not in (None, generation):
            return False
        self._generation = generation
        return True

    @staticmethod
    async def _invoke(entry: _Preparation, method_name: str, *args):
        from gateway.session_authorities import owner_scope

        if getattr(entry.agent, "_memory_manager", None) is not entry.manager:
            raise RuntimeError("provider_binding_changed")
        method = getattr(entry.agent, method_name)
        with owner_scope(entry.authority):
            if inspect.iscoroutinefunction(method):
                return await method(*args)
            result = await asyncio.to_thread(method, *args)
            if inspect.isawaitable(result):
                return await result
            return result

    @staticmethod
    def _call_task(entry: _Preparation, method_name: str) -> asyncio.Task | None:
        if not callable(getattr(entry.agent, method_name, None)):
            return None
        return asyncio.create_task(GlobalProviderCustody._invoke(entry, method_name))

    @staticmethod
    def _report(report: Any, manager: Any) -> tuple[str, Any, str] | None:
        from agent.memory_retirement import ManagerPreparationReport, ManagerRetirementLease

        if not isinstance(report, ManagerPreparationReport):
            return None
        if report.lease is not None and (
            not isinstance(report.lease, ManagerRetirementLease)
            or report.lease.manager is not manager
        ):
            return None
        status = getattr(report, "status", None)
        lease = getattr(report, "lease", None)
        reason = getattr(report, "reason", None)
        prepared = getattr(report, "prepared", None)
        if type(status) is not str or status not in {"prepared", "unknown"}:
            return None
        if type(reason) is not str or not reason:
            return None
        if status == "prepared" and lease is None:
            return None
        if type(prepared) is not bool or prepared is not (status == "prepared"):
            return None
        return status, lease, reason

    @staticmethod
    def _retirement_report(report: Any) -> tuple[str, str] | None:
        from agent.memory_retirement import MemoryRetirementReport

        if not isinstance(report, MemoryRetirementReport):
            return None
        status = getattr(report, "status", None)
        reason = getattr(report, "reason", None)
        released = getattr(report, "released", None)
        if type(status) is not str or status not in {"released", "unknown"}:
            return None
        if type(reason) is not str or not reason:
            return None
        if type(released) is not bool or released is not (status == "released"):
            return None
        return status, reason

    async def _observe(self, entry: _Preparation, remaining: float | None) -> bool:
        task = entry.task
        if task is None:
            return False
        try:
            if remaining is None:
                report = await asyncio.shield(task)
            else:
                report = await asyncio.wait_for(asyncio.shield(task), max(0.0, remaining))
            observed = self._report(report, entry.manager)
        except TimeoutError:
            entry.status = "unknown"
            entry.reason = "preparation_timeout"
            return False
        except Exception:  # health: an opaque agent boundary retains the exact close custody
            logger.warning("Global memory-provider preparation failed", exc_info=True)
            entry.status = "unknown"
            entry.reason = "provider_prepare_failed"
            return False
        if observed is None:
            entry.status = "unknown"
            entry.reason = "invalid_preparation"
            return False
        status, lease, reason = observed
        if entry.lease is not None and lease is not entry.lease:
            entry.status = "unknown"
            entry.reason = "lease_changed"
            return False
        entry.report = report
        entry.lease = lease
        entry.status = status
        entry.reason = reason
        entry.task = None
        return status == "prepared"

    async def _prepare_entry(self, entry: _Preparation, remaining: float | None) -> bool:
        if entry.status == "prepared" and entry.lease is not None:
            return True
        if entry.task is None:
            entry.task = self._call_task(entry, "prepare_memory_provider")
            if entry.task is None:
                entry.status = "unknown"
                entry.reason = "missing_capability"
                return False
        return await self._observe(entry, remaining)

    async def _settle_pending(
        self, entries: tuple[_Preparation, ...], remaining: float | None,
    ) -> bool:
        """Observe every still-running public call before a source can reopen."""
        deadline = None if remaining is None else asyncio.get_running_loop().time() + max(0.0, remaining)
        for entry in entries:
            if entry.task is None:
                continue
            entry_remaining = None if deadline is None else max(
                0.0, deadline - asyncio.get_running_loop().time(),
            )
            await self._observe(entry, entry_remaining)
        return all(entry.task is None for entry in entries)

    @staticmethod
    def _lease_less_unknown_is_safe(entry: _Preparation) -> bool:
        # The only safe lease-less UNKNOWN is a capability refusal observed before invoking a
        # provider boundary.  A timeout, exception or malformed report may have fenced work that
        # Core cannot identify; keep that close UNKNOWN until the same call settles.
        return entry.lease is not None or entry.reason == "missing_capability"

    async def prepare(self, generation: int, *, remaining: float | None = None) -> GlobalProviderOutcome:
        """Prepare every exact manager represented by the captured profile cache graph."""
        if not self._begin_generation(generation) or not self._snapshot_valid(generation):
            return GlobalProviderOutcome(False, "provider_binding_changed")
        agents = self._agents(generation)
        if agents is None:
            return GlobalProviderOutcome(False, "provider_binding_changed")
        deadline = None if remaining is None else asyncio.get_running_loop().time() + max(0.0, remaining)
        for agent, _snapshot, authority, manager in agents:
            entry = self._entries.setdefault(
                id(agent), _Preparation(agent=agent, authority=authority, manager=manager),
            )
            if entry.authority is not authority or entry.manager is not manager:
                return GlobalProviderOutcome(False, "provider_binding_changed")
            entry_remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
            await self._prepare_entry(entry, entry_remaining)
        ready = bool(agents) and all(
            self._entries[id(agent)].status == "prepared"
            and self._entries[id(agent)].lease is not None
            for agent, _snapshot, _authority, _manager in agents
        )
        if not agents:
            ready = True
        if ready:
            if not self._custody().mark_global_provider_prepared(generation, True):
                return GlobalProviderOutcome(False, "provider_binding_changed")
            return GlobalProviderOutcome(True, "prepared")
        self._custody().mark_global_provider_prepared(generation, False)
        reason = next(
            (entry.reason for entry in self._entries.values() if entry.status != "prepared"),
            "preparation_incomplete",
        )
        return GlobalProviderOutcome(False, reason)

    async def _refence(self, entries: tuple[_Preparation, ...], remaining: float | None) -> bool:
        entries = tuple(
            entry for entry in entries
            if entry.lease is not None or entry.status == "resumed"
        )
        if not entries:
            return True
        ok = True
        for entry in entries:
            entry.task = None
            entry.lease = None
            entry.status = "unprepared"
            if not await self._prepare_entry(entry, remaining):
                ok = False
        return ok

    async def resume(self, generation: int, *, remaining: float | None = None) -> bool:
        """Resume all exact manager leases, re-fencing any sibling after a partial failure."""
        if self._generation is None:
            return True
        if self._generation != generation or not self._snapshot_valid(generation):
            return False
        entries = tuple(self._entries.values())
        if not await self._settle_pending(entries, remaining):
            return False
        if any(
            entry.status == "unknown" and not self._lease_less_unknown_is_safe(entry)
            for entry in entries
        ):
            return False
        to_resume = tuple(entry for entry in entries if entry.lease is not None)
        resumed: list[_Preparation] = []
        for entry in to_resume:
            method = getattr(entry.agent, "resume_memory_provider", None)
            if not callable(method):
                break
            try:
                result = await self._invoke(entry, "resume_memory_provider", entry.lease)
            except Exception:  # health: an opaque resume boundary retains the exact lease
                logger.warning("Global memory-provider resume failed", exc_info=True)
                result = False
            if type(result) is not bool or result is not True:
                break
            entry.status = "resumed"
            resumed.append(entry)
        if len(resumed) == len(to_resume):
            return True
        # A manager facade normally re-fences its own provider siblings.  This second pass covers
        # the cross-agent case: every agent that did resume must acquire a fresh source-owned lease
        # before the coordinator is allowed to reopen.
        if await self._refence(tuple(resumed), remaining):
            return False
        return False

    async def refence(self, generation: int, *, remaining: float | None = None) -> bool:
        """Re-fence resumed managers if a later source abort step refuses."""
        if self._generation is None:
            return True
        if self._generation != generation or not self._snapshot_valid(generation):
            return False
        entries = tuple(self._entries.values())
        if not await self._settle_pending(entries, remaining):
            return False
        if any(
            entry.status == "unknown" and not self._lease_less_unknown_is_safe(entry)
            for entry in entries
        ):
            return False
        return await self._refence(entries, remaining)

    def native_entries(self, generation: int):
        """Return the exact prepared manager entries for the coordinator native phase.

        This is a source-owned snapshot, not a cache enumeration at ACK time.  A missing,
        replaced, stale, or partially prepared entry refuses; callers cannot turn an empty or
        malformed manager graph into permission to release profile locks.
        """
        if self._generation != generation or not self._snapshot_valid(generation):
            return None
        agents = self._agents(generation)
        if agents is None:
            return None
        expected_ids = {id(agent) for agent, _snapshot, _authority, _manager in agents}
        if set(self._entries) != expected_ids:
            return None
        entries = []
        for agent, _snapshot, authority, manager in agents:
            entry = self._entries.get(id(agent))
            if (
                entry is None
                or entry.agent is not agent
                or entry.authority is not authority
                or entry.manager is not manager
                or entry.status != "prepared"
                or entry.lease is None
            ):
                return None
            entries.append(entry)
        return tuple(entries)

    async def commit_native_entry(self, generation: int, entry: _Preparation) -> bool:
        """Commit one exact prepared entry after the coordinator's native close.

        The participant retains the same ``_Preparation`` and opaque manager lease across
        UNKNOWN/retry.  No new cache lookup or replacement agent is admitted here.
        """
        if self._generation != generation or not self._snapshot_valid(generation):
            return False
        if not isinstance(entry, _Preparation) or self._entries.get(id(entry.agent)) is not entry:
            return False
        if entry.status == "committed":
            return True
        if entry.status not in {"prepared", "commit_unknown"} or entry.lease is None:
            return False
        method = getattr(entry.agent, "commit_memory_provider", None)
        if not callable(method):
            entry.status = "commit_unknown"
            entry.reason = "missing_capability"
            return False
        try:
            report = await self._invoke(entry, "commit_memory_provider", entry.lease)
        except Exception:  # health: opaque commit failure retains exact source lease for retry
            logger.warning("Global memory-provider native commit failed", exc_info=True)
            entry.status = "commit_unknown"
            entry.reason = "commit_failed"
            return False
        observed = self._retirement_report(report)
        if observed is None:
            entry.status = "commit_unknown"
            entry.reason = "invalid_result"
            return False
        status, reason = observed
        entry.reason = reason
        if status != "released":
            entry.status = "commit_unknown"
            return False
        entry.status = "committed"
        return True

    async def commit(self, generation: int) -> bool:
        """Commit only prepared leases after the coordinator reaches handoff_pending."""
        if self._generation is None:
            return True
        if self._generation != generation or not self._snapshot_valid(generation):
            return False
        entries = tuple(entry for entry in self._entries.values() if entry.lease is not None)
        for entry in entries:
            if not await self.commit_native_entry(generation, entry):
                return False
        return True

    def discard(self, generation: int) -> None:
        if self._generation == generation:
            self._generation = None
            self._entries.clear()


def custody_for(runner: Any) -> GlobalProviderCustody:
    custody = getattr(runner, "_profile_global_provider_custody", None)
    if not isinstance(custody, GlobalProviderCustody) or custody.runner is not runner:
        custody = GlobalProviderCustody(runner)
        runner._profile_global_provider_custody = custody
    return custody


async def prepare_global_memory_providers(runner: Any, generation: int, *, remaining: float | None = None):
    return await custody_for(runner).prepare(generation, remaining=remaining)


async def resume_global_memory_providers(runner: Any, generation: int, *, remaining: float | None = None) -> bool:
    return await custody_for(runner).resume(generation, remaining=remaining)


async def refence_global_memory_providers(runner: Any, generation: int, *, remaining: float | None = None) -> bool:
    return await custody_for(runner).refence(generation, remaining=remaining)


async def commit_global_memory_providers(runner: Any, generation: int) -> bool:
    return await custody_for(runner).commit(generation)


def discard_global_memory_providers(runner: Any, generation: int) -> None:
    custody_for(runner).discard(generation)
