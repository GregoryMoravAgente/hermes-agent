"""Source-owned custody for multiplexed profile authority lifecycle changes.

The profile registry is an ingress boundary in its own right.  A profile can be idle while its
authority, database handle, lock and cached agents are still live, so an empty session map is not a
retirement receipt.  This module installs one immutable identity witness per authority and retains
the same witness through a retire/remove or retire/successor transition.

It deliberately does not own the runtime lease ledger.  The coordinator remains the owner of
process close and operation leases; this module owns the profile lifecycle source's admission fence
and its typed registry transition evidence.
"""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from dataclasses import dataclass
import inspect
import logging
from pathlib import Path
import uuid
from typing import Any


OWNER_CLASS = "I18_multiplex_profile_retirement"
logger = logging.getLogger(__name__)
SOURCE = (
    "gateway/session_authorities.py:22-112,178-212;gateway/run_runtime.py;"
    "gateway/session_hosted_service.py"
)


@dataclass(frozen=True)
class ProfileLifecycleLease:
    """Immutable identity for one source-owned profile authority incarnation."""

    profile_id: str
    profile_home: str
    authority_epoch: int
    process_instance: str
    physical_owner_epoch: str
    lease_id: str


@dataclass(frozen=True)
class ProfileLifecycleProducerLease:
    """One accepted retire/replace producer held until its finalizer settles."""

    lease: ProfileLifecycleLease
    producer_id: str
    close_generation: int


@dataclass(frozen=True)
class ProfileHotServeLease:
    """Source-owned admission for a hot-serve build that has no authority yet.

    The target profile is already held by the process ownership lock, but its new
    ``SessionAuthority`` does not exist until the asynchronous build completes.  Keep
    this exact producer in the same I18 custody registry so a global close cannot read
    the current-authority map as an empty/complete source while the build is admitted.
    """

    profile_home: str
    profile_name: str
    process_instance: str
    physical_owner_epoch: str
    producer_id: str
    predecessor_lease_id: str | None = None


@dataclass(frozen=True)
class ProfileLifecycleReceipt:
    """Source receipt for a completed authority finalizer."""

    schema: str
    profile_id: str
    profile_home: str
    authority_epoch: int
    process_instance: str
    close_generation: int
    workers_stopped: bool
    rooms_stopped: bool
    source_finalizers_settled: bool


@dataclass
class _Record:
    lease: ProfileLifecycleLease
    authority: Any
    graph: Any
    registered_witness: str
    state: str = "open"
    close_generation: int = 0
    producer: ProfileLifecycleProducerLease | None = None
    receipt: ProfileLifecycleReceipt | None = None
    transition: dict[str, Any] | None = None
    predecessor_lease_id: str | None = None
    ownership_rebinds: int = 0
    # A loaded memory provider is a downstream owner of this exact authority.  The callback and
    # producer are retained together so a source-only receipt can never be mistaken for a full
    # retirement after a provider reports UNKNOWN.
    provider_callback: Any = None
    provider_producer: ProfileLifecycleProducerLease | None = None
    provider_snapshot: Any = None
    provider_cache_keys: tuple[str, ...] | None = None
    provider_result: Any = None
    provider_settled: bool = True
    global_provider_snapshot: Any = None
    global_provider_cache_keys: tuple[str, ...] | None = None
    global_provider_prepared: bool = False


@dataclass(frozen=True)
class _CanonicalProviderRetirement:
    """The source-owned provider operation retained across every lifecycle retry.

    The operation is deliberately a stable object owned by the custody record.  A reconcile
    callback may disappear with the route, but it must not be able to replace the provider
    authority or cause a later retry to snapshot a reused cache key.
    """

    custody: ProfileLifecycleCustody
    record: _Record

    async def __call__(self, authority: Any, *, expected_snapshot: Any = None) -> bool:
        if authority is not self.record.authority:
            return False
        current = self.custody._record_for(authority)
        if current is not self.record or current.provider_callback is not self:
            return False
        if expected_snapshot is not None and expected_snapshot is not self.record.provider_snapshot:
            return False
        return await self.custody._retire_canonical_provider(self.record)


class ProfileLifecycleCustody:
    """Identity-bound profile lifecycle source registry attached to one gateway runner."""

    def __init__(self, runner: Any):
        self.runner = runner
        self._current: dict[str, _Record] = {}
        self._history: list[_Record] = []
        self._global_close_generation: int | None = None
        self._pending_hot_serves: dict[str, ProfileHotServeLease] = {}

    @staticmethod
    def _home(authority: Any) -> str:
        profile_id = getattr(authority, "profile_id", None)
        if not isinstance(profile_id, str) or not profile_id:
            raise RuntimeError("profile_lifecycle_profile_missing")
        db_path = getattr(getattr(authority, "db", None), "db_path", None)
        if db_path is None:
            return str(Path(profile_id).resolve())
        return str(Path(db_path).parent.resolve())

    def _coordinator(self):
        coordinator = getattr(self.runner, "runtime_custody", None)
        if coordinator is None:
            raise RuntimeError("profile_lifecycle_coordinator_missing")
        return coordinator

    @staticmethod
    def _same_identity(left: ProfileLifecycleLease, right: ProfileLifecycleLease) -> bool:
        return (
            left.profile_id == right.profile_id
            and left.profile_home == right.profile_home
            and left.authority_epoch == right.authority_epoch
            and left.process_instance == right.process_instance
            and left.physical_owner_epoch == right.physical_owner_epoch
        )

    def _registry_authority(self, home: str):
        registry = getattr(self.runner, "session_authorities", None)
        if registry is not None:
            return registry.for_home(Path(home))
        authority = getattr(self.runner, "session_authority", None)
        return authority if authority is not None and self._home(authority) == home else None

    def _identity(self, authority: Any, *, require_registry: bool = True):
        coordinator = self._coordinator()
        home = self._home(authority)
        profile_id = getattr(authority, "profile_id", None)
        epoch = getattr(authority, "epoch", None)
        process_instance = getattr(authority, "instance_id", None)
        if (
            type(epoch) is not int or epoch < 1
            or type(process_instance) is not str or not process_instance
            or getattr(coordinator, "process_instance", None) != process_instance
            or (require_registry and self._registry_authority(home) is not authority)
        ):
            raise RuntimeError("profile_lifecycle_identity_mismatch")
        observation = coordinator.profile_scope_observation(
            profile_id, epoch, require_serving=False, allow_retiring=True,
        )
        if (
            not getattr(observation, "ok", False)
            or type(getattr(observation, "physical_owner_epoch", None)) is not str
            or not observation.physical_owner_epoch
        ):
            raise RuntimeError(getattr(observation, "reason", None) or "foreign_custody")
        graph = getattr(authority, "_preservation_producer_graph", None)
        if graph is not None and not graph.matches(authority, coordinator):
            raise RuntimeError("profile_lifecycle_source_graph_mismatch")
        return (
            ProfileLifecycleLease(
                profile_id=profile_id, profile_home=home, authority_epoch=epoch,
                process_instance=process_instance,
                physical_owner_epoch=observation.physical_owner_epoch,
                lease_id=uuid.uuid4().hex,
            ),
            graph,
        )

    def register(
        self, authority: Any, *, hot_serve: ProfileHotServeLease | None = None,
    ) -> ProfileLifecycleLease:
        """Install the source witness before the authority can publish lifecycle work."""
        home = self._home(authority)
        pending = self._pending_hot_serves.get(home)
        if pending is not hot_serve and (pending is not None or hot_serve is not None):
            raise RuntimeError("profile_lifecycle_hot_serve_unsettled")
        current = self._current.get(home)
        if current is not None:
            if current.authority is authority and hot_serve is None:
                return current.lease
            raise RuntimeError("profile_lifecycle_registry_collision")
        lease, graph = self._identity(authority)
        if hot_serve is not None and (
            lease.profile_home != hot_serve.profile_home
            or lease.process_instance != hot_serve.process_instance
            or lease.physical_owner_epoch != hot_serve.physical_owner_epoch
        ):
            raise RuntimeError("profile_lifecycle_hot_serve_identity_mismatch")
        record = _Record(
            lease=lease, authority=authority, graph=graph,
            registered_witness=uuid.uuid4().hex,
        )
        self._current[home] = record
        authority._profile_lifecycle_lease = lease
        authority._profile_lifecycle_source = self
        if hot_serve is not None:
            self._pending_hot_serves.pop(home, None)
        return lease

    @staticmethod
    def _physical_owner_epoch(home: str) -> str | None:
        """Read the existing native profile lock witness for a pre-authority hot serve."""
        from gateway.runtime_ownership import process_ownership

        try:
            evidence = process_ownership.reservation_evidence(Path(home))
        except (OSError, TypeError, ValueError):
            return None
        if not isinstance(evidence, dict):
            return None
        owner_epoch = evidence.get("owner_epoch")
        descriptor = evidence.get("native_descriptor")
        if (
            evidence.get("profile_home") != home
            or type(owner_epoch) is not str or not owner_epoch
            or evidence.get("native_handle_retained") is not True
            or evidence.get("os_lock_owner") is not True
            or evidence.get("cross_process_proof") is not True
            or evidence.get("observed") is not True
            or evidence.get("physical_binding") != (home, owner_epoch, descriptor)
            or type(descriptor) is not int or descriptor < 0
        ):
            return None
        return owner_epoch

    def _physical_owner_matches(self, record: _Record) -> bool:
        """Re-read the native reservation after registry/authority teardown has begun."""
        authority = record.authority
        try:
            return (
                getattr(self._coordinator(), "process_instance", None)
                == record.lease.process_instance
                and getattr(authority, "profile_id", None) == record.lease.profile_id
                and getattr(authority, "epoch", None) == record.lease.authority_epoch
                and self._physical_owner_epoch(record.lease.profile_home)
                == record.lease.physical_owner_epoch
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            return False

    def _canonical_provider_cache_keys(self, authority: Any) -> tuple[str, ...] | None:
        """Resolve the exact cache namespace owned by *authority* from the live registry.

        The lifecycle source, rather than a reconcile callback, chooses the cache scope.  This
        keeps a retry on a withdrawn route attached to the authority that admitted the source
        producer, even if a profile name or cache key is reused later.
        """
        runner = self.runner
        cache = getattr(runner, "_agent_cache", None)
        if cache is None:
            return ()
        if not isinstance(cache, dict):
            return None
        registry = getattr(runner, "session_authorities", None)
        try:
            profile_name = registry.profile_name(authority) if registry is not None else None
            from gateway.session import _session_key_namespace
            prefix = _session_key_namespace(profile_name) + ":"
        except (AttributeError, ImportError, TypeError, ValueError):
            return None
        lock = getattr(runner, "_agent_cache_lock", None)
        try:
            if lock is None:
                return tuple(key for key in list(cache) if str(key).startswith(prefix))
            with lock:
                return tuple(key for key in list(cache) if str(key).startswith(prefix))
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return None

    @staticmethod
    def _cache_snapshot_matches(runner: Any, expected: Any) -> bool:
        """Check that the live cache still contains only the captured identities."""
        if expected is None:
            return False
        try:
            from gateway.run_agent_cache import _first_agent
        except ImportError:
            return False
        cache = getattr(runner, "_agent_cache", None)
        if cache is None:
            return expected == (frozenset(), ())
        if not isinstance(cache, dict):
            return False
        try:
            initial_cache_keys, cached_agents = expected
        except (TypeError, ValueError):
            return False
        lock = getattr(runner, "_agent_cache_lock", None)
        missing = _MISSING_PROVIDER_CACHE_ENTRY

        def check():
            if not frozenset(cache).issubset(initial_cache_keys):
                return False
            for key, _was_present, captured_agent, captured_manager in cached_agents:
                entry = cache.get(key, missing)
                if entry is missing:
                    if captured_agent is not None and getattr(
                        captured_agent, "_memory_manager", None,
                    ) is not captured_manager:
                        return False
                    continue
                current_agent = _first_agent(entry)
                if captured_agent is None:
                    if current_agent is not None:
                        return False
                    continue
                if current_agent is not captured_agent:
                    return False
                if getattr(captured_agent, "_memory_manager", None) is not captured_manager:
                    return False
            return True

        try:
            if lock is None:
                return check()
            with lock:
                return check()
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return False

    def _capture_provider_snapshot(self, authority: Any):
        """Return the source-selected cache scope and its exact live identities."""
        cache_keys = self._canonical_provider_cache_keys(authority)
        if cache_keys is None:
            return None
        snapshot = snapshot_loaded_memory_provider_identities(self.runner, cache_keys)
        if snapshot is None:
            return None
        return cache_keys, snapshot

    @staticmethod
    def _snapshot_may_have_provider(snapshot: Any) -> bool:
        """Return whether a captured cache graph contains a live manager owner.

        The global close boundary must not ask a provider to retire while ingress is only being
        fenced.  A cached agent/manager pair is therefore conservatively pending until the
        canonical callback has observed the provider result after source workers join.  An empty
        cache snapshot remains the ordinary zero-provider path.
        """
        try:
            _initial_cache_keys, cached_agents = snapshot
            rows = tuple(cached_agents)
        except Exception:  # health: allow BLE001 -- malformed provider snapshot retains custody
            return True
        for row in rows:
            if not isinstance(row, tuple) or len(row) != 4:
                return True
            _key, _was_present, agent, manager = row
            if agent is not None and manager is not None:
                return True
        return False

    def _prepare_provider_retirement(
        self, record: _Record, *, captured: tuple[tuple[str, ...], Any] | None = None,
    ) -> bool:
        """Bind the canonical provider operation after source workers have settled."""
        if captured is None:
            captured = self._capture_provider_snapshot(record.authority)
        if captured is None:
            return False
        cache_keys, snapshot = captured
        if record.provider_snapshot is not None and not self._cache_snapshot_matches(
            self.runner, record.provider_snapshot,
        ):
            # A replacement appeared after an explicit legacy binding.  Retain the original
            # source witness rather than rebinding it to the replacement.
            return False
        record.provider_cache_keys = cache_keys
        record.provider_snapshot = snapshot
        if record.provider_callback is None:
            record.provider_callback = _CanonicalProviderRetirement(self, record)
            record.provider_producer = record.producer
            record.provider_settled = False
            record.provider_result = None
        elif record.provider_producer is None and record.producer is not None:
            # A reversible global-close attempt may have bound the exact callback before the
            # coordinator aborted.  If the authority later enters its ordinary per-profile
            # retirement, attach that same callback to the newly admitted exact producer rather
            # than leaving a global-only ``None`` producer that can never retry.
            record.provider_producer = record.producer
        return True

    async def _retire_canonical_provider(self, record: _Record) -> bool:
        if record.provider_cache_keys is None or record.provider_snapshot is None:
            return False
        try:
            from gateway.run import _profile_runtime_scope
            scope = _profile_runtime_scope(
                Path(record.lease.profile_home), hydrate_secrets=False,
            )
        except (AttributeError, ImportError, TypeError, ValueError):
            scope = nullcontext()
        with scope:
            result = await retire_loaded_memory_providers(
                self.runner, record.provider_cache_keys,
                expected_snapshot=record.provider_snapshot,
            )
        return result is True

    def begin_hot_serve(self, name: str, home: str | Path) -> ProfileHotServeLease:
        """Admit one real hot-serve producer before its first await or side effect."""
        if type(name) is not str or not name:
            raise ValueError("profile_lifecycle_profile_name_missing")
        canonical_home = str(Path(home).resolve())
        if self._global_close_generation is not None:
            raise RuntimeError("profile_lifecycle_admission_closed")
        coordinator = self._coordinator()
        if coordinator.state != "open":
            raise RuntimeError("runtime_draining")
        if canonical_home in self._pending_hot_serves:
            raise RuntimeError("profile_lifecycle_hot_serve_conflict")
        current = self._current.get(canonical_home)
        if current is not None and current.state != "open":
            raise RuntimeError("profile_lifecycle_admission_closed")
        if self._registry_authority(canonical_home) is not None and current is None:
            raise RuntimeError("profile_lifecycle_registry_collision")
        physical_owner_epoch = self._physical_owner_epoch(canonical_home)
        if physical_owner_epoch is None:
            raise RuntimeError("foreign_custody")
        predecessor = self.last_removed_authority(canonical_home)
        predecessor_lease = getattr(
            getattr(predecessor, "_profile_lifecycle_lease", None), "lease_id", None,
        )
        producer = ProfileHotServeLease(
            profile_home=canonical_home,
            profile_name=name,
            process_instance=coordinator.process_instance,
            physical_owner_epoch=physical_owner_epoch,
            producer_id=uuid.uuid4().hex,
            predecessor_lease_id=predecessor_lease,
        )
        self._pending_hot_serves[canonical_home] = producer
        return producer

    def abort_hot_serve(self, producer: ProfileHotServeLease | None) -> bool:
        """Release one exact pending hot-serve admission after a failed build."""
        if producer is None:
            return True
        current = self._pending_hot_serves.get(getattr(producer, "profile_home", ""))
        if current is not producer:
            return False
        self._pending_hot_serves.pop(producer.profile_home, None)
        return True

    def begin_global_close(self, close_generation: int) -> bool:
        """Fence lifecycle ingress for one reversible global close generation."""
        if type(close_generation) is not int or close_generation < 1:
            return False
        if self._global_close_generation not in (None, close_generation):
            return False
        # A hot-serve task has authenticated the target profile's native lock but has
        # not yet built an authority record.  Treat that exact producer as live source
        # custody; the close caller must abort/retry instead of publishing zero work.
        if self._pending_hot_serves:
            return False
        records = tuple(self._current.values())
        if not records:
            return False
        validated = []
        for record in records:
            if record.state not in {"open", "closing"} or record.producer is not None:
                return False
            try:
                lease, graph = self._identity(record.authority)
            except (OSError, RuntimeError, TypeError, ValueError):
                return False
            if (
                lease.profile_id != record.lease.profile_id
                or lease.authority_epoch != record.lease.authority_epoch
                or lease.process_instance != record.lease.process_instance
                or lease.physical_owner_epoch != record.lease.physical_owner_epoch
                or graph is not record.graph
            ):
                return False
            captured = self._capture_provider_snapshot(record.authority)
            if captured is None:
                return False
            cache_keys, snapshot = captured
            if record.provider_callback is not None and record.provider_snapshot is not None:
                # A prior reversible close may already own this exact source callback.  Reuse its
                # identity witness for the next generation instead of binding a newly captured
                # tuple that would make the global callback look foreign to its own record.
                if not self._cache_snapshot_matches(self.runner, record.provider_snapshot):
                    return False
                cache_keys = record.provider_cache_keys or cache_keys
                snapshot = record.provider_snapshot
            validated.append((record, cache_keys, snapshot))
        self._global_close_generation = close_generation
        for record, cache_keys, snapshot in validated:
            record.state = "closing"
            record.close_generation = close_generation
            record.global_provider_cache_keys = cache_keys
            record.global_provider_snapshot = snapshot
            record.global_provider_prepared = not self._snapshot_may_have_provider(snapshot)
            # A live cached manager is a downstream owner even before its public retirement
            # callback is bound.  Keep the source UNKNOWN/pending until prepare_handoff has
            # joined accepted authority work and consumed the exact canonical snapshot.
            if record.provider_callback is None and self._snapshot_may_have_provider(snapshot):
                record.provider_settled = False
        return True

    def abort_global_close(self, close_generation: int) -> bool:
        if self._global_close_generation != close_generation:
            return False
        for record in self._current.values():
            if record.close_generation == close_generation and record.state == "closing":
                record.state = "open"
                record.close_generation = 0
                record.global_provider_cache_keys = None
                record.global_provider_snapshot = None
                record.global_provider_prepared = False
        self._global_close_generation = None
        return True

    def global_provider_agents(self, close_generation: int):
        """Return the exact cached agent identities captured for one global close generation."""
        if self._global_close_generation != close_generation:
            return None
        agents: dict[int, tuple[Any, Any, Any, Any]] = {}
        for record in tuple(self._current.values()):
            if (
                record.close_generation != close_generation
                or record.global_provider_snapshot is None
            ):
                return None
            try:
                _initial_keys, cached_agents = record.global_provider_snapshot
                rows = tuple(cached_agents)
            except Exception:  # health: allow BLE001 -- malformed snapshot retains custody
                return None
            for row in rows:
                if not isinstance(row, tuple) or len(row) != 4:
                    return None
                _key, _was_present, agent, manager = row
                if agent is not None and manager is not None:
                    if getattr(agent, "_memory_manager", None) is not manager:
                        return None
                    previous = agents.get(id(agent))
                    if previous is not None and (
                        previous[1] is not record.global_provider_snapshot
                        or previous[2] is not record.authority
                        or previous[3] is not manager
                    ):
                        return None
                    agents[id(agent)] = (
                        agent, record.global_provider_snapshot, record.authority, manager,
                    )
        return tuple(agents.values())

    def global_provider_snapshots_valid(self, close_generation: int) -> bool:
        """Revalidate every captured cache/manager identity before a global provider transition."""
        if self._global_close_generation != close_generation:
            return False
        for record in tuple(self._current.values()):
            if (
                record.close_generation != close_generation
                or record.global_provider_snapshot is None
                or not self._cache_snapshot_matches(self.runner, record.global_provider_snapshot)
            ):
                return False
        return True

    def mark_global_provider_prepared(self, close_generation: int, prepared: bool) -> bool:
        """Publish the source-owned manager preparation result for the current close generation."""
        if self._global_close_generation != close_generation:
            return False
        records = tuple(self._current.values())
        if not records or any(record.close_generation != close_generation for record in records):
            return False
        for record in records:
            record.global_provider_prepared = prepared
        return True

    def begin_retirement(self, authority: Any) -> ProfileLifecycleProducerLease | None:
        """Fence one profile's lifecycle producer before retirement side effects begin."""
        home = self._home(authority)
        record = self._current.get(home)
        if record is None:
            # Bare legacy runners have no coordinator/source registry and retain their historical
            # profile teardown. Managed runners register every authority during bootstrap.
            return None
        if record.authority is not authority:
            raise RuntimeError("profile_lifecycle_admission_closed")
        if record.state == "closing" and record.producer is not None:
            return record.producer
        if record.state != "open":
            raise RuntimeError("profile_lifecycle_admission_closed")
        lease, graph = self._identity(authority)
        if graph is not record.graph:
            raise RuntimeError("profile_lifecycle_identity_mismatch")
        if not self._same_identity(lease, record.lease):
            stable_identity = (
                lease.profile_id == record.lease.profile_id
                and lease.profile_home == record.lease.profile_home
                and lease.authority_epoch == record.lease.authority_epoch
                and lease.process_instance == record.lease.process_instance
            )
            if not stable_identity:
                raise RuntimeError("profile_lifecycle_identity_mismatch")
            # The physical profile lock may be explicitly reacquired by this same authority after
            # an external test/deleter race.  Preserve the source lease id while recording the new
            # lock incarnation; any coordinator child lease still carries its own stale-owner
            # refusal and cannot be silently adopted here.
            record.lease = ProfileLifecycleLease(
                profile_id=record.lease.profile_id,
                profile_home=record.lease.profile_home,
                authority_epoch=record.lease.authority_epoch,
                process_instance=record.lease.process_instance,
                physical_owner_epoch=lease.physical_owner_epoch,
                lease_id=record.lease.lease_id,
            )
            # Keep the authority's source backpointer on the exact immutable lease that the
            # custody record and the producer will carry.  A stale attribute here would let a
            # child hold the previous physical lock incarnation while the source finalizer
            # reports the newly rebound one.
            authority._profile_lifecycle_lease = record.lease
            record.ownership_rebinds += 1
        if getattr(authority, "_profile_lifecycle_lease", None) is not record.lease:
            # Do not adopt a foreign/stale source backpointer merely because the public fields
            # happen to match.  The producer must carry the exact lease object owned by this record.
            raise RuntimeError("profile_lifecycle_lease_mismatch")
        status = self._coordinator().status()
        generation = int(status.get("close_generation") or 0)
        if status.get("state") != "open":
            raise RuntimeError("runtime_draining")
        producer = ProfileLifecycleProducerLease(
            lease=record.lease, producer_id=uuid.uuid4().hex, close_generation=generation,
        )
        record.state = "closing"
        record.close_generation = generation
        record.producer = producer
        return producer

    def complete_retirement(
        self, authority: Any, producer: ProfileLifecycleProducerLease | None,
        *, workers_stopped: bool, rooms_stopped: bool, source_finalizers_settled: bool,
    ) -> ProfileLifecycleReceipt:
        """Consume the producer only after every existing finalizer has a real success result."""
        record = self._record_for(authority)
        if record is None:
            raise RuntimeError("profile_lifecycle_lease_missing")
        # A completed receipt is valid only for the exact producer admitted by this source.  The
        # finalizer flags cannot substitute for that identity: ``None`` or a copied dataclass could
        # otherwise consume an active source lease while the original producer is still live.
        if (
            producer is None
            or record.producer is not producer
            or producer.lease is not record.lease
            or getattr(authority, "_profile_lifecycle_lease", None) is not record.lease
        ):
            raise RuntimeError("profile_lifecycle_lease_mismatch")
        if record.state != "closing":
            raise RuntimeError("profile_lifecycle_admission_closed")
        if not all((workers_stopped, rooms_stopped, source_finalizers_settled)):
            raise RuntimeError("profile_lifecycle_finalizer_incomplete")
        # The native profile lock may be released and reacquired while the accepted producer is
        # draining.  Re-read the live identity before consuming the producer; a new physical
        # incarnation is foreign custody, so keep the original producer and receipt unresolved.
        try:
            live_lease, live_graph = self._identity(authority)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise RuntimeError("profile_lifecycle_identity_mismatch") from exc
        if live_graph is not record.graph or not self._same_identity(live_lease, record.lease):
            raise RuntimeError("profile_lifecycle_identity_mismatch")
        # Source workers have settled at this point.  Capture the canonical cache/agent/manager
        # graph before consuming the producer, so a failed snapshot leaves the same producer
        # retryable and cannot turn ``None`` into permission to release the profile.
        if not self._prepare_provider_retirement(record):
            raise RuntimeError("profile_lifecycle_provider_snapshot_missing")
        try:
            live_lease, live_graph = self._identity(authority)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise RuntimeError("profile_lifecycle_identity_mismatch") from exc
        if live_graph is not record.graph or not self._same_identity(live_lease, record.lease):
            raise RuntimeError("profile_lifecycle_identity_mismatch")
        receipt = ProfileLifecycleReceipt(
            schema="hermes.gateway.profile-lifecycle-receipt.v1",
            profile_id=record.lease.profile_id, profile_home=record.lease.profile_home,
            authority_epoch=record.lease.authority_epoch,
            process_instance=record.lease.process_instance,
            close_generation=record.close_generation,
            workers_stopped=True, rooms_stopped=True, source_finalizers_settled=True,
        )
        record.producer = None
        record.receipt = receipt
        record.state = "retired_ready"
        return receipt

    def bind_provider_retirement(
        self, authority: Any, producer: ProfileLifecycleProducerLease | None, callback: Any,
        *, provider_snapshot: Any = None,
    ) -> bool:
        """Bind one exact downstream provider finalizer to the active source producer.

        The profile source owns this binding.  A caller cannot replace it with a new callback or
        copied producer after the source receipt has been accepted; retries must use the same
        authority-owned callback and cache/manager snapshot.
        """
        record = self._current.get(self._home(authority))
        if (
            record is None
            or record.authority is not authority
            or producer is None
            or record.producer is not producer
            or not callable(callback)
        ):
            return False
        if record.provider_callback is not None:
            return (
                record.provider_callback is callback
                and record.provider_producer is producer
                and record.provider_snapshot is provider_snapshot
            )
        record.provider_callback = callback
        record.provider_producer = producer
        record.provider_snapshot = provider_snapshot
        record.provider_settled = False
        return True

    def provider_retirement_pending(self, authority: Any) -> bool:
        record = self._record_for(authority)
        # A global close deliberately captures a live provider graph before its canonical
        # callback is bound.  That callback-less interval is still pending/UNKNOWN; exposing it
        # as settled would let a caller seal the owner before the awaited handoff boundary.
        return bool(record is not None and not record.provider_settled)

    def provider_retirement_settled(self, authority: Any) -> bool:
        record = self._record_for(authority)
        # The absence of a callback is not proof of settlement while global admission has marked
        # the captured provider graph pending.  An unregistered authority remains the ordinary
        # vacuous-success case; a registered pending record stays UNKNOWN until its exact receipt.
        return True if record is None else record.provider_settled

    async def settle_provider_retirement(self, authority: Any) -> bool:
        """Consume the exact bound provider receipt, once, before source release."""
        record = self._record_for(authority)
        if record is None:
            return True
        if record.provider_callback is None and (
            not record.provider_settled
            and self._global_close_generation == record.close_generation
            and record.global_provider_snapshot is not None
            and record.global_provider_cache_keys is not None
        ):
            # Global close only snapshots the provider graph during admission.  Bind the stable
            # source-owned callback at the awaited prepare boundary, after accepted authority work
            # has joined; begin_global_close must remain reversible and side-effect free.
            if not self._prepare_provider_retirement(
                record,
                captured=(record.global_provider_cache_keys, record.global_provider_snapshot),
            ):
                return False
        if record.provider_callback is None:
            return True
        if record.provider_settled:
            return True
        callback = record.provider_callback
        producer = record.provider_producer
        global_binding = (
            producer is None
            and self._global_close_generation == record.close_generation
            and record.global_provider_snapshot is record.provider_snapshot
            and record.global_provider_cache_keys == record.provider_cache_keys
        )
        if (producer is None and not global_binding) or not callable(callback):
            return False
        try:
            if record.provider_snapshot is None:
                result = callback(authority)
            else:
                # A callback bound with an identity snapshot must consume that exact snapshot;
                # silently falling back to a fresh cache lookup could retire a replacement under
                # the same key after the original owner returned UNKNOWN.
                result = callback(authority, expected_snapshot=record.provider_snapshot)
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            logger.error(
                "Profile %s provider retirement did not settle; ownership retained",
                getattr(authority, "profile_id", "<unknown>"),
                exc_info=True,
            )
            return False
        if result is not True:
            return False
        current = self._record_for(authority)
        if (
            current is not record
            or current.provider_callback is not callback
            or current.provider_producer is not producer
        ):
            return False
        record.provider_result = result
        record.provider_settled = True
        return True

    def record_removed(self, authority: Any) -> bool:
        """Capture exact registry removal after the authority finalizer receipt."""
        home = self._home(authority)
        record = self._current.get(home)
        if (
            record is None
            or record.authority is not authority
            or record.state != "retired_ready"
            or not self.provider_retirement_settled(authority)
        ):
            return False
        if self._registry_authority(home) is authority:
            return False
        # The registry removal is a separate await boundary from provider settlement and physical
        # lock release.  Re-read the native owner after that boundary so a replacement lock cannot
        # be certified by the predecessor's receipt.
        if not self._physical_owner_matches(record):
            return False
        record.transition = {
            "kind": "removed", "profile_home": home,
            "authority_epoch": record.lease.authority_epoch,
            "same_authority": True,
        }
        record.state = "terminal"
        self._current.pop(home, None)
        self._history.append(record)
        return True

    def record_successor(self, old_authority: Any, successor: Any) -> bool:
        """Bind a new authority only after the old identity was removed and the new epoch is live."""
        old = next((item for item in reversed(self._history) if item.authority is old_authority), None)
        if (
            old is None
            or old.transition is None
            or old.transition.get("kind") != "removed"
            or not old.provider_settled
        ):
            return False
        home = old.lease.profile_home
        if self._registry_authority(home) is not successor:
            return False
        try:
            lease, graph = self._identity(successor)
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        if (
            lease.profile_id != old.lease.profile_id
            or lease.authority_epoch <= old.lease.authority_epoch
            or lease.process_instance != old.lease.process_instance
        ):
            return False
        old.transition = {
            "kind": "successor", "profile_home": home,
            "old_authority_epoch": old.lease.authority_epoch,
            "new_authority_epoch": lease.authority_epoch,
            "same_process_instance": lease.process_instance == old.lease.process_instance,
        }
        record = _Record(
            lease=lease, authority=successor, graph=graph,
            registered_witness=uuid.uuid4().hex,
            predecessor_lease_id=old.lease.lease_id,
        )
        self._current[home] = record
        successor._profile_lifecycle_lease = lease
        successor._profile_lifecycle_source = self
        return True

    def last_removed_authority(self, home: str):
        """Return the exact predecessor whose registry slot was just removed, if any."""
        for record in reversed(self._history):
            if (
                record.lease.profile_home == home
                and record.transition is not None
                and record.transition.get("kind") == "removed"
            ):
                return record.authority
        return None

    def _record_for(self, authority: Any) -> _Record | None:
        record = self._current.get(self._home(authority))
        if record is not None and record.authority is authority:
            return record
        return next((item for item in reversed(self._history) if item.authority is authority), None)

    def owner_evidence(self, *, process_instance: str, authority_epoch: int,
                       close_generation: int, owner_id: str) -> dict[str, Any] | None:
        """Read all current source records without manufacturing readiness from empty maps."""
        if (
            self._global_close_generation != close_generation
            or not owner_id
            or self._pending_hot_serves
        ):
            return None
        profiles = []
        for record in tuple(self._current.values()):
            if record.state not in {"closing", "retired_ready"} or record.close_generation != close_generation:
                return None
            try:
                lease, graph = self._identity(record.authority)
            except (OSError, RuntimeError, TypeError, ValueError):
                return None
            if (
                graph is not record.graph
                or lease.process_instance != process_instance
                or not self._same_identity(lease, record.lease)
                or record.producer is not None
                or not record.global_provider_prepared
                or record.global_provider_snapshot is None
                or not self._cache_snapshot_matches(
                    self.runner, record.global_provider_snapshot,
                )
            ):
                return None
            source_zero_work_witness = (
                record.producer is None
                and record.global_provider_prepared
                and record.global_provider_snapshot is not None
            )
            profiles.append({
                "profile_id": lease.profile_id,
                "profile_home": lease.profile_home,
                "authority_epoch": lease.authority_epoch,
                "process_instance": lease.process_instance,
                "physical_owner_epoch": lease.physical_owner_epoch,
                "close_generation": close_generation,
                "registered_witness": record.registered_witness,
                "zero_work_witness": source_zero_work_witness,
                "source_zero_work_witness": source_zero_work_witness,
                "provider_snapshot_witness": True,
                "registry_transition": record.transition or {
                    "kind": "retained", "same_authority": True,
                },
                "ownership_rebinds": record.ownership_rebinds,
            })
        if not profiles:
            return None
        return {
            "source": SOURCE,
            "process_instance": process_instance,
            "authority_epoch": authority_epoch,
            "close_generation": close_generation,
            "owner_id": owner_id,
            "observed": True,
            "cross_process_proof": True,
            "profiles": tuple(profiles),
            "profile_ids": tuple(item["profile_id"] for item in profiles),
            "profile_homes": tuple(item["profile_home"] for item in profiles),
            "profile_epochs": tuple(
                (item["profile_id"], item["authority_epoch"]) for item in profiles
            ),
            "native_handle_retained": True,
            "os_lock_owner": True,
        }


def custody_for(runner: Any) -> ProfileLifecycleCustody:
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody) or custody.runner is not runner:
        custody = ProfileLifecycleCustody(runner)
        runner._profile_lifecycle_custody = custody
    return custody


def canonical_profile_home(home: str | Path) -> Path:
    """Resolve a profile home in the synchronous source-admission boundary."""
    return Path(home).resolve()


def finish_profile_authority_retirement(authority: Any) -> bool:
    """Release the exact source binding after downstream profile owners settle."""
    if not getattr(authority, "_profile_lifecycle_ready", False):
        return False
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if isinstance(custody, ProfileLifecycleCustody):
        if not custody.provider_retirement_settled(authority):
            return False
        record = custody._record_for(authority)
        if record is None:
            return False
        try:
            live_lease, live_graph = custody._identity(authority)
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        if live_graph is not record.graph or not custody._same_identity(live_lease, record.lease):
            return False
    from gateway.session_cron import unbind_owner
    unbind_owner(authority)
    authority.retired = True
    authority._profile_lifecycle_ready = False
    return True


def profile_lifecycle_ready_result(authority: Any, defer_release: bool):
    """Resume a completed source finalizer without repeating its side effects."""
    if not getattr(authority, "_profile_lifecycle_ready", False):
        return None
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if isinstance(custody, ProfileLifecycleCustody) and not custody.provider_retirement_settled(authority):
        # The async owner retry must consume the exact bound provider callback first.  A synchronous
        # ready flag is only the source receipt and cannot authorize release on its own.
        return None
    if defer_release:
        return True
    return finish_profile_authority_retirement(authority)


def register_profile_lifecycle_source(
    runner: Any, authority: Any, *, hot_serve: ProfileHotServeLease | None = None,
) -> ProfileLifecycleLease:
    return custody_for(runner).register(authority, hot_serve=hot_serve)


def begin_profile_hot_serve(runner: Any, name: str, home: str | Path):
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody):
        return None
    return custody.begin_hot_serve(name, home)


def abort_profile_hot_serve(runner: Any, producer: ProfileHotServeLease | None) -> bool:
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return producer is None or (
        isinstance(custody, ProfileLifecycleCustody) and custody.abort_hot_serve(producer)
    )


def begin_profile_lifecycle_close(runner: Any, close_generation: int) -> bool:
    return custody_for(runner).begin_global_close(close_generation)


def abort_profile_lifecycle_close(runner: Any, close_generation: int) -> bool:
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return isinstance(custody, ProfileLifecycleCustody) and custody.abort_global_close(close_generation)


def begin_profile_retirement(authority: Any) -> ProfileLifecycleProducerLease | None:
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return None if not isinstance(custody, ProfileLifecycleCustody) else custody.begin_retirement(authority)


def bind_profile_provider_retirement(
    authority: Any, producer: ProfileLifecycleProducerLease | None, callback: Any,
) -> bool:
    """Attach the exact loaded-provider finalizer to this authority's active producer."""
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody):
        return True
    return custody.bind_provider_retirement(
        authority, producer, callback,
        provider_snapshot=getattr(callback, "_provider_snapshot", None),
    )


async def settle_profile_provider_retirement(authority: Any) -> bool:
    """Consume the authority-bound provider receipt before any source release or successor."""
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody):
        return True
    return await custody.settle_provider_retirement(authority)


def complete_profile_retirement(authority: Any, producer: ProfileLifecycleProducerLease | None,
                                *, workers_stopped: bool, rooms_stopped: bool,
                                source_finalizers_settled: bool):
    runner = getattr(authority, "runner", None)
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody):
        return None
    return custody.complete_retirement(
        authority, producer, workers_stopped=workers_stopped,
        rooms_stopped=rooms_stopped, source_finalizers_settled=source_finalizers_settled,
    )


def record_profile_removed(runner: Any, authority: Any) -> bool:
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return isinstance(custody, ProfileLifecycleCustody) and custody.record_removed(authority)


def record_profile_successor(runner: Any, old_authority: Any, successor: Any) -> bool:
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return isinstance(custody, ProfileLifecycleCustody) and custody.record_successor(old_authority, successor)


def last_removed_profile_authority(runner: Any, home: str):
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    return None if not isinstance(custody, ProfileLifecycleCustody) else custody.last_removed_authority(home)


def profile_lifecycle_owner_evidence(runner: Any, *, process_instance: str,
                                     authority_epoch: int, close_generation: int,
                                     owner_id: str):
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    if not isinstance(custody, ProfileLifecycleCustody):
        return None
    return custody.owner_evidence(
        process_instance=process_instance, authority_epoch=authority_epoch,
        close_generation=close_generation, owner_id=owner_id,
    )


def seal_profile_lifecycle_owner(runner: Any, coordinator: Any, receipt: Any,
                                 descriptor: dict[str, Any]) -> None:
    """Seal the I18 owner from the live profile registry or retain it UNKNOWN."""
    try:
        evidence = profile_lifecycle_owner_evidence(
            runner, process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{descriptor.get("instance_id", receipt.process_instance)}:I18',
        )
        if evidence is None:
            coordinator.mark_owner_unknown(OWNER_CLASS, 'missing_source_hook')
            return
        refusal = coordinator.seal_owner(OWNER_CLASS, evidence)
        if refusal is not None:
            coordinator.mark_owner_unknown(OWNER_CLASS, getattr(refusal, 'code', 'read_failed'))
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        coordinator.mark_owner_unknown(OWNER_CLASS, 'read_failed')


def complete_profile_lifecycle_receipt(authority: Any, producer: ProfileLifecycleProducerLease | None,
                                       *, workers_stopped: bool, rooms_stopped: bool) -> bool:
    """Consume the source finalizer receipt, retaining the authority on any ambiguity."""
    try:
        complete_profile_retirement(
            authority, producer, workers_stopped=workers_stopped,
            rooms_stopped=rooms_stopped, source_finalizers_settled=True,
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        return False
    return True


def close_profile_mcp_ingress(authority: Any) -> bool:
    """Close the profile MCP ingress before its authority finalizers run."""
    mcp_factory = getattr(authority, '_mcp_source_factory', None)
    if mcp_factory is None or not getattr(mcp_factory, '_config_seen', False):
        return True
    try:
        coordinator = getattr(authority, '_preservation_coordinator', lambda: None)()
        coordinator = coordinator or getattr(authority.runner, 'runtime_custody', None)
        status = coordinator.status() if coordinator is not None else {}
        closed = mcp_factory.close_ingress(status.get('close_generation') or 0)
        if getattr(closed, 'outcome', None) == 'unknown':
            logger.error('Profile %s MCP source ingress is UNKNOWN; ownership retained',
                         authority.profile_id)
            return False
    except Exception:  # health: allow BLE001 -- an opaque source close cannot prove absence
        logger.error('Profile %s MCP source ingress did not close; ownership retained',
                     authority.profile_id, exc_info=True)
        return False
    return True


def snapshot_loaded_memory_provider_identities(runner: Any, cache_keys: tuple[str, ...]):
    """Capture the live cache/agent/manager identities before a retirement callback is bound.

    A provider callback can be retried after its first receipt is UNKNOWN.  Its later invocation
    must still refer to the agent that owned the original authority, rather than snapshotting a
    replacement which happened to reuse the same cache key.
    """
    try:
        from gateway.run_agent_cache import _first_agent
    except ImportError:
        return None
    cache = getattr(runner, "_agent_cache", None)
    if cache is None:
        return frozenset(), ()
    if not isinstance(cache, dict):
        return None
    cache_lock = getattr(runner, "_agent_cache_lock", None)

    def capture():
        initial_cache_keys = frozenset(cache)
        records = []
        for key in cache_keys:
            entry = cache.get(key, _MISSING_PROVIDER_CACHE_ENTRY)
            if entry is _MISSING_PROVIDER_CACHE_ENTRY:
                records.append((key, False, None, None))
                continue
            agent = _first_agent(entry)
            records.append((key, True, agent, getattr(agent, "_memory_manager", None)))
        return initial_cache_keys, tuple(records)

    try:
        if cache_lock is None:
            return capture()
        with cache_lock:
            return capture()
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        # An opaque cache snapshot retains custody; the boundary is limited to cache/lock and
        # identity lookup failures rather than hiding an unrelated programming error.
        return None


_MISSING_PROVIDER_CACHE_ENTRY = object()


async def retire_loaded_memory_providers(
    runner: Any, cache_keys: tuple[str, ...], *, expected_snapshot=None,
) -> bool:
    """Hard-retire loaded providers before profile cache/store/lock removal.

    The current Core may not yet carry the provider-side receiving API.  A loaded manager without
    that operation is an explicit refusal and keeps the exact profile authority/cache in place.
    """
    try:
        from agent.memory_retirement import MemoryRetirementReport
        from gateway.run_agent_cache import _first_agent
    except ImportError:
        # The provider-side receiving contract is optional on older Core snapshots.  A loaded
        # provider must remain owned when the typed report cannot be imported.
        return False

    cache = getattr(runner, "_agent_cache", None)
    if cache is None:
        return True
    if not isinstance(cache, dict):
        # A cache with an unknown shape may still contain a live provider.  Refuse rather than
        # turning an unenumerable cache into permission to release the profile authority.
        return False
    cache_lock = getattr(runner, "_agent_cache_lock", None)
    missing = _MISSING_PROVIDER_CACHE_ENTRY

    def _cache_call(callback):
        if cache_lock is None:
            return callback()
        with cache_lock:
            return callback()

    try:
        # Retain the exact cache key/agent/manager tuple.  The caller may evict the old entry while
        # the provider operation is off-loop, but a replacement or newly-created cache key must
        # refuse the profile rather than allowing the later LRU sweep to evict the replacement.
        if expected_snapshot is None:
            snapshot = snapshot_loaded_memory_provider_identities(runner, cache_keys)
            if snapshot is None:
                return False
            initial_cache_keys, cached_agents = snapshot
        else:
            initial_cache_keys, cached_agents = expected_snapshot
    except Exception:  # health: allow BLE001 -- an opaque cache snapshot retains custody
        return False

    def _validate_locked():
        # Deletion of an exact captured entry is allowed: the local references keep the old agent
        # and manager alive until the hard operation settles.  Any addition is ambiguous because
        # it may be a replacement/new session that the caller must not evict on our old receipt.
        if not frozenset(cache).issubset(initial_cache_keys):
            return False
        for key, _was_present, captured_agent, captured_manager in cached_agents:
            entry = cache.get(key, missing)
            if entry is missing:
                if captured_agent is not None and getattr(captured_agent, "_memory_manager", None) is not captured_manager:
                    return False
                continue
            current_agent = _first_agent(entry)
            if captured_agent is None:
                if current_agent is not None:
                    return False
                continue
            if current_agent is not captured_agent:
                return False
            if getattr(captured_agent, "_memory_manager", None) is not captured_manager:
                return False
        return True

    def _validate_cache_identity():
        try:
            return _cache_call(_validate_locked)
        except Exception:  # health: allow BLE001 -- an opaque identity read retains custody
            return False

    def _operation_for(captured_agent, captured_manager):
        def resolve_locked():
            if not _validate_locked():
                return False, None
            operation = getattr(captured_agent, "retire_memory_provider", None)
            if not callable(operation):
                operation = getattr(captured_manager, "retire_hard", None)
            return callable(operation), operation

        try:
            return _cache_call(resolve_locked)
        except Exception:  # health: allow BLE001 -- an opaque API lookup retains custody
            return False, None

    seen_managers: set[int] = set()
    for _key, _was_present, agent, manager in cached_agents:
        if not _validate_cache_identity():
            return False
        if agent is None or manager is None or id(manager) in seen_managers:
            continue
        try:
            providers = getattr(manager, "providers", None)
            providers = providers() if callable(providers) else providers
        except Exception:  # health: allow BLE001 -- an opaque provider query retains custody
            return False
        if not _validate_cache_identity():
            return False
        if not providers:
            continue
        seen_managers.add(id(manager))
        valid, operation = _operation_for(agent, manager)
        if not valid:
            return False
        try:
            if inspect.iscoroutinefunction(operation):
                result = await operation()
            else:
                # Provider close/flush can block on SQLite and worker joins.  Never run a
                # synchronous provider operation on the profile reconcile event loop.
                result = await asyncio.to_thread(operation)
            if inspect.isawaitable(result):
                result = await result
        except Exception:  # health: allow BLE001 -- an opaque provider close retains custody
            return False
        if not isinstance(result, MemoryRetirementReport) or result.status != "released":
            return False
        # Re-read the cache and the captured agent's live manager after the exact operation.  A
        # replacement that arrived while the old manager was blocked is not covered by its receipt.
        if not _validate_cache_identity():
            return False
    return _validate_cache_identity()


def refence_preservation_lifecycle_sources(runner: Any, close_generation: int) -> bool:
    """Re-fence both source graphs after an incomplete coordinated abort."""
    from gateway.lifecycle_custody import begin_preservation_lifecycle_close

    foreign_ok = begin_preservation_lifecycle_close(runner, close_generation) is None
    profile_ok = True
    if getattr(runner, "_profile_lifecycle_custody", None) is not None:
        profile_ok = begin_profile_lifecycle_close(runner, close_generation)
    return foreign_ok and profile_ok


def resume_preservation_lifecycle_sources(runner: Any, close_generation: int) -> bool:
    """Restore both actual source fences before opening the coordinator."""
    from gateway.lifecycle_custody import abort_preservation_lifecycle_close

    profile_active = getattr(runner, "_profile_lifecycle_custody", None) is not None
    if profile_active and not abort_profile_lifecycle_close(runner, close_generation):
        return False
    if not abort_preservation_lifecycle_close(runner, close_generation):
        if profile_active:
            begin_profile_lifecycle_close(runner, close_generation)
        return False
    return True
