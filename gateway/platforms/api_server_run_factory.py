"""Source-owned factory proof for an idle or settled API run adapter.

The existing transport registration fences ingress and the coordinator owns all run
leases. This module retains their actual factory/index/profile identities; it does
not create another work ledger or infer readiness from a never-set activity bit.
"""
from __future__ import annotations

from dataclasses import dataclass
import importlib
from pathlib import Path
import sqlite3
from typing import Any


_PRODUCERS = (
    ("gateway.platforms.api_server_run_custody", "ApiRunCustody.reserve"),
    ("gateway.platforms.api_server_run_custody", "reserve_run"),
    ("gateway.platforms.api_server_run_custody", "reserve_legacy"),
    ("gateway.platforms.api_server_run_dispatch", "_reserve"),
    ("gateway.platforms.api_server_run_execution", "begin_legacy_direct_turn"),
    ("gateway.platforms.api_server_run_execution", "submit_legacy_stream_worker"),
    ("gateway.platforms.api_server_run_execution", "bind_legacy_worker"),
    ("gateway.platforms.api_server_run_execution", "finalize_api_run"),
    ("gateway.platforms.api_server_runs", "_RunLaunch"),
    ("gateway.platforms.api_server_runs", "_RunLaunch.__post_init__"),
    ("gateway.platforms.api_server", "_reserve_pending_api_work"),
)
_INDEXES = (
    "_run_custody", "_run_launches", "_active_run_tasks", "_active_run_agents",
    "_run_streams", "_run_approval_sessions", "_run_admission_aliases",
    "_run_idempotency_ids", "_stopping_run_ids", "_run_statuses", "_run_owners",
    "_shutdown_interruptible_agents",
)
_LIVE_INDEXES = (
    "_active_run_tasks", "_active_run_agents", "_run_streams",
    "_run_approval_sessions", "_run_admission_aliases", "_run_idempotency_ids",
    "_stopping_run_ids", "_shutdown_interruptible_agents",
)


def _producer(module: str, name: str):
    value = importlib.import_module(module)
    for part in name.split("."):
        value = getattr(value, part)
    return getattr(value, "__func__", value)


def _physical_home(value: str | Path) -> Path:
    from gateway.runtime_ownership import canonical_home

    return canonical_home(Path(value))


@dataclass(frozen=True)
class _ProfileFactory:
    authority: Any
    db: Any
    home: str
    epoch: int
    physical_owner_epoch: str


class ApiRunFactory:
    """The exact adapter factory joined to the existing source-close gate."""

    def __init__(self, adapter, runner, coordinator, registry, registration):
        self.adapter = adapter
        self.runner = runner
        self.coordinator = coordinator
        self.registry = registry
        self.registration = registration
        self.authorities = getattr(runner, "session_authorities", None)
        self.indexes = {name: getattr(adapter, name, None) for name in _INDEXES}
        self.store = getattr(adapter, "_run_idempotency_store", None)
        self.producers = tuple(_producer(*item) for item in _PRODUCERS)
        self.profiles: tuple[_ProfileFactory, ...] = ()
        self.invalid = False
        self.refresh_profiles()

    def refresh_profiles(self) -> bool:
        """Accept only source-published additive profiles while admission is OPEN."""
        from gateway.session_authorities import all_authorities

        if self.coordinator.status().get("state") != "open":
            return False
        current = tuple(all_authorities(self.runner))
        old = {item.home: item for item in self.profiles}
        result = []
        try:
            for authority in current:
                home = getattr(authority, "profile_id", None)
                epoch = getattr(authority, "epoch", None)
                db = getattr(authority, "db", None)
                observation = self.coordinator.profile_scope_observation(
                    home, epoch, require_serving=True, allow_retiring=False,
                )
                if not observation.ok or not observation.physical_owner_epoch or db is None:
                    self.invalid = True
                    return False
                prior = old.get(home)
                if prior is not None and (
                    prior.authority is not authority or prior.db is not db
                    or prior.epoch != epoch
                    or prior.physical_owner_epoch != observation.physical_owner_epoch
                ):
                    self.invalid = True
                    return False
                result.append(prior or _ProfileFactory(
                    authority, db, home, epoch, observation.physical_owner_epoch,
                ))
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            self.invalid = True
            return False
        if not result or not set(old).issubset({item.home for item in result}):
            self.invalid = True
            return False
        self.profiles = tuple(result)
        return True

    def _current_profiles(self):
        from gateway.session_authorities import all_authorities

        current = tuple(all_authorities(self.runner))
        if len(current) != len(self.profiles):
            return None
        rows = []
        for retained in self.profiles:
            authority = retained.authority
            if not any(item is authority for item in current):
                return None
            if (
                getattr(authority, "db", None) is not retained.db
                or getattr(authority, "profile_id", None) != retained.home
                or getattr(authority, "epoch", None) != retained.epoch
                or _physical_home(Path(retained.db.db_path).parent) != _physical_home(retained.home)
                or not self.coordinator.profile_scope_valid(
                    retained.home, retained.epoch, require_serving=True,
                    allow_retiring=False, physical_owner_epoch=retained.physical_owner_epoch,
                )
            ):
                return None
            with retained.db._read_ctx() as connection:
                row = connection.execute(
                    "SELECT epoch,instance_id FROM runtime_epoch WHERE singleton=1"
                ).fetchone()
            if row is None or tuple(row) != (retained.epoch, self.coordinator.process_instance):
                return None
            rows.append({
                "profile_id": retained.home, "authority_epoch": retained.epoch,
                "physical_owner_epoch": retained.physical_owner_epoch,
                "epoch_readback": True, "cross_process_proof": True,
            })
        return tuple(rows)

    def evidence(self, *, process_instance, authority_epoch, close_generation, owner_id):
        """Authenticate current factories, source fence, stores and native profile owners."""
        from gateway.preservation_source_close import source_owner_ready

        try:
            state = self.coordinator.status()
            if (
                self.invalid or getattr(self.adapter, "gateway_runner", None) is not self.runner
                or getattr(self.runner, "runtime_custody", None) is not self.coordinator
                or getattr(self.runner, "session_authorities", None) is not self.authorities
                or getattr(self.runner, "_preservation_source_close", None) is not self.registry
                or self.registry._registrations.get(id(self.adapter)) is not self.registration
                or not self.registry._registration_is_current(self.registration)
                or self.registration.close_generation != close_generation
                or state.get("state") != "closing"
                or state.get("close_generation") != close_generation
                or "I03_api_run_direct" in state.get("active_owner_classes", ())
                or "I03_api_run_direct" in state.get("unknown_owner_classes", ())
                or process_instance != self.coordinator.process_instance
                or authority_epoch != self.coordinator.authority_epoch
                or not isinstance(owner_id, str) or not owner_id
                or not source_owner_ready(self.runner, close_generation)
                or any(_producer(*item) is not expected for item, expected in zip(_PRODUCERS, self.producers))
                or any(getattr(self.adapter, name, None) is not value for name, value in self.indexes.items())
                or any(not isinstance(value, (dict, set)) for value in self.indexes.values())
                or getattr(self.adapter, "_run_idempotency_store", None) is not self.store
                or self.store is None
                or getattr(self.adapter, "_pending_agent_requests", None) != 0
                or getattr(self.adapter, "_inflight_agent_runs", None) != 0
            ):
                return None
            profiles = self._current_profiles()
            if not profiles:
                return None
            launches = self.indexes["_run_launches"]
            custody = self.indexes["_run_custody"]
            if set(launches) != set(custody):
                return None
            if not launches and any(self.indexes[name] for name in _INDEXES):
                return None
            if launches and any(
                set(self.indexes[name]) - set(launches)
                for name in _INDEXES if name != "_shutdown_interruptible_agents"
            ):
                return None
            if self.indexes["_shutdown_interruptible_agents"]:
                return None
            witnesses = []
            from gateway.platforms.api_server_run_witness import read_run_witness
            for run_id, launch in tuple(launches.items()):
                owner = custody[run_id]
                if (
                    getattr(launch, "owner", None) is not self.adapter
                    or getattr(launch, "custody", None) is not owner
                    or getattr(owner, "coordinator", None) is not self.coordinator
                    or getattr(owner, "released", False) is not True
                    or getattr(owner, "unknown_reason", None)
                    or not self.coordinator.profile_scope_valid(
                        owner.profile_id, owner.authority_epoch,
                        require_serving=True, allow_retiring=False,
                        physical_owner_epoch=owner.handle.physical_owner_epoch,
                    )
                ):
                    return None
                witness, refusal = read_run_witness(self.adapter, owner)
                if refusal is not None:
                    return None
                witnesses.append(witness)
            return {
                "process_instance": process_instance, "authority_epoch": authority_epoch,
                "close_generation": close_generation, "owner_id": owner_id,
                "observed": True, "cross_process_proof": True,
                "factory_kind": "api_run_registered_source_v1",
                "source_registration_generation": self.registration.generation,
                "profiles": profiles, "run_ids": tuple(sorted(launches)),
                "witnesses": tuple(witnesses),
                "process_receipts": tuple(item["process_receipt"] for item in witnesses),
            }
        except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
                TypeError, ValueError, sqlite3.Error):
            return None


def register_api_run_factory(runner, adapter):
    """Bind only from actual runtime bootstrap/transport callback installation."""
    from gateway.platforms.api_server import APIServerAdapter

    if not isinstance(adapter, APIServerAdapter):
        return None
    coordinator = getattr(runner, "runtime_custody", None)
    registry = getattr(runner, "_preservation_source_close", None)
    registration = getattr(registry, "_registrations", {}).get(id(adapter))
    if coordinator is None or registration is None:
        return None
    existing = getattr(adapter, "_api_run_factory", None)
    if existing is not None:
        if (
            not isinstance(existing, ApiRunFactory) or existing.runner is not runner
            or existing.coordinator is not coordinator or existing.registry is not registry
            or existing.registration is not registration
        ):
            return None
        existing.refresh_profiles()
        return existing
    factory = ApiRunFactory(adapter, runner, coordinator, registry, registration)
    adapter._api_run_factory = factory
    return factory


def refresh_api_run_factories(runner):
    from gateway.run_runtime import _api_server_adapters

    for adapter in _api_server_adapters(runner):
        register_api_run_factory(runner, adapter)


def preservation_owner_evidence(adapters: Any, runner: Any, *, process_instance: str,
                                 authority_epoch: int, close_generation: int, owner_id: str):
    """Return I03 quiescence only from each adapter's retained source-owned run witnesses."""
    if not isinstance(adapters, (list, tuple)) or not adapters:
        return None
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    if (descriptor.get("instance_id") != process_instance
            or descriptor.get("authority_epoch") != authority_epoch):
        return None
    from pathlib import Path
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authorities import all_authorities
    from gateway.platforms.api_server_run_witness import owner_evidence
    authorities = all_authorities(runner)
    homes = [getattr(authority, "profile_id", None) for authority in authorities]
    if (not authorities or any(not isinstance(home, str) or not home for home in homes)
            or any(not process_ownership.owns(Path(home)) for home in homes)):
        return None
    adapter_evidence = []
    for adapter in adapters:
        evidence = owner_evidence(
            adapter, process_instance=process_instance, authority_epoch=authority_epoch,
            close_generation=close_generation, owner_id=owner_id)
        if evidence is None:
            return None
        adapter_evidence.append(evidence)
    witnesses = tuple(
        witness for evidence in adapter_evidence for witness in evidence.get("witnesses", ()))
    process_receipts = tuple(
        receipt for evidence in adapter_evidence for receipt in evidence.get("process_receipts", ()))
    factory_proofs = tuple(
        evidence for evidence in adapter_evidence
        if evidence.get("factory_kind") == "api_run_registered_source_v1"
        and evidence.get("cross_process_proof") is True
        and evidence.get("profiles")
    )
    if ((not witnesses and len(factory_proofs) != len(adapter_evidence))
            or len(process_receipts) != len(witnesses)):
        return None
    return {
        "process_instance": process_instance, "authority_epoch": authority_epoch,
        "close_generation": close_generation, "owner_id": owner_id, "observed": True,
        "cross_process_proof": len(factory_proofs) == len(adapter_evidence) or all(
            isinstance(receipt, dict) and receipt.get("observed") is True
            and receipt.get("kind") in {
                "parent_handle", "I15_process_registry_terminal", "I03_direct_executor"
            }
            and receipt.get("cross_process_proof") is True
            for receipt in process_receipts),
        "profile_ids": tuple(sorted(str(authority.profile_id) for authority in authorities)),
        "registered_adapters": len(adapters),
        "run_witnesses": witnesses,
        "process_receipts": process_receipts,
        "source_factory_proofs": factory_proofs,
    }

