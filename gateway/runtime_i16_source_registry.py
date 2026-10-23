"""Closed-world source graph for the I16 sidecar preservation owner.

The runtime custody coordinator remains the only lease ledger.  This module retains the
exact ``ServiceLease``/``ServiceProcessReceipt`` objects that already carry the native
process, reader, and durable-result witnesses.  It adds the missing source graph so a
global seal cannot succeed from an empty map, a PID, or a boolean readiness flag.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import importlib
from typing import Any

from gateway.runtime_sidecar_transport import RequiredStreamsError, required_reader_streams


I16_PRODUCER_ENTRYPOINTS = {
    "provider": ("gateway.runtime_service_operations", "_managed_turn_policy"),
    "acp": ("agent.copilot_acp_client", "CopilotACPClient"),
    "codex": ("agent.transports.codex_app_server_custody", "CodexAppServerCustody"),
    "shell": ("agent.shell_hooks", "_spawn"),
    "mcp": ("tools.mcp_tool_discovery", "_prepare_i16_source_owner"),
    "browser": ("gateway.runtime_sidecar_browser", "_reserve_managed_browser"),
    "cua": ("gateway.runtime_sidecar_cua", "_reserve_managed_parent"),
    "environment": ("tools.environments.local_custody", "prepare_managed_local_sidecar"),
}
I16_PRODUCER_KEYS = tuple(I16_PRODUCER_ENTRYPOINTS)
I16_FACTORY_ATTRIBUTES = {
    "mcp": "_mcp_source_factory",
    "browser": "_browser_source_factory",
    "cua": "_cua_source_factory",
}


class I16SourceGraphError(RuntimeError):
    """The source graph cannot authenticate one exact I16 route."""


@dataclass(frozen=True)
class I16ProducerBinding:
    key: str
    entrypoint: Any
    module: str
    qualname: str


@dataclass
class I16SourceRegistry:
    """Exact producer, service, request, and physical witnesses for one coordinator."""

    coordinator: Any
    producer_bindings: dict[str, I16ProducerBinding] = field(default_factory=dict)
    factory_bindings: dict[str, list[tuple[Any, Any]]] = field(default_factory=dict)
    authorities: tuple[Any, ...] = ()
    services: dict[str, Any] = field(default_factory=dict)
    requests: dict[str, Any] = field(default_factory=dict)
    receipts: dict[str, list[Any]] = field(default_factory=dict)
    missing: dict[str, str] = field(default_factory=dict)
    bootstrapped: bool = False
    # Standalone ACP/Codex/CUA parents have no profile authority/factory graph.  They still
    # receive the same closed producer-entrypoint graph and service/receipt checks, but are
    # authenticated against their exact process/profile tuple instead of an invented authority.
    standalone: bool = False
    standalone_identity: tuple[str, str, int] | None = None

    @property
    def _lock(self):
        return getattr(self.coordinator, "_lock", None)

    def _with_lock(self):
        lock = self._lock
        if lock is None:
            raise I16SourceGraphError("i16_coordinator_lock_missing")
        return lock

    def register_producer(self, key: str, entrypoint: Any) -> Any:
        with self._with_lock():
            if key not in I16_PRODUCER_ENTRYPOINTS or not callable(entrypoint):
                self.missing[key] = "entrypoint_invalid"
                return None
            current = self.producer_bindings.get(key)
            if current is not None and current.entrypoint is not entrypoint:
                self.missing[key] = "entrypoint_replaced"
                return None
            self.producer_bindings[key] = I16ProducerBinding(
                key=key,
                entrypoint=entrypoint,
                module=str(getattr(entrypoint, "__module__", "")),
                qualname=str(getattr(entrypoint, "__qualname__", "")),
            )
            self.missing.pop(key, None)
            return entrypoint

    def register_factory(self, key: str, authority: Any, factory: Any) -> Any:
        with self._with_lock():
            if key not in I16_FACTORY_ATTRIBUTES or authority is None or factory is None:
                self.missing[f"factory:{key}"] = "factory_invalid"
                return None
            entries = self.factory_bindings.setdefault(key, [])
            for existing_authority, existing_factory in entries:
                if existing_authority is authority:
                    if existing_factory is not factory:
                        self.missing[f"factory:{key}"] = "factory_replaced"
                        return None
                    self.missing.pop(f"factory:{key}", None)
                    return factory
            entries.append((authority, factory))
            self.missing.pop(f"factory:{key}", None)
            return factory

    def require_producer(self, key: str) -> None:
        with self._with_lock():
            if key not in I16_PRODUCER_ENTRYPOINTS:
                raise I16SourceGraphError("i16_source_unregistered")
            if self.producer_bindings.get(key) is None or key in self.missing:
                raise I16SourceGraphError("i16_source_unregistered")

    @staticmethod
    def _lease_id(handle: Any) -> str | None:
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        return lease_id if type(lease_id) is str and lease_id else None

    def register_service(self, service: Any, producer_key: str) -> Any:
        with self._with_lock():
            self.require_producer(producer_key)
            if (
                getattr(service, "coordinator", None) is not self.coordinator
                or getattr(service, "producer_key", None) != producer_key
                or getattr(getattr(service, "handle", None), "owner_class", None)
                != "I16_turn_sidecars"
            ):
                raise I16SourceGraphError("i16_service_identity_mismatch")
            lease_id = self._lease_id(getattr(service, "handle", None))
            if lease_id is None:
                raise I16SourceGraphError("i16_service_handle_missing")
            current = self.services.get(lease_id)
            if current is not None and current is not service:
                raise I16SourceGraphError("i16_service_replaced")
            self.services[lease_id] = service
            return service

    def register_request(self, service: Any, request: Any) -> Any:
        with self._with_lock():
            service_id = self._lease_id(getattr(service, "handle", None))
            request_id = self._lease_id(request)
            if (
                service_id is None
                or self.services.get(service_id) is not service
                or getattr(request, "owner_class", None) != "I16_turn_sidecars"
                or getattr(getattr(request, "lease", None), "parent_lease_id", None) != service_id
            ):
                raise I16SourceGraphError("i16_request_identity_mismatch")
            current = self.requests.get(request_id)
            if current is not None and current is not request:
                raise I16SourceGraphError("i16_request_replaced")
            self.requests[request_id] = request
            return request

    def register_receipt(self, service: Any, receipt: Any) -> Any:
        with self._with_lock():
            service_id = self._lease_id(getattr(service, "handle", None))
            physical_id = self._lease_id(getattr(receipt, "handle", None))
            parent_id = getattr(
                getattr(getattr(receipt, "handle", None), "lease", None),
                "parent_lease_id",
                None,
            )
            parent_is_service = parent_id == service_id
            parent_is_request = (
                isinstance(parent_id, str)
                and parent_id in self.requests
                and getattr(
                    getattr(self.requests[parent_id], "lease", None), "parent_lease_id", None,
                ) == service_id
            )
            if (
                service_id is None
                or self.services.get(service_id) is not service
                or getattr(receipt, "service", None) is not service
                or getattr(getattr(receipt, "handle", None), "owner_class", None)
                != "I16_turn_sidecars"
                or not (parent_is_service or parent_is_request)
                or physical_id is None
            ):
                raise I16SourceGraphError("i16_receipt_identity_mismatch")
            entries = self.receipts.setdefault(service_id, [])
            if all(existing is not receipt for existing in entries):
                entries.append(receipt)
            return receipt

    def _factory_graph_is_complete(self) -> bool:
        if self.standalone:
            return not self.authorities and not any(self.factory_bindings.values())
        for key, attribute in I16_FACTORY_ATTRIBUTES.items():
            entries = self.factory_bindings.get(key, [])
            if len(entries) != len(self.authorities):
                return False
            for authority in self.authorities:
                factory = getattr(authority, attribute, None)
                if factory is None or not any(
                    owner is authority and registered is factory
                    for owner, registered in entries
                ):
                    return False
        return True

    @staticmethod
    def _terminal_handle(coordinator: Any, handle: Any) -> bool:
        lease_id = I16SourceRegistry._lease_id(handle)
        completed = getattr(coordinator, "_completed_handles", None)
        records = getattr(coordinator, "_records", None)
        if lease_id is None or completed is None or not isinstance(records, dict):
            return False
        return completed.get(lease_id) is handle and lease_id not in records

    @staticmethod
    def _process_is_closed(receipt: Any) -> bool:
        process = getattr(receipt, "process", None)
        if process is None or not callable(getattr(process, "poll", None)):
            return False
        try:
            if process.poll() is None:
                return False
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False
        transaction = getattr(receipt, "transaction", None)
        if transaction is not None:
            if getattr(transaction, "process", None) is not process:
                return False
            if getattr(transaction, "state", None) != "closed":
                return False
            if getattr(transaction, "process_handle_closed", False) is not True:
                return False
            if getattr(transaction, "job_handle_closed", False) is not True:
                return False
            return True
        scope = getattr(receipt, "scope", None)
        if scope == "posix_direct":
            return True
        if scope == "posix_process_group":
            provider = getattr(receipt, "contained_scope_provider", None)
            observe = getattr(provider, "observe_empty", None)
            if not callable(observe):
                return False
            try:
                return observe() is True
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                return False
        return False

    @classmethod
    def _receipt_is_terminal(cls, receipt: Any) -> bool:
        state = getattr(receipt, "state", None)
        if state == "released":
            return (
                getattr(receipt, "unknown_reason", None) is None
                and getattr(receipt, "process", None) is None
                and getattr(receipt, "transaction", None) is None
                and not getattr(receipt, "readers", {})
                and not getattr(receipt, "reader_start_states", {})
                and not getattr(receipt, "eof_streams", set())
                and not getattr(receipt, "joined_streams", set())
                and not getattr(receipt, "durable_proofs", {})
                and not getattr(receipt, "durable_proof_history", [])
                and not getattr(receipt, "delivery_requests", set())
                and not getattr(receipt, "completed_requests", set())
                and not getattr(receipt, "managed_witness", None)
                and not getattr(receipt, "managed_child_history", [])
                and getattr(receipt, "native_close_committed", False) is not True
                and getattr(receipt, "external_publication_proof", None) is None
            )
        if state != "closed" or getattr(receipt, "unknown_reason", None) is not None:
            return False
        if getattr(receipt, "reader_error", None) is not None:
            return False
        try:
            streams = required_reader_streams(receipt)
        except (RequiredStreamsError, AttributeError, RuntimeError, TypeError, ValueError):
            return False
        reader_start_states = getattr(receipt, "reader_start_states", {})
        if (
            set(getattr(receipt, "readers", {})) != set(streams)
            or set(reader_start_states) != set(streams)
            or any(reader_start_states.get(stream) != "started" for stream in streams)
            or getattr(receipt, "eof_streams", set()) != set(streams)
            or getattr(receipt, "joined_streams", set()) != set(streams)
            or any(
                not callable(getattr(reader, "is_alive", None))
                or reader.is_alive() is not False
                for reader in getattr(receipt, "readers", {}).values()
            )
            or not cls._process_is_closed(receipt)
        ):
            return False
        source_receipt = getattr(receipt, "source_receipt", None)
        source_readback = None
        if (
            getattr(getattr(receipt, "service", None), "binding", None) is None
            and source_receipt is not None
            and getattr(source_receipt, "physical", None) is receipt
            and getattr(source_receipt, "state", None) == "closed"
            and callable(getattr(source_receipt, "readback", None))
        ):
            try:
                source_readback = source_receipt.readback()
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                source_readback = None
        has_result = bool(getattr(receipt, "durable_proofs", {})) or bool(
            getattr(receipt, "durable_proof_history", [])
        )
        publication = getattr(receipt, "external_publication_proof", None)
        has_publication = publication is not None and (
            getattr(publication, "durable_result", False) is True
            or getattr(publication, "cache_readback", False) is True
        )
        return has_result or has_publication or source_readback is not None

    def source_owner_evidence(
        self, *, process_instance: str, authority_epoch: int, close_generation: int,
        owner_id: str,
    ) -> dict[str, Any] | None:
        """Return a source-backed seal witness, or refuse when any edge is missing."""
        lock = self._with_lock()
        with lock:
            if (
                not self.bootstrapped
                or tuple(self.producer_bindings) != I16_PRODUCER_KEYS
                or self.missing
                or (not self.standalone and not self.authorities)
                or (self.standalone and self.standalone_identity is None)
                or not self._factory_graph_is_complete()
            ):
                return None
            if (
                process_instance != getattr(self.coordinator, "process_instance", None)
                or authority_epoch != getattr(self.coordinator, "authority_epoch", None)
                or type(close_generation) is not int
                or type(owner_id) is not str
                or not owner_id
            ):
                return None
            if self.standalone and (
                self.standalone_identity is None
                or self.standalone_identity[1] != process_instance
                or self.standalone_identity[2] != authority_epoch
            ):
                return None
            known: dict[str, Any] = {}
            terminal_receipts: list[dict[str, Any]] = []
            for service_id, service in self.services.items():
                if (
                    service_id != self._lease_id(getattr(service, "handle", None))
                    or getattr(service, "coordinator", None) is not self.coordinator
                    or (
                        self.standalone
                        and (
                            self.standalone_identity is None
                            or getattr(getattr(service, "handle", None), "profile_id", None)
                            != self.standalone_identity[0]
                        )
                    )
                    or getattr(service, "unknown", False)
                    or getattr(service, "requests", None)
                    or getattr(service, "durable_requests", None)
                    or getattr(service, "delivery_unknown_reasons", None)
                    or getattr(service, "delivery_terminalized", None)
                    or not self._terminal_handle(self.coordinator, service.handle)
                ):
                    return None
                known[service_id] = service.handle
                for request_id, request in self.requests.items():
                    if getattr(getattr(request, "lease", None), "parent_lease_id", None) == service_id:
                        if (
                            request_id != self._lease_id(request)
                            or (
                                self.standalone
                                and (
                                    self.standalone_identity is None
                                    or getattr(request, "profile_id", None)
                                    != self.standalone_identity[0]
                                )
                            )
                        ):
                            return None
                        known[request_id] = request
                receipts = self.receipts.get(service_id, [])
                current = getattr(service, "physical", None)
                if current is not None and all(current is not item for item in receipts):
                    return None
                for receipt in receipts:
                    if (
                        getattr(receipt, "service", None) is not service
                        or (
                            self.standalone
                            and (
                                self.standalone_identity is None
                                or getattr(getattr(receipt, "handle", None), "profile_id", None)
                                != self.standalone_identity[0]
                            )
                        )
                        or not self._terminal_handle(self.coordinator, receipt.handle)
                        or not self._receipt_is_terminal(receipt)
                    ):
                        return None
                    receipt_id = self._lease_id(receipt.handle)
                    if receipt_id is None:
                        return None
                    known[receipt_id] = receipt.handle
                    terminal_receipts.append({
                        "service": service_id,
                        "handle": receipt_id,
                        "state": receipt.state,
                        "durable_results": len(getattr(receipt, "durable_proofs", {})),
                        "historical_results": len(getattr(receipt, "durable_proof_history", [])),
                    })
            records = getattr(self.coordinator, "_records", None)
            if not isinstance(records, dict) or any(
                getattr(record.handle, "owner_class", None) == "I16_turn_sidecars"
                for record in records.values()
            ):
                return None
            completed = getattr(self.coordinator, "_completed_handles", None)
            if completed is None:
                return None
            for lease_id, handle in list(completed.items()):
                if getattr(handle, "owner_class", None) == "I16_turn_sidecars" and (
                    lease_id not in known or known[lease_id] is not handle
                ):
                    return None
            return {
                # The coordinator owns the canonical closed-world source label.  The exact
                # callable graph is carried separately below so this contract cannot be
                # mistaken for the older filename-only manifest.
                "source": self._owner_source_label(),
                "source_graph": "gateway/runtime_i16_source_registry.py:I16_PRODUCER_ENTRYPOINTS",
                "process_instance": process_instance,
                "authority_epoch": authority_epoch,
                "close_generation": close_generation,
                "owner_id": owner_id,
                "observed": True,
                "cross_process_proof": True,
                "registered_producers": tuple(self.producer_bindings),
                "producer_entrypoints": tuple(
                    (key, binding.module, binding.qualname)
                    for key, binding in self.producer_bindings.items()
                ),
                "registered_factories": tuple(
                    (key, len(entries)) for key, entries in self.factory_bindings.items()
                ),
                "standalone": self.standalone,
                "standalone_identity": self.standalone_identity,
                "service_handles": tuple(sorted(self.services)),
                "terminal_receipts": tuple(terminal_receipts),
                "native_evidence": tuple(
                    (item["handle"], item["state"]) for item in terminal_receipts
                ),
            }

    def source_owner_evidence_and_seal(
        self, *, process_instance: str, authority_epoch: int, close_generation: int,
        owner_id: str,
    ) -> tuple[dict[str, Any] | None, Any]:
        """Read and seal the graph under the coordinator's close/admission lock.

        Existing accepted continuations may still publish a receipt while the runtime is draining.
        Holding the same lock through the final source read and owner seal prevents a late registry
        mutation from landing between those two operations.
        """
        with self._with_lock():
            evidence = self.source_owner_evidence(
                process_instance=process_instance,
                authority_epoch=authority_epoch,
                close_generation=close_generation,
                owner_id=owner_id,
            )
            if evidence is None:
                return None, None
            return evidence, self.coordinator.seal_owner("I16_turn_sidecars", evidence)

    @staticmethod
    def _owner_source_label() -> str:
        from gateway.runtime_custody import OWNER_SPECS
        return OWNER_SPECS["I16_turn_sidecars"].source


def ensure_i16_source_registry(coordinator: Any) -> I16SourceRegistry:
    registry = getattr(coordinator, "_i16_source_registry", None)
    if isinstance(registry, I16SourceRegistry) and registry.coordinator is coordinator:
        return registry
    registry = I16SourceRegistry(coordinator)
    coordinator._i16_source_registry = registry
    return registry


def _runner_authorities(runner: Any) -> tuple[Any, ...]:
    try:
        from gateway.session_authorities import all_authorities
        return tuple(authority for authority in all_authorities(runner) if authority is not None)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return tuple(
            authority for authority in (getattr(runner, "session_authority", None),)
            if authority is not None
        )


def refresh_i16_source_registry(runner: Any, coordinator: Any) -> I16SourceRegistry:
    """Add every live/surviving profile authority and its exact source factories.

    A retired authority stays in the graph until this process closes because its service and
    receipt objects may still be draining.  A successor is therefore appended by identity; a
    copied authority or replaced factory cannot satisfy the old entry.
    """
    registry = ensure_i16_source_registry(coordinator)
    authorities = _runner_authorities(runner)
    with registry._with_lock():
        registry.standalone = False
        registry.standalone_identity = None
        merged = list(registry.authorities)
        for authority in authorities:
            if all(existing is not authority for existing in merged):
                merged.append(authority)
        registry.authorities = tuple(merged)
        if authorities:
            registry.missing.pop("authority_graph", None)
        for authority in authorities:
            for key, attr_name in I16_FACTORY_ATTRIBUTES.items():
                factory = getattr(authority, attr_name, None)
                if factory is None:
                    registry.missing[f"factory:{key}"] = "factory_unregistered"
                else:
                    registry.register_factory(key, authority, factory)
    return registry


def explicit_i16_producer_key(producer_key: Any) -> str | None:
    """Accept only a binding supplied by a trusted internal producer entrypoint.

    ``owner_id``, ``source_identity`` and family labels are descriptive data.  They are
    intentionally not consulted here: a caller-controlled string must never authenticate
    itself as one of the closed producer classes.
    """
    if type(producer_key) is str and producer_key in I16_PRODUCER_KEYS:
        return producer_key
    return None


def bootstrap_i16_source_registry(runner: Any, coordinator: Any) -> I16SourceRegistry:
    registry = ensure_i16_source_registry(coordinator)
    authorities = _runner_authorities(runner)
    with registry._with_lock():
        registry.standalone = False
        registry.standalone_identity = None
        registry.authorities = authorities
        if not authorities:
            registry.missing["authority_graph"] = "authority_registry_empty"
    for key, (module_name, attr_name) in I16_PRODUCER_ENTRYPOINTS.items():
        try:
            entrypoint = getattr(importlib.import_module(module_name), attr_name)
        except (AttributeError, ImportError, OSError, TypeError, ValueError) as exc:
            registry.missing[key] = f"entrypoint_unavailable:{type(exc).__name__}"
            continue
        registry.register_producer(key, entrypoint)
    refresh_i16_source_registry(runner, coordinator)
    registry.bootstrapped = True
    return registry


def bootstrap_standalone_i16_source_registry(
    coordinator: Any, *, profile_id: Any, process_instance: Any, authority_epoch: Any,
) -> I16SourceRegistry:
    """Bootstrap the closed producer graph for a standalone ACP/Codex/CUA parent.

    Standalone work has no served ``SessionAuthority``.  Keeping that distinction explicit avoids
    fabricating a profile factory while still requiring every supported producer entrypoint before
    a service or receipt can be registered.
    """
    if (
        type(profile_id) is not str or not profile_id
        or type(process_instance) is not str or not process_instance
        or type(authority_epoch) is not int or authority_epoch < 1
        or getattr(coordinator, "process_instance", None) != process_instance
        or getattr(coordinator, "authority_epoch", None) != authority_epoch
    ):
        raise I16SourceGraphError("i16_standalone_identity_mismatch")
    registry = ensure_i16_source_registry(coordinator)
    with registry._with_lock():
        registry.standalone = True
        registry.standalone_identity = (profile_id, process_instance, authority_epoch)
        registry.authorities = ()
        registry.factory_bindings.clear()
        registry.missing.clear()
    for key, (module_name, attr_name) in I16_PRODUCER_ENTRYPOINTS.items():
        try:
            entrypoint = getattr(importlib.import_module(module_name), attr_name)
        except (AttributeError, ImportError, OSError, TypeError, ValueError) as exc:
            registry.missing[key] = f"entrypoint_unavailable:{type(exc).__name__}"
            continue
        registry.register_producer(key, entrypoint)
    registry.bootstrapped = True
    return registry


def source_owner_evidence(coordinator: Any, **kwargs: Any) -> dict[str, Any] | None:
    registry = getattr(coordinator, "_i16_source_registry", None)
    if not isinstance(registry, I16SourceRegistry):
        return None
    try:
        return registry.source_owner_evidence(**kwargs)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None


def seal_source_owner(coordinator: Any, **kwargs: Any) -> tuple[dict[str, Any] | None, Any]:
    registry = getattr(coordinator, "_i16_source_registry", None)
    if not isinstance(registry, I16SourceRegistry):
        return None, None
    try:
        return registry.source_owner_evidence_and_seal(**kwargs)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None, None
