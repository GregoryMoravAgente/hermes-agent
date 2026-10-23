"""Runtime-custody adapter for hosted TUI and compute-host work.

The runtime coordinator is the only ownership ledger.  This module only attaches its
opaque handles to the real authority, supervisor, and pending-request objects that
already own the work; it never mirrors those objects in a second registry.
"""

from __future__ import annotations

import asyncio
import sys
import os
import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_OWNER_CLASS = "I17_hosted_tui_services"
_PRODUCER_REGISTRY_SOURCE = "tui_gateway.hosted_tui_custody:i17-producer-registry-v1"
_RUNTIME_RUNNER = None
_RUNTIME_COORDINATOR = None
_CHILD_BRIDGE = None
_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class I17ColdPostOpenCapability:
    """Coordinator-bound completion for a cold prepared I17 successor registry."""

    runner: Any
    coordinator: Any
    authority: Any
    registry: dict
    binding: Any
    root: Any
    receipt: Any


def i17_cold_abort_participant(capability):
    """Return the exact source tuple retained by the canonical post-OPEN transition."""
    return capability.authority, capability.receipt, capability


class _I17RebindSource:
    """Boot-installed I17 source capability used by the coordinator's abort boundary."""

    __slots__ = ("__weakref__", "authority", "coordinator", "registry", "runner")

    def __init__(self, authority, runner, coordinator, registry):
        self.authority = authority
        self.runner = runner
        self.coordinator = coordinator
        self.registry = registry

    def update_registry(self, registry):
        self.registry = registry

    def _authority_rebind_context(self, retired):
        from gateway.session_authorities import all_authorities

        authority = self.authority
        coordinator = self.coordinator
        if getattr(self.runner, "runtime_custody", None) is not coordinator:
            return None
        if not any(item is authority for item in all_authorities(self.runner)):
            return None
        if getattr(authority, "_i17_runtime_custody", None) is not coordinator:
            return None
        if getattr(authority, "_i17_service_handle", None) is not retired:
            return None
        registry = _producer_registry(authority)
        if registry is None or registry is not self.registry:
            return None
        factory = registry.get("room_factory")
        if registry.get("authority") is not authority:
            return None
        if registry.get("runner") is not self.runner:
            return None
        if not isinstance(factory, dict):
            return None
        if factory.get("source") != "gateway.session_hosted_service:CanonicalHostedRoomService":
            return None
        if factory.get("authority") is not authority or factory.get("runner") is not self.runner:
            return None
        if getattr(authority, "_i17_service_closed", False) is not True:
            return None
        valid, receipts = _profile_host_evidence(authority)
        if not valid:
            return None
        profile_id = str(getattr(authority, "profile_id", ""))
        authority_epoch = getattr(authority, "epoch", None)
        if not profile_id or type(authority_epoch) is not int or authority_epoch < 1:
            return None
        return coordinator, registry, receipts, profile_id, authority_epoch

    def _rebind_scope_observation(self, coordinator, profile_id, authority_epoch, retired):
        scope = coordinator.profile_scope_observation(
            profile_id, authority_epoch, require_serving=False, allow_retiring=True,
        )
        retired_physical_epoch = getattr(retired, "physical_owner_epoch", None)
        if not scope.ok or type(scope.physical_owner_epoch) is not str:
            return None
        if not scope.physical_owner_epoch:
            return None
        if type(retired_physical_epoch) is not str or not retired_physical_epoch:
            return None
        if scope.physical_owner_epoch != retired_physical_epoch:
            return None
        return scope

    def observe_rebind(self, retired, close):
        from hermes_cli.backend_retirement import RetirementClose
        from gateway.runtime_custody import TerminalRootRebindObservation

        if not isinstance(close, RetirementClose):
            return None
        context = self._authority_rebind_context(retired)
        if context is None:
            return None
        coordinator, registry, receipts, profile_id, authority_epoch = context
        scope = self._rebind_scope_observation(
            coordinator, profile_id, authority_epoch, retired,
        )
        if scope is None:
            return None
        return TerminalRootRebindObservation(
            source=_PRODUCER_REGISTRY_SOURCE,
            predecessor=retired,
            predecessor_owner_id=retired.owner_id,
            process_instance=coordinator.process_instance,
            profile_id=profile_id,
            authority_epoch=authority_epoch,
            close_generation=close.close_generation,
            physical_owner_epoch=scope.physical_owner_epoch,
            producer_graph_digest=_producer_graph_digest(registry, receipts),
            producer_registry=registry,
            source_binding=self,
        )


def _server():
    return sys.modules.get("tui_gateway.server")


def _runtime():
    server = _server()
    if server is not None:
        runner = getattr(server, "_i17_runtime_runner", None)
        coordinator = getattr(server, "_i17_runtime_custody", None)
        if runner is not None and coordinator is not None:
            return runner, coordinator
    return _RUNTIME_RUNNER, _RUNTIME_COORDINATOR


def _child_bridge():
    server = _server()
    if server is not None:
        bridge = getattr(server, "_i17_child_bridge", None)
        if bridge is not None:
            return bridge
    return _CHILD_BRIDGE


def bind_child_bridge(payload: dict, *, on_unknown=None):
    """Install the immutable parent-owned bridge in a compute child."""
    global _CHILD_BRIDGE
    from tui_gateway.i17_child_bridge import install
    bridge = install(payload, on_unknown=on_unknown)
    _CHILD_BRIDGE = bridge
    server = _server()
    if server is not None:
        server._i17_child_bridge = bridge
    return bridge


def bind_runtime(runner, coordinator, *, reserve=True):
    """Publish the canonical runtime binding and optionally reserve served profile roots.

    Gateway bootstrap publishes the binding before the control listener exists.  It must not
    make every legacy runtime fixture look like an active hosted-TUI producer; the canonical
    hosted-service preparation calls :func:`bind_authority` while the coordinator is still open.
    Direct hosted-TUI callers retain the historical eager reservation by default.
    """
    global _RUNTIME_RUNNER, _RUNTIME_COORDINATOR, _CHILD_BRIDGE
    _RUNTIME_RUNNER = runner
    _RUNTIME_COORDINATOR = coordinator
    _CHILD_BRIDGE = None
    server = _server()
    if server is not None:
        server._i17_runtime_runner = runner
        server._i17_runtime_custody = coordinator
        server._i17_child_bridge = None
    if not reserve:
        return
    # Reserve every authority while the coordinator is still OPEN.  A cold served profile has no
    # producer callback that can call bind_authority before preservation begins; discovering it
    # from owner_evidence after begin_close would therefore try to acquire a parentless root while
    # closing and fail closed without a real source graph.  The authority registry is the
    # source-owned declaration of every profile this runtime serves, including profiles with no
    # host or room producer yet.
    from gateway.session_authorities import all_authorities
    for authority in tuple(all_authorities(runner)):
        if authority is not None:
            bind_authority(authority, runner=runner, coordinator=coordinator)


def _rebind_existing_authority(
    authority, runner, coordinator, existing, profile_id, epoch, owner_id,
    close_receipt, source_proof,
):
    registry = _producer_registry(authority)
    if registry is None:
        raise RuntimeError("I17 producer registry is unavailable")
    binding = _ensure_rebind_source(authority, runner, coordinator, registry)
    if not getattr(authority, "_i17_service_closed", False):
        if not coordinator.is_active_handle(
            existing, owner_class=_OWNER_CLASS, profile_id=profile_id,
            authority_epoch=epoch,
        ):
            raise RuntimeError("I17 authority root is stale")
        return existing
    # A reversible prepare terminalizes the old root.  Abort must prove that exact predecessor
    # before allocating a new root; clearing the closed flag or reusing the removed lease would
    # let a copied/stale root cross the profile boundary.
    if close_receipt is not None or source_proof is not None:
        if close_receipt is None or source_proof is None:
            raise RuntimeError("I17 abort proof is incomplete")
        from gateway.runtime_custody import TerminalRootRebindProof
        if not isinstance(source_proof, TerminalRootRebindProof):
            raise RuntimeError("I17 abort proof type is invalid")
        if source_proof.producer_registry is not _producer_registry(authority):
            raise RuntimeError("I17 abort producer registry is not canonical")
        rebound = coordinator.rebind_terminal_root_during_abort(
            existing, owner_id, profile_id=profile_id, authority_epoch=epoch,
            owner_class=_OWNER_CLASS, receipt=close_receipt, source_proof=source_proof,
        )
    else:
        rebound = coordinator.rebind_terminal_root(
            existing, owner_id, profile_id=profile_id, authority_epoch=epoch,
            owner_class=_OWNER_CLASS,
        )
    if getattr(rebound, "lease", None) is None:
        raise RuntimeError(
            f"I17 profile root rebind refused: {getattr(rebound, 'code', 'unknown')}"
        )
    authority._i17_retired_service_handle = existing
    authority._i17_service_handle = rebound
    authority._i17_service_closed = False
    authority._i17_service_owner_id = owner_id
    authority._i17_runtime_custody = coordinator
    authority._i17_prepared_successor_pending = (
        close_receipt is not None and source_proof is not None
    )
    _reset_producer_registry(
        authority, coordinator, runner=runner,
        intake_closed=close_receipt is not None and source_proof is not None,
    )
    binding.update_registry(_producer_registry(authority))
    service = getattr(authority, "hosted_room_service", None)
    if service is not None and close_receipt is not None and source_proof is not None:
        # Keep the exact predecessor room handle reachable until the prepared successor child is
        # authenticated below the rebound root.  This is an identity witness, not a new registry.
        service._i17_retired_service_root = existing
    return rebound


def bind_authority(authority, *, runner=None, coordinator=None, close_receipt=None, source_proof=None):
    """Reserve one profile root before hosted service or RPC effects are published."""
    existing = getattr(authority, "_i17_service_handle", None)
    if runner is None or coordinator is None:
        runtime_runner, runtime_coordinator = _runtime()
        runner = runner or runtime_runner
        coordinator = coordinator or runtime_coordinator
    if runner is None or coordinator is None:
        return None
    profile_id = str(getattr(authority, "profile_id", ""))
    epoch = getattr(authority, "epoch", None)
    if not profile_id or type(epoch) is not int or epoch < 1:
        raise RuntimeError("I17 authority identity is unavailable")
    owner_id = f"{coordinator.process_instance}:I17:{profile_id}"
    if existing is not None:
        return _rebind_existing_authority(
            authority, runner, coordinator, existing, profile_id, epoch, owner_id,
            close_receipt, source_proof,
        )
    handle = coordinator.reserve_root(
        _OWNER_CLASS, owner_id, profile_id=profile_id, authority_epoch=epoch)
    if getattr(handle, "lease", None) is None:
        raise RuntimeError(f"I17 profile custody refused: {getattr(handle, 'code', 'unknown')}")
    authority._i17_service_handle = handle
    authority._i17_service_owner_id = owner_id
    authority._i17_runtime_custody = coordinator
    authority._i17_producer_registry = _new_producer_registry(
        coordinator, profile_id=profile_id, authority_epoch=epoch,
        authority=authority, runner=runner,
    )
    _ensure_rebind_source(
        authority, runner, coordinator, authority._i17_producer_registry,
    )
    return handle


def _new_producer_registry(
    coordinator, *, profile_id: str, authority_epoch: int, authority=None, runner=None,
) -> dict:
    """Create the source-owned closed-world producer declaration for one profile."""
    return {
        "source": _PRODUCER_REGISTRY_SOURCE,
        "process_instance": coordinator.process_instance,
        "authority_epoch": authority_epoch,
        "profile_id": profile_id,
        "intake_closed": False,
        # The rebind source is the typed, boot-installed witness for this exact registry.  A
        # registry object without its source binding is only an untrusted dict and cannot certify
        # a cold/no-room close or an authenticated successor.
        "source_binding": None,
        "prepared_successor_pending": False,
        "authority": authority,
        "runner": runner,
        "room_factory": {
            "source": "gateway.session_hosted_service:CanonicalHostedRoomService",
            "authority": authority,
            "runner": runner,
        },
        "host": {"state": "absent"},
        "room": {"state": "absent"},
    }


def _reset_producer_registry(
    authority, coordinator, *, runner=None, intake_closed: bool = False,
) -> None:
    """Reopen the exact source graph after abort; no prior terminal receipt is reused."""
    profile_id = str(getattr(authority, "profile_id", ""))
    epoch = getattr(authority, "epoch", None)
    if not profile_id or type(epoch) is not int or epoch < 1:
        raise RuntimeError("I17 authority identity is unavailable")
    binding = getattr(authority, "_i17_rebind_source", None)
    registry = _new_producer_registry(
        coordinator, profile_id=profile_id, authority_epoch=epoch,
        authority=authority, runner=runner,
    )
    registry["intake_closed"] = bool(intake_closed)
    registry["prepared_successor_pending"] = bool(intake_closed)
    if isinstance(binding, _I17RebindSource):
        binding.update_registry(registry)
        registry["source_binding"] = binding
    authority._i17_producer_registry = registry
    # A new registry cannot inherit scalar receipts or mutable producer entries from its
    # predecessor.  The exact room service object is rebound later under the new source witness.
    for name in (
        "_i17_host_producer_entry", "_i17_room_producer_entry",
        "_i17_host_shutdown_receipt", "_i17_host_terminal_identity",
        "_i17_room_terminal_receipt", "_i17_room_terminal_identity",
    ):
        setattr(authority, name, None)


def _authority_for_home(home):
    runner, coordinator = _runtime()
    if runner is None or coordinator is None:
        return None, None, None
    from gateway.session_authorities import authority_for_home
    authority = authority_for_home(runner, Path(home).resolve())
    if authority is None:
        return runner, coordinator, None
    return runner, coordinator, bind_authority(
        authority, runner=runner, coordinator=coordinator)


def _producer_registry(authority) -> dict | None:
    registry = getattr(authority, "_i17_producer_registry", None)
    if not isinstance(registry, dict) or registry.get("source") != _PRODUCER_REGISTRY_SOURCE:
        return None
    return registry


def _ensure_rebind_source(authority, runner, coordinator, registry):
    binding = getattr(authority, "_i17_rebind_source", None)
    if binding is None:
        binding = _I17RebindSource(authority, runner, coordinator, registry)
        authority._i17_rebind_source = binding
    elif (
        binding.authority is not authority
        or binding.runner is not runner
        or binding.coordinator is not coordinator
        or binding.registry is not registry
    ):
        raise RuntimeError("I17 source binding is not canonical")
    registry["source_binding"] = binding
    refusal = coordinator._register_source_rebind_binding(
        _OWNER_CLASS, str(getattr(authority, "profile_id", "")), binding,
    )
    if refusal is not None:
        raise RuntimeError(
            f"I17 source binding refused: {getattr(refusal, 'code', 'unknown')}"
        )
    return binding


def _host_entry_is_current(authority, producer, supervisor=None, handle=None) -> bool:
    """Require the source-owned host entry and its opaque custody objects to be identical."""
    registry = _producer_registry(authority)
    root = getattr(authority, "_i17_service_handle", None)
    expected_handle = handle if handle is not None else getattr(supervisor, "_i17_host_handle", None)
    expected_supervisor = supervisor
    return bool(
        registry is not None
        and isinstance(producer, dict)
        and producer is registry.get("host")
        and producer is getattr(authority, "_i17_host_producer_entry", None)
        and producer is getattr(supervisor, "_i17_host_producer_entry", None)
        and producer.get("authority") is authority
        and producer.get("root") is root
        and getattr(supervisor, "_i17_service_root", None) is root
        and producer.get("handle") is expected_handle
        and producer.get("supervisor") is expected_supervisor
        and producer.get("state") in {"registered", "terminal"}
        and _registry_identity_valid(authority, registry)
    )


def _room_entry_is_current(authority, producer, service=None, handle=None) -> bool:
    """Require the exact room/service/runtime graph captured at admission."""
    registry = _producer_registry(authority)
    expected_service = service
    expected_handle = handle if handle is not None else getattr(service, "_i17_room_handle", None)
    expected_runtime = getattr(expected_service, "runtime", None)
    expected_db = getattr(authority, "db", None)
    expected_member_rpcs = getattr(expected_service, "member_rpcs", None)
    return bool(
        registry is not None
        and isinstance(producer, dict)
        and producer is registry.get("room")
        and producer is getattr(authority, "_i17_room_producer_entry", None)
        and producer is getattr(expected_service, "_i17_room_producer_entry", None)
        and producer.get("authority") is authority
        and producer.get("root") is getattr(authority, "_i17_service_handle", None)
        and getattr(expected_service, "_i17_service_root", None) is producer.get("root")
        and producer.get("handle") is expected_handle
        and producer.get("service") is expected_service
        and producer.get("runtime") is expected_runtime
        and producer.get("db") is expected_db
        and producer.get("member_rpcs") is expected_member_rpcs
        and producer.get("state") in {"registered", "terminal"}
        and _registry_identity_valid(authority, registry)
    )


def _child(parent, owner_id: str, coordinator):
    if getattr(parent, "lease", None) is None:
        raise RuntimeError("I17 parent custody is unavailable")
    reservation = coordinator.reserve_child(
        parent, _OWNER_CLASS, owner_id, profile_id=parent.profile_id,
        authority_epoch=parent.authority_epoch)
    if getattr(reservation, "reservation", None) is None:
        raise RuntimeError(f"I17 child custody refused: {getattr(reservation, 'code', 'unknown')}")
    handle = coordinator.activate_child(reservation)
    if getattr(handle, "lease", None) is None:
        raise RuntimeError(f"I17 child activation refused: {getattr(handle, 'code', 'unknown')}")
    return handle


def bind_supervisor(supervisor, home, owner_id: str):
    """Attach a real host-process child to its profile root before ``start``."""
    runner, coordinator, root = _authority_for_home(home)
    if runner is None or coordinator is None:
        return False
    if root is None:
        raise RuntimeError("I17 profile authority is unavailable")
    from gateway.session_authorities import authority_for_home
    authority = authority_for_home(runner, Path(home))
    if authority is None:
        raise RuntimeError("I17 profile authority is unavailable")
    registry = _producer_registry(authority)
    if registry is None or registry.get("intake_closed") is True:
        raise RuntimeError("I17 producer intake is closed or unregistered")
    existing = getattr(supervisor, "_i17_host_handle", None)
    if existing is not None and coordinator.is_active_handle(existing, owner_class=_OWNER_CLASS):
        if not _host_entry_is_current(
            authority, registry.get("host"), supervisor, existing,
        ):
            raise RuntimeError("I17 host producer entry is stale")
        return existing
    if existing is not None and not coordinator.is_terminal_handle(existing, owner_class=_OWNER_CLASS):
        raise RuntimeError("I17 host producer handle is stale")
    handle = _child(root, f"host:{owner_id}", coordinator)
    supervisor._i17_runtime_custody = coordinator
    supervisor._i17_service_root = root
    supervisor._i17_host_handle = handle
    supervisor._i17_profile_home = str(Path(home).resolve())
    supervisor._i17_profile_authority = authority
    supervisor._i17_shutdown_native_identity = None
    entry = {
        "state": "registered", "owner_id": f"host:{owner_id}",
        "profile_id": root.profile_id, "authority_epoch": root.authority_epoch,
        "process_instance": root.process_instance, "supervisor": supervisor,
        "authority": authority, "root": root, "handle": handle,
    }
    registry["host"] = entry
    authority._i17_host_producer_entry = entry
    supervisor._i17_host_producer_entry = entry
    from tui_gateway.i17_child_bridge import build_identity
    supervisor._i17_child_bridge = build_identity(
        root, handle, profile_home=str(Path(home).resolve()))
    return True


def _host_effect_started(supervisor) -> bool:
    return any(getattr(supervisor, name, None) is not None for name in (
        "_proc", "_owner", "_registry_lease", "_producer_identity", "_producer_process_handle",
        "_registry_owner_key"))


def _host_native_identity(supervisor):
    return {
        "process_handle": getattr(supervisor, "_producer_process_handle", None),
        "owner": getattr(supervisor, "_owner", None),
        "producer_identity": getattr(supervisor, "_producer_identity", None),
        "owner_identity": getattr(supervisor, "_owner_identity", None),
    }


def _capture_host_shutdown_identity(supervisor):
    """Retain the native owner before a producer's shutdown clears its mutable fields."""
    identity = _host_native_identity(supervisor)
    supervisor._i17_shutdown_native_identity = identity
    return identity


def _host_native_identity_unchanged(supervisor, identity) -> bool:
    if not isinstance(identity, dict):
        return False
    return all(
        getattr(supervisor, attribute, None) is None
        or getattr(supervisor, attribute, None) is identity[key]
        for attribute, key in (
            ("_producer_process_handle", "process_handle"),
            ("_owner", "owner"),
            ("_producer_identity", "producer_identity"),
            ("_owner_identity", "owner_identity"),
        )
    )


def host_start_failed(supervisor):
    """Settle a failed start only when no child/registry effect crossed the boundary."""
    handle = getattr(supervisor, "_i17_host_handle", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    if handle is None or coordinator is None:
        return False
    if _host_effect_started(supervisor):
        coordinator.mark_unknown(handle, "child_start_unknown")
        return False
    if not coordinator.is_active_handle(
        handle, owner_class=_OWNER_CLASS, profile_id=handle.profile_id,
        authority_epoch=handle.authority_epoch,
    ):
        return False
    authority = getattr(supervisor, "_i17_profile_authority", None)
    root = getattr(supervisor, "_i17_service_root", None)
    registry = _producer_registry(authority)
    producer = registry.get("host") if registry is not None else None
    if (
        authority is None
        or root is None
        or getattr(authority, "_i17_runtime_custody", None) is not coordinator
        or getattr(authority, "_i17_service_handle", None) is not root
        or getattr(supervisor, "_i17_host_handle", None) is not handle
        or registry is None
        or registry.get("authority") is not authority
        or getattr(authority, "runner", None) is None
        or registry.get("runner") is not getattr(authority, "runner", None)
        or not _registry_identity_valid(authority, registry)
        or not isinstance(producer, dict)
        or not _host_entry_is_current(authority, producer, supervisor, handle)
        or producer.get("state") != "registered"
        or producer.get("profile_id") != handle.profile_id
        or producer.get("authority_epoch") != handle.authority_epoch
        or producer.get("process_instance") != handle.process_instance
        or producer.get("owner_id") != handle.owner_id
    ):
        return False
    if coordinator.release_unaccepted(handle) is not None:
        return False
    producer["state"] = "absent"
    if authority is not None:
        authority._i17_host_producer_entry = None
    supervisor._i17_shutdown_native_identity = None
    return True


def host_shutdown_failed(supervisor):
    handle = getattr(supervisor, "_i17_host_handle", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    if handle is not None and coordinator is not None:
        coordinator.mark_unknown(handle, "host_close_unknown")


def host_shutdown(supervisor, receipt: dict | None):
    """Terminalize a host child only after the supervisor's real receipt is acknowledged."""
    handle = getattr(supervisor, "_i17_host_handle", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    if handle is None or coordinator is None:
        return True
    if not _host_receipt_valid(receipt):
        coordinator.mark_unknown(handle, "host_close_unknown")
        return False
    authority = getattr(supervisor, "_i17_profile_authority", None)
    registry = _producer_registry(authority)
    producer = registry.get("host") if registry is not None else None
    if not _host_entry_is_current(authority, producer, supervisor, handle):
        coordinator.mark_unknown(handle, "host_close_unknown")
        return False
    if producer.get("state") == "terminal":
        return producer.get("receipt") == receipt and coordinator.is_terminal_handle(
            handle, owner_class=_OWNER_CLASS,
        )
    coordinator.reconcile_unknown(
        handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
        process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
    refusal = coordinator.mark_terminal(handle)
    if refusal is not None:
        coordinator.mark_unknown(handle, "host_close_unknown")
        return False
    if authority is not None:
        # Keep the exact source-owned supervisor/child handle and the native identity observed by
        # the real shutdown path.  Later evidence must not reread mutable supervisor scalar fields.
        native_identity = getattr(supervisor, "_i17_shutdown_native_identity", None)
        if native_identity is None:
            native_identity = _host_native_identity(supervisor)
        terminal_identity = {
            "supervisor": supervisor,
            "root": producer.get("root"),
            "handle": handle,
            "native_identity": native_identity,
            "process_handle": native_identity["process_handle"],
            "producer_identity": native_identity["producer_identity"],
            "owner_identity": native_identity["owner_identity"],
            "host_generation": receipt["host_generation"],
            "host_boot_id": receipt["host_boot_id"],
            "host_pid": receipt["host_pid"],
        }
        # Keep the exact receipt object issued by the supervisor.  Re-copying the mapping here
        # would sever the source-issued identity that the later evidence reader authenticates.
        producer.update({
            "state": "terminal", "receipt": receipt,
            "native_identity": native_identity,
            "terminal_identity": terminal_identity,
        })
        authority._i17_host_shutdown_receipt = producer["receipt"]
        authority._i17_host_terminal_identity = terminal_identity
    supervisor._i17_terminal_receipt = receipt
    return True


def _bind_prepared_room_child(
    service, authority, coordinator, registry, root, existing, close_receipt,
):
    if close_receipt is None:
        raise RuntimeError("I17 prepared successor proof is required")
    existing_binding = getattr(authority, "_i17_rebind_source", None)
    source_runner = getattr(existing_binding, "runner", None)
    if source_runner is None:
        source_runner, _source_coordinator = _runtime()
    binding = _ensure_rebind_source(authority, source_runner, coordinator, registry)
    if coordinator.validate_i17_prepared_root(root, close_receipt, binding) is not None:
        raise RuntimeError("I17 prepared successor root is not authenticated")
    predecessor = coordinator.i17_prepared_predecessor(root, close_receipt, binding)
    if getattr(predecessor, "lease", None) is None:
        raise RuntimeError("I17 prepared predecessor is unavailable")
    if existing is not None and coordinator.is_active_handle(existing, owner_class=_OWNER_CLASS):
        if existing.lease.parent_lease_id != root.lease.lease_id:
            raise RuntimeError("I17 room producer parent is not the rebound root")
        current = registry.get("room")
        if _room_entry_is_current(authority, current, service, existing):
            return existing
    if existing is not None:
        if not coordinator.is_terminal_handle(existing, owner_class=_OWNER_CLASS):
            raise RuntimeError("I17 room producer handle is stale")
        if existing.lease.parent_lease_id != predecessor.lease.lease_id:
            raise RuntimeError("I17 room predecessor does not own the terminal child")
    profile = str(getattr(authority, "profile_id", ""))
    handle = coordinator.admit_i17_prepared_child(
        root, owner_id=f"room-service:{profile}", profile_id=profile,
        authority_epoch=root.authority_epoch, receipt=close_receipt,
        source_binding=binding,
    )
    if getattr(handle, "lease", None) is None:
        raise RuntimeError(
            f"I17 prepared child custody refused: {getattr(handle, 'code', 'unknown')}"
        )
    return handle


def _bind_open_room_child(service, authority, coordinator, registry, root, existing, close_receipt):
    if close_receipt is not None:
        raise RuntimeError("I17 prepared successor registry is missing")
    if existing is not None and coordinator.is_active_handle(existing, owner_class=_OWNER_CLASS):
        current = registry.get("room")
        if _room_entry_is_current(authority, current, service, existing):
            return existing
    if existing is not None and not coordinator.is_terminal_handle(existing, owner_class=_OWNER_CLASS):
        raise RuntimeError("I17 room producer handle is stale")
    profile = str(getattr(authority, "profile_id", ""))
    return _child(root, f"room-service:{profile}", coordinator)


def bind_room_service(service, authority, *, close_receipt=None):
    """Reserve a hosted-room worker below the profile root before its thread starts."""
    existing = getattr(service, "_i17_room_handle", None)
    root = getattr(authority, "_i17_service_handle", None)
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    if root is None or coordinator is None:
        runner, coordinator_runtime = _runtime()
        if runner is None or coordinator_runtime is None:
            return None
        root = bind_authority(authority, runner=runner, coordinator=coordinator_runtime)
        coordinator = coordinator_runtime
    if root is None or coordinator is None:
        return None
    current_service = getattr(authority, "hosted_room_service", None)
    if current_service is not None and current_service is not service:
        raise RuntimeError("I17 room producer service is replaced")
    registry = _producer_registry(authority)
    if registry is None:
        raise RuntimeError("I17 producer registry is unavailable")
    if registry.get("intake_closed") is True:
        handle = _bind_prepared_room_child(
            service, authority, coordinator, registry, root, existing, close_receipt,
        )
    else:
        handle = _bind_open_room_child(
            service, authority, coordinator, registry, root, existing, close_receipt,
        )
    profile = str(getattr(authority, "profile_id", ""))
    if current_service is None:
        # Publish the service only after the exact child admission succeeds; an unproven prepared
        # successor must not leave a mutable service object attached to the authority.
        authority.hosted_room_service = service
    service._i17_runtime_custody = coordinator
    service._i17_service_root = root
    service._i17_room_handle = handle
    service._i17_profile_authority = authority
    service._i17_room_started = False
    entry = {
        "state": "registered", "owner_id": f"room-service:{profile}",
        "profile_id": root.profile_id, "authority_epoch": root.authority_epoch,
        "process_instance": root.process_instance, "service": service,
        "authority": authority, "root": root, "handle": handle,
        "runtime": getattr(service, "runtime", None),
        "db": getattr(authority, "db", None),
        "member_rpcs": getattr(service, "member_rpcs", None),
    }
    registry["room"] = entry
    authority._i17_room_producer_entry = entry
    service._i17_room_producer_entry = entry
    return handle


def room_service_start_failed(service):
    handle = getattr(service, "_i17_room_handle", None)
    coordinator = getattr(service, "_i17_runtime_custody", None)
    if handle is None or coordinator is None:
        return
    authority = getattr(service, "_i17_profile_authority", None)
    registry = _producer_registry(authority)
    producer = registry.get("room") if registry is not None else None
    if not _room_entry_is_current(authority, producer, service, handle):
        coordinator.mark_unknown(handle, "room_start_unknown")
        return
    runtime = getattr(service, "runtime", None)
    try:
        running = bool(runtime is not None and runtime.status().get("running"))
    except BaseException:  # health: allow BLE001 -- unreadable runtime cannot prove no worker started.
        running = True
    if running or getattr(service, "_i17_room_started", False):
        coordinator.mark_unknown(handle, "room_start_unknown")
    else:
        coordinator.release_unaccepted(handle)
        if authority is not None:
            producer = _producer_registry(authority)
            producer = producer.get("room") if producer is not None else None
            if isinstance(producer, dict) and producer.get("service") is service:
                producer["state"] = "absent"
                authority._i17_room_producer_entry = None
                service._i17_room_producer_entry = None


def _room_runtime_stopped(service) -> bool:
    runtime = getattr(service, "runtime", None)
    if runtime is None:
        return False
    try:
        status = runtime.status()
        if status.get("running") or status.get("current_tasks"):
            return False
        with getattr(runtime, "_status_lock", _NullLock()):
            thread = getattr(runtime, "_thread", None)
            room_threads = tuple(getattr(runtime, "_room_threads", {}).values())
    except BaseException:  # health: allow BLE001 -- unreadable stop state must remain UNKNOWN.
        return False
    return (thread is None or not thread.is_alive()) and not any(
        room_thread.is_alive() for room_thread in room_threads)


class _NullLock:
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False


def room_service_stop(service, stopped: bool):
    """Terminalize the in-process hosted-room child only after every worker joined."""
    handle = getattr(service, "_i17_room_handle", None)
    coordinator = getattr(service, "_i17_runtime_custody", None)
    if handle is None or coordinator is None:
        return True
    identity = _room_terminal_identity(service)
    if stopped is not True or identity is None or not _room_runtime_stopped(service):
        coordinator.mark_unknown(handle, "room_close_unknown")
        return False
    authority = getattr(service, "_i17_profile_authority", None)
    registry = _producer_registry(authority)
    producer = registry.get("room") if registry is not None else None
    if not _room_entry_is_current(authority, producer, service, handle):
        coordinator.mark_unknown(handle, "room_close_unknown")
        return False
    refusal = coordinator.mark_terminal(handle)
    if refusal is not None:
        coordinator.mark_unknown(handle, "room_close_unknown")
        return False
    receipt = {
        "stopped": True,
        "running": False,
        "workers_joined": True,
    }
    service._i17_room_terminal_identity = identity
    if authority is not None:
        authority._i17_room_terminal_identity = identity
        registry = _producer_registry(authority)
        if registry is not None:
            producer = registry.get("room")
            if isinstance(producer, dict) and producer.get("service") is service:
                producer.update({
                    "state": "terminal", "receipt": receipt,
                    "terminal_identity": identity,
                })
                receipt = producer["receipt"]
                authority._i17_room_terminal_receipt = receipt
                service._i17_room_terminal_receipt = receipt
    if getattr(service, "_i17_room_terminal_receipt", None) is None:
        service._i17_room_terminal_receipt = receipt
    return True


def _room_terminal_identity(service):
    """Capture the exact runtime/thread objects observed at a real room stop.

    The legacy three-field receipt is intentionally kept stable for existing callers.  This
    separate source-owned observation prevents a boolean ``stop`` result from certifying a
    replacement runtime or a worker whose thread/accepted task was only *reported* as done.
    """
    runtime = getattr(service, "runtime", None)
    if runtime is None:
        return None
    try:
        status = runtime.status()
        with getattr(runtime, "_status_lock", _NullLock()):
            supervisor_thread = getattr(runtime, "_thread", None)
            room_threads = tuple(getattr(runtime, "_room_threads", {}).values())
            current_tasks = tuple(getattr(runtime, "_current_tasks", {}).values())
    except BaseException:  # health: allow BLE001 -- unreadable runtime retains UNKNOWN custody.
        return None
    if (
        not isinstance(status, dict)
        or not isinstance(status.get("process_generation"), str)
        or not status.get("process_generation")
        or status.get("running") is not False
        or status.get("current_tasks")
        or status.get("room_threads")
        or current_tasks
        or supervisor_thread is not None and supervisor_thread.is_alive()
        or any(thread.is_alive() for thread in room_threads)
    ):
        return None
    return {
        "runtime": runtime,
        "process_generation": status["process_generation"],
        "supervisor_thread": supervisor_thread,
        "room_threads": room_threads,
        "workers_joined": True,
    }


def _settle(handle, coordinator, *, known: bool, reason: str):
    if handle is None or coordinator is None:
        return True
    if known:
        refusal = coordinator.mark_terminal(handle)
        return refusal is None
    coordinator.mark_unknown(handle, reason)
    return False


def submit_turn(supervisor, frame: dict, callback):
    """Reserve a child before the real host write and settle it after callback consumption."""
    parent = getattr(supervisor, "_i17_host_handle", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    if parent is None or coordinator is None:
        return supervisor.submit_turn(frame, on_complete=callback)
    request_id = str(frame.get("request_id") or frame.get("turn_id") or "")
    child = _child(parent, f"turn:{request_id}", coordinator)

    def completed(done):
        try:
            result = callback(done) if callback is not None else False
        except BaseException:
            _settle(child, coordinator, known=False, reason="callback_failed")
            raise
        known = result is True and isinstance(done, dict) and done.get("custody") != "unknown"
        if not _settle(child, coordinator, known=known, reason="turn_settlement_unknown"):
            if known:
                return False
        return result

    completed._requires_custody = True
    try:
        return supervisor.submit_turn(frame, on_complete=completed)
    except BaseException as exc:
        ambiguous = bool(getattr(exc, "custody_unknown", False)
                         or getattr(exc, "lease_release_pending", False))
        if ambiguous or supervisor.has_pending_turn(str(frame.get("sid") or "")):
            _settle(child, coordinator, known=False, reason="turn_admission_unknown")
        else:
            coordinator.release_unaccepted(child)
        raise


def control(supervisor, sid: str, *, route_name: str, payload: dict | None,
            wait: bool, timeout: float, on_late_ack=None):
    """Custody-wrap an existing host control/worker handoff."""
    parent = getattr(supervisor, "_i17_host_handle", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    if parent is None or coordinator is None:
        return supervisor.control(sid, route_name=route_name, payload=payload,
                                  wait=wait, timeout=timeout, on_late_ack=on_late_ack)
    request_id = str((payload or {}).get("request_id") or route_name)
    child = _child(parent, f"control:{sid}:{request_id}", coordinator)
    callback_accepted = True

    def late_ack(ack):
        nonlocal callback_accepted
        try:
            if on_late_ack is not None:
                callback_accepted = on_late_ack(ack) is not False
        except BaseException:
            callback_accepted = False
            _settle(child, coordinator, known=False, reason="control_callback_failed")
            raise
        _settle(child, coordinator, known=callback_accepted,
                reason="control_settlement_unknown")

    try:
        response = supervisor.control(
            sid, route_name=route_name, payload=payload, wait=wait,
            timeout=timeout, on_late_ack=late_ack if on_late_ack is not None else None)
    except BaseException:
        if on_late_ack is None:
            _settle(child, coordinator, known=False, reason="control_settlement_unknown")
        raise
    if wait:
        if not _settle(child, coordinator, known=isinstance(response, dict),
                       reason="control_settlement_unknown"):
            raise RuntimeError("I17 control settlement is unknown")
    elif on_late_ack is None:
        _settle(child, coordinator, known=False, reason="control_settlement_unknown")
    # With ``wait=False`` and a callback, the child remains active until that callback is
    # actually invoked. A timeout or missing late ACK is therefore retained as UNKNOWN.
    return response


def rpc_admission(req: dict):
    """Reserve one direct RPC worker beneath the active profile root, if gateway-bound."""
    child_bridge = _child_bridge()
    if child_bridge is not None:
        return child_bridge.admit(req)
    if os.environ.get("HERMES_COMPUTE_HOST_CHILD") == "1":
        return None, "child_custody_bridge_missing"
    runner, coordinator = _runtime()
    if runner is None or coordinator is None:
        return None, None
    params = req.get("params") if isinstance(req, dict) else None
    owner = params.get("owner") if isinstance(params, dict) else None
    profile = owner.get("profile_id") if isinstance(owner, dict) else None
    from gateway.session_authorities import active_authority, authority_for_home
    authority = (authority_for_home(runner, Path(profile)) if isinstance(profile, str) and profile
                 else active_authority(runner))
    if authority is None:
        return None, "profile_mismatch"
    root = bind_authority(authority, runner=runner, coordinator=coordinator)
    if root is None:
        return None, "runtime_custody_unavailable"
    rid = str(req.get("id") or "notification")
    method = str(req.get("method") or "rpc")
    try:
        return _child(root, f"rpc:{method}:{rid}", coordinator), None
    except RuntimeError as exc:
        return None, str(exc)


def rpc_settle(handle, *, known: bool):
    if handle is None:
        if os.environ.get("HERMES_COMPUTE_HOST_CHILD") == "1" and _child_bridge() is None:
            return False
        return True
    from tui_gateway.i17_child_bridge import ChildProducerToken
    if isinstance(handle, ChildProducerToken):
        bridge = _child_bridge()
        return bridge is not None and bridge.settle(handle, known=known)
    _runner, coordinator = _runtime()
    return _settle(handle, coordinator, known=known, reason="rpc_worker_unknown")


def child_operation_admission(frame: dict):
    """Admit one child control under the authenticated host-parent contract."""
    bridge = _child_bridge()
    if bridge is not None:
        method = str(frame.get("route_name") or frame.get("type") or "control")
        request = {
            "method": method,
            "params": {"owner": {"profile_id": bridge.identity.profile_id}},
        }
        return bridge.admit(request)
    if os.environ.get("HERMES_COMPUTE_HOST_CHILD") == "1":
        return None, "child_custody_bridge_missing"
    return None, None


def child_operation_settle(token, *, known: bool) -> bool:
    if token is None:
        return True
    bridge = _child_bridge()
    return bridge is not None and bridge.settle(token, known=known)


def _profile_host_evidence(authority):
    receipt = getattr(authority, "_i17_host_shutdown_receipt", None)
    room_receipt = getattr(authority, "_i17_room_terminal_receipt", None)
    registry = _producer_registry(authority)
    if registry is None:
        return False, (receipt, room_receipt)
    terminal_registry = getattr(authority, "_i17_terminal_registry", None)
    intake_closed = registry.get("intake_closed") is True
    # I09 reopens its intake fence before I17's authenticated abort rebind.  A terminal root may
    # therefore carry ``intake_closed=False`` during that one reversible window, but only when
    # the exact source registry object that was terminalized is still attached to this authority.
    reopened_for_abort = (
        getattr(authority, "_i17_service_closed", False) is True
        and terminal_registry is registry
    )
    if not _registry_identity_valid(authority, registry) or not (intake_closed or reopened_for_abort):
        return False, (receipt, room_receipt)
    host_ok = _host_producer_evidence(authority, registry.get("host"), receipt)
    room_ok = _room_producer_evidence(authority, registry.get("room"), room_receipt)
    return host_ok and room_ok, (receipt, room_receipt)


def _registry_identity_valid(authority, registry) -> bool:
    if registry is None:
        return False
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    binding = getattr(authority, "_i17_rebind_source", None)
    return bool(
        coordinator is not None
        and registry.get("process_instance") == coordinator.process_instance
        and registry.get("profile_id") == str(getattr(authority, "profile_id", ""))
        and registry.get("authority_epoch") == getattr(authority, "epoch", None)
        and registry.get("authority") is authority
        and binding is not None
        and registry.get("source_binding") is binding
        and binding.authority is authority
        and binding.coordinator is coordinator
        and binding.registry is registry
        and registry.get("runner") is binding.runner
    )


def validate_i17_cold_abort_participant(runner, coordinator, transition, entry) -> bool:
    """Validate one cold successor before its authenticated post-OPEN completion."""
    if type(entry) is not tuple or len(entry) != 3:
        return False
    authority, receipt, capability = entry
    if not isinstance(capability, I17ColdPostOpenCapability):
        return False
    if (
        capability.runner is not runner
        or capability.coordinator is not coordinator
        or capability.authority is not authority
        or capability.receipt is not receipt
        or capability.registry is not getattr(authority, "_i17_producer_registry", None)
        or capability.binding is not getattr(authority, "_i17_rebind_source", None)
        or capability.root is not getattr(authority, "_i17_service_handle", None)
        or getattr(authority, "_i17_service_closed", False) is True
        or getattr(authority, "hosted_room_service", None) is not None
        or not _registry_identity_valid(authority, capability.registry)
        or capability.registry.get("intake_closed") is not True
        or capability.registry.get("prepared_successor_pending") is not True
        or capability.registry.get("host", {}).get("state") != "absent"
        or capability.registry.get("room", {}).get("state") != "absent"
    ):
        return False
    transition_receipt = getattr(transition, "receipt", None)
    fields = ("receipt_id", "process_instance", "authority_epoch", "close_generation")
    if any(getattr(transition_receipt, field, None) != getattr(receipt, field, None)
           for field in fields):
        return False
    check = getattr(coordinator, "check_post_open_transition", None)
    return callable(check) and check(transition)


def complete_i17_cold_abort(capability, coordinator, transition) -> bool:
    """Validate one cold registry before consuming its coordinator-issued token."""
    entry = i17_cold_abort_participant(capability)
    return validate_i17_cold_abort_participant(
        capability.runner, coordinator, transition, entry,
    )


def clear_i17_cold_abort(capability, transition) -> bool:
    """Drop the source capability only after the coordinator consumed the exact token."""
    runner = capability.runner
    completed = getattr(capability.coordinator, "completed_post_open_transition_for", None)
    if not callable(completed) or completed(capability.receipt) is not transition:
        return False
    capabilities = tuple(getattr(runner, "_i17_post_open_capabilities", ()))
    if not any(item is capability for item in capabilities):
        return False
    registry = capability.registry
    if (
        registry.get("intake_closed") is not True
        or registry.get("prepared_successor_pending") is not True
        or registry.get("host", {}).get("state") != "absent"
        or registry.get("room", {}).get("state") != "absent"
        or getattr(capability.authority, "_i17_producer_registry", None) is not registry
    ):
        return False
    registry["intake_closed"] = False
    registry["prepared_successor_pending"] = False
    capability.authority._i17_prepared_successor_pending = False
    capability.authority._i17_terminal_registry = None
    runner._i17_post_open_capabilities = tuple(
        item for item in capabilities if item is not capability
    )
    return True


def _host_producer_evidence(authority, producer, receipt) -> bool:
    if not isinstance(producer, dict) or producer.get("state") == "absent":
        return (
            receipt is None
            and getattr(authority, "_i17_host_producer_entry", None) is None
            and not _unregistered_host_present(authority)
        )
    supervisor = producer.get("supervisor")
    handle = producer.get("handle")
    if (
        producer.get("state") != "terminal"
        or supervisor is None
        or handle is None
        or not _host_entry_is_current(authority, producer, supervisor, handle)
    ):
        return False
    if producer.get("profile_id") != str(getattr(authority, "profile_id", "")):
        return False
    stored_receipt = producer.get("receipt")
    terminal_identity = producer.get("terminal_identity")
    if not _host_receipt_valid(receipt) or stored_receipt is not receipt:
        return False
    if not isinstance(terminal_identity, dict):
        return False
    return (
        terminal_identity.get("supervisor") is supervisor
        and terminal_identity.get("root") is producer.get("root")
        and terminal_identity.get("handle") is handle
        and terminal_identity.get("native_identity") is producer.get("native_identity")
        and _host_native_identity_unchanged(
            supervisor, terminal_identity.get("native_identity"),
        )
        and terminal_identity.get("host_generation") == receipt.get("host_generation")
        and terminal_identity.get("host_boot_id") == receipt.get("host_boot_id")
        and terminal_identity.get("host_pid") == receipt.get("host_pid")
        and not _has_active_isolated_sessions(authority)
        and not _has_live_host(authority)
    )


def _room_producer_evidence(authority, producer, receipt) -> bool:
    if not isinstance(producer, dict) or producer.get("state") == "absent":
        service = getattr(authority, "hosted_room_service", None)
        # Service construction is a prepared source, not a worker effect.  A cold profile may
        # therefore have a configured service before its room child is registered; accept that
        # no-work state only after the real runtime reports no live worker and no child handle.
        return (
            receipt is None
            and getattr(authority, "_i17_room_producer_entry", None) is None
            and (service is None or (
                getattr(service, "_i17_room_handle", None) is None
                and not _has_live_room_service(authority)
            ))
        )
    service = producer.get("service")
    current_service = getattr(authority, "hosted_room_service", None)
    return (
        producer.get("state") == "terminal"
        and service is not None
        and current_service is service
        and _room_entry_is_current(authority, producer, service, producer.get("handle"))
        and producer.get("profile_id") == str(getattr(authority, "profile_id", ""))
        and producer.get("receipt") is receipt
        and _room_receipt_valid(receipt)
        and _room_terminal_identity_matches(service, producer.get("terminal_identity"))
        and not _has_live_room_service(authority)
    )


def _unregistered_host_present(authority) -> bool:
    home = str(getattr(authority, "profile_id", ""))
    server = _server()
    sessions = getattr(server, "_sessions", None) if server is not None else None
    if not isinstance(sessions, dict):
        return server is not None
    if any(str(item.get("profile_home") or home) == home for item in sessions.values()):
        return True
    bridge = sys.modules.get("tui_gateway.compute_host_bridge")
    supervisors = getattr(bridge, "_compute_host_supervisors", None) if bridge is not None else None
    if not isinstance(supervisors, dict):
        return bridge is not None
    return any(str(getattr(item, "_i17_profile_home", "")) == home for item in supervisors.values())


def _host_receipt_valid(receipt) -> bool:
    required = ("clean", "process_exited", "natural_exit", "stdout_eof", "stderr_eof",
                "completion_eof", "shutdown_ack", "exit_settled")
    return (
        isinstance(receipt, dict)
        and all(receipt.get(key) is True for key in required)
        and type(receipt.get("host_generation")) is int
        and receipt.get("host_generation") >= 1
        and isinstance(receipt.get("host_boot_id"), str)
        and bool(receipt.get("host_boot_id"))
        and type(receipt.get("host_pid")) is int
        and receipt.get("host_pid") > 0
    )


def _room_receipt_valid(receipt) -> bool:
    return (
        isinstance(receipt, dict)
        and receipt.get("stopped") is True
        and receipt.get("running") is False
        and receipt.get("workers_joined") is True
    )


def _room_terminal_identity_matches(service, identity) -> bool:
    if not isinstance(identity, dict):
        return False
    runtime = getattr(service, "runtime", None)
    if runtime is None or identity.get("runtime") is not runtime:
        return False
    try:
        status = runtime.status()
        with getattr(runtime, "_status_lock", _NullLock()):
            supervisor_thread = getattr(runtime, "_thread", None)
            room_threads = tuple(getattr(runtime, "_room_threads", {}).values())
            current_tasks = tuple(getattr(runtime, "_current_tasks", {}).values())
    except BaseException:  # health: allow BLE001 -- replacement/unreadable runtime stays UNKNOWN.
        return False
    if (
        not isinstance(status, dict)
        or status.get("process_generation") != identity.get("process_generation")
        or status.get("running") is not False
        or status.get("current_tasks")
        or status.get("room_threads")
        or current_tasks
        or supervisor_thread is not identity.get("supervisor_thread")
        or tuple(room_threads) != tuple(identity.get("room_threads", ()))
        or supervisor_thread is not None and supervisor_thread.is_alive()
        or any(thread.is_alive() for thread in room_threads)
    ):
        return False
    profile_authority = getattr(service, "_i17_profile_authority", None)
    service_authority = getattr(service, "authority", profile_authority)
    if profile_authority is not None and service_authority is not profile_authority:
        return False
    # A canonical hosted service must have completed transport installation before it may
    # contribute a terminal source receipt.  Small legacy unit doubles do not expose this flag;
    # their existing three-field tests remain covered by the local runtime identity above.
    installed = getattr(service, "_transport_installed", None)
    return installed is not False


async def _stage_terminal_room_authority(authority, deadline, loop):
    """Stage one source-owned room stop without consuming its custody handle."""
    registry = _producer_registry(authority)
    if not _registry_identity_valid(authority, registry):
        return False
    if registry.get("intake_closed") is not True:
        return False
    if not _settle_registered_host(authority):
        return False
    room = registry.get("room")
    if not isinstance(room, dict):
        return False
    if room.get("state") == "absent":
        return None if _room_producer_evidence(authority, room, None) else False
    if room.get("state") == "terminal":
        receipt = getattr(authority, "_i17_room_terminal_receipt", None)
        return None if _room_producer_evidence(authority, room, receipt) else False
    service = room.get("service")
    if room.get("state") != "registered" or service is None:
        return False
    if service is not getattr(authority, "hosted_room_service", None):
        return False
    if not _room_entry_is_current(authority, room, service, room.get("handle")):
        return False
    runtime = getattr(service, "runtime", None)
    if runtime is None:
        return False
    try:
        before = runtime.status()
    except BaseException:  # health: allow BLE001 -- unreadable worker stays UNKNOWN.
        return False
    remaining = deadline - loop.time()
    if remaining <= 0:
        return False
    try:
        stopped = await asyncio.to_thread(service.stop, timeout=remaining)
    except BaseException:  # health: allow BLE001 -- stop failure retains exact custody.
        return False
    return authority, service, stopped, before.get("running") is True


async def _stage_terminal_room_services(authorities, deadline, loop):
    staged = []
    for authority in authorities:
        result = await _stage_terminal_room_authority(authority, deadline, loop)
        if result is False:
            return staged, False
        if result is not None:
            staged.append(result)
    return staged, True


def _rollback_terminal_room_services(staged):
    for _authority, service, _stopped, was_running in reversed(staged):
        if not was_running:
            continue
        try:
            service.start()
        except BaseException:  # health: allow BLE001 -- rollback failure retains source custody.
            _LOG.exception("I17 hosted-room rollback failed; retaining source custody")


def _retain_partial_terminalization(staged):
    """Keep exact stopped objects and roots UNKNOWN after a late sibling finalization failure."""
    snapshot = []
    for authority, service, _stopped, _was_running in staged:
        root = getattr(authority, "_i17_service_handle", None)
        handle = getattr(service, "_i17_room_handle", None)
        coordinator = getattr(authority, "_i17_runtime_custody", None)
        snapshot.append((authority, service, root, handle))
        if root is not None and coordinator is not None:
            coordinator.mark_unknown(root, "i17_partial_terminalization")
        authority._i17_terminalization_partial = tuple(snapshot)


async def terminalize_preservation_rooms(runner, timeout: float) -> bool:
    """Stop exact registered hosted rooms before I17 sealing and retain them for abort."""
    from gateway.session_authorities import all_authorities

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout))
    staged, valid = await _stage_terminal_room_services(
        tuple(all_authorities(runner)), deadline, loop,
    )
    # Do not consume any child handle until every real runtime has stopped.  If a later service
    # misses its deadline, restart the exact earlier objects while their children remain owned.
    if not valid or any(
        stopped is not True or _room_terminal_identity(service) is None
        for _authority, service, stopped, _was_running in staged
    ):
        _rollback_terminal_room_services(staged)
        return False
    for authority, service, _stopped, was_running in staged:
        if not room_service_stop(service, True):
            _retain_partial_terminalization(staged)
            return False
        if was_running:
            authority._i17_terminal_room_service = service
            authority._i17_terminal_room_was_running = True
    return True


def _has_active_isolated_sessions(authority) -> bool:
    server = _server()
    sessions = getattr(server, "_sessions", {}) if server is not None else {}
    home = str(getattr(authority, "profile_id", ""))
    return any(str(session.get("profile_home") or home) == home and (
        session.get("_compute_host_active") or session.get("_compute_host_turn_id")
    ) for session in sessions.values())


def _has_live_host(authority) -> bool:
    bridge = sys.modules.get("tui_gateway.compute_host_bridge")
    supervisors = getattr(bridge, "_compute_host_supervisors", {}) if bridge is not None else {}
    home = str(getattr(authority, "profile_id", ""))
    for supervisor in supervisors.values():
        if str(getattr(supervisor, "_i17_profile_home", "")) == home:
            if getattr(supervisor, "_proc", None) is not None or supervisor.is_running():
                return True
    return False


def _settle_registered_host(authority) -> bool:
    """Consume the real compute-host shutdown receipt before I17 can seal a profile."""
    registry = _producer_registry(authority)
    producer = registry.get("host") if registry is not None else None
    if not isinstance(producer, dict) or producer.get("state") == "absent":
        return True
    supervisor = producer.get("supervisor")
    handle = producer.get("handle")
    if producer.get("state") == "terminal":
        return _host_entry_is_current(authority, producer, supervisor, handle)
    if (
        producer.get("state") != "registered"
        or supervisor is None
        or not _host_entry_is_current(authority, producer, supervisor, handle)
    ):
        return False
    # A reservation with no Popen/owner effect is a pre-effect start failure.  It must be settled
    # by host_start_failed, not by manufacturing a shutdown receipt from an unstarted supervisor.
    if not _host_effect_started(supervisor):
        return False
    supervisor._i17_shutdown_native_identity = _host_native_identity(supervisor)
    try:
        receipt = supervisor.shutdown()
    except BaseException:  # health: allow BLE001 -- native shutdown failure retains the exact host handle.
        host_shutdown_failed(supervisor)
        return False
    return host_shutdown(supervisor, receipt)


def _has_live_room_service(authority) -> bool:
    service = getattr(authority, "hosted_room_service", None)
    if service is None:
        return False
    runtime = getattr(service, "runtime", None)
    if runtime is None:
        return True
    try:
        status = runtime.status()
    except BaseException:  # health: allow BLE001 -- unreadable room state cannot prove clean close.
        return True
    return bool(status.get("running") or status.get("current_tasks"))


def close_profile(authority):
    """Terminalize a profile root only after the coordinator and source graph agree."""
    root = getattr(authority, "_i17_service_handle", None)
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    if root is None or coordinator is None:
        return True
    if getattr(authority, "_i17_service_closed", False):
        return True
    registry = _producer_registry(authority)
    if registry is None:
        coordinator.mark_unknown(root, "missing_source_hook")
        return False
    # Closing intake is a source-owned transition.  It prevents a producer from appearing between
    # the graph snapshot and exact root terminalization; abort reopens it only after a fresh root.
    registry["intake_closed"] = True
    if not _settle_registered_host(authority):
        coordinator.mark_unknown(root, "host_close_unknown")
        return False
    valid, _receipt = _profile_host_evidence(authority)
    if not valid:
        coordinator.mark_unknown(root, "host_close_unknown")
        return False
    coordinator.reconcile_unknown(
        root, owner_id=root.owner_id, profile_id=root.profile_id,
        process_instance=root.process_instance, authority_epoch=root.authority_epoch)
    refusal = coordinator.mark_terminal(root)
    if refusal is not None:
        return False
    # Keep the exact source registry identity through I09's reversible intake reopen.  This is
    # a binding fact, not a second ownership ledger: the coordinator still owns the root lease,
    # while the registry object proves that abort is reusing the same terminal graph.
    authority._i17_terminal_registry = registry
    authority._i17_service_closed = True
    return True


def release_profile_source(authority):
    """Revoke a terminal profile's source binding before its authority is removed.

    ``close_profile`` intentionally keeps the binding available for an authenticated reversible
    abort.  Permanent profile retirement has a separate boundary: once the exact terminal root is
    settled and the authority is about to leave the registry, the coordinator must revoke that
    source capability so a successor with the same profile name can bind cleanly.
    """
    root = getattr(authority, "_i17_service_handle", None)
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    binding = getattr(authority, "_i17_rebind_source", None)
    if root is None and coordinator is None and binding is None:
        return True
    if root is None or coordinator is None or binding is None:
        return False
    refusal = coordinator._unregister_source_rebind_binding(
        _OWNER_CLASS, str(getattr(authority, "profile_id", "")), binding,
        terminal_handle=root,
    )
    return refusal is None


def owner_evidence(runner, receipt):
    """Close all real profile roots and return one cross-process owner receipt."""
    coordinator = getattr(runner, "runtime_custody", None)
    if coordinator is None:
        return None
    from gateway.session_authorities import all_authorities
    authorities = tuple(all_authorities(runner))
    profiles = []
    snapshots = []
    for authority in authorities:
        try:
            if getattr(authority, "_i17_service_closed", False) is True:
                # The handoff ACK path re-reads I17 after its first seal.  A terminal root is
                # already the exact source-owned predecessor; calling bind_authority without the
                # authenticated abort proof would try to mint a replacement root and turn an
                # otherwise valid reread into UNKNOWN.  Validate the retained graph in place.
                root = getattr(authority, "_i17_service_handle", None)
                if (
                    root is None
                    or not coordinator.is_terminal_handle(root, owner_class=_OWNER_CLASS)
                    or not _profile_host_evidence(authority)[0]
                ):
                    return None
            else:
                bind_authority(authority, runner=runner, coordinator=coordinator)
        except (RuntimeError, TypeError, ValueError):
            return None
        if not close_profile(authority):
            return None
        valid, _receipts = _profile_host_evidence(authority)
        registry = _producer_registry(authority)
        if not valid or registry is None:
            return None
        profiles.append(str(getattr(authority, "profile_id", "")))
        snapshots.append({
            "profile_id": registry["profile_id"],
            "authority_epoch": registry["authority_epoch"],
            "process_instance": registry["process_instance"],
            "host": registry["host"].get("state"),
            "room": registry["room"].get("state"),
        })
    if not profiles:
        return None
    from gateway.runtime_custody import OWNER_SPECS
    receipts = tuple(getattr(authority, "_i17_host_shutdown_receipt", None)
                     for authority in authorities)
    room_receipts = tuple(getattr(authority, "_i17_room_terminal_receipt", None)
                          for authority in authorities)
    return {
        "source": OWNER_SPECS[_OWNER_CLASS].source,
        "process_instance": receipt.process_instance,
        "authority_epoch": receipt.authority_epoch,
        "close_generation": receipt.close_generation,
        "owner_id": f"{receipt.process_instance}:I17",
        "observed": True,
        "cross_process_proof": bool(snapshots) and all(
            item["host"] in {"absent", "terminal"}
            and item["room"] in {"absent", "terminal"}
            for item in snapshots
        ),
        "profile_roots": tuple(profiles),
        "producer_graph": tuple(snapshots),
        "host_receipts": tuple(item for item in receipts if item is not None),
        "room_receipts": tuple(item for item in room_receipts if item is not None),
    }


def seal_owner(runner, coordinator, receipt):
    """Publish I17's source-owned cross-process close evidence for this generation."""
    evidence = owner_evidence(runner, receipt)
    if evidence is None:
        coordinator.mark_owner_unknown(_OWNER_CLASS, "cross_process_unproven")
        return False
    refusal = coordinator.seal_owner(_OWNER_CLASS, evidence)
    if refusal is not None:
        coordinator.mark_owner_unknown(_OWNER_CLASS, getattr(refusal, "code", "read_failed"))
        return False
    return True


def _producer_graph_digest(registry, receipts):
    def receipt_facts(value):
        if not isinstance(value, dict):
            return None
        keys = (
            "host_generation", "host_boot_id", "host_pid", "clean",
            "process_exited", "natural_exit", "stdout_eof", "stderr_eof",
            "completion_eof", "shutdown_ack", "exit_settled", "stopped",
            "running", "workers_joined",
        )
        return {key: value.get(key) for key in keys if key in value}

    graph = {
        "source": registry["source"],
        "process_instance": registry["process_instance"],
        "profile_id": registry["profile_id"],
        "authority_epoch": registry["authority_epoch"],
        "intake_closed": registry["intake_closed"],
        "host": {
            "state": registry["host"].get("state"),
            "profile_id": registry["host"].get("profile_id"),
            "receipt": receipt_facts(receipts[0]),
        },
        "room": {
            "state": registry["room"].get("state"),
            "profile_id": registry["room"].get("profile_id"),
            "receipt": receipt_facts(receipts[1]),
            "terminal_identity": (
                {
                    "process_generation": registry["room"].get(
                        "terminal_identity", {}).get("process_generation"),
                    "workers_joined": registry["room"].get(
                        "terminal_identity", {}).get("workers_joined"),
                }
                if isinstance(registry["room"].get("terminal_identity"), dict)
                else None
            ),
        },
    }
    encoded = json.dumps(graph, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _build_abort_rebind_proof(authority, receipt):
    """Build an immutable proof from the profile's real closed producer graph."""
    from gateway.runtime_custody import TerminalRootRebindProof

    root = getattr(authority, "_i17_service_handle", None)
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    registry = _producer_registry(authority)
    if root is None or coordinator is None or registry is None:
        return None
    valid, receipts = _profile_host_evidence(authority)
    if not valid or not getattr(authority, "_i17_service_closed", False):
        return None
    if getattr(receipt, "close_generation", None) is None:
        return None

    binding = getattr(authority, "_i17_rebind_source", None)
    if binding is None or binding.registry is not registry:
        return None
    if type(root.physical_owner_epoch) is not str or not root.physical_owner_epoch:
        return None
    return TerminalRootRebindProof(
        source=_PRODUCER_REGISTRY_SOURCE,
        predecessor=root,
        predecessor_owner_id=root.owner_id,
        process_instance=coordinator.process_instance,
        profile_id=str(getattr(authority, "profile_id", "")),
        authority_epoch=authority.epoch,
        close_generation=receipt.close_generation,
        physical_owner_epoch=root.physical_owner_epoch,
        producer_graph_digest=_producer_graph_digest(registry, receipts),
        producer_registry=registry,
        source_binding=binding,
    )


def restore_after_abort(runner, receipt=None) -> bool:
    """Restore every profile root after an authenticated reversible-abort.

    ``close_profile`` may have retired the old root while preparing the handoff.  Only that exact
    terminal predecessor is eligible for a fresh reservation; authorities left UNKNOWN remain
    refused so an abort cannot turn an incomplete producer graph into a ready runtime.
    """
    from gateway.session_authorities import all_authorities

    ok = True
    cold_capabilities = []
    for authority in tuple(all_authorities(runner)):
        registry = _producer_registry(authority)
        if getattr(authority, "_i17_service_closed", False):
            terminal_room_service = getattr(authority, "_i17_terminal_room_service", None)
            try:
                if receipt is None:
                    # Preserve the established post-abort API for callers that already released
                    # the coordinator receipt; the lifecycle owner has proved OPEN separately.
                    bind_authority(authority, runner=runner, coordinator=runner.runtime_custody)
                else:
                    proof = _build_abort_rebind_proof(authority, receipt)
                    if proof is None:
                        ok = False
                        continue
                    bind_authority(
                        authority, runner=runner, coordinator=runner.runtime_custody,
                        close_receipt=receipt, source_proof=proof,
                    )
                if terminal_room_service is not None:
                    if terminal_room_service is not getattr(authority, "hosted_room_service", None):
                        raise RuntimeError("I17 terminal room service was replaced")
                    if not getattr(authority, "_i17_terminal_room_was_running", False):
                        raise RuntimeError("I17 terminal room restore state is missing")
                    bind_room_service(
                        terminal_room_service, authority,
                        close_receipt=receipt if receipt is not None else None,
                    )
                    try:
                        terminal_room_service.start()
                    except BaseException:
                        room_service_start_failed(terminal_room_service)
                        raise
                    terminal_room_service._i17_room_started = True
                    restored_registry = _producer_registry(authority)
                    if restored_registry is not None:
                        restored_registry["intake_closed"] = False
                    authority._i17_prepared_successor_pending = False
                    authority._i17_terminal_room_service = None
                    authority._i17_terminal_room_was_running = False
                    authority._i17_terminal_registry = None
                elif receipt is not None:
                    registry = _producer_registry(authority)
                    binding = getattr(authority, "_i17_rebind_source", None)
                    root = getattr(authority, "_i17_service_handle", None)
                    if (
                        registry is None
                        or binding is None
                        or root is None
                        or registry.get("intake_closed") is not True
                        or registry.get("prepared_successor_pending") is not True
                        or registry.get("host", {}).get("state") != "absent"
                        or registry.get("room", {}).get("state") != "absent"
                    ):
                        raise RuntimeError("I17 cold successor witness is unavailable")
                    cold_capabilities.append(I17ColdPostOpenCapability(
                        runner=runner, coordinator=runner.runtime_custody,
                        authority=authority, registry=registry, binding=binding,
                        root=root, receipt=receipt,
                    ))
            except (RuntimeError, TypeError, ValueError):
                ok = False
        elif registry is not None and registry.get("intake_closed"):
            # A failed close left an accepted root/UNKNOWN producer state.  Keep it fail-closed;
            # the owner must reconcile that source receipt before another admission.
            ok = False
    if ok:
        runner._i17_post_open_capabilities = tuple(cold_capabilities)
    return ok
