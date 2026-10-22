"""Runtime-custody adapter for hosted TUI and compute-host work.

The runtime coordinator is the only ownership ledger.  This module only attaches its
opaque handles to the real authority, supervisor, and pending-request objects that
already own the work; it never mirrors those objects in a second registry.
"""

from __future__ import annotations

import sys
import os
import hashlib
import json
from pathlib import Path
from typing import Any

_OWNER_CLASS = "I17_hosted_tui_services"
_PRODUCER_REGISTRY_SOURCE = "tui_gateway.hosted_tui_custody:i17-producer-registry-v1"
_RUNTIME_RUNNER = None
_RUNTIME_COORDINATOR = None
_CHILD_BRIDGE = None


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
    _reset_producer_registry(authority, coordinator, runner=runner)
    binding.update_registry(_producer_registry(authority))
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


def _reset_producer_registry(authority, coordinator, *, runner=None) -> None:
    """Reopen the exact source graph after abort; no prior terminal receipt is reused."""
    profile_id = str(getattr(authority, "profile_id", ""))
    epoch = getattr(authority, "epoch", None)
    if not profile_id or type(epoch) is not int or epoch < 1:
        raise RuntimeError("I17 authority identity is unavailable")
    authority._i17_producer_registry = _new_producer_registry(
        coordinator, profile_id=profile_id, authority_epoch=epoch,
        authority=authority, runner=runner,
    )


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
    refusal = coordinator._register_source_rebind_binding(
        _OWNER_CLASS, str(getattr(authority, "profile_id", "")), binding,
    )
    if refusal is not None:
        raise RuntimeError(
            f"I17 source binding refused: {getattr(refusal, 'code', 'unknown')}"
        )
    return binding


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
        return existing
    if existing is not None and not coordinator.is_terminal_handle(existing, owner_class=_OWNER_CLASS):
        raise RuntimeError("I17 host producer handle is stale")
    handle = _child(root, f"host:{owner_id}", coordinator)
    supervisor._i17_runtime_custody = coordinator
    supervisor._i17_service_root = root
    supervisor._i17_host_handle = handle
    supervisor._i17_profile_home = str(Path(home).resolve())
    supervisor._i17_profile_authority = authority
    registry["host"] = {
        "state": "registered", "owner_id": f"host:{owner_id}",
        "profile_id": root.profile_id, "authority_epoch": root.authority_epoch,
        "process_instance": root.process_instance, "supervisor": supervisor,
    }
    from tui_gateway.i17_child_bridge import build_identity
    supervisor._i17_child_bridge = build_identity(
        root, handle, profile_home=str(Path(home).resolve()))
    return True


def _host_effect_started(supervisor) -> bool:
    return any(getattr(supervisor, name, None) is not None for name in (
        "_proc", "_owner", "_registry_lease", "_producer_identity", "_registry_owner_key"))


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
        or producer.get("state") != "registered"
        or producer.get("supervisor") is not supervisor
        or producer.get("profile_id") != handle.profile_id
        or producer.get("authority_epoch") != handle.authority_epoch
        or producer.get("process_instance") != handle.process_instance
        or producer.get("owner_id") != handle.owner_id
    ):
        return False
    if coordinator.release_unaccepted(handle) is not None:
        return False
    producer["state"] = "absent"
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
    coordinator.reconcile_unknown(
        handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
        process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
    refusal = coordinator.mark_terminal(handle)
    if refusal is not None:
        return False
    authority = getattr(supervisor, "_i17_profile_authority", None)
    if authority is not None:
        authority._i17_host_shutdown_receipt = dict(receipt)
        registry = _producer_registry(authority)
        if registry is not None:
            producer = registry.get("host")
            if isinstance(producer, dict) and producer.get("supervisor") is supervisor:
                producer.update({"state": "terminal", "receipt": dict(receipt)})
    supervisor._i17_terminal_receipt = dict(receipt)
    return True


def bind_room_service(service, authority):
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
    registry = _producer_registry(authority)
    if registry is None or registry.get("intake_closed") is True:
        raise RuntimeError("I17 producer intake is closed or unregistered")
    if existing is not None and coordinator.is_active_handle(existing, owner_class=_OWNER_CLASS):
        return existing
    if existing is not None and not coordinator.is_terminal_handle(existing, owner_class=_OWNER_CLASS):
        raise RuntimeError("I17 room producer handle is stale")
    profile = str(getattr(authority, "profile_id", ""))
    handle = _child(root, f"room-service:{profile}", coordinator)
    service._i17_runtime_custody = coordinator
    service._i17_service_root = root
    service._i17_room_handle = handle
    service._i17_profile_authority = authority
    service._i17_room_started = False
    registry["room"] = {
        "state": "registered", "owner_id": f"room-service:{profile}",
        "profile_id": root.profile_id, "authority_epoch": root.authority_epoch,
        "process_instance": root.process_instance, "service": service,
    }
    return handle


def room_service_start_failed(service):
    handle = getattr(service, "_i17_room_handle", None)
    coordinator = getattr(service, "_i17_runtime_custody", None)
    if handle is None or coordinator is None:
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
    if stopped is not True or not _room_runtime_stopped(service):
        coordinator.mark_unknown(handle, "room_close_unknown")
        return False
    refusal = coordinator.mark_terminal(handle)
    if refusal is not None:
        return False
    receipt = {
        "stopped": True,
        "running": False,
        "workers_joined": True,
    }
    service._i17_room_terminal_receipt = receipt
    authority = getattr(service, "_i17_profile_authority", None)
    if authority is not None:
        authority._i17_room_terminal_receipt = dict(receipt)
        registry = _producer_registry(authority)
        if registry is not None:
            producer = registry.get("room")
            if isinstance(producer, dict) and producer.get("service") is service:
                producer.update({"state": "terminal", "receipt": dict(receipt)})
    return True


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
    if not _registry_identity_valid(authority, registry) or not registry.get("intake_closed"):
        return False, (receipt, room_receipt)
    host_ok = _host_producer_evidence(authority, registry.get("host"), receipt)
    room_ok = _room_producer_evidence(authority, registry.get("room"), room_receipt)
    return host_ok and room_ok, (receipt, room_receipt)


def _registry_identity_valid(authority, registry) -> bool:
    if registry is None:
        return False
    coordinator = getattr(authority, "_i17_runtime_custody", None)
    return bool(
        coordinator is not None
        and registry.get("process_instance") == coordinator.process_instance
        and registry.get("profile_id") == str(getattr(authority, "profile_id", ""))
        and registry.get("authority_epoch") == getattr(authority, "epoch", None)
    )


def _host_producer_evidence(authority, producer, receipt) -> bool:
    if not isinstance(producer, dict) or producer.get("state") == "absent":
        return receipt is None and not _unregistered_host_present(authority)
    supervisor = producer.get("supervisor")
    if producer.get("state") != "terminal" or supervisor is None:
        return False
    if producer.get("profile_id") != str(getattr(authority, "profile_id", "")):
        return False
    if not _host_receipt_valid(receipt):
        return False
    return (
        receipt.get("host_generation") == getattr(supervisor, "_host_generation", None)
        and receipt.get("host_boot_id") == getattr(supervisor, "_host_boot_id", None)
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
            and (service is None or (
                getattr(service, "_i17_room_handle", None) is None
                and not _has_live_room_service(authority)
            ))
        )
    service = producer.get("service")
    return (
        producer.get("state") == "terminal"
        and service is not None
        and producer.get("profile_id") == str(getattr(authority, "profile_id", ""))
        and _room_receipt_valid(receipt)
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
    authority._i17_service_closed = True
    return True


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
    for authority in tuple(all_authorities(runner)):
        registry = _producer_registry(authority)
        if getattr(authority, "_i17_service_closed", False):
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
            except (RuntimeError, TypeError, ValueError):
                ok = False
        elif registry is not None and registry.get("intake_closed"):
            # A failed close left an accepted root/UNKNOWN producer state.  Keep it fail-closed;
            # the owner must reconcile that source receipt before another admission.
            ok = False
    return ok
