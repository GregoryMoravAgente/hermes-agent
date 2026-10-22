"""Neutral external-process operation parent adapter.

The runtime custody coordinator remains the only ledger. This module composes its existing
I16/I04 handles with profile authority/lock and ServiceProcessReceipt so providers can share
one explicit producer policy without manufacturing a turn binding or spawning unowned work.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import logging
from pathlib import Path
import uuid
from typing import Any, ContextManager, Literal

from hermes_state_runtime import RuntimeStoreError


logger = logging.getLogger(__name__)

ProcessMode = Literal[
    "managed_turn", "live_profile_discovery", "standalone_maintenance", "injected_no_spawn",
]


class ExternalProcessOperationError(RuntimeError):
    """A typed refusal before an external process can create an effect."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def retire_per_invocation_parent(
    service: Any, binding: Any, request: Any, physical: Any,
) -> None:
    """Retire one shell parent only after its physical delivery fence is closed."""
    from gateway.runtime_service_custody import ServiceCustodyError

    owner = getattr(service, "external_parent", None)
    if owner is None or getattr(owner, "delivery_lifecycle", "persistent") != "per_invocation":
        return
    if (
        getattr(owner, "_service", None) is not service
        or getattr(owner, "profile_authority", None) is not binding.authority
        or getattr(owner, "turn_binding", None) is not binding
        or getattr(owner, "root_handle", None) is not service.handle
        or getattr(owner, "_physical", None) is not physical
        or getattr(owner, "_request", None) is not request
        or physical.state != "closed"
    ):
        raise ServiceCustodyError("external_parent_identity_mismatch")
    if owner.state == "terminal":
        forget = getattr(getattr(owner, "profile_authority", None), "forget_external_process_finalizer", None)
        if callable(forget) and not forget(owner.operation_id, owner):
            raise ServiceCustodyError("external_parent_registry_retire_failed")
        owner._request = None
        owner._physical = None
        return
    if owner.state == "unknown":
        restored = service.coordinator.reconcile_unknown(
            owner.root_handle,
            owner_id=owner.root_handle.owner_id,
            profile_id=binding.profile_id,
            process_instance=binding.process_instance,
            authority_epoch=binding.authority_epoch,
        )
        if restored is not None:
            raise ServiceCustodyError("external_parent_reconcile_failed")
    result = service.coordinator.mark_terminal(owner.root_handle)
    if result is not None:
        owner.state = "unknown"
        raise ServiceCustodyError("external_parent_retire_failed")
    owner.state = "terminal"
    forget = getattr(getattr(owner, "profile_authority", None), "forget_external_process_finalizer", None)
    if callable(forget) and not forget(owner.operation_id, owner):
        raise ServiceCustodyError("external_parent_registry_retire_failed")
    owner._request = None
    owner._physical = None


@dataclass
class ExternalProcessParent:
    """Exact profile/authority/operation identity passed to an external producer."""

    mode: ProcessMode
    profile_home: Path
    profile_id: str
    operation_id: str
    source_identity: str
    principal_id: str | None
    process_instance: str
    authority_epoch: int
    coordinator: Any
    root_handle: Any
    # The source graph class is carried with the same parent object that owns the service.
    producer_key: str | None = None
    turn_binding: Any = None
    profile_authority: Any = None
    lock_owner: Any = None
    # The physical profile-lock incarnation is separate from the authority/process epoch.
    # It is captured from the real ProfileOwnership reservation and is never inferred from
    # a truthy owner, PID, or service map.
    physical_owner_epoch: str | None = None
    finalizer: Any = None
    state: str = "parent_reserved"
    # A one-shot hook parent is consumed by the canonical delivery finalizer.  Persistent
    # ACP/app-server parents deliberately retain request_reserved ownership across turns.
    delivery_lifecycle: Literal["persistent", "per_invocation"] = "persistent"
    # Private references are the same existing ServiceLease/receipt, never a second ledger.
    _service: Any = field(default=None, repr=False)
    _physical: Any = field(default=None, repr=False)
    _maintenance_context: Any = field(default=None, repr=False)
    _maintenance_hold_released: bool = field(default=False, repr=False)
    _profile_db: Any = field(default=None, repr=False)
    _catalog_client: Any = field(default=None, repr=False)
    _owner_agent: Any = field(default=None, repr=False)
    _request: Any = field(default=None, repr=False)
    # Remote I16 routes retain the exact SDK transport/session receipt on this
    # object.  The SessionAuthority finalizer index remains the only registry.
    _remote_receipt: Any = field(default=None, repr=False)


@dataclass(frozen=True)
class ProducerPolicy:
    """Explicit spawn/cleanup policy; missing context never selects a permissive mode."""

    mode: ProcessMode
    parent: ExternalProcessParent | None
    require_parent_before_spawn: bool
    allow_existing_session_only: bool
    legacy_tree_kill_allowed: bool


@dataclass(frozen=True)
class ExternalPublicationProof:
    """Source-owned result/cache proof for a non-I04 external operation."""

    operation_id: str
    profile_id: str
    process_instance: str
    authority_epoch: int
    source_identity: str
    result_identity: str
    durable_result: bool = False
    cache_readback: bool = False
    cache_identity: str | None = None
    payload_digest: str | None = None


def provider_process_kwargs(agent: Any, profile: Any, client_kwargs: dict) -> dict:
    """Attach the explicit external-process policy at the provider boundary."""
    external = (
        getattr(profile, "auth_type", None) == "external_process"
        or str(client_kwargs.get("base_url", "") or "").lower().startswith("acp://")
    )
    if not external:
        return client_kwargs
    producer_key = (
        "acp" if str(client_kwargs.get("base_url", "") or "").lower().startswith(("acp://", "acp+tcp://"))
        else "provider"
    )
    policy = getattr(agent, "_external_process_policy", None) or ensure_agent_process_policy(
        agent, producer_key=producer_key,
    )
    parent = getattr(policy, "parent", None)
    if parent is not None:
        agent._external_process_parent = parent
        agent._external_process_parent_context = getattr(parent, "_maintenance_context", None)
        parent._owner_agent = agent
    return {**client_kwargs, "producer_policy": policy}


def _external_provider(provider: Any, base_url: Any = None) -> bool:
    if str(base_url or "").lower().startswith("acp://"):
        return True
    try:
        from hermes_cli.runtime_provider_backends import _is_external_process_provider
        return _is_external_process_provider(provider)
    except (ImportError, AttributeError, TypeError, ValueError):
        logger.debug("external provider detection failed", exc_info=True)
        return False


def _db_home(profile_db: Any) -> Path:
    path = getattr(profile_db, "db_path", None)
    if path is None:
        raise ExternalProcessOperationError("owner_epoch_unavailable")
    return _canonical_home(Path(path).expanduser().resolve().parent)


def _current_physical_owner_epoch(profile_home: Path) -> str:
    """Read the currently held physical profile-lock incarnation.

    This is intentionally a strict read of the existing ProfileOwnership API.  A missing or
    stale reservation is an ownership refusal; no process/PID or empty-map fallback is valid.
    """
    from gateway.runtime_ownership import process_ownership
    epoch = process_ownership.owner_epoch(profile_home)
    if type(epoch) is not str or not epoch:
        raise ExternalProcessOperationError("physical_owner_unknown")
    return epoch


def _policy_parent_is_current(policy: Any, binding: Any) -> bool:
    parent = getattr(policy, "parent", None)
    try:
        from gateway.runtime_ownership import process_ownership
        physical_current = (
            isinstance(parent, ExternalProcessParent)
            and process_ownership.reservation_is_current(
                parent.profile_home, parent.physical_owner_epoch,
            )
        )
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        physical_current = False
    return (
        isinstance(parent, ExternalProcessParent)
        and getattr(parent, "turn_binding", None) is binding
        and getattr(parent, "state", None) not in {"unknown", "terminal"}
        and getattr(parent, "process_instance", None) == getattr(binding, "process_instance", None)
        and getattr(parent, "authority_epoch", None) == getattr(binding, "authority_epoch", None)
        and getattr(parent, "coordinator", None) is getattr(binding, "coordinator", None)
        and getattr(parent, "profile_id", None) == getattr(binding, "profile_id", None)
        and getattr(parent, "profile_authority", None) is getattr(binding, "authority", None)
        and getattr(parent, "_service", None) is not None
        and getattr(parent, "principal_id", None) in {None, getattr(binding, "principal_id", None)}
        and getattr(parent, "source_identity", None) in {
            getattr(binding, "source_identity", None), getattr(binding, "principal_id", None),
        }
        and physical_current
    )


def _clear_context_parent(context: Any) -> None:
    parent = getattr(context, "_external_process_parent", None)
    if parent is None:
        return
    if parent.state != "terminal":
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    release_agent_process_parent(context)
    context._external_process_parent = None
    context._external_process_parent_context = None
    context._external_process_policy = None


def _managed_turn_policy(
    runner: Any,
    context: Any,
    binding: Any,
    *,
    producer_key: str,
    cached_policy: ProducerPolicy | None = None,
) -> ProducerPolicy:
    current = getattr(context, "_external_process_policy", None)
    if (
        current is None
        and isinstance(cached_policy, ProducerPolicy)
        and cached_policy.mode == "managed_turn"
    ):
        current = cached_policy
    if current is not None:
        if _policy_parent_is_current(current, binding):
            parent = current.parent
            if parent._service is not None:
                parent._service.validate_binding(binding)
            return validate_producer_policy(current, spawning=True)
        parent = getattr(current, "parent", None)
        service = getattr(parent, "_service", None)
        if (
            isinstance(parent, ExternalProcessParent)
            and parent.mode == "managed_turn"
            and service is not None
            and parent.state not in {"unknown", "terminal"}
            and not service.unknown
            and not service.requests
            and not service.durable_requests
            and _persistent_service_physical_ready(parent, service)
            and _persistent_service_identity_matches(parent, binding)
        ):
            request = _begin_managed_request(binding, service, parent)
            parent.turn_binding = binding
            parent._request = request
            parent.state = "request_reserved"
            policy = policy_for_parent(parent, mode="managed_turn")
            context._external_process_parent = parent
            context._external_process_policy = policy
            return policy
        if getattr(parent, "state", None) not in {"terminal"}:
            raise ExternalProcessOperationError("preservation_owner_unmapped")
        _clear_context_parent(context)
    from gateway.runtime_service_custody import reserve_service
    owner_id = f"external:{binding.admission_id}"
    service = reserve_service(binding, owner_id, producer_key=producer_key)
    parent = parent_for_managed_service(binding, service, operation_id=binding.admission_id)
    _register_authority_finalizer(binding.authority, parent)
    request = _begin_managed_request(binding, service, parent)
    parent._request = request
    context._external_process_parent = parent
    policy = policy_for_parent(parent, mode="managed_turn")
    context._external_process_policy = policy
    return policy


def _persistent_service_identity_matches(parent: ExternalProcessParent, binding: Any) -> bool:
    """Allow a new admission only on the same authenticated service owner."""
    return (
        getattr(parent, "coordinator", None) is getattr(binding, "coordinator", None)
        and getattr(parent, "profile_id", None) == getattr(binding, "profile_id", None)
        and getattr(parent, "authority_epoch", None) == getattr(binding, "authority_epoch", None)
        and getattr(parent, "process_instance", None) == getattr(binding, "process_instance", None)
        and getattr(parent, "profile_authority", None) is getattr(binding, "authority", None)
        and getattr(parent, "principal_id", None) in {None, getattr(binding, "principal_id", None)}
        and getattr(parent, "source_identity", None) in {
            getattr(binding, "source_identity", None), getattr(binding, "principal_id", None),
        }
    )


def _persistent_service_physical_ready(parent: ExternalProcessParent, service: Any) -> bool:
    """Permit settled persistent app-server custody to cross an admission boundary."""
    physical = getattr(service, "physical", None)
    if physical is None:
        return True
    return (
        physical is getattr(parent, "_physical", None)
        and getattr(physical, "ephemeral_process", False) is False
        and getattr(physical, "state", None) == "process_bound"
        and getattr(physical, "process", None) is not None
        and getattr(physical, "unknown_reason", None) is None
        and not getattr(service, "unknown", False)
        and not getattr(physical, "durable_proofs", None)
        and not getattr(physical, "delivery_requests", None)
    )


def _begin_managed_request(binding: Any, service: Any, parent: ExternalProcessParent):
    """Reserve the I04-linked request before a managed provider is constructed."""
    request = service.begin_request(binding)
    binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
    if not callable(binder):
        service.finish_request(binding, durable=False)
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    try:
        binder(binding.admission_id, binding.parent_handle, service, request)
    except BaseException:
        try:
            service.finish_request(binding, durable=False)
        except RuntimeError:
            logger.debug("managed external request cleanup failed", exc_info=True)
        marker = getattr(binding.authority, "_preservation_mark_unknown", None)
        if callable(marker):
            try:
                marker(binding.admission_id, "join_failed")
            except RuntimeError:
                logger.debug("managed admission unknown marker failed", exc_info=True)
        raise
    return request


def begin_cua_request(service: Any, binding: Any, parent: ExternalProcessParent):
    """Bind a cached CUA service to one exact current I04 request.

    The persistent service root and its physical receipt remain the same across
    cached turns.  Only ``ServiceLease.begin_request`` may create the new request
    child, after the old request map is empty and the parent/service edges still
    point at the same objects.
    """
    from gateway.runtime_service_custody import ServiceCustodyError
    if (
        getattr(service, "external_parent", None) is not parent
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "root_handle", None) is not getattr(service, "handle", None)
        or getattr(service, "unknown", False)
        or getattr(service, "requests", None)
    ):
        raise ServiceCustodyError("cua_request_owner_mismatch")
    old = getattr(service, "binding", None)
    if old is not None and any(
        getattr(old, field, None) != getattr(binding, field, None)
        for field in (
            "profile_id", "process_instance", "authority_epoch", "session_id",
            "source_identity", "principal_id",
        )
    ):
        raise ServiceCustodyError("cua_request_owner_mismatch")
    if old is not None and (
        getattr(old, "authority", None) is not getattr(binding, "authority", None)
        or getattr(old, "coordinator", None) is not getattr(binding, "coordinator", None)
    ):
        raise ServiceCustodyError("cua_request_owner_mismatch")
    service.binding = binding
    try:
        return service.begin_request(binding)
    except BaseException:
        service.binding = old
        raise


def register_cua_source_factory(authority: Any, factory: Any) -> Any:
    """Bind one profile CUA factory before public computer-use ingress."""
    current = authority._cua_source_factory
    if current is not None and current is not factory:
        raise RuntimeStoreError("cua_factory_registry_collision")
    if getattr(factory, "authority", None) is not authority:
        raise RuntimeStoreError("cua_factory_registry_mismatch")
    authority._cua_source_factory = factory
    return factory


def cua_source_factory(authority: Any) -> Any:
    """Return the exact profile CUA factory, if bootstrap installed one."""
    return authority._cua_source_factory


def retire_cua_sidecar(authority: Any, admission_id: Any, request: Any) -> None:
    """Stage the exact source request for the canonical service forget fence."""
    request_id = getattr(getattr(request, "lease", None), "lease_id", None)
    entries = authority._preservation_sidecars
    entry = entries.get(request_id)
    if (
        entry is None or entry["request"] is not request
        or entry["admission_id"] != admission_id
        or entry["result_durable"] or entry["delivery_complete"]
    ):
        raise RuntimeStoreError("stale_service_request")
    entry["terminal"] = True


def forget_cua_sidecar(authority: Any, admission_id: Any, request: Any) -> None:
    """Forget only a source request whose exact terminal fence was recorded."""
    request_id = getattr(getattr(request, "lease", None), "lease_id", None)
    entries = authority._preservation_sidecars
    entry = entries.get(request_id)
    if entry is None or entry["request"] is not request or entry["admission_id"] != admission_id:
        raise RuntimeStoreError("stale_service_request")
    if not entry["terminal"]:
        raise RuntimeStoreError("service_request_not_terminal")
    entries.pop(request_id, None)


def _validate_cua_request(service: Any, binding: Any, request: Any, parent: ExternalProcessParent) -> None:
    from gateway.runtime_service_custody import ServiceCustodyError
    if (
        getattr(service, "external_parent", None) is not parent
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "root_handle", None) is not getattr(service, "handle", None)
        or getattr(service, "requests", {}).get(getattr(binding, "admission_id", None)) is not request
        or getattr(getattr(request, "lease", None), "parent_lease_id", None)
        != getattr(getattr(service.handle, "lease", None), "lease_id", None)
        or getattr(service, "binding", None) is not binding
    ):
        service.coordinator.mark_unknown(parent.root_handle, "cua_request_identity_mismatch")
        raise ServiceCustodyError("cua_request_identity_mismatch")
    service.validate_binding(binding)


def _retire_cua_sidecar(binding: Any, request: Any, service: Any) -> None:
    retire = getattr(getattr(binding, "authority", None), "_preservation_retire_sidecar", None)
    if not callable(retire):
        return
    try:
        retire(binding.admission_id, request)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
        service.unknown = True
        from gateway.runtime_service_custody import ServiceCustodyError
        raise ServiceCustodyError("cua_sidecar_retire_failed") from exc


def _cua_request_is_terminal(service: Any, request: Any) -> bool:
    records = getattr(service.coordinator, "_records", None)
    lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
    record = records.get(lease_id) if isinstance(records, dict) else None
    return getattr(record, "handle", None) is request and getattr(record, "state", None) == "terminal"


def _cua_root_is_terminal(parent: ExternalProcessParent) -> bool:
    coordinator = getattr(parent, "coordinator", None)
    records = getattr(coordinator, "_records", None)
    lock = getattr(coordinator, "_lock", None)
    lease = getattr(getattr(parent, "root_handle", None), "lease", None)
    lease_id = getattr(lease, "lease_id", None)
    if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
        return False
    with lock:
        record = records.get(lease_id)
        return (
            getattr(record, "handle", None) is getattr(parent, "root_handle", None)
            and getattr(record, "state", None) == "terminal"
        )


def release_cua_request(service: Any, binding: Any, request: Any, parent: ExternalProcessParent) -> None:
    """Retire a CUA request that was refused before the native SDK effect."""
    from gateway.runtime_service_custody import ServiceCustodyError
    _validate_cua_request(service, binding, request, parent)
    _retire_cua_sidecar(binding, request, service)
    if not _cua_request_is_terminal(service, request) and service.coordinator.mark_terminal_with_secondary_parents(
        request, (binding.parent_handle,),
    ) is not None:
        service.unknown = True
        raise ServiceCustodyError("cua_release_failed")


def complete_cua_request(
    service: Any, binding: Any, request: Any, parent: ExternalProcessParent, receipt: Any,
) -> None:
    """Retire one closed CUA request through the existing secondary-parent edge."""
    from gateway.runtime_service_custody import ServiceCustodyError
    _validate_cua_request(service, binding, request, parent)
    current = getattr(parent, "_cua_receipt", None)
    if current is None or getattr(receipt, "owner", None) is not getattr(current, "owner", None):
        raise ServiceCustodyError("cua_receipt_identity_mismatch")
    if getattr(receipt, "state", None) != "closed" or receipt.readback() is None:
        service.unknown = True
        raise ServiceCustodyError("cua_receipt_unknown")
    _retire_cua_sidecar(binding, request, service)
    if not _cua_request_is_terminal(service, request) and service.coordinator.mark_terminal_with_secondary_parents(
        request, (binding.parent_handle,),
    ) is not None:
        service.unknown = True
        raise ServiceCustodyError("cua_request_finalize_failed")


def producer_policy_for_turn(
    runner: Any,
    context: Any,
    provider: Any,
    base_url: Any = None,
    *,
    cached_policy: ProducerPolicy | None = None,
) -> ProducerPolicy | None:
    """Select the authenticated owner before an external provider/client is constructed."""
    if not _external_provider(provider, base_url):
        return None
    producer_key = (
        "acp" if str(base_url or "").lower().startswith(("acp://", "acp+tcp://"))
        else "provider"
    )
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if binding is not None:
        return _managed_turn_policy(
            runner, context, binding, producer_key=producer_key, cached_policy=cached_policy,
        )
    current = getattr(context, "_external_process_policy", None)
    if current is not None:
        if getattr(current, "mode", None) == "managed_turn":
            raise ExternalProcessOperationError("preservation_owner_unmapped")
        return validate_producer_policy(current, spawning=True)
    db = getattr(runner, "_session_db", None)
    db = getattr(db, "_db", db)
    if db is None:
        raise ExternalProcessOperationError("owner_epoch_unavailable")
    home = _db_home(db)
    from gateway.session_authorities import authority_for_home
    authority = authority_for_home(runner, home)
    if authority is not None:
        parent = resolve_external_process_parent(
            runner, profile_home=home, operation_id=uuid.uuid4().hex,
            source_identity=str(provider), mode="live_profile_discovery", producer_key=producer_key,
        )
        policy = policy_for_parent(parent, mode="live_profile_discovery")
        context._external_process_parent = parent
        context._external_process_policy = policy
        return policy
    parent_context = reserve_standalone_external_process_parent(
        profile_home=home, profile_id=str(home), operation_id=uuid.uuid4().hex,
        source_identity=str(provider), profile_db=db, producer_key=producer_key,
    )
    parent = parent_context.__enter__()
    policy = policy_for_parent(parent, mode="standalone_maintenance")
    context._external_process_parent_context = parent_context
    context._external_process_parent = parent
    context._external_process_policy = policy
    return policy


def standalone_policy_for_session_db(
    provider: Any, profile_db: Any, *, producer_key: str,
    operation_id: str | None = None, base_url: Any = None,
) -> ProducerPolicy:
    """Acquire the real standalone owner used by direct ACP/model-discovery callers."""
    if not _external_provider(provider, base_url):
        raise ExternalProcessOperationError("invalid_process_mode")
    home = _db_home(profile_db)
    parent_context = reserve_standalone_external_process_parent(
        profile_home=home, profile_id=str(home), operation_id=operation_id or uuid.uuid4().hex,
        source_identity=str(provider), profile_db=profile_db, producer_key=producer_key,
    )
    parent = parent_context.__enter__()
    parent._maintenance_context = parent_context
    return policy_for_parent(parent, mode="standalone_maintenance")


def _finish_external_catalog_owner(profile_db: Any, policy: ProducerPolicy) -> bool:
    """Retire one catalog owner only after its exact child receipt is terminal."""
    parent = getattr(policy, "parent", None)
    if not isinstance(parent, ExternalProcessParent):
        return False
    if parent.state == "unknown" and not _reconcile_external_catalog_owner(parent):
        return False
    if parent.state != "terminal" and close_after_finalizers(parent) is not True:
        return False
    if parent.state != "terminal":
        return False
    context = getattr(parent, "_maintenance_context", None)
    if context is not None:
        context.__exit__(None, None, None)
    if getattr(profile_db, "_external_catalog_policy", None) is policy:
        delattr(profile_db, "_external_catalog_policy")
    from hermes_state_registry import release_or_close
    release_or_close(profile_db)
    return True


def _reconcile_external_catalog_owner(parent: ExternalProcessParent) -> bool:
    """Permit only a fully published, same-owner catalog retry to leave unknown."""
    physical = getattr(parent, "_physical", None)
    if physical is None or getattr(physical, "unknown_reason", None) not in {
        "catalog_discovery_failed", "cache_readback_unknown",
    }:
        return False
    proof = getattr(physical, "external_publication_proof", None)
    if (
        not _publication_proof_matches(parent, proof)
        or parent.operation_id not in getattr(physical, "completed_requests", set())
        or physical.process is None
        or physical.reader_error is not None
        or getattr(physical, "eof_streams", set()) != {"stdout", "stderr"}
        or getattr(physical, "joined_streams", set()) != {"stdout", "stderr"}
    ):
        return False
    return True


def _finalize_external_catalog_cache(
    policy: ProducerPolicy, provider: str, row: Any, result: Any, *, expected_fingerprint: str,
) -> None:
    """Consume the cache row as the source-owned durable publication receipt."""
    parent = getattr(policy, "parent", None)
    if not isinstance(parent, ExternalProcessParent) or parent._physical is None:
        return
    if getattr(parent._physical, "state", None) == "unknown":
        raise ExternalProcessOperationError("custody_unknown")
    if (
        type(expected_fingerprint) is not str
        or not expected_fingerprint
        or not isinstance(row, dict)
        or type(row.get("fp")) is not str
        or row.get("fp") != expected_fingerprint
        or not isinstance(row.get("models"), list)
    ):
        mark_unknown(parent, "cache_readback_unknown")
        raise ExternalProcessOperationError("cache_readback_unknown")
    expected = list(result) if isinstance(result, list) else None
    if expected is None or row.get("models") != expected:
        mark_unknown(parent, "cache_readback_unknown")
        raise ExternalProcessOperationError("cache_readback_unknown")
    client = getattr(parent, "_catalog_client", None)
    if client is None:
        mark_unknown(parent, "catalog_client_unmapped")
        raise ExternalProcessOperationError("catalog_client_unmapped")
    import hashlib
    import json
    payload = json.dumps(
        {"provider": provider, "fp": row["fp"], "models": row["models"]},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=16).hexdigest()
    client.finalize_external_catalog(
        result_identity=f"{provider}:{digest}",
        cache_identity=f"{provider}:{row['fp']}:{row.get('at')}",
        payload_digest=digest,
    )


def _retry_external_catalog_owner(parent: ExternalProcessParent) -> bool:
    """Reopen one same-owner discovery receipt before another child is spawned."""
    physical = getattr(parent, "_physical", None)
    if physical is None or getattr(physical, "unknown_reason", None) not in {
        "catalog_discovery_failed", "cache_readback_unknown",
    }:
        return False
    restored = parent.coordinator.reconcile_unknown(
        parent.root_handle, owner_id=parent.root_handle.owner_id,
        profile_id=parent.profile_id, process_instance=parent.process_instance,
        authority_epoch=parent.authority_epoch,
    )
    if restored is not None:
        return False
    if not physical.reopen_external_catalog_retry():
        parent.coordinator.mark_unknown(parent.root_handle, "catalog_retry_unproven")
        return False
    parent.state = "request_reserved"
    return True


def external_catalog_model_ids(
    provider: str, *, force_refresh: bool = False, non_blocking: bool = False,
) -> list[str]:
    """Run the direct model-discovery route under a real standalone owner."""
    from hermes_state_registry import acquire
    profile_db = acquire()
    policy = getattr(profile_db, "_external_catalog_policy", None)
    if policy is not None and getattr(getattr(policy, "parent", None), "state", None) == "unknown":
        if _finish_external_catalog_owner(profile_db, policy):
            from hermes_cli.models import _load_provider_models_cache
            normalized = str(provider).strip().lower()
            try:
                from hermes_cli.models import normalize_provider
                normalized = normalize_provider(provider) or normalized
            except (ImportError, AttributeError, TypeError, ValueError):
                logger.debug("provider normalization failed during retry recovery", exc_info=True)
            row = _load_provider_models_cache().get(normalized)
            if isinstance(row, dict) and isinstance(row.get("models"), list):
                return list(row["models"])
        if not _retry_external_catalog_owner(policy.parent):
            raise ExternalProcessOperationError("custody_unknown")
    try:
        if policy is None:
            policy = standalone_policy_for_session_db(provider, profile_db, producer_key="provider")
            profile_db._external_catalog_policy = policy
        else:
            validate_producer_policy(policy, spawning=True)
    except BaseException:
        from hermes_state_registry import release_or_close
        release_or_close(profile_db)
        raise
    try:
        from hermes_cli.models import _load_provider_models_cache
        from hermes_cli.models import cached_provider_model_ids
        from hermes_cli.models import _credential_fingerprint, normalize_provider
        normalized = str(provider).strip().lower()
        try:
            normalized = normalize_provider(provider) or normalized
        except (ImportError, AttributeError, TypeError, ValueError):
            logger.debug("provider normalization failed during catalog ownership", exc_info=True)
        # Capture the credential identity before the producer runs. The later
        # cache row must match this same authorizing snapshot exactly.
        expected_fingerprint = _credential_fingerprint(normalized)
        result = cached_provider_model_ids(
            provider, force_refresh=force_refresh, non_blocking=non_blocking, producer_policy=policy,
        )
        row = _load_provider_models_cache().get(normalized)
        _finalize_external_catalog_cache(
            policy, normalized, row, result, expected_fingerprint=expected_fingerprint,
        )
        if not _finish_external_catalog_owner(profile_db, policy):
            raise ExternalProcessOperationError("custody_unknown")
        return result
    except BaseException:
        _finish_external_catalog_owner(profile_db, policy)
        raise


def _require_text(value: Any, code: str) -> str:
    if type(value) is not str or not value:
        raise ExternalProcessOperationError(code)
    return value


def _canonical_home(value: Any) -> Path:
    try:
        home = Path(value).expanduser().resolve()
    except (OSError, TypeError, ValueError) as exc:
        raise ExternalProcessOperationError("profile_mismatch") from exc
    if not home.is_absolute():
        raise ExternalProcessOperationError("profile_mismatch")
    return home


def _coordinator_identity(coordinator: Any, *, process_instance: str, authority_epoch: int) -> None:
    if coordinator is None:
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    if getattr(coordinator, "process_instance", None) != process_instance:
        raise ExternalProcessOperationError("stale_authority_epoch")
    if getattr(coordinator, "authority_epoch", None) != authority_epoch:
        raise ExternalProcessOperationError("stale_authority_epoch")


def _reserve_root(
    coordinator: Any, owner_id: str, *, profile_id: str, authority_epoch: int, parent: Any = None,
):
    from gateway.runtime_custody import CustodyRefusal

    handle = coordinator.reserve_root(
        "I16_turn_sidecars", owner_id, profile_id=profile_id,
        authority_epoch=authority_epoch, parent=parent,
    )
    if isinstance(handle, CustodyRefusal):
        raise ExternalProcessOperationError(handle.code)
    return handle


def _parent_service(parent: ExternalProcessParent):
    if parent._service is None:
        # Standalone/discovery operations use the existing ServiceLease/Receipt identity
        # directly under their I16 root.  No synthetic TurnCustodyBinding or I04 parent is
        # manufactured; binding validation remains exclusive to the managed-turn service.
        from gateway.runtime_service_custody import service_for_external_parent
        parent._service = service_for_external_parent(
            parent.coordinator, parent.root_handle, parent.operation_id,
            producer_key=parent.producer_key,
        )
    return parent._service


def parent_for_managed_service(
    binding: Any, service: Any, *, operation_id: str | None = None,
    delivery_lifecycle: Literal["persistent", "per_invocation"] = "persistent",
) -> ExternalProcessParent:
    """View an already-reserved managed ServiceLease as an explicit producer parent."""
    from gateway.runtime_service_custody import ServiceCustodyError, TurnCustodyBinding

    if (
        not isinstance(binding, TurnCustodyBinding)
        or service is None
        or delivery_lifecycle not in {"persistent", "per_invocation"}
    ):
        raise ServiceCustodyError("preservation_owner_unmapped")
    if not binding.source_identity and not binding.principal_id:
        raise ServiceCustodyError("preservation_owner_mismatch")
    if (getattr(service, "coordinator", None) is not binding.coordinator
            or getattr(service, "binding", None) is not binding
            or getattr(service, "handle", None) is None):
        raise ServiceCustodyError("preservation_owner_mismatch")
    try:
        physical_owner_epoch = _current_physical_owner_epoch(_canonical_home(binding.profile_id))
    except ExternalProcessOperationError as exc:
        raise ServiceCustodyError(exc.code) from exc
    parent = ExternalProcessParent(
        mode="managed_turn", profile_home=_canonical_home(binding.profile_id),
        profile_id=binding.profile_id, operation_id=operation_id or binding.admission_id,
        source_identity=binding.source_identity or binding.principal_id,
        producer_key=getattr(service, "producer_key", None),
        principal_id=binding.principal_id or None,
        process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
        coordinator=binding.coordinator, root_handle=service.handle, turn_binding=binding,
        profile_authority=binding.authority, lock_owner=None, finalizer=service,
        physical_owner_epoch=physical_owner_epoch,
        state="request_reserved", delivery_lifecycle=delivery_lifecycle, _service=service,
    )
    service.external_parent = parent
    return parent


def resolve_external_process_parent(
    runner: Any, *, profile_home: Path, operation_id: str, source_identity: str,
    mode: Literal["managed_turn", "live_profile_discovery"], producer_key: str | None = None,
    principal_id: str | None = None, turn_binding: Any = None,
) -> ExternalProcessParent:
    """Resolve one explicitly selected live route before a discovery spawn.

    ``managed_turn`` is deliberately not a discovery fallback.  Its service lease is
    already reserved by the admission executor and must be adapted with
    :func:`parent_for_managed_service`; refusing here prevents a lost ContextVar from
    silently creating a second live-profile root.
    """
    if mode not in {"managed_turn", "live_profile_discovery"}:
        raise ExternalProcessOperationError("invalid_process_mode")
    from gateway.runtime_service_custody import TurnCustodyBinding
    if mode == "managed_turn":
        if not isinstance(turn_binding, TurnCustodyBinding):
            raise ExternalProcessOperationError("preservation_owner_unmapped")
        raise ExternalProcessOperationError("managed_service_required")
    if turn_binding is not None:
        raise ExternalProcessOperationError("preservation_owner_mismatch")
    profile_home = _canonical_home(profile_home)
    operation_id = _require_text(operation_id, "invalid_operation_id")
    source_identity = _require_text(source_identity, "invalid_source_identity")
    from gateway.session_authorities import authority_for_home, require_serving
    from gateway.runtime_ownership import process_ownership
    from gateway.runtime_i16_source_registry import explicit_i16_producer_key

    authority = authority_for_home(runner, profile_home)
    if authority is None:
        raise ExternalProcessOperationError("profile_mismatch")
    require_serving(authority)
    if process_ownership.owns(profile_home) is not True:
        raise ExternalProcessOperationError("foreign_custody")
    coordinator_getter = getattr(authority, "_preservation_coordinator", None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    process_instance = getattr(authority, "instance_id", None)
    authority_epoch = getattr(authority, "epoch", None)
    if type(process_instance) is not str or not process_instance or type(authority_epoch) is not int or authority_epoch < 1:
        raise ExternalProcessOperationError("stale_authority_epoch")
    _coordinator_identity(coordinator, process_instance=process_instance, authority_epoch=authority_epoch)
    producer_key = explicit_i16_producer_key(producer_key)
    registry = getattr(coordinator, "_i16_source_registry", None)
    if registry is not None and producer_key is None:
        raise ExternalProcessOperationError("i16_source_unregistered")
    if registry is not None:
        try:
            registry.require_producer(producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise ExternalProcessOperationError("i16_source_unregistered") from exc
    physical_owner_epoch = _current_physical_owner_epoch(profile_home)
    root = _reserve_root(
        coordinator, f"external:{operation_id}", profile_id=authority.profile_id,
        authority_epoch=authority_epoch,
    )
    parent = ExternalProcessParent(
        mode="live_profile_discovery", profile_home=profile_home,
        profile_id=authority.profile_id, operation_id=operation_id,
        source_identity=source_identity,
        producer_key=producer_key,
        principal_id=principal_id,
        process_instance=process_instance, authority_epoch=authority_epoch,
        coordinator=coordinator, root_handle=root, turn_binding=turn_binding,
        profile_authority=authority, lock_owner=process_ownership,
        finalizer=None, state="parent_reserved",
        physical_owner_epoch=physical_owner_epoch,
    )
    _parent_service(parent)
    try:
        _register_authority_finalizer(authority, parent)
    except BaseException:
        release = getattr(coordinator, "release_unaccepted", None)
        if callable(release):
            release(root)
        raise
    return parent


def _register_authority_finalizer(authority: Any, parent: ExternalProcessParent) -> None:
    register = getattr(authority, "register_external_process_finalizer", None)
    if not callable(register):
        raise ExternalProcessOperationError("finalizer_registry_missing")
    registered = register(parent.operation_id, parent)
    if registered is not parent:
        raise ExternalProcessOperationError("finalizer_registry_mismatch")


def _canonical_db_identity(profile_home: Path, profile_id: str, profile_db: Any) -> str:
    """Prove the DB and profile label identify the same canonical serving profile."""
    db_path = getattr(profile_db, "db_path", None)
    if db_path is None:
        raise ExternalProcessOperationError("owner_epoch_unavailable")
    try:
        resolved_db = Path(db_path).expanduser().resolve()
    except (OSError, TypeError, ValueError) as exc:
        raise ExternalProcessOperationError("profile_mismatch") from exc
    if resolved_db.parent != profile_home:
        raise ExternalProcessOperationError("profile_mismatch")
    canonical_id = str(profile_home)
    if profile_id != canonical_id:
        raise ExternalProcessOperationError("profile_mismatch")
    return canonical_id


def _release_maintenance_hold(parent: ExternalProcessParent) -> bool:
    """Release only this standalone parent token after its terminal proof."""
    if parent.mode != "standalone_maintenance" or parent._maintenance_context is None:
        return True
    if parent._maintenance_hold_released:
        return True
    if parent.lock_owner is None:
        return False
    if parent.physical_owner_epoch is None:
        return False
    if parent.lock_owner.reservation_is_current(
        parent.profile_home, parent.physical_owner_epoch,
    ) is not True:
        return False
    from gateway.runtime_ownership import release_maintenance_hold
    if release_maintenance_hold(parent.profile_home, parent, owner=parent.lock_owner) is not True:
        return False
    parent._maintenance_hold_released = True
    return True


@contextmanager
def reserve_standalone_external_process_parent(
    *, profile_home: Path, profile_id: str, operation_id: str, source_identity: str,
    profile_db: Any, producer_key: str | None = None, principal_id: str | None = None,
) -> ContextManager[ExternalProcessParent]:
    """Hold the existing profile lock and real runtime epoch through finalization."""
    profile_home = _canonical_home(profile_home)
    profile_id = _require_text(profile_id, "invalid_profile_id")
    operation_id = _require_text(operation_id, "invalid_operation_id")
    source_identity = _require_text(source_identity, "invalid_source_identity")
    from gateway.runtime_i16_source_registry import explicit_i16_producer_key
    producer_key = explicit_i16_producer_key(producer_key)
    if producer_key is None:
        raise ExternalProcessOperationError("i16_source_unregistered")
    if profile_db is None or getattr(profile_db, "db_path", None) is None:
        raise ExternalProcessOperationError("owner_epoch_unavailable")
    profile_id = _canonical_db_identity(profile_home, profile_id, profile_db)
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_i16_source_registry import bootstrap_standalone_i16_source_registry
    from gateway.runtime_ownership import (
        _maintenance,
        exclusive_maintenance,
        retain_maintenance,
    )
    from hermes_state_runtime import begin_runtime_epoch

    maintenance_context = exclusive_maintenance(
        [profile_home], purpose="external-process-operation",
    )
    maintenance_context.__enter__()
    try:
        instance_id = uuid.uuid4().hex
        try:
            epoch = begin_runtime_epoch(profile_db, instance_id=instance_id)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise ExternalProcessOperationError("owner_epoch_unavailable") from exc
        if type(epoch) is not int or epoch < 1:
            raise ExternalProcessOperationError("owner_epoch_unavailable")
        coordinator = RuntimeCustodyCoordinator(process_instance=instance_id, authority_epoch=epoch)
        bootstrap_standalone_i16_source_registry(
            coordinator,
            profile_id=profile_id,
            process_instance=instance_id,
            authority_epoch=epoch,
        )
        root = _reserve_root(
            coordinator, f"external:{operation_id}", profile_id=profile_id, authority_epoch=epoch,
        )
        maintenance = getattr(_maintenance, "state", None)
        lock_owner = maintenance[1] if isinstance(maintenance, tuple) and len(maintenance) == 2 else None
        physical_owner_epoch = (
            lock_owner.owner_epoch(profile_home) if lock_owner is not None else None
        )
        if type(physical_owner_epoch) is not str or not physical_owner_epoch:
            raise ExternalProcessOperationError("physical_owner_unknown")
        parent = ExternalProcessParent(
            mode="standalone_maintenance", profile_home=profile_home, profile_id=profile_id,
            operation_id=operation_id, source_identity=source_identity, principal_id=principal_id,
            process_instance=instance_id, authority_epoch=epoch, coordinator=coordinator,
            root_handle=root, producer_key=producer_key, turn_binding=None,
            profile_authority=None, lock_owner=lock_owner,
            physical_owner_epoch=physical_owner_epoch,
            finalizer=None, state="parent_reserved", _service=None,
            _maintenance_context=maintenance_context, _profile_db=profile_db,
        )
        if parent.lock_owner is None:
            raise ExternalProcessOperationError("preservation_owner_unmapped")
        # The exact parent is the finalizer token.  Retain the existing lock through
        # an ambiguous child/reader result before the context manager can unwind.
        retain_maintenance(parent.profile_home, parent, owner=parent.lock_owner)
    except BaseException as exc:
        maintenance_context.__exit__(type(exc), exc, exc.__traceback__)
        raise
    try:
        yield parent
    finally:
        if parent.state != "terminal":
            parent.state = "unknown"
            parent.coordinator.mark_unknown(parent.root_handle, "custody_unknown")
        else:
            if not _release_maintenance_hold(parent):
                parent.state = "unknown"
                try:
                    parent.coordinator.mark_unknown(parent.root_handle, "maintenance_hold_release_failed")
                finally:
                    raise ExternalProcessOperationError("maintenance_hold_release_failed")
            maintenance_context.__exit__(None, None, None)


def policy_for_parent(parent: ExternalProcessParent, *, mode: ProcessMode) -> ProducerPolicy:
    if not isinstance(parent, ExternalProcessParent) or parent.state in {"unknown", "terminal"}:
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    if mode not in {"managed_turn", "live_profile_discovery", "standalone_maintenance"}:
        raise ExternalProcessOperationError("invalid_process_mode")
    if parent.mode != mode:
        raise ExternalProcessOperationError("preservation_owner_mismatch")
    policy = ProducerPolicy(mode, parent, True, False, False)
    validate_producer_policy(policy, spawning=True)
    return policy


def policy_without_spawn() -> ProducerPolicy:
    policy = ProducerPolicy("injected_no_spawn", None, False, True, False)
    validate_producer_policy(policy, spawning=False)
    return policy


def validate_producer_policy(policy: Any, *, spawning: bool) -> ProducerPolicy:
    """Fail closed on policy shape before any provider can create a process."""
    if not isinstance(policy, ProducerPolicy):
        raise ExternalProcessOperationError("producer_policy_missing")
    if any(type(value) is not bool for value in (
        policy.require_parent_before_spawn, policy.allow_existing_session_only,
        policy.legacy_tree_kill_allowed,
    )):
        raise ExternalProcessOperationError("producer_policy_invalid")
    if policy.mode == "injected_no_spawn":
        if policy.parent is not None or policy.require_parent_before_spawn is not False or \
                policy.allow_existing_session_only is not True or policy.legacy_tree_kill_allowed:
            raise ExternalProcessOperationError("producer_policy_invalid")
        if spawning:
            raise ExternalProcessOperationError("injected_no_spawn")
        return policy
    if policy.mode not in {"managed_turn", "live_profile_discovery", "standalone_maintenance"}:
        raise ExternalProcessOperationError("invalid_process_mode")
    if not isinstance(policy.parent, ExternalProcessParent) or policy.parent.mode != policy.mode:
        raise ExternalProcessOperationError("preservation_owner_mismatch")
    if policy.require_parent_before_spawn is not True or policy.allow_existing_session_only is not False \
            or policy.legacy_tree_kill_allowed is not False:
        raise ExternalProcessOperationError("producer_policy_invalid")
    if policy.parent.state in {"unknown", "terminal"}:
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    registry = getattr(policy.parent.coordinator, "_i16_source_registry", None)
    if registry is not None:
        from gateway.runtime_i16_source_registry import explicit_i16_producer_key
        producer_key = explicit_i16_producer_key(getattr(policy.parent, "producer_key", None))
        if producer_key is None:
            raise ExternalProcessOperationError("i16_source_unregistered")
        try:
            registry.require_producer(producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise ExternalProcessOperationError("i16_source_unregistered") from exc
    try:
        from gateway.runtime_ownership import ProfileOwnership, process_ownership
        owner = getattr(policy.parent, "lock_owner", None)
        if owner is None and policy.parent.mode == "managed_turn":
            # Managed-turn admission already owns the process-wide profile reservation; keep
            # this compatibility path explicit rather than accepting an arbitrary truthy map.
            owner = process_ownership
        if not isinstance(owner, ProfileOwnership):
            raise ExternalProcessOperationError("physical_owner_unknown")
        if not owner.reservation_is_current(
            policy.parent.profile_home, policy.parent.physical_owner_epoch,
        ):
            raise ExternalProcessOperationError("physical_owner_unknown")
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
        raise ExternalProcessOperationError("physical_owner_unknown") from exc
    return policy


def reserve_request(parent: ExternalProcessParent):
    if not isinstance(parent, ExternalProcessParent) or parent.state in {"unknown", "terminal"}:
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    if parent.turn_binding is not None and parent._service is not None:
        request = parent._service.requests.get(parent.turn_binding.admission_id)
        if request is None:
            raise ExternalProcessOperationError("service_request_missing")
        parent.state = "request_reserved"
        return request
    parent.state = "request_reserved"
    return parent.root_handle


def reserve_physical(parent: ExternalProcessParent):
    """Return the existing ServiceProcessReceipt for this exact operation."""
    if parent._physical is not None:
        return parent._physical
    service = _parent_service(parent)
    if parent.turn_binding is not None:
        receipt = service.reserve_physical(parent.turn_binding)
    else:
        from gateway.runtime_service_custody import ServiceProcessReceipt
        handle = _reserve_root(
            parent.coordinator, f"{parent.operation_id}:physical", profile_id=parent.profile_id,
            authority_epoch=parent.authority_epoch, parent=parent.root_handle,
        )
        receipt = ServiceProcessReceipt(service, handle, f"{parent.operation_id}:physical")
        service.physical = receipt
        registry = getattr(parent.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_receipt(service, receipt)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                receipt.mark_unknown("unmapped_writer")
                raise ExternalProcessOperationError("i16_source_unregistered") from exc
    parent._physical = receipt
    parent.finalizer = receipt
    return receipt


def bind_physical(
    parent: ExternalProcessParent, transaction: Any, process: Any, *, pid: int,
    process_started_at: int | None, scope: str, scope_identity: str,
):
    receipt = reserve_physical(parent)
    if not receipt.bind_process(pid, process_started_at, scope, scope_identity):
        receipt.mark_unknown("process_identity_rejected")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    receipt.bind_launch(process, transaction)
    parent.state = "physical_bound"
    return receipt


def bind_windows_launch(parent: ExternalProcessParent, transaction: Any, process: Any):
    """Complete the receipt bind after WindowsLaunchTransaction assigned the native Job."""
    receipt = reserve_physical(parent)
    if transaction is None or process is None:
        receipt.mark_unknown("physical_launch_identity_mismatch")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    receipt.bind_launch(process, transaction)
    parent.state = "physical_bound"
    return receipt


def bind_reader(parent: ExternalProcessParent, stream: str, reader: Any) -> None:
    reserve_physical(parent).bind_reader(stream, reader)


def _publication_proof_matches(parent: ExternalProcessParent, proof: Any) -> bool:
    if not isinstance(proof, ExternalPublicationProof):
        return False
    if any(getattr(proof, field) != getattr(parent, field) for field in (
        "operation_id", "profile_id", "process_instance", "authority_epoch", "source_identity",
    )):
        return False
    if type(proof.result_identity) is not str or not proof.result_identity:
        return False
    if type(proof.durable_result) is not bool or type(proof.cache_readback) is not bool:
        return False
    if proof.cache_readback and (type(proof.cache_identity) is not str or not proof.cache_identity):
        return False
    if proof.durable_result and (type(proof.payload_digest) is not str or not proof.payload_digest):
        return False
    return proof.durable_result or proof.cache_readback


def publish_catalog(parent: ExternalProcessParent, proof: Any) -> None:
    if (
        parent._physical is None
        or parent.state in {"unknown", "terminal"}
        or getattr(parent._physical, "state", None) == "unknown"
    ):
        raise ExternalProcessOperationError("custody_unknown")
    if not _publication_proof_matches(parent, proof):
        mark_unknown(parent, "durable_publication_unknown")
        raise ExternalProcessOperationError("durable_publication_unknown")
    parent._physical.external_publication_proof = proof
    parent._physical.completed_requests.add(parent.operation_id)
    parent.state = "physical_bound"


def release_before_effect(parent: ExternalProcessParent) -> bool:
    """Release an I16 child only when the existing receipt proves no process effect."""
    receipt = parent._physical if isinstance(parent, ExternalProcessParent) else None
    if receipt is None:
        return True
    if receipt.release_before_effect() is not True:
        mark_unknown(parent, "partial_launch")
        return False
    parent._physical = None
    parent.state = "request_reserved"
    return True


def mark_unknown(parent: ExternalProcessParent, reason: str) -> None:
    if not isinstance(parent, ExternalProcessParent):
        raise ExternalProcessOperationError("preservation_owner_unmapped")
    if parent.state == "unknown":
        return
    parent.state = "unknown"
    remote = getattr(parent, "_remote_receipt", None)
    if remote is not None:
        marker = getattr(remote, "mark_unknown", None)
        if callable(marker):
            marker(reason)
    if parent._physical is not None:
        parent._physical.mark_unknown(reason)
    parent.coordinator.mark_unknown(parent.root_handle, reason)


def _service_close_is_blocked(parent: ExternalProcessParent) -> bool:
    service = getattr(parent, "_service", None)
    return bool(
        service is not None
        and (
            (getattr(service, "unknown", False) and parent._physical is None)
            or getattr(service, "requests", None)
            or getattr(service, "durable_requests", None)
            or getattr(service, "delivery_unknown_reasons", None)
            or getattr(service, "delivery_terminalized", None)
        )
    )


def close_after_finalizers(parent: ExternalProcessParent) -> bool:
    if not isinstance(parent, ExternalProcessParent):
        return False
    if _service_close_is_blocked(parent):
        return False
    if parent.state == "terminal":
        return True
    remote = getattr(parent, "_remote_receipt", None)
    if remote is not None:
        if getattr(remote, "state", None) != "closed":
            parent.state = "unknown"
            return False
        if getattr(remote, "readback", lambda: None)() is None:
            parent.state = "unknown"
            return False
        result = parent.coordinator.mark_terminal(parent.root_handle)
        if result is not None:
            parent.state = "unknown"
            return False
        parent.state = "terminal"
        forget = getattr(parent.profile_authority, "forget_external_process_finalizer", None)
        if callable(forget) and not forget(parent.operation_id, parent):
            parent.state = "unknown"
            return False
        parent._remote_receipt = None
        return True
    physical = parent._physical
    if physical is None:
        if parent.state not in {"parent_reserved", "request_reserved"}:
            parent.state = "unknown"
            return False
        result = parent.coordinator.mark_terminal(parent.root_handle)
        if result is not None:
            parent.state = "unknown"
            return False
        parent.state = "terminal"
        return True
    if physical.state != "closed" and not physical.close_after_delivery():
        parent.state = "unknown"
        return False
    parent._physical = None
    result = parent.coordinator.mark_terminal(parent.root_handle)
    if result is not None:
        parent.state = "unknown"
        return False
    parent.state = "terminal"
    return True


def release_agent_process_parent(agent: Any) -> bool:
    parent = getattr(agent, "_external_process_parent", None)
    if parent is None:
        return True
    if parent.state != "terminal":
        return False
    context = getattr(agent, "_external_process_parent_context", None) or getattr(
        parent, "_maintenance_context", None,
    )
    if context is not None:
        context.__exit__(None, None, None)
    authority = getattr(parent, "profile_authority", None)
    forget = getattr(authority, "forget_external_process_finalizer", None)
    if callable(forget):
        forget(parent.operation_id, parent)
    agent._external_process_parent_context = None
    agent._external_process_parent = None
    agent._external_process_policy = None
    return True


def ensure_agent_process_policy(agent: Any, *, producer_key: str | None = None) -> ProducerPolicy:
    """Resolve the explicit producer policy at the actual direct/standalone route."""
    current = getattr(agent, "_external_process_policy", None)
    if current is not None:
        return validate_producer_policy(
            current, spawning=getattr(current, "mode", None) != "injected_no_spawn",
        )
    if getattr(agent, "_codex_session", None) is not None:
        return policy_without_spawn()
    db = getattr(agent, "_session_db", None)
    db_path = getattr(db, "db_path", None)
    if db is None or db_path is None:
        raise ExternalProcessOperationError("owner_epoch_unavailable")
    home = Path(db_path).expanduser().resolve().parent
    context = reserve_standalone_external_process_parent(
        profile_home=home, profile_id=str(home), operation_id=uuid.uuid4().hex,
        source_identity=str(getattr(agent, "provider", "codex")), profile_db=db,
        producer_key=producer_key,
    )
    parent = context.__enter__()
    agent._external_process_parent_context = context
    agent._external_process_parent = parent
    policy = policy_for_parent(parent, mode="standalone_maintenance")
    agent._external_process_policy = policy
    return policy
