"""I10 source registry and close-witness helpers for recovery custody."""

from __future__ import annotations

from typing import Callable

from hermes_state_runtime import RuntimeStoreError


def _custody_module():
    import gateway.session_recovery_custody as custody
    return custody


def valid_close_witness(authority, consumer, witness, *, close_generation=None):
    custody = _custody_module()
    if not isinstance(witness, custody.RecoveryConsumerCloseWitness):
        return False
    if (
        witness.schema != custody._CLOSE_WITNESS_SCHEMA
        or witness.consumer != consumer
        or witness.source != custody._SOURCE_BY_CONSUMER.get(consumer)
        or witness.profile_id != authority.profile_id
        or witness.profile_home != str(custody._home(authority))
        or witness.process_instance != authority.instance_id
        or witness.authority_epoch != authority.epoch
        or type(witness.close_generation) is not int
        or witness.close_generation < 1
        or type(witness.source_generation) is not int
        or witness.source_generation < 1
        or witness.ingress_state != "fenced"
        or not isinstance(witness.registry_digest, str)
        or not witness.registry_digest
        or not isinstance(witness.pending_ids, tuple)
        or not all(isinstance(item, str) and item for item in witness.pending_ids)
    ):
        return False
    return close_generation is None or witness.close_generation == close_generation


def close_fence_current(authority, close_generation):
    """Re-read the live close fence before accepting source-owned I10 evidence."""
    custody = _custody_module()
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        return False
    try:
        status = coordinator.status()
        custody._current_epoch(authority)
        custody._physical_owner(authority)
    except (OSError, RuntimeStoreError, AttributeError, RuntimeError, TypeError, ValueError):
        return False
    return (
        isinstance(status, dict)
        and status.get("state") == "closing"
        and status.get("close_generation") == close_generation
        and getattr(authority.runner, "_external_drain_active", False) is True
        and getattr(authority.runner, "_preservation_drain_active", False) is True
    )


def register_consumer_source(authority, consumer, probe: Callable):
    """Register one canonical source hook while the coordinator is open."""
    custody = _custody_module()
    if consumer not in custody.CONSUMERS or not callable(probe):
        raise RuntimeStoreError("preservation_owner_mismatch")
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is not None:
        try:
            status = coordinator.status()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            raise RuntimeStoreError("preservation_owner_unmapped") from None
        if not isinstance(status, dict) or status.get("state") != "open":
            raise RuntimeStoreError("preservation_close_in_progress")
    registry = getattr(authority, "_i10_source_registry", None)
    if registry is None:
        registry = authority._i10_source_registry = {}
    elif not isinstance(registry, dict) or any(key not in custody.CONSUMERS for key in registry):
        raise RuntimeStoreError("preservation_owner_unmapped")
    existing = registry.get(consumer)
    if existing is not None and (
        not isinstance(existing, dict)
        or type(existing.get("generation")) is not int
        or existing.get("generation") < 1
    ):
        raise RuntimeStoreError("preservation_owner_unmapped")
    expected = {
        "source": custody._SOURCE_BY_CONSUMER[consumer],
        "profile_id": authority.profile_id,
        "profile_home": str(custody._home(authority)),
        "process_instance": authority.instance_id,
        "authority_epoch": authority.epoch,
        "generation": 1 if existing is None else existing.get("generation", 0) + 1,
        "probe": probe,
    }
    if existing is not None and any(
        existing.get(key) != expected[key]
        for key in ("source", "profile_id", "profile_home", "process_instance", "authority_epoch")
    ):
        raise RuntimeStoreError("preservation_owner_unmapped")
    registry[consumer] = expected
    return expected


def source_registry(authority):
    custody = _custody_module()
    registry = getattr(authority, "_i10_source_registry", None)
    if not isinstance(registry, dict) or set(registry) != set(custody.CONSUMERS):
        return None
    for consumer in custody.CONSUMERS:
        item = registry.get(consumer)
        if (
            not isinstance(item, dict)
            or item.get("source") != custody._SOURCE_BY_CONSUMER[consumer]
            or item.get("profile_id") != authority.profile_id
            or item.get("profile_home") != str(custody._home(authority))
            or item.get("process_instance") != authority.instance_id
            or item.get("authority_epoch") != authority.epoch
            or type(item.get("generation")) is not int
            or item.get("generation") < 1
            or not callable(item.get("probe"))
        ):
            return None
    return registry


def close_witness(authority, consumer, close_generation, source_generation, snapshot):
    custody = _custody_module()
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        return None
    try:
        status = coordinator.status()
        custody._current_epoch(authority)
        custody._physical_owner(authority)
    except (OSError, RuntimeStoreError, AttributeError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(status, dict)
        or status.get("state") != "closing"
        or status.get("close_generation") != close_generation
        or getattr(authority.runner, "_external_drain_active", False) is not True
        or getattr(authority.runner, "_preservation_drain_active", False) is not True
    ):
        return None
    pending_ids = snapshot.get("pending_ids", ()) if isinstance(snapshot, dict) else ()
    if not isinstance(pending_ids, (tuple, list)) or not all(
        isinstance(item, str) and item for item in pending_ids
    ):
        return None
    registry_digest = snapshot.get("registry_digest") if isinstance(snapshot, dict) else None
    if not isinstance(registry_digest, str) or not registry_digest:
        registry_digest = custody._json_digest(snapshot)
    return custody.RecoveryConsumerCloseWitness(
        custody._CLOSE_WITNESS_SCHEMA, consumer, custody._SOURCE_BY_CONSUMER[consumer],
        authority.profile_id, str(custody._home(authority)), authority.instance_id,
        authority.epoch, close_generation, source_generation, "fenced", registry_digest,
        tuple(sorted(pending_ids)),
    )


def attach_close_witness(authority, consumer, witness):
    custody = _custody_module()
    observations = getattr(authority, "_i10_consumer_evidence", None)
    if not isinstance(observations, dict):
        return False
    stored = observations.get(consumer)
    receipts = stored if isinstance(stored, tuple) else (stored,)
    if not receipts or any(receipt is None for receipt in receipts):
        return False
    updated = []
    for receipt in receipts:
        if isinstance(receipt, custody.RecoveryConsumerAbsence):
            updated.append(custody.RecoveryConsumerAbsence(
                receipt.schema, receipt.consumer, receipt.source, receipt.process_instance,
                receipt.authority_epoch, receipt.profile_home, receipt.reason, witness,
            ))
        elif isinstance(receipt, custody.RecoveryConsumerReceipt):
            updated.append(custody.RecoveryConsumerReceipt(
                receipt.schema, receipt.consumer, receipt.source, receipt.state, receipt.identity,
                receipt.handle, receipt.source_receipts, receipt.finalizer_digest, receipt.reason,
                witness,
            ))
        else:
            return False
    observations[consumer] = updated[0] if len(updated) == 1 else tuple(updated)
    return True


def refresh_source_evidence(authority, close_generation):
    """Read all registered source queues at close and attach exact witnesses."""
    custody = _custody_module()
    registry = source_registry(authority)
    if registry is None:
        return False
    complete = True
    for consumer in custody.CONSUMERS:
        registration = registry[consumer]
        try:
            snapshot = registration["probe"](authority)
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
            complete = False
            continue
        if not isinstance(snapshot, dict):
            complete = False
            continue
        current_registry = source_registry(authority)
        if current_registry is not registry or current_registry.get(consumer) is not registration:
            complete = False
            continue
        source_generation = registration.get("generation")
        if type(source_generation) is not int or source_generation < 1:
            complete = False
            continue
        witness = close_witness(authority, consumer, close_generation, source_generation, snapshot)
        if witness is None:
            complete = False
            continue
        pending_ids = witness.pending_ids
        observations = getattr(authority, "_i10_consumer_evidence", {})
        existing = observations.get(consumer) if isinstance(observations, dict) else None
        if pending_ids:
            rows = snapshot.get("rows", ())
            if isinstance(rows, (tuple, list)) and rows:
                custody.observe_consumer_rows(
                    authority, consumer, rows=list(rows),
                    reason="close-time source scan found accepted recovery work",
                )
            complete = False
            continue
        if existing is None:
            absence = custody.unused_consumer(
                authority, consumer,
                reason="source-owned close scan found no canonical recovery work",
                close_witness=witness,
            )
            if not custody.observe_consumer(authority, consumer, absence):
                complete = False
                continue
        elif not attach_close_witness(authority, consumer, witness):
            complete = False
    return complete


def owner_evidence(
    authority, *, process_instance, authority_epoch, close_generation, owner_id,
    require_close_witness=False,
):
    custody = _custody_module()
    if not custody.owner_ready(authority):
        return None
    registry = None
    if require_close_witness:
        if not close_fence_current(authority, close_generation):
            return None
        registry = source_registry(authority)
        if registry is None:
            return None
    try:
        physical = custody._physical_owner(authority)
        current_epoch = custody._current_epoch(authority)
    except (RuntimeStoreError, AttributeError, TypeError, ValueError):
        return None
    if process_instance != authority.instance_id or authority_epoch != authority.epoch:
        return None
    observations = getattr(authority, "_i10_consumer_evidence", {})
    consumer_proofs = []
    for consumer in custody.CONSUMERS:
        stored = observations.get(consumer)
        receipts = stored if isinstance(stored, tuple) else (stored,)
        for receipt in receipts:
            if isinstance(receipt, custody.RecoveryConsumerReceipt):
                if require_close_witness and not valid_close_witness(
                    authority, consumer, receipt.close_witness,
                    close_generation=close_generation,
                ):
                    return None
                if require_close_witness and (
                    registry[consumer].get("generation") != receipt.close_witness.source_generation
                ):
                    return None
                consumer_proofs.append((consumer, receipt.identity.admission_id, receipt.finalizer_digest))
            elif isinstance(receipt, custody.RecoveryConsumerAbsence):
                if require_close_witness and not valid_close_witness(
                    authority, consumer, receipt.close_witness,
                    close_generation=close_generation,
                ):
                    return None
                if require_close_witness and (
                    registry[consumer].get("generation") != receipt.close_witness.source_generation
                ):
                    return None
                consumer_proofs.append((consumer, "unused", receipt.reason))
    return {
        "source": "gateway/run_startup.py;gateway/session_local_recovery.py;"
        "gateway/platforms/webhook_ingress.py;gateway/run_adapters.py;"
        "gateway/run_notifications.py;gateway/session_bot_mailbox.py",
        "process_instance": authority.instance_id,
        "authority_epoch": current_epoch[0],
        "close_generation": close_generation,
        "owner_id": owner_id,
        "profile_id": authority.profile_id,
        "profile_home": str(custody._home(authority)),
        "consumers": tuple(consumer_proofs),
        "observed": True,
        "cross_process_proof": physical.get("cross_process_proof") is True,
        "os_lock_owner": physical.get("os_lock_owner") is True,
        "native_handle_retained": physical.get("native_handle_retained") is True,
        "record_binding": physical.get("record_binding"),
    }
