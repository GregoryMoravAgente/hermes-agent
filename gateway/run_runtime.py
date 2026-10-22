"""Session-runtime lifecycle owned by the ordinary gateway bootstrap.

One SessionAuthority per reserved profile home. The launch home always has one; under
``gateway.multiplex_profiles`` every served secondary gets its own, built under that
profile's runtime scope against that profile's ``state.db``. ``runner.session_authority``
stays the launch profile's authority so single-profile behaviour is byte-identical.

The served set is not frozen at boot: ``serve_profile_runtime`` / ``unserve_profile_runtime``
grow and shrink it for the hot-serve reconcile, and a secondary whose store cannot be opened
is parked (logged, left out of ``served_profiles``) instead of aborting every other profile.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import uuid

from gateway.runtime_sidecar_remote import RemoteSourceError

logger = logging.getLogger(__name__)


def reserved_profile_homes(runner):
    """``(name, home)`` pairs this process reserved: launch home first, then served secondaries."""
    from hermes_constants import get_hermes_home
    launch = get_hermes_home().resolve()
    homes = [(getattr(runner, '_primary_profile_name', None) or 'default', launch)]
    if getattr(runner.config, 'multiplex_profiles', False):
        reserved = getattr(runner.config, '_runtime_profile_homes', None) or ()
        for name, home in reserved:
            canonical = Path(home).resolve()
            if canonical != launch and canonical not in {h for _, h in homes}:
                homes.append((name, canonical))
    return homes


async def _build_profile_authority(runner, name, home, *, register, defer_local_recovery=False):
    """One profile's authority against its own ``state.db``; raises when that store is unusable."""
    from gateway.run import _profile_runtime_scope
    registry = runner.session_authorities
    instance_id = runner.session_runtime_descriptor['instance_id']
    # Each home's store resolves through the runner's scope-following handle cache, exactly
    # the handle every later scoped read of that profile uses (one writer per state.db).
    with _profile_runtime_scope(home, hydrate_secrets=False):
        db = getattr(runner._session_db, '_db', runner._session_db)
        if db is None:
            raise RuntimeError(_store_open_failure(runner, home))
        if (await asyncio.to_thread(Path(db.db_path).resolve)).parent != home:
            raise RuntimeError(f'session authority database does not belong to the reserved profile {home}')
        registry.add(home, None, name=name)
        try:
            authority = await initialize_session_authority(
                runner, profile_id=str(home), instance_id=instance_id, db=db, register=register,
                defer_local_recovery=defer_local_recovery)
        except BaseException:
            registry.remove(home)
            raise
    registry.replace(home, authority)
    return authority


def _store_open_failure(runner, home):
    """Why *home*'s ``state.db`` has no handle: the scoped handle caches answer None for a failed open
    (and while they back off), so the park reason named an ownership mismatch over the real cause, a
    legacy store that crashed its migration on every open (#141467). The reason is the exception the
    failed open recorded; reopening here to learn it could create or migrate the store, and would skip
    the caches' backoff."""
    path = home / 'state.db'
    caches = (getattr(getattr(runner, 'session_store', None), '_db_handle_cache', None),
              getattr(runner, '_session_db_handle_cache', None))
    error = next((e for e in (c.last_error(path) for c in caches if c is not None) if e), '')
    if error:
        return f'{path} did not open: {error}'
    return f'{path} is unavailable (its last open failed; retry pending)'


def bootstrap_i10_consumer_sources(runner) -> None:
    """Register each real recovery source before a preservation close can begin.

    Registration is only a source hook installation.  The source modules perform their own
    canonical queue/registry read at the close fence; no empty map or boot-time flag becomes an
    absence receipt.
    """
    from gateway.platforms.webhook_ingress import register_i10_source as register_webhook
    from gateway.run_notifications import register_i10_source as register_notifications
    from gateway.run_startup import register_i10_source as register_startup
    from gateway.session_bot_mailbox import register_i10_source as register_mailbox
    from gateway.session_local_recovery import register_i10_source as register_local
    from gateway.run_notifications import initialize_i10_source_indexes
    from gateway.session_authorities import all_authorities, owner_scope

    initialize_i10_source_indexes(runner)
    for authority in all_authorities(runner):
        with owner_scope(authority):
            register_startup(authority)
            register_local(authority)
            register_webhook(authority)
            runner.register_i10_source(authority)
            register_notifications(authority)
            register_mailbox(authority)


async def initialize_session_authority(
        runner, *, profile_id, instance_id, db=None, register=True, defer_local_recovery=False):
    """Build and publish one profile authority after exclusive ownership is established."""
    from hermes_state_runtime import begin_runtime_epoch, recover_session_inputs
    from gateway.session_authority import (
        SessionAuthority, backfill_compression_created_sources, backfill_legacy_session_owners,
    )
    from gateway.session_unattended_unknown import settle_recovered_unattended

    if db is None:
        db = getattr(runner._session_db, '_db', runner._session_db)
    db_key = await asyncio.to_thread(Path(db.db_path).resolve)
    epoch = await asyncio.to_thread(begin_runtime_epoch, db, instance_id=instance_id)
    await asyncio.to_thread(recover_session_inputs, db, epoch=epoch)
    await asyncio.to_thread(settle_recovered_unattended, db, epoch)
    await asyncio.to_thread(backfill_legacy_session_owners, db)
    await asyncio.to_thread(backfill_compression_created_sources, db)
    authority = SessionAuthority(runner, profile_id=profile_id, instance_id=instance_id, db=db, epoch=epoch)
    # Register the profile-scoped MCP source factory before gateway discovery
    # can enter any configured remote transport.  The factory is only a
    # capability boundary; the authority's existing object finalizer index
    # remains the source-owner registry.
    from gateway.runtime_sidecar_factory import register_mcp_source_factory
    register_mcp_source_factory(runner, authority)
    from gateway.runtime_sidecar_browser import register_browser_source_factory
    register_browser_source_factory(runner, authority)
    from gateway.runtime_sidecar_cua import register_cua_source_factory
    register_cua_source_factory(runner, authority)
    managed_bootstrap = hasattr(runner, 'session_authorities')
    if not defer_local_recovery:
        from gateway.session_local_adoption import adopt_local_compressions
        await adopt_local_compressions(authority)
    if register:
        runner.session_authority = authority
    from gateway.session_cron import bind_owner
    bind_owner(authority)
    store = runner.session_store
    if register:
        store._local_authority_epoch = epoch
    epochs = getattr(store, '_local_authority_epochs', None)
    if epochs is None:
        epochs = store._local_authority_epochs = {}
    epochs[db_key] = epoch
    if not defer_local_recovery:
        from gateway.session_local_recovery import recover_local_sessions
        recover_local_sessions(authority, allow_legacy=not managed_bootstrap)
    return authority


def _park_reserved_profile(runner, name, home, exc):
    """A secondary whose store is unusable is parked: logged, unreserved, left out of
    ``served_profiles`` and named under ``parked_profiles`` in the runtime descriptor. Boot
    continues for every other profile (a runtime hot-add parks the same way)."""
    logger.error("[MULTIPLEX] Profile '%s' not served: its session store is unusable (%s): %s",
                 name, home, exc)
    release_profile_home(runner, home)
    park_profile(runner, name, f'session store unusable: {exc}')


def park_profile(runner, name, reason):
    """Publish *name* under ``parked_profiles`` (name -> reason) in the runtime descriptor and the
    root's ``gateway_state.json``: a client of that profile then gets a terminal ``profile_parked``
    verdict from ``ensure`` instead of waiting its whole deadline for a service that never comes."""
    parked = parked_profile_map(runner)
    parked[name] = str(reason)[:500]
    _record_parked_profiles(parked)


def unpark_profile(runner, name):
    parked = parked_profile_map(runner)
    if parked.pop(name, None) is not None:
        _record_parked_profiles(parked)


def profile_parked(runner, name):
    """True when *name* is parked: not served now, but a rescan serves it again, so its ingress is
    temporarily unavailable (retry it), never an invalid route (refuse it for good)."""
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    return bool(name) and name in (descriptor.get('parked_profiles') or {})


def refuse_parked_scope(runner):
    """An API request under a parked profile's ``/p/<profile>/`` prefix finds no authority: refuse it
    retryably (``runtime_draining``, a 503 with ``Retry-After``), not with the final
    ``profile_mismatch`` of a profile this gateway does not serve."""
    from gateway.session_authorities import served_profile_name
    from hermes_constants import get_hermes_home
    name = served_profile_name(get_hermes_home())
    if profile_parked(runner, name):
        from gateway.parked_route_refusal import ParkedProfileRefusal
        raise ParkedProfileRefusal(name)


def parked_profile_map(runner):
    descriptor = getattr(runner, 'session_runtime_descriptor', None)
    if descriptor is None:
        descriptor = runner.session_runtime_descriptor = {}
    return descriptor.setdefault('parked_profiles', {})


def _record_parked_profiles(parked):
    try:
        from gateway.status import write_runtime_status
        write_runtime_status(parked_profiles=dict(parked))
    except Exception:
        logger.debug('could not record parked_profiles', exc_info=True)


async def initialize_gateway_runtime(runner):
    from gateway.runtime_bootstrap import TicketStore
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authorities import SessionAuthorities

    homes = reserved_profile_homes(runner)
    for _name, home in homes:
        if not process_ownership.owns(home):
            raise RuntimeError(f'session authority requires reserved profile ownership: {home}')
    instance_id = uuid.uuid4().hex
    descriptor = {
        'instance_id': instance_id, 'runtime_protocol': 1,
        'state': 'starting', 'capabilities': [],
        'served_profiles': [],
    }
    runner.session_runtime_descriptor = descriptor
    registry = SessionAuthorities(homes[0][1], multiplexed=getattr(runner.config, 'multiplex_profiles', False))
    runner.session_authorities = registry
    for index, (name, home) in enumerate(homes):
        try:
            await _build_profile_authority(
                runner, name, home, register=index == 0, defer_local_recovery=True)
        except Exception as exc:
            if index == 0:
                raise  # the launch profile's store is the process's own; nothing to park it behind
            _park_reserved_profile(runner, name, home, exc)
    descriptor['authority_epoch'] = registry.launch.epoch
    descriptor['served_profiles'] = registry.served_profiles()
    # The preservation coordinator is installed only after the launch authority epoch exists.
    # It owns the process-global close receipt; it does not replace the per-profile authority
    # registry or the OS profile locks.
    from gateway.runtime_custody import install_runtime_custody
    coordinator = install_runtime_custody(runner)
    # Every managed authority now has the process coordinator. Each source reserves its I10/I11
    # owner before the existing compression write; the legacy initializer never enters this loop.
    from gateway.session_authorities import all_authorities, owner_scope
    from gateway.session_local_adoption import adopt_local_compressions
    from gateway.session_local_recovery import recover_local_sessions
    for authority in all_authorities(runner):
        name = registry.profile_name(authority)
        try:
            with owner_scope(authority):
                await adopt_local_compressions(authority)
                recover_local_sessions(authority, allow_legacy=False)
        except Exception as exc:
            if name is None:
                raise
            home = await asyncio.to_thread(Path(authority.profile_id).resolve)
            if not await _retire_profile_authority(authority):
                descriptor['served_profiles'] = registry.served_profiles()
                _withdraw_profile_home(runner, home)
                park_profile(
                    runner, name,
                    f'startup local recovery failed; ownership retained: {exc}',
                )
                continue
            registry.remove(home)
            descriptor['served_profiles'] = registry.served_profiles()
            release_profile_home(runner, home)
            park_profile(
                runner, name,
                f'startup local recovery failed; profile parked: {exc}',
            )
    bootstrap_i10_consumer_sources(runner)
    # Bind the process registry only after the authenticated process instance and authority epoch
    # exist. Pre-existing children are retained as unknown instead of being adopted after Popen.
    from tools.process_registry import process_registry
    process_registry.bind_runtime_custody(coordinator)
    from tui_gateway.hosted_tui_custody import bind_runtime
    # Publish the binding during bootstrap.  Reserve an I17 root here only when a real adapter is
    # already attached (the API-owned init-only path); the ordinary gateway has no control listener
    # or hosted producer at this point and must not make every legacy authority look active.  The
    # production startup path binds every served authority once the control listener is live, before
    # hosted-service preparation or any preservation close can begin.
    bind_runtime(
        runner,
        coordinator,
        reserve=bool(_api_server_adapters(runner))
        or getattr(runner, "session_control_server", None) is not None,
    )
    # Publish this boot's verdict (an empty map clears a previous run's parked set).
    _record_parked_profiles(parked_profile_map(runner))
    runner.session_ticket_store = TicketStore(instance_id, registry.profile_ids())
    release_unknown_turn_markers(runner)


def release_unknown_turn_markers(runner):
    """Before the unclean-start pass reads crash-left turn markers: a marked turn whose canonical
    admission recovered ``unknown`` (the owner died before its terminal commit) owes no reply, so
    its marker is cleared rather than ledgered as a delivery the FIFO never committed. A terminal
    admission keeps its marker: its persisted reply is ledgered and sent once, never re-run."""
    from gateway.session_authorities import admission_owner, owner_scope
    store = getattr(runner, 'session_store', None)
    if store is None:
        return
    for authority in _authorities(runner):
        with owner_scope(authority):
            unknown = {row['target_session_id'] for row in authority.db._read_all(
                "SELECT DISTINCT target_session_id FROM session_admissions WHERE status='unknown'")}
            if not unknown:
                continue
            for entry in store.list_sessions():
                if entry.active_turn_token and admission_owner(authority, entry.session_id) in unknown:
                    store.clear_turn_active(entry.session_key, entry.active_turn_token)


def _publish_served_set(runner):
    registry = runner.session_authorities
    runner.session_runtime_descriptor['served_profiles'] = registry.served_profiles()
    runner.session_ticket_store.profile_ids = registry.profile_ids()


def reserve_profile_home(runner, name, home):
    """Grow the process reservation by one profile (hot-serve): take its ``gateway.lock`` and add it
    to the frozen boot set, so every reader of the reservation — and the next restart's
    all-or-nothing reserve — sees it. ``OwnershipConflict`` when another gateway owns the home."""
    from gateway.runtime_ownership import process_ownership
    home = Path(home).resolve()
    process_ownership.reserve([home])
    reserved = getattr(runner.config, '_runtime_profile_homes', None)
    if reserved is not None and all(Path(h).resolve() != home for _n, h in reserved):
        runner.config._runtime_profile_homes = (*reserved, (name, home))


def release_profile_home(runner, home):
    """Release the real profile lock before withdrawing its canonical reservation.

    A failed or ambiguous close remains owned and retryable; no served/reserved map or successor
    path may describe that home as clean until the exact descriptor is released.
    """
    from gateway.runtime_ownership import process_ownership
    home = Path(home).resolve()
    registry = getattr(runner, 'session_authorities', None)
    authority = None if registry is None else registry.for_home(home)
    if authority is not None:
        if not _settle_external_process_finalizers(authority):
            raise RuntimeError('External process finalizer must settle before ownership is released')
        raise RuntimeError('Profile authority must retire before ownership is released')
    outcome = process_ownership.release_outcome(home)
    if getattr(outcome, "value", None) != "released":
        raise ProfileOwnershipRetained(
            f"profile {home} retains physical custody ({getattr(outcome, 'value', outcome)})"
        )
    _withdraw_profile_home(runner, home)


def _settle_external_process_finalizers(authority) -> bool:
    """Consume every registered external parent before a profile can be retired.

    The registry contains object-bound parents backed by the existing custody coordinator. A
    failed reader/native/durable/cache finalizer stays registered and therefore keeps the profile
    owner alive for the same-handle retry; an empty local map is never treated as quiescence.
    """
    getter = getattr(authority, 'external_process_finalizers', None)
    if not callable(getter):
        return True
    from gateway.runtime_service_operations import close_after_finalizers
    forget = getattr(authority, 'forget_external_process_finalizer', None)
    for parent in tuple(getter()):
        if getattr(parent, 'state', None) != 'terminal':
            try:
                if close_after_finalizers(parent) is not True:
                    return False
            except Exception:
                logger.warning('External process finalizer did not settle for %s',
                               getattr(parent, 'operation_id', '?'), exc_info=True)
                return False
        if not callable(forget) or forget(getattr(parent, 'operation_id', None), parent) is not True:
            return False
    return not tuple(getter())


def _preservation_ownership_reason(runner, *, require_running: bool = True):
    """Return a refusal when the existing profile locks/lifecycle owner cannot be proven.

    ``RetirementFence`` is process-local.  The profile ``gateway.lock`` handles are the existing
    cross-process exclusion boundary; preservation must hold them for every served home before it
    can claim that an independent CLI, cron process, or successor is absent.  This helper only
    observes that authority and never acquires, releases, or replaces a lock.
    """
    if require_running and (
        not getattr(runner, '_running', False)
        or getattr(runner, '_restart_requested', False)
        or getattr(runner, '_stop_task', None) is not None
    ):
        return 'lifecycle_owned'
    from gateway.runtime_ownership import process_ownership
    homes = reserved_profile_homes(runner)
    if not homes:
        return 'unmapped_writer'
    if any(
        process_ownership.reservation_evidence(home) is None
        for _name, home in homes
    ):
        return 'foreign_custody'
    return None


def _preservation_receipt(params):
    receipt = params.get('receipt') if isinstance(params, dict) else None
    if not isinstance(receipt, dict):
        raise ValueError('invalid_params')
    from gateway.runtime_custody import DrainReceipt
    required = {'receipt_id', 'process_instance', 'authority_epoch', 'close_generation', 'status'}
    if set(receipt) != required:
        raise ValueError('invalid_params')
    if receipt.get('status') not in {'closing', 'handoff_pending'}:
        raise ValueError('invalid_params')
    return DrainReceipt(
        receipt['receipt_id'], receipt['process_instance'], receipt['authority_epoch'],
        receipt['close_generation'], receipt['status'])


_INTEGRATED_PRESERVATION_OWNER_CLASSES = (
    # I01 and I06 are sealed only after their registered adapter sources close for this exact
    # generation and all producer/marker/delivery-ledger registries plus durable admissions are
    # quiescent; an empty queue or an activity bit alone cannot seal either class.
    'I01_native_inbound',
    'I02_api_session_turn', 'I04_fifo_turn_execution',
    'I06_busy_followup_delivery',
    # Automation supplies its own observed-ingress and durable/terminal readiness checks.
    'I05_controls_and_unknown', 'I07_automation_route_wakes', 'I08_goals_loops_heartbeat',
    'I11_session_mutations_finalizers',
    'I09_a2a_hosted_kanban',
)


def _strict_physical_owner_bindings(physical, homes):
    """Validate the physical evidence tuple before handing I20 to the existing coordinator."""
    if not physical or len(physical) != len(homes):
        return None
    bindings = []
    seen_homes = set()
    for evidence, (_name, home) in zip(physical, homes):
        if not isinstance(evidence, dict):
            return None
        try:
            expected_home = str(Path(home).resolve())
            profile_home = Path(evidence['profile_home']).resolve()
            epoch = evidence['owner_epoch']
            descriptor = evidence['native_descriptor']
            record = evidence['record']
            record_home = Path(record['hermes_home']).resolve()
        except (KeyError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if (
            str(profile_home) != expected_home
            or not isinstance(epoch, str) or not epoch
            or isinstance(descriptor, bool) or not isinstance(descriptor, int) or descriptor < 0
            or not isinstance(record, dict)
            or record.get('owner_epoch') != epoch
            or str(record_home) != expected_home
            or evidence.get('physical_binding') != (str(profile_home), epoch, descriptor)
            or evidence.get('native_handle_retained') is not True
            or evidence.get('os_lock_owner') is not True
            or evidence.get('cross_process_proof') is not True
            or evidence.get('observed') is not True
            or expected_home in seen_homes
        ):
            return None
        seen_homes.add(expected_home)
        bindings.append((expected_home, epoch, descriptor))
    return tuple(bindings)


def _api_server_adapters(runner):
    from gateway.config import Platform
    found = []
    primary = (getattr(runner, "adapters", None) or {}).get(Platform.API_SERVER)
    if primary is not None:
        found.append(primary)
    for profile_adapters in (getattr(runner, "_profile_adapters", None) or {}).values():
        adapter = (profile_adapters or {}).get(Platform.API_SERVER)
        if adapter is not None and all(adapter is not item for item in found):
            found.append(adapter)
    return found


def _seal_physical_profile_owner(runner, coordinator, receipt, descriptor):
    """Seal I20 from each actual retained profile descriptor and physical incarnation."""
    from gateway.runtime_custody import OWNER_SPECS
    # I20 is sealed only from the actual ProfileOwnership reservation.  A running flag,
    # PID, empty map, or ``_seal_all`` cannot prove the cross-process exclusion that a
    # successor must wait for.  Each served home contributes its own physical incarnation;
    # authority/process epochs remain separate fields in the receipt.
    try:
        from gateway.runtime_ownership import process_ownership
        homes = reserved_profile_homes(runner)
        physical = [
            process_ownership.reservation_evidence(home)
            for _name, home in homes
        ]
        physical_bindings = _strict_physical_owner_bindings(physical, homes)
        if physical_bindings is None:
            coordinator.mark_owner_unknown('I20_runtime_ownership_and_foreign_handoff', 'foreign_custody')
        else:
            epochs = {item['owner_epoch'] for item in physical}
            evidence = {
                'source': OWNER_SPECS['I20_runtime_ownership_and_foreign_handoff'].source,
                'process_instance': receipt.process_instance,
                'authority_epoch': receipt.authority_epoch,
                'close_generation': receipt.close_generation,
                'owner_id': f'{descriptor.get("instance_id", receipt.process_instance)}:I20',
                'observed': True,
                'cross_process_proof': all(item.get('cross_process_proof') is True for item in physical),
                'os_lock_owner': all(item.get('os_lock_owner') is True for item in physical),
                'native_handle_retained': all(item.get('native_handle_retained') is True for item in physical),
                'owner_epochs': tuple(sorted(epochs)),
                'native_handles': tuple(item['native_descriptor'] for item in physical),
                'profile_homes': tuple(item['profile_home'] for item in physical),
                'physical_bindings': physical_bindings,
            }
            refusal = coordinator.seal_owner('I20_runtime_ownership_and_foreign_handoff', evidence)
            if refusal is not None:
                coordinator.mark_owner_unknown(
                    'I20_runtime_ownership_and_foreign_handoff',
                    getattr(refusal, 'code', 'foreign_custody'),
                )
    except BaseException:  # health: allow BLE001 -- interrupted I20 evidence stays unknown
        coordinator.mark_owner_unknown('I20_runtime_ownership_and_foreign_handoff', 'read_failed')


def _authority_owner_ready(authority, owner_class: str, coordinator) -> bool:
    try:
        return bool(authority.preservation_owner_ready(owner_class))
    except (AttributeError, RuntimeError, TypeError, ValueError):
        coordinator.mark_owner_unknown(owner_class, 'read_failed')
        return False


def _authority_source_owner_evidence(runner, _authority, receipt, owner_class, owner_id):
    from gateway.preservation_source_close import source_owner_evidence

    return source_owner_evidence(
        runner, owner_class,
        process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch,
        close_generation=receipt.close_generation,
        owner_id=owner_id,
    )


def _authority_goal_owner_evidence(_runner, authority, receipt, _owner_class, owner_id):
    from gateway.session_goal_tick_custody import source_owner_evidence

    return source_owner_evidence(
        authority,
        process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch,
        close_generation=receipt.close_generation,
        owner_id=owner_id,
    )


def _authority_hosted_owner_evidence(_runner, authority, receipt, _owner_class, owner_id):
    from gateway.session_hosted_service import preservation_i09_owner_evidence

    return preservation_i09_owner_evidence(
        authority, process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch,
        close_generation=receipt.close_generation, owner_id=owner_id,
    )


def _authority_readiness_owner_evidence(_runner, authority, receipt, owner_class, owner_id):
    from gateway.session_authority_readiness import preservation_owner_evidence

    return preservation_owner_evidence(
        authority, owner_class,
        process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch,
        close_generation=receipt.close_generation,
        owner_id=owner_id,
    )


_AUTHORITY_OWNER_EVIDENCE_HANDLERS = {
    'I01_native_inbound': _authority_source_owner_evidence,
    'I06_busy_followup_delivery': _authority_source_owner_evidence,
    'I08_goals_loops_heartbeat': _authority_goal_owner_evidence,
    'I09_a2a_hosted_kanban': _authority_hosted_owner_evidence,
    'I05_controls_and_unknown': _authority_readiness_owner_evidence,
    'I11_session_mutations_finalizers': _authority_readiness_owner_evidence,
}


def _authority_preservation_owner_evidence(
    runner, coordinator, receipt, descriptor, authority, owner_class: str,
) -> None:
    from gateway.runtime_custody import OWNER_SPECS

    owner_id = f'{descriptor.get("instance_id", receipt.process_instance)}:{owner_class}'
    handler = _AUTHORITY_OWNER_EVIDENCE_HANDLERS.get(owner_class)
    evidence = (
        handler(runner, authority, receipt, owner_class, owner_id)
        if handler is not None else {
            'source': OWNER_SPECS[owner_class].source,
            'process_instance': receipt.process_instance,
            'authority_epoch': receipt.authority_epoch,
            'close_generation': receipt.close_generation,
            'owner_id': owner_id,
            'observed': True,
        }
    )
    return evidence


def _seal_authorities_preservation_owner(
    runner, coordinator, receipt, descriptor, authorities, owner_class,
):
    """Seal once, only after every served authority supplies this close's evidence."""
    from gateway.runtime_custody import OWNER_SPECS

    observations = []
    for authority in authorities:
        try:
            evidence = _authority_preservation_owner_evidence(
                runner, coordinator, receipt, descriptor, authority, owner_class,
            )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            coordinator.mark_owner_unknown(owner_class, 'read_failed')
            return
        if (not isinstance(evidence, dict)
                or evidence.get('source') != OWNER_SPECS[owner_class].source
                or evidence.get('process_instance') != receipt.process_instance
                or evidence.get('authority_epoch') != receipt.authority_epoch
                or evidence.get('close_generation') != receipt.close_generation
                or evidence.get('observed') is not True
                or not isinstance(evidence.get('owner_id'), str) or not evidence['owner_id']):
            coordinator.mark_owner_unknown(owner_class, 'owner_observation_missing')
            return
        observations.append(evidence)
    if not observations:
        return
    # Preserve each source's evidence instead of allowing the last profile's successful seal
    # to replace an earlier missing observation. Admission is already closed for this generation.
    evidence = dict(observations[0])
    evidence['authority_observations'] = tuple(observations)
    refusal = coordinator.seal_owner(owner_class, evidence)
    if refusal is not None:
        coordinator.mark_owner_unknown(owner_class, getattr(refusal, 'code', 'read_failed'))


def _seal_i10_preservation_owner(runner, coordinator, receipt, descriptor):
    """Aggregate each live profile's six I10 source proofs under one global close receipt."""
    from gateway.runtime_custody import OWNER_SPECS
    from gateway.session_authorities import owner_scope
    from gateway.session_recovery_custody import owner_evidence, refresh_i10_source_evidence

    authorities = _authorities(runner)
    if not authorities:
        coordinator.mark_owner_unknown('I10_startup_recovery_handoff', 'missing_source_hook')
        return
    profiles = []
    for authority in authorities:
        try:
            with owner_scope(authority):
                if not refresh_i10_source_evidence(authority, receipt.close_generation):
                    coordinator.mark_owner_unknown(
                        'I10_startup_recovery_handoff', 'enumeration_failed')
                    return
                proof = owner_evidence(
                    authority,
                    process_instance=authority.instance_id,
                    authority_epoch=authority.epoch,
                    close_generation=receipt.close_generation,
                    owner_id=(
                        f'{descriptor.get("instance_id", receipt.process_instance)}:I10:'
                        f'{authority.profile_id}'
                    ),
                    require_close_witness=True,
                )
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
            coordinator.mark_owner_unknown('I10_startup_recovery_handoff', 'read_failed')
            return
        if proof is None:
            coordinator.mark_owner_unknown('I10_startup_recovery_handoff', 'enumeration_failed')
            return
        profiles.append(proof)

    evidence = {
        'source': OWNER_SPECS['I10_startup_recovery_handoff'].source,
        'process_instance': receipt.process_instance,
        'authority_epoch': receipt.authority_epoch,
        'close_generation': receipt.close_generation,
        'owner_id': f'{descriptor.get("instance_id", receipt.process_instance)}:I10',
        'observed': True,
        # Keep every profile's own source evidence intact. A single mutable dict/last profile
        # cannot overwrite the secondary epoch or consumer receipts.
        'profiles': tuple(profiles),
        'profile_ids': tuple(proof['profile_id'] for proof in profiles),
        'profile_homes': tuple(proof['profile_home'] for proof in profiles),
        'profile_epochs': tuple(
            (proof['profile_id'], proof['authority_epoch']) for proof in profiles
        ),
        'consumers': tuple(
            (proof['profile_id'], tuple(proof['consumers'])) for proof in profiles
        ),
        'cross_process_proof': all(proof.get('cross_process_proof') is True for proof in profiles),
        'os_lock_owner': all(proof.get('os_lock_owner') is True for proof in profiles),
        'native_handle_retained': all(
            proof.get('native_handle_retained') is True for proof in profiles
        ),
    }
    refusal = coordinator.seal_owner('I10_startup_recovery_handoff', evidence)
    if refusal is not None:
        coordinator.mark_owner_unknown(
            'I10_startup_recovery_handoff', getattr(refusal, 'code', 'read_failed'))


def _seal_integrated_preservation_owners(runner, coordinator, receipt):
    """Seal source consumers whose durable terminal paths are wired here.

    Per-authority hooks retain the exact I01/I02/I04/I05/I07/I08/I09/I11 readiness predicates.
    Cron uses its ledger, process and pipe evidence independently, even without session
    authorities. Every other class stays unwired until its own source seam proves readiness.
    """
    authorities = _authorities(runner)
    from gateway.runtime_custody import OWNER_SPECS
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    try:
        from tui_gateway.hosted_tui_custody import seal_owner
        seal_owner(runner, coordinator, receipt)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        coordinator.mark_owner_unknown('I17_hosted_tui_services', 'read_failed')
    if authorities:
        _seal_i10_preservation_owner(runner, coordinator, receipt, descriptor)
        for owner_class in _INTEGRATED_PRESERVATION_OWNER_CLASSES:
            if not all(_authority_owner_ready(authority, owner_class, coordinator) for authority in authorities):
                continue
            _seal_authorities_preservation_owner(
                runner, coordinator, receipt, descriptor, authorities, owner_class,
            )

    # Direct API runs have a separate adapter-owned source hook.  It may seal only after the
    # adapter registered its run lifecycle and every source-owned run/status/transport/finalizer
    # record is terminal; an absent or incomplete hook remains unwired and blocks handoff.
    api_adapters = _api_server_adapters(runner)
    if api_adapters:
        try:
            from gateway.platforms.api_server_run_custody import preservation_owner_evidence
            evidence = preservation_owner_evidence(
                api_adapters, runner, process_instance=receipt.process_instance,
                authority_epoch=receipt.authority_epoch, close_generation=receipt.close_generation,
                owner_id=f'{descriptor.get("instance_id", receipt.process_instance)}:I03_api_run_direct')
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            coordinator.mark_owner_unknown('I03_api_run_direct', 'read_failed')
        else:
            if evidence is not None:
                evidence = dict(evidence)
                evidence['source'] = OWNER_SPECS['I03_api_run_direct'].source
                refusal = coordinator.seal_owner('I03_api_run_direct', evidence)
                if refusal is not None:
                    coordinator.mark_owner_unknown(
                        'I03_api_run_direct', getattr(refusal, 'code', 'read_failed'))

    # ProcessRegistry is a cross-process owner: it must supply a real reservation/identity/reader
    # observation, not an empty running map. A missing or failed source hook is explicitly unknown.
    try:
        from tools.process_registry import process_registry
        from tools.process_registry_custody_evidence import (
            preservation_owner_evidence_for_profiles,
        )
        process_evidence = preservation_owner_evidence_for_profiles(
            process_registry,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{descriptor.get("instance_id", receipt.process_instance)}:I15_process_registry_terminal',
            launch_profile_id=process_registry._process_profile_id(),
        )
        if process_evidence is None:
            coordinator.mark_owner_unknown('I15_process_registry_terminal', 'cross_process_unproven')
        else:
            process_evidence = dict(process_evidence)
            process_evidence['source'] = OWNER_SPECS['I15_process_registry_terminal'].source
            refusal = coordinator.seal_owner('I15_process_registry_terminal', process_evidence)
            if refusal is not None:
                coordinator.mark_owner_unknown('I15_process_registry_terminal', getattr(refusal, 'code', 'read_failed'))
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        coordinator.mark_owner_unknown('I15_process_registry_terminal', 'read_failed')

    _seal_physical_profile_owner(runner, coordinator, receipt, descriptor)

    try:
        from cron.runtime_custody import preservation_owner_evidence
        for owner_class in (
            'I12_cron_fire', 'I13_external_cron_worker', 'I14_api_detached_cron',
        ):
            evidence = preservation_owner_evidence(
                owner_class,
                process_instance=receipt.process_instance,
                authority_epoch=receipt.authority_epoch,
                close_generation=receipt.close_generation,
                owner_id=f'{descriptor.get("instance_id", receipt.process_instance)}:{owner_class}',
            )
            if evidence is None:
                continue
            refusal = coordinator.seal_owner(owner_class, evidence)
            if refusal is not None:
                coordinator.mark_owner_unknown(owner_class, getattr(refusal, 'code', 'read_failed'))
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        for owner_class in (
            'I12_cron_fire', 'I13_external_cron_worker', 'I14_api_detached_cron',
        ):
            coordinator.mark_owner_unknown(owner_class, 'read_failed')


def _preservation_begin_gate(runner, params, peer_subject):
    """Validate the authenticated close request before touching coordinator state."""
    if not isinstance(peer_subject, str) or not peer_subject:
        return {'status': 'refused', 'reason': 'permission_denied'}, None, None
    if not isinstance(params, dict) or set(params) != {
        'instance_id', 'authority_epoch', 'owner_id', 'scope', 'deadline_s'
    }:
        return {'status': 'refused', 'reason': 'invalid_params'}, None, None
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    if params['instance_id'] != descriptor.get('instance_id'):
        return {'status': 'refused', 'reason': 'stale_instance'}, None, None
    if params['authority_epoch'] != descriptor.get('authority_epoch'):
        return {'status': 'refused', 'reason': 'stale_authority_epoch'}, None, None
    ownership_reason = _preservation_ownership_reason(runner)
    if ownership_reason is not None:
        return {'status': 'refused', 'reason': ownership_reason}, None, None
    coordinator = getattr(runner, 'runtime_custody', None)
    if coordinator is None:
        return {'status': 'refused', 'reason': 'runtime_custody_unavailable'}, None, None
    return None, descriptor, coordinator


async def begin_preservation_drain(runner, params, peer_subject):
    """Atomically close fresh roots through the existing authenticated private socket.

    This reversible phase only changes the admission state. It intentionally does not call the
    destructive shutdown drain/interrupt/kill path: a pre-handoff refusal must be able to restore
    the same owner without losing accepted leases.
    """
    refusal, descriptor, coordinator = _preservation_begin_gate(runner, params, peer_subject)
    if refusal is not None:
        return refusal
    receipt = coordinator.begin_close(
        owner_id=f'{peer_subject}:{params["owner_id"]}',
        scope=params['scope'],
        deadline_s=params['deadline_s'],
    )
    if hasattr(receipt, 'as_wire'):
        wire = receipt.as_wire()
    else:
        wire = {'status': 'refused', 'reason': getattr(receipt, 'code', 'close_conflict')}
    if wire.get('status') == 'closing':
        # Freeze MCP discovery before any close worker can cancel its task.  The
        # factory is attached to each authority during bootstrap and uses the
        # existing finalizer/coordinator roots for every accepted route.
        mcp_entries, mcp_failure = _close_mcp_factories_for_begin(
            runner, receipt.close_generation,
        )
        from gateway.session_hosted_service import begin_i09_source_close
        from gateway.preservation_source_close import (
            abort_source_close,
            await_source_resume,
            begin_source_close,
        )
        if mcp_failure is not None:
            if not _resume_mcp_factories(runner, receipt.close_generation, mcp_entries):
                _reclose_mcp_factories(mcp_entries)
                return {'status': 'refused', 'reason': 'mcp_source_resume_pending', 'receipt': wire}
            completed, reason = _abort_mcp_factories(
                coordinator, receipt, mcp_entries,
            )
            if not completed:
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            return {'status': 'refused', 'reason': mcp_failure}
        if not begin_i09_source_close(runner, receipt.close_generation):
            if not _resume_mcp_factories(runner, receipt.close_generation, mcp_entries):
                _reclose_mcp_factories(mcp_entries)
                return {'status': 'refused', 'reason': 'mcp_source_resume_pending', 'receipt': wire}
            completed, reason = _abort_mcp_factories(
                coordinator, receipt, mcp_entries,
            )
            if not completed:
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            return {'status': 'refused', 'reason': 'hosted_source_close_conflict'}
        source_registry = getattr(runner, '_preservation_source_close', None)
        if source_registry is not None and not begin_source_close(runner, receipt.close_generation):
            from gateway.session_hosted_service import abort_i09_source_close
            remaining = coordinator.remaining_seconds()
            source_rolled_back = False
            if remaining is not None and await await_source_resume(runner, receipt.close_generation, remaining):
                source_rolled_back = bool(
                    abort_i09_source_close(runner, receipt.close_generation)
                    and abort_source_close(runner, receipt.close_generation)
                )
            mcp_rolled_back = _resume_mcp_factories(runner, receipt.close_generation, mcp_entries)
            if not source_rolled_back or not mcp_rolled_back:
                _reclose_preservation_sources(
                    runner, receipt.close_generation, source_registry, mcp_entries,
                )
                return {'status': 'refused', 'reason': 'source_close_conflict', 'receipt': wire}
            completed, reason = _abort_mcp_factories(
                coordinator, receipt, mcp_entries,
            )
            if not completed:
                _reclose_preservation_sources(
                    runner, receipt.close_generation, source_registry, mcp_entries,
                )
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            return {'status': 'refused', 'reason': 'source_close_conflict'}
        # Preserve the existing reversible external-drain distinction: fresh roots refuse, while
        # already accepted FIFO continuations may claim and settle under their live parent lease.
        # Ordinary ``_draining`` remains reserved for shutdown/restart and must stay false here.
        runner._external_drain_active = True
        runner._preservation_drain_active = True
        descriptor.update(state='draining', capabilities=[], drain_reason='preservation')
    return wire


async def preservation_drain_status(runner, params, peer_subject):
    if not isinstance(peer_subject, str) or not peer_subject:
        return {'status': 'refused', 'reason': 'permission_denied'}
    if not isinstance(params, dict) or set(params) != {'instance_id', 'authority_epoch'}:
        return {'status': 'refused', 'reason': 'invalid_params'}
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    if params['instance_id'] != descriptor.get('instance_id'):
        return {'status': 'refused', 'reason': 'stale_instance'}
    if params['authority_epoch'] != descriptor.get('authority_epoch'):
        return {'status': 'refused', 'reason': 'stale_authority_epoch'}
    coordinator = getattr(runner, 'runtime_custody', None)
    if coordinator is None:
        return {'status': 'refused', 'reason': 'runtime_custody_unavailable'}
    return coordinator.status()


async def prepare_preservation_handoff(runner, params, peer_subject):
    """Return a typed local-drain or unknown result; native ACK remains a later phase."""
    if not isinstance(peer_subject, str) or not peer_subject:
        return {'status': 'refused', 'reason': 'permission_denied'}
    if not isinstance(params, dict):
        return {'status': 'refused', 'reason': 'invalid_params'}
    try:
        receipt = _preservation_receipt(params)
    except (TypeError, ValueError, KeyError):
        return {'status': 'refused', 'reason': 'invalid_params'}
    coordinator = getattr(runner, 'runtime_custody', None)
    if coordinator is None:
        return {'status': 'refused', 'reason': 'runtime_custody_unavailable'}
    ownership_reason = _preservation_ownership_reason(runner)
    if ownership_reason is not None:
        return {'status': 'refused', 'reason': ownership_reason}
    from gateway.session_runtime_workers import await_authority_work
    remaining = coordinator.remaining_seconds()
    if remaining is None:
        return {'status': 'refused', 'reason': 'stale_close_receipt'}
    # The source close is an awaited transport boundary.  Its close response carries the exact
    # queue epoch, registration and retained backlog snapshot; source_owner_ready is meaningful
    # only after that response and every accepted callback have settled.
    from gateway.preservation_source_close import await_source_close, source_owner_ready
    from gateway.session_hosted_service import await_i09_source_close
    source_registry = getattr(runner, '_preservation_source_close', None)
    if (source_registry is not None
            and not await await_source_close(runner, receipt.close_generation, remaining)):
        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        blocked = coordinator.prepare_handoff(receipt)
        if hasattr(blocked, 'as_wire'):
            return blocked.as_wire()
        return {'status': 'refused', 'reason': getattr(blocked, 'code', 'custody_pending')}
    remaining = coordinator.remaining_seconds()
    if remaining is None or not await await_i09_source_close(runner, receipt.close_generation, remaining):
        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        blocked = coordinator.prepare_handoff(receipt)
        if hasattr(blocked, 'as_wire'):
            return blocked.as_wire()
        return {'status': 'refused', 'reason': getattr(blocked, 'code', 'custody_pending')}
    if (source_registry is not None
            and not source_owner_ready(runner, receipt.close_generation)):
        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        blocked = coordinator.prepare_handoff(receipt)
        if hasattr(blocked, 'as_wire'):
            return blocked.as_wire()
        return {'status': 'refused', 'reason': getattr(blocked, 'code', 'custody_pending')}
    remaining = coordinator.remaining_seconds()
    if remaining is None:
        return {'status': 'refused', 'reason': 'stale_close_receipt'}
    for authority in _authorities(runner):
        try:
            await await_authority_work(authority, remaining)
        except TimeoutError:
            return {'status': 'refused', 'reason': 'custody_pending'}
    _seal_integrated_preservation_owners(runner, coordinator, receipt)
    prepared = coordinator.prepare_handoff(receipt)
    if hasattr(prepared, 'as_wire'):
        return prepared.as_wire()
    return {'status': 'refused', 'reason': getattr(prepared, 'code', 'custody_pending')}


def _mcp_factory_participants(runner):
    """Return configured source factories whose ingress joined this close generation."""
    return tuple(
        factory for authority in _authorities(runner)
        for factory in (getattr(authority, '_mcp_source_factory', None),)
        if factory is not None and getattr(factory, '_config_seen', False)
    )


def _close_mcp_factories_for_begin(runner, close_generation: int):
    """Close configured factories and retain their exact witnesses for rollback."""
    entries = []
    for factory in _mcp_factory_participants(runner):
        try:
            receipt = factory.close_ingress(close_generation)
        except (RemoteSourceError, OSError, RuntimeError, TypeError, ValueError):
            return tuple(entries), 'mcp_source_close_conflict'
        entries.append((factory, receipt))
        if receipt.outcome == 'unknown':
            return tuple(entries), 'mcp_source_close_unknown'
    return tuple(entries), None


def _mcp_factory_entries(runner, close_generation: int):
    """Read the exact source-owned factory receipts for one coordinator generation."""
    entries = []
    for factory in _mcp_factory_participants(runner):
        receipt = getattr(factory, 'close_receipt', None)
        if receipt is None or getattr(receipt, 'close_generation', None) != close_generation:
            return None
        entries.append((factory, receipt))
    return tuple(entries)


def _i09_close_active(runner) -> bool:
    """Distinguish an I09 source that joined this close from an untouched optional source."""
    from gateway.session_hosted_service import (
        _i09_intake_is_closed,
        _i09_source_catalog,
        _i09_source_installed,
    )

    if not _i09_source_installed(runner):
        return False
    catalog = _i09_source_catalog(runner)
    if catalog is None:
        return True
    if catalog.no_producer:
        return _i09_intake_is_closed(catalog.rows)
    return any(row.get('i17_registry', {}).get('intake_closed') is True for row in catalog.rows)


def _reclose_mcp_factories(entries) -> bool:
    """Re-fence every factory whose exact close witness is still live."""
    ok = True
    for factory, receipt in tuple(entries or ()):
        try:
            if not factory.reclose_ingress(receipt):
                ok = False
        except (RemoteSourceError, OSError, RuntimeError, TypeError, ValueError):
            ok = False
    return ok


def _resume_mcp_factories(runner, close_generation: int, entries=None) -> bool:
    """Resume all participating factories, re-fencing earlier successes on any failure."""
    entries = _mcp_factory_entries(runner, close_generation) if entries is None else tuple(entries)
    if entries is None:
        return False
    for factory, receipt in entries:
        try:
            if not factory.abort_ingress(receipt):
                raise RemoteSourceError("mcp_source_resume_failed")
        except (RemoteSourceError, RuntimeError, TypeError, ValueError):
            _reclose_mcp_factories(entries)
            return False
    return True


def _prepare_mcp_factories(entries, coordinator, coordinator_receipt) -> bool:
    """Authenticate every source release while the canonical coordinator is still CLOSING."""
    prepared = []
    for factory, receipt in tuple(entries or ()):
        try:
            if not factory.prepare_finish_abort(
                receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
            ):
                raise RemoteSourceError("mcp_source_finalization_pending")
            prepared.append((factory, receipt))
        except (RemoteSourceError, OSError, RuntimeError, TypeError, ValueError):
            _reclose_mcp_factories(entries)
            return False
    return len(prepared) == len(tuple(entries or ()))


def _finish_mcp_factories(entries, coordinator, coordinator_receipt) -> bool:
    """Release bounded factory witnesses after the coordinator has reopened."""
    ok = True
    for factory, receipt in tuple(entries or ()):
        try:
            if not factory.finish_abort(
                receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
            ):
                ok = False
        except (RemoteSourceError, OSError, RuntimeError, TypeError, ValueError):
            ok = False
    return ok


def _abort_mcp_factories(coordinator, receipt, entries) -> tuple[bool, str | None]:
    """Open the coordinator only after every resumed MCP source has a prepared release."""
    if not _prepare_mcp_factories(entries, coordinator, receipt):
        _reclose_mcp_factories(entries)
        return False, 'mcp_source_finalization_pending'
    refused = coordinator.abort_before_handoff(receipt)
    if refused is not None:
        _reclose_mcp_factories(entries)
        return False, getattr(refused, 'code', 'close_release_failed')
    if not _finish_mcp_factories(entries, coordinator, receipt):
        return False, 'mcp_source_finalization_pending'
    return True, None


def _reclose_preservation_sources(runner, close_generation: int, source_registry, mcp_entries=None) -> bool:
    """Re-fence every source after a partial abort while the coordinator stays CLOSING."""
    from gateway.preservation_source_close import begin_source_close
    from gateway.session_hosted_service import begin_i09_source_close

    hosted_ok = begin_i09_source_close(runner, close_generation)
    legacy_ok = source_registry is None or begin_source_close(runner, close_generation)
    mcp_ok = _reclose_mcp_factories(
        _mcp_factory_entries(runner, close_generation)
        if mcp_entries is None else mcp_entries
    )
    return hosted_ok and legacy_ok and mcp_ok


async def abort_preservation_drain(runner, params, peer_subject):
    """Reopen only the exact receipt after every source and profile root has restored."""
    if not isinstance(peer_subject, str) or not peer_subject:
        return {'status': 'refused', 'reason': 'permission_denied'}
    try:
        receipt = _preservation_receipt(params)
    except (TypeError, ValueError, KeyError):
        return {'status': 'refused', 'reason': 'invalid_params'}
    coordinator = getattr(runner, 'runtime_custody', None)
    if coordinator is None:
        return {'status': 'refused', 'reason': 'runtime_custody_unavailable'}
    ownership_reason = _preservation_ownership_reason(runner)
    if ownership_reason is not None:
        # Keep the close receipt and all unresolved leases live for the active lifecycle owner.
        # A failed ownership check is never permission to release or reopen that custody.
        return {'status': 'refused', 'reason': ownership_reason}
    remaining = coordinator.remaining_seconds()
    if remaining is None:
        return {'status': 'refused', 'reason': 'stale_close_receipt'}
    from gateway.preservation_source_close import abort_source_close, await_source_resume
    from gateway.session_hosted_service import abort_i09_source_close
    source_registry = getattr(runner, '_preservation_source_close', None)
    mcp_entries = _mcp_factory_entries(runner, receipt.close_generation)
    if mcp_entries is None:
        return {'status': 'refused', 'reason': 'mcp_source_receipt_missing'}
    source_close_active = (
        source_registry is not None
        and getattr(source_registry, 'state', None) == 'closing'
        and getattr(source_registry, 'close_generation', None) == receipt.close_generation
    )
    if (source_close_active
            and not await await_source_resume(runner, receipt.close_generation, remaining)):
        return {'status': 'refused', 'reason': 'source_resume_pending'}

    # Keep RuntimeCustodyCoordinator CLOSING while source and I17 finalizers restore.  A partial
    # source abort is re-fenced at the same generation, so retry sees the original receipt and
    # exact accepted leases instead of a falsely reopened runtime.
    i09_close_active = _i09_close_active(runner)
    if i09_close_active and not abort_i09_source_close(runner, receipt.close_generation):
        return {'status': 'refused', 'reason': 'hosted_source_resume_conflict'}
    if source_close_active and not abort_source_close(runner, receipt.close_generation):
        _reclose_preservation_sources(runner, receipt.close_generation, source_registry)
        return {'status': 'refused', 'reason': 'source_resume_conflict'}
    from tui_gateway.hosted_tui_custody import restore_after_abort
    if not restore_after_abort(runner, receipt):
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return {'status': 'refused', 'reason': 'i17_rebind_pending'}
    if not _resume_mcp_factories(runner, receipt.close_generation, mcp_entries):
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return {'status': 'refused', 'reason': 'mcp_source_resume_conflict'}
    if not _prepare_mcp_factories(mcp_entries, coordinator, receipt):
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return {'status': 'refused', 'reason': 'mcp_source_finalization_pending'}
    refused = coordinator.abort_before_handoff(receipt)
    if refused is not None:
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return refused.as_wire()
    if not _finish_mcp_factories(mcp_entries, coordinator, receipt):
        return {'status': 'refused', 'reason': 'mcp_source_finalization_pending'}
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    runner._external_drain_active = False
    runner._preservation_drain_active = False
    descriptor.update(state='ready', capabilities=['session-authority-v1', 'durable-admission-v1', 'event-replay-v1'])
    descriptor.pop('drain_reason', None)
    return {'status': 'open', 'instance_id': descriptor.get('instance_id'),
            'authority_epoch': descriptor.get('authority_epoch')}


async def acknowledge_preservation_native(runner, params, peer_subject):
    if not isinstance(peer_subject, str) or not peer_subject:
        return {'status': 'refused', 'reason': 'permission_denied'}
    try:
        receipt = _preservation_receipt(params)
    except (TypeError, ValueError, KeyError):
        return {'status': 'refused', 'reason': 'invalid_params'}
    coordinator = getattr(runner, 'runtime_custody', None)
    if coordinator is None:
        return {'status': 'refused', 'reason': 'runtime_custody_unavailable'}
    ownership_reason = _preservation_ownership_reason(runner, require_running=False)
    if ownership_reason is not None:
        return {'status': 'refused', 'reason': ownership_reason}
    result = coordinator.acknowledge_native(receipt)
    if hasattr(result, 'as_wire'):
        return result.as_wire()
    return {'status': 'refused', 'reason': getattr(result, 'code', 'native_ack_mismatch')}


def _withdraw_profile_home(runner, home):
    """Drop *home* from the served set every startup consumer reads (adapters, served status, cron
    ticks, ingress, spool recovery) without releasing its ``gateway.lock``."""
    reserved = getattr(runner.config, '_runtime_profile_homes', None)
    if reserved is not None:
        runner.config._runtime_profile_homes = tuple(
            entry for entry in reserved if Path(entry[1]).resolve() != home)


def _authority_tasks(authority):
    """Snapshot every async task family owned directly by one session authority."""
    tasks = [live.task for live in authority.sessions.values() if live.task is not None]
    tasks.extend(getattr(authority, '_bot_receipt_tasks', ()))
    from gateway.session_runtime_workers import mutation_tasks
    tasks.extend(mutation_tasks(authority))
    return list(dict.fromkeys(tasks))


def _api_finalizer_completion_succeeded(authority, admission_id, completion, future):
    """Require the source-owned API completion's exact released success receipt."""
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletionReceipt
    try:
        result = future.result()
    except BaseException:  # health: allow BLE001 -- opaque completion future failure keeps the authority retained
        return False
    receipt = getattr(completion, 'receipt', None)
    if (
        result is not receipt
        or not isinstance(receipt, ApiFinalizerCompletionReceipt)
        or receipt.state != 'complete'
        or receipt.finalizer_ok is not True
        or receipt.release_ok is not True
        or receipt.waiter_delivered is not True
        or receipt.reason is not None
    ):
        return False
    custody = getattr(completion, 'custody', None)
    handle = getattr(custody, 'handle', None)
    parent = getattr(completion, 'parent', None)
    expected = {
        'admission_id': admission_id,
        'run_id': getattr(custody, 'run_id', None),
        'profile_id': getattr(authority, 'profile_id', None),
        'authority_epoch': getattr(authority, 'epoch', None),
        'process_instance': getattr(authority, 'instance_id', None),
        'close_generation': getattr(handle, 'close_generation', None),
        'execution_generation': getattr(custody, 'execution_generation', None),
        'session_id': getattr(custody, 'admission_session_id', None),
        'principal': getattr(custody, 'producer_principal', None),
        'child_owner_id': getattr(handle, 'owner_id', None),
        'parent_owner_id': getattr(parent, 'owner_id', None),
    }
    if any(getattr(receipt, key, None) != value for key, value in expected.items()):
        return False
    if getattr(custody, 'finalizer_completion_receipt', None) is not receipt:
        return False
    identity_matches = getattr(completion, '_identity_matches', None)
    release_matches = getattr(completion, '_child_release_identity_matches', None)
    try:
        return (
            callable(identity_matches)
            and callable(release_matches)
            and identity_matches() is True
            and release_matches() is True
        )
    except BaseException:  # health: allow BLE001 -- opaque identity validation failure keeps the authority retained
        return False


async def _wait_api_finalizer_completions(authority, timeout):
    """Join source-owned API completion futures before unbinding the retiring profile."""
    from gateway.platforms.api_server_run_completion import (
        ApiFinalizerCompletion, completion_for, completion_for_custody,
    )
    registry = getattr(authority, '_api_finalizer_completions', None)
    if registry is None:
        return True
    if not isinstance(registry, dict):
        return False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout))
    while True:
        futures = []
        for admission_id, completion in tuple(registry.items()):
            if (
                type(admission_id) is not str
                or not isinstance(completion, ApiFinalizerCompletion)
                or completion_for(authority, admission_id) is not completion
                or getattr(completion, 'authority', None) is not authority
                or getattr(completion, 'admission_id', None) != admission_id
            ):
                return False
            custody = getattr(completion, 'custody', None)
            if (
                getattr(custody, 'admission_authority', None) is not authority
                or getattr(custody, 'admission_id', None) != admission_id
                or completion_for_custody(custody) is not completion
            ):
                return False
            future = getattr(completion, 'future', None)
            if not asyncio.isfuture(future):
                return False
            if future.done():
                if not _api_finalizer_completion_succeeded(authority, admission_id, completion, future):
                    return False
            else:
                futures.append(future)
        if not futures:
            return True
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        _, pending = await asyncio.wait(futures, timeout=remaining)
        if pending:
            return False


# How long a Stopped turn gets to settle durably before shutdown refuses to release ownership: a managed worker
# acknowledges Stop within STOP_ACK_SECONDS (30) or is terminated, then closed (<=10).
TURN_SETTLE_SECONDS = 45.0
# What a stop keeps of its watchdog leash for the phases after the settle (adapter teardown, executor
# quiesce, SessionDB close, exit-state writes): the settle never spends it.
STOP_TAIL_RESERVE_SECONDS = 10.0


def managed_turn_count(runner):
    """Unmapped claims, managed workers and owner mutations across the served authorities."""
    from gateway.session_runtime_workers import uncounted_runtime_work
    return uncounted_runtime_work(runner)


def stop_authority_turns(authority, *, in_process=False, reason=None):
    """Cooperative Stop for every turn *authority* is executing: the control a user's Stop sends to a
    managed worker (acknowledged, or terminated within STOP_ACK_SECONDS, and the admission settles
    through its own fenced path). ``in_process`` also hard-interrupts the running agent of each
    executing in-process turn, or latches the generation ``adopt_agent`` consumes before it exists.
    ``reason`` (a gateway restart/shutdown) is recorded on each worker it signals, so the turn settles
    with that cause; a worker already stopped (the user's Stop came first) keeps none. A worker
    still being spawned gets the same Stop and cause through latches (``_stop_spawning_managed``)."""
    from hermes_state_runtime import RuntimeStoreError
    signalled = 0
    workers = getattr(authority, '_managed_workers', {})
    for worker in list(workers.values()):
        if worker.closed.is_set() or (getattr(worker, 'stop', None) is not None and worker.stop.is_set()):
            continue
        worker.stop_cause = reason
        try:
            worker.control({'type': 'stop'})
        except RuntimeStoreError as exc:
            # A full control queue: control() latched worker.stop first, and the owner's read
            # loop escalates on that latch alone (terminate after STOP_ACK_SECONDS).
            logger.debug('managed Stop not queued (%s); the latch escalates it', exc.reason)
        signalled += 1
    signalled += _stop_spawning_managed(authority, workers, reason)
    if not in_process:
        return signalled
    from agent.interrupt_compat import request_hard_interrupt
    from gateway.run import _AGENT_PENDING_SENTINEL
    running = getattr(authority.runner, '_running_agents', {})
    for sid, live in list(authority.sessions.items()):
        generation = live.event_stream.execution.get('execution_generation')
        if generation is None or sid in workers or sid in getattr(authority, '_managed_spawning', {}):
            continue
        adopted_generation, agent = authority.adopted.get(sid, (None, None))
        if adopted_generation != generation:
            agent = running.get(live.route)
        if agent is None or agent is _AGENT_PENDING_SENTINEL:
            authority.pending_stops[sid] = generation
        else:
            request_hard_interrupt(agent, 'Profile stopping', tool_reason='gateway shutdown')
        signalled += 1
    return signalled


def _stop_spawning_managed(authority, workers, reason):
    """A managed turn whose worker is still being spawned has nothing to signal yet: latch the
    Stop for its generation (``adopt_agent`` delivers it to the worker, which is never
    bootstrapped) and, for a gateway restart/shutdown, the cause its pre-bootstrap interrupted
    result carries (#134889 GW-22). A user's Stop latched first keeps no cause."""
    stopped = 0
    for sid, generation in list(getattr(authority, '_managed_spawning', {}).items()):
        if sid in workers:
            continue
        stops = authority.pending_stops
        if reason and stops.get(sid) != generation:
            causes = getattr(authority, 'pending_stop_causes', None)
            if causes is None:
                causes = authority.pending_stop_causes = {}
            causes[sid] = (generation, reason)
        stops[sid] = generation
        stopped += 1
    return stopped


def stop_managed_turns(runner, reason=None):
    """``stop_authority_turns`` across every served authority (whole-runtime shutdown)."""
    return sum(stop_authority_turns(authority, reason=reason) for authority in _authorities(runner))


def interrupt_side_agents(runner, reason):
    """Whole-runtime shutdown: interrupt every counted side agent (``session_runtime_workers``)."""
    from gateway.session_runtime_workers import interrupt_side_agents as interrupt
    return interrupt(runner, reason)


async def _retire_profile_authority(authority, timeout=None):
    """Stop profile-local services and turns before its ownership is released.

    Claims are refused from here on; every executing turn gets a cooperative Stop, and its session
    task is joined (an in-process turn's task ends only after its executor thread returned and the
    admission settled). The hosted-room stop runs alongside that join under the same deadline: it
    never interrupts accepted turns and its room threads wait for the member turns they observe, so
    those turns must be signalled first or the room stop cannot finish. False when a turn or room
    thread misses *timeout* (default TURN_SETTLE_SECONDS): it may still call tools and write
    history, so the caller must keep the profile's reservation and store handles."""
    from gateway.session_cron import unbind_owner
    from gateway.session_runtime_workers import join_authority_work, stop_authority_work
    timeout = TURN_SETTLE_SECONDS if timeout is None else timeout
    authority.retiring = True
    # Close the profile's MCP ingress before worker/finalizer retirement.  The
    # factory records the same runtime close generation when preservation is
    # active; an unobserved config is left to normal discovery rather than
    # being treated as authenticated no-route.
    mcp_factory = getattr(authority, '_mcp_source_factory', None)
    if mcp_factory is not None and getattr(mcp_factory, '_config_seen', False):
        try:
            generation = (getattr(authority, '_preservation_coordinator', lambda: None)() or
                          getattr(authority.runner, 'runtime_custody', None))
            status = generation.status() if generation is not None else {}
            closed = mcp_factory.close_ingress(status.get('close_generation') or 0)
            if getattr(closed, 'outcome', None) == 'unknown':
                logger.error('Profile %s MCP source ingress is UNKNOWN; ownership retained',
                             authority.profile_id)
                return False
        except Exception:
            logger.error('Profile %s MCP source ingress did not close; ownership retained',
                         authority.profile_id, exc_info=True)
            return False
    if getattr(authority.runner, 'session_ticket_store', None) is not None:
        _publish_served_set(authority.runner)  # discovery and tickets stop naming it now
    stop_authority_work(authority)
    from gateway.session_llm_oneshot import stop as stop_oneshots
    stop_oneshots(authority)
    service = getattr(authority, 'hosted_room_service', None)
    rooms = None if service is None else asyncio.ensure_future(asyncio.to_thread(service.stop, timeout=timeout))
    try:
        await join_authority_work(authority, timeout)
        workers_stopped = True
    except TimeoutError:
        logger.error('Profile %s workers did not stop; ownership retained', authority.profile_id)
        workers_stopped = False
    if rooms is None:
        rooms_stopped = True
    else:
        room_result = await rooms
        # The retirement path owns the same in-process room finalizer as the ordinary hosted
        # service stop path.  A successful thread join is only a physical observation; the I17
        # child lease must consume that exact receipt before the profile root can close.
        from tui_gateway.hosted_tui_custody import room_service_stop
        rooms_stopped = room_service_stop(service, room_result)
    if not rooms_stopped:
        logger.error('Profile %s hosted rooms did not stop; ownership retained', authority.profile_id)
    if not (workers_stopped and rooms_stopped):
        return False
    # The public MCP lifecycle owns each task's actual SDK context manager and
    # calls the remote receipt before the authority finalizer sweep below.
    try:
        from tools.mcp_tool_lifecycle import shutdown_mcp_servers
        multiplex = bool(getattr(getattr(authority.runner, 'config', None),
                                 'multiplex_profiles', False))
        scope = authority.profile_id if multiplex else None
        await asyncio.to_thread(shutdown_mcp_servers, scope=scope, timeout=timeout)
    except Exception:
        logger.error('Profile %s MCP servers did not settle; ownership retained',
                     authority.profile_id, exc_info=True)
        return False
    # No drain is left to answer an observer while the authority is still the canonical retiring
    # owner.  Release these existing futures before joining wrapper/finalizer tasks: API/I03/I05
    # cleanup must resolve against this authority's retiring epoch, not a retired/unbound profile.
    from hermes_state_runtime import RuntimeStoreError
    authority.native_waiters.clear()
    for waiter in authority.waiters.values():
        if not waiter.done():
            waiter.set_exception(RuntimeStoreError('runtime_draining'))
    authority.waiters.clear()
    # Idle drains and receipt watchers now wake or refuse against the still-canonical authority.
    rest = _authority_tasks(authority)
    for task in rest:
        task.cancel()
    if rest:
        await asyncio.gather(*rest, return_exceptions=True)
    if not await _wait_api_finalizer_completions(authority, timeout):
        logger.error('Profile %s API finalizer completions did not settle; ownership retained',
                     authority.profile_id)
        return False
    if not _settle_external_process_finalizers(authority):
        logger.error('Profile %s external process finalizers did not settle; ownership retained',
                     authority.profile_id)
        return False
    from tui_gateway.hosted_tui_custody import close_profile, release_profile_source
    if not close_profile(authority):
        logger.error('Profile %s hosted TUI custody did not settle; ownership retained',
                     authority.profile_id)
        return False
    # ``close_profile`` keeps the exact source binding available for a reversible preservation
    # abort.  This profile is leaving the authority registry permanently, so revoke that exact
    # terminal binding before the successor can reuse the profile name.
    if not release_profile_source(authority):
        logger.error('Profile %s hosted TUI source binding did not revoke; ownership retained',
                     authority.profile_id)
        return False
    # Keep the source owner bound while the exact I17 root is read/terminalized. A failed close
    # leaves the authority and its UNKNOWN custody available for retry; release the owner only
    # after that terminal receipt succeeds.
    unbind_owner(authority)
    # Ownership is released next; until a successor bumps the epoch, A's epoch still fences its
    # writes as current, so a stale connection to A must find it retired instead.
    authority.retired = True
    return True


class ProfileOwnershipRetained(RuntimeError):
    """A retired attempt's writer is still live: the profile keeps its reservation and authority."""


async def serve_profile_runtime(runner, name, home):
    """Hot-serve one reserved profile's runtime: build its authority, recover its durable state and
    publish it in the descriptor/ticket store — the steps boot performs per secondary. Raises when
    the profile's store is unusable; the caller parks it (and releases the reservation). Its
    queued native rows are recovered once its adapters connect (``recover_served_native_rows``)."""
    from gateway.session_authorities import owner_scope
    from gateway.session_hosted_service import _ensure_hosted_service, start_ready_hosted_services
    from gateway.session_local_adoption import adopt_local_compressions
    from gateway.session_local_recovery import recover_local_sessions
    from gateway.platforms.webhook_ingress import recover_webhook_finalizations
    home = await asyncio.to_thread(Path(home).resolve)
    registry = runner.session_authorities
    existing = registry.for_home(home)
    if existing is not None and getattr(existing, 'retiring', False):
        # An earlier failed attempt kept this authority because its writer outlived Stop. It refuses
        # every admission, so serving it would start adapters on a dead profile: finish its
        # retirement without waiting, or refuse (the caller parks and keeps the reservation).
        if not await _retire_profile_authority(existing, timeout=0):
            raise ProfileOwnershipRetained(f'an earlier serve of {home} is still stopping')
        registry.remove(home)
    elif existing is not None:
        return existing
    authority = await _build_profile_authority(
        runner, name, home, register=False, defer_local_recovery=True)
    try:
        # Hot-served profiles are added after the hosted runtime binding is published.  Reserve
        # the I17 root before recovery can publish a hosted service, room, or compute-host
        # producer; waiting for owner_evidence would be too late because preservation has already
        # entered closing.  Lightweight recovery callers without a live hosted control listener
        # do not claim an I17 owner merely by constructing an authority.
        coordinator = getattr(runner, 'runtime_custody', None)
        if coordinator is not None and getattr(runner, 'session_control_server', None) is not None:
            from tui_gateway.hosted_tui_custody import bind_authority
            bind_authority(authority, runner=runner, coordinator=coordinator)
        with owner_scope(authority):
            _drop_retired_local_adapter(authority)
            await recover_bot_mailbox(authority)
            await adopt_local_compressions(authority)
            recover_local_sessions(authority, schedule=True, allow_legacy=False)
            await recover_webhook_finalizations(authority)
            _recover_published_api_turns(runner, authority)
            # Hot-served authorities join the same source-owned I10 close graph before they can
            # publish a served profile or accept new ingress.
            bootstrap_i10_consumer_sources(runner)
        if getattr(runner, 'session_control_server', None) is not None:
            await _ensure_hosted_service(runner, authority)
    except BaseException:
        if await _retire_profile_authority(authority):
            registry.remove(home)
        raise
    _publish_served_set(runner)
    start_ready_hosted_services(runner)
    return authority


def _drop_retired_local_adapter(authority):
    """A LOCAL adapter still bound to this profile's previous authority, once that authority fully
    retired, gives way to *authority*'s own: restore and create otherwise refuse every local session
    of the served profile as ``runtime_draining``. A retained writer never gets here (the serve is
    refused while the previous authority is still retiring)."""
    from gateway.config import Platform
    from gateway.session_local_recovery import local_adapter_map
    adapters = local_adapter_map(authority)
    previous = getattr(adapters.get(Platform.LOCAL), 'authority', None)
    if previous is not None and previous is not authority and getattr(previous, 'retired', False):
        del adapters[Platform.LOCAL]


def _recover_published_api_turns(runner, authority):
    """Boot recovers queued API turns when the launch API adapter is published
    (``recover_api_turns``); a profile served after that runs the same pass on its own store."""
    from gateway.config import Platform
    api = (getattr(runner, 'adapters', None) or {}).get(Platform.API_SERVER)
    if api is not None:
        from gateway.session_api_turn import recover_authority_api_turns
        recover_authority_api_turns(api, authority)


async def recover_served_native_rows(runner):
    """Boot's native-row recovery once hot-served profiles' adapters connected (boot runs it after
    every adapter connects), in every serving ledger: a ``bot_profile`` route runs a bot's messages
    on another profile, so the new bots can carry rows queued in any ledger. A session already
    draining keeps its one drain. A store that cannot answer leaves its rows queued for the next
    start and never stops the other ledgers or the reconcile."""
    if getattr(runner, 'session_authorities', None) is None:
        return
    from gateway.session_authorities import serving_authorities
    for authority in serving_authorities(runner):
        try:
            await recover_profile_native_sessions(runner, authority)
        except Exception:  # health: allow BLE001 -- logged; the rows stay queued for the next start
            logger.error('Native session recovery failed for %s', authority.profile_id, exc_info=True)


async def unserve_profile_runtime(runner, home):
    """Retire one profile's authority (deleted while running) and shrink the published set.
    False when one of its turns outlived the Stop deadline (keep the reservation); True otherwise,
    including for a profile this process never served."""
    home = await asyncio.to_thread(Path(home).resolve)
    registry = runner.session_authorities
    authority = registry.for_home(home)
    if authority is None:
        return True
    retired = await _retire_profile_authority(authority)
    if not retired:
        return False
    registry.remove(home)
    _publish_served_set(runner)
    return retired


def _authorities(runner):
    from gateway.session_authorities import all_authorities
    return all_authorities(runner)


async def recover_bot_mailbox(authority):
    """Owner-startup Bot Chat recovery, skipped when this token cannot use the mailbox dir.

    An elevated install/update can leave ``runtime/bot_live_delivery`` owned by Administrators with
    no entry for the user (#139485, #138872): the gateway cannot even open its ``.lock``. Bot Chat
    deliveries pause (``_locked`` already logged the dir, its owner and the remedy); every other
    client keeps working instead of the whole gateway refusing to start. Any other failure still
    aborts startup."""
    from gateway.session_bot import recover_bot_deliveries
    from tools.bot_live_delivery import MailboxUnusable
    try:
        await recover_bot_deliveries(authority)
    except MailboxUnusable as exc:
        logger.warning('Bot Chat recovery skipped for %s: %s', authority.db.db_path, exc)


async def start_gateway_runtime_api(runner):
    from gateway.run_api import start_gateway_api
    runner.session_api = await start_gateway_api(runner)
    runner.session_runtime_descriptor['api_origin'] = runner.session_api.api_origin
    for authority in _authorities(runner):
        await _recover_profile_bot_mailbox(runner, authority)


async def _recover_profile_bot_mailbox(runner, authority):
    """Startup Bot Chat recovery of one served profile. A secondary whose durable leftovers cannot
    be recovered is parked like one whose store cannot be opened (retired, unreserved, named under
    ``parked_profiles``): under multiplex it would otherwise take every other profile down with it,
    on every restart (#140018). The launch profile's failure still aborts the start."""
    from gateway.session_authorities import owner_scope
    try:
        with owner_scope(authority):
            await recover_bot_mailbox(authority)
    except Exception as exc:
        registry = getattr(runner, 'session_authorities', None)
        name = None if registry is None else registry.profile_name(authority)
        if name is None:
            raise  # the launch profile's store is the process's own; nothing to park it behind
        logger.error("[MULTIPLEX] Profile '%s' not served: startup recovery failed: %s", name, exc,
                     exc_info=True)
        home = await asyncio.to_thread(Path(authority.profile_id).resolve)
        if not await _retire_profile_authority(authority):
            # A writer outlived Stop: the lock and the retiring authority stay for shutdown to
            # settle, but the profile leaves the served set, so later boot never starts it.
            _withdraw_profile_home(runner, home)
            park_profile(runner, name, f'startup recovery failed; ownership retained (a control-socket rescan, '
                                       f'`hermes -p {name} gateway run`, serves it once its writer stops): {exc}')
            return
        registry.remove(home)
        _publish_served_set(runner)
        release_profile_home(runner, home)
        park_profile(runner, name, f'startup recovery failed (a control-socket rescan, '
                                   f'`hermes -p {name} gateway run`, serves it again): {exc}')


async def recover_gateway_native_sessions(runner):
    """Recover against the published routing index and currently connected adapters.

    Stored envelopes are input, not authority to create a route or reconnect a
    transport. The authority preflights every queued sender before any claim.
    """
    from gateway.session_authorities import owner_scope, serving_authorities
    authorities = serving_authorities(runner)
    if not authorities:
        return {}
    from gateway.session_hosted_service import ensure_hosted_service
    await ensure_hosted_service(runner)
    from gateway.session_local_recovery import recover_local_sessions
    from gateway.platforms.webhook_ingress import recover_webhook_finalizations
    results = {}
    for authority in authorities:
        with owner_scope(authority):
            recover_local_sessions(authority, schedule=True)
            await recover_webhook_finalizations(authority)
        results.update(await recover_profile_native_sessions(runner, authority))
    return results


async def recover_profile_native_sessions(runner, authority):
    """Bind one authority's queued native rows to the adapters connected now."""
    from gateway.session_authorities import owner_scope
    from gateway.session_local_recovery import restore_pending_local_automation
    with owner_scope(authority):
        pending_rows = authority.db._read_all(
            "SELECT DISTINCT target_session_id FROM session_admissions WHERE status IN ('queued','unknown')")
        pending = {row['target_session_id'] for row in pending_rows}
        restore_pending_local_automation(authority, pending)
        bindings = [(owner, entry.origin, runner._adapter_for_source(entry.origin))
                    for entry in runner.session_store.list_sessions() if entry.origin is not None
                    for owner in [authority.logical_owner(entry.session_id)] if owner in pending]
        outcome = await authority.recover_native_sessions(bindings)
    for sid, verdict in outcome.items():
        logger.info('Native session startup recovery %s (%s): %s', sid, authority.profile_id, verdict)
    return outcome


def publish_gateway_runtime_ready(runner):
    descriptor = runner.session_runtime_descriptor
    if runner.session_api.task.done() or not runner._running or runner._draining:
        raise RuntimeError('gateway stopped before session API readiness')
    descriptor.update(state='ready', capabilities=[
        'session-authority-v1', 'durable-admission-v1', 'event-replay-v1'])
    from gateway.session_hosted_service import start_ready_hosted_services
    start_ready_hosted_services(runner)


async def wait_gateway_runtime(runner):
    """A vanished interactive listener is fatal, not a healthy headless runtime."""
    shutdown = asyncio.create_task(runner.wait_for_shutdown())
    listener = runner.session_api.task
    try:
        done, _ = await asyncio.wait({shutdown, listener}, return_when=asyncio.FIRST_COMPLETED)
        if listener in done and not runner._draining:
            runner.session_runtime_descriptor.update(state='failed', capabilities=[])
            error = None if listener.cancelled() else listener.exception()
            raise RuntimeError('gateway session API stopped unexpectedly') from error
        await shutdown
    finally:
        if not shutdown.done():
            shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)


async def drain_gateway_runtime(runner):
    """Withdraw admission before any await; close sockets before DB teardown."""
    descriptor = getattr(runner, 'session_runtime_descriptor', None)
    if descriptor is None:
        return
    runner._draining = True
    descriptor.update(state='draining', capabilities=[])
    from gateway.drain_control import RESTART_WAIT_REASON
    if descriptor.get('drain_reason') == RESTART_WAIT_REASON:
        del descriptor['drain_reason']  # stopping now: no client attaches to answer anymore
    from gateway.session_hosted_service import stop_hosted_service
    await stop_hosted_service(runner)
    # Withdraw the public ingress callback without disconnecting egress needed
    # by already admitted work. Base adapters refuse before stamping acceptance.
    for adapter in runner.adapters.values():
        adapter.set_message_handler(None)
    for adapters in getattr(runner, '_profile_adapters', {}).values():
        for adapter in adapters.values():
            adapter.set_message_handler(None)


async def settle_gateway_runtime(runner, budget=None, reason=None):
    """Keep authority tasks alive until their last durable settlement write, within a bound.

    A managed turn still running here gets its Stop first (its worker acknowledges or is
    terminated, and the admission settles interrupted or ``unknown``); a task that still misses
    TURN_SETTLE_SECONDS (clamped to ``budget``, what the stop's watchdog leash has left, minus
    STOP_TAIL_RESERVE_SECONDS) is logged and left running for recovery. The stop sequence continues:
    the executor quiesce skips the DB close while a writer is live, and process exit releases
    the ownership lock, so raising here would only skip teardown and disarm the watchdog."""
    authorities = _authorities(runner)
    stop_managed_turns(runner, reason)
    from gateway.session_llm_oneshot import stop as stop_oneshots
    for authority in authorities:
        stop_oneshots(authority)  # an answer is no durable settlement: never waited for
    tasks = [task for authority in authorities for task in _authority_tasks(authority)]
    wait = TURN_SETTLE_SECONDS if budget is None else max(0.0, min(TURN_SETTLE_SECONDS,
                                                                   budget - STOP_TAIL_RESERVE_SECONDS))
    if tasks:
        done, pending = await asyncio.wait(tasks, timeout=wait) if wait > 0 else (set(), set(tasks))
        for task in done:
            if not task.cancelled():
                task.exception()
        if pending:
            logger.warning('%d authority task(s) did not settle within %.0fs; left for recovery',
                           len(pending), wait)
    from gateway.session_runtime_workers import join_authority_work
    for authority in authorities:
        try:
            await join_authority_work(authority, 0)
        except TimeoutError:
            logger.warning('Profile %s still has live writers at shutdown; its admissions stay for recovery',
                           getattr(authority, 'profile_id', '?'))
        if not _settle_external_process_finalizers(authority):
            logger.warning('Profile %s external process finalizers remain retained at shutdown',
                           getattr(authority, 'profile_id', '?'))
    # Work settled above. ACP has no per-session destroy,
    # so the stop is the end of every ACP session nobody is viewing (#118216).
    from gateway.session_acp_lifecycle import end_idle_acp_sessions
    for authority in _authorities(runner):
        try:
            end_idle_acp_sessions(authority)
        except Exception:
            # Best-effort bookkeeping: a store that cannot answer must not abort the stop sequence.
            import logging
            logging.getLogger(__name__).warning('ACP sessions of %s not ended at shutdown',
                                                getattr(authority, 'profile_id', '?'), exc_info=True)
    from gateway.run_api import stop_gateway_api
    store = getattr(runner, 'session_ticket_store', None)
    if store is not None:
        store.revoke()
    handle = getattr(runner, 'session_api', None)
    if handle is not None:
        await stop_gateway_api(handle)
