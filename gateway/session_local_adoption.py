"""A local receipt's compression catch-up (#140060), committed off the owner loop.

``adopt_untracked_compression`` takes the writer lock, up to ``_WRITE_PATIENCE_S`` when contended. On
the owner loop that stalls every profile it serves (streams, controls, admissions), including during
a hot-profile activation. So the async paths that restore a receipt (boot and hot-profile recovery, a
socket's first read) commit the catch-up in a worker first, and the restore that follows only reads.
A managed path reserves the existing I11 operation before that worker and, when a local admission is
queued, keeps its exact I10 root as the parent.
"""
import asyncio
from dataclasses import dataclass
import logging
from functools import partial
import sqlite3

from hermes_state_runtime import RuntimeStoreError

logger = logging.getLogger(__name__)

_PAUSED_RUNTIME_REASONS = frozenset({
    'join_failed', 'read_failed', 'runtime_draining', 'stale_generation',
    'storage_unavailable', 'stale_epoch',
})


@dataclass(frozen=True)
class LocalAdoptionCandidate:
    receipt: dict
    children: tuple[str, ...]


@dataclass(frozen=True)
class PreparedLocalAdoption:
    candidate: LocalAdoptionCandidate | None
    source: object
    operation_key: object
    # ``parent_handle`` is one exact I10 handle for the legacy path, or the immutable tuple
    # reserved for a multi-admission FIFO compression.
    parent_handle: object | tuple[object, ...] | None


def on_owner_loop():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _managed(authority):
    # Attribute presence, including a present-but-None registry, is the managed bootstrap marker.
    return hasattr(authority.runner, 'session_authorities')


def _due(authority, sid, *, strict=False):
    """Return the persisted receipt and exact untracked children, or refuse the catch-up."""
    from gateway.session_local_recovery import owned_receipt
    from hermes_state_local_lineage import adoptable_compression
    try:
        receipt = owned_receipt(authority, sid)[0]
        with authority.db._read_ctx() as conn:
            adoptable = adoptable_compression(conn, receipt)
        if adoptable is None:
            return None
        children = adoptable[0]
        if (not isinstance(children, (tuple, list)) or not children
                or not all(isinstance(child, str) and child for child in children)):
            raise RuntimeStoreError('preservation_receipt_unmapped')
        return LocalAdoptionCandidate(receipt, tuple(children))
    except (KeyError, TypeError, ValueError, AttributeError, OSError,
            RuntimeStoreError, sqlite3.Error) as exc:
        if strict:
            if isinstance(exc, RuntimeStoreError):
                raise
            raise RuntimeStoreError('storage_unavailable') from exc
        return None  # the restore refuses this receipt and logs the check that failed


def _adopt(authority, sid, receipt):
    from hermes_state_local_lineage import adopt_untracked_compression
    adopt_untracked_compression(authority.db, epoch=authority.epoch, session_id=sid, expected=receipt)


def _unknown_reason(exc):
    if isinstance(exc, RuntimeStoreError):
        return exc.reason if exc.reason in {'read_failed', 'join_failed',
                                            'foreign_custody', 'runtime_draining'} else 'read_failed'
    if isinstance(exc, (OSError, sqlite3.Error)):
        return 'read_failed'
    return 'join_failed'


def _supported_pause(exc):
    return isinstance(exc, RuntimeStoreError) and exc.reason in _PAUSED_RUNTIME_REASONS


def _retain_failure(authority, prepared, exc):
    """Retain exact custody while preserving the source failure for identity/storage faults."""
    reason = _unknown_reason(exc)
    try:
        _retain_unknown(authority, prepared, reason)
    except BaseException:
        logger.warning('Local session %s could not retain exact UNKNOWN custody',
                       prepared.source.session_id, exc_info=True)


def _parent_tuple(parent_handle):
    if parent_handle is None:
        return ()
    if isinstance(parent_handle, tuple):
        return parent_handle
    return (parent_handle,)


def _mark_parent_unknown(authority, parent_handle, reason):
    parents = _parent_tuple(parent_handle)
    if not parents:
        return
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    for parent in parents:
        coordinator.mark_unknown(parent, reason)
        lease_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
        record = getattr(coordinator, '_records', {}).get(lease_id)
        if record is None or record.handle is not parent or record.state != 'unknown':
            raise RuntimeStoreError('preservation_owner_unmapped')


def _retain_unknown(authority, prepared, reason):
    """Retain the I11 operation and every exact I10 parent on any uncertain path."""
    try:
        authority._preservation_operation_unknown_verified(prepared.operation_key, reason)
    finally:
        _mark_parent_unknown(authority, prepared.parent_handle, reason)



def _prepare_managed(authority, sid, candidate):
    if authority._preservation_coordinator() is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    from gateway.session_local_recovery import local_adoption_parents
    from gateway.session_recovery_custody import local_policy_lineage_plan

    parent = None
    try:
        parent = local_adoption_parents(authority, sid) or None
        source = local_policy_lineage_plan(
            authority, sid, candidate.receipt, expected_children=candidate.children,
        )
        key = authority._preservation_operation_start_local_compression(
            source, parent_handles=parent,
        )
    except Exception:
        _mark_parent_unknown(authority, parent, 'read_failed')
        raise
    return PreparedLocalAdoption(candidate, source, key, parent)


def _current_source_is_committed(authority, prepared):
    from gateway.session_recovery_custody import local_policy_lineage_witness
    try:
        current = local_policy_lineage_witness(authority, prepared.source.session_id)
    except (AttributeError, KeyError, LookupError, RuntimeStoreError, TypeError, ValueError):
        return None
    return current if authority._local_compression_source_current(
        prepared.source, committed=True,
    ) else None


def _resume_managed(authority, sid):
    """Reconcile one retained handle before looking for a newly due tail."""
    retained = authority._preservation_operation_find_local_compression(sid)
    if retained is None:
        return False, None
    key, record, source, parent = retained
    if not record.get('unknown'):
        return True, None
    try:
        if authority._local_compression_source_current(source, committed=True):
            current = authority._preservation_operation_reconcile_local_compression_committed(
                key, record, source, parent,
            )
            return True, PreparedLocalAdoption(None, current, key, parent)
        candidate = _due(authority, sid, strict=True)
        if candidate is None or not authority._local_compression_source_current(source):
            return True, None
        authority._preservation_operation_retry_local_compression(
            key, record, source, parent,
        )
        return True, PreparedLocalAdoption(candidate, source, key, parent)
    except RuntimeStoreError as exc:
        prepared = PreparedLocalAdoption(None, source, key, parent)
        _retain_failure(authority, prepared, exc)
        if _supported_pause(exc):
            logger.warning('Local session %s remains paused during managed reconciliation: %s',
                           sid, exc.reason)
            return True, None
        raise
    except (OSError, sqlite3.Error) as exc:
        prepared = PreparedLocalAdoption(None, source, key, parent)
        _retain_failure(authority, prepared, exc)
        raise
    except Exception as exc:  # health: allow BLE001 -- unexpected custody failure must surface
        prepared = PreparedLocalAdoption(None, source, key, parent)
        _retain_failure(authority, prepared, exc)
        raise


async def _run_managed(authority, prepared, *, finish, write_effect=True):
    sid = prepared.source.session_id
    try:
        if write_effect:
            from gateway.session_runtime_workers import tracked_write
            if prepared.candidate is None:
                raise RuntimeStoreError('preservation_owner_unmapped')
            await tracked_write(authority, partial(_adopt, authority, sid, prepared.candidate.receipt))
        if getattr(authority, 'retiring', False):
            raise RuntimeStoreError('runtime_draining')
        committed = _current_source_is_committed(authority, prepared)
        if committed is None:
            raise RuntimeStoreError('read_failed')
        if write_effect:
            committed = authority._preservation_operation_record_local_compression_committed(
                prepared.operation_key, prepared.source,
            )
        else:
            # Reconciliation reads a fresh immutable witness.  Rebind that exact readback to the
            # retained operation before terminal proof; object identity is part of the custody
            # contract and prevents a copied/foreign witness from settling the I11 handle.
            committed = authority._preservation_operation_record_local_compression_committed(
                prepared.operation_key, prepared.source,
            )
        if finish == 'mount':
            from gateway.session_local_recovery import restore_local_session
            restore_local_session(authority, sid)
        else:
            from gateway.session_local_recovery import complete_local_recovery
            complete_local_recovery(
                authority, sid, schedule=finish == 'schedule', allow_legacy=False,
            )
        if not authority._preservation_operation_local_compression_terminal(
                prepared.operation_key, committed):
            raise RuntimeStoreError('read_failed')
    except asyncio.CancelledError:
        _retain_unknown(authority, prepared, 'join_failed')
        raise
    except RuntimeStoreError as exc:
        _retain_failure(authority, prepared, exc)
        if _supported_pause(exc):
            logger.warning('Local session %s paused after managed compression catch-up: %s',
                           sid, exc.reason)
            return
        raise
    except (OSError, sqlite3.Error) as exc:
        _retain_failure(authority, prepared, exc)
        raise
    except Exception as exc:  # health: allow BLE001 -- unexpected custody failure must surface
        _retain_failure(authority, prepared, exc)
        raise


async def adopt_before_restore(authority, sid, *, tracked=True):
    """Commit *sid*'s due catch-up in a worker, so the restore that follows only reads."""
    if _managed(authority):
        retained, prepared = _resume_managed(authority, sid)
        if retained:
            if prepared is None:
                return
            await _run_managed(
                authority, prepared, finish='mount' if not tracked else 'observe',
                write_effect=prepared.candidate is not None,
            )
            return
    try:
        candidate = _due(authority, sid, strict=_managed(authority))
    except RuntimeStoreError as exc:
        if _supported_pause(exc):
            logger.warning('Local session %s remains paused before managed catch-up: %s', sid, exc.reason)
            return
        raise
    if candidate is None:
        return
    if not _managed(authority):
        # Direct callers without a managed registry retain the historical legacy entry semantics.
        write = partial(_adopt, authority, sid, candidate.receipt)
        try:
            if tracked:
                from gateway.session_runtime_workers import tracked_write
                await tracked_write(authority, write)
            else:
                await asyncio.to_thread(write)
        except RuntimeStoreError as exc:
            logger.warning('Local session %s compression catch-up failed: %s', sid, exc.reason)
        return
    try:
        prepared = _prepare_managed(authority, sid, candidate)
    except RuntimeStoreError as exc:
        if _supported_pause(exc):
            logger.warning('Local session %s remains paused before managed reservation: %s', sid, exc.reason)
            return
        raise
    await _run_managed(
        authority, prepared, finish='mount' if not tracked else 'observe', write_effect=True,
    )


async def adopt_local_compressions(authority):
    """Boot and hot-profile recovery: reserve and finish every due local catch-up."""
    from gateway.session_local_recovery import local_receipt_ids
    for sid in local_receipt_ids(authority):
        await adopt_before_restore(authority, sid, tracked=False)


def schedule_adoption(authority, sid, receipt):
    """A sync restore on the owner loop hands its due catch-up to a tracked source worker."""
    from gateway.session_runtime_workers import track_mutation
    pending = getattr(authority, '_local_adoptions', None)
    if pending is None:
        pending = authority._local_adoptions = {}
    if sid in pending and not pending[sid].done():
        return
    logger.info('Local session %s: committing its compression catch-up off the owner loop', sid)
    if not _managed(authority):
        async def adopt_then_publish_legacy():
            try:
                await asyncio.to_thread(_adopt, authority, sid, receipt)
                if not getattr(authority, 'retiring', False):
                    from gateway.session_local_recovery import restore_local_session
                    restore_local_session(authority, sid)
            except RuntimeStoreError as exc:
                logger.warning('Local session %s paused after its compression catch-up: %s', sid, exc.reason)
        pending[sid] = track_mutation(authority, adopt_then_publish_legacy())
        return
    retained, prepared = _resume_managed(authority, sid)
    if retained:
        if prepared is None:
            return
        operation = _run_managed(
            authority, prepared, finish='schedule', write_effect=prepared.candidate is not None,
        )
    else:
        candidate = _due(authority, sid, strict=True)
        if candidate is None or candidate.receipt != receipt:
            raise RuntimeStoreError('stale_generation')
        prepared = _prepare_managed(authority, sid, candidate)
        operation = _run_managed(authority, prepared, finish='schedule', write_effect=True)
    try:
        pending[sid] = track_mutation(authority, operation)
    except BaseException as exc:
        operation.close()
        _retain_unknown(authority, prepared, _unknown_reason(exc))
        raise
