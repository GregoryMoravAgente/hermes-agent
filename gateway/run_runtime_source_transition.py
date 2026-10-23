"""Public reversible preservation transition orchestration.

The facade in :mod:`gateway.run_runtime` keeps its historical import surface; this sibling owns
the source-transition phase ladder so the facade remains a thin routing module.
"""
from __future__ import annotations

import time

from gateway import run_runtime

_preservation_begin_gate = run_runtime._preservation_begin_gate
_close_mcp_factories_for_begin = run_runtime._close_mcp_factories_for_begin
_resume_mcp_factories = run_runtime._resume_mcp_factories
_reclose_mcp_factories = run_runtime._reclose_mcp_factories
_abort_mcp_factories = run_runtime._abort_mcp_factories
_reclose_preservation_sources = run_runtime._reclose_preservation_sources
_preservation_receipt = run_runtime._preservation_receipt
_preservation_ownership_reason = run_runtime._preservation_ownership_reason
_mcp_factory_entries = run_runtime._mcp_factory_entries
_i09_close_active = run_runtime._i09_close_active


async def _begin_cron_source_fence(coordinator, receipt):
    """Acquire the real per-profile cron fences before the transition starts source cleanup."""
    registry = getattr(coordinator, "_cron_source_registry", None)
    graph_ready = bool(
        registry is not None
        and callable(getattr(registry, "_graph_ready_locked", None))
        and registry._graph_ready_locked()
        and callable(getattr(registry, "_validate_graph_locked", None))
        and registry._validate_graph_locked()
    )
    if not graph_ready:
        return None
    remaining = coordinator.remaining_seconds()
    if remaining is None or remaining <= 0:
        return {"status": "refused", "reason": "cron_source_fence_deadline"}
    from gateway.runtime_cron_source_registry import begin_cron_close_async
    if not await begin_cron_close_async(
        coordinator, receipt.close_generation, deadline=time.monotonic() + remaining,
    ):
        return {"status": "refused", "reason": "cron_source_fence_conflict"}
    return None


async def begin_preservation_drain_impl(runner, params, peer_subject):
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
        # Hold each real per-profile cron jobs lock for this same close generation.  If the
        # bounded source barrier cannot finish, keep the coordinator CLOSING with its receipt;
        # releasing only the coordinator would leave a live cron producer outside the transition.
        cron_refusal = await _begin_cron_source_fence(coordinator, receipt)
        if cron_refusal is not None:
            cron_refusal['receipt'] = wire
            return cron_refusal
        # Fence the actual lifecycle ingress before any preservation source starts its await.  An
        # already accepted stop/restart/idle operation keeps its exact ForeignLifecycleLease and
        # makes this reversible close refuse; an empty registered ingress is valid live no-work.
        from gateway.lifecycle_custody import begin_preservation_lifecycle_close
        from gateway.lifecycle_custody import abort_preservation_lifecycle_close as _abort_lifecycle_close
        lifecycle_refusal = begin_preservation_lifecycle_close(runner, receipt.close_generation)
        if lifecycle_refusal is not None:
            coordinator.abort_before_handoff(receipt)
            return lifecycle_refusal.as_wire()
        from gateway.profile_lifecycle_custody import begin_profile_lifecycle_close
        profile_lifecycle_active = getattr(runner, "_profile_lifecycle_custody", None) is not None
        if profile_lifecycle_active and not begin_profile_lifecycle_close(
            runner, receipt.close_generation,
        ):
            _abort_lifecycle_close(runner, receipt.close_generation)
            refused = coordinator.abort_before_handoff(receipt)
            return refused.as_wire() if hasattr(refused, "as_wire") else {
                "status": "refused", "reason": "profile_lifecycle_source_close_conflict",
            }
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
            completed, reason = await _abort_mcp_factories(
                runner, coordinator, receipt, mcp_entries,
            )
            if not completed:
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            if not _abort_lifecycle_close(runner, receipt.close_generation):
                return {'status': 'refused', 'reason': 'lifecycle_resume_conflict', 'receipt': wire}
            return {'status': 'refused', 'reason': mcp_failure}
        if not begin_i09_source_close(runner, receipt.close_generation):
            if not _resume_mcp_factories(runner, receipt.close_generation, mcp_entries):
                _reclose_mcp_factories(mcp_entries)
                return {'status': 'refused', 'reason': 'mcp_source_resume_pending', 'receipt': wire}
            completed, reason = await _abort_mcp_factories(
                runner, coordinator, receipt, mcp_entries,
            )
            if not completed:
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            if not _abort_lifecycle_close(runner, receipt.close_generation):
                return {'status': 'refused', 'reason': 'lifecycle_resume_conflict', 'receipt': wire}
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
            completed, reason = await _abort_mcp_factories(
                runner, coordinator, receipt, mcp_entries,
            )
            if not completed:
                _reclose_preservation_sources(
                    runner, receipt.close_generation, source_registry, mcp_entries,
                )
                return {'status': 'refused', 'reason': reason, 'receipt': wire}
            if not _abort_lifecycle_close(runner, receipt.close_generation):
                return {'status': 'refused', 'reason': 'lifecycle_resume_conflict', 'receipt': wire}
            return {'status': 'refused', 'reason': 'source_close_conflict'}
        # Preserve the existing reversible external-drain distinction: fresh roots refuse, while
        # already accepted FIFO continuations may claim and settle under their live parent lease.
        # Ordinary ``_draining`` remains reserved for shutdown/restart and must stay false here.
        runner._external_drain_active = True
        runner._preservation_drain_active = True
        descriptor.update(state='draining', capabilities=[], drain_reason='preservation')
    return wire


async def _retry_post_open_transition(runner, coordinator, receipt):
    """Retry the one coordinator-issued OPEN cleanup token and restore lifecycle admission."""
    from gateway.run_runtime_source_abort import retry_sources_post_open

    outcome = retry_sources_post_open(runner, coordinator, receipt)
    if not outcome.completed:
        return {
            'status': 'open',
            'pending_cleanup': {
                'source': 'preservation_sources',
                'close_generation': receipt.close_generation,
                'reason': outcome.reason or 'source_finalization_pending',
            },
        }
    from gateway.lifecycle_custody import abort_preservation_lifecycle_close
    if not abort_preservation_lifecycle_close(runner, receipt.close_generation):
        return {
            'status': 'open',
            'pending_cleanup': {
                'source': 'gateway_lifecycle',
                'close_generation': receipt.close_generation,
            },
        }
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    runner._external_drain_active = False
    runner._preservation_drain_active = False
    descriptor.update(
        state='ready',
        capabilities=['session-authority-v1', 'durable-admission-v1', 'event-replay-v1'],
    )
    descriptor.pop('drain_reason', None)
    return {'status': 'open', 'instance_id': descriptor.get('instance_id'),
            'authority_epoch': descriptor.get('authority_epoch')}


async def abort_preservation_drain_impl(runner, params, peer_subject):
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
    # A source may have observed OPEN before a late cleanup callback failed.  Retry only the
    # coordinator-issued capability; the old wire receipt cannot reopen or mint a new transition.
    pending_transition = getattr(coordinator, 'pending_post_open_transition_for', lambda *_: None)(receipt)
    if pending_transition is not None:
        return await _retry_post_open_transition(runner, coordinator, receipt)
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
    from gateway.session_goal_tick_custody import (
        has_i08_source_graph, i08_close_active, resume_i08_source_close,
    )
    i08_active = has_i08_source_graph(runner) and i08_close_active(
        runner, receipt.close_generation,
    )
    if i08_active:
        if remaining is None or not await resume_i08_source_close(
                runner, receipt.close_generation, remaining):
            _reclose_preservation_sources(
                runner, receipt.close_generation, source_registry, mcp_entries,
            )
            return {'status': 'refused', 'reason': 'i08_source_resume_conflict'}
    from gateway.run_runtime_source_abort import (
        finish_sources_atomically, prepare_source_participants,
    )
    prepared, prepare_reason = prepare_source_participants(
        runner, coordinator, receipt, include_i08=i08_active,
    )
    if not prepared:
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return {'status': 'refused', 'reason': prepare_reason or 'source_finalization_pending'}
    from gateway.run_runtime_source_abort import (
        refence_profile_sources, resume_profile_sources_before_open,
    )
    if not await resume_profile_sources_before_open(runner, coordinator, receipt):
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        return {'status': 'refused', 'reason': 'profile_lifecycle_source_resume_conflict'}
    outcome = finish_sources_atomically(runner, mcp_entries, coordinator, receipt)
    if not outcome.completed:
        if outcome.committed_open:
            return {
                'status': 'open',
                'pending_cleanup': {
                    'source': 'preservation_sources',
                    'close_generation': receipt.close_generation,
                    'reason': outcome.reason or 'source_finalization_pending',
                },
            }
        _reclose_preservation_sources(
            runner, receipt.close_generation, source_registry, mcp_entries,
        )
        await refence_profile_sources(runner, coordinator, receipt)
        return {'status': 'refused', 'reason': outcome.reason or 'source_finalization_pending'}
    from gateway.lifecycle_custody import abort_preservation_lifecycle_close
    if not abort_preservation_lifecycle_close(runner, receipt.close_generation):
        # Coordinator/source OPEN is already published; report a truthful pending cleanup rather
        # than returning a pre-open Refused result or attempting a second close/reclose.
        return {
            'status': 'open',
            'pending_cleanup': {
                'source': 'gateway_lifecycle',
                'close_generation': receipt.close_generation,
            },
        }
    descriptor = getattr(runner, 'session_runtime_descriptor', None) or {}
    runner._external_drain_active = False
    runner._preservation_drain_active = False
    descriptor.update(state='ready', capabilities=['session-authority-v1', 'durable-admission-v1', 'event-replay-v1'])
    descriptor.pop('drain_reason', None)
    return {'status': 'open', 'instance_id': descriptor.get('instance_id'),
            'authority_epoch': descriptor.get('authority_epoch')}


