"""Lease-backed operation helpers for the gateway session authority.

The mixin keeps the I03/I05/I06/I11 operation bookkeeping topical while the
authority retains the newer I01 scheduler and recovery seams.  It owns no
independent authority: every handle is issued and settled by the runner's
RuntimeCustodyCoordinator.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3

from hermes_state_runtime import RuntimeStoreError


_LOCAL_COMPRESSION_SOURCE = 'local-recovery-compression:'


_AUTOMATION_OWNER_CLASSES = frozenset({
    'I07_automation_route_wakes',
    'I08_goals_loops_heartbeat',
    'I09_a2a_hosted_kanban',
})


class PreservationOperationsMixin:
    def _preservation_coordinator(self):
        return getattr(self.runner, 'runtime_custody', None)

    def _require_preservation_coordinator(self):
        """Require custody for an authority-backed runtime, preserving bare legacy adapters."""
        coordinator = self._preservation_coordinator()
        if coordinator is not None:
            return coordinator
        runner = self.runner
        if (getattr(runner, 'session_authorities', None) is not None
                or getattr(runner, 'session_authority', None) is not None):
            raise RuntimeStoreError('runtime_custody_unavailable')
        return None

    def _preservation_active_cancel_control(self, admission_id):
        """Recognize the exact live I05 stop/cancel child during its own terminal write.

        ``cancel_queued`` settles the durable row before the authenticated control wrapper
        terminalizes its I05 handle.  The I02 callback scan therefore sees that real control
        child for a short interval.  It is a pending owner boundary, not an unmapped writer;
        validate the retained objects and row here so the release gate can defer without
        marking the I02 parent UNKNOWN.
        """
        if not isinstance(admission_id, str) or not admission_id:
            return False
        handles = getattr(self, '_preservation_handles', None)
        parent = handles.get(admission_id) if isinstance(handles, dict) else None
        if getattr(parent, 'owner_class', None) != 'I02_api_session_turn':
            return False
        coordinator = self._preservation_coordinator()
        records = getattr(coordinator, '_records', None) if coordinator is not None else None
        coordinator_id = getattr(coordinator, '_coordinator_id', None)
        process_instance = getattr(coordinator, 'process_instance', None)
        if (coordinator is None or not isinstance(records, dict)
                or getattr(parent, 'coordinator_id', None) != coordinator_id
                or getattr(parent, 'profile_id', None) != getattr(self, 'profile_id', None)
                or getattr(parent, 'authority_epoch', None) != getattr(self, 'epoch', None)
                or getattr(parent, 'process_instance', None) != process_instance):
            return False
        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(self.db, admission_id=admission_id)
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
                sqlite3.Error, TypeError, ValueError):
            return False
        if (not isinstance(row, dict) or row.get('admission_id') != admission_id
                or row.get('status') != 'terminal' or row.get('outcome') != 'cancelled'
                or row.get('owner_epoch') != self.epoch
                or row.get('target_session_id') is None):
            return False
        parent_lease_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
        matches = []
        for key, record in getattr(self, '_preservation_operations', {}).items():
            if (not isinstance(record, dict)
                    or record.get('owner_class') != 'I05_controls_and_unknown'
                    or record.get('operation') not in {'cancel', 'stop'}
                    or record.get('admission_id') != admission_id
                    or record.get('unknown') is True
                    or record.get('parent_handle') is not parent):
                continue
            handle = record.get('handle')
            lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
            live = records.get(lease_id) if isinstance(lease_id, str) else None
            if (live is None or getattr(live, 'handle', None) is not handle
                    or getattr(live, 'state', None) not in {'accepted', 'finalizing'}
                    or getattr(handle, 'owner_class', None) != 'I05_controls_and_unknown'
                    or getattr(handle, 'coordinator_id', None) != coordinator_id
                    or getattr(handle, 'profile_id', None) != getattr(self, 'profile_id', None)
                    or getattr(handle, 'authority_epoch', None) != getattr(self, 'epoch', None)
                    or getattr(handle, 'process_instance', None) != process_instance):
                continue
            if (record.get('principal_id') != row.get('principal_id')
                    or record.get('session_id') != row.get('target_session_id')
                    or record.get('admission_request_id') != row.get('request_id')):
                continue
            if not isinstance(parent_lease_id, str) or not parent_lease_id:
                continue
            matches.append(key)
        return len(matches) == 1

    def _preservation_release_terminal_gate(self, admission_id):
        """Return the exact completion gate and sidecars before mutating source ownership."""
        from gateway.session_classic_delivery import (
            CLASSIC_CALLBACK_NOT_REQUIRED,
            CLASSIC_CALLBACK_UNKNOWN,
            classic_callback_requirement,
            classic_post_delivery_callback_settled,
        )
        completion = None
        api_required = False
        try:
            from gateway.platforms.api_server_run_completion import (
                api_i02_completion_required, completion_for,
            )
            completion = completion_for(self, admission_id)
            api_required = completion is not None or api_i02_completion_required(self, admission_id)
        except (
            ImportError, AttributeError, KeyError, LookupError, OSError, RuntimeError,
            TypeError, ValueError,
        ):
            # A missing or partially failing API source has no canonical direct-observer
            # proof at this gate. Retain API custody as UNKNOWN rather than releasing it.
            completion = None
            api_required = True
        if api_required and completion is None:
            self._preservation_mark_unknown(
                admission_id, 'finalizer_completion_registry_missing',
            )
            return False, False, None, True, []
        if completion is not None and getattr(getattr(completion, 'receipt', None), 'state', None) == 'complete':
            return False, True, completion, api_required, []
        callback_requirement = classic_callback_requirement(self, admission_id)
        if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
            if self._preservation_active_cancel_control(admission_id):
                return False, False if api_required else None, completion, api_required, []
            if completion is not None:
                completion.fail('api_callback_requirement_unknown')
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            return False, False if api_required else None, completion, api_required, []
        if (callback_requirement != CLASSIC_CALLBACK_NOT_REQUIRED
                and not classic_post_delivery_callback_settled(self, admission_id)):
            return False, False if api_required else None, completion, api_required, []
        adopter = getattr(self, 'marker_adopters', {}).get(admission_id)
        marker = getattr(adopter, '_gateway_marker_released', None)
        if marker is not None and not marker.is_set():
            # Delivery may release the marker after this drain returns. Keep the exact lease live
            # and let the tracked marker watcher perform the same terminal transition later.
            self._preservation_watch_marker(admission_id, marker)
            return False, False if api_required else None, completion, api_required, []
        sidecars = [entry for entry in self._preservation_sidecars.values()
                    if entry['admission_id'] == admission_id]
        if sidecars and not all(entry['terminal'] for entry in sidecars):
            # A cached sidecar owns an exact request child. Its service finalizer calls back here
            # after the durable result/transport boundary, so the FIFO child cannot be released
            # while physical work is still in flight.
            return False, False if api_required else None, completion, api_required, sidecars
        if completion is not None and not getattr(completion, '_delivery_staged', False):
            # A control may finish before the API observer has consumed its terminal result.
            # Keep the exact completion pending; only the completion owner may classify a
            # missing delivery/readback as UNKNOWN.
            return False, False, completion, api_required, sidecars
        if completion is not None and not completion.prepare_parent_release():
            completion.fail('api_parent_release_receipt_missing')
            self._preservation_mark_unknown(admission_id, 'api_parent_release_receipt_missing')
            return False, False, completion, api_required, sidecars
        return True, None, completion, api_required, sidecars

    def _preservation_recover_i02_unknown(self, admission_id, parent):
        """Reopen the original unknown I02 parent before its normal release boundary runs."""
        from gateway.runtime_custody import CustodyRefusal

        coordinator = self._preservation_coordinator()
        handles = getattr(self, '_preservation_handles', None)
        coordinator_id = getattr(coordinator, '_coordinator_id', None)
        parent_lease = getattr(parent, 'lease', None)
        parent_lease_id = getattr(parent_lease, 'lease_id', None)
        if (
            coordinator is None
            or not isinstance(handles, dict)
            or handles.get(admission_id) is not parent
            or getattr(parent, 'owner_class', None) != 'I02_api_session_turn'
            or parent_lease is None
            or not isinstance(parent_lease_id, str)
            or not parent_lease_id
            or getattr(parent, 'coordinator_id', None) != coordinator_id
            or getattr(parent, 'profile_id', None) != getattr(self, 'profile_id', None)
            or getattr(parent, 'authority_epoch', None) != getattr(self, 'epoch', None)
            or getattr(parent, 'process_instance', None)
            != getattr(coordinator, 'process_instance', None)
        ):
            return CustodyRefusal('api_parent_release_identity_mismatch', 'I02_api_session_turn')

        # ``_preservation_mark_unknown`` marks the retained I04 child before the I02
        # parent.  Reopening only the parent leaves the original child sticky-unknown,
        # so the normal child-before-parent terminal boundary still refuses with
        # ``custody_unknown``.  Authenticate the exact mapped child and its existing
        # coordinator edge before changing either record; never construct a replacement
        # handle or infer a child from the admission id.
        children = getattr(self, '_preservation_children', {})
        if not isinstance(children, dict):
            return CustodyRefusal('api_child_release_identity_mismatch', 'I04_fifo_turn_execution')
        child = children.get(admission_id)
        if child is not None:
            if (
                not hasattr(child, 'lease')
                or getattr(child, 'owner_class', None) != 'I04_fifo_turn_execution'
                or getattr(child, 'coordinator_id', None) != coordinator_id
                or getattr(child, 'profile_id', None) != getattr(parent, 'profile_id', None)
                or getattr(child, 'authority_epoch', None) != getattr(parent, 'authority_epoch', None)
                or getattr(child, 'process_instance', None)
                != getattr(parent, 'process_instance', None)
                or not isinstance(getattr(getattr(child, 'lease', None), 'lease_id', None), str)
                or not getattr(child.lease, 'lease_id', None)
                or getattr(child.lease, 'parent_lease_id', None) != parent_lease_id
            ):
                return CustodyRefusal('api_child_release_identity_mismatch', 'I04_fifo_turn_execution')
            refusal = coordinator.reconcile_unknown(
                child,
                owner_id=child.owner_id,
                profile_id=child.profile_id,
                process_instance=child.process_instance,
                authority_epoch=child.authority_epoch,
            )
            if refusal is not None:
                return refusal
        refusal = coordinator.reconcile_unknown(
            parent,
            owner_id=parent.owner_id,
            profile_id=parent.profile_id,
            process_instance=parent.process_instance,
            authority_epoch=parent.authority_epoch,
        )
        if refusal is not None and child is not None:
            # Parent reconciliation is the second authenticated state transition.  If
            # it refuses, restore the exact child to the same fail-closed state; a
            # partial child reopen must never make the parent appear recoverable.
            coordinator.mark_unknown(child, 'custody_unknown')
        return refusal

    def _preservation_i02_release_receipt(self, admission_id, parent, coordinator, release_receipt):
        if release_receipt is None:
            registry = getattr(self, '_api_finalizer_completions', None)
            completion = registry.get(admission_id) if isinstance(registry, dict) else None
            release_receipt = getattr(completion, '_parent_release_receipt', None)
        if not isinstance(release_receipt, dict):
            return None
        expected = {
            'admission_id': admission_id,
            'parent_lease_id': getattr(getattr(parent, 'lease', None), 'lease_id', None),
            'coordinator_id': getattr(coordinator, '_coordinator_id', None),
            'parent_owner_id': getattr(parent, 'owner_id', None),
            'profile_id': getattr(parent, 'profile_id', None),
            'authority_epoch': getattr(parent, 'authority_epoch', None),
            'process_instance': getattr(parent, 'process_instance', None),
            'close_generation': getattr(parent, 'close_generation', None),
            'released': True,
        }
        if set(release_receipt) != {*expected, 'process_receipt'}:
            return None
        if any(release_receipt.get(key) != value for key, value in expected.items()):
            return None
        process_receipt = release_receipt.get('process_receipt')
        if (
            not isinstance(process_receipt, dict)
            or set(process_receipt) != {
                'kind', 'process_instance', 'authority_epoch', 'close_generation',
                'owner_id', 'task_id', 'observed', 'cross_process_proof',
            }
            or process_receipt.get('kind') != 'parent_handle'
            or process_receipt.get('owner_id') != expected['parent_owner_id']
            or process_receipt.get('task_id') is not None
            or process_receipt.get('process_instance') != expected['process_instance']
            or process_receipt.get('authority_epoch') != expected['authority_epoch']
            or process_receipt.get('close_generation') != expected['close_generation']
            or process_receipt.get('observed') is not True
            or process_receipt.get('cross_process_proof') is not False
        ):
            return None
        return release_receipt

    def _preservation_i02_parent_released(self, admission_id, parent, release_receipt=None):
        """Confirm a terminal marker for this exact parent under the coordinator lock."""
        coordinator = self._preservation_coordinator()
        handles = getattr(self, '_preservation_handles', None)
        if (
            coordinator is None
            or not isinstance(handles, dict)
            or getattr(parent, 'owner_class', None) != 'I02_api_session_turn'
            or getattr(parent, 'coordinator_id', None)
            != getattr(coordinator, '_coordinator_id', None)
            or getattr(parent, 'profile_id', None) != getattr(self, 'profile_id', None)
            or getattr(parent, 'authority_epoch', None) != getattr(self, 'epoch', None)
            or getattr(parent, 'process_instance', None)
            != getattr(coordinator, 'process_instance', None)
        ):
            return False
        lease_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
        if not isinstance(lease_id, str) or not lease_id:
            return False
        mapped = handles.get(admission_id)
        registry = getattr(self, '_api_finalizer_completions', None)
        completion = registry.get(admission_id) if isinstance(registry, dict) else None
        if (
            getattr(completion, 'admission_id', None) != admission_id
            or getattr(completion, 'parent', None) is not parent
        ):
            return False
        if mapped is not parent and mapped is not None:
            return False
        lock = getattr(coordinator, '_lock', None)
        records = getattr(coordinator, '_records', None)
        completed = getattr(coordinator, '_completed', None)
        if lock is None or not isinstance(records, dict) or not isinstance(completed, dict):
            return False
        release_receipt = self._preservation_i02_release_receipt(
            admission_id, parent, coordinator, release_receipt,
        )
        if release_receipt is None or release_receipt.get('parent_lease_id') != lease_id:
            return False
        with lock:
            parent_readback = getattr(coordinator, '_parent', None)
            return (
                callable(parent_readback)
                and parent_readback(parent) is None
                and lease_id not in records
                and completed.get(lease_id) == 'terminal'
            )

    def _preservation_commit_i02_parent_release(self, admission_id, parent, release_receipt=None):
        """Drop the staged authority reference only after the coordinator terminal marker is read."""
        if not self._preservation_i02_parent_released(admission_id, parent, release_receipt):
            return False
        handles = getattr(self, '_preservation_handles', None)
        if not isinstance(handles, dict):
            return False
        mapped = handles.get(admission_id)
        if mapped is parent:
            handles.pop(admission_id, None)
            self._preservation_automation_bindings.pop(admission_id, None)
            return True
        return mapped is None

    def _preservation_prepare_i10_release(self, admission_id, terminal_receipt):
        bindings = getattr(self, '_i10_recovery_bindings', {})
        if admission_id not in bindings:
            return None
        from gateway.session_recovery_custody import prepare_i10_terminal_release
        proof = prepare_i10_terminal_release(self, admission_id, terminal_receipt)
        return proof if proof is not None else False

    def _preservation_commit_i10_release(self, proof):
        if proof is None:
            return True
        from gateway.session_recovery_custody import (
            commit_i10_terminal_release, validate_i10_terminal_release,
        )
        if not validate_i10_terminal_release(self, proof):
            return False
        return commit_i10_terminal_release(self, proof)

    def _preservation_terminalize_child(self, admission_id, coordinator, parent):
        child = self._preservation_children.get(admission_id)
        if child is None or not hasattr(child, 'lease'):
            return True
        result = coordinator.mark_terminal(child)
        if result is not None:
            # An accepted I06 follow-up may still be bound to this I04 after its
            # durable row completes.  Its own finalizer will retry the parent.
            if parent is not None and result.code != 'custody_pending':
                coordinator.mark_unknown(parent, 'lease_release_failed')
            return False
        self._preservation_children.pop(admission_id, None)
        return True

    def _preservation_release_terminal(self, admission_id, *, terminal_receipt=None):
        """Release child then root only after typed delivery and API-child evidence are visible."""
        can_release, early_result, completion, api_required, sidecars = (
            self._preservation_release_terminal_gate(admission_id)
        )
        if not can_release:
            return early_result
        i10_proof = self._preservation_prepare_i10_release(admission_id, terminal_receipt)
        if i10_proof is False:
            return False
        for entry in sidecars:
            request_id = getattr(getattr(entry['request'], 'lease', None), 'lease_id', None)
            self._preservation_sidecars.pop(request_id, None)
        self._preservation_delivery_retries.pop(admission_id, None)
        coordinator = self._preservation_coordinator()
        # /new cancellation is a durable route move, not a terminal producer result. Keep the
        # exact I07 root and rebind it to the new queued row before any successor can claim it.
        if admission_id in self._preservation_automation_rehomes:
            self._preservation_finish_automation_rehome(admission_id)
            return False if api_required else None
        if coordinator is None:
            if (admission_id in self._preservation_handles
                    or admission_id in self._preservation_children
                    or any(record.get('admission_id') == admission_id
                           for record in self._preservation_operations.values())):
                self._require_preservation_coordinator()
            if completion is not None:
                completion.fail('api_parent_release_unavailable')
            return False if api_required else None
        parent = self._preservation_handles.get(admission_id)
        if completion is not None and parent is None:
            completion.fail('api_parent_release_missing')
            self._preservation_mark_unknown(admission_id, 'api_parent_release_missing')
            return False
        if not self._preservation_terminalize_child(admission_id, coordinator, parent):
            if completion is not None:
                completion.fail('api_child_release_failed')
            return False
        if parent is not None:
            if completion is not None:
                # The marker cleanup is part of the same authenticated parent boundary. Perform it
                # before the irreversible coordinator operation so a cleanup exception cannot leave
                # a missing parent receipt after mark_terminal has deleted the live row.
                try:
                    from gateway.session_classic_delivery import clear_classic_marker_adopter
                    clear_classic_marker_adopter(self, admission_id)
                except BaseException:  # health: allow BLE001 -- opaque marker cleanup retains custody
                    completion.fail('api_parent_cleanup_failed')
                    self._preservation_mark_unknown(admission_id, 'api_parent_cleanup_failed')
                    return False
            result = coordinator.mark_terminal(parent)
            if result is not None:
                if completion is not None:
                    completion.fail('api_parent_release_failed')
                return False
            if completion is None:
                self._preservation_handles.pop(admission_id, None)
                self._preservation_automation_bindings.pop(admission_id, None)
        elif completion is not None:
            completion.fail('api_parent_release_missing')
            self._preservation_mark_unknown(admission_id, 'api_parent_release_missing')
            return False
        if completion is None:
            from gateway.session_classic_delivery import clear_classic_marker_adopter
            clear_classic_marker_adopter(self, admission_id)
        if not self._preservation_commit_i10_release(i10_proof):
            return False
        return True if api_required else None

    async def _preservation_finish_settled_delivery(self, admission_id, delivery_receipt, response):
        """Stage an I02 finalizer before its waiter, then let the bound I03 child settle."""
        from gateway.platforms.api_server_run_completion import (
            api_i02_completion_required, completion_for,
        )
        from gateway.session_ingress import finish_preservation_delivery
        completion = completion_for(self, admission_id)
        api_required = completion is not None or api_i02_completion_required(self, admission_id)
        if not api_required:
            waiter_delivered = await self._resolve_settled_waiter(admission_id, response)
            return await finish_preservation_delivery(
                self, admission_id, delivery_receipt, waiter_delivered,
            )
        failure = None
        try:
            staged = await finish_preservation_delivery(
                self, admission_id, delivery_receipt, False,
            )
        except BaseException as exc:  # health: allow BLE001 -- delivery failure is retained and re-raised
            staged = False
            failure = exc
        waiter_delivered = await self._resolve_settled_waiter(admission_id, response)
        if completion is not None:
            completion.note_waiter_delivery(waiter_delivered)
            # The observer waiter is now released, so run_legacy_authority_turn can
            # terminalize the bound I03 child.  Keep this FIFO drain at the same
            # admission until that typed completion settles; this is a sibling
            # barrier, not an API waiter/finalizer cycle.
            try:
                completion_receipt = await completion.wait()
            except asyncio.CancelledError:
                completion.fail("api_drain_cancelled")
                raise
            if getattr(completion_receipt, "state", None) != "complete":
                staged = False
        if failure is not None:
            raise failure
        return staged

    def _preservation_watch_fifo_close(self, admission_id):
        """Re-enter the existing release boundary when this admission's FIFO drain ends.

        A Bot projection may finish before the authenticated I04 child has closed.  The child
        drain task is the existing source-owned completion boundary; its done callback is only a
        trigger.  ``_preservation_release_terminal`` still re-reads the canonical row, callback,
        marker, sidecar, handle, and coordinator identity before releasing anything.
        """
        bindings = getattr(self, '_preservation_automation_bindings', None)
        binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
        if not isinstance(binding, dict):
            return False
        descriptor = binding.get('descriptor')
        if not isinstance(descriptor, dict):
            return False
        row = binding.get('row')
        if row is not None and (
                not isinstance(row, dict) or row.get('admission_id') != admission_id):
            return False
        handles = getattr(self, '_preservation_handles', None)
        children = getattr(self, '_preservation_children', None)
        handle = binding.get('handle')
        child = children.get(admission_id) if isinstance(children, dict) else None
        if (not isinstance(handles, dict) or handles.get(admission_id) is not handle
                or child is None or not hasattr(child, 'lease')):
            return False
        session_id = row.get('target_session_id') if isinstance(row, dict) else None
        if not isinstance(session_id, str) or not session_id:
            session_id = descriptor.get('session_id')
        sessions = getattr(self, 'sessions', None)
        live = sessions.get(session_id) if isinstance(sessions, dict) else None
        task = getattr(live, 'task', None)
        if task is None or not callable(getattr(task, 'add_done_callback', None)):
            return False
        if binding.get('fifo_close_task') is not None:
            return True
        binding['fifo_close_task'] = task

        def retry(_done):
            try:
                self._preservation_release_terminal(admission_id)
            except BaseException:  # health: allow BLE001 -- failed re-entry retains custody unknown
                self._preservation_mark_unknown(admission_id, 'join_failed')

        task.add_done_callback(retry)
        return True

    def _preservation_root(self, owner_class, owner_id):
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return None
        from gateway.runtime_custody import CustodyRefusal
        handle = coordinator.reserve_root(
            owner_class, owner_id, profile_id=self.profile_id, authority_epoch=self.epoch)
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        return handle

    def _preservation_operation_parent(
        self, session_id, admission_id=None, *, generation=None, principal_id=None, queued_control=False,
        shared_group_principal=False,
    ):
        """Return the exact active parent lease for a real operation.

        An explicit admission id is authoritative.  A late callback may not fall back to the
        live execution entry when that entry names a different claim, and an implicit lookup is
        accepted only after the durable row agrees with the session, principal, and generation.
        """
        live = self.sessions.get(session_id)
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        execution_admission = execution.get('admission_id')
        other_execution = admission_id is not None and execution_admission not in (None, admission_id)
        if other_execution and not queued_control:
            return None
        candidates = (admission_id,) if admission_id is not None else (execution_admission,)
        from hermes_state_runtime import get_session_admission
        for current in candidates:
            if not current:
                continue
            row = get_session_admission(self.db, admission_id=current)
            if row is None or row.get('target_session_id') != session_id:
                continue
            if other_execution and row.get('status') != 'queued':
                continue
            if generation is not None and row.get('generation') != generation:
                continue
            if (principal_id is not None and row.get('principal_id') != principal_id
                    and not shared_group_principal):
                continue
            if (execution_admission is not None
                    and execution_admission == current
                    and generation is not None
                    and execution.get('execution_generation') not in (None, generation)):
                continue
            for handles in (self._preservation_children, self._preservation_handles):
                handle = handles.get(current)
                if (handle is not None and hasattr(handle, 'lease')
                        and handle.profile_id == self.profile_id
                        and handle.authority_epoch == self.epoch):
                    return handle
        return None
    @staticmethod
    def _preservation_operation_identity(owner_class, *, profile_id, authority_epoch, session_id, principal_id,
                                         operation, request_id=None, admission_id=None,
                                         generation=None, source_identity=None):
        """Build a complete owner identity before a control/task/callback is created."""
        values = {
            'class': owner_class,
            'principal': principal_id,
            'profile': profile_id,
            'session': session_id,
            'epoch': authority_epoch,
            'generation': 'pre-admission' if generation is None else generation,
            'operation': operation,
            'request': request_id or 'none',
            'admission': admission_id or 'none',
            'source': source_identity or 'owner',
        }
        return '|'.join(f'{key}={str(value).replace("|", "%7C")}' for key, value in values.items())
    @staticmethod
    def _preservation_operation_generation_state(generation):
        if generation is None or generation == 'pre-admission':
            return 'unclaimed'
        if type(generation) is int and generation == 0:
            return 'unclaimed'
        if type(generation) is not int or generation < 1:
            raise RuntimeStoreError('stale_generation')
        return 'pinned'

    def _preservation_operation_admission_request_id(
        self, owner_class, principal_id, session_id, admission_id, generation, request_id
    ):
        # A prompt/control request and the turn that owns it have separate request namespaces.
        # Capture the parent row's identity before reserving; keep the operation request immutable.
        if owner_class != 'I05_controls_and_unknown' or admission_id is None:
            return request_id
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id)
        if (row is None or row.get('owner_epoch') != self.epoch
                or row.get('principal_id') != principal_id
                or row.get('target_session_id') != session_id
                or (generation is not None and row.get('generation') != generation)
                or not isinstance(row.get('request_id'), str) or not row['request_id']):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return row['request_id']

    def _preservation_operation_row_identity_matches(self, record, row, admission_id):
        if not isinstance(row, dict):
            return False
        expected = {
            'admission_id': admission_id,
            'owner_epoch': record.get('authority_epoch', self.epoch),
            'principal_id': record.get('principal_id'),
            'target_session_id': record.get('session_id'),
        }
        if record.get('admission_id') is not None:
            expected['admission_id'] = record['admission_id']
        request_id = record.get('admission_request_id', record.get('request_id'))
        if request_id is not None:
            expected['request_id'] = request_id
        return all(row.get(name) == value for name, value in expected.items())

    @staticmethod
    def _preservation_operation_row_generation_info(record, row, admission_id):
        if row.get('status') not in {'queued', 'started', 'steering', 'unknown', 'terminal'}:
            return None
        binding = (record.get('admission_bindings') or {}).get(admission_id)
        state = ((binding or {}).get('generation_state')
                 or record.get('generation_state')
                 or PreservationOperationsMixin._preservation_operation_generation_state(
                     record.get('generation')))
        if state not in {'unclaimed', 'pinned'}:
            return None
        row_generation = row.get('generation')
        expected_generation = ((binding or {}).get('generation')
                               if binding is not None else record.get('generation'))
        if state == 'pinned':
            valid = (type(expected_generation) is int and expected_generation >= 1
                     and row_generation == expected_generation)
        elif row_generation is None:
            valid = row.get('status') in {'queued', 'terminal'}
        else:
            valid = type(row_generation) is int and row_generation >= 1
        return (state, row_generation, binding) if valid else None

    def _preservation_operation_live_row_matches(
            self, record, admission_id, row_generation, row_status):
        if (row_generation is None and record.get('owner_class') == 'I05_controls_and_unknown'
                and record.get('operation') in {'cancel', 'stop'}):
            # The identity-checked queued target has no execution. Its cancellation must not
            # borrow, interrupt, or be vetoed by another admission's live generation.
            return True
        if (row_status == 'queued' and row_generation is None
                and record.get('owner_class') == 'I06_busy_followup_delivery'
                and record.get('parent_handle') is None
                and record.get('parent_generation') is None
                and getattr(record.get('handle'), 'lease', None) is not None
                and getattr(getattr(record.get('handle'), 'lease', None), 'parent_lease_id', None) is None
                and record.get('operation') in {'running-slot', 'fifo', 'debounce'}
                and record.get('unknown') is False):
            # A direct classic occurrence reserved its own root before the write.
            # A different FIFO claim may begin while its native media capture is
            # still committing; that claim cannot veto this exact queued row.
            return True
        live = self.sessions.get(record.get('session_id'))
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        expected = {
            'admission_id': (None, admission_id),
            'authority_epoch': (None, record.get('authority_epoch', self.epoch)),
        }
        if row_generation is not None:
            expected['execution_generation'] = (None, row_generation)
        return not execution or all(execution.get(name) in allowed
                                    for name, allowed in expected.items())

    def _preservation_operation_expected_parent_valid(self, record, expected_parent):
        """Validate the exact retained opaque I04/I07 parent for a row readback."""
        if not isinstance(record, dict) or record.get('parent_handle') is not expected_parent:
            return False
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return False
        try:
            from gateway.runtime_custody import CustodyHandle
            if not isinstance(expected_parent, CustodyHandle):
                return False
            return self._preservation_operation_validate_parent_handle(
                expected_parent, coordinator, self.profile_id, self.epoch, self.instance_id,
            ) is expected_parent
        except (AttributeError, RuntimeStoreError, TypeError, ValueError):
            return False

    def _preservation_operation_parented_native_row_live_matches(
        self, record, row, admission_id, row_generation, expected_parent,
    ):
        from gateway.session_authority_parented_native import parented_native_row_live_matches
        return parented_native_row_live_matches(
            self, record, row, admission_id, row_generation, expected_parent,
        )

    @staticmethod
    def _preservation_operation_pin_row(record, state, row_generation, binding):
        # This helper is called only after all identity, generation, and live checks pass.
        if state == 'unclaimed' and row_generation is not None:
            if binding is not None:
                binding['generation'] = row_generation
                binding['generation_state'] = 'pinned'
            record['generation'] = row_generation
            record['generation_state'] = 'pinned'
        record['admission_identity'] = {
            'profile_id': record.get('profile_id'),
            'authority_epoch': record.get('authority_epoch'),
            'principal_id': record.get('principal_id'),
            'target_session_id': record.get('session_id'),
            'request_id': record.get('admission_request_id', record.get('request_id')),
            'operation_request_id': record.get('request_id'),
            'requested_admission_id': record.get('requested_admission_id'),
            'generation_state': record.get('generation_state', state),
            'generation': record.get('generation'),
        }

    def _preservation_operation_validate_row(
            self, record, row, admission_id, *, expected_parent=None,
    ):
        """Validate one exact durable row and pin its committed generation once."""
        if not self._preservation_operation_row_identity_matches(record, row, admission_id):
            return None
        generation_info = self._preservation_operation_row_generation_info(
            record, row, admission_id)
        if generation_info is None:
            return None
        state, row_generation, binding = generation_info
        queued_parented = (
            isinstance(record, dict)
            and record.get('parent_handle') is not None
            and isinstance(row, dict)
            and row.get('status') == 'queued'
            and row_generation is None
        )
        queued_control = (
            queued_parented
            and record.get('owner_class') == 'I05_controls_and_unknown'
            and record.get('operation') in {'cancel', 'stop'}
        )
        if queued_parented:
            bot_parented = (
                isinstance(record, dict)
                and record.get('owner_class') == 'I06_busy_followup_delivery'
                and record.get('operation') == 'bot-delivery'
                and getattr(expected_parent, 'owner_class', None)
                    == 'I07_automation_route_wakes'
            )
            if queued_control:
                parent = record.get('parent_handle')
                coordinator = self._preservation_coordinator()
                parent_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
                records = getattr(coordinator, '_records', None) if coordinator is not None else None
                parent_record = records.get(parent_id) if isinstance(records, dict) else None
                if (
                    coordinator is None
                    or parent_record is None
                    or getattr(parent_record, 'handle', None) is not parent
                    or getattr(parent, 'owner_class', None) != 'I02_api_session_turn'
                    or getattr(parent, 'profile_id', None) != self.profile_id
                    or getattr(parent, 'authority_epoch', None) != self.epoch
                    or getattr(parent, 'process_instance', None) != self.instance_id
                    or getattr(parent, 'coordinator_id', None) != getattr(coordinator, '_coordinator_id', None)
                    or getattr(parent_record, 'state', None) not in {'accepted', 'finalizing'}
                ):
                    return None
            elif bot_parented:
                if (expected_parent is None
                        or not self._preservation_automation_row_matches(row, expected_parent)):
                    return None
            elif (expected_parent is None
                  or not self._preservation_operation_parented_native_row_live_matches(
                      record, row, admission_id, row_generation, expected_parent)):
                return None
        elif not self._preservation_operation_live_row_matches(
                record, admission_id, row_generation, row.get('status')):
            return None
        self._preservation_operation_pin_row(record, state, row_generation, binding)
        return row

    def _preservation_operation_bind_admission(
            self, key, admission_id, *, expected_parent=None,
    ):
        """Bind a real native receipt to the operation that reserved before admission."""
        if key is None or not isinstance(admission_id, str) or not admission_id:
            return False
        record = self._preservation_operations.get(key)
        if record is None:
            return False
        current = record.get('admission_id')
        if current is not None and current != admission_id:
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        from hermes_state_runtime import get_session_admission
        try:
            row = get_session_admission(self.db, admission_id=admission_id)
        except (OSError, RuntimeError, sqlite3.Error, KeyError, TypeError, ValueError):
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        if self._preservation_operation_validate_row(
                record, row, admission_id, expected_parent=expected_parent) is None:
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        record['admission_id'] = admission_id
        admission_ids = record.setdefault('admission_ids', [])
        if admission_id not in admission_ids:
            admission_ids.append(admission_id)
        return True






    def _preservation_operation_bind_row(self, key, row):
        """Bind only the exact started row and pin its owner epoch/generation."""
        record = self._preservation_operations.get(key)
        if record is None or not isinstance(row, dict):
            return False
        admission_id = row.get('admission_id')
        if (not isinstance(admission_id, str) or not admission_id
                or row.get('status') != 'started'
                or self._preservation_operation_validate_row(record, row, admission_id) is None):
            self._preservation_operation_unknown(key, 'unmapped_writer')
            return False
        current = record.get('admission_id')
        if current is not None and current != admission_id:
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        if not self._preservation_operation_bind_admission(key, admission_id):
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        record.setdefault('admission_bindings', {})[admission_id] = {
            'owner_epoch': row.get('owner_epoch'),
            'principal_id': row.get('principal_id'),
            'target_session_id': row.get('target_session_id'),
            'request_id': row.get('request_id'),
            'generation': row.get('generation'),
            'generation_state': 'pinned',
        }
        return True
    def _preservation_operation_row(self, key, *, expected_parent=None):
        """Read the exact admission identity, never a session-wide status census."""
        record = self._preservation_operations.get(key)
        retired = False
        if record is None:
            matches = []
            for adopter in (getattr(self, 'marker_adopters', None) or {}).values():
                owner = getattr(adopter, '_classic_execution_owner_event', None) or adopter
                bridge = getattr(owner, '_classic_execution_bridge', None)
                retired_record = getattr(bridge, 'retired_operation_record', None)
                if (isinstance(retired_record, dict)
                        and getattr(bridge, 'operation_key', None) == key
                        and getattr(bridge, 'admission_id', None) == retired_record.get('admission_id')
                        and retired_record.get('owner_class') == 'I06_busy_followup_delivery'
                        and retired_record.get('producer_object') is owner
                        and getattr(owner, '_classic_custody_authority', None) is self):
                    matches.append(retired_record)
            if len(matches) != 1:
                return None
            record = matches[0]
            retired = True
        if expected_parent is not None and not self._preservation_operation_expected_parent_valid(
                record, expected_parent):
            self._preservation_operation_unknown(key, 'read_failed')
            return None
        from hermes_state_runtime import find_session_admission, get_session_admission
        admission_id = record.get('admission_id')
        valid_targets = {record['session_id']}
        current = self._current_owner(record['session_id'])
        if current not in valid_targets:
            valid_targets.add(current)
        if admission_id:
            row = get_session_admission(self.db, admission_id=admission_id)
        else:
            row = next(
                (candidate for candidate in (
                    find_session_admission(
                        self.db, principal_id=record['principal_id'], session_id=session_id,
                        request_id=record['request_id'])
                    for session_id in valid_targets)
                 if candidate is not None),
                None,
            )
        if row is None:
            return None
        if (row.get('principal_id') != record['principal_id']
                or row.get('target_session_id') not in valid_targets
                or (record.get('admission_request_id', record.get('request_id')) is not None
                    and row.get('request_id') != record.get('admission_request_id', record.get('request_id')))
                or self._preservation_operation_validate_row(
                    record, row, row.get('admission_id'),
                    expected_parent=expected_parent) is None):
            self._preservation_operation_unknown(key, 'read_failed')
            return None
        if not retired and not self._preservation_operation_bind_admission(
                key, row.get('admission_id'), expected_parent=expected_parent):
            self._preservation_operation_unknown(key, 'read_failed')
            return None
        return row




    def _preservation_prompt_operation_start(self, *, session_id, generation, operation, request_id):
        """Reserve a prompt lifecycle under the exact started admission before external delivery."""
        if type(generation) is not int:
            raise RuntimeStoreError('stale_generation')
        live = self.sessions.get(session_id)
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        admission_id = execution.get('admission_id')
        if not isinstance(admission_id, str) or not admission_id:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if execution.get('authority_epoch') != self.epoch:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if execution.get('execution_generation') != generation:
            raise RuntimeStoreError('stale_generation')
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id)
        if (row is None or row.get('status') != 'started'
                or row.get('target_session_id') != session_id
                or row.get('generation') != generation
                or not isinstance(row.get('principal_id'), str)
                or not row.get('principal_id')):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return self._preservation_operation_start(
            'I05_controls_and_unknown', principal_id=row['principal_id'], session_id=session_id,
            operation=operation, request_id=request_id, admission_id=admission_id,
            generation=generation, source_identity='prompt_producer')

    def _preservation_control_identity(self, session_id, generation):
        """Return the source-owned identity for one currently running control request."""
        if type(generation) is not int:
            raise RuntimeStoreError('stale_generation')
        # A completed or superseded execution has no live parent to map. Preserve its public
        # generation refusal before requiring custody for a control that could be accepted.
        self.check_approval_generation(session_id, generation)
        live = self.sessions.get(session_id)
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        admission_id = execution.get('admission_id')
        if execution.get('authority_epoch') != self.epoch:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if execution.get('execution_generation') != generation:
            raise RuntimeStoreError('stale_generation')
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id) if admission_id else None
        if (row is None or row.get('status') != 'started'
                or row.get('target_session_id') != session_id
                or row.get('generation') != generation
                or not isinstance(row.get('principal_id'), str)
                or not row.get('principal_id')):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return row

    def _preservation_operation_current_admission(self, record):
        """Re-read the exact started admission before reopening an unknown operation."""
        admission_id = record.get('admission_id')
        if not isinstance(admission_id, str) or not admission_id:
            return None
        live = self.sessions.get(record.get('session_id'))
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        if (execution.get('admission_id') != admission_id
                or execution.get('authority_epoch') != self.epoch
                or execution.get('execution_generation') != record.get('generation')):
            return None
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id)
        if (row is None or row.get('status') != 'started'
                or row.get('owner_epoch') != record.get('authority_epoch', self.epoch)
                or row.get('target_session_id') != record.get('session_id')
                or row.get('generation') != record.get('generation')
                or row.get('principal_id') != record.get('principal_id')
                or self._preservation_operation_validate_row(
                    record, row, admission_id) is None):
            return None
        return row

    @staticmethod
    def _preservation_operation_generation_call_matches(record, generation):
        """Accept the original request generation, or a committed generation with exact row proof."""
        requested = record.get('requested_generation', record.get('generation'))
        state = record.get('generation_state')
        if state is None:
            state = ('unclaimed' if requested is None or requested == 'pre-admission'
                     or (type(requested) is int and requested == 0) else 'pinned')
        if state == 'unclaimed':
            return generation == requested
        if state != 'pinned':
            return False
        committed = record.get('generation')
        if type(committed) is not int or committed < 1:
            return False
        # The immutable request tuple remains authoritative. A committed integer is accepted only
        # as a same-row retry; _preservation_operation_current_admission revalidates that row.
        return generation == requested or generation == committed

    @staticmethod
    def _preservation_operation_admission_call_matches(record, admission_id):
        """Accept the original admission argument or its exact committed binding."""
        requested = record.get('requested_admission_id', record.get('admission_id'))
        committed = record.get('admission_id')
        if requested is None:
            return admission_id is None or admission_id == committed
        return admission_id == requested and committed in (None, requested)

    def _preservation_operation_replay_key(
        self, *, owner_class, principal_id, session_id, operation, request_id,
        admission_id, generation, source_identity
    ):
        """Find one pinned same-id record for an authenticated committed-generation retry."""
        candidates = []
        found_tuple = False
        for candidate_key, record in self._preservation_operations.items():
            expected = {
                'owner_class': owner_class, 'principal_id': principal_id,
                'session_id': session_id, 'operation': operation,
                'request_id': request_id, 'source_identity': source_identity,
            }
            if any(record.get(name) != value for name, value in expected.items()):
                continue
            found_tuple = True
            if not self._preservation_operation_admission_call_matches(record, admission_id):
                continue
            record_generation = record.get('generation')
            record_state = record.get('generation_state')
            if record_state is None:
                requested = record.get('requested_generation', record_generation)
                record_state = ('unclaimed' if requested is None or requested == 'pre-admission'
                                or (type(requested) is int and requested == 0) else 'pinned')
            if (record_state != 'pinned'
                    or generation != record_generation
                    or self._preservation_operation_current_admission(record) is None):
                continue
            candidates.append(candidate_key)
        if len(candidates) > 1 or (found_tuple and not candidates):
            # A stable request tuple already owns the decision. A mismatched or stale generation
            # must refuse before a new key/handle can be minted.
            raise RuntimeStoreError('preservation_owner_unmapped')
        return candidates[0] if candidates else None

    def _preservation_operation_retry(self, key, record, *, owner_class, principal_id,
                                      session_id, operation, request_id, admission_id,
                                      generation, source_identity):
        """Reopen only an exact unknown operation; never create a successor lease for it."""
        expected = {
            'owner_class': owner_class, 'principal_id': principal_id, 'session_id': session_id,
            'operation': operation, 'request_id': request_id,
            'source_identity': source_identity,
        }
        if (any(record.get(name) != value for name, value in expected.items())
                or not self._preservation_operation_admission_call_matches(record, admission_id)
                or not self._preservation_operation_generation_call_matches(record, generation)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if self._preservation_operation_current_admission(record) is None:
            raise RuntimeStoreError('stale_generation')
        handle = record['handle']
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            self._require_preservation_coordinator()
            return key
        refusal = coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
            process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
        if refusal is not None:
            raise RuntimeStoreError('admission_conflict')
        record['unknown'] = False
        return key

    def _preservation_operation_matches(self, record, *, owner_class, principal_id, session_id,
                                         operation, request_id, admission_id, generation,
                                         source_identity):
        expected = {
            'owner_class': owner_class, 'principal_id': principal_id, 'session_id': session_id,
            'operation': operation, 'request_id': request_id,
            'source_identity': source_identity,
        }
        return (all(record.get(name) == value for name, value in expected.items())
                and self._preservation_operation_admission_call_matches(record, admission_id)
                and self._preservation_operation_generation_call_matches(record, generation))

    @staticmethod
    def _preservation_operation_validate_parent_handle(parent_handle, coordinator, profile_id, epoch,
                                                        instance_id):
        """Validate an explicit opaque parent before either lookup or reservation."""
        if parent_handle is None:
            return None
        parent_owner = getattr(parent_handle, 'owner_class', None)
        if parent_owner not in {
                'I04_fifo_turn_execution', 'I07_automation_route_wakes',
                'I10_startup_recovery_handoff',
        }:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if (getattr(parent_handle, 'profile_id', None) != profile_id
                or getattr(parent_handle, 'authority_epoch', None) != epoch
                or getattr(parent_handle, 'process_instance', None)
                != getattr(coordinator, 'process_instance', None)
                or getattr(parent_handle, 'process_instance', None) != instance_id
                or getattr(coordinator, 'process_instance', None) != instance_id
                or getattr(parent_handle, 'coordinator_id', None)
                != getattr(coordinator, '_coordinator_id', None)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        records = getattr(coordinator, '_records', None)
        parent_id = getattr(getattr(parent_handle, 'lease', None), 'lease_id', None)
        record = records.get(parent_id) if isinstance(records, dict) else None
        if (record is None or record.handle is not parent_handle
                or record.state not in {'accepted', 'finalizing', 'unknown'}):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if parent_owner != 'I10_startup_recovery_handoff' and getattr(
                coordinator, '_parent', lambda _handle: None)(parent_handle) is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if not coordinator.profile_scope_valid(
                profile_id, epoch, require_serving=False, allow_retiring=True,
                physical_owner_epoch=getattr(parent_handle, 'physical_owner_epoch', None)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return parent_handle

    @staticmethod
    def _preservation_operation_validate_parent_handles(parent_handles, coordinator, profile_id, epoch,
                                                         instance_id):
        if parent_handles is None:
            return None
        if not isinstance(parent_handles, tuple) or not parent_handles:
            raise RuntimeStoreError('preservation_owner_unmapped')
        validated = tuple(
            PreservationOperationsMixin._preservation_operation_validate_parent_handle(
                parent, coordinator, profile_id, epoch, instance_id,
            )
            for parent in parent_handles
        )
        if len({id(parent) for parent in validated}) != len(validated):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return validated

    @staticmethod
    def _preservation_operation_parent_tuple(parent_handle):
        if parent_handle is None:
            return ()
        if isinstance(parent_handle, tuple):
            return parent_handle
        return (parent_handle,)

    def _preservation_operation_validate_source_proof(
            self, owner_class, principal_id, session_id, operation, request_id,
            admission_id, generation, source_identity, source_proof):
        """Constrain source-authenticated proof to the exact I11 local operation tuple."""
        if source_proof is None:
            return
        from gateway.session_recovery_custody import LocalPolicyLineageWitness

        if (
            not isinstance(source_proof, LocalPolicyLineageWitness)
            or owner_class != 'I11_session_mutations_finalizers'
            or operation != 'local-compression-adoption'
            or admission_id is not None
            or generation is not None
            or principal_id != source_proof.principal_id
            or session_id != source_proof.session_id
            or request_id != source_proof.request_id
            or source_identity != self._local_compression_source_identity(source_proof)
            or source_proof.effect_state not in {'planned', 'committed'}
        ):
            raise RuntimeStoreError('preservation_owner_unmapped')

    def _local_compression_parent_handles(self, record):
        if (not isinstance(record, dict)
                or record.get('owner_class') != 'I11_session_mutations_finalizers'
                or record.get('operation') != 'local-compression-adoption'):
            return ()
        stored = record.get('parent_handles')
        if stored is None:
            return self._preservation_operation_parent_tuple(record.get('parent_handle'))
        return stored if isinstance(stored, tuple) else None

    def _local_compression_record_identity_matches(self, record, source, parent_handle):
        from gateway.session_recovery_custody import LocalPolicyLineageWitness
        expected_parents = self._preservation_operation_parent_tuple(parent_handle)
        stored_parents = self._local_compression_parent_handles(record)
        return (
            isinstance(source, LocalPolicyLineageWitness)
            and isinstance(record, dict)
            and isinstance(stored_parents, tuple)
            and len(stored_parents) == len(expected_parents)
            and all(left is right for left, right in zip(stored_parents, expected_parents))
            and ((not stored_parents and record.get('parent_handle') is None)
                 or (stored_parents and record.get('parent_handle') is stored_parents[0]))
            and record.get('owner_class') == 'I11_session_mutations_finalizers'
            and record.get('operation') == 'local-compression-adoption'
            and record.get('principal_id') == source.principal_id
            and record.get('session_id') == source.session_id
            and record.get('request_id') == source.request_id
            and record.get('admission_id') is None
            and record.get('generation') is None
            and record.get('source_witness') is source
            and record.get('source_identity') == self._local_compression_source_identity(source)
        )

    def _local_compression_current_parent_handles(self, record):
        handles = self._local_compression_parent_handles(record)
        if handles is None:
            return None
        return handles

    def _local_compression_current_witness(self, source, *, committed=False):
        from gateway.session_recovery_custody import (
            LocalPolicyLineageWitness, local_policy_lineage_plan,
            local_policy_lineage_witness,
        )
        if not isinstance(source, LocalPolicyLineageWitness):
            return None
        try:
            if source.effect_state == "planned" and not committed:
                children = source.expected_lineage[len(source.lineage):]
                return local_policy_lineage_plan(
                    self, source.session_id, expected_children=children,
                )
            return local_policy_lineage_witness(self, source.session_id)
        except (AttributeError, KeyError, LookupError, RuntimeStoreError, TypeError, ValueError):
            return None

    def _local_compression_source_current(self, source, *, committed=False):
        current = self._local_compression_current_witness(source, committed=committed)
        if current is None:
            return False
        same_source = (
            current.schema == source.schema
            and current.source == source.source
            and current.session_id == source.session_id
            and current.route == source.route
            and current.profile_id == source.profile_id == self.profile_id
            and current.authority_epoch == source.authority_epoch == self.epoch
            and current.process_instance == source.process_instance == self.instance_id
            and current.principal_id == source.principal_id
            and current.request_id == source.request_id
            and current.policy_digest == source.policy_digest
            and current.effect_state == ("committed" if committed else source.effect_state)
        )
        expected = (
            current.physical_session_id == source.expected_physical_session_id
            and current.lineage == source.expected_lineage
        )
        base = (
            current.physical_session_id == source.physical_session_id
            and current.lineage == source.lineage
        )
        return same_source and (expected if committed else base)

    @staticmethod
    def _local_compression_source_identity(source):
        return f'{_LOCAL_COMPRESSION_SOURCE}{source.policy_digest}'

    def _local_compression_record_matches(self, record, source, parent_handle):
        from gateway.session_recovery_custody import _coordinator_record

        handle = record.get('handle') if isinstance(record, dict) else None
        return (
            self._local_compression_record_identity_matches(record, source, parent_handle)
            and handle is not None
            and _coordinator_record(self, handle) is not None
        )

    def _preservation_operation_retry_local_compression(
            self, key, record, source, parent_handles):
        if (self._preservation_operations.get(key) is not record
                or not self._local_compression_source_current(source)
                or not self._local_compression_record_matches(record, source, parent_handles)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        coordinator = self._require_preservation_coordinator()
        if coordinator is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
        handle = record['handle']
        if parent_handles:
            refusal = coordinator.reconcile_unknown_with_parents(handle, parent_handles)
        else:
            refusal = coordinator.reconcile_unknown(
                handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
                process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
        if refusal is not None:
            raise RuntimeStoreError('admission_conflict')
        record['unknown'] = False
        return key

    def _preservation_operation_find_local_compression(self, session_id):
        """Find one retained local-compression operation before due-work enumeration."""
        from gateway.session_recovery_custody import LocalPolicyLineageWitness

        matches = []
        for key, record in self._preservation_operations.items():
            source = record.get('source_witness') if isinstance(record, dict) else None
            parents = self._local_compression_parent_handles(record)
            if (
                not isinstance(record, dict)
                or record.get('owner_class') != 'I11_session_mutations_finalizers'
                or record.get('operation') != 'local-compression-adoption'
                or record.get('session_id') != session_id
                or not isinstance(source, LocalPolicyLineageWitness)
                or source.session_id != session_id
                or record.get('source_identity') != self._local_compression_source_identity(source)
                or parents is None
                or not self._local_compression_record_matches(record, source, parents)
            ):
                continue
            matches.append((key, record, source, parents or None))
        if len(matches) > 1:
            raise RuntimeStoreError('preservation_owner_unmapped')
        return matches[0] if matches else None

    def _preservation_operation_reconcile_local_compression_committed(
            self, key, record, source, parent_handles):
        if (self._preservation_operations.get(key) is not record
                or not self._local_compression_record_matches(record, source, parent_handles)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        current = self._local_compression_current_witness(source, committed=True)
        if current is None or not self._local_compression_source_current(source, committed=True):
            raise RuntimeStoreError('read_failed')
        coordinator = self._require_preservation_coordinator()
        if coordinator is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
        handle = record['handle']
        if record.get('unknown'):
            if parent_handles:
                refusal = coordinator.reconcile_unknown_with_parents(handle, parent_handles)
            else:
                refusal = coordinator.reconcile_unknown(
                    handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
                    process_instance=handle.process_instance, authority_epoch=handle.authority_epoch,
                )
            if refusal is not None:
                raise RuntimeStoreError('admission_conflict')
            record['unknown'] = False
        record['source_witness'] = current
        record['committed_witness'] = current
        record['source_identity'] = self._local_compression_source_identity(current)
        return current

    def _preservation_operation_record_local_compression_committed(self, key, source):
        record = self._preservation_operations.get(key)
        parents = self._local_compression_parent_handles(record)
        if not self._local_compression_record_matches(record, source, parents):
            raise RuntimeStoreError('preservation_owner_unmapped')
        current = self._local_compression_current_witness(source, committed=True)
        if current is None or not self._local_compression_source_current(source, committed=True):
            raise RuntimeStoreError('read_failed')
        record['source_witness'] = current
        record['committed_witness'] = current
        record['source_identity'] = self._local_compression_source_identity(current)
        return current

    def _preservation_operation_start_local_compression(self, source, *, parent_handles=None):
        if not self._local_compression_source_current(source):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return self._preservation_operation_start(
            'I11_session_mutations_finalizers', principal_id=source.principal_id,
            session_id=source.session_id, operation='local-compression-adoption',
            request_id=source.request_id, source_identity=self._local_compression_source_identity(source),
            source_proof=source, parent_handles=parent_handles)

    def _preservation_operation_local_compression_terminal(self, key, source):
        record = self._preservation_operations.get(key)
        parent_handles = self._local_compression_parent_handles(record)
        if not self._local_compression_record_matches(record, source, parent_handles):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if not self._local_compression_source_current(source, committed=True):
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        handle = record['handle']
        if not self._preservation_operation_terminal(
                key, f'local-compression:{source.session_id}:{source.expected_physical_session_id}'):
            return False
        coordinator = self._preservation_coordinator()
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        completed = getattr(coordinator, '_completed', None) if coordinator is not None else None
        records = getattr(coordinator, '_records', None) if coordinator is not None else None
        return (
            key not in self._preservation_operations
            and isinstance(completed, dict) and completed.get(lease_id) == 'terminal'
            and isinstance(records, dict) and lease_id not in records
        )

    def _preservation_operation_unknown_verified(self, key, reason):
        self._preservation_operation_unknown(key, reason)
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        lease_id = getattr(record.get('handle') if isinstance(record, dict) else None, 'lease', None)
        lease_id = getattr(lease_id, 'lease_id', None)
        records = getattr(coordinator, '_records', None) if coordinator is not None else None
        if (record is None or coordinator is None or not isinstance(records, dict)
                or lease_id is None or getattr(records.get(lease_id), 'state', None) != 'unknown'):
            raise RuntimeStoreError('preservation_owner_unmapped')
        parents = self._local_compression_parent_handles(record) if isinstance(record, dict) else ()
        for parent in parents or ():
            parent_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
            parent_record = records.get(parent_id)
            if parent_record is None or parent_record.handle is not parent or parent_record.state != 'unknown':
                raise RuntimeStoreError('preservation_owner_unmapped')
        return True

    def _preservation_operation_existing_start(
            self, key, existing, *, owner_class, principal_id, session_id, operation,
            request_id, admission_id, generation, source_identity, parent_handle, parent_handles,
            source_proof):
        if not self._preservation_operation_matches(
                existing, owner_class=owner_class, principal_id=principal_id,
                session_id=session_id, operation=operation, request_id=request_id,
                admission_id=admission_id, generation=generation, source_identity=source_identity):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if parent_handles is not None:
            stored = self._local_compression_parent_handles(existing)
            if (not isinstance(stored, tuple) or len(stored) != len(parent_handles)
                    or any(left is not right for left, right in zip(stored, parent_handles))):
                raise RuntimeStoreError('preservation_owner_unmapped')
        elif parent_handle is not None and existing.get('parent_handle') is not parent_handle:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if source_proof is not None:
            if (not self._local_compression_source_current(source_proof)
                    or not self._local_compression_record_matches(
                        existing, source_proof, parent_handles if parent_handles is not None else parent_handle)):
                raise RuntimeStoreError('preservation_owner_unmapped')
            if not existing.get('unknown'):
                return key
            return self._preservation_operation_retry_local_compression(
                key, existing, source_proof,
                parent_handles if parent_handles is not None else parent_handle)
        if self._preservation_operation_current_admission(existing) is None:
            raise RuntimeStoreError('stale_generation')
        if not existing.get('unknown'):
            return key
        return self._preservation_operation_retry(
            key, existing, owner_class=owner_class, principal_id=principal_id,
            session_id=session_id, operation=operation, request_id=request_id,
            admission_id=admission_id, generation=generation, source_identity=source_identity)

    def _preservation_operation_parent_for_start(
            self, coordinator, owner_class, session_id, principal_id, admission_id,
            generation, source_identity, source_proof, parent_handle, parent_handles, operation):
        if parent_handles is not None:
            return parent_handles
        if parent_handle is not None:
            return parent_handle
        parent = self._preservation_operation_parent(
            session_id, admission_id, generation=generation, principal_id=principal_id,
            queued_control=owner_class == 'I05_controls_and_unknown' and operation in {'cancel', 'stop'},
        )
        if parent is None and owner_class in {
                'I03_api_run_direct', 'I07_automation_route_wakes',
                'I08_goals_loops_heartbeat', 'I09_a2a_hosted_kanban',
                'I11_session_mutations_finalizers',
        }:
            standalone_mutation = (
                owner_class == 'I11_session_mutations_finalizers'
                and source_identity == 'direct_authenticated_mutation'
                and admission_id is None
            )
            local_compression_root = (
                owner_class == 'I11_session_mutations_finalizers'
                and admission_id is None
                and source_identity is not None
                and source_proof is not None
                and source_identity == self._local_compression_source_identity(source_proof)
                and self._local_compression_source_current(source_proof)
            )
            producer_root = owner_class in _AUTOMATION_OWNER_CLASSES
            if not standalone_mutation and not local_compression_root and not producer_root:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if coordinator.state != 'open':
                raise RuntimeStoreError('runtime_draining')
        elif parent is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        return parent

    def _preservation_operation_reserve_record(
            self, coordinator, key, *, owner_class, principal_id, session_id, operation,
            request_id, admission_id, generation, source_identity, source_proof,
            admission_request_id, parent, parent_handles):
        from gateway.runtime_custody import CustodyRefusal
        if parent_handles is not None:
            handle = coordinator.reserve_root_with_parents(
                owner_class, key, profile_id=self.profile_id,
                authority_epoch=self.epoch, parents=parent_handles)
            stored_parent = parent_handles[0]
        elif parent is None:
            handle = coordinator.reserve_root(
                owner_class, key, profile_id=self.profile_id, authority_epoch=self.epoch)
            stored_parent = None
        elif coordinator.state == 'closing':
            handle = coordinator.reserve_root(
                owner_class, key, profile_id=self.profile_id,
                authority_epoch=self.epoch, parent=parent)
            stored_parent = parent
        else:
            reservation = coordinator.reserve_child(
                parent, owner_class, key, profile_id=self.profile_id, authority_epoch=self.epoch)
            handle = (coordinator.activate_child(reservation)
                      if not isinstance(reservation, CustodyRefusal) else reservation)
            stored_parent = parent
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        self._preservation_operations[key] = {
            'handle': handle,
            'owner_class': owner_class,
            'profile_id': self.profile_id,
            'authority_epoch': self.epoch,
            'principal_id': principal_id,
            'session_id': session_id,
            'operation': operation,
            'request_id': request_id,
            'admission_request_id': admission_request_id,
            'admission_id': admission_id,
            'requested_admission_id': admission_id,
            'admission_ids': [] if admission_id is None else [admission_id],
            'generation': generation,
            'requested_generation': generation,
            'generation_state': self._preservation_operation_generation_state(generation),
            'admission_bindings': {},
            'source_identity': source_identity,
            'source_witness': source_proof,
            'parent_handle': stored_parent,
            'parent_handles': (tuple(parent_handles)
                               if owner_class == 'I11_session_mutations_finalizers'
                               and operation == 'local-compression-adoption'
                               and parent_handles is not None else None),
            'unknown': False,
        }
        return key

    def _preservation_operation_start(self, owner_class, *, principal_id, session_id, operation,
                                      request_id=None, admission_id=None, generation=None,
                                      source_identity=None, parent_handle=None, parent_handles=None,
                                      source_proof=None):
        """Reserve an I03/I05/I06/I11 operation before its first await/task/callback."""
        self._preservation_operation_validate_source_proof(
            owner_class, principal_id, session_id, operation, request_id,
            admission_id, generation, source_identity, source_proof,
        )
        coordinator = self._require_preservation_coordinator()
        if coordinator is None:
            if parent_handle is not None or parent_handles:
                raise RuntimeStoreError('preservation_owner_unmapped')
            return None
        validated_parent = self._preservation_operation_validate_parent_handle(
            parent_handle, coordinator, self.profile_id, self.epoch, self.instance_id)
        validated_parents = self._preservation_operation_validate_parent_handles(
            parent_handles, coordinator, self.profile_id, self.epoch, self.instance_id)
        key = self._preservation_operation_identity(
            owner_class, profile_id=self.profile_id, authority_epoch=self.epoch,
            session_id=session_id, principal_id=principal_id,
            operation=operation, request_id=request_id, admission_id=admission_id,
            generation=generation, source_identity=source_identity)
        if owner_class == 'I06_busy_followup_delivery':
            self._busy_inbound_seen = True
        existing = self._preservation_operations.get(key)
        if existing is None:
            replay_key = self._preservation_operation_replay_key(
                owner_class=owner_class, principal_id=principal_id, session_id=session_id,
                operation=operation, request_id=request_id, admission_id=admission_id,
                generation=generation, source_identity=source_identity)
            if replay_key is not None:
                key = replay_key
                existing = self._preservation_operations[key]
        if existing is not None:
            return self._preservation_operation_existing_start(
                key, existing, owner_class=owner_class, principal_id=principal_id,
                session_id=session_id, operation=operation, request_id=request_id,
                admission_id=admission_id, generation=generation, source_identity=source_identity,
                parent_handle=validated_parent, parent_handles=validated_parents,
                source_proof=source_proof)
        admission_request_id = self._preservation_operation_admission_request_id(
            owner_class, principal_id, session_id, admission_id, generation, request_id)
        parent = self._preservation_operation_parent_for_start(
            coordinator, owner_class, session_id, principal_id, admission_id, generation,
            source_identity, source_proof, validated_parent, validated_parents, operation)
        parent_handles_for_record = parent if isinstance(parent, tuple) else None
        parent_for_record = None if parent_handles_for_record is not None else parent
        return self._preservation_operation_reserve_record(
            coordinator, key, owner_class=owner_class, principal_id=principal_id,
            session_id=session_id, operation=operation, request_id=request_id,
            admission_id=admission_id, generation=generation, source_identity=source_identity,
            source_proof=source_proof, admission_request_id=admission_request_id,
            parent=parent_for_record, parent_handles=parent_handles_for_record)

    def _preservation_operation_start_direct_mutation(
        self, *, principal_id, session_id, operation, request_id=None, admission_id=None, generation=None
    ):
        """Reserve the standalone authenticated mutation root with a source-owned identity."""
        return self._preservation_operation_start(
            'I11_session_mutations_finalizers', principal_id=principal_id, session_id=session_id,
            operation=operation, request_id=request_id, admission_id=admission_id,
            generation=generation, source_identity='direct_authenticated_mutation')



    def _preservation_operation_unknown(self, key, reason):
        if key is None:
            return
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        if record is None:
            return
        record['unknown'] = True
        if coordinator is None:
            self._require_preservation_coordinator()
            return
        coordinator.mark_unknown(record['handle'], reason)
        for parent in self._local_compression_parent_handles(record) or ():
            coordinator.mark_unknown(parent, reason)
    def _preservation_operation_release_unaccepted(self, key):
        if key is None:
            return
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        if record is None:
            return
        if coordinator is None:
            self._require_preservation_coordinator()
            return
        result = coordinator.release_unaccepted(record['handle'])
        if result is None or result.code == 'stale_lease':
            self._preservation_operations.pop(key, None)
        else:
            coordinator.mark_unknown(record['handle'], 'read_failed')
    def _preservation_operation_completed_local_compression(self, key, record):
        source = record.get('source_witness') if isinstance(record, dict) else None
        parents = self._local_compression_parent_handles(record)
        if not self._local_compression_record_identity_matches(record, source, parents):
            return False
        expected_key = self._preservation_operation_identity(
            record['owner_class'], profile_id=record['profile_id'],
            authority_epoch=record['authority_epoch'], session_id=record['session_id'],
            principal_id=record['principal_id'], operation=record['operation'],
            request_id=record.get('request_id'), admission_id=record.get('admission_id'),
            generation=record.get('generation'), source_identity=record.get('source_identity'),
        )
        handle = record.get('handle')
        if (key != expected_key or getattr(handle, 'owner_id', None) != key
                or record.get('committed_witness') is not source
                or not self._local_compression_source_current(source, committed=True)):
            return False
        coordinator = self._preservation_coordinator()
        lock = getattr(coordinator, '_lock', None) if coordinator is not None else None
        if lock is None:
            return False
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        with lock:
            completed = getattr(coordinator, '_completed', None)
            records = getattr(coordinator, '_records', None)
            return (isinstance(completed, dict) and completed.get(lease_id) == 'terminal'
                    and isinstance(records, dict) and lease_id not in records)

    def _preservation_operation_terminal(self, key, finalizer_id):
        if key is None:
            return False
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        if record is None:
            return False
        if record.get('terminal') is True:
            self._preservation_operations.pop(key, None)
            return True
        if coordinator is None:
            self._require_preservation_coordinator()
            return False
        if self._preservation_operation_completed_local_compression(key, record):
            self._preservation_operations.pop(key, None)
            return True
        finalized = coordinator.mark_finalizing(record['handle'], finalizer_id)
        if finalized is not None:
            if self._preservation_operation_completed_local_compression(key, record):
                self._preservation_operations.pop(key, None)
                return True
            self._preservation_operation_unknown(key, 'read_failed')
            return False
        parents = self._local_compression_parent_handles(record)
        if parents:
            result = coordinator.mark_terminal_with_parents(record['handle'], parents)
        else:
            result = coordinator.mark_terminal(record['handle'])
        if result is None:
            if record.get('owner_class') == 'I06_busy_followup_delivery':
                owner = record.get('producer_object')
                bridge = getattr(owner, '_classic_execution_bridge', None)
                if (getattr(bridge, 'operation_key', None) == key
                        and getattr(bridge, 'admission_id', None) == record.get('admission_id')
                        and getattr(owner, '_classic_custody_authority', None) is self):
                    bridge.retired_operation_record = record
            self._preservation_operations.pop(key, None)
            return True
        if result.code == 'stale_lease' and self._preservation_operation_completed_local_compression(
                key, record):
            self._preservation_operations.pop(key, None)
            return True
        if result.code != 'stale_lease':
            self._preservation_operation_unknown(key, 'lease_release_failed')
        return False

    def _preservation_restart_control_row(self, principal_id, session_id, admission_id, generation):
        """Authenticate a lost admission without adopting its former live owner."""
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id)
        if (row is None or row.get('principal_id') != principal_id
                or row.get('target_session_id') != session_id):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if type(generation) is not int or generation < 1 or row.get('generation') != generation:
            raise RuntimeStoreError('stale_generation')
        if row.get('owner_epoch') == self.epoch:
            return None
        if (type(row.get('owner_epoch')) is not int or row['owner_epoch'] >= self.epoch
                or row.get('status') not in {'unknown', 'terminal'}
                or not isinstance(row.get('request_id'), str) or not row['request_id']):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return row

    def _preservation_restart_control_handle_matches(self, coordinator, handle, key):
        expected = {
            'owner_class': 'I05_controls_and_unknown', 'owner_id': key,
            'profile_id': self.profile_id, 'authority_epoch': self.epoch,
            'process_instance': self.instance_id, 'coordinator_id': coordinator._coordinator_id,
        }
        return all(getattr(handle, name, None) == value for name, value in expected.items())

    def _preservation_restart_control_start(
        self, *, principal_id, session_id, request_id, admission_id, generation, source_identity
    ):
        """Reserve a current recovery operation, never a fabricated old TurnLease.

        The canonical durable resolver decides whether the exact lost row can resolve/replay.
        Its old identity stays immutable; only this new control has current-epoch custody.
        """
        row = self._preservation_restart_control_row(
            principal_id, session_id, admission_id, generation)
        if row is None:
            return False, None
        coordinator = self._require_preservation_coordinator()
        if coordinator is None:
            return False, None
        identity = {name: row[name] for name in (
            'admission_id', 'owner_epoch', 'principal_id', 'target_session_id', 'generation', 'request_id')}
        key = self._preservation_operation_identity(
            'I05_controls_and_unknown', profile_id=self.profile_id, authority_epoch=self.epoch,
            session_id=session_id, principal_id=principal_id, operation='resolve_unknown',
            request_id=request_id, admission_id=admission_id, generation=generation,
            source_identity=f'restart:{row["owner_epoch"]}:{source_identity or "owner"}')
        record = self._preservation_operations.get(key)
        if record is not None:
            handle = record['handle']
            if (record.get('recovery_identity') != identity
                    or not self._preservation_restart_control_handle_matches(coordinator, handle, key)):
                raise RuntimeStoreError('preservation_owner_unmapped')
            if record.get('unknown'):
                refusal = coordinator.reconcile_unknown(
                    handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
                    process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
                if refusal is not None:
                    raise RuntimeStoreError('admission_conflict')
                record['unknown'] = False
            elif coordinator._parent(handle) is None:
                raise RuntimeStoreError('preservation_owner_unmapped')
            return True, key
        from gateway.runtime_custody import CustodyRefusal
        handle = coordinator.reserve_root(
            'I05_controls_and_unknown', key, profile_id=self.profile_id, authority_epoch=self.epoch)
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        self._preservation_operations[key] = {
            'handle': handle, 'owner_class': 'I05_controls_and_unknown',
            'profile_id': self.profile_id, 'authority_epoch': self.epoch,
            'principal_id': principal_id, 'session_id': session_id,
            'operation': 'resolve_unknown', 'request_id': request_id,
            'admission_id': admission_id, 'generation': generation,
            'recovery_identity': identity, 'unknown': False,
        }
        return True, key

    def _preservation_control_terminal(self, key, operation, request_id, admission_id, session_id, principal_id):
        self._preservation_operation_terminal(
            key, f'control:{operation}:{request_id or admission_id or session_id}')
        if key is None or key in self._preservation_operations or operation not in {'cancel', 'stop'}:
            return
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(self.db, admission_id=admission_id)
        if (row is not None and row.get('status') == 'terminal' and row.get('owner_epoch') == self.epoch
                and row.get('principal_id') == principal_id and row.get('target_session_id') == session_id):
            # The durable cancellation tried to release its parent while this control child
            # was still live. Retry that existing terminalizer after the exact child closes.
            completion = None
            try:
                from gateway.platforms.api_server_run_completion import completion_for
                completion = completion_for(self, admission_id)
            except (ImportError, RuntimeError):
                completion = None
            if (completion is not None
                    and getattr(completion, '_pending_parent_release', False)):
                completion.settle_after_child()
                return
            self._preservation_release_terminal(admission_id)

    async def _preservation_control_call(self, *, principal_id, session_id, operation,
                                         request_id=None, admission_id=None, generation=None,
                                         source_identity=None, call):
        """Run one authenticated control and retain custody across cancellation/late failure."""
        recovery, key = False, None
        if operation == 'resolve_unknown' and admission_id is not None:
            recovery, key = self._preservation_restart_control_start(
                principal_id=principal_id, session_id=session_id, request_id=request_id,
                admission_id=admission_id, generation=generation, source_identity=source_identity)
        if not recovery:
            key = self._preservation_operation_start(
                'I05_controls_and_unknown', principal_id=principal_id, session_id=session_id,
                operation=operation, request_id=request_id, admission_id=admission_id,
                generation=generation, source_identity=source_identity)
            if key is not None and admission_id is not None and self._preservation_operation_row(key) is None:
                raise RuntimeStoreError('preservation_owner_unmapped')
        try:
            result = await call()
        except asyncio.CancelledError:
            self._preservation_operation_unknown(key, 'join_failed')
            raise
        except RuntimeStoreError as exc:
            if exc.reason in {
                'invalid_params', 'not_found', 'permission_denied', 'profile_mismatch',
                'runtime_draining', 'session_busy', 'stale_generation', 'unsupported_control',
                'execution_not_ready', 'admission_conflict', 'approval_not_pending',
            }:
                self._preservation_operation_release_unaccepted(key)
            else:
                self._preservation_operation_unknown(key, 'read_failed')
            raise
        except Exception:  # health: allow BLE001 -- control callback failure retains exact custody unknown
            self._preservation_operation_unknown(key, 'join_failed')
            raise
        self._preservation_control_terminal(key, operation, request_id, admission_id, session_id, principal_id)
        return result
    def _preservation_mutation_done(self, key, *, cancelled=False, error=None):
        """Finish a real session.mutate task only after exact receipt reconciliation."""
        if key is None:
            return
        if cancelled:
            self._preservation_operation_unknown(key, 'join_failed')
            return
        record = self._preservation_operations.get(key)
        if record is None:
            return
        if error is None:
            self._preservation_operation_terminal(key, f'mutation:{record["request_id"] or record["session_id"]}')
            return
        try:
            from hermes_state_mutation_retirement import has_mutation_receipt
            committed = has_mutation_receipt(
                self.db, record['principal_id'], record['session_id'], record['request_id'])
        except (OSError, RuntimeError, sqlite3.Error, RuntimeStoreError, TypeError, ValueError, KeyError):
            # Exact receipt read failure retains custody unknown; no exception value is published.
            self._preservation_operation_unknown(key, 'read_failed')
            return
        if committed:
            self._preservation_operation_terminal(key, f'mutation-reconcile:{record["request_id"]}')
        else:
            self._preservation_operation_release_unaccepted(key)
