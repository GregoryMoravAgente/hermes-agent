"""Profile authority's local preservation bindings and readiness checks.

These methods act on the existing SessionAuthority state; the mixin adds no owner or
second custody ledger.
"""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError, find_session_admission


class SessionAuthorityPreservationLocalMixin:
    def _preservation_bind_scheduler(self, scheduler_id, handle, *, expected_job_id=None):
        """Bind a joinable scheduler task, retaining a trusted Teams job identity when supplied."""
        if handle is None:
            return
        expected = None if expected_job_id is None else str(expected_job_id).strip()
        if expected_job_id is not None and not expected:
            coordinator = self._preservation_coordinator()
            if coordinator is not None:
                coordinator.mark_unknown(handle, 'unmapped_writer')
            return
        if scheduler_id in self._preservation_scheduler_handles:
            coordinator = self._preservation_coordinator()
            if coordinator is not None:
                coordinator.mark_unknown(handle, 'unmapped_writer')
            return
        self._preservation_scheduler_handles[scheduler_id] = {
            'handle': handle, 'expected_job_id': expected,
        }
        self._native_inbound_seen = True
    def _preservation_adopt_scheduler_job(self, job_id):
        """Bind a recovered Teams job to its exact predecessor lease or a new successor root."""
        job_key = str(job_id or '').strip()
        if not job_key:
            raise RuntimeStoreError('preservation_owner_unmapped')
        scheduler_id = f'teams-recovery:{job_key}'
        prior = self._preservation_scheduler_retry_handles.pop(job_key, None)
        if prior is not None:
            prior_id, handle = prior
            self._preservation_scheduler_handles.pop(prior_id, None)
        else:
            handle = self._preservation_root('I01_native_inbound', scheduler_id)
        if handle is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
        self._preservation_bind_scheduler(scheduler_id, handle, expected_job_id=job_key)
        return scheduler_id
    def _preservation_scheduler_done(self, scheduler_id, task):
        """Settle a generic joinable task or an identity-bound durable Teams task."""
        binding = self._preservation_scheduler_handles.get(scheduler_id)
        if binding is None:
            return
        if isinstance(binding, dict):
            handle = binding.get('handle')
            expected_job_id = binding.get('expected_job_id')
        else:  # compatibility with a pre-R9 in-memory binding during a live upgrade
            handle, expected_job_id = binding, None
        coordinator = self._preservation_coordinator()
        if coordinator is None or handle is None:
            return
        if task.cancelled():
            coordinator.mark_unknown(handle, 'join_failed')
            return
        if expected_job_id is None:
            try:
                task.result()
            except BaseException:  # health: allow BLE001 -- opaque scheduler task settlement failure retains its exact custody as unknown without exception values
                coordinator.mark_unknown(handle, 'join_failed')
                return
            # A generic scheduler owns no Teams durable row. A successful joinable task is its
            # complete ownership boundary; its result shape must not be mistaken for a Teams model.
            terminal = coordinator.mark_terminal(handle)
            if terminal is None:
                self._preservation_scheduler_handles.pop(scheduler_id, None)
            else:
                coordinator.mark_unknown(handle, 'lease_release_failed')
            return
        try:
            result = task.result()
        except BaseException:  # health: allow BLE001 -- opaque scheduler task settlement failure retains its exact custody as unknown without exception values
            coordinator.mark_unknown(handle, 'join_failed')
            return
        job_id = result.get('job_id') if isinstance(result, dict) else getattr(result, 'job_id', None)
        status = result.get('status') if isinstance(result, dict) else getattr(result, 'status', None)
        if str(job_id or '').strip() != expected_job_id:
            coordinator.mark_unknown(handle, 'foreign_custody')
            return
        if status not in {'completed', 'failed', 'retry_scheduled'}:
            coordinator.mark_unknown(handle, 'join_failed')
            return
        # The task's returned model is not the durable authority. Re-read the exact expected job
        # record so a provider callback cannot claim terminal custody before the JSON store persists it.
        runtime = getattr(self.runner, '_teams_pipeline_runtime', None)
        store = getattr(runtime, 'store', None) if runtime is not None else None
        from plugins.teams_pipeline.store_ownership import TeamsStoreOwnershipError
        try:
            persisted = None if store is None else store.get_job(expected_job_id)
        except (OSError, ValueError, KeyError, TypeError, TeamsStoreOwnershipError):
            persisted = None
        if not isinstance(persisted, dict) or persisted.get('status') != status:
            coordinator.mark_unknown(handle, 'read_failed')
            return
        if status == 'retry_scheduled':
            # Future work is not a business-terminal failure. Keep the original lease live and let
            # startup/rebind transfer it to this exact expected job id.
            self._preservation_scheduler_retry_handles[expected_job_id] = (scheduler_id, handle)
            return
        terminal = coordinator.mark_terminal(handle)
        if terminal is None:
            self._preservation_scheduler_handles.pop(scheduler_id, None)
        else:
            coordinator.mark_unknown(handle, 'lease_release_failed')
    def _preservation_retain_terminal_automation(self, row, handle):
        """Retain one live automation lease until terminal callback classification consumes it."""
        if (not isinstance(row, dict)
                or row.get('status') != 'terminal'
                or row.get('outcome') == 'cancelled'
                or getattr(handle, 'owner_class', None) not in {
                    'I07_automation_route_wakes', 'I08_goals_loops_heartbeat',
                    'I09_a2a_hosted_kanban',
                }):
            return False
        admission_id = row.get('admission_id')
        if not isinstance(admission_id, str) or not admission_id:
            return False
        descriptor = self._preservation_automation_row_descriptor(row)
        if not isinstance(descriptor, dict):
            return False
        coordinator = self._preservation_coordinator()
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        records = getattr(coordinator, '_records', None) if coordinator is not None else None
        lock = getattr(coordinator, '_lock', None) if coordinator is not None else None
        if not isinstance(lease_id, str) or not isinstance(records, dict) or lock is None:
            return False
        with lock:
            live = records.get(lease_id)
            if (live is None or getattr(live, 'handle', None) is not handle
                    or getattr(live, 'state', None) not in {'accepted', 'finalizing'}):
                return False
        existing = self._preservation_automation_bindings.get(admission_id)
        if (isinstance(existing, dict) and existing.get('handle') is not None
                and existing.get('handle') is not handle):
            return False
        mapped = self._preservation_handles.get(admission_id)
        if mapped is not None and mapped is not handle:
            return False
        if not self._preservation_automation_row_matches(row, handle, allow_terminal=True):
            return False
        self._preservation_forget_pending_root(handle)
        self._preservation_handles[admission_id] = handle
        binding = dict(existing or {})
        binding.update({
            'handle': handle,
            'row': dict(row),
            'descriptor': dict(descriptor),
            'source_identity': descriptor.get('source_identity') or binding.get('source_identity'),
            'delivery_id': descriptor.get('delivery_id') or binding.get('delivery_id'),
            'terminal_pending': True,
        })
        self._preservation_automation_bindings[admission_id] = binding
        return True


    def _preservation_bind_or_release(self, row, handle):
        if handle is None:
            return
        if row is not None and row.get('status') != 'terminal':
            if not self._preservation_pending_matches_row(row, handle):
                coordinator = self._preservation_coordinator()
                if coordinator is not None:
                    coordinator.mark_unknown(handle, 'unmapped_writer')
                return
            admission_id = row['admission_id']
            existing = self._preservation_handles.get(admission_id)
            if existing is not None:
                self._preservation_forget_pending_root(handle)
                if existing is not handle:
                    self._preservation_release_unaccepted(handle)
                return
            # ``tracked_write(ordered=True)`` makes the durable row order authoritative. Bind the
            # callback's own object, then let a later exact duplicate observe ``existing`` and
            # release its own object; no request-id lookup can replace the original lease.
            self._preservation_forget_pending_root(handle)
            self._preservation_handles[admission_id] = handle
            if getattr(handle, 'owner_class', None) == 'I07_automation_route_wakes':
                descriptor = self._preservation_automation_row_descriptor(row)
                self._preservation_automation_bindings[admission_id] = {
                    'handle': handle,
                    'descriptor': dict(descriptor or {}),
                    'source_identity': (descriptor or {}).get('source_identity'),
                    'delivery_id': (descriptor or {}).get('delivery_id'),
                }
            if handle.owner_class == 'I01_native_inbound':
                self._native_inbound_seen = True
            return
        if row is not None:
            if self._preservation_retain_terminal_automation(row, handle):
                return
            if row.get('status') == 'terminal':
                coordinator = self._preservation_coordinator()
                if coordinator is not None:
                    coordinator.mark_unknown(handle, 'terminal_row_unmapped')
                return
            self._preservation_forget_pending_root(handle)
        self._preservation_release_unaccepted(handle)
    def _preservation_reconcile_failed(self, handle, *, principal_id, session_id, request_id):
        """Reconcile an admission write failure at its exact durable identity."""
        if handle is None:
            return
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return
        session_ids = [session_id]
        try:
            current = self._current_owner(session_id)
        except BaseException:  # health: allow BLE001 -- opaque owner query failure marks the exact lease unknown and retains custody
            coordinator.mark_unknown(handle, 'read_failed')
            return
        if current not in session_ids:
            session_ids.append(current)
        try:
            row = next(
                (candidate for candidate in (
                    find_session_admission(
                        self.db, principal_id=principal_id, session_id=candidate_id,
                        request_id=request_id)
                    for candidate_id in session_ids)
                 if candidate is not None),
                None,
            )
        except BaseException:  # health: allow BLE001 -- opaque admission identity query failure marks the exact lease unknown and retains custody
            coordinator.mark_unknown(handle, 'read_failed')
            return
        self._preservation_bind_or_release(row, handle)
    def _preservation_reconcile_api_failure(self, request_id, handle, *, session_id):
        self._preservation_reconcile_failed(
            handle, principal_id='api', session_id=session_id, request_id=request_id)
    def _preservation_teams_job_pending(self):
        """Keep I01 open while a durable Teams job remains recoverable after restart/adoption."""
        runtime = getattr(self.runner, '_teams_pipeline_runtime', None)
        if runtime is None:
            # False is reserved for a source that the existing adapter/plugin gate proves absent.
            # A configured Graph source whose owner-aware runtime failed to bind remains unknown.
            from gateway.config import Platform
            from gateway.run import _teams_pipeline_plugin_enabled
            adapters = getattr(self.runner, 'adapters', {}) or {}
            if (Platform.MSGRAPH_WEBHOOK not in adapters
                    or not _teams_pipeline_plugin_enabled()):
                return False
            return None
        store = getattr(runtime, 'store', None)
        if store is None:
            return None
        from plugins.teams_pipeline.store_ownership import TeamsStoreOwnershipError
        try:
            jobs = store.list_jobs().values()
        except (OSError, ValueError, KeyError, TypeError, TeamsStoreOwnershipError):
            return None
        recoverable = {
            'received', 'resolving_meeting', 'fetching_transcript', 'downloading_recording',
            'transcribing_audio', 'summarizing', 'writing_notion', 'writing_linear', 'sending_teams',
            'retry_scheduled',
        }
        return any(isinstance(job, dict) and job.get('status') in recoverable for job in jobs)
    def _preservation_reserve_claim(self, ref, first):
        if first is None:
            return None
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return None
        admission_id = first['admission_id']
        parent = self._preservation_handles.get(admission_id)
        if parent is None:
            custody = self._preservation_automation_row_descriptor(first)
            # Legacy local/operator rows retain FIFO; managed I07/I09 rows recover their exact witness.
            if not isinstance(custody, dict) or custody.get('owner_class') not in {
                    'I07_automation_route_wakes', 'I09_a2a_hosted_kanban'}:
                return None
            self._preservation_recover_automation(first)
            parent = self._preservation_handles.get(admission_id)
            if parent is None:
                raise RuntimeStoreError('preservation_owner_unmapped')
        from gateway.runtime_custody import CustodyRefusal
        owner_id = f"claim:{admission_id}"
        # RetirementFence.reserve_child is a pre-close reservation API.  Once the reversible close
        # owns the fence, an already accepted parent may acquire only this direct child with the
        # exact parent lease; it is never a new root and it cannot be admitted without that parent.
        if coordinator.state == 'closing':
            child = coordinator.reserve_root(
                'I04_fifo_turn_execution', owner_id, profile_id=self.profile_id,
                authority_epoch=self.epoch, parent=parent)
        else:
            child = coordinator.reserve_child(
                parent, 'I04_fifo_turn_execution', owner_id,
                profile_id=self.profile_id, authority_epoch=self.epoch)
        if isinstance(child, CustodyRefusal):
            raise RuntimeStoreError(child.code)
        self._preservation_children[admission_id] = child
        # Admission id is the only stable key.  Keep both logical identities for diagnostics and
        # never derive the cleanup key from ref.session_id or target_session_id interchangeably.
        self._preservation_claiming[admission_id] = {
            'session_id': ref.session_id,
            'target_session_id': first['target_session_id'],
        }
        return child
    def _preservation_drop_for_ref(self, ref, reason):
        """Find a pending child through its stored exact admission identity, never a guessed key."""
        for admission_id, identity in list(self._preservation_claiming.items()):
            if identity.get('session_id') == ref.session_id:
                self._preservation_drop_unclaimed(admission_id, reason)
    def _preservation_drop_unclaimed(self, admission_id, reason):
        """Release a child reservation when its exact durable FIFO claim did not run."""
        if admission_id is None:
            return
        self._preservation_claiming.pop(admission_id, None)
        child = self._preservation_children.get(admission_id)
        coordinator = self._preservation_coordinator()
        if child is None or coordinator is None:
            return
        if hasattr(child, 'lease'):
            result = coordinator.mark_terminal(child)
            if result is None:
                self._preservation_children.pop(admission_id, None)
            else:
                parent = self._preservation_handles.get(admission_id)
                if parent is not None:
                    coordinator.mark_unknown(parent, reason)
            return
        result = coordinator.release_child_reservation(child)
        if result is None:
            self._preservation_children.pop(admission_id, None)
            return
        parent = self._preservation_handles.get(admission_id)
        if parent is not None:
            coordinator.mark_unknown(parent, reason)
    def _preservation_activate_claim(self, admission_id, row, child):
        if child is None or admission_id is None:
            return
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return
        self._preservation_claiming.pop(admission_id, None)
        if not row:
            result = (coordinator.mark_terminal(child) if hasattr(child, 'lease')
                       else coordinator.release_child_reservation(child))
            if result is None:
                self._preservation_children.pop(admission_id, None)
            else:
                parent = self._preservation_handles.get(admission_id)
                if parent is not None:
                    coordinator.mark_unknown(parent, 'read_failed')
            return
        from gateway.runtime_custody import CustodyRefusal
        # A direct child acquired under a closing parent is already an active lease.  Only a
        # pre-close ChildReservation needs activation, and both forms remain bound to this id.
        if hasattr(child, 'lease'):
            return
        active = coordinator.activate_child(child)
        if isinstance(active, CustodyRefusal):
            parent = self._preservation_handles.get(admission_id)
            if parent is not None:
                coordinator.mark_unknown(parent, 'join_failed')
            raise RuntimeStoreError(active.code)
        self._preservation_children[admission_id] = active
    def _preservation_mark_finalizing(self, admission_id, finalizer_id):
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return
        child = self._preservation_children.get(admission_id)
        parent = self._preservation_handles.get(admission_id)
        if child is not None and hasattr(child, 'lease'):
            result = coordinator.mark_finalizing(child, f'{finalizer_id}:child')
            if result is not None and parent is not None:
                coordinator.mark_unknown(parent, 'read_failed')
        if parent is not None:
            result = coordinator.mark_finalizing(parent, finalizer_id)
            if result is not None:
                coordinator.mark_unknown(parent, 'read_failed')
    def _preservation_mark_unknown(self, admission_id, reason):
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return
        retryable_delivery = reason in {'delivery_failed', 'delivery_finalize_failed'}
        for entry in self._preservation_sidecars.values():
            if entry.get('admission_id') != admission_id:
                continue
            service = entry.get('service')
            request = entry.get('request')
            if retryable_delivery and service is not None and request is not None:
                from gateway.runtime_service_custody import ServiceCustodyError
                try:
                    service.mark_delivery_unknown(service.binding, request, reason)
                except ServiceCustodyError:
                    # A failed typed transition is itself retained as generic unknown; no
                    # accepted or copied handle may be reopened by a partial mark.
                    physical = getattr(service, 'physical', None)
                    if physical is not None:
                        physical.mark_unknown('delivery_reconcile_failed')
                        coordinator.mark_unknown(physical.handle, 'join_failed')
                    coordinator.mark_unknown(request, 'join_failed')
                    sidecar_parent = entry.get('parent')
                    if sidecar_parent is not None and hasattr(sidecar_parent, 'lease'):
                        coordinator.mark_unknown(sidecar_parent, 'join_failed')
                continue
            physical = getattr(service, 'physical', None)
            if physical is not None:
                physical.mark_unknown(reason)
                # The process receipt's diagnostic state is not the custody ledger. Mark the
                # exact native handle too, so reconciliation cannot reopen an accepted record.
                coordinator.mark_unknown(physical.handle, reason)
            if request is not None and hasattr(request, 'lease'):
                coordinator.mark_unknown(request, reason)
            sidecar_parent = entry.get('parent')
            if sidecar_parent is not None and hasattr(sidecar_parent, 'lease'):
                coordinator.mark_unknown(sidecar_parent, reason)
        child = self._preservation_children.get(admission_id)
        parent = self._preservation_handles.get(admission_id)
        if child is not None and hasattr(child, 'lease'):
            coordinator.mark_unknown(child, reason)
        # A typed delivery failure retains the I01 root as an accepted parent while its exact
        # I04 child/request/physical tuple is reconciled. Other uncertainty remains root-unknown.
        if parent is not None and not retryable_delivery:
            coordinator.mark_unknown(parent, reason)
    def _preservation_bind_sidecar(self, admission_id, parent, service, request):
        """Attach one sidecar request to its exact active FIFO child before physical work."""
        coordinator = self._preservation_coordinator()
        if coordinator is None or admission_id is None or parent is None or request is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if getattr(parent, 'owner_class', None) != 'I04_fifo_turn_execution':
            raise RuntimeStoreError('preservation_owner_mismatch')
        if getattr(parent, 'profile_id', None) != self.profile_id or getattr(parent, 'authority_epoch', None) != self.epoch:
            raise RuntimeStoreError('preservation_owner_mismatch')
        request_id = getattr(getattr(request, 'lease', None), 'lease_id', None)
        if not isinstance(request_id, str) or not request_id:
            raise RuntimeStoreError('preservation_owner_unmapped')
        existing = self._preservation_sidecars.get(request_id)
        if existing is not None and existing['request'] is not request:
            raise RuntimeStoreError('duplicate_service_request')
        self._preservation_sidecars[request_id] = {
            'admission_id': admission_id, 'parent': parent, 'service': service, 'request': request,
            'result_durable': False, 'delivery_complete': False, 'terminal': False,
        }
    def _preservation_release_sidecar(self, admission_id, request, *, durable):
        """Stage result durability; delivery and physical close remain separate fences."""
        request_id = getattr(getattr(request, 'lease', None), 'lease_id', None)
        entry = self._preservation_sidecars.get(request_id)
        if entry is None or entry['request'] is not request or entry['admission_id'] != admission_id:
            raise RuntimeStoreError('stale_service_request')
        if not durable:
            self._preservation_mark_unknown(admission_id, 'join_failed')
            return
        entry['result_durable'] = True

    def _preservation_retire_sidecar(self, admission_id, request):
        """Stage the exact source request for the canonical service forget fence."""
        from gateway.runtime_service_operations import retire_cua_sidecar
        return retire_cua_sidecar(self, admission_id, request)

    def _preservation_forget_sidecar(self, admission_id, request):
        """Forget only a source request whose exact terminal fence was recorded."""
        from gateway.runtime_service_operations import forget_cua_sidecar
        return forget_cua_sidecar(self, admission_id, request)

    def _preservation_has_sidecar(self, admission_id):
        return any(entry['admission_id'] == admission_id for entry in self._preservation_sidecars.values())

    def _preservation_complete_sidecar_delivery(self, admission_id):
        from gateway.session_ingress import preservation_delivery_outcome
        return preservation_delivery_outcome(self, admission_id) == 'complete'

    async def _preservation_retry_delivery(self, admission_id, reason, *, effect_state=None):
        from gateway.session_ingress import retry_preservation_delivery
        return await retry_preservation_delivery(
            self, admission_id, reason, effect_state=effect_state,
        )

    def _preservation_delivery_ready(self, admission_id):
        adopter = getattr(self, 'marker_adopters', {}).get(admission_id)
        marker = getattr(adopter, '_gateway_marker_released', None)
        return marker is None or marker.is_set()

    async def _preservation_wait_marker(self, admission_id, marker):
        from gateway.session_classic_delivery import wait_for_classic_marker_release
        await wait_for_classic_marker_release(self, admission_id, marker)

    def _preservation_watch_marker(self, admission_id, marker):
        if admission_id in self._preservation_marker_watchers:
            return
        from gateway.session_runtime_workers import track_mutation
        try:
            task = track_mutation(self, self._preservation_wait_marker(admission_id, marker))
        except BaseException:  # health: allow BLE001 -- opaque marker watcher registration failure retains the exact lease as unknown
            self._preservation_mark_unknown(admission_id, 'join_failed')
            return
        self._preservation_marker_watchers[admission_id] = task

    def preservation_owner_ready(self, owner_class):
        if owner_class == 'I09_a2a_hosted_kanban':
            # The hosted service has a source-owned inventory and ACK/readback contract. Keep the
            # shared API/I10/I17 readiness helper intact for every other owner class; an empty
            # generic authority map cannot substitute for the hosted producer's exact evidence.
            from gateway.session_hosted_service import preservation_i09_owner_ready
            coordinator = self._preservation_coordinator()
            if coordinator is None:
                return False
            custody = coordinator.status()
            if (owner_class in custody.get('active_owner_classes', ())
                    or owner_class in custody.get('unknown_owner_classes', ())):
                return False
            return preservation_i09_owner_ready(self, custody)
        from gateway.session_authority_readiness import preservation_owner_ready
        return preservation_owner_ready(self, owner_class)
