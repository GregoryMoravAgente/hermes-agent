"""Lease-backed operation helpers for the gateway session authority.

The mixin keeps the I03/I05/I06/I11 operation bookkeeping topical while the
authority retains the newer I01 scheduler and recovery seams.  It owns no
independent authority: every handle is issued and settled by the runner's
RuntimeCustodyCoordinator.
"""
from __future__ import annotations

import asyncio
import sqlite3

from hermes_state_runtime import RuntimeStoreError


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
        self, session_id, admission_id=None, *, generation=None, principal_id=None, queued_control=False
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
            if principal_id is not None and row.get('principal_id') != principal_id:
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

    def _preservation_operation_live_row_matches(self, record, admission_id, row_generation):
        if (row_generation is None and record.get('owner_class') == 'I05_controls_and_unknown'
                and record.get('operation') in {'cancel', 'stop'}):
            # The identity-checked queued target has no execution. Its cancellation must not
            # borrow, interrupt, or be vetoed by another admission's live generation.
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

    def _preservation_operation_validate_row(self, record, row, admission_id):
        """Validate one exact durable row and pin its committed generation once."""
        if not self._preservation_operation_row_identity_matches(record, row, admission_id):
            return None
        generation_info = self._preservation_operation_row_generation_info(
            record, row, admission_id)
        if generation_info is None:
            return None
        state, row_generation, binding = generation_info
        if not self._preservation_operation_live_row_matches(record, admission_id, row_generation):
            return None
        self._preservation_operation_pin_row(record, state, row_generation, binding)
        return row

    def _preservation_operation_bind_admission(self, key, admission_id):
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
        if self._preservation_operation_validate_row(record, row, admission_id) is None:
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
    def _preservation_operation_row(self, key):
        """Read the exact admission identity, never a session-wide status census."""
        record = self._preservation_operations.get(key)
        if record is None:
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
                    record, row, row.get('admission_id')) is None):
            self._preservation_operation_unknown(key, 'read_failed')
            return None
        if not self._preservation_operation_bind_admission(key, row.get('admission_id')):
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
        if parent_owner not in {'I04_fifo_turn_execution', 'I07_automation_route_wakes'}:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if (getattr(parent_handle, 'profile_id', None) != profile_id
                or getattr(parent_handle, 'authority_epoch', None) != epoch
                or getattr(parent_handle, 'process_instance', None)
                != getattr(coordinator, 'process_instance', None)
                or getattr(parent_handle, 'process_instance', None) != instance_id
                or getattr(coordinator, 'process_instance', None) != instance_id
                or getattr(parent_handle, 'coordinator_id', None)
                != getattr(coordinator, '_coordinator_id', None)
                or getattr(coordinator, '_parent', lambda _handle: None)(parent_handle) is None):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return parent_handle

    def _preservation_operation_start(self, owner_class, *, principal_id, session_id, operation,
                                      request_id=None, admission_id=None, generation=None,
                                      source_identity=None, parent_handle=None):
        """Reserve an I03/I05/I06/I11 operation before its first await/task/callback."""
        coordinator = self._require_preservation_coordinator()
        if coordinator is None:
            # Bare adapters have no managed authority; an explicit parent still requires custody.
            if parent_handle is not None:
                raise RuntimeStoreError('preservation_owner_unmapped')
            return None
        validated_parent = self._preservation_operation_validate_parent_handle(
            parent_handle, coordinator, self.profile_id, self.epoch, self.instance_id)
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
            if not self._preservation_operation_matches(
                    existing, owner_class=owner_class, principal_id=principal_id,
                    session_id=session_id, operation=operation, request_id=request_id,
                    admission_id=admission_id, generation=generation, source_identity=source_identity):
                raise RuntimeStoreError('preservation_owner_unmapped')
            # Same-key retries with an explicit parent must use the exact retained object; a
            # copied/replaced handle is not an equivalent authority proof.
            if parent_handle is not None and existing.get('parent_handle') is not parent_handle:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if self._preservation_operation_current_admission(existing) is None:
                raise RuntimeStoreError('stale_generation')
            if not existing.get('unknown'):
                return key
            return self._preservation_operation_retry(
                key, existing, owner_class=owner_class, principal_id=principal_id,
                session_id=session_id, operation=operation, request_id=request_id,
                admission_id=admission_id, generation=generation, source_identity=source_identity)
        admission_request_id = self._preservation_operation_admission_request_id(
            owner_class, principal_id, session_id, admission_id, generation, request_id)
        from gateway.runtime_custody import CustodyRefusal
        if parent_handle is not None:
            parent = validated_parent
        else:
            parent = self._preservation_operation_parent(
                session_id, admission_id, generation=generation, principal_id=principal_id,
                queued_control=owner_class == 'I05_controls_and_unknown' and operation in {'cancel', 'stop'})
        if parent is None and owner_class in {
            'I03_api_run_direct', 'I07_automation_route_wakes',
            'I08_goals_loops_heartbeat', 'I09_a2a_hosted_kanban',
            'I11_session_mutations_finalizers',
        }:
            # A direct authenticated session.mutate is the one source-owned I11 entrypoint that
            # may start a fresh root. It is allowed only while the coordinator is still open;
            # callbacks/finalizers and API runs must remain children of an exact parent lease.
            standalone_mutation = (
                owner_class == 'I11_session_mutations_finalizers'
                and source_identity == 'direct_authenticated_mutation'
                and admission_id is None
            )
            producer_root = owner_class in _AUTOMATION_OWNER_CLASSES
            if not standalone_mutation and not producer_root:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if coordinator.state != 'open':
                raise RuntimeStoreError('runtime_draining')
        elif parent is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if parent is None:
            handle = coordinator.reserve_root(
                owner_class, key, profile_id=self.profile_id, authority_epoch=self.epoch)
        elif coordinator.state == 'closing':
            handle = coordinator.reserve_root(
                owner_class, key, profile_id=self.profile_id, authority_epoch=self.epoch, parent=parent)
        else:
            reservation = coordinator.reserve_child(
                parent, owner_class, key, profile_id=self.profile_id, authority_epoch=self.epoch)
            handle = (coordinator.activate_child(reservation)
                      if not isinstance(reservation, CustodyRefusal) else reservation)
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
            'parent_handle': parent,
            'unknown': False,
        }
        return key

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
    def _preservation_operation_terminal(self, key, finalizer_id):
        if key is None:
            return
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        if record is None:
            return
        if coordinator is None:
            self._require_preservation_coordinator()
            return
        finalized = coordinator.mark_finalizing(record['handle'], finalizer_id)
        if finalized is not None:
            coordinator.mark_unknown(record['handle'], 'read_failed')
            return
        result = coordinator.mark_terminal(record['handle'])
        if result is None:
            self._preservation_operations.pop(key, None)
        elif result.code != 'stale_lease':
            coordinator.mark_unknown(record['handle'], 'lease_release_failed')
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
