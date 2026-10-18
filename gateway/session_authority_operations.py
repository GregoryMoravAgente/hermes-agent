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


class PreservationOperationsMixin:
    def _preservation_operation_parent(
        self, session_id, admission_id=None, *, generation=None, principal_id=None
    ):
        """Return the exact active parent lease for a real operation.

        An explicit admission id is authoritative.  A late callback may not fall back to the
        live execution entry when that entry names a different claim, and an implicit lookup is
        accepted only after the durable row agrees with the session, principal, and generation.
        """
        live = self.sessions.get(session_id)
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        execution_admission = execution.get('admission_id')
        if admission_id is not None and execution_admission not in (None, admission_id):
            return None
        candidates = (admission_id,) if admission_id is not None else (execution_admission,)
        from hermes_state_runtime import get_session_admission
        for current in candidates:
            if not current:
                continue
            row = get_session_admission(self.db, admission_id=current)
            if row is None or row.get('target_session_id') != session_id:
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
    def _preservation_operation_bind_admission(self, key, admission_id):
        """Bind a real native receipt to the operation that reserved before admission."""
        if key is None or not isinstance(admission_id, str) or not admission_id:
            return
        record = self._preservation_operations.get(key)
        if record is None:
            return
        current = record.get('admission_id')
        if current is not None and current != admission_id:
            self._preservation_operation_unknown(key, 'read_failed')
            return
        record['admission_id'] = admission_id
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
                or row.get('request_id') != record['request_id']):
            self._preservation_operation_unknown(key, 'read_failed')
            return None
        self._preservation_operation_bind_admission(key, row.get('admission_id'))
        return row
    def _preservation_operation_start(self, owner_class, *, principal_id, session_id, operation,
                                      request_id=None, admission_id=None, generation=None,
                                      source_identity=None):
        """Reserve an I03/I05/I06/I11 operation before its first await/task/callback."""
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            return None
        key = self._preservation_operation_identity(
            owner_class, profile_id=self.profile_id, authority_epoch=self.epoch,
            session_id=session_id, principal_id=principal_id,
            operation=operation, request_id=request_id, admission_id=admission_id,
            generation=generation, source_identity=source_identity)
        existing = self._preservation_operations.get(key)
        if existing is not None:
            # An unresolved exact retry keeps the original custody. Reusing the same handle for a
            # second execution would make two callbacks look like one terminal operation.
            raise RuntimeStoreError('admission_conflict')
        from gateway.runtime_custody import CustodyRefusal
        parent = self._preservation_operation_parent(
            session_id, admission_id, generation=generation, principal_id=principal_id)
        if parent is None and owner_class in {
            'I03_api_run_direct', 'I11_session_mutations_finalizers',
        }:
            # A direct authenticated session.mutate is the one source-owned I11 entrypoint that
            # may start a fresh root. It is allowed only while the coordinator is still open;
            # callbacks/finalizers and API runs must remain children of an exact parent lease.
            standalone_mutation = (
                owner_class == 'I11_session_mutations_finalizers'
                and source_identity == 'direct_authenticated_mutation'
                and admission_id is None
            )
            if not standalone_mutation:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if coordinator.state != 'open':
                raise RuntimeStoreError('runtime_draining')
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
            'principal_id': principal_id,
            'session_id': session_id,
            'request_id': request_id,
            'admission_id': admission_id,
            'generation': generation,
            'source_identity': source_identity,
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
        if record is not None and coordinator is not None:
            coordinator.mark_unknown(record['handle'], reason)
    def _preservation_operation_release_unaccepted(self, key):
        if key is None:
            return
        record = self._preservation_operations.get(key)
        coordinator = self._preservation_coordinator()
        if record is None or coordinator is None:
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
        if record is None or coordinator is None:
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
    async def _preservation_control_call(self, *, principal_id, session_id, operation,
                                         request_id=None, admission_id=None, generation=None,
                                         call):
        """Run one authenticated control and retain custody across cancellation/late failure."""
        key = self._preservation_operation_start(
            'I05_controls_and_unknown', principal_id=principal_id, session_id=session_id,
            operation=operation, request_id=request_id, admission_id=admission_id,
            generation=generation)
        try:
            result = await call()
        except asyncio.CancelledError:
            self._preservation_operation_unknown(key, 'join_failed')
            raise
        except RuntimeStoreError as exc:
            if exc.reason in {
                'invalid_params', 'not_found', 'permission_denied', 'profile_mismatch',
                'runtime_draining', 'session_busy', 'stale_generation', 'unsupported_control',
                'execution_not_ready', 'admission_conflict',
            }:
                self._preservation_operation_release_unaccepted(key)
            else:
                self._preservation_operation_unknown(key, 'read_failed')
            raise
        except Exception:  # health: allow BLE001 -- control callback failure retains exact custody unknown
            self._preservation_operation_unknown(key, 'join_failed')
            raise
        self._preservation_operation_terminal(key, f'control:{operation}:{request_id or admission_id or session_id}')
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

