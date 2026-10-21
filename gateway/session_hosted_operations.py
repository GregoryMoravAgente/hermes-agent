"""Hosted-room admission operations for the gateway session authority.

This mixin contains the exact hosted-operation helpers moved from
``session_authority_operations.py`` to keep that module below the source-size ratchet.
It owns no authority or ledger; SessionAuthority supplies the existing generic helpers
through its MRO.
"""
from __future__ import annotations

import json
import sqlite3

from hermes_state_runtime import RuntimeStoreError


class HostedOperationsMixin:
    def _preservation_operation_validate_terminal_row(self, record, row, admission_id):
        """Validate an exact terminal row without borrowing a newer live execution."""
        if (not isinstance(row, dict) or row.get('status') != 'terminal'
                or row.get('outcome') not in {
                    'completed', 'failed', 'interrupted', 'rejected', 'cancelled', 'steered',
                }
                or not self._preservation_operation_row_identity_matches(
                    record, row, admission_id)):
            return None
        generation_info = self._preservation_operation_row_generation_info(
            record, row, admission_id)
        if generation_info is None:
            return None
        state, row_generation, binding = generation_info
        # A terminal row is its own durable execution witness.  The current session may already
        # be running a later FIFO item, so the live-execution check used for queued/started rows
        # would incorrectly reject an authenticated hosted callback retry.
        self._preservation_operation_pin_row(record, state, row_generation, binding)
        return row

    def _preservation_operation_validate_hosted_row(self, record, row, admission_id):
        """Validate a hosted root's queued row while another FIFO item may be running."""
        if (isinstance(row, dict) and row.get('status') == 'queued'
                and row.get('generation') is None
                and self._preservation_operation_row_identity_matches(
                    record, row, admission_id)):
            generation_info = self._preservation_operation_row_generation_info(
                record, row, admission_id)
            if generation_info is not None:
                state, row_generation, binding = generation_info
                self._preservation_operation_pin_row(record, state, row_generation, binding)
                return row
        if isinstance(row, dict) and row.get('status') == 'terminal':
            return self._preservation_operation_validate_terminal_row(record, row, admission_id)
        return self._preservation_operation_validate_row(record, row, admission_id)

    def _preservation_operation_reconcile_exact_admission(
            self, key, admission_id, *, source_identity=None,
    ):
        """Reopen one retained operation only against its exact durable admission row."""
        if key is None or not isinstance(admission_id, str) or not admission_id:
            raise RuntimeStoreError('preservation_owner_unmapped')
        record = self._preservation_operations.get(key)
        if not isinstance(record, dict):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if (record.get('owner_class') != 'I09_a2a_hosted_kanban'
                or (source_identity is not None
                    and record.get('source_identity') != source_identity)):
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        current = record.get('admission_id')
        if current is not None and current != admission_id:
            self._preservation_operation_unknown(key, 'read_failed')
            raise RuntimeStoreError('preservation_owner_unmapped')
        from hermes_state_runtime import get_session_admission
        try:
            row = get_session_admission(self.db, admission_id=admission_id)
        except (OSError, RuntimeError, sqlite3.Error, KeyError, TypeError, ValueError) as exc:
            self._preservation_operation_unknown(key, 'read_failed')
            raise RuntimeStoreError('storage_unavailable') from exc
        if row is None or row.get('status') == 'unknown':
            raise RuntimeStoreError('unknown_execution')
        if record.get('owner_class') == 'I09_a2a_hosted_kanban':
            valid = self._preservation_operation_validate_hosted_row(record, row, admission_id)
        elif row.get('status') == 'terminal':
            valid = self._preservation_operation_validate_terminal_row(record, row, admission_id)
        else:
            valid = self._preservation_operation_validate_row(record, row, admission_id)
        if valid is None:
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        record['admission_id'] = admission_id
        admission_ids = record.setdefault('admission_ids', [])
        if admission_id not in admission_ids:
            admission_ids.append(admission_id)
        if record.get('unknown'):
            coordinator = self._preservation_coordinator()
            if coordinator is None:
                coordinator = self._require_preservation_coordinator()
            handle = record.get('handle')
            refusal = coordinator.reconcile_unknown(
                handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
                process_instance=handle.process_instance, authority_epoch=handle.authority_epoch)
            if refusal is not None:
                raise RuntimeStoreError('admission_conflict')
            record['unknown'] = False
        return row

    def _preservation_operation_retry_without_admission(
            self, key, *, owner_class, principal_id, session_id, operation,
            request_id, source_identity,
    ):
        """Reopen one exact pre-admission producer lease after an opaque create failure.

        Hosted creation has no FIFO admission to bind.  Its durable session receipt is the
        later target witness, so a retry must authenticate the retained operation tuple and
        coordinator handle before it resumes that same request.  It may not mint a second root
        from the deterministic request id.
        """
        if key is None:
            return None
        record = self._preservation_operations.get(key)
        expected = {
            'owner_class': owner_class,
            'principal_id': principal_id,
            'session_id': session_id,
            'operation': operation,
            'request_id': request_id,
            'source_identity': source_identity,
        }
        if (not isinstance(record, dict)
                or any(record.get(name) != value for name, value in expected.items())
                or record.get('admission_id') is not None
                or record.get('admission_ids') not in (None, [])):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if not record.get('unknown'):
            return key
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            coordinator = self._require_preservation_coordinator()
        handle = record.get('handle')
        if handle is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        refusal = coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
            process_instance=handle.process_instance, authority_epoch=handle.authority_epoch,
        )
        if refusal is not None:
            raise RuntimeStoreError(getattr(refusal, 'code', 'admission_conflict'))
        record['unknown'] = False
        return key

    def _preservation_operation_terminal_after_exact_session(
            self, key, *, session_id, principal_id, request_id, source_identity,
            finalizer_id, source='bot_room', title=None,
    ):
        """Release a hosted-create root only after its exact local receipt is durable."""
        if key is None:
            return None
        record = self._preservation_operations.get(key)
        if (not isinstance(record, dict)
                or record.get('owner_class') != 'I09_a2a_hosted_kanban'
                or record.get('profile_id') != self.profile_id
                or record.get('authority_epoch') != self.epoch
                or record.get('principal_id') != principal_id
                or record.get('session_id') != session_id
                or record.get('request_id') != request_id
                or record.get('source_identity') != source_identity
                or record.get('admission_id') is not None):
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        try:
            row = self.db.get_session(session_id)
            try:
                source_fields = json.loads(source_identity)
            except (TypeError, ValueError):
                self._preservation_operation_unknown(key, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
            expected_profile = source_fields.get('profile') if isinstance(source_fields, dict) else None
            if not isinstance(expected_profile, str) or not expected_profile:
                self._preservation_operation_unknown(key, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
            if (not isinstance(row, dict)
                    or row.get('id') != session_id
                    # Older/profile-local stores may leave this auxiliary label NULL.  The
                    # receipt below is the authoritative profile binding; reject a present
                    # label from another owner, while retaining compatibility with a row whose
                    # own store predates profile_name stamping.
                    or (row.get('profile_name') is not None
                        and row.get('profile_name') != expected_profile)
                    or row.get('user_id') != principal_id
                    or row.get('source') != source
                    or (title is not None and row.get('title') != title)
                    or not bool(row.get('hidden'))):
                raise RuntimeStoreError('preservation_owner_unmapped')
            from hermes_state_local import local_receipt
            receipt = local_receipt(self.db, session_id)
            entry = receipt.get('entry') if isinstance(receipt, dict) else None
            if (receipt.get('profile_id') != self.profile_id
                    or receipt.get('principal_id') != principal_id
                    or receipt.get('request_id') != request_id
                    or receipt.get('session_id') != session_id
                    or not isinstance(entry, dict)
                    or entry.get('session_id') != session_id):
                raise RuntimeStoreError('preservation_owner_unmapped')
        except RuntimeStoreError:
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise
        except (OSError, sqlite3.Error, KeyError, TypeError, ValueError) as exc:
            self._preservation_operation_unknown(key, 'read_failed')
            raise RuntimeStoreError('storage_unavailable') from exc
        self._preservation_operation_terminal(key, finalizer_id)
        if key in self._preservation_operations:
            raise RuntimeStoreError('custody_pending')
        return row

    @staticmethod
    def _preservation_room_input_source_identity(
        *, room_id, event_id, gateway_id, authority_epoch,
    ):
        """Return the immutable identity for one hosted-room input admission.

        A room message is accepted before a session admission exists.  Its durable room event
        is the source-owned receipt, so the operation uses the room/event/authority tuple as its
        request identity and never borrows a session row or a caller-supplied handle.
        """
        return json.dumps({
            'service': 'hosted_room_input.v1',
            'room_id': str(room_id),
            'event_id': str(event_id),
            'gateway_id': str(gateway_id),
            'authority_epoch': int(authority_epoch),
            'actor': {'kind': 'user', 'id': 'desktop'},
        }, sort_keys=True, separators=(',', ':'))

    def _preservation_room_input_start(
        self, *, room_id, event_id, gateway_id, authority_epoch,
    ):
        """Reserve one hosted input before validation or its first durable room effect."""
        source_identity = self._preservation_room_input_source_identity(
            room_id=room_id, event_id=event_id, gateway_id=gateway_id,
            authority_epoch=authority_epoch,
        )
        session_id = f'room:{room_id}'
        principal_id = 'desktop'
        operation = 'hosted-room-input'
        key = self._preservation_operation_identity(
            'I09_a2a_hosted_kanban', profile_id=self.profile_id,
            authority_epoch=self.epoch, session_id=session_id,
            principal_id=principal_id, operation=operation, request_id=event_id,
            generation=None, source_identity=source_identity,
        )
        records = getattr(self, '_preservation_operations', {})
        if key in records:
            # A close-race retry must reopen the original opaque handle against the exact
            # room/event tuple.  It may never mint a replacement root after close.
            self._preservation_operation_retry_without_admission(
                key, owner_class='I09_a2a_hosted_kanban', principal_id=principal_id,
                session_id=session_id, operation=operation, request_id=event_id,
                source_identity=source_identity,
            )
            return key, source_identity
        key = self._preservation_operation_start(
            'I09_a2a_hosted_kanban', principal_id=principal_id, session_id=session_id,
            operation=operation, request_id=event_id, generation=None,
            source_identity=source_identity,
        )
        return key, source_identity

    def _preservation_room_input_terminal(
        self, key, *, room_id, event_id, source_identity, finalizer_id,
    ):
        """Retire a room-input handle after the exact durable task publication is visible."""
        if key is None:
            return
        record = self._preservation_operations.get(key)
        expected = {
            'owner_class': 'I09_a2a_hosted_kanban',
            'profile_id': self.profile_id,
            'authority_epoch': self.epoch,
            'principal_id': 'desktop',
            'session_id': f'room:{room_id}',
            'operation': 'hosted-room-input',
            'request_id': event_id,
            'source_identity': source_identity,
        }
        if (
            not isinstance(record, dict)
            or any(record.get(field) != value for field, value in expected.items())
            or record.get('admission_id') is not None
        ):
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        self._preservation_operation_terminal(key, finalizer_id)
        if key in self._preservation_operations:
            raise RuntimeStoreError('custody_pending')

    def _preservation_operation_terminal_after_exact_admission(
            self, key, admission_id, finalizer_id, *, source_identity=None,
    ):
        """Release hosted custody only after the exact durable terminal row is re-read."""
        row = self._preservation_operation_reconcile_exact_admission(
            key, admission_id, source_identity=source_identity)
        if row.get('status') != 'terminal':
            raise RuntimeStoreError('custody_pending')
        self._preservation_operation_terminal(key, finalizer_id)
        if key in self._preservation_operations:
            # ``_preservation_operation_terminal`` retains an ambiguous or refused lease as
            # UNKNOWN. The caller must retry this exact finalizer; it may not report success.
            raise RuntimeStoreError('custody_pending')
        return row
