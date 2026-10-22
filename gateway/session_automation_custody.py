"""Durable identity helpers for trusted automation admissions.

The coordinator still owns every lease.  This module only canonicalizes the source-owned tuple
that is stamped into an existing FIFO payload so a successor can authenticate the same row.
"""
import asyncio
from functools import partial
import hashlib
import json
from pathlib import Path

from hermes_state_runtime import RuntimeStoreError, admit_session_input


_KANBAN_OCCURRENCE_FIELDS = frozenset({
    'schema', 'board', 'task_id', 'old_cursor', 'claimed_cursor', 'event_ids',
})
_KANBAN_OCCURRENCE_COMPACT_FIELDS = frozenset({
    'schema', 'board', 'task_id', 'old_cursor', 'claimed_cursor',
    'event_count', 'event_ids_digest',
})
_KANBAN_OCCURRENCE_SCHEMA = 'kanban_task_events.v1'
_AUTOMATION_OWNER_CLASSES = frozenset({
    'I07_automation_route_wakes',
    'I08_goals_loops_heartbeat',
    'I09_a2a_hosted_kanban',
})


def _kanban_occurrence_digest(board, task_id, old_cursor, claimed_cursor, event_ids):
    """Hash the complete ordered producer claim without carrying it into the durable payload."""
    raw = json.dumps({
        'schema': _KANBAN_OCCURRENCE_SCHEMA,
        'board': board,
        'task_id': task_id,
        'old_cursor': old_cursor,
        'claimed_cursor': claimed_cursor,
        'event_ids': event_ids,
    }, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _valid_event_digest(value):
    return (isinstance(value, str) and len(value) == 64
            and all(character in '0123456789abcdef' for character in value))


def _automation_generation_valid(row, *, allow_terminal=False, allow_preclaim_cancel=False):
    """Validate the durable row's execution-generation state before any successor effect."""
    if not isinstance(row, dict):
        return False
    status = row.get('status')
    generation = row.get('generation')
    if status == 'queued':
        return generation is None
    terminal_witness = (allow_terminal and status == 'terminal'
                        and row.get('outcome') in {
                            'completed', 'failed', 'interrupted', 'rejected', 'cancelled', 'steered'})
    if status in {'started', 'unknown'}:
        return type(generation) is int and generation >= 1
    if terminal_witness and type(generation) is int and generation >= 1:
        return True
    # A queued producer can be cancelled before an I04 claim.  This is a callback-only witness,
    # never an admission/recovery generation and never a generic terminal-row waiver.
    return (
        allow_preclaim_cancel
        and terminal_witness
        and row.get('outcome') == 'cancelled'
        and generation is None
    )


def _automation_descriptor_complete(descriptor):
    if not isinstance(descriptor, dict):
        return False
    for field in ('owner_class', 'owner_id', 'profile_id', 'profile_home', 'process_instance',
                  'coordinator_id', 'session_id', 'principal_id', 'request_id', 'source_identity'):
        if not isinstance(descriptor.get(field), str) or not descriptor[field]:
            return False
    if (type(descriptor.get('authority_epoch')) is not int or descriptor['authority_epoch'] < 1
            or type(descriptor.get('close_generation')) is not int or descriptor['close_generation'] < 0):
        return False
    delivery_id = descriptor.get('delivery_id')
    return delivery_id is None or (isinstance(delivery_id, str) and bool(delivery_id))


def normalize_kanban_occurrence(value):
    """Validate the source-owned occurrence of one claimed Kanban wake.

    Event ids and claim cursors are the durable producer witness.  Text, event kinds, and
    the rendered card are deliberately not identity material because two distinct events can
    render identically.  ``None`` means this is a legacy/non-Kanban route and preserves the
    established generic wake identity.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeStoreError('invalid_params')
    compact = set(value) == _KANBAN_OCCURRENCE_COMPACT_FIELDS
    if not compact and set(value) != _KANBAN_OCCURRENCE_FIELDS:
        raise RuntimeStoreError('invalid_params')
    if value.get('schema') != _KANBAN_OCCURRENCE_SCHEMA:
        raise RuntimeStoreError('invalid_params')
    board, task_id = value.get('board'), value.get('task_id')
    if (not isinstance(board, str) or not board.strip()
            or not isinstance(task_id, str) or not task_id.strip()):
        raise RuntimeStoreError('invalid_params')
    old_cursor, claimed_cursor = value.get('old_cursor'), value.get('claimed_cursor')
    if (type(old_cursor) is not int or old_cursor < 0
            or type(claimed_cursor) is not int or claimed_cursor < old_cursor):
        raise RuntimeStoreError('invalid_params')
    if compact:
        event_count = value.get('event_count')
        event_ids_digest = value.get('event_ids_digest')
        if (type(event_count) is not int or event_count < 1
                or not _valid_event_digest(event_ids_digest)):
            raise RuntimeStoreError('invalid_params')
    else:
        event_ids = value.get('event_ids')
        if (not isinstance(event_ids, list) or not event_ids
                or any(type(event_id) is not int or event_id <= 0 for event_id in event_ids)
                or event_ids != sorted(set(event_ids))):
            raise RuntimeStoreError('invalid_params')
        event_count = len(event_ids)
        event_ids_digest = _kanban_occurrence_digest(
            board.strip(), task_id.strip(), old_cursor, claimed_cursor, event_ids)
    return {
        'schema': _KANBAN_OCCURRENCE_SCHEMA,
        'board': board.strip(),
        'task_id': task_id.strip(),
        'old_cursor': old_cursor,
        'claimed_cursor': claimed_cursor,
        'event_count': event_count,
        'event_ids_digest': event_ids_digest,
    }


def kanban_occurrence_metadata(metadata):
    """Return the validated occurrence field, or no field for legacy producer metadata."""
    if not isinstance(metadata, dict) or 'kanban_occurrence' not in metadata:
        return {}
    occurrence = normalize_kanban_occurrence(metadata.get('kanban_occurrence'))
    if occurrence is None:
        raise RuntimeStoreError('invalid_params')
    return {'kanban_occurrence': occurrence}


def automation_source_identity(event, route):
    source = event.source.to_dict() if hasattr(event.source, 'to_dict') else {}
    source = {key: value for key, value in source.items() if key != 'message_id'}
    raw = json.dumps({'route': route, 'source': source}, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def route_wake_identity(producer, route, text, metadata=None):
    """Return the same request id for a retried producer wake."""
    normalized = dict(metadata) if isinstance(metadata, dict) else metadata or {}
    if isinstance(normalized, dict) and 'kanban_occurrence' in normalized:
        if producer != 'kanban-wake':
            raise RuntimeStoreError('invalid_params')
        normalized['kanban_occurrence'] = normalize_kanban_occurrence(normalized['kanban_occurrence'])
    raw = json.dumps({'producer': producer, 'route': route, 'text': text, 'metadata': normalized},
                     sort_keys=True, separators=(',', ':'), default=str)
    digest = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    # Keep the established producer-first envelope: route migration and heartbeat/completion
    # consumers use element zero to distinguish their own wake family.  The final digest replaces
    # the old random token, so the same source tuple deduplicates after a lost acknowledgement.
    return json.dumps([producer, route, digest], separators=(',', ':'))


def row_descriptor(row):
    payload = row.get('payload') if isinstance(row, dict) else None
    if not isinstance(payload, dict):
        return None
    local = payload.get('local_automation_v1')
    if isinstance(local, dict) and isinstance(local.get('custody'), dict):
        return local['custody']
    native = payload.get('native_text_v1')
    automation = native.get('automation') if isinstance(native, dict) else None
    if isinstance(automation, dict) and isinstance(automation.get('custody'), dict):
        return automation['custody']
    # I09 A2A/Kanban uses a private, source-owned carrier because the public Submission
    # allowlist must remain closed.  It is accepted only from the internal typed writer;
    # generic client submission never admits this key.
    direct = payload.get('_automation_custody_v1')
    if isinstance(direct, dict):
        return direct
    return None



class AutomationCustodyMixin:
    """Session-authority custody methods for automation roots and delivery children.

    This is a topical mixin only: RuntimeCustodyCoordinator remains the sole owner ledger,
    while SessionAuthority supplies the profile/session indexes consumed by these methods.
    """
    def _preservation_automation_root(self, *, request_id, principal_id, session_id,
                                      source_identity, delivery_id=None,
                                      owner_class='I07_automation_route_wakes'):
        """Reserve one automation root before its producer creates any side effect.

        The durable admission is the handoff record.  This method only obtains the opaque
        coordinator lease and records its exact producer tuple until the ordered writer returns;
        it does not create a second ownership ledger.
        """
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                # Bare legacy adapters remain functional, but managed automation cannot
                # reserve effects without the source-owned coordinator.
                return None
        values = (request_id, principal_id, session_id, source_identity, owner_class)
        if any(not isinstance(value, str) or not value for value in values):
            raise RuntimeStoreError('invalid_params')
        if owner_class not in _AUTOMATION_OWNER_CLASSES:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if owner_class == 'I08_goals_loops_heartbeat':
            from gateway.session_goal_tick_custody import mark_source_observed
            mark_source_observed(self, 'automation-root')
        root_key = delivery_id or request_id
        existing = self._preservation_automation_roots.get(root_key)
        coordinator = self._preservation_coordinator()
        if existing is not None and coordinator is not None:
            record = getattr(coordinator, '_records', {}).get(
                getattr(getattr(existing, 'lease', None), 'lease_id', None))
            if record is not None and record.handle is existing:
                if (existing.owner_class != owner_class
                        or existing.profile_id != self.profile_id
                        or existing.authority_epoch != self.epoch
                        or existing.owner_id != self._automation_owner_id(
                            owner_class, session_id, request_id)):
                    raise RuntimeStoreError('preservation_owner_mismatch')
                if record.state == 'unknown':
                    reconciled = coordinator.reconcile_unknown(
                        existing, owner_id=existing.owner_id, profile_id=self.profile_id,
                        process_instance=self.instance_id, authority_epoch=self.epoch,
                    )
                    if reconciled is not None:
                        raise RuntimeStoreError(getattr(reconciled, 'code', 'custody_unknown'))
                self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
                self._preservation_pending_root(
                    request_id, existing, principal_id=principal_id, session_id=session_id,
                    source_identity=source_identity, delivery_id=delivery_id,
                )
                return existing
            self._preservation_automation_roots.pop(root_key, None)
        owner_id = self._automation_owner_id(owner_class, session_id, request_id)
        handle = self._preservation_root(owner_class, owner_id)
        self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
        self._preservation_automation_roots[root_key] = handle
        self._preservation_pending_root(
            request_id, handle, principal_id=principal_id, session_id=session_id,
            source_identity=source_identity, delivery_id=delivery_id,
        )
        return handle

    def _automation_owner_id(self, owner_class, session_id, request_id):
        if owner_class == 'I07_automation_route_wakes':
            return f'automation:{self.profile_id}:{session_id}:{request_id}'
        return f'{owner_class}:{self.profile_id}:{session_id}:{request_id}'

    def _preservation_automation_descriptor(self, handle, *, request_id, principal_id, session_id,
                                            source_identity, delivery_id=None):
        """Return the durable, non-secret identity bound to an I07 row/receipt."""
        if handle is None:
            return None
        coordinator = self._preservation_coordinator()
        if (coordinator is None
                or getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES):
            raise RuntimeStoreError('preservation_owner_unmapped')
        home = str(Path(self.db.db_path).parent.resolve())
        descriptor = {
            'owner_class': handle.owner_class,
            'owner_id': handle.owner_id,
            'profile_id': self.profile_id,
            'profile_home': home,
            'authority_epoch': self.epoch,
            'process_instance': handle.process_instance,
            'close_generation': handle.close_generation,
            'coordinator_id': handle.coordinator_id,
            'session_id': session_id,
            'principal_id': principal_id,
            'request_id': request_id,
            'source_identity': source_identity,
        }
        if delivery_id is not None:
            descriptor['delivery_id'] = delivery_id
        return descriptor

    def _preservation_validate_i09_descriptor(self, descriptor, *, handle=None,
                                              request_id, principal_id, session_id,
                                              source_identity, delivery_id=None):
        """Validate the source-owned I09 tuple before a direct durable write."""
        if (not _automation_descriptor_complete(descriptor)
                or descriptor.get('owner_class') != 'I09_a2a_hosted_kanban'
                or descriptor.get('profile_id') != self.profile_id
                or descriptor.get('profile_home') != str(Path(self.db.db_path).parent.resolve())
                or descriptor.get('session_id') != session_id
                or descriptor.get('principal_id') != principal_id
                or descriptor.get('request_id') != request_id
                or descriptor.get('source_identity') != source_identity
                or (delivery_id is not None and descriptor.get('delivery_id') != delivery_id)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if handle is not None:
            bound = next((value for value in self._preservation_automation_bindings.values()
                          if value.get('handle') is handle), None)
            if bound is not None and bound.get('successor'):
                if descriptor != bound.get('descriptor'):
                    raise RuntimeStoreError('preservation_owner_unmapped')
            else:
                if descriptor.get('authority_epoch') != getattr(handle, 'authority_epoch', None):
                    raise RuntimeStoreError('preservation_owner_unmapped')
                if any(descriptor.get(field) != getattr(handle, field, None)
                       for field in ('owner_id', 'process_instance', 'close_generation',
                                     'coordinator_id')):
                    raise RuntimeStoreError('preservation_owner_unmapped')
            if (getattr(handle, 'profile_id', None) != self.profile_id
                    or getattr(handle, 'authority_epoch', None) != self.epoch):
                raise RuntimeStoreError('preservation_owner_unmapped')
        return dict(descriptor)

    def _preservation_i09_descriptor_for_row(self, row, *, source_identity, delivery_id=None):
        descriptor = self._preservation_automation_row_descriptor(row)
        binding = (self._preservation_automation_bindings.get(row.get('admission_id'))
                   if isinstance(row, dict) else None)
        handle = binding.get('handle') if isinstance(binding, dict) else None
        # A restarted authority may already have rebound this exact row to a current handle.
        # Validate that retained binding before accepting the durable row's old descriptor epoch;
        # a scalar epoch match alone must never mint or transfer custody.
        if binding is not None and (
                handle is None or not self._preservation_automation_row_matches(row, handle)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if (not isinstance(row, dict)
                or (binding is None and row.get('owner_epoch') != (descriptor or {}).get('authority_epoch'))):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return self._preservation_validate_i09_descriptor(
            descriptor, request_id=row.get('request_id'), principal_id=row.get('principal_id'),
            session_id=row.get('target_session_id'), source_identity=source_identity,
            delivery_id=delivery_id, handle=handle)

    @staticmethod
    def _preservation_automation_row_descriptor(row):
        from gateway.session_automation_custody import row_descriptor
        return row_descriptor(row)

    def _preservation_automation_row_matches(
            self, row, handle, *, allow_terminal=False, allow_preclaim_cancel=False):
        descriptor = self._preservation_automation_row_descriptor(row)
        owner_class = getattr(handle, 'owner_class', None)
        pending = self._preservation_pending_roots.get(id(handle))
        binding = self._preservation_automation_bindings.get(row.get('admission_id')) if isinstance(row, dict) else None
        descriptor_valid = _automation_descriptor_complete(descriptor)
        preclaim_cancel = (
            allow_preclaim_cancel
            and isinstance(row, dict)
            and row.get('status') == 'terminal'
            and row.get('outcome') == 'cancelled'
            and row.get('generation') is None
        )
        if preclaim_cancel:
            # A cancellation witness is valid only before the exact FIFO child was reserved.
            children = getattr(self, '_preservation_children', None)
            claiming = getattr(self, '_preservation_claiming', None)
            if ((isinstance(children, dict) and children.get(row.get('admission_id')) is not None)
                    or (isinstance(claiming, dict) and row.get('admission_id') in claiming)):
                return False
        if (owner_class not in _AUTOMATION_OWNER_CLASSES
                or (owner_class in _AUTOMATION_OWNER_CLASSES and not descriptor_valid)
                or (pending is None and binding is None)
                or type(row.get('owner_epoch')) is not int or row['owner_epoch'] < 1
                or not _automation_generation_valid(
                    row, allow_terminal=allow_terminal, allow_preclaim_cancel=preclaim_cancel)):
            return False
        if pending is not None and pending.get('handle') is not handle:
            pending = None
        if binding is not None and binding.get('handle') is not handle:
            binding = None
        if pending is None and binding is None:
            return False
        bound_row = (binding or {}).get('row')
        recovered_queued = False
        recovered_started = False
        if bound_row is not None:
            for field in ('admission_id', 'target_session_id', 'principal_id', 'request_id'):
                if row.get(field) != bound_row.get(field):
                    return False
            successor = bool((binding or {}).get('successor'))
            expected_bound_epoch = (descriptor.get('authority_epoch')
                                    if successor else self.epoch)
            exact_claim = (row.get('owner_epoch') == bound_row.get('owner_epoch')
                           and row.get('generation') == bound_row.get('generation'))
            recovered_claim = (
                bound_row.get('status') == 'queued'
                and type(bound_row.get('owner_epoch')) is int
                and bound_row['owner_epoch'] >= 1
                and bound_row['owner_epoch'] == expected_bound_epoch
                and (row.get('status') in {'started', 'unknown'}
                     or (allow_terminal and row.get('status') == 'terminal'))
                and row.get('owner_epoch') == self.epoch
                and type(row.get('generation')) is int
                and row['generation'] >= 1
            )
            recovered_queued = (
                successor
                and bound_row.get('status') == 'queued'
                and row.get('status') == 'queued'
                and row.get('owner_epoch') == bound_row.get('owner_epoch')
                and row.get('generation') is None
            )
            recovered_started = (
                successor
                and bound_row.get('status') == 'started'
                and row.get('status') == 'started'
                and row.get('owner_epoch') == bound_row.get('owner_epoch')
                and row.get('generation') == bound_row.get('generation')
                and type(row.get('generation')) is int
                and row['generation'] >= 1
            )
            if not (exact_claim or recovered_claim or recovered_queued or recovered_started):
                return False
        successor = bool((binding or {}).get('successor'))
        if (getattr(handle, 'profile_id', None) != self.profile_id
                or getattr(handle, 'authority_epoch', None) != self.epoch
                or (row.get('owner_epoch') != self.epoch
                    and not (successor and (recovered_queued or recovered_started)))):
            return False
        if descriptor_valid and (
                descriptor.get('owner_class') != owner_class
                or descriptor.get('profile_id') != self.profile_id
                or descriptor.get('profile_home') != str(Path(self.db.db_path).parent.resolve())
                or descriptor.get('session_id') != row.get('target_session_id')
                or descriptor.get('principal_id') != row.get('principal_id')
                or descriptor.get('request_id') != row.get('request_id')
                or (not successor and (
                    descriptor.get('owner_id') != handle.owner_id
                    or descriptor.get('authority_epoch') != self.epoch
                    or descriptor.get('process_instance') != handle.process_instance
                    or descriptor.get('close_generation') != handle.close_generation
                    or descriptor.get('coordinator_id') != handle.coordinator_id))
                or (successor and descriptor != (binding or {}).get('descriptor'))):
            return False
        expected_source = (pending or {}).get('source_identity') or (binding or {}).get('source_identity')
        expected_delivery = (pending or {}).get('delivery_id') or (binding or {}).get('delivery_id')
        if descriptor_valid and expected_source is not None and descriptor.get('source_identity') != expected_source:
            return False
        if descriptor_valid and expected_delivery is not None and descriptor.get('delivery_id') != expected_delivery:
            return False
        return True

    def _preservation_recover_existing_automation(self, row):
        """Return a current exact handle without rechecking the successor lock."""
        existing = self._preservation_handles.get(row.get('admission_id'))
        if existing is None:
            return None
        if (getattr(existing, 'profile_id', None) != self.profile_id
                or getattr(existing, 'authority_epoch', None) != self.epoch
                or not self._preservation_automation_row_matches(row, existing)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return existing

    def _preservation_recover_automation(self, row, receipt=None):
        """Adopt one Bot/automation row after restart using its durable tuple.

        A mailbox receipt is only supporting evidence.  The canonical SQLite row, its profile
        home, principal, request and session must all agree before a fresh current-process root
        is reserved.  A copied lease id or a local map cannot satisfy this check.
        """
        principal = row.get('principal_id') if isinstance(row, dict) else None
        descriptor = self._preservation_automation_row_descriptor(row) if isinstance(row, dict) else None
        trusted_direct = (_automation_descriptor_complete(descriptor)
                          and descriptor.get('owner_class') == 'I09_a2a_hosted_kanban'
                          and descriptor.get('principal_id') == principal)
        if (not isinstance(row, dict) or not isinstance(principal, str)
                or (not principal.startswith('automation:') and not trusted_direct)):
            return None
        if descriptor is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if not _automation_descriptor_complete(descriptor):
            raise RuntimeStoreError('preservation_owner_unmapped')
        # The mailbox is a projection.  If it is present during recovery, require its custody
        # witness to be byte-for-byte equal to the canonical SQLite payload before adopting the
        # row; a forged/cross-profile receipt must never mint a successor lease.
        if receipt is not None:
            receipt_descriptor = receipt.get('automation_custody') if isinstance(receipt, dict) else None
            if not isinstance(receipt_descriptor, dict) or receipt_descriptor != descriptor:
                raise RuntimeStoreError('preservation_owner_unmapped')
        home = str(Path(self.db.db_path).parent.resolve())
        owner_class = descriptor.get('owner_class')
        if owner_class not in _AUTOMATION_OWNER_CLASSES:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if any((descriptor.get(name) != value for name, value in {
            'owner_class': owner_class,
            'profile_id': self.profile_id,
            'profile_home': home,
            'session_id': row.get('target_session_id'),
            'principal_id': row.get('principal_id'),
            'request_id': row.get('request_id'),
        }.items())):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if row.get('status') not in {'queued', 'started', 'unknown'}:
            return None
        if not _automation_generation_valid(row):
            raise RuntimeStoreError('preservation_owner_unmapped')
        # A retry issued by this authority must use the exact retained handle.  The process
        # ownership check below is only for minting a successor after restart; requiring the
        # profile lock again here turns a valid same-owner retry into foreign_custody.
        existing = self._preservation_recover_existing_automation(row)
        if existing is not None:
            return existing
        row_owner_epoch = row.get('owner_epoch')
        if (type(row_owner_epoch) is not int or row_owner_epoch < 1
                or row_owner_epoch != descriptor.get('authority_epoch')):
            raise RuntimeStoreError('preservation_owner_unmapped')
        # Recovery may mint a successor only while THIS process still owns the profile lock and
        # the store's current epoch points at THIS authority.  The old descriptor is retained as
        # a witness and is intentionally allowed to carry the prior process/epoch; comparing it to
        # the new authority would reject a legitimate restart.  A lock/epoch mismatch refuses
        # before looking up or reserving any successor handle.
        from gateway.runtime_ownership import process_ownership
        if not process_ownership.owns(Path(home)):
            raise RuntimeStoreError('foreign_custody')
        try:
            with self.db._read_ctx() as conn:
                current_epoch = conn.execute(
                    'SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1'
                ).fetchone()
        except Exception as exc:
            raise RuntimeStoreError('read_failed') from exc
        try:
            epoch_matches = current_epoch is not None and int(current_epoch[0]) == self.epoch
        except (TypeError, ValueError):
            epoch_matches = False
        if not epoch_matches or current_epoch[1] != self.instance_id:
            raise RuntimeStoreError('stale_epoch')
        # An unknown row is a source-owned uncertainty boundary.  A fresh authority may not mint
        # a new root from its scalar row fields; only an already-retained exact handle can retry it.
        if row.get('status') == 'unknown':
            return None
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                raise RuntimeStoreError('runtime_custody_unavailable')
        owner_id = f"successor:{row['admission_id']}:{row['request_id']}"
        handle = coordinator.reserve_root(
            owner_class, owner_id,
            profile_id=self.profile_id, authority_epoch=self.epoch,
        )
        from gateway.runtime_custody import CustodyRefusal
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        # The new lease is valid only because the durable row is the exact source-owned handoff;
        # keep the old process/epoch witness in the descriptor for diagnosis, never as a new owner.
        self._preservation_handles[row['admission_id']] = handle
        self._preservation_automation_roots[descriptor.get('delivery_id', row['request_id'])] = handle
        self._preservation_automation_bindings[row['admission_id']] = {
            'handle': handle, 'row': dict(row), 'descriptor': dict(descriptor),
            'source_identity': descriptor.get('source_identity'),
            'delivery_id': descriptor.get('delivery_id'), 'successor': True,
        }
        self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
        return handle

    def _preservation_bind_claim(self, admission_id, row):
        """Bind the real SQLite claim generation before publishing execution state."""
        if not isinstance(row, dict) or admission_id is None:
            return
        handle = self._preservation_handles.get(admission_id)
        if handle is not None and getattr(handle, 'owner_class', None) in _AUTOMATION_OWNER_CLASSES:
            if (row.get('admission_id') != admission_id or row.get('status') != 'started'
                    or row.get('owner_epoch') != self.epoch or type(row.get('generation')) is not int
                    or not self._preservation_automation_row_matches(row, handle)):
                self._preservation_mark_unknown(admission_id, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
            previous = self._preservation_automation_bindings.get(admission_id) or {}
            descriptor = self._preservation_automation_row_descriptor(row) or previous.get('descriptor')
            self._preservation_automation_bindings[admission_id] = {
                'handle': handle,
                'descriptor': dict(descriptor or {}),
                'source_identity': (descriptor or {}).get('source_identity')
                    or previous.get('source_identity'),
                'delivery_id': (descriptor or {}).get('delivery_id')
                    or previous.get('delivery_id'),
                'successor': bool(previous.get('successor')),
                'row': {'admission_id': admission_id, 'target_session_id': row.get('target_session_id'),
                        'principal_id': row.get('principal_id'), 'request_id': row.get('request_id'),
                        'owner_epoch': row.get('owner_epoch'), 'generation': row.get('generation')},
            }
        for key, record in list(self._preservation_operations.items()):
            if record.get('admission_id') == admission_id:
                if not self._preservation_operation_bind_row(key, row):
                    self._preservation_mark_unknown(admission_id, 'unmapped_writer')
                    raise RuntimeStoreError('preservation_owner_unmapped')

    def _preservation_pending_root(self, request_id, handle, *, principal_id, session_id,
                                   source_identity=None, delivery_id=None):
        if handle is not None:
            self._preservation_pending_roots[id(handle)] = {
                'handle': handle,
                'owner_class': handle.owner_class,
                'principal_id': principal_id,
                'session_id': session_id,
                'request_id': request_id,
                'source_identity': source_identity,
                'delivery_id': delivery_id,
            }

    async def _submit_i09_automation(self, actor, ref, request_id, payload, handle, *,
                                      source_identity, delivery_id=None, descriptor=None,
                                      kanban_card=None):
        """Admit a source-owned A2A/Kanban row with custody before FIFO publication."""
        self.authorize(actor, ref, 'session:submit')
        if (not isinstance(payload, dict)
                or set(payload) - {'text', 'finite'}
                or not isinstance(payload.get('text'), str)
                or ('finite' in payload and type(payload['finite']) is not bool)):
            raise RuntimeStoreError('invalid_params')
        self._require_fresh_admission_open()
        async with self.sessions[ref.session_id].mutation_lock:
            self.authorize(actor, ref, 'session:submit')
            if descriptor is None:
                descriptor = self._preservation_automation_descriptor(
                    handle, request_id=request_id, principal_id=actor.subject,
                    session_id=ref.session_id, source_identity=source_identity,
                    delivery_id=delivery_id)
            descriptor = self._preservation_validate_i09_descriptor(
                descriptor, handle=handle, request_id=request_id,
                principal_id=actor.subject, session_id=ref.session_id,
                source_identity=source_identity, delivery_id=delivery_id)
            durable_payload = dict(payload)
            if kanban_card is not None:
                if (not isinstance(source_identity, str) or not source_identity.startswith('kanban:')
                        or not isinstance(kanban_card, dict)
                        or set(kanban_card) != {'board', 'task_id', 'run_id'}
                        or not isinstance(kanban_card['board'], str)
                        or not isinstance(kanban_card['task_id'], str)
                        or type(kanban_card['run_id']) is not int):
                    raise RuntimeStoreError('invalid_params')
                durable_payload['kanban_card_v1'] = dict(kanban_card)
            durable_payload['_automation_custody_v1'] = descriptor
            admitted = partial(self._admitted, ref, None)
            def committed(row):
                if handle is not None:
                    self._preservation_bind_submitted_automation(
                        row, handle, source_identity=source_identity,
                        delivery_id=delivery_id, durable_descriptor=descriptor)
                return admitted(row)
            from gateway.session_runtime_workers import tracked_write
            try:
                return await tracked_write(
                    self,
                    partial(admit_session_input, self.db, epoch=self.epoch,
                            principal_id=actor.subject, session_id=ref.session_id,
                            request_id=request_id, payload=durable_payload,
                            _authorize_write=self._admission_gate(fresh=True)),
                    then=committed, ordered=True,
                    after=partial(self._schedule_admitted, ref))
            except asyncio.CancelledError:
                self._preservation_reconcile_failed(
                    handle, principal_id=actor.subject, session_id=ref.session_id,
                    request_id=request_id)
                raise
            except BaseException:
                self._preservation_reconcile_failed(
                    handle, principal_id=actor.subject, session_id=ref.session_id,
                    request_id=request_id)
                raise


    def _preservation_bind_submitted_automation(self, row, handle, *, source_identity=None,
                                                delivery_id=None, durable_descriptor=None):
        """Bind one source-owned I09 row before FIFO claim or validate its exact replay."""
        if handle is None or not isinstance(row, dict):
            raise RuntimeStoreError('preservation_owner_unmapped')
        owner_class = getattr(handle, 'owner_class', None)
        if owner_class != 'I09_a2a_hosted_kanban':
            raise RuntimeStoreError('preservation_owner_unmapped')
        pending = self._preservation_pending_roots.get(id(handle))
        admission_id = row.get('admission_id')
        if (not isinstance(admission_id, str)
                or row.get('status') not in {'queued', 'started', 'unknown', 'terminal'}):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        descriptor = self._preservation_automation_row_descriptor(row)
        if durable_descriptor is not None and descriptor != durable_descriptor:
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        if (not _automation_descriptor_complete(descriptor)
                or descriptor.get('owner_class') != owner_class
                or descriptor.get('profile_id') != self.profile_id
                or descriptor.get('session_id') != row.get('target_session_id')
                or descriptor.get('principal_id') != row.get('principal_id')
                or descriptor.get('request_id') != row.get('request_id')
                or (source_identity is not None and descriptor.get('source_identity') != source_identity)
                or (delivery_id is not None and descriptor.get('delivery_id') != delivery_id)):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        existing = self._preservation_handles.get(admission_id)
        binding = self._preservation_automation_bindings.get(admission_id)
        if existing is not None and existing is not handle:
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        if binding is not None and (binding.get('handle') is not handle
                                    or binding.get('descriptor') != descriptor):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        if binding is None:
            if (pending is None or pending.get('handle') is not handle
                    or row.get('principal_id') != pending.get('principal_id')
                    or row.get('target_session_id') != pending.get('session_id')
                    or row.get('request_id') != pending.get('request_id')):
                self._preservation_mark_unknown(admission_id, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
        recovered_queued = bool(binding and binding.get('successor')
                              and row.get('status') == 'queued'
                              and row.get('owner_epoch') == descriptor.get('authority_epoch')
                              and row.get('generation') is None)
        if not recovered_queued and not self._preservation_automation_row_matches(row, handle):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        if pending is not None:
            self._preservation_forget_pending_root(handle)
        self._preservation_handles[admission_id] = handle
        self._preservation_automation_bindings[admission_id] = {
            'handle': handle, 'row': dict(row), 'descriptor': dict(descriptor),
            'source_identity': source_identity or descriptor.get('source_identity'),
            'delivery_id': delivery_id or descriptor.get('delivery_id'),
            **({'successor': True} if binding and binding.get('successor') else {}),
        }
        self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
        return admission_id

    def _preservation_record_automation_binding(self, row, handle, *, source_identity=None,
                                                delivery_id=None):
        """Retain an admitted automation row after the generic callback consumes its pending root."""
        if handle is None or not isinstance(row, dict):
            raise RuntimeStoreError('preservation_owner_unmapped')
        owner_class = getattr(handle, 'owner_class', None)
        admission_id = row.get('admission_id')
        if (owner_class not in _AUTOMATION_OWNER_CLASSES
                or not isinstance(admission_id, str)
                or row.get('status') == 'terminal'
                or row.get('owner_epoch') != self.epoch
                or self._preservation_handles.get(admission_id) is not handle):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        descriptor = self._preservation_automation_row_descriptor(row)
        if owner_class in {'I07_automation_route_wakes', 'I08_goals_loops_heartbeat', 'I09_a2a_hosted_kanban'}:
            if (not _automation_descriptor_complete(descriptor)
                    or descriptor.get('owner_class') != owner_class
                    or descriptor.get('profile_id') != self.profile_id
                    or descriptor.get('authority_epoch') != self.epoch
                    or descriptor.get('process_instance') != handle.process_instance
                    or descriptor.get('close_generation') != handle.close_generation
                    or descriptor.get('coordinator_id') != handle.coordinator_id
                    or descriptor.get('session_id') != row.get('target_session_id')
                    or descriptor.get('principal_id') != row.get('principal_id')
                    or descriptor.get('request_id') != row.get('request_id')):
                self._preservation_mark_unknown(admission_id, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
        pending = self._preservation_pending_roots.get(id(handle))
        if pending is not None and (
                pending.get('handle') is not handle
                or pending.get('principal_id') != row.get('principal_id')
                or pending.get('request_id') != row.get('request_id')
                or pending.get('session_id') != row.get('target_session_id')):
            self._preservation_mark_unknown(admission_id, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        self._preservation_automation_bindings[admission_id] = {
            'handle': handle,
            'row': dict(row),
            'descriptor': dict(descriptor or {}),
            'source_identity': source_identity or (descriptor or {}).get('source_identity'),
            'delivery_id': delivery_id or (descriptor or {}).get('delivery_id'),
        }
        self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
        return admission_id

    def _preservation_automation_terminal_witness_ready(self, admission_id):
        """Require the canonical preclaim boundary or a real typed transport receipt."""
        bindings = getattr(self, '_preservation_automation_bindings', None)
        binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
        handle = binding.get('handle') if isinstance(binding, dict) else None
        if getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES:
            return True

        # Cancellation is the one terminal path with no producer transport.  It is accepted only
        # from the canonical row after the queued producer was cancelled before an I04 child or
        # claim existed; no in-memory witness is minted for this case.
        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(self.db, admission_id=admission_id)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return False
        preclaim_cancel = (
            isinstance(row, dict)
            and row.get('admission_id') == admission_id
            and row.get('status') == 'terminal'
            and row.get('outcome') == 'cancelled'
            and row.get('generation') is None
        )
        if preclaim_cancel and isinstance(binding, dict):
            bound_row = binding.get('row')
            if (isinstance(bound_row, dict)
                    and all(row.get(field) == bound_row.get(field) for field in (
                        'admission_id', 'target_session_id', 'principal_id', 'request_id'))
                    and _automation_generation_valid(
                        row, allow_terminal=True, allow_preclaim_cancel=True)
                    and self._preservation_automation_row_matches(
                        row, handle, allow_terminal=True, allow_preclaim_cancel=True)):
                children = getattr(self, '_preservation_children', {})
                claiming = getattr(self, '_preservation_claiming', {})
                if (not isinstance(children, dict) or children.get(admission_id) is None) and (
                        not isinstance(claiming, dict) or admission_id not in claiming):
                    return True

        # A waiter completing only proves that a local future was resolved.  The release fence
        # consumes the typed receipt returned by the source-owned transport path instead.
        try:
            from gateway.session_ingress import DeliveryReceipt
        except ImportError:
            return False
        receipt = binding.get('terminal_delivery_receipt') if isinstance(binding, dict) else None
        if (not isinstance(receipt, DeliveryReceipt)
                or receipt.admission_id != admission_id
                or receipt.attempted is not True
                or receipt.delivered is not True
                or receipt.reason is not None
                or receipt.effect_state != 'delivered'):
            return False
        sidecars = [entry for entry in getattr(self, '_preservation_sidecars', {}).values()
                    if entry.get('admission_id') == admission_id]
        return not sidecars or all(
            entry.get('delivery_complete') is True and entry.get('terminal') is True
            for entry in sidecars
        )

    def _preservation_automation_no_delivery_terminal_witness(
            self, admission_id, delivery_receipt):
        """Accept the durable terminal row when this producer has no outbound recipient.

        Heartbeat and direct A2A/Kanban admissions can complete without a waiter or a
        ``pending_deliveries`` entry.  That source-owned path has no transport receipt to
        record; the committed result plus the exact automation binding is its terminal
        witness.  A no-delivery receipt that followed an actual pending delivery remains
        unqualified and keeps the original custody fence.
        """
        if (
            getattr(delivery_receipt, 'admission_id', None) != admission_id
            or getattr(delivery_receipt, 'attempted', None) is not False
            or getattr(delivery_receipt, 'delivered', None) is not False
            or getattr(delivery_receipt, 'reason', None) != 'no_delivery'
            or getattr(delivery_receipt, 'effect_state', None) != 'none'
        ):
            return False
        bindings = getattr(self, '_preservation_automation_bindings', None)
        binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
        handle = binding.get('handle') if isinstance(binding, dict) else None
        if getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES:
            return False
        if binding.get('terminal_delivery_receipt') is not None:
            return False
        if self._preservation_has_sidecar(admission_id):
            return False
        if admission_id in getattr(self, 'pending_deliveries', {}):
            return False
        if admission_id in getattr(self, 'native_waiters', set()):
            return False
        if admission_id in getattr(self, 'waiters', {}):
            return False
        for registry_name in (
                'marker_adopters', 'held_turn_markers', 'unsettled_deliveries'):
            registry = getattr(self, registry_name, {})
            if admission_id in registry:
                return False
        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(self.db, admission_id=admission_id)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return False
        if (
            not isinstance(row, dict)
            or row.get('admission_id') != admission_id
            or row.get('status') != 'terminal'
            or row.get('outcome') == 'cancelled'
            or type(row.get('generation')) is not int
            or row.get('generation') < 1
            or not self._preservation_automation_row_matches(
                row, handle, allow_terminal=True,
            )
        ):
            return False
        try:
            from gateway.session_results import admission_result
            return admission_result(self.db, admission_id) is not None
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return False

    def _preservation_accept_automation_direct_delivery(
            self, admission_id, delivery_receipt, *, physical_delivery, waiter_delivered):
        if physical_delivery:
            return self._preservation_record_automation_terminal_witness(
                admission_id, delivery_receipt=delivery_receipt,
                waiter_delivered=waiter_delivered, finalizer_outcome='direct_delivery')
        return self._preservation_automation_no_delivery_terminal_witness(
            admission_id, delivery_receipt)

    def _preservation_record_automation_terminal_witness(
        self, admission_id, *, delivery_receipt=None, waiter_delivered=False,
        finalizer_outcome=None,
    ):
        """Bind the source-owned typed transport receipt after the canonical finalizer boundary."""
        bindings = getattr(self, '_preservation_automation_bindings', None)
        binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
        handle = binding.get('handle') if isinstance(binding, dict) else None
        if getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES:
            return True

        def reject(reason='delivery_reconcile_failed'):
            self._preservation_mark_unknown(admission_id, reason)
            return False

        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(self.db, admission_id=admission_id)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return reject()
        if not isinstance(row, dict) or row.get('admission_id') != admission_id:
            return reject('unmapped_writer')
        preclaim_cancel = row.get('outcome') == 'cancelled' and row.get('generation') is None
        if preclaim_cancel:
            # The cancellation caller is the sole consumer of this boundary.  A delivery recorder
            # must never turn a no-execution cancellation into a fabricated transport witness.
            return reject('unmapped_writer')
        if row.get('status') != 'terminal' or not _automation_generation_valid(
                row, allow_terminal=True):
            return reject('unmapped_writer')
        descriptor = self._preservation_automation_row_descriptor(row)
        if (not _automation_descriptor_complete(descriptor)
                or descriptor != binding.get('descriptor')
                or not self._preservation_automation_row_matches(
                    row, handle, allow_terminal=True)):
            return reject('unmapped_writer')
        bound_row = binding.get('row')
        if isinstance(bound_row, dict) and any(
                row.get(field) != bound_row.get(field)
                for field in ('admission_id', 'target_session_id', 'principal_id', 'request_id')):
            return reject('unmapped_writer')
        try:
            from gateway.session_ingress import DeliveryReceipt
        except ImportError:
            return reject()
        if (not isinstance(delivery_receipt, DeliveryReceipt)
                or delivery_receipt.admission_id != admission_id
                or delivery_receipt.attempted is not True
                or delivery_receipt.delivered is not True
                or delivery_receipt.reason is not None
                or delivery_receipt.effect_state != 'delivered'):
            return reject('delivery_reconcile_failed')
        sidecars = [entry for entry in getattr(self, '_preservation_sidecars', {}).values()
                    if entry.get('admission_id') == admission_id]
        expected_finalizer = 'complete' if sidecars else 'direct_delivery'
        if finalizer_outcome != expected_finalizer:
            return reject('delivery_reconcile_failed')
        existing = binding.get('terminal_delivery_receipt')
        if existing is not None and existing != delivery_receipt:
            return reject('unmapped_writer')
        # Keep the actual source-owned receipt object.  Do not project waiter state or booleans into
        # a second terminal dictionary: readiness re-reads this typed receipt and live sidecars.
        binding['terminal_delivery_receipt'] = delivery_receipt
        return True

    def _preservation_prepare_automation_rehome(self, admission_id, row, new_session_id):
        """Hold one queued wake's I07 root while its durable row moves to a fresh route."""
        coordinator = self._preservation_coordinator()
        handle = self._preservation_handles.get(admission_id)
        descriptor = self._preservation_automation_row_descriptor(row)
        if (coordinator is None or handle is None
                or getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES
                or not isinstance(row, dict) or row.get('status') != 'queued'
                or not isinstance(new_session_id, str) or not new_session_id
                or not self._preservation_automation_row_matches(row, handle)
                or not isinstance(descriptor, dict)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if admission_id in self._preservation_children:
            raise RuntimeStoreError('preservation_owner_unmapped')
        rehome = {
            'handle': handle,
            'principal_id': row.get('principal_id'),
            'request_id': row.get('request_id'),
            'source_identity': descriptor.get('source_identity'),
            'delivery_id': descriptor.get('delivery_id'),
            'new_session_id': new_session_id,
        }
        self._preservation_automation_rehomes[admission_id] = rehome
        return rehome

    def _preservation_abort_automation_rehome(self, admission_id):
        self._preservation_automation_rehomes.pop(admission_id, None)

    def _preservation_finish_automation_rehome(self, admission_id):
        """Move a validated I07 root into the pending tuple for its new durable row."""
        rehome = self._preservation_automation_rehomes.get(admission_id)
        if rehome is None:
            return None
        handle = rehome.get('handle')
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                raise RuntimeStoreError('runtime_custody_unavailable')
        if (handle is None
                or getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES
                or getattr(handle, 'profile_id', None) != self.profile_id
                or getattr(handle, 'authority_epoch', None) != self.epoch
                or getattr(handle, 'process_instance', None)
                != getattr(coordinator, 'process_instance', None)
                or getattr(handle, 'process_instance', None) != self.instance_id
                or getattr(handle, 'coordinator_id', None)
                != getattr(coordinator, '_coordinator_id', None)
                or getattr(coordinator, '_parent', lambda _handle: None)(handle) is None):
            raise RuntimeStoreError('preservation_owner_unmapped')
        self._preservation_automation_rehomes.pop(admission_id, None)
        self._preservation_handles.pop(admission_id, None)
        self._preservation_automation_bindings.pop(admission_id, None)
        self._preservation_pending_root(
            rehome['request_id'], handle,
            principal_id=rehome['principal_id'], session_id=rehome['new_session_id'],
            source_identity=rehome['source_identity'], delivery_id=rehome['delivery_id'],
        )
        return handle

    def _preservation_forget_pending_root(self, handle):
        if handle is None:
            return
        pending = self._preservation_pending_roots.pop(id(handle), None)
        # Retain custody if an object-id were ever reused while the old record was still present.
        if pending is not None and pending.get('handle') is not handle:
            self._preservation_pending_roots[id(pending['handle'])] = pending

    def _preservation_pending_matches_row(self, row, handle):
        """Require the durable callback to belong to the exact object-bound producer identity."""
        pending = self._preservation_pending_roots.get(id(handle))
        if pending is None:
            return False
        if (pending.get('handle') is not handle
                or pending.get('owner_class') != getattr(handle, 'owner_class', None)
                or row.get('principal_id') != pending.get('principal_id')
                or row.get('request_id') != pending.get('request_id')):
            return False
        if (getattr(handle, 'owner_class', None) in _AUTOMATION_OWNER_CLASSES
                and not self._preservation_automation_row_matches(row, handle)):
            return False
        target_session_id = row.get('target_session_id')
        if target_session_id == pending.get('session_id'):
            return True
        try:
            return target_session_id == self._current_owner(pending['session_id'])
        except Exception:  # health: allow BLE001 -- opaque rotated-route identity lookup must fail closed rather than match a different producer
            return False

    def _preservation_release_unaccepted(self, handle):
        if handle is None:
            return
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                return
        result = coordinator.release_unaccepted(handle)
        if result is not None and result.code != 'stale_lease':
            coordinator.mark_unknown(handle, 'read_failed')

    def _preservation_bind_automation_child(self, admission_id, key):
        if not isinstance(admission_id, str) or not isinstance(key, str):
            raise RuntimeStoreError('preservation_owner_unmapped')
        record = self._preservation_operations.get(key)
        if record is None or record.get('owner_class') != 'I06_busy_followup_delivery':
            raise RuntimeStoreError('preservation_owner_unmapped')
        if record.get('admission_id') not in (None, admission_id):
            self._preservation_operation_unknown(key, 'unmapped_writer')
            raise RuntimeStoreError('preservation_owner_unmapped')
        record['admission_id'] = admission_id
        self._preservation_automation_children[admission_id] = key

    def _preservation_finish_automation(self, admission_id, *, finalizer_id, keep_root=False,
                                        unknown=False):
        """Finish one I06 child after the receipt/result boundary, then optionally its I07 root."""
        if admission_id is None:
            return
        key = self._preservation_automation_children.get(admission_id)
        if key is not None:
            if unknown:
                self._preservation_operation_unknown(key, 'join_failed')
            else:
                self._preservation_operation_terminal(key, finalizer_id)
                if key not in self._preservation_operations:
                    self._preservation_automation_children.pop(admission_id, None)
        if keep_root:
            return
        handle = self._preservation_handles.get(admission_id)
        if handle is None:
            return
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                raise RuntimeStoreError('runtime_custody_unavailable')
        if unknown:
            coordinator.mark_unknown(handle, 'join_failed')
            return
        terminal = coordinator.mark_finalizing(handle, finalizer_id)
        if terminal is not None:
            coordinator.mark_unknown(handle, 'lease_release_failed')
            return
        terminal = coordinator.mark_terminal(handle)
        if terminal is None:
            self._preservation_handles.pop(admission_id, None)
            self._preservation_automation_bindings.pop(admission_id, None)
            for key, value in list(self._preservation_automation_roots.items()):
                if value is handle:
                    self._preservation_automation_roots.pop(key, None)
        elif getattr(terminal, 'code', None) == 'custody_pending':
            # The FIFO's own I04 child may still be draining when the Bot receipt is published.
            # Keep the accepted I07 root finalizing; the existing release-terminal boundary will
            # retry it after that exact child is closed.  A temporary dependency is not unknown.
            self._preservation_watch_fifo_close(admission_id)
            return
        else:
            coordinator.mark_unknown(handle, 'lease_release_failed')

    def _preservation_release_automation_unaccepted(self, child_key, handle):
        """Release the exact pre-row child/root after an explicit absent-row refusal."""
        if child_key is not None:
            self._preservation_operation_release_unaccepted(child_key)
            for admission_id, operation_key in list(self._preservation_automation_children.items()):
                if operation_key == child_key:
                    self._preservation_automation_children.pop(admission_id, None)
        if handle is None:
            return
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                raise RuntimeStoreError('runtime_custody_unavailable')
        result = coordinator.release_unaccepted(handle)
        if result is None or getattr(result, 'code', None) == 'stale_lease':
            for key, value in list(self._preservation_automation_roots.items()):
                if value is handle:
                    self._preservation_automation_roots.pop(key, None)
            self._preservation_forget_pending_root(handle)
        else:
            coordinator.mark_unknown(handle, 'read_failed')

    def _preservation_mark_finalizing(self, admission_id, finalizer_id):
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
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
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
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
