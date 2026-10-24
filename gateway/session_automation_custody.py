"""Durable identity helpers for trusted automation admissions.

The coordinator still owns every lease.  This module only canonicalizes the source-owned tuple
that is stamped into an existing FIFO payload so a successor can authenticate the same row.
"""
import asyncio
from dataclasses import dataclass
from functools import partial
import hashlib
import json
from pathlib import Path
import sqlite3

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


_I07_SOURCE_INDEX_TYPES = (
    ('_preservation_automation_roots', dict),
    ('_preservation_automation_children', dict),
    ('_preservation_automation_bindings', dict),
    ('_preservation_automation_rehomes', dict),
    ('_preservation_handles', dict),
    ('_preservation_children', dict),
    ('_preservation_pending_roots', dict),
)
_I07_READ_FAILED = object()
_I07_IGNORE_ROW = object()


def _physical_home(value):
    """Canonicalize a filesystem home without changing the logical profile identity."""
    from gateway.runtime_ownership import canonical_home

    return canonical_home(Path(value))


def _physical_home_matches(value, expected) -> bool:
    if not isinstance(value, (str, Path)):
        return False
    try:
        candidate = Path(value)
        return candidate.is_absolute() and _physical_home(candidate) == _physical_home(expected)
    except (OSError, TypeError, ValueError):
        return False


@dataclass(frozen=True)
class AutomationProducerGraph:
    """Exact bootstrap witness for the real I07 producer families.

    This is an identity-bound source graph, not another custody ledger.  The existing coordinator
    remains the only owner of leases; these references let the close reader distinguish a registered
    empty automation source from a missing, replaced, or malformed source index.
    """

    authority: object
    coordinator: object | None
    profile_id: str
    instance_id: str
    authority_epoch: int
    database: object
    database_path: str
    indexes: tuple[tuple[str, object, type], ...]
    producer_families: tuple[tuple[str, object], ...]
    # The source graph keeps the exact native profile-owner incarnation captured when it is
    # bound to the live coordinator.  Close-time code may revalidate this witness, but it must
    # never replace it with a newly acquired lock or a current owner observation.
    physical_owner_epoch: str | None = None
    native_descriptor: int | None = None
    physical_binding: tuple[str, str, int] | None = None

    def matches(self, authority, coordinator) -> bool:
        database_path = getattr(getattr(authority, 'db', None), 'db_path', None)
        if (
            self.authority is not authority
            or self.coordinator is not coordinator
            or getattr(authority, 'runner', None) is None
            or getattr(authority.runner, 'runtime_custody', None) is not coordinator
            or getattr(authority, 'profile_id', None) != self.profile_id
            or getattr(authority, 'instance_id', None) != self.instance_id
            or getattr(authority, 'epoch', None) != self.authority_epoch
            or getattr(authority, 'db', None) is not self.database
            or not isinstance(database_path, (str, Path))
            or str(_physical_home(database_path)) != self.database_path
            or getattr(coordinator, 'process_instance', None) != self.instance_id
        ):
            return False
        sentinel = object()
        for name, expected, expected_type in self.indexes:
            current = getattr(authority, name, sentinel)
            if current is not expected or type(current) is not expected_type:
                return False
        if self.coordinator is not None and (
            type(self.physical_owner_epoch) is not str
            or not self.physical_owner_epoch
            or type(self.native_descriptor) is not int
            or isinstance(self.native_descriptor, bool)
            or not isinstance(self.physical_binding, tuple)
            or self.physical_binding != (
                str(_physical_home(self.profile_id)),
                self.physical_owner_epoch, self.native_descriptor,
            )
        ):
            return False
        return self.producer_families == _i07_producer_families()


def _i07_producer_families():
    """Resolve the public and trusted I07 producer entrypoints at the source boundary."""
    from gateway.run_plugin_injection import GatewayPluginInjectionMixin
    from gateway.session_automation import admit_automation, admit_route_wake
    from gateway.session_bot import deliver
    from gateway.wake import deliver_wake

    return (
        ('automation', admit_automation),
        ('route_wake', admit_route_wake),
        ('plugin_injection', GatewayPluginInjectionMixin._admit_plugin_injection),
        ('bot_delivery', deliver),
        ('wake_delivery', deliver_wake),
    )


def _i07_graph(authority, coordinator):
    graph = getattr(authority, '_preservation_automation_source_graph', None)
    if not isinstance(graph, AutomationProducerGraph):
        return None
    try:
        return graph if graph.matches(authority, coordinator) else None
    except (AttributeError, TypeError, ValueError):
        return None


def _i07_graph_indexes_empty(graph):
    return all(not value for _, value, _ in graph.indexes)


def _i07_runtime_epoch_matches(authority):
    try:
        with authority.db._read_ctx() as conn:
            row = conn.execute(
                'SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1'
            ).fetchone()
    except (AttributeError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return False
    return (
        row is not None
        and type(row[0]) is int
        and row[0] == authority.epoch
        and row[1] == authority.instance_id
    )


def _i07_pending_admissions(authority):
    try:
        with authority.db._read_ctx() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM session_admissions "
                "WHERE status IN ('queued', 'started', 'unknown')"
            ).fetchone()
    except (AttributeError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return None
    if row is None or type(row[0]) is not int:
        return None
    return row[0]


def _i07_native_reservation(home):
    """Read the retained descriptor-bound native owner witness for one profile."""
    try:
        home = _physical_home(home)
        from gateway.runtime_ownership import process_ownership
        reservation = process_ownership.reservation_evidence(home)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(reservation, dict)
        or reservation.get('profile_home') != str(home)
        or type(reservation.get('owner_epoch')) is not str
        or not reservation.get('owner_epoch')
        or reservation.get('observed') is not True
        or reservation.get('native_handle_retained') is not True
        or reservation.get('os_lock_owner') is not True
        or reservation.get('cross_process_proof') is not True
        or type(reservation.get('native_descriptor')) is not int
        or isinstance(reservation.get('native_descriptor'), bool)
        or reservation.get('physical_binding') != (
            str(home), reservation.get('owner_epoch'), reservation.get('native_descriptor'),
        )
    ):
        return None
    return reservation


def _i07_native_owner_snapshot(authority, coordinator):
    """Read the bound profile owner and reject a replacement incarnation."""
    graph = _i07_graph(authority, coordinator)
    if graph is None:
        return None
    home = _physical_home(authority.profile_id)
    if _physical_home(Path(authority.db.db_path).parent) != home:
        return None
    if not _i07_runtime_epoch_matches(authority):
        return None
    try:
        scope = coordinator.profile_scope_observation(
            authority.profile_id, authority.epoch,
            require_serving=False, allow_retiring=True,
        )
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not getattr(scope, 'ok', False)
        or type(getattr(scope, 'physical_owner_epoch', None)) is not str
        or not scope.physical_owner_epoch
    ):
        return None
    reservation = _i07_native_reservation(home)
    if reservation is None:
        return None
    if (
        reservation.get('owner_epoch') != graph.physical_owner_epoch
        or reservation.get('native_descriptor') != graph.native_descriptor
        or reservation.get('physical_binding') != graph.physical_binding
        or scope.physical_owner_epoch != graph.physical_owner_epoch
    ):
        return None
    return {
        'profile_home': str(home),
        'profile_id': authority.profile_id,
        'authority_epoch': authority.epoch,
        'process_instance': authority.instance_id,
        'physical_owner_epoch': graph.physical_owner_epoch,
        'database_path': str(_physical_home(authority.db.db_path)),
        'database_identity': id(authority.db),
        'native_descriptor': graph.native_descriptor,
        'native_handle_retained': True,
        'os_lock_owner': True,
        'cross_process_proof': True,
    }


def _i07_close_source_snapshot(authority, coordinator, close_generation, *, require_empty=True):
    """Read the common live-source witness for this exact close generation.

    I07 has two terminal shapes: a genuinely untouched source and a source that admitted a
    wake and later published its durable terminal result.  Both shapes must authenticate the
    same registered graph and the current profile/DB/OS owner before the owner can be sealed.
    """
    graph = _i07_graph(authority, coordinator)
    if graph is None or (require_empty and not _i07_graph_indexes_empty(graph)):
        return None
    try:
        status = coordinator.status()
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    if close_generation is None:
        if status.get('state') != 'open' or status.get('close_generation') is not None:
            return None
    elif (
        status.get('state') != 'closing'
        or status.get('close_generation') != close_generation
        or type(close_generation) is not int
    ):
        return None
    if (
        'I07_automation_route_wakes' in status.get('active_owner_classes', ())
        or 'I07_automation_route_wakes' in status.get('unknown_owner_classes', ())
    ):
        return None
    pending = _i07_pending_admissions(authority)
    if pending is None or pending != 0:
        return None
    native = _i07_native_owner_snapshot(authority, coordinator)
    if native is None:
        return None
    return {
        'close_generation': close_generation,
        'producer_families': tuple(name for name, _ in graph.producer_families),
        'registered_indexes': tuple(name for name, _, _ in graph.indexes),
        'empty_indexes': tuple(name for name, value, _ in graph.indexes if not value),
        'pending_admissions': 0,
        'coordinator_id': getattr(coordinator, '_coordinator_id', None),
        **native,
    }


def _i07_terminal_root_index(graph):
    """Return the retained terminal-root map when every other I07 index is empty."""
    root_index = None
    for name, value, _ in graph.indexes:
        if name == '_preservation_automation_roots':
            root_index = value
        elif value:
            return None
    return root_index


def _i07_terminal_admission_ids(authority):
    try:
        with authority.db._read_ctx() as conn:
            return tuple(row[0] for row in conn.execute(
                "SELECT admission_id FROM session_admissions WHERE status='terminal' ORDER BY seq"
            ))
    except (AttributeError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return _I07_READ_FAILED


def _i07_terminal_receipt_row(
        authority, admission_id, *, home, current_process, current_coordinator,
        effective_close_generation,
):
    """Read and authenticate one current-process I07 terminal row, or ignore old history."""
    try:
        from hermes_state_runtime import get_session_admission
        from gateway.session_results import admission_result
        row = get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return _I07_READ_FAILED
    descriptor = row_descriptor(row)
    if not isinstance(descriptor, dict) or descriptor.get('owner_class') != 'I07_automation_route_wakes':
        return _I07_IGNORE_ROW
    # A current-process I07 row with any identity mismatch is an untrusted source read, not a
    # reason to choose a different row. Rows from an older process or epoch are history.
    if descriptor.get('process_instance') != current_process or descriptor.get('authority_epoch') != authority.epoch:
        return _I07_IGNORE_ROW
    if (
        not _automation_descriptor_complete(descriptor)
        or descriptor.get('profile_id') != authority.profile_id
        or not _physical_home_matches(descriptor.get('profile_home'), home)
        or descriptor.get('process_instance') != authority.instance_id
        or descriptor.get('coordinator_id') != current_coordinator
        or descriptor.get('session_id') != row.get('target_session_id')
        or descriptor.get('principal_id') != row.get('principal_id')
        or descriptor.get('request_id') != row.get('request_id')
        or descriptor.get('owner_id') != authority._automation_owner_id(
            'I07_automation_route_wakes', row.get('target_session_id'), row.get('request_id'))
        or row.get('owner_epoch') != authority.epoch
        or type(descriptor.get('close_generation')) is not int
        or descriptor.get('close_generation') < 0
        or descriptor.get('close_generation') > effective_close_generation
    ):
        return _I07_READ_FAILED
    generation = row.get('generation')
    outcome = row.get('outcome')
    cancelled = outcome == 'cancelled' and generation is None
    result = None
    if not cancelled and (type(generation) is not int or generation < 1):
        return _I07_READ_FAILED
    if not cancelled:
        try:
            result = admission_result(authority.db, admission_id)
        except (AttributeError, KeyError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
            return _I07_READ_FAILED
        if result is None:
            return _I07_READ_FAILED
    root_keys = {descriptor.get('delivery_id') or descriptor['request_id']}
    # Bot delivery keeps the mailbox key as the in-memory root key while the durable admission
    # identity is ``bot:<key>``. This normalization is source-owned and still requires the exact
    # terminal handle below; it is not a generic caller alias.
    request_id = descriptor['request_id']
    if descriptor.get('delivery_id') is None and request_id.startswith('bot:'):
        root_keys.add(request_id[4:])
    return {
        'receipt': {
            'admission_id': admission_id,
            'owner_id': descriptor['owner_id'],
            'authority_epoch': descriptor['authority_epoch'],
            'process_instance': descriptor['process_instance'],
            'coordinator_id': descriptor['coordinator_id'],
            'close_generation': descriptor['close_generation'],
            'generation': generation,
            'outcome': outcome,
            'result_present': cancelled or result is not None,
        },
        'root_keys': root_keys,
    }


def _i07_terminal_root_matches(authority, coordinator, common, root_key, handle, root_keys, receipts):
    if not isinstance(root_key, str) or root_key not in root_keys:
        return False
    if not coordinator.is_terminal_handle(
            handle, owner_class='I07_automation_route_wakes',
            profile_id=authority.profile_id, authority_epoch=authority.epoch):
        return False
    matching = next((receipt for receipt in receipts
                     if receipt['owner_id'] == getattr(handle, 'owner_id', None)), None)
    return (
        getattr(handle, 'process_instance', None) == common['process_instance']
        and getattr(handle, 'coordinator_id', None) == common.get('coordinator_id')
        and getattr(handle, 'physical_owner_epoch', None) == common['physical_owner_epoch']
        and matching is not None
    )


def _i07_validate_terminal_roots(authority, coordinator, common, root_index, root_keys, receipts):
    if root_index is None:
        return None
    retained = {}
    for root_key, handle in root_index.items():
        if not _i07_terminal_root_matches(
                authority, coordinator, common, root_key, handle, root_keys, receipts):
            return None
        retained[root_key] = handle
    return tuple(sorted(retained))


def _i07_terminal_receipt_snapshot(authority, coordinator, close_generation):
    """Read one real source-owned terminal I07 admission receipt.

    ``_route_wake_inbound_seen`` is only an ingress hint. It is never terminal evidence: the
    durable admission row, exact automation descriptor, and committed result must match this
    authority/process/coordinator. Old rows from an earlier process or profile epoch are history.
    """
    common = _i07_close_source_snapshot(
        authority, coordinator, close_generation, require_empty=False,
    )
    if common is None or authority._route_wake_inbound_seen is not True:
        return None
    graph = _i07_graph(authority, coordinator)
    if graph is None:
        return None
    root_index = _i07_terminal_root_index(graph)
    admission_ids = _i07_terminal_admission_ids(authority)
    if admission_ids is _I07_READ_FAILED or not admission_ids:
        return None
    home = str(_physical_home(Path(authority.db.db_path).parent))
    current_process = getattr(coordinator, 'process_instance', None)
    current_coordinator = getattr(coordinator, '_coordinator_id', None)
    effective_close_generation = close_generation if type(close_generation) is int else 0
    receipts = []
    root_keys = set()
    for admission_id in admission_ids:
        item = _i07_terminal_receipt_row(
            authority, admission_id, home=home, current_process=current_process,
            current_coordinator=current_coordinator, effective_close_generation=effective_close_generation,
        )
        if item is _I07_READ_FAILED:
            return None
        if item is _I07_IGNORE_ROW:
            continue
        receipts.append(item['receipt'])
        root_keys.update(item['root_keys'])
    if not receipts:
        return None
    retained = _i07_validate_terminal_roots(
        authority, coordinator, common, root_index, root_keys, receipts,
    )
    if retained is None:
        return None
    return {
        'mode': 'active_terminal',
        'terminal_receipts': tuple(receipts),
        'retained_terminal_roots': retained,
        **common,
    }


def automation_zero_work_snapshot(authority, coordinator, close_generation):
    """Return a typed I07 zero-work witness only at a fenced close boundary."""
    if getattr(authority, '_route_wake_inbound_seen', None) is not False:
        return None
    common = _i07_close_source_snapshot(authority, coordinator, close_generation)
    if common is None:
        return None
    return {
        'mode': 'zero_work',
        'zero_work': True,
        'ingress_closed': True,
        **common,
    }


def register_automation_source_graph(authority, coordinator=None):
    """Install or bind the exact I07 graph during authenticated runtime bootstrap."""
    graph = getattr(authority, '_preservation_automation_source_graph', None)
    if graph is None:
        db = getattr(authority, 'db', None)
        db_path = getattr(db, 'db_path', None)
        if (
            db is None
            or not isinstance(db_path, (str, Path))
            or not isinstance(getattr(authority, 'profile_id', None), str)
            or not isinstance(getattr(authority, 'instance_id', None), str)
            or type(getattr(authority, 'epoch', None)) is not int
            or authority.epoch < 1
        ):
            raise RuntimeStoreError('automation_source_graph_invalid')
        indexes = []
        sentinel = object()
        for name, expected_type in _I07_SOURCE_INDEX_TYPES:
            value = getattr(authority, name, sentinel)
            if value is sentinel or type(value) is not expected_type:
                raise RuntimeStoreError('automation_source_graph_invalid')
            indexes.append((name, value, expected_type))
        graph = AutomationProducerGraph(
            authority=authority,
            coordinator=None,
            profile_id=authority.profile_id,
            instance_id=authority.instance_id,
            authority_epoch=authority.epoch,
            database=db,
            database_path=str(_physical_home(db_path)),
            indexes=tuple(indexes),
            producer_families=_i07_producer_families(),
        )
    elif not isinstance(graph, AutomationProducerGraph) or graph.authority is not authority:
        raise RuntimeStoreError('automation_source_graph_invalid')
    if coordinator is None:
        authority._preservation_automation_source_graph = graph
        return graph
    if getattr(authority.runner, 'runtime_custody', None) is not coordinator:
        raise RuntimeStoreError('automation_source_graph_binding_mismatch')
    if getattr(coordinator, 'process_instance', None) != authority.instance_id:
        raise RuntimeStoreError('automation_source_graph_binding_mismatch')
    if graph.coordinator is not None and graph.coordinator is not coordinator:
        records = getattr(graph.coordinator, '_records', None)
        if getattr(graph.coordinator, 'state', None) != 'open' or type(records) is not dict or records:
            raise RuntimeStoreError('automation_source_graph_binding_collision')
    if graph.coordinator is coordinator:
        # A source graph already bound to this coordinator owns a single immutable native
        # incarnation.  Re-reading a different current lock here would silently adopt a
        # replacement profile owner and make the old close receipt appear valid.
        if (
            graph.physical_owner_epoch is None
            and graph.native_descriptor is None
            and graph.physical_binding is None
        ):
            # Some embedded/legacy adapters do not own an OS profile lock.  Keep their source
            # graph registered so their existing admission/unknown paths remain functional, but
            # leave I07 readiness UNKNOWN rather than manufacturing a physical witness.
            return graph
        if (
            type(graph.physical_owner_epoch) is not str
            or type(graph.native_descriptor) is not int
            or graph.physical_binding != (
                str(_physical_home(graph.profile_id)),
                graph.physical_owner_epoch, graph.native_descriptor,
            )
        ):
            raise RuntimeStoreError('automation_source_graph_binding_mismatch')
        current = _i07_native_reservation(_physical_home(graph.profile_id))
        if (
            current is None
            or current.get('owner_epoch') != graph.physical_owner_epoch
            or current.get('native_descriptor') != graph.native_descriptor
            or current.get('physical_binding') != graph.physical_binding
        ):
            raise RuntimeStoreError('automation_source_graph_binding_mismatch')
        return graph
    reservation = _i07_native_reservation(_physical_home(graph.profile_id))
    physical_owner_epoch = reservation.get('owner_epoch') if reservation is not None else None
    native_descriptor = reservation.get('native_descriptor') if reservation is not None else None
    physical_binding = reservation.get('physical_binding') if reservation is not None else None
    graph = AutomationProducerGraph(
        authority=graph.authority,
        coordinator=coordinator,
        profile_id=graph.profile_id,
        instance_id=graph.instance_id,
        authority_epoch=graph.authority_epoch,
        database=graph.database,
        database_path=graph.database_path,
        indexes=graph.indexes,
        producer_families=graph.producer_families,
        physical_owner_epoch=physical_owner_epoch,
        native_descriptor=native_descriptor,
        physical_binding=physical_binding,
    )
    authority._preservation_automation_source_graph = graph
    return graph


def preservation_owner_evidence(
    authority, *, process_instance, authority_epoch, close_generation, owner_id,
):
    """Build the dedicated I07 evidence, including active terminal or typed zero-work paths."""
    from gateway.runtime_custody import OWNER_SPECS

    coordinator = authority._preservation_coordinator()
    if authority._route_wake_inbound_seen is True:
        snapshot = _i07_terminal_receipt_snapshot(authority, coordinator, close_generation)
    else:
        snapshot = automation_zero_work_snapshot(authority, coordinator, close_generation)
    if (
        coordinator is None
        or snapshot is None
        or process_instance != coordinator.process_instance
        or authority_epoch != coordinator.authority_epoch
        or coordinator.status().get('close_generation') != close_generation
        or not isinstance(owner_id, str)
        or not owner_id
    ):
        return None
    return {
        'source': OWNER_SPECS['I07_automation_route_wakes'].source,
        'process_instance': process_instance,
        'authority_epoch': authority_epoch,
        'close_generation': close_generation,
        'owner_id': owner_id,
        'observed': True,
        'source_snapshot': snapshot,
    }


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
    def _register_automation_source_graph(self, coordinator=None):
        return register_automation_source_graph(self, coordinator)
    def _preservation_automation_handle_is_canonical(self, handle):
        if getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES:
            return True
        roots = getattr(self, '_preservation_automation_roots', None)
        if not isinstance(roots, dict):
            return False
        coordinator = self._preservation_coordinator()
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        records = getattr(coordinator, '_records', None) if coordinator is not None else None
        record = records.get(lease_id) if isinstance(records, dict) else None
        direct_root = any(value is handle for value in roots.values())
        if not direct_root and isinstance(records, dict) and record is not None:
            # An I08 child is authentic only through its exact canonical parent.
            parent_id = getattr(getattr(handle, 'lease', None), 'parent_lease_id', None)
            parent_record = records.get(parent_id)
            parent_handle = getattr(parent_record, 'handle', None)
            direct_root = any(value is parent_handle for value in roots.values())
        return (
            direct_root
            and record is not None
            and getattr(record, 'handle', None) is handle
            and getattr(handle, 'profile_id', None) == self.profile_id
            and getattr(handle, 'authority_epoch', None) == self.epoch
            and getattr(handle, 'process_instance', None) == self.instance_id
            and getattr(handle, 'coordinator_id', None) == getattr(coordinator, '_coordinator_id', None)
            and getattr(record, 'state', None) in {'accepted', 'finalizing', 'unknown'})

    def _preservation_automation_identity_matches(
            self, handle, *, request_id, principal_id, session_id, source_identity,
            delivery_id=None):
        """Match every retained root descriptor before an existing root can be reused."""
        if not self._preservation_automation_handle_is_canonical(handle):
            return False
        pending = self._preservation_pending_roots.get(id(handle))
        retained = []
        if pending is not None:
            retained.append(('pending', pending))
        for binding in getattr(self, '_preservation_automation_bindings', {}).values():
            if isinstance(binding, dict) and binding.get('handle') is handle:
                retained.append(('binding', binding))
        if not retained:
            return False
        for kind, value in retained:
            if kind == 'pending':
                if (value.get('handle') is not handle
                        or value.get('request_id') != request_id
                        or value.get('principal_id') != principal_id
                        or value.get('session_id') != session_id
                        or value.get('source_identity') != source_identity):
                    return False
                stored_delivery = value.get('delivery_id')
            else:
                descriptor = value.get('descriptor')
                if not _automation_descriptor_complete(descriptor):
                    return False
                if any(descriptor.get(field) != expected for field, expected in {
                    'request_id': request_id,
                    'principal_id': principal_id,
                    'session_id': session_id,
                    'source_identity': source_identity,
                    'profile_id': self.profile_id,
                }.items()):
                    return False
                bound_row = value.get('row')
                if isinstance(bound_row, dict) and any(
                        bound_row.get(field) != expected for field, expected in {
                            'request_id': request_id,
                            'principal_id': principal_id,
                            'target_session_id': session_id,
                        }.items()):
                    return False
                stored_delivery = value.get('delivery_id')
                if stored_delivery is None:
                    stored_delivery = descriptor.get('delivery_id')
            # Omitted delivery ids preserve a retained id; a new id never reuses this root.
            if stored_delivery is None:
                if delivery_id is not None:
                    return False
            elif delivery_id is not None and stored_delivery != delivery_id:
                return False
        return True

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
        # Some embedded authorities install the coordinator directly rather than through the
        # full runtime bootstrap.  Bind the exact source graph at the producer boundary before
        # reserving this root; a close-time fallback would authenticate work only after the fact.
        register_graph = getattr(self, '_register_automation_source_graph', None)
        if not callable(register_graph):
            raise RuntimeStoreError('automation_source_graph_invalid')
        register_graph(coordinator)
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
            records = getattr(coordinator, '_records', None)
            record = records.get(
                getattr(getattr(existing, 'lease', None), 'lease_id', None)
            ) if isinstance(records, dict) else None
            if record is None or record.handle is not existing:
                # A source-owned root with a missing/replaced coordinator record is not a stale
                # cache entry that can be swapped for a fresh lease.  Retain the opaque mapping
                # and refuse so a lost/unknown root cannot silently acquire a new owner.
                raise RuntimeStoreError('preservation_custody_unknown')
            if (existing.owner_class != owner_class
                    or existing.profile_id != self.profile_id
                    or existing.authority_epoch != self.epoch
                    or existing.owner_id != self._automation_owner_id(
                        owner_class, session_id, request_id)):
                raise RuntimeStoreError('preservation_owner_mismatch')
            if not self._preservation_automation_identity_matches(
                    existing, request_id=request_id, principal_id=principal_id,
                    session_id=session_id, source_identity=source_identity,
                    delivery_id=delivery_id):
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
        owner_id = self._automation_owner_id(owner_class, session_id, request_id)
        handle = self._preservation_root(owner_class, owner_id)
        self._route_wake_inbound_seen |= owner_class == 'I07_automation_route_wakes'
        # This is the sole fresh-root birth seam.  _preservation_root returned the exact
        # coordinator-owned handle above; seal its complete source tuple before publishing the
        # root map.  The ordinary pending-root setter remains strict for every existing, bound,
        # or rehomed handle and cannot be used as a canonical-handle-only overwrite path.
        self._preservation_pending_roots[id(handle)] = {
            'handle': handle,
            'owner_class': owner_class,
            'principal_id': principal_id,
            'session_id': session_id,
            'request_id': request_id,
            'source_identity': source_identity,
            'delivery_id': delivery_id,
        }
        self._preservation_automation_roots[root_key] = handle
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
        home = str(_physical_home(Path(self.db.db_path).parent))
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
                or not _physical_home_matches(
                    descriptor.get('profile_home'), Path(self.db.db_path).parent,
                )
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
        allow_terminal = isinstance(row, dict) and row.get('status') == 'terminal'
        if binding is not None and (
                handle is None
                or not self._preservation_automation_handle_is_canonical(handle)
                or not self._preservation_automation_row_matches(
                    row, handle, allow_terminal=allow_terminal)):
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
                or not _physical_home_matches(
                    descriptor.get('profile_home'), Path(self.db.db_path).parent,
                )
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
        home = str(_physical_home(Path(self.db.db_path).parent))
        owner_class = descriptor.get('owner_class')
        if (owner_class not in _AUTOMATION_OWNER_CLASSES
                or not _physical_home_matches(descriptor.get('profile_home'), home)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        if any((descriptor.get(name) != value for name, value in {
            'owner_class': owner_class,
            'profile_id': self.profile_id,
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
        if handle is None:
            return
        owner_class = getattr(handle, 'owner_class', None)
        existing = self._preservation_pending_roots.get(id(handle))
        if owner_class in _AUTOMATION_OWNER_CLASSES:
            if (not isinstance(source_identity, str) or not source_identity
                    or not self._preservation_automation_handle_is_canonical(handle)):
                raise RuntimeStoreError('preservation_owner_mismatch')
            if existing is not None:
                if not self._preservation_automation_identity_matches(
                        handle, request_id=request_id, principal_id=principal_id,
                        session_id=session_id, source_identity=source_identity,
                        delivery_id=delivery_id):
                    raise RuntimeStoreError('preservation_owner_mismatch')
            else:
                bindings = getattr(self, '_preservation_automation_bindings', {})
                bound = any(isinstance(value, dict) and value.get('handle') is handle
                            for value in bindings.values())
                rehome = next((value for value in getattr(
                    self, '_preservation_automation_rehomes', {}).values()
                    if isinstance(value, dict) and value.get('handle') is handle), None)
                if rehome is None:
                    # Once pending state has been consumed, only the exact retained binding may
                    # re-enter this seam.  A canonical opaque handle by itself carries no
                    # principal/source/request tuple and must never be allowed to overwrite it.
                    if (not bound
                            or not self._preservation_automation_identity_matches(
                                handle, request_id=request_id, principal_id=principal_id,
                                session_id=session_id, source_identity=source_identity,
                                delivery_id=delivery_id)):
                        raise RuntimeStoreError('preservation_owner_mismatch')
                else:
                    if any(rehome.get(field) != expected for field, expected in {
                            'principal_id': principal_id, 'request_id': request_id,
                            'source_identity': source_identity,
                        }.items()):
                        raise RuntimeStoreError('preservation_owner_mismatch')
                    if rehome.get('new_session_id') != session_id:
                        raise RuntimeStoreError('preservation_owner_mismatch')
                    retained_delivery = rehome.get('delivery_id')
                    if (retained_delivery is not None
                            and delivery_id not in (None, retained_delivery)):
                        raise RuntimeStoreError('preservation_owner_mismatch')
            if delivery_id is None and existing is not None:
                delivery_id = existing.get('delivery_id')
            if delivery_id is None and existing is None:
                if rehome is not None:
                    delivery_id = rehome.get('delivery_id')
                else:
                    binding = next((value for value in getattr(
                        self, '_preservation_automation_bindings', {}).values()
                        if isinstance(value, dict) and value.get('handle') is handle), None)
                    if isinstance(binding, dict):
                        delivery_id = binding.get('delivery_id') or (
                            binding.get('descriptor') or {}).get('delivery_id')
        self._preservation_pending_roots[id(handle)] = {
                'handle': handle,
                'owner_class': owner_class,
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
            from gateway.session_i09_managed_replay import prepare_i09_payload
            durable_payload = await prepare_i09_payload(
                self, ref, request_id, payload, handle, descriptor=descriptor,
                source_identity=source_identity, delivery_id=delivery_id,
                kanban_card=kanban_card,
            )
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
                    or row.get('request_id') != pending.get('request_id')
                    or pending.get('source_identity') != descriptor.get('source_identity')
                    or ((pending.get('delivery_id') is None)
                        != (descriptor.get('delivery_id') is None))
                    or (pending.get('delivery_id') is not None
                        and pending.get('delivery_id') != descriptor.get('delivery_id'))):
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
                or pending.get('session_id') != row.get('target_session_id')
                or pending.get('source_identity') != descriptor.get('source_identity')
                or ((pending.get('delivery_id') is None)
                    != (descriptor.get('delivery_id') is None))
                or (pending.get('delivery_id') is not None
                    and pending.get('delivery_id') != descriptor.get('delivery_id'))):
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
        from gateway.session_automation_rehome import prepare_automation_rehome

        return prepare_automation_rehome(self, admission_id, row, new_session_id)

    def _preservation_abort_automation_rehome(self, admission_id):
        from gateway.session_automation_rehome import abort_automation_rehome

        return abort_automation_rehome(self, admission_id)

    def _preservation_rehome_source_matches(self, admission_id, handle, rehome):
        from gateway.session_automation_rehome import rehome_source_matches

        return rehome_source_matches(self, admission_id, handle, rehome)

    def _preservation_finish_automation_rehome(self, admission_id):
        from gateway.session_automation_rehome import finish_automation_rehome

        return finish_automation_rehome(self, admission_id)

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
        root = getattr(self, '_preservation_handles', {}).get(admission_id)
        if getattr(root, 'owner_class', None) in _AUTOMATION_OWNER_CLASSES:
            try:
                from hermes_state_runtime import get_session_admission
                row = get_session_admission(self.db, admission_id=admission_id)
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                row = None
            descriptor = self._preservation_automation_row_descriptor(row)
            binding = getattr(self, '_preservation_automation_bindings', {}).get(admission_id)
            if (not isinstance(row, dict)
                    or not _automation_descriptor_complete(descriptor)
                    or descriptor.get('source_identity') != record.get('source_identity')
                    or not isinstance(binding, dict)
                    or binding.get('handle') is not root
                    or binding.get('source_identity') != descriptor.get('source_identity')):
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
        child_released = True
        if child_key is not None:
            child_released = self._preservation_operation_release_unaccepted(child_key)
            if not child_released:
                # A failed child release is an accepted/unknown custody boundary, not an absent
                # row. Keep both indexes so the exact child can be reconciled; never release the
                # parent or publish a no-send/refusal while that child remains live.
                if handle is not None:
                    coordinator = self._preservation_coordinator()
                    if coordinator is not None:
                        coordinator.mark_unknown(handle, 'read_failed')
                return False
            for admission_id, operation_key in list(self._preservation_automation_children.items()):
                if operation_key == child_key:
                    self._preservation_automation_children.pop(admission_id, None)
        if handle is None:
            return child_released
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            require = getattr(self, '_require_preservation_coordinator', None)
            coordinator = require() if callable(require) else None
            if coordinator is None:
                raise RuntimeStoreError('runtime_custody_unavailable')
        result = coordinator.release_unaccepted(handle)
        # A stale/foreign refusal cannot prove that this exact root was never accepted.  Retain
        # the root/index for reconciliation; only the source-issued ``None`` result permits the
        # public no-effect path to forget it.
        if result is None:
            for key, value in list(self._preservation_automation_roots.items()):
                if value is handle:
                    self._preservation_automation_roots.pop(key, None)
            self._preservation_forget_pending_root(handle)
            return True
        coordinator.mark_unknown(handle, 'read_failed')
        return False

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
