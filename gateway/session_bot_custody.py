"""Bot admission custody setup and ordered commit boundaries.

The mailbox remains a delivery projection.  These helpers only keep the real I07 root and I06
child attached to the canonical admission until the result projection is durable.
"""
import logging
from dataclasses import dataclass
from pathlib import Path

from gateway.platforms.event import MessageEvent
from hermes_state_runtime import RuntimeStoreError, get_session_admission


@dataclass(frozen=True)
class BotExecutionBinding:
    """The authenticated I07 root/I06 child pair consumed by Bot ingress."""

    admission_id: str
    root_handle: object
    child_key: str
    child_handle: object
    source_identity: str


def _validate_bot_admission_row(authority, record, row, *, admission_id, request_id,
                                receipt_epoch=None, require_custody=True):
    """Authenticate the exact durable Bot row before binding or projecting its receipt.

    A queued row is deliberately unclaimed: its durable generation is ``None``.  Only a real
    ``started`` row may pin a positive execution generation; this helper never invents generation
    zero or substitutes the current FIFO head for a missing row.
    """
    if not isinstance(row, dict) or row.get('admission_id') != admission_id:
        raise RuntimeStoreError('preservation_owner_unmapped')
    expected_principal = record.get('admission_principal_id')
    if expected_principal is None:
        expected_principal = (record.get('automation_custody') or {}).get('principal_id')
    expected_session = record.get('session_id')
    expected_request = request_id
    if (not isinstance(expected_principal, str) or not expected_principal
            or row.get('principal_id') != expected_principal
            or row.get('target_session_id') != expected_session
            or row.get('request_id') != expected_request):
        raise RuntimeStoreError('preservation_owner_unmapped')
    row_epoch = row.get('owner_epoch')
    if type(row_epoch) is not int or row_epoch < 1:
        raise RuntimeStoreError('stale_epoch')
    if receipt_epoch is not None and row_epoch != receipt_epoch:
        raise RuntimeStoreError('stale_epoch')
    recorded_epoch = record.get('authority_epoch')
    if recorded_epoch is not None and (type(recorded_epoch) is not int or recorded_epoch < 1):
        raise RuntimeStoreError('stale_epoch')
    if recorded_epoch is not None and row_epoch != recorded_epoch:
        # Recovery/adoption may transfer the exact canonical row to this authority's current
        # epoch. That transfer is readable only after the row is terminal; queued/unknown rows
        # retain the original owner epoch and must fail closed.
        adopted_terminal = (
            row.get('status') == 'terminal'
            and row_epoch == getattr(authority, 'epoch', None)
            and row_epoch > recorded_epoch
        )
        if not adopted_terminal:
            raise RuntimeStoreError('stale_epoch')
    if require_custody:
        expected = record.get('automation_custody')
        from gateway.session_automation_custody import row_descriptor
        actual = row_descriptor(row)
        if expected is not None:
            if not isinstance(actual, dict):
                raise RuntimeStoreError('preservation_owner_unmapped')
            # ``admit_automation`` reconstructs the descriptor in its canonical writer.  The
            # delivery id is a mailbox detail and may be absent on that writer's projection; all
            # owner/profile/epoch/session/principal/request/source fields must match exactly.
            for field in ('owner_class', 'owner_id', 'profile_id', 'profile_home',
                          'authority_epoch', 'process_instance', 'close_generation',
                          'coordinator_id', 'session_id', 'principal_id', 'request_id',
                          'source_identity'):
                if actual.get(field) != expected.get(field):
                    raise RuntimeStoreError('preservation_owner_unmapped')
    status, generation = row.get('status'), row.get('generation')
    if status not in {'queued', 'started', 'unknown', 'terminal'}:
        raise RuntimeStoreError('preservation_owner_unmapped')
    unclaimed_allowed = (status in {'queued', 'unknown'}
                         or (status == 'terminal' and row.get('outcome') in {'cancelled', 'rejected'}))
    if ((generation is None and not unclaimed_allowed)
            or (generation is not None and (status == 'queued'
                                            or type(generation) is not int or generation < 1))):
        raise RuntimeStoreError('stale_generation')
    return row


def _pin_bot_generation(authority, record, *, admission_id, request_id, field,
                        receipt_epoch=None, require_custody=True):
    """Pin a committed generation only after the exact row identity has been read back."""
    row = get_session_admission(authority.db, admission_id=admission_id)
    if row is None:
        return None
    _validate_bot_admission_row(
        authority, record, row, admission_id=admission_id, request_id=request_id,
        receipt_epoch=receipt_epoch, require_custody=require_custody,
    )
    pinned = record.get(field)
    if pinned is not None and pinned != row.get('generation'):
        raise RuntimeStoreError('stale_generation')
    if record.get('authority_epoch') is None:
        record['authority_epoch'] = row['owner_epoch']
    status, generation = row.get('status'), row.get('generation')
    if status == 'queued':
        record.pop(field, None)
        record['generation_state'] = 'unclaimed'
    elif status == 'started' or (status == 'terminal' and generation is not None):
        record[field] = generation
        record['generation_state'] = 'pinned'
    elif status == 'unknown':
        if generation is not None:
            record[field] = generation
        record['generation_state'] = 'unknown'
    else:
        record.pop(field, None)
        record['generation_state'] = 'terminal_unclaimed'
    return row


def prepare_bot_admission(authority, actor, home, root, key, message, ref, live, entry,
                          author=None, legacy=None, notification_category='result'):
    from gateway.session_automation import automation_notification_metadata, automation_source_identity

    notification = automation_notification_metadata({'notification_category': notification_category})
    path = root / f'{key}.json'
    event = MessageEvent(text=message, source=live.source, internal=True,
                         message_id='bot:' + key,
                         metadata={'gateway_session_key': live.route,
                                   'gateway_session_id': entry.session_id})
    event.metadata.update(notification)
    if author is not None:
        event.metadata['turn_author'] = dict(author)
    principal_id = 'automation:' + entry.session_key
    source_identity = automation_source_identity(event, entry.session_key)
    automation_root = authority._preservation_automation_root(
        request_id='bot:' + key, principal_id=principal_id, session_id=ref.session_id,
        source_identity=source_identity, delivery_id=key)
    child_key = None
    if automation_root is not None:
        try:
            child_key = authority._preservation_operation_start(
                'I06_busy_followup_delivery', principal_id=principal_id, session_id=ref.session_id,
                operation='bot-delivery', request_id='bot:' + key, generation='pre-admission',
                source_identity=source_identity, parent_handle=automation_root)
        except BaseException:
            # The root was reserved before the child reservation.  A failed child reservation has
            # no durable admission to reconcile, so release this exact root; if release itself is
            # uncertain the coordinator must retain it as unknown rather than minting a successor.
            try:
                authority._preservation_release_automation_unaccepted(None, automation_root)
            except BaseException:
                logging.getLogger(__name__).exception('Unaccepted Bot custody release failed')
                coordinator = authority._preservation_coordinator()
                if coordinator is not None:
                    coordinator.mark_unknown(automation_root, 'join_failed')
            raise
    custody = (authority._preservation_automation_descriptor(
        automation_root, request_id='bot:' + key, principal_id=principal_id, session_id=ref.session_id,
        source_identity=source_identity, delivery_id=key) if automation_root is not None else None)
    # Pin the physical target before committing. A process death in this two-store window leaves
    # an explicit unknown record, never a new target. The custody tuple is a durable handoff
    # witness, while the mailbox remains only a projection/readback boundary.
    record = dict(legacy or {}, delivery_id=key, profile_home=str(home), session_id=ref.session_id,
                  principal_id=actor.subject, admission_principal_id=principal_id,
                  admission_request_id='bot:' + key, message=message, status='ambiguous')
    record.update(notification)
    if custody is not None:
        record['automation_custody'] = custody
    if 'session:operator' in actor.capabilities:
        record['operator'] = True  # admitted on operator access: its one retry keeps it
    if author is not None:
        record['author'] = dict(author)
    return path, event, automation_root, child_key, record


async def _write_bot_pin(authority, home, key, record, child_key, automation_root, legacy):
    """Write the mailbox projection and reconcile whether that write committed."""
    from gateway.session_bot_mailbox import read_receipt, write_receipt
    try:
        written = await write_receipt(home, record, expect=(legacy or {}).get('status'))
    except BaseException:
        # Re-read before releasing: the physical pin may have committed despite a cancelled
        # caller or a filesystem exception.
        _, existing = await read_receipt(home, key)
        if existing is None:
            authority._preservation_release_automation_unaccepted(child_key, automation_root)
        else:
            if child_key is not None:
                authority._preservation_operation_unknown(child_key, 'read_failed')
            if automation_root is not None:
                authority._preservation_automation_bindings.setdefault(key, {'handle': automation_root})
                authority._preservation_coordinator().mark_unknown(automation_root, 'read_failed')
        raise
    if not written:
        if legacy is not None:
            authority._preservation_release_automation_unaccepted(child_key, automation_root)
            return False
        if child_key is not None:
            authority._preservation_operation_unknown(child_key, 'unmapped_writer')
        if automation_root is not None:
            authority._preservation_coordinator().mark_unknown(automation_root, 'unmapped_writer')
        raise RuntimeStoreError('admission_conflict')
    return True


async def _retain_committed_bot_pin(authority, home, key, ref, record, event=None):
    """Persist the exact admission id when the canonical writer committed before an error."""
    try:
        from gateway.session_bot import _admission_at
        admission_id = _admission_at(authority, ref.session_id, 'bot:' + key)
        if admission_id is None:
            return
        row = get_session_admission(authority.db, admission_id=admission_id)
    except BaseException:
        logging.getLogger(__name__).exception(
            'Bot delivery %s admission identity could not be re-read after an error', key)
        return
    if (not isinstance(row, dict)
            or row.get('principal_id') != record.get('admission_principal_id')
            or row.get('target_session_id') != record.get('session_id')
            or row.get('request_id') != record.get('admission_request_id')):
        return
    if getattr(event, '_gateway_accepted', False):
        # The canonical writer already acknowledged this exact row, but the caller failed after
        # that acknowledgement. Keep the mailbox as the original ambiguous pin: its exact
        # principal/session/request still lets owner recovery prove the row exists, while no
        # public readback is promoted until a terminal row is observed.
        record.pop('admission_id', None)
        record.pop('authority_epoch', None)
        record['_committed_admission_id'] = admission_id
        record['_committed_authority_epoch'] = row['owner_epoch']
        record['status'] = 'ambiguous'
        try:
            from gateway.session_bot_mailbox import write_receipt
            await write_receipt(home, record)
        except BaseException:
            logging.getLogger(__name__).exception(
                'Bot delivery %s ambiguous pin could not be retained after accepted-row error', key)
        return
    record.update(status='ambiguous', admission_id=admission_id,
                  authority_epoch=row.get('owner_epoch'))
    try:
        from gateway.session_bot_mailbox import write_receipt
        await write_receipt(home, record)
    except BaseException:
        # The original writer/validation error remains the source of truth; a failed projection
        # readback must not turn an uncertain committed row into a release decision.
        logging.getLogger(__name__).exception(
            'Bot delivery %s committed row could not be projected after admission error', key)


def _stamp_bot_callback_identity(authority, child_key, row):
    """Retain the exact Bot child identity consumed by the callback classifier.

    Bot delivery uses the generic I06 operation record, but it has no classic producer event or
    bridge.  The callback boundary still requires the I06 identity projection, so carry only the
    source-owned operation fields that were read back from the canonical row and opaque handle.
    This does not create a classic bridge or receipt.
    """
    records = getattr(authority, '_preservation_operations', None)
    record = records.get(child_key) if isinstance(records, dict) else None
    handle = record.get('handle') if isinstance(record, dict) else None
    if (
        not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('operation') != 'bot-delivery'
        or not isinstance(row, dict)
        or row.get('admission_id') != record.get('admission_id')
        or row.get('target_session_id') != record.get('session_id')
        or row.get('principal_id') != record.get('principal_id')
        or row.get('request_id') != record.get('request_id')
        or row.get('owner_epoch') != getattr(authority, 'epoch', None)
        or getattr(handle, 'owner_class', None) != record.get('owner_class')
        or getattr(handle, 'profile_id', None) != getattr(authority, 'profile_id', None)
        or getattr(handle, 'authority_epoch', None) != getattr(authority, 'epoch', None)
        or getattr(handle, 'process_instance', None) != getattr(authority, 'instance_id', None)
        or not isinstance(record.get('source_identity'), str)
        or not record.get('source_identity')
    ):
        raise RuntimeStoreError('preservation_owner_unmapped')
    record['classic_identity'] = {
        'owner_class': record['owner_class'],
        'profile_id': handle.profile_id,
        'process_instance': handle.process_instance,
        'authority_epoch': handle.authority_epoch,
        'session_id': record['session_id'],
        'principal_id': record['principal_id'],
        'request_id': record['request_id'],
        'source_identity': record['source_identity'],
    }


def _bot_row_signature(row):
    """Recognize the source-owned Bot envelope before looking at operation indexes."""
    if not isinstance(row, dict) or not isinstance(row.get('request_id'), str):
        return None
    if not row['request_id'].startswith('bot:'):
        return None
    payload = row.get('payload')
    local = payload.get('local_automation_v1') if isinstance(payload, dict) else None
    return local if isinstance(local, dict) else {}, (
        local.get('custody') if isinstance(local, dict) else None
    )


def _bot_descriptor_matches(authority, row, local, descriptor):
    if (not isinstance(local, dict) or not isinstance(descriptor, dict)
            or descriptor.get('owner_class') != 'I07_automation_route_wakes'
            or local.get('identity') != row.get('request_id')
            or local.get('owner') != row.get('target_session_id')
            or descriptor.get('request_id') != row.get('request_id')
            or descriptor.get('session_id') != row.get('target_session_id')
            or descriptor.get('principal_id') != row.get('principal_id')
            or descriptor.get('profile_id') != getattr(authority, 'profile_id', None)
            or descriptor.get('profile_home') != str(Path(authority.db.db_path).parent.resolve())
            or not isinstance(descriptor.get('source_identity'), str)
            or not descriptor.get('source_identity')):
        return False
    delivery_id = descriptor.get('delivery_id')
    return delivery_id is None or delivery_id == row.get('request_id')[4:]


def _bot_execution_unknown(authority, admission_id, keys, reason='bot_owner_unknown'):
    coordinator = authority._preservation_coordinator()
    canonical_handles = []
    records = getattr(coordinator, '_records', {}) if coordinator is not None else {}
    operations = getattr(authority, '_preservation_operations', None)
    for key in dict.fromkeys(keys):
        record = operations.get(key) if isinstance(operations, dict) else None
        handle = record.get('handle') if isinstance(record, dict) else None
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        canonical = records.get(lease_id) if isinstance(records, dict) else None
        if canonical is not None and canonical.handle is not handle:
            canonical_handles.append(canonical.handle)
    for handle in canonical_handles:
        coordinator.mark_unknown(handle, reason)
    for key in dict.fromkeys(keys):
        authority._preservation_operation_unknown(key, reason)
    handles = getattr(authority, '_preservation_handles', None)
    root = handles.get(admission_id) if isinstance(handles, dict) else None
    if root is not None and coordinator is not None:
        coordinator.mark_unknown(root, reason)
    mark_unknown = getattr(authority, '_preservation_mark_unknown', None)
    if callable(mark_unknown):
        mark_unknown(admission_id, reason)
    from gateway.session_managed_worker import ManagedExecutionUnknown
    raise ManagedExecutionUnknown('bot_owner_unknown')


def _bot_handle_graph_valid(authority, root, child):
    coordinator = authority._preservation_coordinator()
    if coordinator is None or root is None or child is None:
        return False
    expected = (getattr(authority, 'profile_id', None), getattr(authority, 'epoch', None),
                getattr(authority, 'instance_id', None), getattr(coordinator, '_coordinator_id', None))
    for handle in (root, child):
        if (getattr(handle, 'profile_id', None), getattr(handle, 'authority_epoch', None),
                getattr(handle, 'process_instance', None), getattr(handle, 'coordinator_id', None)) != expected:
            return False
    if getattr(root, 'physical_owner_epoch', None) != getattr(child, 'physical_owner_epoch', None):
        return False
    try:
        if not coordinator.profile_scope_valid(
                root.profile_id, root.authority_epoch, require_serving=False,
                allow_retiring=True, physical_owner_epoch=root.physical_owner_epoch):
            return False
        if not coordinator.profile_scope_valid(
                child.profile_id, child.authority_epoch, require_serving=False,
                allow_retiring=True, physical_owner_epoch=child.physical_owner_epoch):
            return False
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    root_id = getattr(getattr(root, 'lease', None), 'lease_id', None)
    child_id = getattr(getattr(child, 'lease', None), 'lease_id', None)
    if (not isinstance(root_id, str) or not isinstance(child_id, str)
            or getattr(getattr(child, 'lease', None), 'parent_lease_id', None) != root_id):
        return False
    with coordinator._lock:
        records = getattr(coordinator, '_records', {})
        root_record, child_record = records.get(root_id), records.get(child_id)
        return (
            root_record is not None and root_record.handle is root
            and root_record.state in {'accepted', 'finalizing'}
            and child_record is not None and child_record.handle is child
            and child_record.state in {'accepted', 'finalizing'}
        )


def _bot_root_map_witness_valid(authority, row, descriptor, root):
    """Authenticate the retained I07 root through its canonical delivery-key index.

    The generic automation writer may rebuild a pending-root snapshot without the optional
    delivery id. In that case the durable Bot request id carries the same source-owned key. The
    opaque root object and a unique canonical map entry are still required; this never reconstructs
    or selects a root from a string alone.
    """
    roots = getattr(authority, '_preservation_automation_roots', None)
    if not isinstance(roots, dict):
        return False
    delivery_id = descriptor.get('delivery_id') if isinstance(descriptor, dict) else None
    if delivery_id is not None:
        return roots.get(delivery_id) is root
    request_id = row.get('request_id') if isinstance(row, dict) else None
    if not isinstance(request_id, str) or not request_id.startswith('bot:'):
        return False
    root_entries = [(key, value) for key, value in roots.items() if value is root]
    return len(root_entries) == 1 and root_entries[0][0] == request_id[4:]


def _bot_execution_record_valid(authority, row, descriptor, root, child_key, record):
    if (not isinstance(record, dict)
            or record.get('owner_class') != 'I06_busy_followup_delivery'
            or record.get('operation') != 'bot-delivery'
            or record.get('unknown') is True
            or record.get('admission_id') != row.get('admission_id')
            or record.get('parent_handle') is not root
            or record.get('principal_id') != row.get('principal_id')
            or record.get('session_id') != row.get('target_session_id')
            or record.get('request_id') != row.get('request_id')
            or record.get('admission_request_id', row.get('request_id')) != row.get('request_id')
            or record.get('source_identity') != descriptor.get('source_identity')
            or record.get('handle') is None
            or not _bot_handle_graph_valid(authority, root, record.get('handle'))):
        return False
    bindings = getattr(authority, '_preservation_automation_bindings', None)
    binding = bindings.get(row.get('admission_id')) if isinstance(bindings, dict) else None
    if not isinstance(binding, dict) or binding.get('handle') is not root:
        return False
    if not _bot_root_map_witness_valid(authority, row, descriptor, root):
        return False
    if not authority._preservation_automation_row_matches(row, root):
        return False
    try:
        reread = authority._preservation_operation_row(child_key, expected_parent=root)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return False
    return (
        isinstance(reread, dict)
        and reread.get('admission_id') == row.get('admission_id')
        and reread.get('status') == row.get('status') == 'started'
        and reread.get('generation') == row.get('generation')
        and type(row.get('generation')) is int and row['generation'] >= 1
        and record.get('generation') == row.get('generation')
        and record.get('generation_state') == 'pinned'
        and record.get('producer_object') is None
    )


def bot_execution_binding_for_admission(authority, row):
    """Authenticate a started Bot admission and bypass the classic producer resolver.

    Bot delivery deliberately owns an I07 root and an I06 child without a classic producer event.
    Every field below comes from the canonical row, retained opaque handles, and the coordinator's
    live records; a missing or conflicting witness retains the exact handles as UNKNOWN.
    """
    signature = _bot_row_signature(row)
    if signature is None:
        return None
    local, descriptor = signature
    admission_id = row.get('admission_id') if isinstance(row, dict) else None
    operations = getattr(authority, '_preservation_operations', None)
    candidates = [
        (key, record) for key, record in (operations.items() if isinstance(operations, dict) else ())
        if (isinstance(record, dict)
            and record.get('owner_class') == 'I06_busy_followup_delivery'
            and record.get('operation') == 'bot-delivery'
            and record.get('admission_id') == admission_id)
    ]
    candidate_keys = [key for key, _record in candidates]
    if (not isinstance(admission_id, str) or not admission_id
            or not _bot_descriptor_matches(authority, row, local, descriptor)):
        _bot_execution_unknown(authority, admission_id, candidate_keys)
    handles = getattr(authority, '_preservation_handles', None)
    children = getattr(authority, '_preservation_automation_children', None)
    root = handles.get(admission_id) if isinstance(handles, dict) else None
    child_key = children.get(admission_id) if isinstance(children, dict) else None
    record = operations.get(child_key) if isinstance(operations, dict) and isinstance(child_key, str) else None
    if (len(candidates) != 1 or child_key != candidates[0][0]
            or root is None or record is not candidates[0][1]
            or not _bot_execution_record_valid(authority, row, descriptor, root, child_key, record)):
        _bot_execution_unknown(authority, admission_id, candidate_keys or ([child_key] if child_key else []))
    return BotExecutionBinding(
        admission_id=admission_id, root_handle=root, child_key=child_key,
        child_handle=record['handle'], source_identity=descriptor['source_identity'],
    )


async def _admit_bot_row(authority, home, key, ref, live, event, record, child_key, automation_root, legacy):
    """Commit the canonical row and retain exact custody on every refusal or unknown result."""
    from gateway.session_bot import _OWNER_STATE_REFUSALS, _admission_at
    from gateway.session_bot_mailbox import refuse_pin, withdraw_pin
    try:
        kwargs = ({'_preservation_handle': automation_root,
                   '_preservation_child_key': child_key} if automation_root is not None else {})
        receipt = await authority.admit_automation(
            authority.runner._adapter_for_source(live.source), event, 'bot:' + key, **kwargs)
        row = get_session_admission(authority.db, admission_id=receipt.admission_id)
        _validate_bot_admission_row(
            authority, record, row, admission_id=receipt.admission_id,
            request_id='bot:' + key, receipt_epoch=getattr(receipt, 'authority_epoch', None),
            require_custody=True,
        )
        if (child_key is not None and getattr(receipt, 'status', None) != 'terminal'
                and authority._preservation_handles.get(receipt.admission_id) is automation_root):
            authority._preservation_operation_bind_admission(
                child_key, receipt.admission_id, expected_parent=automation_root)
            authority._preservation_bind_automation_child(receipt.admission_id, child_key)
            _stamp_bot_callback_identity(authority, child_key, row)
        elif child_key is not None:
            authority._preservation_operation_release_unaccepted(child_key)
    except RuntimeStoreError as exc:
        await _retain_committed_bot_pin(authority, home, key, ref, record, event)
        if _admission_at(authority, ref.session_id, 'bot:' + key) is not None:
            if child_key is not None:
                authority._preservation_operation_unknown(child_key, 'read_failed')
            if automation_root is not None:
                authority._preservation_coordinator().mark_unknown(automation_root, 'read_failed')
            raise  # committed after all: the pin stays the explicit unknown record
        if exc.reason in _OWNER_STATE_REFUSALS:
            if legacy is None:
                # Refused before any INSERT, not for the delivery itself: the same-id resend admits.
                if await withdraw_pin(home, key):
                    authority._preservation_release_automation_unaccepted(child_key, automation_root)
                elif child_key is not None:
                    authority._preservation_operation_unknown(child_key, 'read_failed')
            else:
                # The legacy ticket remains the FIFO's durable pin, but this admission never
                # existed. Release only the exact pre-row custody so the next scan can retry.
                authority._preservation_release_automation_unaccepted(child_key, automation_root)
        else:
            # Refused for good with nothing committed: a terminal receipt, not an unknown pin.
            logging.getLogger(__name__).info('Bot Chat delivery %s refused (%s)', key, exc.reason)
            await refuse_pin(home, key)
            authority._preservation_release_automation_unaccepted(child_key, automation_root)
        raise
    except BaseException:
        # The ordered writer may still commit after a cancelled caller. Reconcile the exact row
        # before deciding whether either lease can be released.
        await _retain_committed_bot_pin(authority, home, key, ref, record, event)
        if _admission_at(authority, ref.session_id, 'bot:' + key) is None:
            if child_key is not None:
                authority._preservation_operation_unknown(child_key, 'join_failed')
            if automation_root is not None:
                authority._preservation_coordinator().mark_unknown(automation_root, 'join_failed')
        else:
            if child_key is not None:
                authority._preservation_operation_unknown(child_key, 'read_failed')
            if automation_root is not None:
                authority._preservation_coordinator().mark_unknown(automation_root, 'read_failed')
        raise
    return receipt


async def commit_bot_admission(authority, home, key, ref, live, event, record, child_key,
                               automation_root, legacy):
    """Write the mailbox pin, then commit the canonical FIFO row with exact lease handoff."""
    if not await _write_bot_pin(authority, home, key, record, child_key, automation_root, legacy):
        return None
    return await _admit_bot_row(authority, home, key, ref, live, event, record, child_key, automation_root, legacy)


def _managed_bot_projection_binding(authority, admission_id, record):
    """Return whether this projection still has its exact source-owned I07 binding."""
    descriptor = record.get('automation_custody') if isinstance(record, dict) else None
    if not isinstance(descriptor, dict) or descriptor.get('owner_class') != 'I07_automation_route_wakes':
        return False
    bindings = getattr(authority, '_preservation_automation_bindings', None)
    handles = getattr(authority, '_preservation_handles', None)
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    handle = binding.get('handle') if isinstance(binding, dict) else None
    if (not isinstance(handles, dict)
            or handles.get(admission_id) is not handle
            or getattr(handle, 'owner_class', None) != 'I07_automation_route_wakes'
            or not isinstance(binding, dict)
            or not isinstance(binding.get('descriptor'), dict)
            or any(binding['descriptor'].get(field) != descriptor.get(field) for field in (
                'owner_class', 'owner_id', 'profile_id', 'profile_home', 'authority_epoch',
                'process_instance', 'close_generation', 'coordinator_id', 'session_id',
                'principal_id', 'request_id', 'source_identity'))):
        raise RuntimeStoreError('preservation_owner_unmapped')
    return True


async def finish_bot_projection(authority, home, record, written):
    """Close the exact delivery child only after terminal mailbox projection readback."""
    from gateway.session_bot_mailbox import read_receipt
    from tools.bot_live_delivery import _TERMINAL

    admission_id = record.get('admission_id') or record.get('_committed_admission_id')
    handles = getattr(authority, '_preservation_handles', None)
    if handles is None:
        # Legacy receipt projections predate custody. A managed descriptor cannot use that
        # compatibility path when its authority has lost the source-owned handle registry.
        if record.get('admission_principal_id') or record.get('automation_custody') is not None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        return
    handle = handles.get(admission_id)
    if handle is None or record.get('status') not in _TERMINAL:
        return
    managed = _managed_bot_projection_binding(authority, admission_id, record)
    try:
        if written is not True:
            raise RuntimeStoreError('storage_unavailable')
        _, saved = await read_receipt(home, record['delivery_id'])
        fields = ('delivery_id', 'admission_id', 'profile_home', 'session_id', 'principal_id',
                  'admission_principal_id', 'admission_request_id', 'authority_epoch',
                  'execution_generation', 'automation_custody', 'status', 'reply',
                  'retry_admission_id', 'retry_execution_generation', 'error', 'reason')
        if not isinstance(saved, dict) or any(saved.get(key) != record.get(key) for key in fields):
            raise RuntimeStoreError('preservation_owner_unmapped')
        row = _pin_bot_generation(
            authority, record, admission_id=admission_id,
            request_id=record['admission_request_id'], field='execution_generation',
        )
        if row is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if row['status'] != 'terminal':
            if (record.get('status') == 'ambiguous'
                    and row['status'] in {'queued', 'started', 'unknown'}):
                # This is a typed pending projection boundary. The exact row and receipt were
                # already authenticated above; retain the existing I07/I06 custody untouched
                # (an old owner may already have marked it UNKNOWN, while a successor root must
                # remain adoptable), and let the next record/startup pass continue. No replay or
                # release is implied; an unexpected identity failure still reaches the catch.
                return
            raise RuntimeStoreError('preservation_owner_unmapped')
        _reconcile_unknown_bot_handles(authority, admission_id, row)
        if managed:
            # The mailbox is a projection, but this exact terminal readback is also the Bot
            # child's source-owned finalizer boundary.  Retire only I06 here; the typed
            # DeliveryReceipt/callback path still owns the I07 root release and re-reads the
            # same binding through the normal terminal gate.
            authority._preservation_finish_automation(
                admission_id, finalizer_id='bot-projection:' + record['delivery_id'],
                keep_root=True,
            )
            # The first terminal gate may have observed this exact I06 child as still active
            # and left the I07 root finalizing.  Re-enter that same gate after the child receipt
            # is committed; it performs the complete callback/sidecar/marker checks again.
            authority._preservation_release_terminal(admission_id)
            return
    except BaseException:
        authority._preservation_finish_automation(
            admission_id, finalizer_id='bot-projection:' + record['delivery_id'], unknown=True,
        )
        raise
    authority._preservation_finish_automation(
        admission_id, finalizer_id='bot-projection:' + record['delivery_id'],
    )


def _validate_retired_bot_projection(authority, saved, admission_id):
    """Validate the read-only deleted-session boundary for one private Bot pin."""
    from hermes_state_mutation_retirement import retired_session
    if not retired_session(authority.db, saved['session_id']):
        return False
    # Retirement compacts the canonical payload before deleting the live row, so the tombstone
    # cannot carry the custody descriptor. Authenticate the durable identity fields that survive
    # there, then bind the saved descriptor to this authority/profile.
    retired = get_session_admission(authority.db, admission_id=admission_id)
    if not isinstance(retired, dict):
        raise RuntimeStoreError('preservation_owner_unmapped')
    if any(retired.get(field) != expected for field, expected in {
        'admission_id': admission_id,
        'target_session_id': saved['session_id'],
        'principal_id': saved.get('admission_principal_id'),
        'request_id': saved.get('admission_request_id'),
        'status': 'terminal',
    }.items()):
        raise RuntimeStoreError('preservation_owner_unmapped')
    retired_epoch = retired.get('owner_epoch')
    committed_epoch = saved.get('_committed_authority_epoch')
    if (type(retired_epoch) is not int or retired_epoch < 1
            or type(committed_epoch) is not int or committed_epoch < 1
            or retired_epoch != committed_epoch
            or authority.db.get_session(saved['session_id']) is not None):
        raise RuntimeStoreError('preservation_owner_unmapped')
    from pathlib import Path
    descriptor = saved.get('automation_custody')
    actual_home = str(Path(authority.db.db_path).parent.resolve())
    if (saved.get('profile_home') != actual_home or not isinstance(descriptor, dict)
            or any(descriptor.get(field) != expected for field, expected in {
                'owner_class': 'I07_automation_route_wakes',
                'profile_id': getattr(authority, 'profile_id', None),
                'profile_home': actual_home,
                'authority_epoch': retired_epoch,
                'session_id': saved['session_id'],
                'principal_id': retired.get('principal_id'),
                'request_id': retired.get('request_id'),
            }.items())):
        raise RuntimeStoreError('preservation_owner_unmapped')
    owner_id_for = getattr(authority, '_automation_owner_id', None)
    if (not callable(owner_id_for)
            or descriptor.get('owner_id') != owner_id_for(
                'I07_automation_route_wakes', saved['session_id'], retired['request_id'])):
        raise RuntimeStoreError('preservation_owner_unmapped')
    for field in ('owner_id', 'process_instance', 'coordinator_id', 'source_identity'):
        if not isinstance(descriptor.get(field), str) or not descriptor[field]:
            raise RuntimeStoreError('preservation_owner_unmapped')
    if type(descriptor.get('close_generation')) is not int or descriptor['close_generation'] < 0:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if ('delivery_id' in descriptor
            and descriptor['delivery_id'] != saved.get('delivery_id')):
        raise RuntimeStoreError('preservation_owner_unmapped')
    return True


async def preserve_pending_bot_projection(authority, home, record):
    """Keep a private committed pin unchanged while its canonical row is still non-terminal."""
    if not isinstance(record, dict) or not record.get('delivery_id'):
        return None
    from gateway.session_bot_mailbox import read_receipt
    _, saved = await read_receipt(home, record['delivery_id'])
    if not isinstance(saved, dict):
        raise RuntimeStoreError('preservation_owner_unmapped')
    identity_fields = (
        'delivery_id', 'profile_home', 'session_id', 'principal_id', 'admission_principal_id',
        'admission_request_id', 'message', 'automation_custody', '_committed_admission_id',
        '_committed_authority_epoch',
    )
    if any(saved.get(field) != record.get(field) for field in identity_fields):
        raise RuntimeStoreError('preservation_owner_unmapped')
    if saved.get('status') != 'ambiguous' or saved.get('_committed_admission_id') is None:
        return None
    if any(field in saved for field in ('admission_id', 'authority_epoch', 'profile_id', 'reply')):
        raise RuntimeStoreError('preservation_owner_unmapped')
    admission_id = saved['_committed_admission_id']
    if not isinstance(admission_id, str) or not admission_id:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if not isinstance(saved.get('session_id'), str) or not saved['session_id']:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if _validate_retired_bot_projection(authority, saved, admission_id):
        # The deleted target's exact admission is now permanently fenced. Keep the private pin
        # byte-for-byte unchanged and refuse the historical result; never replay or mint a root.
        raise RuntimeStoreError('unknown_execution')
    probe = dict(saved)
    row = _pin_bot_generation(
        authority, probe, admission_id=admission_id,
        request_id=saved.get('admission_request_id') or 'bot:' + saved['delivery_id'],
        field='execution_generation',
    )
    if row is None:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if row['status'] == 'terminal':
        return None
    if row['status'] not in {'queued', 'started', 'unknown'}:
        raise RuntimeStoreError('preservation_owner_unmapped')
    # The saved record is the exact private projection. Return only a non-acknowledging answer to
    # a live caller; never write derived public identity/reply fields back over this pin.
    return {field: saved[field] for field in (
        'status', 'delivery_id', 'profile_home', 'session_id', 'message', 'reply', 'error', 'reason'
    ) if field in saved}


def _reconcile_unknown_bot_handles(authority, admission_id, row):
    """Reopen only the exact Bot child/root after the canonical row is terminal."""
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        raise RuntimeStoreError('preservation_owner_unmapped')
    from pathlib import Path
    from gateway.session_automation_custody import row_descriptor

    descriptor = row_descriptor(row)
    current_epoch = getattr(authority, 'epoch', None)
    current_instance = getattr(authority, 'instance_id', None)
    coordinator_id = getattr(coordinator, '_coordinator_id', None)
    home = str(Path(authority.db.db_path).parent.resolve())
    binding = getattr(authority, '_preservation_automation_bindings', {}).get(admission_id)
    successor = isinstance(binding, dict) and binding.get('successor') is True
    if (not isinstance(descriptor, dict) or descriptor.get('owner_class') != 'I07_automation_route_wakes'
            or type(current_epoch) is not int or current_epoch < 1
            or row.get('owner_epoch') != current_epoch
            or descriptor.get('profile_id') != authority.profile_id
            or descriptor.get('profile_home') != home
            or descriptor.get('session_id') != row.get('target_session_id')
            or descriptor.get('principal_id') != row.get('principal_id')
            or descriptor.get('request_id') != row.get('request_id')
            or not isinstance(descriptor.get('process_instance'), str)
            or not descriptor.get('process_instance')
            or not isinstance(descriptor.get('coordinator_id'), str)
            or not descriptor.get('coordinator_id')
            or type(descriptor.get('authority_epoch')) is not int
            or descriptor['authority_epoch'] < 1
            or descriptor['authority_epoch'] > current_epoch
            or type(descriptor.get('close_generation')) is not int
            or descriptor['close_generation'] < 0
            or not isinstance(descriptor.get('source_identity'), str)
            or not descriptor['source_identity']):
        raise RuntimeStoreError('preservation_owner_unmapped')
    expected_owner_id = authority._automation_owner_id(
        descriptor['owner_class'], row['target_session_id'], row['request_id'])
    if descriptor.get('owner_id') != expected_owner_id:
        raise RuntimeStoreError('preservation_owner_unmapped')

    root_handle = getattr(authority, '_preservation_handles', {}).get(admission_id)
    if root_handle is None:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if successor:
        bound_row = binding.get('row')
        if (binding.get('handle') is not root_handle
                or binding.get('descriptor') != descriptor
                or not isinstance(bound_row, dict)
                or any(bound_row.get(field) != row.get(field)
                       for field in ('admission_id', 'target_session_id', 'principal_id', 'request_id'))
                or bound_row.get('owner_epoch') != current_epoch
                or type(bound_row.get('generation')) is not int
                or bound_row['generation'] < 1
                or binding.get('source_identity') != descriptor.get('source_identity')):
            raise RuntimeStoreError('preservation_owner_unmapped')
        expected_successor_id = f"successor:{admission_id}:{row['request_id']}"
        if (getattr(root_handle, 'owner_id', None) != expected_successor_id
                or getattr(root_handle, 'profile_id', None) != authority.profile_id
                or getattr(root_handle, 'authority_epoch', None) != current_epoch
                or getattr(root_handle, 'process_instance', None) != current_instance
                or getattr(root_handle, 'coordinator_id', None) != coordinator_id):
            raise RuntimeStoreError('preservation_owner_unmapped')
    elif (descriptor.get('process_instance') != current_instance
          or descriptor.get('coordinator_id') != coordinator_id):
        raise RuntimeStoreError('preservation_owner_unmapped')
    handles = []
    child_key = getattr(authority, '_preservation_automation_children', {}).get(admission_id)
    operation = getattr(authority, '_preservation_operations', {}).get(child_key) if child_key else None
    if operation is not None:
        child_handle = operation.get('handle')
        parent_handle = operation.get('parent_handle')
        if (operation.get('owner_class') != 'I06_busy_followup_delivery'
                or operation.get('principal_id') != row.get('principal_id')
                or operation.get('session_id') != row.get('target_session_id')
                or operation.get('request_id') != row.get('request_id')
                or operation.get('admission_id') != admission_id
                or operation.get('source_identity') != descriptor.get('source_identity')
                or parent_handle is not root_handle
                or child_handle is None
                or getattr(child_handle, 'owner_class', None) != 'I06_busy_followup_delivery'
                or getattr(child_handle, 'profile_id', None) != authority.profile_id
                or getattr(child_handle, 'process_instance', None) != current_instance
                or getattr(child_handle, 'coordinator_id', None) != coordinator_id
                or getattr(child_handle, 'authority_epoch', None)
                != (current_epoch if successor else descriptor['authority_epoch'])
                or getattr(getattr(child_handle, 'lease', None), 'parent_lease_id', None)
                != getattr(getattr(root_handle, 'lease', None), 'lease_id', None)):
            raise RuntimeStoreError('preservation_owner_unmapped')
        handles.append(child_handle)
    if (getattr(root_handle, 'owner_class', None) != descriptor['owner_class']
            or getattr(root_handle, 'profile_id', None) != authority.profile_id
            or getattr(root_handle, 'process_instance', None) != current_instance
            or getattr(root_handle, 'coordinator_id', None) != coordinator_id
            or (not successor and (
                getattr(root_handle, 'owner_id', None) != descriptor.get('owner_id')
                or getattr(root_handle, 'authority_epoch', None) != descriptor['authority_epoch']
                or getattr(root_handle, 'close_generation', None) != descriptor.get('close_generation')))):
        raise RuntimeStoreError('preservation_owner_unmapped')
    handles.append(root_handle)
    for handle in handles:
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        custody = getattr(coordinator, '_records', {}).get(lease_id)
        if custody is None or custody.handle is not handle:
            raise RuntimeStoreError('preservation_owner_unmapped')
        if custody.state != 'unknown':
            continue
        refusal = coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
            process_instance=handle.process_instance, authority_epoch=handle.authority_epoch,
        )
        if refusal is not None:
            raise RuntimeStoreError('preservation_owner_unmapped')
