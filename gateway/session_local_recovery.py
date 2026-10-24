"""Restore only private, profile-bound local policy into the existing authority."""
import asyncio
import json
import logging
from pathlib import Path
import sqlite3

from gateway.config import Platform
from gateway.session import SessionEntry, SessionSource
from gateway.session_contract import SessionRef
from hermes_state_keys import local_identity
from hermes_state_local import POLICY_PREFIX, local_receipt
from hermes_state_runtime import RuntimeStoreError, get_session_admission, list_session_admissions


def _registry(authority):
    """Presence marks managed bootstrap; a present-but-None registry is never legacy."""
    runner = authority.runner
    if not hasattr(runner, 'session_authorities'):
        return None
    registry = runner.session_authorities
    if registry is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    return registry


def local_adapter_map(authority):
    """The adapter map a LOCAL session of *authority* lives in: ``runner.adapters`` for the launch
    profile, ``runner._profile_adapters[name]`` for a served secondary (its routed home, so
    ``_adapter_for_source`` / ``_adapters_for_profile`` resolve the right transport)."""
    runner = authority.runner
    registry = _registry(authority)
    name = registry.profile_name(authority) if registry is not None else None
    if name is None:
        return runner.adapters
    profile_adapters = getattr(runner, '_profile_adapters', None)
    if profile_adapters is None:
        profile_adapters = runner._profile_adapters = {}
    return profile_adapters.setdefault(name, {})


def local_source(authority, chat_id, user_id):
    """LOCAL SessionSource for a session owned by *authority*. A served secondary's local sessions
    carry its profile so ``build_session_key`` namespaces them (``agent:<name>:local:...``) and the
    SessionStore writes their rows to that profile's ``state.db``, not the launch store."""
    source = SessionSource(platform=Platform.LOCAL, chat_id=chat_id, user_id=user_id, chat_type='dm')
    registry = _registry(authority)
    name = registry.profile_name(authority) if registry is not None else None
    if name is not None:
        source.profile = name
    return source


def _refused_check(exc):
    """``<reason or error> at <function>:<line>``: where a receipt check raised, and why, followed
    through every chained ``__cause__`` (``restore_policy`` and ``local_receipt`` wrap the check
    that failed into a bare ``storage_unavailable``)."""
    import traceback
    frames = traceback.extract_tb(exc.__traceback__)
    cause = exc.reason if isinstance(exc, RuntimeStoreError) else f'{type(exc).__name__}: {exc}'
    where = f' at {frames[-1].name}:{frames[-1].lineno}' if frames else ''
    chained = f', caused by {_refused_check(exc.__cause__)}' if exc.__cause__ is not None else ''
    return f'{cause}{where}{chained}'


class _ReceiptRefused(RuntimeStoreError):
    """A refused receipt ``restore_local_session`` already logged with its failing check."""


class _CatchUpPending(RuntimeStoreError):
    """The receipt's compression catch-up is committing in a worker; this restore is refused."""


def _with_untracked_compression(authority, sid, receipt):
    """*receipt*, after recording compression steps an older build published past its tip (#140060).

    The catch-up writes, so it never runs on the owner loop: a contended writer there would stall every
    profile the loop serves. The async restore paths commit it in a worker first
    (``session_local_adoption``); a sync restore on the loop that still finds it hands it to a tracked
    worker, which publishes the session once it commits, and refuses this read.
    """
    from hermes_state_local_lineage import adopt_untracked_compression, adoptable_compression
    with authority.db._read_ctx() as conn:
        if adoptable_compression(conn, receipt) is None:
            return receipt
    from gateway.session_local_adoption import on_owner_loop, schedule_adoption
    if on_owner_loop():
        schedule_adoption(authority, sid, receipt)
        raise _CatchUpPending('storage_unavailable')
    adopt_untracked_compression(authority.db, epoch=authority.epoch, session_id=sid, expected=receipt)
    return local_receipt(authority.db, sid)


def _checked_entry(authority, receipt, source, route):
    """*receipt*'s entry, once its route, origin and stored identity are this authority's."""
    if receipt['route'] != route:
        raise ValueError('route mismatch')
    entry = SessionEntry.from_dict(receipt['entry'])
    # Older local receipts omitted the served profile from the origin. The receipt's
    # home and route already matched this authority; fill only that absent field,
    # never accept an explicitly different profile or change the durable route.
    if 'profile' not in receipt['entry']['origin']:
        entry.origin.profile = source.profile
    if (entry.session_key != route
            or entry.origin.to_dict() != source.to_dict()):
        raise ValueError('entry mismatch')
    row = authority.db.get_session(entry.session_id)
    if row is None or (row['session_key'], row['chat_id'], row['user_id']) != (route, source.chat_id, source.user_id):
        raise ValueError('stored identity mismatch')
    return entry


def owned_receipt(authority, sid):
    """*sid*'s stored receipt with its LOCAL source, route and entry, once all are this owner's."""
    receipt = local_receipt(authority.db, sid)
    if receipt['profile_id'] != authority.profile_id:
        raise RuntimeStoreError('profile_mismatch')
    chat_id = (receipt['legacy_session_id'] if 'legacy_session_id' in receipt else
               local_identity(receipt['profile_id'], receipt['principal_id'], receipt['request_id']))
    if receipt['session_id'] != sid or sid != chat_id:
        raise ValueError('identity mismatch')
    source = local_source(authority, chat_id, receipt['principal_id'])
    route = authority.runner.session_store._generate_session_key(source)
    return receipt, source, route, _checked_entry(authority, receipt, source, route)


def _stray_route_target(authority, receipt, session_id):
    """*session_id* is a replacement the SessionStore's stale-route self-heal minted on this local
    route before #140642: outside the receipt's lineage, never admitted, no transcript. The receipt
    is the route's truth, so restoring it drops that entry instead of refusing the conversation
    forever (``admission_conflict`` on every resume). Anything with history or ledger rows still
    refuses: it is not provably a stray."""
    if not session_id or session_id in receipt.get('lineage', [receipt['session_id']]):
        return False
    with authority.db._read_ctx() as conn:
        busy = conn.execute(
            'SELECT 1 FROM messages WHERE session_id=? UNION ALL SELECT 1 FROM session_admissions '
            'WHERE target_session_id=? UNION ALL SELECT 1 FROM worker_executions WHERE session_id=? '
            'UNION ALL SELECT 1 FROM sessions WHERE parent_session_id=? LIMIT 1',
            (session_id, session_id, session_id, session_id)).fetchone()
    if busy is not None:
        return False
    logging.getLogger(__name__).warning(
        'Local route for %s named an empty replacement %s outside its lineage; restoring the receipt target',
        receipt['session_id'], session_id)
    return True


def restore_local_session(authority, sid):
    _registry(authority)
    from gateway.session_authority import LiveSession
    from gateway.session_local import LocalSessionAdapter
    from gateway.session_policy import restore_policy
    try:
        # The catch-up writes, so the receipt as stored must already be this owner's (#140060).
        receipt, source, route, entry = owned_receipt(authority, sid)
        chat_id, store = source.chat_id, authority.runner.session_store
        adopted = _with_untracked_compression(authority, sid, receipt)
        policy = restore_policy(adopted['policy'])
        if adopted is not receipt:  # re-read after a catch-up: check the copy that will be served
            receipt, entry = adopted, _checked_entry(authority, adopted, source, route)
        target = entry.session_id
        from hermes_state_local_lineage import validate_local_lineage
        with authority.db._read_ctx() as conn:
            validate_local_lineage(conn, receipt)
    except _CatchUpPending:
        raise
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        # The client only sees the bounded code: gateway.log names the check that refused (#140099).
        logging.getLogger(__name__).warning('Local session %s receipt refused: %s', sid, _refused_check(exc))
        if isinstance(exc, RuntimeStoreError):
            raise _ReceiptRefused(exc.reason, field=exc.field, detail=exc.detail) from exc
        raise _ReceiptRefused('storage_unavailable') from exc
    adapters = local_adapter_map(authority)
    adapter = adapters.get(Platform.LOCAL)
    if adapter is None:
        adapter = LocalSessionAdapter(authority)
        adapters[Platform.LOCAL] = adapter
    if not isinstance(adapter, LocalSessionAdapter) or adapter.authority is not authority:
        raise RuntimeStoreError('runtime_draining')
    live = authority.sessions.get(sid)
    if live is None:
        with store._lock:
            store._ensure_loaded_locked()
            current = store._entries.get(route)
            if current is not None and current.session_id != target:
                if not _stray_route_target(authority, receipt, current.session_id):
                    raise RuntimeStoreError('admission_conflict')
                current = None
            # The receipt is the creation copy; a persisted entry for the same target keeps the
            # route's explicit ``/yolo`` toggle (an OFF over a ``--yolo`` launch must survive a
            # cold restore). An untoggled entry stays None and keeps following the launch policy.
            if current is not None:
                entry.yolo = current.yolo
            entry.origin = source
            store._entries[route] = entry
        adapter.policies[chat_id] = policy
        authority.sessions[sid] = LiveSession(source, route)
        adapter.register_source(source)
    elif (not adapter.authorize_source(live.source)
          or live.source.to_dict() != source.to_dict()
          or adapter.policies.get(chat_id) != policy):
        raise RuntimeStoreError('storage_unavailable')
    else:
        with store._lock:
            current = store._entry_locked(route)
            if current is None or (current.session_id not in receipt.get('lineage', [sid])
                                   and not _stray_route_target(authority, receipt, current.session_id)):
                raise RuntimeStoreError('admission_conflict')
            current.session_id = target
    return SessionRef(authority.profile_id, sid)


def transcript_target(authority, ref):
    """The current physical transcript of logical *ref* (a local receipt's entry, else the tip)."""
    live = authority.sessions[ref.session_id]
    if live.source is not None and live.source.platform == Platform.LOCAL:
        restore_local_session(authority, ref.session_id)
        return local_receipt(authority.db, ref.session_id)['entry']['session_id']
    return authority.physical_target(ref)


# A resume reply's rows reach Ink twice (``messages`` and ``display_messages``). Archived rows are display-only
# and fill only what the live rows leave of this budget, so they keep the reply well under the gateway clients'
# 8 MiB WebSocket frame cap (``hermes_cli/gateway_client.py``) and never push a reply main could send past it.
# Live rows are never dropped.
RESUME_HISTORY_BYTES = 3 * 1024 * 1024


def display_rows(db, target):
    """``(live row ids, display rows, truncated)`` of physical *target*, the live ids read first
    (``local_history``'s fence). The display rows are the compression lineage as main's resume and the
    REST transcript show it: every live row, and the newest compaction-archived rows that fit what the
    live rows leave of ``RESUME_HISTORY_BYTES`` (``truncated`` when older ones were left out). A lineage
    over the resume cap shows only the live rows."""
    from hermes_state import SessionResumeTooLargeError
    live_rows = db.get_messages_as_conversation(target, include_row_ids=True)
    live = {row['_row_id'] for row in live_rows}
    try:
        db.assert_resume_safe(target)
    except SessionResumeTooLargeError:
        return live, [], False
    kept, truncated = _fit_archive(live_rows, db.get_messages_as_conversation(
        target, include_ancestors=True, include_compacted=True, include_row_ids=True))
    return live, kept, truncated


def _fit_archive(live_rows, display):
    """``(display rows kept, truncated)``: every live row of *display*, and its newest archived rows that fit
    what *live_rows* leave of ``RESUME_HISTORY_BYTES``."""
    live = {row['_row_id'] for row in live_rows}
    kept, used, truncated = [], sum(len(json.dumps(row, default=str)) for row in live_rows), False
    for row in reversed(display):
        if row['_row_id'] not in live:
            used += len(json.dumps(row, default=str))
            truncated = truncated or used > RESUME_HISTORY_BYTES
            if truncated:
                continue
        kept.append(row)
    return kept[::-1], truncated


async def prepare_history(authority, ref):
    """``local_history``'s lineage read, off the owner loop; the locked part re-reads only the live tip."""
    target = transcript_target(authority, ref)
    return target, await asyncio.to_thread(display_rows, authority.db, target)


def _merge_live_in_stored_order(display, live):
    """The live rows in stored (``row_id``) order, each archived display row placed before the live
    row it preceded in display order.

    Display deduplication keeps a carried message at its FIRST copy's position, so after a rotated
    compression the copied tail sits before the new summary. Clients take the live rows of a resume
    as the transcript (the CLI's ``conversation_history``, ``/undo``'s prefix, rollback, ACP replay),
    so the live subsequence must be the stored one; archived rows only anchor between live rows."""
    current = {row['_row_id'] for row in live}
    groups, pending, shown = [], [], 0
    for row in display:
        if row['_row_id'] in current:
            groups.append(pending)
            pending, shown = [], shown + 1
        else:
            pending.append({**row, 'archived': True})
    shown_ids = {row['_row_id'] for row in display} & current
    rows, seen = [], 0
    for row in sorted(live, key=lambda r: r['_row_id']):
        if row['_row_id'] in shown_ids:
            rows.extend(groups[seen])
            seen += 1
        rows.append(row)
    return rows + pending


def local_history(authority, ref, prepared=None):
    """Display history of logical *ref*: every stored row the user saw, in display order.

    A row outside the current physical transcript's active set (summarized away by a compression)
    carries ``archived: true``: it is shown, never live, so no undo, retry or rewind may target it.
    The rows without it are exactly the live transcript's, so a client choosing among live rows sees
    what it saw before. That includes a micro-compaction merge (``display_metadata.model_only``): its
    originals are the archived rows renderers show, so renderers skip the merge itself.

    Each row carries its durable ``messages.id`` as ``row_id`` (the ``TranscriptMessage`` field
    Desktop hydration and the legacy ``session.history`` already use): a ``session.mutate`` rewind
    addresses its ``target_message_id`` by it, so a snapshot without it leaves no way to undo/retry.
    *prepared* (``prepare_history``) is kept while every row it read as live still is; a rewind or
    compaction since then re-reads the lineage here. When the byte budget left older archived rows out
    (``display_rows``), the first row carries ``archived_truncated: true``.
    """
    db = authority.db
    target = transcript_target(authority, ref)
    read_live, display, truncated = (prepared[1] if prepared is not None and prepared[0] == target
                                     else display_rows(db, target))
    live = db.get_messages_as_conversation(target, include_row_ids=True)
    live_ids = {row['_row_id'] for row in live}
    if not read_live <= live_ids:
        read_live, display, truncated = display_rows(db, target)
        live = db.get_messages_as_conversation(target, include_row_ids=True)
    elif live_ids - read_live:
        # Rows appended since the prepare count against the archive budget too (no lineage re-read).
        display, cut = _fit_archive(live, display)
        truncated = truncated or cut
    rows = [{**{k: v for k, v in row.items() if k != '_row_id'}, 'row_id': row['_row_id']}
            for row in _merge_live_in_stored_order(display, live)]
    if truncated and rows:
        rows[0]['archived_truncated'] = True  # earlier archived history was left out (the byte budget)
    return rows


def reopen_local_session(authority, ref):
    """The first admitted turn makes a finalized local session live again (#85303).

    Mounting (``session.resume``) is a read and leaves ``ended_at``/``end_reason`` alone; the drain
    calls this after the turn's local preflight, before execution routes it. A TUI shutdown / WS
    disconnect / idle eviction stamps ``end_reason`` on the physical row. ``SessionStore`` reads any
    stamped row as a stale route (#54878) and, since those reasons are not recoverable, answers the
    turn with a FRESH session — the client keeps waiting on an id that never emits again. The classic
    surfaces clear the stamp on the first real turn (tui_gateway ``_reopen_if_finalized``); the
    authority must too. Only the lineage tip is reopened: a
    compression parent or reset predecessor is never the physical target.
    """
    live = authority.sessions.get(ref.session_id)
    if live is None or live.source is None or live.source.platform != Platform.LOCAL:
        return
    target = local_receipt(authority.db, ref.session_id)['entry']['session_id']
    row = authority.db.get_session(target)
    if row is not None and row.get('end_reason') is not None:
        authority.db.reopen_session(target)


def reset_local_session(store, old_entry, session_id, now, display_name):
    from hermes_state_local_lineage import reset_local_target
    entry = SessionEntry(old_entry.session_key, session_id, now, now,
        origin=old_entry.origin, platform=old_entry.platform, chat_type=old_entry.chat_type,
        display_name=display_name if display_name is not None else old_entry.display_name,
        is_fresh_reset=True)
    db = store._db_for_key(old_entry.session_key)
    epochs = getattr(store, '_local_authority_epochs', None) or {}
    epoch = epochs.get(Path(db.db_path).resolve(), store._local_authority_epoch) if db is not None else store._local_authority_epoch
    reset_local_target(db, epoch=epoch,
                       parent_session_id=old_entry.session_id, entry=entry.to_dict())
    store._entries[old_entry.session_key] = entry
    return entry


def local_receipt_ids(authority):
    with authority.db._read_ctx() as conn:
        return [row[0][len(POLICY_PREFIX):] for row in conn.execute(
            'SELECT key FROM state_meta WHERE key LIKE ?', (POLICY_PREFIX + '%',))]


def i10_source_snapshot(authority):
    """Read LOCAL policy receipts, active admissions and the local adoption fences."""
    pending = set()
    rows = []
    receipt_ids = tuple(local_receipt_ids(authority))
    for sid in receipt_ids:
        _latest, active, local, policy_source = _active_local_rows(authority, sid)
        if any(_local_recovery_kind(row, policy_source) == 'unknown' for row in active):
            pending.add(f"unknown:{sid}")
        rows.extend(local)
        for fence in _local_recovery_absence_fences(authority, sid, allow_legacy=False):
            pending.add(f"{fence}:{sid}")
    pending.update(str(row['admission_id']) for row in rows if row.get('admission_id'))
    return {
        "pending_ids": tuple(sorted(pending)),
        "rows": tuple(rows),
        "registry": {"local_receipts": tuple(sorted(receipt_ids)),
                     "active_admissions": tuple(sorted(row.get('admission_id', '') for row in rows))},
    }


def register_i10_source(authority):
    """Install LOCAL recovery's canonical close-time probe for the profile authority."""
    from gateway.session_recovery_custody import register_i10_consumer_source
    return register_i10_consumer_source(authority, "local_recovery", i10_source_snapshot)


_LOCAL_POLICY_SOURCES = frozenset({'cli', 'tui', 'gui', 'acp', 'tool', 'oneshot'})
_FOREIGN_POLICY_SOURCES = frozenset({'a2a', 'kanban', 'cron', 'bot_room'})


def _local_policy_source(authority, sid):
    try:
        policy = local_receipt(authority.db, sid).get('policy')
    except (AttributeError, KeyError, TypeError, ValueError):
        return None
    source = policy.get('source') if isinstance(policy, dict) else None
    return source if isinstance(source, str) else None


def _local_recovery_kind(row, policy_source=None):
    """Classify canonical local work without claiming another source's admission."""
    payload = row.get('payload') if isinstance(row, dict) else None
    if not isinstance(payload, dict):
        return 'unknown'
    if ('native_text_v1' in payload or 'api_turn_v1' in payload
            or 'local_automation_v1' in payload or '_automation_custody_v1' in payload):
        return 'foreign'
    if policy_source in _FOREIGN_POLICY_SOURCES:
        return 'foreign'
    if policy_source not in _LOCAL_POLICY_SOURCES:
        return 'unknown'
    return 'local' if isinstance(payload.get('text'), str) else 'unknown'


def _active_local_rows(authority, sid):
    rows = list_session_admissions(authority.db, session_id=sid, pending_only=False)
    active = [row for row in rows if row.get('status') in {'queued', 'started', 'unknown'}]
    policy_source = _local_policy_source(authority, sid)
    return rows, active, [
        row for row in active if _local_recovery_kind(row, policy_source) == 'local'
    ], policy_source


def _local_adoption_fences(authority, sid):
    from gateway.session_viewer_close import CLOSE_PREFIX

    pending_closes = getattr(authority, 'pending_closes', None)
    if isinstance(pending_closes, dict) and sid in pending_closes:
        return ('viewer_close_pending',)
    if authority.db.get_meta(CLOSE_PREFIX + sid) is not None:
        return ('viewer_close_persisted',)
    physical = authority.db.get_compression_tip(sid) or sid
    with authority.db._read_ctx() as conn:
        pending = conn.execute(
            "SELECT 1 FROM worker_executions WHERE session_id IN (?,?) "
            "AND status IN ('registered','running','unknown') LIMIT 1",
            (sid, physical),
        ).fetchone()
    return ('worker_pending',) if pending is not None else ()


def _reconcile_local_parents(authority, handles):
    """Reopen every exact I10 parent only after the tuple is validated atomically."""
    from gateway.session_recovery_custody import _validate_handle

    if not isinstance(handles, tuple) or not handles:
        raise RuntimeStoreError('preservation_owner_unmapped')
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    unknown = []
    for handle in handles:
        record = _validate_handle(authority, handle, allow_unknown=True)
        if record is None or getattr(handle, 'owner_class', None) != 'I10_startup_recovery_handoff':
            raise RuntimeStoreError('unknown_execution')
        if record.state == 'unknown':
            unknown.append(handle)
    if unknown:
        refusal = coordinator.reconcile_unknown_parents(
            tuple(unknown), profile_id=authority.profile_id,
            authority_epoch=authority.epoch, process_instance=authority.instance_id,
        )
        if refusal is not None:
            raise RuntimeStoreError('unknown_execution')
    for handle in handles:
        if _validate_handle(authority, handle) is None:
            raise RuntimeStoreError('unknown_execution')
    return handles


def _reconcile_local_parent(authority, handle):
    """Compatibility wrapper for callers that have exactly one I10 parent."""
    return _reconcile_local_parents(authority, (handle,))[0]


def _local_adoption_handle(authority, row):
    admission_id = row.get('admission_id')
    handle = getattr(authority, '_preservation_handles', {}).get(admission_id)
    if row.get('status') == 'queued' and handle is None:
        from gateway.session_recovery_custody import ensure_recovery_root
        handle = ensure_recovery_root(authority, row, consumer='local_recovery')
    if handle is None:
        raise RuntimeStoreError('unknown_execution')
    bindings = getattr(authority, '_i10_recovery_bindings', None)
    if (
        isinstance(bindings, dict)
        and getattr(handle, 'owner_class', None) == 'I10_startup_recovery_handoff'
    ):
        binding = bindings.get(admission_id)
        if not isinstance(binding, dict) or binding.get('handle') is not handle:
            raise RuntimeStoreError('unknown_execution')
    from gateway.session_recovery_custody import refresh_binding
    current = get_session_admission(authority.db, admission_id=admission_id)
    if not isinstance(current, dict) or not refresh_binding(authority, current):
        raise RuntimeStoreError('unknown_execution')
    return handle


def local_adoption_parents(authority, sid, *, require_no_unknown=False):
    """Return every exact active local I10 parent in durable FIFO order."""
    _registry(authority)
    _rows, active, local, policy_source = _active_local_rows(authority, sid)
    if any(_local_recovery_kind(row, policy_source) != 'local' for row in active):
        raise RuntimeStoreError('preservation_owner_unmapped')
    if require_no_unknown and any(row.get('status') == 'unknown' for row in active):
        raise RuntimeStoreError('unknown_execution')
    if _local_adoption_fences(authority, sid):
        raise RuntimeStoreError('storage_unavailable')
    handles = []
    try:
        for row in local:
            handles.append(_local_adoption_handle(authority, row))
    except Exception:
        if handles:
            _mark_local_recovery_handles_unknown(
                authority, {handle.lease.lease_id: handle for handle in handles}, 'read_failed',
            )
        raise
    return _reconcile_local_parents(authority, tuple(handles)) if handles else ()


def reconcile_local_parents_after_unknown(authority, sid, resolved_admission_id):
    """Reconcile queued LOCAL I10 parents after one durable unknown resolution.

    The resolution write is already committed when this runs.  Only the exact current terminal
    admission may open this path; any other active unknown, foreign payload, stale binding, or
    source fence leaves the retained handles UNKNOWN and returns without failing the resolution RPC.
    """
    try:
        policy_source = _local_policy_source(authority, sid)
        if policy_source not in _LOCAL_POLICY_SOURCES:
            return False
        resolved = get_session_admission(authority.db, admission_id=resolved_admission_id)
        if (
            not isinstance(resolved, dict)
            or resolved.get('admission_id') != resolved_admission_id
            or resolved.get('target_session_id') != sid
            or resolved.get('status') != 'terminal'
            or _local_recovery_kind(resolved, policy_source) != 'local'
        ):
            return False
        parents = local_adoption_parents(authority, sid, require_no_unknown=True)
        return bool(parents)
    except (AttributeError, LookupError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as exc:
        # Durable resolution remains valid even when its later custody rebind cannot be proven.
        # The retained handle is intentionally left UNKNOWN for a later exact retry.
        logging.getLogger(__name__).warning(
            'Local parent reconciliation deferred: %s', type(exc).__name__,
        )
        return False


def local_adoption_parent(authority, sid):
    """Compatibility wrapper returning one handle or the exact parent tuple."""
    parents = local_adoption_parents(authority, sid)
    if len(parents) <= 1:
        return parents[0] if parents else None
    return parents


def _local_recovery_handles(authority, rows):
    return {
        row['admission_id']: getattr(authority, '_preservation_handles', {}).get(row['admission_id'])
        for row in rows if isinstance(row.get('admission_id'), str)
    }


def _local_recovery_absence_fences(authority, sid, *, allow_legacy):
    """Read every local source fence before recording a source-owned absence."""
    if allow_legacy:
        return ()
    from gateway.session_local_adoption import _due
    from gateway.session_viewer_close import CLOSE_PREFIX

    fences = []
    if _due(authority, sid, strict=True) is not None:
        fences.append('compression_pending')
    pending = getattr(authority, '_local_adoptions', None)
    adoption = pending.get(sid) if isinstance(pending, dict) else None
    if adoption is not None:
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        done = getattr(adoption, 'done', None)
        if not callable(done) or (not done() and adoption is not current_task):
            fences.append('compression_adoption_pending')
    pending_closes = getattr(authority, 'pending_closes', None)
    if isinstance(pending_closes, dict) and sid in pending_closes:
        fences.append('viewer_close_pending')
    if authority.db.get_meta(CLOSE_PREFIX + sid) is not None:
        fences.append('viewer_close_persisted')
    physical = authority.db.get_compression_tip(sid) or sid
    with authority.db._read_ctx() as conn:
        pending_worker = conn.execute(
            "SELECT 1 FROM worker_executions WHERE session_id IN (?,?) "
            "AND status IN ('registered','running','unknown') LIMIT 1",
            (sid, physical),
        ).fetchone()
    if pending_worker is not None:
        fences.append('worker_pending')
    return tuple(dict.fromkeys(fences))


def _mark_local_recovery_handles_unknown(authority, handles, reason):
    """Mark exact retained roots unknown; a missing handle is itself an uncertainty."""
    if not handles:
        raise RuntimeStoreError('preservation_owner_unmapped')
    coordinator = getattr(authority.runner, 'runtime_custody', None)
    if coordinator is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    for handle in handles.values():
        if handle is None:
            raise RuntimeStoreError('preservation_owner_unmapped')
        coordinator.mark_unknown(handle, reason)
        lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
        record = getattr(coordinator, '_records', {}).get(lease_id)
        if record is None or record.handle is not handle or record.state != 'unknown':
            raise RuntimeStoreError('preservation_owner_unmapped')


def mark_local_recovery_unknown(authority, sid, reason):
    policy_source = _local_policy_source(authority, sid)
    rows = [
        row for row in list_session_admissions(authority.db, session_id=sid, pending_only=False)
        if row.get('status') in {'queued', 'started', 'unknown'}
        and _local_recovery_kind(row, policy_source) == 'local'
    ]
    _mark_local_recovery_handles_unknown(authority, _local_recovery_handles(authority, rows), reason)


def _managed_local_recovery_handles(authority, local, terminal_local):
    from gateway.session_recovery_custody import ensure_recovery_root, _validate_handle

    handles = _local_recovery_handles(authority, local)
    blocked = False
    for row in local:
        admission_id = row['admission_id']
        handle = handles.get(admission_id)
        if row.get('status') == 'queued' and (
                handle is None or getattr(handle, 'owner_class', None) == 'I02_api_session_turn'):
            handles[admission_id] = ensure_recovery_root(
                authority, row, consumer='local_recovery',
            )
            handle = handles[admission_id]
        bindings = getattr(authority, '_i10_recovery_bindings', None)
        binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
        if (
            handle is None
            or _validate_handle(authority, handle) is None
            or getattr(handle, 'owner_class', None) != 'I10_startup_recovery_handoff'
            or not isinstance(binding, dict)
            or binding.get('handle') is not handle
        ):
            blocked = True
    terminal_handles = _local_recovery_handles(authority, terminal_local)
    bound_terminal = {key: value for key, value in terminal_handles.items() if value is not None}
    if bound_terminal:
        _mark_local_recovery_handles_unknown(authority, bound_terminal, 'terminal_proof_missing')
    return handles, blocked


def _local_recovery_intake(authority, sid, *, allow_legacy):
    if not allow_legacy:
        _registry(authority)
    rows, active, local, policy_source = _active_local_rows(authority, sid)
    foreign = [row for row in active if _local_recovery_kind(row, policy_source) == 'foreign']
    unknown = [row for row in active if _local_recovery_kind(row, policy_source) == 'unknown']
    terminal_local = [
        row for row in rows
        if row.get('status') == 'terminal'
        and _local_recovery_kind(row, policy_source) == 'local'
    ]
    if not allow_legacy and getattr(authority.runner, 'runtime_custody', None) is None:
        raise RuntimeStoreError('runtime_custody_unavailable')
    absence_fences = _local_recovery_absence_fences(authority, sid, allow_legacy=allow_legacy)
    handles = _local_recovery_handles(authority, local)
    # Settled local history may coexist with a fresh local admission. Keep the
    # terminal fence for terminal-only sessions while allowing the active local
    # admission to be restored and drained after a restart.
    blocked = bool(foreign or unknown or (terminal_local and not local) or absence_fences)
    if not allow_legacy:
        handles, handles_blocked = _managed_local_recovery_handles(
            authority, local, terminal_local,
        )
        blocked = blocked or handles_blocked
        if blocked:
            bound = {key: value for key, value in handles.items() if value is not None}
            if bound and (foreign or unknown or terminal_local):
                _mark_local_recovery_handles_unknown(authority, bound, 'recovery_intake_uncertain')
    return {
        'rows': rows, 'local': local, 'foreign': foreign, 'unknown': unknown,
        'terminal_local': terminal_local, 'handles': handles,
        'absence_fences': absence_fences, 'blocked': blocked,
        'policy_source': policy_source,
    }


def _observe_local_recovery(authority, sid, intake, policy_receipt, *, worker_status):
    from gateway.session_recovery_custody import (
        consumer_receipt, local_recovery_source_receipt, observe_consumer, refresh_binding,
    )
    observed = []
    for row in intake['local']:
        admission_id = row['admission_id']
        handle = intake['handles'].get(admission_id)
        if handle is None:
            continue
        current = get_session_admission(authority.db, admission_id=admission_id)
        if not isinstance(current, dict):
            raise RuntimeStoreError('preservation_receipt_unmapped')
        refresh_binding(authority, current)
        source_receipt = local_recovery_source_receipt(
            authority, current, policy_receipt, worker_status=worker_status,
        )
        admission_receipt = authority._receipt(current)
        state = 'unknown' if current.get('status') == 'unknown' else 'active'
        receipt = consumer_receipt(
            authority, 'local_recovery', row=current, handle=handle,
            source_receipts=(source_receipt, admission_receipt), state=state,
        )
        if not observe_consumer(authority, 'local_recovery', receipt):
            raise RuntimeStoreError('preservation_receipt_unmapped')
        observed.append(admission_id)
    return observed


def _recover_one_local_session(authority, sid, *, schedule, allow_legacy, worker_status='mounted'):
    if allow_legacy:
        ref = restore_local_session(authority, sid)
        pending = list_session_admissions(authority.db, session_id=sid)
        from gateway.session_recovery_custody import observe_consumer_rows
        observe_consumer_rows(
            authority, 'local_recovery', rows=pending,
            reason='local receipt had no accepted admission',
        )
        if schedule and pending and not any(row.get('status') == 'unknown' for row in pending):
            authority._schedule(ref)
        return ref
    intake = _local_recovery_intake(authority, sid, allow_legacy=False)
    if intake['blocked']:
        return None
    ref = restore_local_session(authority, sid)
    if intake['local']:
        policy_receipt = local_receipt(authority.db, sid)
        observed = _observe_local_recovery(
            authority, sid, intake, policy_receipt, worker_status=worker_status,
        )
        ready = (
            len(observed) == len(intake['local'])
            and not any(row.get('status') == 'unknown' for row in intake['local'])
        )
    else:
        latest = list_session_admissions(authority.db, session_id=sid, pending_only=False)
        policy_source = intake['policy_source']
        if any(row.get('status') in {'queued', 'started', 'unknown'} for row in latest):
            return None
        if any(row.get('status') == 'terminal'
               and _local_recovery_kind(row, policy_source) == 'local'
               for row in latest):
            return None
        if _local_recovery_absence_fences(authority, sid, allow_legacy=False):
            return None
        from gateway.session_recovery_custody import observe_consumer, unused_consumer
        absence = unused_consumer(
            authority, 'local_recovery',
            reason='local policy inventory contained no local active admission',
        )
        if not observe_consumer(authority, 'local_recovery', absence):
            raise RuntimeStoreError('preservation_receipt_unmapped')
        ready = False
    if schedule and ready:
        authority._schedule(ref)
    return ref


def complete_local_recovery(authority, sid, *, schedule=True, allow_legacy=False):
    """Finish one compression worker through the same managed source path as startup recovery."""
    return _recover_one_local_session(
        authority, sid, schedule=schedule, allow_legacy=allow_legacy,
        worker_status='compression_catchup',
    )


def recover_local_sessions(authority, *, schedule=False, allow_legacy=None):
    if allow_legacy is None:
        allow_legacy = not hasattr(authority.runner, 'session_authorities')
    for sid in local_receipt_ids(authority):
        try:
            _recover_one_local_session(
                authority, sid, schedule=schedule, allow_legacy=allow_legacy,
            )
        except (_ReceiptRefused, _CatchUpPending):
            continue  # the refusal/worker keeps its source handle for a later owner step
        except RuntimeStoreError as exc:
            logging.getLogger(__name__).warning(
                'Local session %s paused: %s', sid, _refused_check(exc),
            )
    if schedule:
        # A last-viewer close the previous owner stopped before completing (#134889 LC-6b).
        from gateway.session_viewer_close import rearm_persisted_closes, settle_viewer_close
        from gateway.session_runtime_workers import track_mutation
        for sid in rearm_persisted_closes(authority):
            authority._schedule(SessionRef(authority.profile_id, sid))
            if any(row['status'] == 'unknown' for row in list_session_admissions(authority.db, session_id=sid)):
                # A hard-killed owner's turn: the drain pauses on it, so the close resolves it.
                track_mutation(authority, settle_viewer_close(authority, sid))


def restore_pending_local_automation(authority, session_ids):
    """Rebuild only routes named by a pending, persisted local automation admission.

    Managed startup normally restores LOCAL policy receipts before native recovery.  A hard-killed
    owner can leave the automation admission's durable row behind while the session-store route is
    absent, though.  The row's own payload selects this narrow repair; ``restore_local_session``
    still authenticates the receipt's profile, source, stored entry and lineage before publishing
    the route.  A refusal leaves the admission queued for the existing fail-closed recovery path.
    """
    for sid in session_ids:
        rows = list_session_admissions(authority.db, session_id=sid, pending_only=False)
        if not any(
            row.get('status') in {'queued', 'started', 'unknown'}
            and isinstance(row.get('principal_id'), str)
            and row['principal_id'].startswith('automation:')
            and isinstance(row.get('payload'), dict)
            and isinstance(row['payload'].get('local_automation_v1'), dict)
            for row in rows
        ):
            continue
        try:
            from gateway.session_recovery_custody import validate_local_automation_recovery
            for row in rows:
                if (
                    row.get('status') in {'queued', 'started', 'unknown'}
                    and isinstance(row.get('principal_id'), str)
                    and row['principal_id'].startswith('automation:')
                    and isinstance(row.get('payload'), dict)
                    and isinstance(row['payload'].get('local_automation_v1'), dict)
                ):
                    validate_local_automation_recovery(authority, row)
            restore_local_session(authority, sid)
        except (_ReceiptRefused, _CatchUpPending, RuntimeStoreError) as exc:
            logging.getLogger(__name__).warning(
                'Local automation route %s paused: %s', sid, _refused_check(exc),
            )
