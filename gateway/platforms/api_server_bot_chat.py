"""Peer turns into the canonical Bot Chat under session authority.

``hermes peer dm`` (``/api/sessions/{id}/chat[/stream]``) and ``hermes peer run`` (``/v1/runs``)
aim at the profile's Bot Chat. Under session authority that chat is a LOCAL session whose only
writer is the authority FIFO — a Desktop showing it is a viewer, never a lease-holding executor —
so the API edge admits the turn through the same door as ``bot_relay.deliver``
(``gateway/session_bot.py``) and reads the reply from that admission's receipt. Binding the
session as an API conversation (``run_api_turn``) would be a second writer and is refused
(``permission_denied``) by design. Without an authority the legacy live-owner mailbox applies.
"""
import asyncio
import logging
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import uuid

from gateway.session_contract import Principal
from hermes_state_runtime import RuntimeStoreError

logger = logging.getLogger(__name__)

_PENDING = ('queued', 'claimed')
# Owner of a Bot Chat the gateway mints for a peer's create-if-missing (no native client asked).
_PEER_SUBJECT = 'api-peer'
# The whole body ``hermes peer dm`` sends (``hermes_cli/subcommands/peer.py::_ensure_bot_chat``).
_PEER_CREATE_FIELDS = frozenset({'title', 'source'})


def _authority(runner):
    from gateway.session_authorities import active_authority
    return active_authority(runner) if getattr(runner, 'session_authority', None) is not None else None


async def create_peer_bot_chat(adapter, body):
    """``POST /api/sessions`` titled "Bot Chat" (``hermes peer dm``'s create-if-missing) under session
    authority: the owner mints the chat through the step a first ``bot_relay.deliver`` takes
    (``session_bot._create_bot_chat``), so a headless gateway's Bot Chat is a LOCAL session its FIFO
    owns, not an API row no remote sender can ever bind (#139370). None when the ordinary handler
    answers: no authority, another title, an explicit id, or the chat exists (its title conflict).
    The owner mints on the profile's own policy, so a create carrying anything else (a model
    lock, route, options, system prompt) is refused by name before anything is written."""
    from tools.bot_mode_probe import BOT_CHAT_TITLE
    authority = _authority(adapter.gateway_runner)
    if (authority is None or str(body.get('title') or '').strip() != BOT_CHAT_TITLE
            or body.get('id') or body.get('session_id')):
        return None
    from aiohttp import web
    from gateway.platforms.api_server import _error_response
    if unsupported := sorted(set(body) - _PEER_CREATE_FIELDS):
        return _error_response(
            f"Bot Chat creation refused: bot_chat_parameters_unsupported ({', '.join(unsupported)}); "
            "the Bot Chat is created with the profile's own model and prompt", 400,
            code='bot_chat_parameters_unsupported')
    from gateway.session_bot import _create_bot_chat
    from gateway.session_runtime_workers import track_mutation
    actor = Principal(_PEER_SUBJECT, authority.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'api-peer-dm')
    try:
        authority._require_admission_open()
        ref = await asyncio.shield(track_mutation(authority, _create_bot_chat(authority, actor)))
    except RuntimeStoreError as exc:
        return _error_response(f'Bot Chat creation refused: {exc.reason}', 503, code=exc.reason)
    if ref is None:
        return None
    row = await asyncio.to_thread(authority.db.get_session, ref.session_id)
    return web.json_response({'object': 'hermes.session', 'session': adapter._session_response(row)}, status=201)


def _refusal(authority, tip, exc):
    """``(reason, error)`` for a refused peer turn. A ``not_found`` on a Bot Chat that is no local
    chat is named: a remote sender never binds unowned history, so a bare code would leave the
    peer nothing to act on. The full clause stays in gateway.log."""
    reason = exc.reason
    if reason == 'not_found' and not str(tip.get('chat_id') or '').startswith('local-'):
        from hermes_state_local_migration import validate_legacy_row
        try:
            validate_legacy_row(authority.db, tip)
            reason, hint = 'bot_chat_unbound', 'open it once with `hermes chat -c "Bot Chat"` on that host'
        except RuntimeStoreError:
            from gateway.session_bot import displaceable_bot_chat
            reason = 'bot_chat_not_local'
            if displaceable_bot_chat(authority, tip) is not None:
                hint = "it is a conversation the gateway cannot own; it is moved aside as 'Bot Chat (<source> <id>)' on the next delivery"
            else:  # local history a peer may not adopt (a served remote login's Desktop row, another profile's): never moved
                hint = 'it is local history owned by another login or profile; open or rename it from the client that owns it'
        logger.warning('Peer DM into Bot Chat %s refused: %s (not_found: field=%s detail=%s)',
                       tip['id'], reason, exc.field, exc.detail)
        return reason, f"Bot Chat admission refused: {reason} (this profile's 'Bot Chat' is not a local chat; {hint})"
    return reason, f'Bot Chat admission refused: {reason}'


async def reset_local_title_owner(db, title, include_hidden, source=None):
    """``[row]`` for the exact-title lookup of a LOCAL conversation that was reset (``/new``, the
    compression-exhaustion auto-reset), else ``[]``. The listing shows a reset lineage as its latest,
    untitled segment, so ``hermes peer dm`` stopped finding its Bot Chat and its create collided
    with the title. The titled creation id still owns the FIFO and is what the chat door admits.
    It applies the listing's own filters: hidden only with ``include_hidden``, never an archived
    row (a deliberate archive stays put; the reaper-archive resurrection is the caller's), and
    ``source`` when one is given."""
    return await asyncio.to_thread(_reset_local_title_owner, db, title, include_hidden, source) if title else []


def _reset_local_title_owner(db, title, include_hidden, source=None):
    row = db.get_session_by_title(title)
    if (row is None or row.get('end_reason') != 'session_reset' or row.get('archived')
            or (row.get('hidden') and not include_hidden) or (source and row.get('source') != source)):
        return []
    from hermes_state_local import local_receipt
    try:
        lineage = local_receipt(db, row['id']).get('lineage') or []
    except RuntimeStoreError:
        return []
    return [row] if len(lineage) > 1 else []


def reset_local_bot_chat_target(runner, session_id):
    """Return the current physical target for one reset LOCAL Bot Chat, or ``None``.

    The API path addresses the stable creation id, while a local reset moves the route to a
    child and keeps that creation id in the authority FIFO.  A retired row is not a general
    permission to resume: every witness below must still belong to the active profile, the
    canonical titled Bot Chat, and the receipt's validated reset lineage.  This is deliberately
    read-only; a missing or stale witness leaves the ordinary retired-session fence in place.
    """
    authority = _authority(runner)
    if (authority is None or type(session_id) is not str or not session_id
            or getattr(authority, 'retiring', False) or getattr(authority, 'retired', False)):
        return None
    try:
        authority._require_admission_open()
        db = authority.db
        row = db.get_session(session_id)
        titled = db.get_session_by_title('Bot Chat')
        if (row is None or titled is None or titled.get('id') != session_id
                or row.get('title') != 'Bot Chat'
                or row.get('end_reason') != 'session_reset'
                or row.get('ended_at') is None):
            return None

        # ``owned_receipt`` authenticates the receipt's profile, logical id, LOCAL source, route,
        # stored physical row and principal before the lineage validator is consulted.
        from gateway.session_local_recovery import owned_receipt
        receipt, source, route, entry = owned_receipt(authority, session_id)
        lineage = receipt.get('lineage')
        if (receipt.get('profile_id') != authority.profile_id
                or receipt.get('session_id') != session_id
                or not isinstance(lineage, list) or len(lineage) < 2
                or lineage[0] != session_id or lineage[-1] != entry.session_id):
            return None
        from hermes_state_local_lineage import validate_local_lineage
        with db._read_ctx() as conn:
            target_id = validate_local_lineage(conn, receipt)
        if target_id != entry.session_id or target_id not in lineage:
            return None
        target = db.get_session(target_id)
        if target is None or target.get('end_reason') is not None:
            return None

        from gateway.config import Platform
        live = authority.sessions.get(session_id)
        if (getattr(source, 'platform', None) != Platform.LOCAL
                or live is None or getattr(live, 'route', None) != route
                or getattr(live, 'source', None) is None
                or live.source.to_dict() != source.to_dict()):
            return None
        store = getattr(authority.runner, 'session_store', None)
        current = store.lookup_by_session_key(route) if store is not None else None
        if current is None or current.session_id != target_id:
            return None
        return target_id
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError,
            sqlite3.Error):
        # A malformed/cross-profile/caught-up receipt is not an opportunity to run the local API
        # agent against the retired row.  The caller retains the standard 409 fence.
        return None


def bot_chat_tip(authority, session_id):
    """The canonical Bot Chat's tip row when *session_id* is in its lineage, else None."""
    db = authority.db
    row = db.get_session_by_title('Bot Chat')
    if row is None:
        return None
    tip = db.get_compression_tip(row['id'])
    if not tip or db.get_compression_tip(session_id) != tip:
        return None
    return db.get_session(tip)


def _peer_delivery_target(runner, authority, session_id, logical_target):
    """Return ``(physical entry, reset pending, valid)`` for one pinned peer delivery.

    The admission row remains keyed to ``logical_target``. A reset only changes the route's
    physical target, and the existing reset witness is the sole source of that replacement.
    """
    live_target = authority.sessions.get(logical_target)
    target_entry = (
        runner.session_store.lookup_by_session_key(live_target.route)
        if live_target is not None and getattr(live_target, 'route', None) else None)
    reset_target = reset_local_bot_chat_target(runner, session_id)
    reset_row = authority.db.get_session(session_id)
    reset_pending = bool(reset_row and reset_row.get('end_reason') == 'session_reset')
    if reset_pending and (reset_target is None or target_entry is None
                          or target_entry.session_id != reset_target):
        return target_entry, True, False
    return target_entry, reset_pending, True


def _reject_nonlocal_peer_turn(authority, tip, custody, home, key):
    if str(tip.get('chat_id') or '').startswith('local-'):
        return None
    from hermes_state_local_migration import validate_legacy_row
    try:
        validate_legacy_row(authority.db, tip)
    except RuntimeStoreError as exc:
        reason, error = _refusal(authority, tip, exc)
    else:
        reason, error = _refusal(authority, tip, RuntimeStoreError('not_found'))
    if custody is not None:
        custody.mark_admission_closed()
        custody.mark_unknown(reason)
    return home, {
        'status': 'failed', 'delivery_id': key, 'reason': reason, 'error': error,
    }


def _peer_actor(authority, user_id):
    return Principal(user_id, authority.profile_id,
                     frozenset({'session:read', 'session:submit'}), 'api-peer-dm')


def _peer_create_actor(authority):
    return Principal(_PEER_SUBJECT, authority.profile_id,
                     frozenset({'session:create', 'session:read', 'session:submit'}), 'api-peer-dm')


def _peer_displacement_failure(custody, home, reason):
    if custody is not None:
        custody.mark_admission_closed()
        custody.mark_unknown(reason)
    return home, {
        'status': 'failed', 'delivery_id': uuid.uuid4().hex,
        'reason': reason,
    }


async def _prepare_peer_bot_chat(authority, tip, custody, home):
    """Return the actor and current LOCAL Bot Chat tip, moving aside a displaceable API row."""
    from gateway.session_bot import displaceable_bot_chat
    holder = authority.db.get_session_by_title('Bot Chat') or tip
    displaced = displaceable_bot_chat(authority, holder)
    if displaced is None:
        return _peer_actor(authority, tip['user_id']), tip, None

    from gateway.session_bot import _create_bot_chat
    from gateway.session_runtime_workers import track_mutation
    actor = _peer_create_actor(authority)
    try:
        authority._require_admission_open()
        await asyncio.shield(track_mutation(
            authority, _create_bot_chat(authority, actor, displace=displaced)))
    except RuntimeStoreError as exc:
        return actor, tip, _peer_displacement_failure(custody, home, exc.reason)

    # Read the title and its physical tip again after the serialized move. The API row is retained
    # under its visible name; only this newly minted LOCAL target receives the peer admission.
    holder = authority.db.get_session_by_title('Bot Chat')
    if holder is None:
        return actor, tip, _peer_displacement_failure(custody, home, 'peer_parent_unmapped')
    tip_id = authority.db.get_compression_tip(holder['id']) or holder['id']
    tip = authority.db.get_session(tip_id)
    if tip is None:
        return actor, tip, _peer_displacement_failure(custody, home, 'peer_parent_unmapped')
    return actor, tip, None


def _prepare_peer_delivery_parent(
        authority, custody, home, key, target_entry, target_id, reset_pending,
):
    """Reserve the exact API parent for the already validated logical/physical target."""
    if custody is None:
        return None, None, True
    parent_handle = authority._preservation_root('I02_api_session_turn', f'api-peer:{key}')
    if reset_pending:
        if parent_handle is None:
            custody.mark_unknown('peer_parent_unmapped')
            return None, None, False
    elif parent_handle is None or target_entry is None or target_entry.session_id != target_id:
        if parent_handle is not None:
            authority._preservation_release_unaccepted(parent_handle)
        custody.mark_unknown('peer_parent_unmapped')
        return None, (home, {
            'status': 'failed', 'delivery_id': key,
            'reason': 'peer_parent_unmapped',
        }), False
    authority._preservation_pending_root(
        'bot:' + key, parent_handle,
        principal_id='automation:' + target_entry.session_key, session_id=target_id)
    return parent_handle, None, True


async def admit_peer_turn(runner, session_id, message, author, *, custody=None):
    """``(profile home, receipt)`` when *session_id* is the Bot Chat and the turn was admitted to its
    FIFO; None when the target is any other session (the caller runs it as an API turn)."""
    from gateway.session_authorities import served_profile_name
    from gateway.session_bot import deliver
    authority = _authority(runner)
    if authority is None:
        return None
    tip = bot_chat_tip(authority, session_id)
    if tip is None:
        return None
    home = Path(authority.db.db_path).parent.resolve()
    actor, tip, displacement_failure = await _prepare_peer_bot_chat(authority, tip, custody, home)
    if displacement_failure is not None:
        return displacement_failure
    key = uuid.uuid4().hex
    nonlocal_refusal = _reject_nonlocal_peer_turn(authority, tip, custody, home, key)
    if nonlocal_refusal is not None:
        return nonlocal_refusal
    params = dict(id=key, profile=served_profile_name(home), message=message)
    if author is not None:
        params['author'] = dict(author)
    # Pin the target selected above.  This lets the existing deliver() target gate detect a
    # concurrent title rotation instead of silently sending the peer turn to another session.
    target_id = authority.logical_owner(tip['id'])
    target_entry, reset_pending, target_valid = _peer_delivery_target(
        runner, authority, session_id, target_id)
    if not target_valid:
        return None
    params['session_id'] = target_entry.session_id if reset_pending else target_id

    parent_handle, parent_failure, parent_valid = _prepare_peer_delivery_parent(
        authority, custody, home, key, target_entry, target_id, reset_pending,
    )
    if not parent_valid:
        return parent_failure

    if custody is not None:
        refusal = custody.mark_admission_started()
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, 'code', 'peer_admission_pending'))
            return home, {
                'status': 'failed', 'delivery_id': key,
                'reason': getattr(refusal, 'code', 'peer_admission_pending'),
            }

    def _bind_admitted(record):
        if custody is None or not isinstance(record, dict):
            return
        if custody.parent_bound and custody.initial_admission_id == record.get('admission_id'):
            return
        record.update({
            "_api_producer_phase": "bot_i02",
            "_api_producer_source": "bot_chat",
        })
        refusal = custody.bind_peer_record(record, authority, expected_parent=parent_handle)
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, 'code', 'peer_parent_unmapped'))
            return
        authority._preservation_forget_pending_root(parent_handle)
        refusal = custody.mark_worker_submitted()
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, 'code', 'peer_worker_submission_unknown'))

    try:
        record = await deliver(
            SimpleNamespace(authority=authority, actor=actor), params,
            on_admitted=_bind_admitted if custody is not None else None,
            _peer_custody=custody if custody is not None else None,
            _peer_parent=parent_handle if custody is not None else None,
        )
    except asyncio.CancelledError:
        if custody is not None:
            # ``deliver`` shields the tracked mutation.  Keep this handle unknown while the
            # late callback binds any admission that commits after the caller disappeared.
            custody.request_no_worker()
            custody.mark_unknown('peer_admission_cancelled')
        raise
    except RuntimeStoreError as exc:
        if custody is not None:
            custody.mark_admission_closed()
        reason, error = _refusal(authority, tip, exc)
        record = {'status': 'failed', 'delivery_id': key, 'reason': reason, 'error': error}
    except (OSError, TypeError, ValueError, LookupError, RuntimeError):
        if custody is not None:
            custody.mark_admission_closed()
            custody.mark_unknown('peer_admission_interrupted')
        raise
    if (parent_handle is not None and custody is not None and not custody.parent_bound
            and not any(handle is parent_handle for handle in authority._preservation_handles.values())):
        authority._preservation_release_unaccepted(parent_handle)
    _bind_admitted(record)
    # The exact authority that admitted this FIFO row is an internal custody input.  Do not
    # rediscover it from the launch profile: multiplexed profiles can share one process while
    # only this authority owns the admission generation.
    if isinstance(record, dict):
        record = {
            **record,
            '_api_admitting_authority': authority,
            '_api_producer_phase': 'bot_i02',
            '_api_producer_source': 'bot_chat',
        }
    return home, record


async def await_live_delivery(adapter, home, record, timeout, *, should_stop=None):
    """Wait for a live Bot Chat receipt through whichever door admitted it: the authority FIFO
    (in-process waiter) or the legacy owner mailbox (``await_delivery_async``)."""
    candidate = record.get('_api_admitting_authority')
    authority = (
        candidate
        if (
            getattr(candidate, 'db', None) is not None
            and getattr(candidate, 'profile_id', None) == record.get('profile_id')
        )
        else _authority(adapter.gateway_runner)
    )
    if authority is not None and record.get('admission_id'):
        return await await_peer_receipt(authority, record, timeout, should_stop=should_stop)
    from tools.bot_live_delivery import await_delivery_async
    return await await_delivery_async(home, record['delivery_id'], timeout, should_stop=should_stop) or record


async def await_peer_receipt(authority, record, timeout, *, should_stop=None):
    """The delivery's receipt after it settles for good, *timeout* lapses, or *should_stop* says so.

    Each pass re-reads which admission is live (``peer_wait_admission``): after a transient failure
    the owner admits ONE retry, whose id is the only future that fires again; the original's interim
    ``failed`` (retry still eligible) reads as pending, never as the answer."""
    from gateway.session_bot import peer_wait_admission
    if record['status'] not in _PENDING:
        return record
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        live, interim = await asyncio.to_thread(peer_wait_admission, authority, record)
        # Register the waiter BEFORE re-reading: a settle between the two either shows in the read
        # or resolves this future; a settle before both would otherwise leave a future nobody pops.
        waiter = None if live is None or interim else authority.waiters.setdefault(live, loop.create_future())
        current = _receipt(authority, record)
        if live is None and current['status'] in _PENDING:
            continue  # The derived retry committed while the receipt was being read.
        if live == record['admission_id'] and not interim and current['status'] == 'failed' \
                and not current.get('retry_admission_id'):
            continue  # The original settled failed after the live read: re-derive (its retry may be owed).
        if interim:
            current['status'] = 'claimed'  # the retry is being admitted
        if live is None or current['status'] not in _PENDING:
            await _owner_published(authority, record, deadline)
            return current
        if should_stop is not None and should_stop():
            return current
        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            return current
        tick = 0.05 if interim else (0.5 if should_stop is not None else remaining)
        if waiter is None:
            await asyncio.sleep(tick if remaining is None else min(tick, remaining))
        else:
            await asyncio.wait([waiter], timeout=tick if remaining is None else min(tick, remaining))


async def _owner_published(authority, record, deadline):
    """Answer only once the owner's reply watcher published the outcome this waiter read from the
    FIFO: its receipt write runs off-loop, and a re-read of the delivery must not say ``queued``
    after the sender already holds the reply."""
    watcher = getattr(authority, '_bot_reply_watchers', {}).get(record['delivery_id'])
    if watcher is not None and not watcher[1].done():
        remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
        await asyncio.wait([watcher[1]], timeout=remaining)


def _receipt(authority, record):
    from gateway.session_bot import _result
    return {**record, **_result(authority, record)}
