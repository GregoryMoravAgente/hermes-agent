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
            reason, hint = 'bot_chat_not_local', "it is an API conversation; rename or delete it there"
        logger.warning('Peer DM into Bot Chat %s refused: %s (not_found: field=%s detail=%s)',
                       tip['id'], reason, exc.field, exc.detail)
        return reason, f"Bot Chat admission refused: {reason} (this profile's 'Bot Chat' is not a local chat; {hint})"
    return reason, f'Bot Chat admission refused: {reason}'


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
    # The API key already authenticated the peer; the turn enters the chat as its owner's own
    # input, the way owner recovery re-admits queued deliveries (``recover_bot_deliveries``).
    actor = Principal(tip['user_id'], authority.profile_id,
                      frozenset({'session:submit', 'session:read'}), 'api-peer-dm')
    key = uuid.uuid4().hex
    params = dict(id=key, profile=served_profile_name(home), message=message)
    if author is not None:
        params['author'] = dict(author)

    parent_handle = None
    if custody is not None:
        parent_handle = authority._preservation_root('I02_api_session_turn', f'api-peer:{key}')
        target_id = authority.logical_owner(tip['id'])
        live_target = authority.sessions.get(target_id)
        target_entry = (
            runner.session_store.lookup_by_session_key(live_target.route)
            if live_target is not None and getattr(live_target, 'route', None) else None)
        if parent_handle is None or target_entry is None or target_entry.session_id != target_id:
            if parent_handle is not None:
                authority._preservation_release_unaccepted(parent_handle)
            custody.mark_unknown('peer_parent_unmapped')
            return home, {
                'status': 'failed', 'delivery_id': key,
                'reason': 'peer_parent_unmapped',
            }
        authority._preservation_pending_root(
            'bot:' + key, parent_handle,
            principal_id='automation:' + target_entry.session_key, session_id=target_id)

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
        from hermes_state_runtime import get_session_admission
        record.update({
            "_api_producer_phase": "bot_i02",
            "_api_producer_source": "bot_chat",
        })
        row = get_session_admission(authority.db, admission_id=record.get('admission_id'))
        authority._preservation_bind_or_release(row, parent_handle)
        refusal = custody.bind_peer_record(record, authority)
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, 'code', 'peer_parent_unmapped'))
            return
        refusal = custody.mark_worker_submitted()
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, 'code', 'peer_worker_submission_unknown'))

    try:
        record = await deliver(
            SimpleNamespace(authority=authority, actor=actor), params,
            on_admitted=_bind_admitted if custody is not None else None)
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
    if parent_handle is not None and not any(
            handle is parent_handle for handle in authority._preservation_handles.values()):
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
