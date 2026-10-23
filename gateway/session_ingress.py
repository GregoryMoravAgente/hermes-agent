"""Trusted messaging admission and the existing TurnRunner invocation boundary."""
import asyncio
import logging
import sqlite3
from contextvars import ContextVar
from contextlib import nullcontext
from dataclasses import dataclass, field, replace

from gateway.platforms.event import MessageEvent, MessageType
from gateway.session_envelope import restore_native
from hermes_state_runtime import RuntimeStoreError

logger = logging.getLogger(__name__)


class DeliveryBoundaryError(RuntimeError):
    """A delivery exception annotated by the source-owned outbound stage."""

    def __init__(self, reason: str, *, effect_state: str):
        self.reason = reason
        self.effect_state = effect_state
        super().__init__(reason)


@dataclass(frozen=True)
class DeliveryReceipt:
    admission_id: str
    attempted: bool
    delivered: bool
    reason: str | None = None
    # ``none`` is retryable only when the source reached a stage before any outbound adapter call.
    # ``ambiguous`` never authorizes a whole-response resend.
    effect_state: str = "none"


@dataclass
class PreservedAutomationHandoff:
    """Private one-shot carrier for a fired loop tick entering the existing automation writer.

    The carrier is process-local and object-bound.  It is not ingress metadata and does not add a
    second ledger: the coordinator handle remains the only custody record until the normal
    ``admit_automation`` writer binds the SQLite row.
    """

    authority: object
    handle: object
    owner_class: str
    request_id: str
    principal_id: str
    session_id: str
    source_identity: str
    state: str = field(default="prepared", init=False)

    def validate(self, authority, adapter, event: MessageEvent) -> None:
        if authority is not self.authority or self.owner_class != "I08_goals_loops_heartbeat":
            raise RuntimeStoreError("preservation_owner_mismatch")
        handle = self.handle
        coordinator = authority._preservation_coordinator()
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        record = (getattr(coordinator, "_records", {}).get(lease_id)
                  if coordinator is not None and lease_id is not None else None)
        if (record is None or record.handle is not handle or record.state not in {"accepted", "finalizing"}
                or getattr(handle, "owner_class", None) != self.owner_class
                or getattr(handle, "profile_id", None) != authority.profile_id
                or getattr(handle, "authority_epoch", None) != authority.epoch
                or coordinator is None or coordinator._parent(handle) is None):
            raise RuntimeStoreError("preservation_owner_unmapped")
        expected_owner_id = authority._automation_owner_id(
            self.owner_class, self.session_id, self.request_id,
        )
        if getattr(handle, "owner_id", None) != expected_owner_id:
            raise RuntimeStoreError("preservation_owner_mismatch")
        runner = authority.runner
        key = runner._session_key_for_source(event.source)
        entry = runner.session_store.lookup_by_session_key(key)
        from gateway.session_automation_custody import automation_source_identity
        actual_source_identity = automation_source_identity(event, key)
        if (entry is None
                or authority.logical_owner(entry.session_id) != self.session_id
                or self.principal_id != "automation:" + key
                or self.source_identity != actual_source_identity
                or event.message_id != self.request_id
                or not event.internal
                or event.message_type is not MessageType.TEXT
                or event.is_command()
                or adapter is None
                or runner._adapter_for_source(event.source) is not adapter):
            raise RuntimeStoreError("preservation_owner_mismatch")

    def mark_admitted(self) -> None:
        if self.state == "unknown":
            raise RuntimeStoreError("preservation_owner_unmapped")
        self.state = "admitted"

    def mark_unknown(self, reason: str) -> None:
        if self.state in {"unknown", "admitted"}:
            return
        coordinator = self.authority._preservation_coordinator()
        if coordinator is not None:
            coordinator.mark_unknown(self.handle, reason)
        self.state = "unknown"


async def admit_preserved_automation(authority, adapter, event: MessageEvent,
                                     handoff: PreservedAutomationHandoff):
    """Consume one fired loop handoff through the existing durable automation admission path."""
    event._preservation_effect_started = True
    try:
        handoff.validate(authority, adapter, event)
        receipt = await authority.admit_automation(
            adapter, event, handoff.request_id, _preservation_handle=handoff.handle,
        )
        handoff.mark_admitted()
        return receipt
    except BaseException:
        # fire_tick already persisted the producer effect; a failed consumer must retain this exact
        # handle unknown, including cancellation/SystemExit, instead of releasing or replacing it.
        handoff.mark_unknown("join_failed")
        raise


async def admit_preserved_route(runner, authority, event: MessageEvent, source, handoff):
    if event.get_command():
        handoff.mark_unknown("command_path_not_custody_bound")
        return False, None
    if authority is None or executing_admission.get():
        handoff.mark_unknown("runtime_custody_unavailable" if authority is None else "join_failed")
        return True, None
    adapter = runner._delivery_adapter_for(source)
    if adapter is None:
        handoff.mark_unknown("no_consumer")
        return True, None
    return True, await admit_preserved_automation(authority, adapter, event, handoff)

admission_author = ContextVar('admission_author', default=None)
executing_admission = ContextVar('executing_admission', default=False)
# The live session while the executing turn is one its viewer submitted (``is_viewer_input``), else None.
viewer_turn: ContextVar = ContextVar('viewer_turn', default=None)
# Set by a busy-path caller that must be released once its input is durably queued.
busy_acceptance = ContextVar('busy_acceptance', default=None)
# Set beside it by a caller that must also know when the row is durably committed (a busy burst's
# commit task, which command barriers and /restart wait on): ``busy_acceptance`` may already resolve
# once the input's FIFO position is fixed behind an earlier input of its chat still capturing.
busy_commit = ContextVar('busy_commit', default=None)
# ``_validate_native`` clauses about the message's own content, safe to name to the sender's chat.
_CONTENT_CLAUSES = frozenset({
    'text', 'auto_skill', 'channel_prompt', 'channel_context', 'reply_expected',
    'media_urls', 'media_types', 'media_text_inlined', 'media_types_length', 'media_text_inlined_length',
})

# Nothing was committed (main's busy-queue cap); /retry answers it before rewriting (#139257).
QUEUE_FULL_REPLY = ('⏳ Too many messages are already waiting in this conversation; '
                    'send this again after the current reply.')


async def admit_message(authority, event):
    from gateway.session_authority import messaging_admission
    accepted, committed = busy_acceptance.get(), busy_commit.get()
    busy_acceptance.set(None)  # the drain task this schedules must not inherit the busy caller's future
    busy_commit.set(None)
    ingress = messaging_admission.set(True if accepted is None else accepted)
    try:
        receipt = await authority.admit_native(event)
        if committed is not None and not committed.done():
            committed.set_result(None)  # the row is durable (any status): a barrier may go on
    except RuntimeStoreError as exc:
        if exc.reason == 'stale_generation' and exc.field == 'route':
            return None  # a later /stop or /new of the chat fenced it, as main cancels a queued row
        if exc.reason == 'invalid_params' and exc.field:
            # Refused before commit (_validate_native): a refusal notice instead of a generic turn error.
            # The reply reaches the whole chat, so only content clauses are named; trust and provenance
            # clauses (and any new one) stay generic, and gateway.log keeps the clause (``_refuse``).
            named = f'{exc.reason}: {exc.field}' if exc.field in _CONTENT_CLAUSES else exc.reason
            return (f'⚠️ Hermes refused this message ({named}), so it was not saved '
                    'or answered. Ask the operator to check gateway.log.')
        if exc.reason != 'queue_full':
            raise
        # Nothing was committed (main's busy-queue cap): say so instead of a generic turn error.
        return QUEUE_FULL_REPLY
    finally:
        messaging_admission.reset(ingress)
    preservation_key = getattr(event, '_preservation_operation_key', None)
    if preservation_key is not None:
        authority._preservation_operation_bind_admission(preservation_key, receipt.admission_id)
    if receipt.status == 'terminal':
        return None
    # Only the delivery waiter is process-local; execution reads the committed snapshot.
    authority.native_waiters.add(receipt.admission_id)
    waiter = authority.waiters.setdefault(receipt.admission_id, asyncio.get_running_loop().create_future())
    adopters = _marker_adopters(authority)
    if getattr(event, '_turn_marker_handoff', False):
        # The adapter lifecycle that sends this reply releases the turn's crash marker only once
        # the reply is in the delivery ledger; the drain hands its marker to this event.
        adopters[receipt.admission_id] = event
    # A drain already running can claim, run and settle the row before this caller resumes; one
    # that settled it already sent (or owes) the reply itself and resolves no waiter registered now.
    from hermes_state_runtime import get_session_admission
    row = get_session_admission(authority.db, admission_id=receipt.admission_id)
    status = 'terminal' if row is None else row['status']  # None: deleted with its chat, nothing owed
    if status in ('terminal', 'unknown'):
        authority.native_waiters.discard(receipt.admission_id)
        if authority.waiters.get(receipt.admission_id) is waiter:
            del authority.waiters[receipt.admission_id]
        if adopters.get(receipt.admission_id) is event:
            del adopters[receipt.admission_id]
        if status == 'terminal' or getattr(event, '_classic_owner_unknown', False):
            return None
        return pause_notice(authority, receipt.ref, 'unknown_execution')
    if accepted is not None and not accepted.done():
        accepted.set_result(None)
    try:
        return await asyncio.shield(waiter)
    except RuntimeStoreError as exc:
        if getattr(event, '_classic_owner_unknown', False):
            return None
        return pause_notice(authority, receipt.ref, exc.reason)
    finally:
        if adopters.get(receipt.admission_id) is event:
            bridge = getattr(event, '_classic_execution_bridge', None)
            if getattr(bridge, 'post_delivery_callback_state', None) not in {
                'registering', 'registered', 'proof_recorded', 'settled', 'not_required',
            }:
                del adopters[receipt.admission_id]


def _marker_adopters(authority):
    adopters = getattr(authority, 'marker_adopters', None)
    if adopters is None:
        adopters = authority.marker_adopters = {}
    return adopters


_MARKER_FIELDS = ('_gateway_active_turn_session_key', '_gateway_active_turn_token')


async def _hand_over_turn_marker(authority, admission_id, event):
    """The executed turn's durable active-turn marker outlives the handler, as the in-process
    turn's does on main, and is released only once its reply is in the delivery ledger (or nothing
    is owed). A kill between the terminal commit and that ledger write then leaves a marked turn
    whose persisted reply the next unclean boot ledgers and sends once, without new inference,
    instead of a settled answer nobody owes. The waiting adapter lifecycle adopts it; a recovered
    no-waiter reply keeps it until ``deliver_settled``; any other turn releases it here."""
    adopter = _marker_adopters(authority).get(admission_id)
    if adopter is not None:
        _move_marker(event, adopter)
        return
    if admission_id in authority.pending_deliveries:
        # Released by ``deliver_settled``, also when settlement failed and dropped the delivery.
        _held_markers(authority)[admission_id] = event
        return
    await _release_turn_marker(authority, event)


def _move_marker(source, target):
    for name in _MARKER_FIELDS:
        if hasattr(source, name):
            setattr(target, name, getattr(source, name))
            delattr(source, name)


def _held_markers(authority):
    held = getattr(authority, 'held_turn_markers', None)
    if held is None:
        held = authority.held_turn_markers = {}
    return held


def _registry(authority, name):
    found = getattr(authority, name, None)
    if found is None:
        found = {}
        setattr(authority, name, found)
    return found


def begin_busy_preservation(adapter, event, session_key):
    """Reserve the busy follow-up before the observer or burst timer is created."""
    runner = getattr(adapter, 'gateway_runner', None)
    if runner is None:
        return None, None
    from gateway.run_busy_custody import begin_classic_event
    from gateway.runtime_service_custody import ServiceCustodyError

    if not begin_classic_event(runner, event, session_key, 'fifo', state='queued'):
        raise ServiceCustodyError('preservation_owner_unmapped')
    authority = getattr(event, '_classic_custody_authority', None)
    key = getattr(event, '_classic_custody_key', None)
    if authority is None or not isinstance(key, str) or not key:
        return None, None
    return authority, key


def finish_busy_preservation(authority, key, accepted, *, unknown=False):
    if authority is None or key is None:
        return
    if not accepted:
        try:
            row = authority._preservation_operation_row(key)
        except (OSError, RuntimeError, sqlite3.Error, RuntimeStoreError, TypeError, ValueError, KeyError):
            # Exact busy admission read failure retains custody; no exception value is published.
            authority._preservation_operation_unknown(key, 'read_failed')
            return
        if row is None:
            authority._preservation_operation_release_unaccepted(key)
        elif row.get('status') == 'terminal':
            authority._preservation_operation_terminal(key, f'busy-refused:{key}')
        else:
            authority._preservation_operation_unknown(key, 'join_failed')
    elif unknown:
        authority._preservation_operation_unknown(key, 'join_failed')
    else:
        try:
            row = authority._preservation_operation_row(key)
        except (OSError, RuntimeError, sqlite3.Error, RuntimeStoreError, TypeError, ValueError, KeyError):
            # Exact busy admission read failure retains custody; no exception value is published.
            authority._preservation_operation_unknown(key, 'read_failed')
            return
        if row is None:
            authority._preservation_operation_unknown(key, 'read_failed')
        elif row.get('status') == 'terminal':
            admission_id = row.get('admission_id')
            from gateway.session_classic_delivery import (
                CLASSIC_CALLBACK_NOT_REQUIRED,
                CLASSIC_CALLBACK_UNKNOWN,
                classic_callback_requirement,
                classic_post_delivery_callback_settled,
            )
            callback_requirement = classic_callback_requirement(authority, admission_id)
            if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
                # A missing, stale, foreign, or unreadable callback witness remains unknown.
                authority._preservation_operation_unknown(key, 'unmapped_writer')
            elif (
                callback_requirement == CLASSIC_CALLBACK_NOT_REQUIRED
                or classic_post_delivery_callback_settled(authority, admission_id)
            ):
                # The callback and canonical row are settled; terminalize the I06 operation, then
                # release only when the existing bridge carries the exact retired operation proof.
                authority._preservation_operation_terminal(key, f'busy-delivery:{key}')
                from gateway.session_classic_delivery import release_busy_retired_callback
                release_busy_retired_callback(authority, key, admission_id)
        else:
            authority._preservation_operation_unknown(key, 'join_failed')


def waiter_replies(authority):
    """admission -> the reply a live native waiter is about to send, kept only until the drain
    settles the turn, so a turn fenced ``unknown`` can still owe it to the chat."""
    return _registry(authority, 'waiter_replies')


def retain_unsettled_delivery(authority, admission_id):
    """Settlement could not commit and the turn is fenced ``unknown``, but this owner keeps its
    captured answer for Discard (``resolve_unknown``). The reply stays owed to its destination,
    and so does the turn's crash marker: the live waiter is released with a pause notice, so the
    marker is taken back from it. Restart while unknown clears the marker (no answer is owed,
    ``release_unknown_turn_markers``); a Discard that commits the captured answer makes it owed,
    sent once by ``deliver_resolved`` or, after a kill before that send, by the boot ledger."""
    delivery = waiter_replies(authority).pop(admission_id, None)
    if delivery is not None:
        adopter = _marker_adopters(authority).get(admission_id)
        if adopter is not None:
            _move_marker(adopter, delivery[1])
            _held_markers(authority)[admission_id] = delivery[1]
    else:
        delivery = authority.pending_deliveries.pop(admission_id, None)
    if delivery is not None:
        _registry(authority, 'unsettled_deliveries')[admission_id] = delivery


async def deliver_resolved(authority, admission_id, *, owed):
    """After a Discard committed: send the retained reply when its captured answer committed with
    the resolution (``owed``), else release its marker and drop it. Popped first, so a repeated
    resolution never sends twice; a turn the drain still owns is delivered by the drain."""
    delivery = _registry(authority, 'unsettled_deliveries').pop(admission_id, None)
    if delivery is None:
        return
    if owed:
        authority.pending_deliveries[admission_id] = delivery
    await deliver_settled(authority, admission_id)


async def _release_turn_marker(authority, event):
    if getattr(event, '_gateway_active_turn_token', None):
        await authority.runner._clear_durable_active_turn(event)


def pause_notice(authority, ref, reason):
    """The committed input stays queued behind a paused FIFO (a turn lost across an owner
    restart, or a head the preflight refused). Tell the platform user once per pause episode
    through the ordinary reply path; later messages onto the same pause are admitted silently."""
    live = authority.sessions[ref.session_id]
    if live.pause_notified:
        return None
    live.pause_notified = True
    if reason == 'runtime_draining':
        # Transient: the row runs when the restarted owner drains it, so /reset would be wrong advice.
        return '⏳ Hermes is restarting — your message is saved and will be answered when it is back.'
    if reason == 'session_busy':
        # Transient: queued behind work already running on this conversation (a worker execution).
        return '⏳ Your message is saved and will be answered when the work running in this conversation finishes.'
    if reason == 'unknown_execution':
        # GW-10: the way out is in this chat. /discard acknowledges the lost turn (never replayed)
        # through the same fenced resolve_unknown as Desktop/TUI, and the saved messages then run.
        # Slack registers no native /discard (its slash cap): the umbrella `/hermes discard` reaches it.
        slack = getattr(getattr(live.source, 'platform', None), 'value', None) == 'slack'
        discard = '/hermes discard' if slack else '/discard'
        return ('⏸️ This conversation is paused: a previous turn did not finish when Hermes restarted, '
                f'and only you know whether to send it again. Your message is saved. Send {discard} to drop '
                'the lost turn and continue (it is not re-run), or /reset to start a fresh conversation.')
    cause = f'a queued message could not be re-authorized ({reason})'
    return (f'⏸️ This conversation is paused: {cause}. Your message is saved but will not be answered here. '
            'Send /reset to start a fresh conversation, or ask the operator to resume this one.')


def row_turn_author(policy, row):
    """Who wrote the admitted input, for memory attribution only: the producer's stamp when it
    left one, else the forwarded peer bound into the session policy; never grants anything."""
    payload = row['payload']
    for key in ('local_automation_v1', 'api_turn_v1', 'cli_turn_v1'):
        author = (payload.get(key) or {}).get('turn_author')
        if author is not None:
            return author
    from gateway.session_a2a import forward_author
    return forward_author(policy)


def _managed_process_route(row):
    """Read the producer-owned route marker without treating missing context as permission."""
    payload = row.get('payload') or {}
    if not isinstance(payload, dict):
        from gateway.runtime_service_custody import ServiceCustodyError
        raise ServiceCustodyError('producer_policy_invalid')
    markers = []
    for descriptor in payload.values():
        if not isinstance(descriptor, dict):
            continue
        marker = descriptor.get('producer_process_mode', descriptor.get('process_route'))
        if marker is not None:
            if type(marker) is not str or not marker:
                from gateway.runtime_service_custody import ServiceCustodyError
                raise ServiceCustodyError('producer_policy_invalid')
            markers.append(marker)
    if len(set(markers)) > 1:
        from gateway.runtime_service_custody import ServiceCustodyError
        raise ServiceCustodyError('producer_policy_conflict')
    if not markers:
        # Legacy/direct rows retain their existing supported route.  A managed producer must
        # stamp its mode at the actual Gateway/CLI owner; ContextVar absence is never a marker.
        return False
    marker = markers[0]
    if marker == 'managed_turn':
        return True
    if marker in {
        'live_profile_discovery', 'standalone_maintenance', 'injected_no_spawn', 'direct',
        'classic_busy',
    }:
        return False
    from gateway.runtime_service_custody import ServiceCustodyError
    raise ServiceCustodyError('producer_policy_invalid')


def _bind_process_route(authority, ref, row, *, require_managed=False):
    from gateway.runtime_service_custody import admission_binding, bind_turn
    if not _managed_process_route(row):
        if require_managed:
            from gateway.runtime_service_custody import ServiceCustodyError
            # A legacy row has no authenticated producer route.  Current policy
            # cannot safely infer that it was a managed producer, so refuse before
            # the child spawn rather than silently taking the untracked path.
            raise ServiceCustodyError('managed_producer_binding_missing')
        return None, None
    binding = admission_binding(authority, ref, row)
    return binding, bind_turn(binding)


def _reset_process_route(token) -> None:
    if token is not None:
        from gateway.runtime_service_custody import reset_turn
        reset_turn(token)


def _mark_classic_owner_unknown(authority, row, event) -> None:
    """Carry an execution-owner loss back to the ingress event that owns its waiter.

    The drain restores a fresh event from the durable row.  The live adapter waiter is still
    attached to the original marker adopter, so marking only the restored copy makes the waiter
    publish a pause notice even though the exact operation is already retained as unknown.
    """
    event._classic_owner_unknown = True
    admission_id = row.get('admission_id') if isinstance(row, dict) else None
    if not isinstance(admission_id, str) or not admission_id:
        return
    adopter = _marker_adopters(authority).get(admission_id)
    if adopter is not None:
        adopter._classic_owner_unknown = True


def _bind_classic_execution_event(authority, row, event) -> None:
    """Bind a restored direct-classic event or retain its exact operation as unknown."""
    from gateway.session_bot_custody import bot_execution_binding_for_admission
    if bot_execution_binding_for_admission(authority, row) is not None:
        # Bot delivery owns an I07 root and an I06 child, but intentionally has no classic
        # producer event or bridge. Its typed binding was authenticated above, so the classic
        # resolver must not reinterpret the child as an ownerless classic occurrence.
        return
    resolver = getattr(authority, '_classic_execution_owner_for_admission', None)
    if not callable(resolver):
        return
    owner, error = resolver(row)
    if error is not None:
        _mark_classic_owner_unknown(authority, row, event)
        from gateway.session_managed_worker import ManagedExecutionUnknown
        raise ManagedExecutionUnknown('classic_owner_unknown')
    if owner is None:
        return
    from gateway.session_classic_custody import bind_classic_execution_event
    if bind_classic_execution_event(owner, event, row):
        return
    key = getattr(owner, '_classic_custody_key', None)
    if isinstance(key, str):
        authority._preservation_operation_unknown(key, 'execution_owner_bind_failed')
    _mark_classic_owner_unknown(authority, row, event)
    from gateway.session_managed_worker import ManagedExecutionUnknown
    raise ManagedExecutionUnknown('classic_owner_bind_failed')


async def execute_admission(authority, ref, row):
    from gateway.session_completion_notes import note_if_unviewed
    noted = await note_if_unviewed(authority, ref, row)  # #134889 LC-7: no viewer, no turn
    if noted is not None:
        return noted
    from gateway.session_policy import policy_for_source
    policy = policy_for_source(authority.runner, authority.sessions[ref.session_id].source)
    if policy is not None and policy.source == 'cron':
        from gateway.session_cron import execute
        return await execute(authority, ref, row, policy)
    local_policy = policy
    from gateway.session_managed_worker import managed_policy, admit_managed
    policy = managed_policy(authority, ref)
    if policy is not None:
        # Managed workers are still I04 admissions.  Bind the exact FIFO child before
        # the worker route can reserve I16 custody or create a process; the ordinary
        # route below keeps its historical binding point and reset.
        managed_binding = None
        managed_binding_token = None
        try:
            managed_binding, managed_binding_token = _bind_process_route(
                authority, ref, row, require_managed=True,
            )
            return await admit_managed(authority, ref, row, policy, managed_binding)
        finally:
            _reset_process_route(managed_binding_token)
    live = authority.sessions[ref.session_id]
    native = row['admission_id'] in authority.native_waiters
    authority.native_waiters.discard(row['admission_id'])
    if 'native_text_v1' in row['payload']:
        event = restore_native(row['payload'], authority.runner)
    else:
        from gateway.session_ingress_media import restore_attachments
        event = MessageEvent(text=row['payload']['text'], source=live.source,
                             message_id=row['admission_id'], **restore_attachments(row['payload']))
        if 'local_automation_v1' in row['payload']:
            from gateway.session_automation import restore_local_automation
            event = restore_local_automation(authority, ref, row)
    _bind_classic_execution_event(authority, row, event)
    if event.internal and 'gateway_session_id' in event.metadata:
        # Main's ownership proof (802fd5782f3): the session the pre-claim check just validated, so the
        # turn's resolver never re-reads (and on a transient failure drops) an admitted completion.
        event.metadata['gateway_proven_session_id'] = (
            authority.physical_target(ref) if 'native_text_v1' in row['payload'] else event.metadata['gateway_session_id'])
    provenance = row['payload'].get('native_text_v1', {}).get('provenance')
    home = None
    if provenance is not None:
        from gateway.session_ingress_context import restore_provenance
        home = restore_provenance(authority.runner, event.source, provenance)
    _turn_binding, turn_binding_token = _bind_process_route(authority, ref, row)
    from gateway.config import Platform
    from gateway.session_api_turn import (
        api_execution, apply_api_notification_presentation, bind_api_notification_metadata,
        prepare_api_execution,
    )
    from gateway.session_results import execution_result
    is_api = live.source.platform == Platform.API_SERVER
    if not (event.internal or is_api or live.source.platform == Platform.LOCAL):
        # The claim on a chat route (native or a viewer's submit): a /new before the turn resolves
        # its session must not move it onto the fresh conversation (main's /new cancelled its task).
        event._claimed_owner = (ref.session_id, row['generation'])
    author = event.metadata.get('turn_author')
    author_token = admission_author.set(author if author is not None else row_turn_author(local_policy, row))
    from gateway.session_operator import is_viewer_input
    viewer_token = viewer_turn.set(live if is_viewer_input(row) else None)
    api_token = api_execution.set(None)
    captured = {}
    result_token = execution_result.set(captured)
    token = executing_admission.set(True)
    # This drain, not the handler's unwind, releases the turn's crash marker (see below).
    event._turn_marker_handoff = True
    handed = False
    try:
        with await _execution_scope(authority, home):
            if is_api:
                # Committed media resolves under the owning profile's home, like admission did.
                prepared = prepare_api_execution(authority, ref, row['payload'])
                api_execution.set(prepared)
                bind_api_notification_metadata(event, prepared)
                if isinstance(event.text, list):
                    # The durable transcript keeps the committed media references (the same
                    # ``[Image attached ...]`` hints native transports persist), not only the caption.
                    event.text = '\n'.join(part['text'] for part in prepared['content'] if part.get('type') == 'text')
                event.allow_gateway_control = False
                event.internal = True  # trust comes from the private binding and preclaim, never client JSON
            response = await authority.runner._handle_message(event)
            result = captured.get('terminal_result', captured.get('result'))
            if result is None:
                # No TurnRunner result means the handler answered without executing the turn.
                # A turn that never ran because it failed (agent initialization raised, history
                # unreadable) recorded its own failure via ``record_unexecuted_failure`` on every
                # surface; any other reply here is a deliberate notice. An API caller asked for
                # work, so its receipt is a failure either way, never a completed apology; so did
                # a finite (``chat -q`` / ``-z``) prompt, whose exit code is the only verdict a
                # script reads, unless the prompt was a command and the reply is its answer.
                result = {'final_response': response or '', 'messages': []}
                if is_api or (row['payload'].get('finite') is True and not event.get_command()):
                    result = {'final_response': '', 'messages': [], 'failed': True, 'completed': False,
                              'error': response or 'The admitted turn failed.'}
            if is_api:
                # Project both real captured outcomes and no-run failures before the durable
                # pending result is published. The helper uses the event's frozen mute snapshot
                # when a turn ran and the canonical policy only for the no-run fallback.
                result = apply_api_notification_presentation(event, result, captured)
            # The drain commits this under the stream lock so no viewer reads `terminal`
            # before the completion event exists in the replay ring.
            authority.pending_results[row['admission_id']] = {
                'result': result, 'usage': captured.get('usage', {}),
                **({'session_usage': captured['session_usage']} if 'session_usage' in captured else {})}
            if not is_api and response:
                adapter = authority.runner._adapter_for_source(event.source)
                if adapter is not None:
                    # A recovered turn with no live delivery waiter: the drain sends this only after
                    # it commits the terminal outcome (deliver_settled), so a failed settlement never
                    # leaves an externally answered admission started -> unknown. A live waiter
                    # sends its own; the record only outlives a settlement fenced unknown.
                    delivery = (adapter, event, live.route, response, home)
                    if native:
                        waiter_replies(authority)[row['admission_id']] = delivery
                    else:
                        authority.pending_deliveries[row['admission_id']] = delivery
            handed = True
            await _hand_over_turn_marker(authority, row['admission_id'], event)
            return response
    finally:
        if not handed:
            await _release_turn_marker(authority, event)
        executing_admission.reset(token)
        execution_result.reset(result_token)
        api_execution.reset(api_token)
        admission_author.reset(author_token)
        viewer_turn.reset(viewer_token)
        _reset_process_route(turn_binding_token)


def _launch_body_if_launch_owner(authority):
    """A standalone owner's turn binds nothing, but the launch profile's is marked at entry: a second
    profile switching this gateway to multiplexing mid-turn must not fail its later credential reads
    closed. Any other owner (a hosted named profile) stays unmarked, never reading the launch env."""
    from pathlib import Path
    from agent.secret_scope import _is_process_home
    owner = Path(str(authority.profile_id))
    if not owner.is_absolute() or not _is_process_home(owner):
        return nullcontext()
    from tui_gateway.launch_profile_policy import launch_body
    return launch_body()


async def _execution_scope(authority, home):
    """Under multiplex, owner-side execution runs under the OWNING profile's home (agent build,
    config, secrets, state.db), never the launch profile's ambient scope; native provenance
    (``home``) refines it. A single-profile gateway keeps its ambient scope byte-for-byte.
    Secret sources (external CLIs) hydrate off the loop, as main's per-message scope does (#99519)."""
    multiplex = getattr(getattr(authority.runner, 'config', None), 'multiplex_profiles', False)
    if home is None and not multiplex:
        return _launch_body_if_launch_owner(authority)
    from gateway.run import _load_profile_secret_scope, _profile_runtime_scope

    def load():
        from pathlib import Path
        scope_home = home
        if scope_home is None:  # owner_scope's home; symbolic test ids keep the ambient scope
            owner = Path(str(authority.profile_id))
            scope_home = owner if owner.is_absolute() and owner.is_dir() else None
        return scope_home, scope_home and _load_profile_secret_scope(scope_home)

    scope_home, secrets = await asyncio.to_thread(load)
    return nullcontext() if scope_home is None else _profile_runtime_scope(scope_home, secrets)


async def deliver_settled(authority, admission_id):
    """Deliver a recovered result and return a typed receipt for managed custody."""
    def record_i10(receipt):
        if admission_id in getattr(authority, '_i10_recovery_bindings', {}):
            from gateway.session_recovery_custody import record_source_delivery_receipt
            record_source_delivery_receipt(authority, admission_id, receipt)
        return receipt

    held = _held_markers(authority).pop(admission_id, None)
    delivery = authority.pending_deliveries.get(admission_id)
    if delivery is not None and admission_id in authority.native_waiters:
        authority.native_waiters.discard(admission_id)
        authority.pending_deliveries.pop(admission_id)
        adopter = _marker_adopters(authority).get(admission_id)
        if held is not None and adopter is not None:
            _move_marker(held, adopter)
            held = None
        delivery = None
    if delivery is None:
        if held is not None:
            await _release_turn_marker(authority, held)
        return record_i10(DeliveryReceipt(
            admission_id, attempted=False, delivered=False, reason="no_delivery", effect_state="none",
        ))
    adapter, event, session_key, response, home = delivery
    managed = bool(getattr(authority, '_preservation_has_sidecar', lambda _id: False)(admission_id))
    managed = managed or admission_id in getattr(authority, '_i10_recovery_bindings', {})
    failed = False
    effect_state = "ambiguous"
    try:
        with await _execution_scope(authority, home):
            effect_state = await deliver_response(adapter, event, session_key, response)
        if effect_state == "ambiguous":
            failed = True
            if managed:
                _held_markers(authority)[admission_id] = held or event
                authority._preservation_mark_unknown(admission_id, 'delivery_ambiguous')
            else:
                logger.warning('Delivery of settled admission %s was ambiguous', admission_id)
            return record_i10(DeliveryReceipt(
                admission_id, attempted=True, delivered=False, reason="delivery_ambiguous",
                effect_state="ambiguous",
            ))
    except DeliveryBoundaryError as exc:
        failed = True
        effect_state = exc.effect_state
        reason = "delivery_failed" if effect_state == "none" else "delivery_ambiguous"
        if managed:
            # Preserve both the delivery row and the adopted marker for the exact retry.
            _held_markers(authority)[admission_id] = held or event
            authority._preservation_mark_unknown(admission_id, reason)
        else:
            logger.exception('Delivery of settled admission %s failed', admission_id)
        return record_i10(DeliveryReceipt(
            admission_id, attempted=True, delivered=False, reason=reason, effect_state=effect_state,
        ))
    except Exception:
        failed = True
        # An exception outside the source-owned pre-effect stage may have reached the platform.
        # It is therefore sticky ambiguous custody, never the one safe resend case.
        if managed:
            _held_markers(authority)[admission_id] = held or event
            authority._preservation_mark_unknown(admission_id, 'delivery_ambiguous')
        else:
            logger.exception('Delivery of settled admission %s failed', admission_id)
        return record_i10(DeliveryReceipt(
            admission_id, attempted=True, delivered=False, reason="delivery_ambiguous",
            effect_state="ambiguous",
        ))
    finally:
        if not (managed and failed):
            authority.pending_deliveries.pop(admission_id, None)
            await _release_turn_marker(authority, event)
        # The authority path does not run BasePlatformAdapter's background finally.  Consume the
        # callback here after the send/marker boundary, before the authority finalizer can release
        # the original I06 operation.  A failed/ambiguous send still reaches the callback so its
        # existing delivery-ledger/receipt failure path can retain UNKNOWN custody.
        if adapter is not None and event is not None:
            from gateway.session_classic_custody import handoff_classic_execution_receipt
            handoff_classic_execution_receipt(
                event, getattr(event, '_classic_terminal_event', None) or event,
            )
            from gateway.session_classic_delivery import fire_authority_post_delivery_callback
            await fire_authority_post_delivery_callback(
                authority, adapter, event, session_key, admission_id=admission_id,
                delivery_event=event, delivery_state=effect_state, marker_event=event,
            )
    return record_i10(DeliveryReceipt(
        admission_id, attempted=True, delivered=True, effect_state=effect_state,
    ))


async def deliver_response(adapter, event, session_key, response):
    from gateway.platforms.base import _thread_metadata_for_event, _mark_notify_metadata
    try:
        text, ttl = adapter._unwrap_ephemeral(response)
        if not text:
            return "none"
        extracted = await adapter._extract_response_content(
            text, event, session_key, is_ephemeral_response=ttl > 0,
        )
        metadata = _mark_notify_metadata(_thread_metadata_for_event(event))
        results = []
        if extracted.text_content:
            await adapter._send_final_text(event, session_key, extracted.text_content,
                                           metadata, ttl > 0, ttl, results.append)
        await adapter._deliver_attachments(event, extracted, metadata, anything_sent=bool(results),
                                           record_delivery=results.append)
    except DeliveryBoundaryError:
        raise
    except Exception as exc:
        # A generic adapter exception does not prove that no outbound effect occurred. Only an
        # explicit DeliveryBoundaryError from the source owner may authorize a pre-effect retry.
        raise DeliveryBoundaryError("delivery_failed", effect_state="ambiguous") from exc
    if not results:
        return "none"
    if all(getattr(result, "success", False) for result in results):
        return "delivered"
    # A false SendResult does not prove no platform effect; the ledger/adapter owns that boundary.
    return "ambiguous"


def _preservation_entries(authority, admission_id):
    return [
        entry for entry in authority._preservation_sidecars.values()
        if entry['admission_id'] == admission_id
    ]


def preservation_delivery_outcome(authority, admission_id):
    """Finish one exact service delivery and retain the original typed failure reason."""
    entries = _preservation_entries(authority, admission_id)
    for entry in entries:
        if entry['terminal']:
            continue
        if not entry['result_durable']:
            authority._preservation_mark_unknown(admission_id, 'delivery_before_durable_result')
            return 'delivery_before_durable_result'
        try:
            service = entry['service']
            service.complete_delivery(service.binding, entry['request'])
            # Shell-hook services are per-invocation roots with no persistent external
            # producer parent.  Their exact physical/request tuple is staged at tool
            # completion and must be closed only after the terminal admission fence above.
            # Managed ACP/app-server services have an authenticated persistent parent and
            # retain that root for the next admission; their owner closes it separately.
            if getattr(service, 'external_parent', None) is None:
                service.close()
        except Exception as exc:
            from gateway.runtime_service_custody import ServiceCustodyError
            if isinstance(exc, ServiceCustodyError) and isinstance(exc.code, str) and exc.code:
                reason = exc.code
            else:
                reason = 'delivery_finalize_unknown'
            logger.warning(
                'Sidecar delivery finalization retained for %s: %s', admission_id, reason, exc_info=True,
            )
            authority._preservation_mark_unknown(admission_id, reason)
            return reason
        entry['delivery_complete'] = True
        entry['terminal'] = True
    return 'complete'


_RETRYABLE_PARENT_FINALIZER_REASONS = {
    'external_parent_reconcile_failed',
    'external_parent_retire_failed',
    'external_parent_registry_retire_failed',
}


async def retry_preservation_delivery(authority, admission_id, reason, *, effect_state=None):
    """Perform one exact retry, limited to a source-owned pre-effect send or typed finalizer."""
    i10_binding = admission_id in getattr(authority, '_i10_recovery_bindings', {})
    if i10_binding:
        if reason != 'delivery_failed' or effect_state != 'none':
            return None
        if authority._preservation_delivery_retries.get(admission_id, 0) >= 1:
            return None
        from gateway.session_recovery_custody import begin_i10_retry, finish_i10_retry
        if not begin_i10_retry(authority, admission_id, reason, effect_state):
            authority._preservation_mark_unknown(admission_id, 'delivery_reconcile_failed')
            return None
        authority._preservation_delivery_retries[admission_id] = 1
        retry_receipt = await deliver_settled(authority, admission_id)
        if finish_i10_retry(authority, admission_id, retry_receipt, reason, effect_state):
            return retry_receipt
        authority._preservation_mark_unknown(admission_id, 'delivery_reconcile_failed')
        return None
    if not authority._preservation_has_sidecar(admission_id):
        return None
    if authority._preservation_delivery_retries.get(admission_id, 0) >= 1:
        return None
    entries = _preservation_entries(authority, admission_id)
    if len(entries) != 1:
        authority._preservation_mark_unknown(admission_id, 'delivery_reconcile_failed')
        return None
    entry = entries[0]
    service = entry.get('service')
    request = entry.get('request')
    if service is None or request is None:
        authority._preservation_mark_unknown(admission_id, 'delivery_reconcile_failed')
        return None
    if reason == 'delivery_failed' and effect_state == 'none':
        from gateway.runtime_service_custody import ServiceCustodyError
        try:
            service.reconcile_delivery(service.binding, request)
        except ServiceCustodyError:
            authority._preservation_mark_unknown(admission_id, 'delivery_reconcile_failed')
            return None
        authority._preservation_delivery_retries[admission_id] = 1
        return await deliver_settled(authority, admission_id)
    if (
        reason == 'delivery_finalize_failed'
        or reason in _RETRYABLE_PARENT_FINALIZER_REASONS
    ) and entry.get('result_durable') is True:
        authority._preservation_delivery_retries[admission_id] = 1
        return preservation_delivery_outcome(authority, admission_id)
    return None


async def _retry_pre_effect_delivery_once(
    authority, admission_id, delivery_receipt, has_sidecar, transport_delivered,
):
    if (
        not has_sidecar
        or not isinstance(delivery_receipt, DeliveryReceipt)
        or delivery_receipt.admission_id != admission_id
        or delivery_receipt.attempted is not True
        or delivery_receipt.delivered is not False
        or delivery_receipt.reason != 'delivery_failed'
        or delivery_receipt.effect_state != 'none'
    ):
        return False, delivery_receipt
    retry_receipt = await retry_preservation_delivery(
        authority, admission_id, delivery_receipt.reason,
        effect_state=delivery_receipt.effect_state,
    )
    if retry_receipt is not None and transport_delivered(retry_receipt):
        return True, retry_receipt
    return True, delivery_receipt


async def _finish_preservation_sidecar_delivery(
    authority, admission_id, delivery_receipt, physical_delivery,
    automation_binding, pre_effect_retry_attempted, waiter_delivered,
    transport_delivered,
):
    """Run the sidecar finalizer and its one bounded delivery retry."""
    from gateway.platforms.api_server_run_completion import can_stage_api_sidecar_delivery
    api_result_pending = (
        not automation_binding
        and can_stage_api_sidecar_delivery(authority, admission_id, delivery_receipt)
    )
    finalizer_outcome = None
    if (physical_delivery or (not automation_binding and (
            getattr(delivery_receipt, 'delivered', False) or waiter_delivered or api_result_pending))):
        finalizer_outcome = preservation_delivery_outcome(authority, admission_id)
        if finalizer_outcome == 'delivery_finalize_failed':
            retried = await retry_preservation_delivery(
                authority, admission_id, finalizer_outcome, effect_state='ambiguous',
            )
            if transport_delivered(retried):
                delivery_receipt = retried
                physical_delivery = True
                finalizer_outcome = preservation_delivery_outcome(authority, admission_id)
            elif isinstance(retried, str):
                finalizer_outcome = retried
            else:
                finalizer_outcome = None
    elif (not pre_effect_retry_attempted
          and getattr(delivery_receipt, 'reason', None) == 'delivery_failed'):
        retry_receipt = await retry_preservation_delivery(
            authority, admission_id, delivery_receipt.reason,
            effect_state=getattr(delivery_receipt, 'effect_state', None),
        )
        if retry_receipt is not None and transport_delivered(retry_receipt):
            delivery_receipt = retry_receipt
            physical_delivery = True
            finalizer_outcome = preservation_delivery_outcome(authority, admission_id)
    return delivery_receipt, physical_delivery, finalizer_outcome


async def _finish_preservation_delivery_impl(authority, admission_id, delivery_receipt, waiter_delivered):
    """Apply the delivery/finalizer fences and hold FIFO successors until terminal custody."""
    from gateway.session_classic_delivery import (
        CLASSIC_CALLBACK_NOT_REQUIRED,
        CLASSIC_CALLBACK_UNKNOWN,
        classic_callback_requirement,
        wait_for_classic_post_delivery_callback,
    )
    from gateway.session_automation_custody import _AUTOMATION_OWNER_CLASSES
    from gateway.platforms.api_server_run_completion import stage_api_finalizer_delivery, completion_for
    completion = completion_for(authority, admission_id)

    has_sidecar = authority._preservation_has_sidecar(admission_id)
    i10_binding = admission_id in getattr(authority, '_i10_recovery_bindings', {})

    def transport_delivered(receipt):
        return (
            isinstance(receipt, DeliveryReceipt)
            and receipt.admission_id == admission_id
            and receipt.attempted is True
            and receipt.delivered is True
            and receipt.reason is None
            and receipt.effect_state == 'delivered'
        )

    pre_effect_retry_attempted, delivery_receipt = await _retry_pre_effect_delivery_once(
        authority, admission_id, delivery_receipt, has_sidecar, transport_delivered,
    )
    physical_delivery = transport_delivered(delivery_receipt)

    bindings = getattr(authority, '_preservation_automation_bindings', None)
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    automation_binding = (
        isinstance(binding, dict)
        and getattr(binding.get('handle'), 'owner_class', None) in _AUTOMATION_OWNER_CLASSES
    )
    i10_pre_effect_failure = (
        i10_binding
        and isinstance(delivery_receipt, DeliveryReceipt)
        and delivery_receipt.admission_id == admission_id
        and delivery_receipt.attempted is True
        and delivery_receipt.delivered is False
        and delivery_receipt.reason == 'delivery_failed'
        and delivery_receipt.effect_state == 'none'
    )

    callback_requirement = None
    if i10_pre_effect_failure:
        # A concrete classic adopter/bridge remains an unresolved delivery obligation.  The
        # recovery watcher alone is the I10 retry fence and must not block its one safe resend;
        # the full callback classifier runs again after that resend and retains UNKNOWN then.
        from gateway.session_classic_delivery import _callback_classic_marker_signal
        marker_adopters = getattr(authority, 'marker_adopters', None)
        marker = marker_adopters.get(admission_id) if isinstance(marker_adopters, dict) else None
        if marker is not None and _callback_classic_marker_signal(authority, admission_id):
            return False
    elif not automation_binding:
        callback_requirement = classic_callback_requirement(
            authority, admission_id, delivery_receipt=delivery_receipt,
        )
        if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
            return False

    if not has_sidecar:
        if i10_binding and completion is None:
            from gateway.session_recovery_custody import finish_no_sidecar_delivery
            return await finish_no_sidecar_delivery(
                authority, admission_id, delivery_receipt, waiter_delivered,
                automation_binding=automation_binding,
                callback_requirement=callback_requirement,
            )
        if automation_binding:
            # A live waiter is an API completion, not a source transport receipt.  Automation roots
            # release only after the typed receipt reached this exact admission's transport path.
            if not authority._preservation_accept_automation_direct_delivery(
                    admission_id, delivery_receipt,
                    physical_delivery=physical_delivery, waiter_delivered=waiter_delivered):
                return False
            callback_requirement = classic_callback_requirement(authority, admission_id)
            if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
                return False
        if (callback_requirement != CLASSIC_CALLBACK_NOT_REQUIRED
                and not await wait_for_classic_post_delivery_callback(authority, admission_id)):
            return False
        if completion is not None:
            return stage_api_finalizer_delivery(
                authority, admission_id,
                delivery_receipt=delivery_receipt,
                waiter_delivered=waiter_delivered,
                finalizer_ok=True,
            )
        authority._preservation_release_terminal(admission_id)
        return True

    delivery_receipt, physical_delivery, finalizer_outcome = (
        await _finish_preservation_sidecar_delivery(
            authority, admission_id, delivery_receipt, physical_delivery,
            automation_binding, pre_effect_retry_attempted, waiter_delivered,
            transport_delivered,
        )
    )
    if automation_binding:
        if not physical_delivery or not authority._preservation_record_automation_terminal_witness(
                admission_id, delivery_receipt=delivery_receipt,
                waiter_delivered=waiter_delivered, finalizer_outcome=finalizer_outcome):
            return False
        callback_requirement = classic_callback_requirement(authority, admission_id)
        if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
            return False
    if pre_effect_retry_attempted and not physical_delivery:
        return False
    finalizer_ok = finalizer_outcome == 'complete'
    if completion is not None:
        return stage_api_finalizer_delivery(
            authority, admission_id,
            delivery_receipt=delivery_receipt,
            waiter_delivered=waiter_delivered,
            finalizer_ok=finalizer_ok,
            reason=finalizer_outcome,
        )
    authority._preservation_release_terminal(admission_id)
    return finalizer_ok


async def finish_preservation_delivery(authority, admission_id, delivery_receipt, waiter_delivered):
    """Stage API finalization before its waiter, retaining UNKNOWN on any finalizer error."""
    from gateway.platforms.api_server_run_completion import (
        api_i02_completion_required, completion_for, fail_api_finalizer_completion,
    )
    completion = completion_for(authority, admission_id)
    api_required = completion is not None or api_i02_completion_required(authority, admission_id)
    if api_required and completion is None:
        authority._preservation_mark_unknown(admission_id, 'finalizer_completion_registry_missing')
        return False
    try:
        result = await _finish_preservation_delivery_impl(
            authority, admission_id, delivery_receipt, waiter_delivered,
        )
    except BaseException:
        if completion is not None:
            fail_api_finalizer_completion(authority, admission_id, 'api_finalizer_error')
        raise
    if not result and completion is not None and completion.receipt is None:
        fail_api_finalizer_completion(authority, admission_id, 'api_finalizer_refused')
    return result


async def dispatch_shared_busy(adapter, event, session_key, *, durable=False, preservation=None):
    """Serial receive loops (IRC, Signal) await this before reading their next frame, so return
    once the follow-up is committed to the FIFO; a tracked observer delivers its one reply."""
    accepted = asyncio.get_running_loop().create_future()
    committed = asyncio.get_running_loop().create_future()
    authority, preservation_key = preservation or begin_busy_preservation(adapter, event, session_key)
    delivery_event = replace(event, source=replace(event.source))

    token, commit_token = busy_acceptance.set(accepted), busy_commit.set(committed)
    try:
        try:
            from gateway.session_ingress_busy import observe_shared_busy
            task = asyncio.create_task(observe_shared_busy(
                adapter, event, delivery_event, session_key, authority, preservation_key,
                accepted, finish_busy_preservation,
            ))
        except Exception:  # health: allow BLE001 -- observer creation failure releases only unaccepted custody
            finish_busy_preservation(authority, preservation_key, False)
            raise
    finally:
        busy_acceptance.reset(token)
        busy_commit.reset(commit_token)
    adapter._background_tasks.add(task)
    task.add_done_callback(adapter._background_tasks.discard)
    try:
        await asyncio.wait({accepted, task}, return_when=asyncio.FIRST_COMPLETED)
        if durable and not task.done():
            await asyncio.wait({committed, task}, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        if not accepted.done():
            task.cancel()  # cancelled before commit: the input was never ACKed
            finish_busy_preservation(authority, preservation_key, False)
        # After the acceptance future resolves, the observer owns the exact lease until its
        # delivery/marker cleanup. Cancelling this caller must not turn a still-running observer
        # into an unknown operation that can never reach its natural terminal callback.
        raise
    if task.done():
        task.result()  # refused, consumed or answered inline before any durable acceptance
    elif not committed.done():
        _hold_until_committed(adapter, committed, task)


def _hold_until_committed(adapter, committed, task):
    """Released before its commit: the reader reads on, but this input is not durable yet. A serial
    reader's next command waits for its commit or its handler's end (``burst_gate``), so /restart never
    closes admission, and /new never cancels the chat's rows, before it lands."""
    settled = asyncio.get_running_loop().create_future()

    def settle(_):
        if not settled.done():
            settled.set_result(None)
    committed.add_done_callback(settle)
    task.add_done_callback(settle)
    held = adapter.__dict__.setdefault('_uncommitted_inputs', set())
    held.add(settled)
    settled.add_done_callback(held.discard)
