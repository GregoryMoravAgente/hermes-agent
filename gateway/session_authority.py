"""Gateway-owned live sessions and canonical durable FIFO scheduling.

Only the runtime bootstrap holding profile ownership may initialize this service.
Transport attachment never constructs an agent or takes a turn lease.
"""
from __future__ import annotations

import asyncio
import contextlib
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from functools import partial
import sqlite3
from types import SimpleNamespace
import uuid

from gateway.session_contract import (
    CANONICAL_GATEWAY_PROTOCOL, AdmissionReceipt, PendingAdmission, Principal, SessionHandle, SessionRef, Submission,
    SubscriptionSnapshot,
)
from gateway.session_events import SessionEvents
from gateway.session_hosted_operations import HostedOperationsMixin
from gateway.session_authority_operations import PreservationOperationsMixin
from gateway.session_authority_admission import SessionAuthorityAdmissionMixin
from gateway.session_authority_preservation_local import SessionAuthorityPreservationLocalMixin
from gateway.session_automation_custody import AutomationCustodyMixin
from gateway.session_classic_custody import ClassicCustodyMixin
from gateway.session_pending_controls import PendingControls
from hermes_state_runtime import (
    STEERING, RuntimeStoreError, admit_session_input, begin_runtime_epoch,
    cancel_session_input, claim_session_input, find_session_admission, get_session_admission,
    list_session_admissions, recover_session_inputs, resolve_unknown_session_input,
)

# Capped backoff for a claim refused by transient storage (writer lock, full disk).
_CLAIM_RETRY_MIN_S, _CLAIM_RETRY_MAX_S = 0.25, 5.0
# Longest a settled turn's adapter may hold its adopted crash marker before the follower is claimed.
_MARKER_RELEASE_WAIT_S = 60.0
# True in the admitting task once ``admit_native``'s shielded ledger write began: a caller cancelled
# after that may still see the row commit, so it must not release the media the write commits.
native_write_started = ContextVar('native_write_started', default=False)
# Set by the messaging ingress around ``admit_native``: True, or the busy caller's acceptance future.
# Such an input is fenced by a later /stop or /new of its route (``fence_route``), and the future is
# released once the input queues behind an earlier one of its chat, so a serial reader moves on
# while acceptance itself still waits for the commit.
messaging_admission = ContextVar('messaging_admission', default=None)


@dataclass(frozen=True)
class SchedulerTaskHandoff:
    """A joinable scheduler task plus an optional trusted durable Teams job identity."""

    task: asyncio.Future
    expected_job_id: str | None = None

    def __await__(self):
        # Keep the historical scheduler fixture contract awaitable while the adapter consumes the
        # typed handoff before it installs the terminal callback.
        return self.task.__await__()


@dataclass
class LiveSession:
    source: object
    route: str
    task: asyncio.Task | None = None
    subscribers: dict = field(default_factory=dict)
    event_stream: SessionEvents = field(default_factory=SessionEvents)
    controls: PendingControls = field(init=False)
    # The messaging ingress tells the platform user once per pause episode (unknown head,
    # preflight refusal), not per message; the drain clears it when the FIFO moves again.
    pause_notified: bool = False
    mutation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Held by the FIFO claim and by compress across its summarization (which runs outside
    # ``mutation_lock``, so admissions are not parked behind an LLM call): no turn is claimed
    # under a compression that is still preparing its replacement transcript.
    claim_gate: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Set by ``_schedule`` while a drain is alive. The claim commits off-loop, so input admitted
    # after its transaction read the FIFO would otherwise be stranded by a drain that exits idle.
    rescan: bool = False
    # admission id -> its started claim, from the moment the settlement worker starts the terminal
    # write until the completion frame is published. The write runs outside ``event_stream.lock``
    # (a contended SQLite writer must not stall loop-side lock takers); readers holding that lock
    # still see the claim as started, so no snapshot shows a terminal row without its completion.
    settling: dict = field(default_factory=dict)
    # ``(revision key, payload)`` of the last ``session.context_breakdown``: Desktop re-polls it on
    # every idle mount and reconnect, and a rebuild walks the whole stored transcript (#140667).
    context_breakdown: tuple | None = None
    # A viewer attached while a guarded deferred pick awaits consent: the next boundary asks again.
    reask_pick: bool = False
    # (principal, effort) of a ``/reasoning`` command that arrived while a turn ran: applied in memory at
    # once, committed to the policy at the next boundary (gateway/session_setting_commands.py).
    pending_reasoning: tuple | None = None
    # A historic messaging row mounted for reading after its lane moved on (``/new``): ``route`` is
    # the lane's CURRENT key, which names another conversation, so nothing route-keyed (agent, Stop,
    # turns, eviction) may act through it (gateway/session_route_recovery.py). The lane publishing
    # this row again clears it.
    superseded: bool = False
    # Mounted by that cold path (not published by its lane): the lane publishing ANOTHER conversation
    # on the same route later supersedes it too.
    history_mount: bool = False
    # The background-review guard of a cold ``/refine`` (gateway/session_refine.py): its side agent is
    # per call, so the run token lives here; a second /refine joins it and the next turn cancels it.
    review: SimpleNamespace = field(default_factory=SimpleNamespace)
    # Revision of the last ``session.control.update`` (gateway/session_control_actions.publish): a tool
    # completion republishes only when a tool actually moved the goal/loop/heartbeat state.
    control_revision: str = ""

    def __post_init__(self):
        self.controls = PendingControls(self.event_stream)


def _log_drain_failure(task):
    """A dead pump is the one failure this module must never swallow."""
    if task.cancelled() or task.exception() is None:
        return
    import logging
    logging.getLogger(__name__).error('Session drain task died: %r', task.exception())



@contextlib.asynccontextmanager
async def keyed_turn(table, key, *, queued=None):
    """One FIFO turn per *key*: ``table[key]`` is ``[asyncio.Lock(), holder + waiters]``, created on
    first use and deleted when the last one leaves. asyncio's Lock is FIFO and a cancelled waiter gives
    up only its own place. *queued* runs (before waiting) when an earlier turn holds or awaits the key."""
    entry = table.setdefault(key, [asyncio.Lock(), 0])
    entry[1] += 1
    if queued is not None and entry[1] > 1:
        queued()
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        if not entry[1]:
            del table[key]


class SessionAuthority(
    SessionAuthorityPreservationLocalMixin,
    SessionAuthorityAdmissionMixin,
    ClassicCustodyMixin,
    AutomationCustodyMixin,
    HostedOperationsMixin,
    PreservationOperationsMixin,
):
    def __init__(self, runner, *, profile_id, instance_id, db, epoch):
        self.runner = runner
        self.profile_id = profile_id
        self.instance_id = instance_id
        self.db = db
        self.epoch = epoch
        self.sessions = {}
        self.waiters = {}
        self.events = {}
        self.native_waiters = set()
        self.pending_results = {}
        # Replies of recovered no-waiter turns, sent only once their terminal outcome commits.
        self.pending_deliveries = {}
        # Stops accepted for a running generation whose agent does not exist yet
        # (first-turn construction); consumed by adopt_agent, keyed session -> generation.
        self.pending_stops = {}
        # session -> (generation, reason): a gateway restart/shutdown Stop latched before that
        # generation's managed worker existed; its pre-bootstrap result carries the cause (GW-22).
        self.pending_stop_causes = {}
        # session -> (generation, [steer text]) accepted before that generation adopted an agent;
        # adopt_agent hands it to that agent (never a later generation's), the drain drops the rest.
        self.pending_steers = {}
        # session -> (generation, {admission_id: delivered}): queued admissions prompt.steer_queued handed
        # to that running claim (gateway/session_queued_controls.py); its settlement decides each one.
        self.steer_transfers = {}
        self.launch_sources = {}  # session -> its frozen launch source (session_queued_controls)
        # session -> generation of the last Stop delivered to a live agent. A repeated
        # Stop can land on the session's reusable cached agent after that turn's finalizer
        # cleared it; the next generation's adopt_agent drops it instead of starting interrupted.
        self.delivered_stops = {}
        # session -> (generation, agent) its turn adopted. Until then the resident agent is the
        # previous turn's cached one, which pre-turn compression or a changed agent config
        # replaces, so a Stop that only reached it would let the rebuilt agent run the turn. After
        # it, a Stop goes to this agent, never through a cache that may have evicted it.
        self.adopted = {}
        # Queued admissions whose cancellation may have committed before its observers settled.
        self.cancel_obligations = set()
        # Set by profile retirement (unserve): the drain claims no successor after its running turn.
        self.retiring = False
        self.retired = False
        self._classic_evict_at = {}  # route -> last leaked/reaped-slot eviction check (monotonic)
        self.executing = {}  # session id -> generation whose execution is in progress (publish fence)
        # session id -> end_reason of a last-viewer close still waiting for an idle FIFO
        # (gateway/session_viewer_close); a viewer attaching again cancels it.
        self.pending_closes = {}
        # Logical owner rotated away by the exhaustion auto-reset -> its session key: an admission
        # that reserved it before the rotation commits on that key's current route instead.
        self.rotated_owners = {}
        # Route key -> [lock, holder + waiters] of the native admissions in flight (``_route_turn``, ``keyed_turn``).
        self._route_turns = {}
        # Per route: how many messaging /stop and /new fenced it (``fence_route``). Kept for the
        # owner's life (one int per chat that ever got one): an input or command turn holding an older
        # value never commits, and a reset entry could read as an old epoch again.
        self._route_epochs = {}
        # route -> the compression-exhaustion auto-reset its LOCAL turn deferred: the store refuses a
        # reset while that turn's admission is started, so the drain runs it once the turn settles.
        self.deferred_resets = {}
        # Preservation handles are bound only after the corresponding durable admission/claim
        # transaction returns.  A failed read leaves the exact lease retained as unknown.
        self._preservation_handles = {}
        self._preservation_children = {}
        # A cached sidecar has a process-owned root and exact per-admission child.  Keep the
        # current FIFO claim attached until that child has durable result and delivery proof;
        # this is an index of coordinator handles, never a second ownership ledger.
        self._preservation_sidecars = {}
        # One bounded retry may resume a retained delivery/finalizer boundary. This is an index of
        # the exact admission, never a second custody ledger or a new admission path.
        self._preservation_delivery_retries = {}
        # Pending roots are object-bound records, never a request-id bucket. Their full producer
        # identity remains attached to the exact handle until the durable callback binds or releases
        # that handle; a Graph/native/API collision can therefore never steal another root.
        self._preservation_pending_roots = {}
        self._preservation_scheduler_handles = {}
        # A retryable Teams result remains attached to its original I01 lease until the next
        # durable owner is bound.  The mapping is keyed by the durable job id, never by a
        # provider request id that another producer could reuse.
        self._preservation_scheduler_retry_handles = {}
        self._preservation_claiming = {}
        self._preservation_marker_watchers = {}
        # I07 automation roots and I06 delivery children are indexes of the same coordinator
        # leases. They key only after the canonical row is known; a delivery id or mailbox path
        # alone can never recreate custody.
        self._preservation_automation_roots = {}
        self._preservation_automation_children = {}
        self._preservation_automation_bindings = {}
        # A queued route wake can be rehomed by /new. Keep the same authenticated I07 root
        # across the old-row cancellation and the new-row commit; a request-id copy alone may
        # never create a successor lease.
        self._preservation_automation_rehomes = {}
        self._route_wake_inbound_seen = False
        self._busy_inbound_seen = False
        # Exact control, busy-delivery, and mutation leases. These are indexes of real
        # RuntimeCustodyCoordinator handles; they are not a second ownership authority.
        self._preservation_operations = {}
        # Source-owned worker and mailbox indexes are created during bootstrap so an explicitly
        # empty graph means "no producer registered".  Readiness refuses a missing or lazily
        # recreated container instead of treating an absent attribute as quiescence.
        self._local_adoptions = {}
        self._policy_refreshes = {}
        self._pending_adoptions = set()
        self._mutation_tasks = set()
        self._bot_mailbox_operations = set()
        self._bot_receipt_tasks = set()
        self._bot_receipt_refreshes = {}
        self._bot_receipt_index = {}
        self._bot_receipt_dirty = set()
        self._bot_reply_watchers = {}
        # Exact external-process parents registered before a provider spawn.  This is an index
        # on the serving authority, not a second ledger; retirement consumes these object-bound
        # finalizers before releasing the profile.
        self._external_process_finalizers = {}
        # One profile-scoped factory capability; accepted source parents remain
        # in the finalizer index above, never in a second ownership ledger.
        self._mcp_source_factory = None
        # Profile browser capability; accepted parents stay in the existing finalizer index.
        self._browser_source_factory = None
        # Profile CUA capability; accepted parents stay in the existing finalizer index.
        self._cua_source_factory = None
        # Source-backed evidence that this authority has actually consumed the native inbound
        # entrypoint.  An empty native registry is not evidence that the connector source is wired.
        self._native_inbound_seen = False
        self._register_preservation_producer_graph()
    def register_mcp_source_factory(self, factory):
        """Bind one profile factory before MCP discovery ingress."""
        current = self._mcp_source_factory
        if current is not None and current is not factory:
            raise RuntimeStoreError('mcp_factory_registry_collision')
        if getattr(factory, 'authority', None) is not self:
            raise RuntimeStoreError('mcp_factory_registry_mismatch')
        self._mcp_source_factory = factory
        return factory
    def mcp_source_factory(self):
        """Return the exact profile factory, if bootstrap installed one."""
        return self._mcp_source_factory
    def register_browser_source_factory(self, factory):
        """Bind one profile browser factory before browser ingress."""
        current = self._browser_source_factory
        if current is not None and current is not factory:
            raise RuntimeStoreError('browser_factory_registry_collision')
        if getattr(factory, 'authority', None) is not self:
            raise RuntimeStoreError('browser_factory_registry_mismatch')
        self._browser_source_factory = factory
        return factory

    def browser_source_factory(self):
        """Return the exact profile browser factory, if bootstrap installed one."""
        return self._browser_source_factory

    def register_cua_source_factory(self, factory):
        """Bind one profile CUA factory before public computer-use ingress."""
        from gateway.runtime_service_operations import register_cua_source_factory
        return register_cua_source_factory(self, factory)

    def cua_source_factory(self):
        """Return the exact profile CUA factory, if bootstrap installed one."""
        from gateway.runtime_service_operations import cua_source_factory
        return cua_source_factory(self)

    def register_external_process_finalizer(self, operation_id, parent):
        """Register the exact external parent that profile retirement must consume."""
        if self.retiring or self.retired:
            raise RuntimeStoreError('profile_retiring')
        if type(operation_id) is not str or not operation_id or parent is None:
            raise RuntimeStoreError('finalizer_registry_invalid')
        if (getattr(parent, 'profile_authority', None) is not self
                or getattr(parent, 'profile_id', None) != self.profile_id
                or getattr(parent, 'process_instance', None) != self.instance_id
                or getattr(parent, 'authority_epoch', None) != self.epoch):
            raise RuntimeStoreError('finalizer_registry_mismatch')
        current = self._external_process_finalizers.get(operation_id)
        if current is not None and current is not parent:
            raise RuntimeStoreError('finalizer_registry_collision')
        self._external_process_finalizers[operation_id] = parent
        return parent
    def external_process_finalizers(self):
        """Return the exact registered parent objects for the retirement owner."""
        return tuple(self._external_process_finalizers.values())
    def forget_external_process_finalizer(self, operation_id, parent):
        current = self._external_process_finalizers.get(operation_id)
        if current is parent and getattr(parent, 'state', None) == 'terminal':
            self._external_process_finalizers.pop(operation_id, None)
            return True
        return False
    def authorize(self, actor, ref, capability):
        """Every handler calls this first, so a later ``self.sessions[ref.session_id]`` is
        safe: a deleted/evicted live entry surfaces here as ``not_found``, not as a KeyError
        deeper in the handler."""
        from gateway.session_authorities import require_serving
        require_serving(self)
        if actor.profile_id != self.profile_id or ref.profile_id != self.profile_id:
            raise RuntimeStoreError('profile_mismatch')
        if capability not in actor.capabilities:
            raise RuntimeStoreError('permission_denied')
        if ref.session_id not in self.sessions:
            row = self.db.get_session(ref.session_id)
            if row is None:
                raise RuntimeStoreError('not_found')
            from hermes_state_local import POLICY_PREFIX
            with self.db._read_ctx() as conn:
                from hermes_state_local_migration import LEGACY_PREFIX
                local = conn.execute('SELECT 1 FROM state_meta WHERE key IN (?,?)',
                    (POLICY_PREFIX + ref.session_id, LEGACY_PREFIX + ref.session_id)).fetchone()
            if local or str(row.get('chat_id') or '').startswith('local-'):
                from gateway.session_local_recovery import restore_local_session
                restore_local_session(self, ref.session_id)
            else:
                from gateway.session_route_recovery import restore_routed_session
                if restore_routed_session(self, ref.session_id) is None:
                    from gateway.session_api import restore_api_session
                    restore_api_session(self, ref.session_id)
        from gateway.config import Platform
        if self.sessions[ref.session_id].superseded and capability != 'session:read':
            # One gateway owns every session and never crosses them: the lane's route now belongs to
            # its newer conversation, so a write here would land there. Named, so a viewer can say so.
            raise RuntimeStoreError('session_superseded', field='session_id',
                                    detail='this conversation\'s chat has moved on to a newer session; it is read-only')
        source = self.sessions[ref.session_id].source
        if (source is not None and source.platform == Platform.LOCAL
                and source.user_id != actor.subject and 'session:operator' not in actor.capabilities):
            raise RuntimeStoreError('permission_denied')

    def _require_admission_open(self, *, queue_only=False):
        """``queue_only``: the caller only COMMITS a queued row (never claims or runs it). A restart's
        after-turn wait accepts those (#139041): the row is durable FIFO input the next owner runs
        exactly once, while this owner's claim path stays refused.

        The reversible external-drain flag is intentionally absent here.  Existing accepted FIFO
        rows must continue through claim/settlement while fresh roots use the stricter gate below.
        """
        if self.retiring:
            raise RuntimeStoreError('runtime_draining')
        if self.runner._draining:
            from gateway.drain_control import restart_wait_open
            if not (queue_only and restart_wait_open(self.runner)):
                raise RuntimeStoreError('runtime_draining')

    def _require_fresh_admission_open(self, *, queue_only=False):
        self._require_admission_open(queue_only=queue_only)
        if (getattr(self.runner, '_external_drain_active', False)
                or getattr(self.runner, '_preservation_drain_active', False)):
            raise RuntimeStoreError('runtime_draining')

    def _admission_gate(self, authorize=None, *, queue_only=False, fresh=False):
        """Re-check the exact root/claim gate inside an off-loop transaction."""
        def guard(conn):
            if fresh:
                self._require_fresh_admission_open(queue_only=queue_only)
            else:
                self._require_admission_open(queue_only=queue_only)
            if authorize is not None:
                authorize(conn)
        return guard

    def logical_owner(self, session_id):
        """The FIFO/admission identity of a route: the root of its compression lineage.
        Compression advances the physical transcript, never the admission identity. A session this
        store does not hold (another served profile's, under multiplex) keeps its own id."""
        if not session_id:
            return session_id
        lineage = self.db.get_compression_lineage(session_id)
        return lineage[0] if lineage else session_id

    def physical_target(self, ref):
        return self.db.get_compression_tip(ref.session_id) or ref.session_id

    def register(self, source):
        return self._publish_route(source, *self._reserve_route(source))

    def _reserve_route(self, source, *, fresh=False):
        """The durable half of ``register`` (routing entry + reserved row): thread-safe, so async
        callers run it off the owner loop and publish the live entry on the loop."""
        if fresh:
            self._require_fresh_admission_open()
        else:
            self._require_admission_open()
        entry = self.runner.session_store.get_or_create_session(source)
        sid = self.logical_owner(entry.session_id)
        # SessionStore reserves routing metadata before the first AIAgent exists.
        if self.db.get_session(sid) is None:
            self.db.create_session(sid, source=source.platform.value)
        return sid, entry.session_key

    def _publish_route(self, source, sid, route, *, fresh=False):
        if fresh:
            self._require_fresh_admission_open()
        else:
            self._require_admission_open()
        live = self.sessions.get(sid)
        if live is None:
            live = self.sessions[sid] = LiveSession(source, route)
            # The lane moved on to *sid*: a cold history mount still on its route is now another
            # conversation's, so it goes read-only (gateway/session_route_recovery.py).
            for other in self.sessions.values():
                if other.history_mount and other.route == route and other is not live:
                    other.superseded = True
        # The lane itself names *sid* (again): a historic mount becomes its live session.
        live.superseded = live.history_mount = False
        return SessionRef(self.profile_id, sid)

    def agent(self, ref):
        live = self.sessions[ref.session_id]
        if live.superseded:
            return None  # the route's agent is the newer conversation's
        lookup = getattr(self.runner, '_resident_agent_for', None) or self.runner._cached_agent_for
        return lookup(live.route)

    def _pending_rows(self, ref):
        """Pending admissions as the event stream presents them: a settlement committed but not yet
        published (``LiveSession.settling``) still reads as its started claim. Callers that pair the
        result with stream state hold ``event_stream.lock``, under which ``settling`` changes."""
        rows = list_session_admissions(self.db, session_id=ref.session_id)
        live = self.sessions.get(ref.session_id)
        masked = dict(live.settling) if live is not None else {}
        if not masked:
            return rows
        rows = {row['admission_id']: row for row in rows}
        rows.update(masked)
        return sorted(rows.values(), key=lambda row: row['seq'])

    def _handle(self, ref):
        row = self.db.get_session(ref.session_id)
        if row is None:  # deleted after ``authorize``: the commit lands before the retirement runs
            raise RuntimeStoreError('not_found')
        pending = self._pending_rows(ref)
        state = 'unknown' if any(r['status'] == 'unknown' for r in pending) else (
            'running' if any(r['status'] == 'started' for r in pending) else 'idle')
        return SessionHandle(ref, self.instance_id, self.epoch, row['runtime_revision'],
                             row['runtime_generation'], state)

    async def resolve(self, actor, ref):
        self.authorize(actor, ref, 'session:read')
        return self._handle(ref)

    def unless_deleted(self, ref, read):
        """``read()``; a refusal because *ref* was deleted after ``authorize`` passed is ``not_found``.
        A delete commits off the loop, so its rows can be gone while the live entry still waits for its
        retirement, and readers meet the missing receipt instead."""
        try:
            return read()
        except RuntimeStoreError:
            if self.db.get_session(ref.session_id) is None:
                raise RuntimeStoreError('not_found') from None
            raise

    async def attach(self, actor, ref):
        self.authorize(actor, ref, 'session:read')
        from gateway.session_local_recovery import local_history, prepare_history
        prepared = await prepare_history(self, ref)  # the lineage read stays off the loop and the lock
        from gateway.session_onboarding import keep_from_discard
        # The last await before the viewer joins: a fresh launch's claim is taken back with no yield
        # before the join, and a delete already past it answers not_found below.
        await keep_from_discard(self, ref.session_id)
        # A delete or eviction can land during those awaits: re-check, so it answers not_found, never a KeyError.
        self.authorize(actor, ref, 'session:read')
        live = self.sessions[ref.session_id]
        from gateway.session_queued_controls import pending_rows
        with live.event_stream.lock:
            handle = self._handle(ref)  # before the viewer joins: a delete committed meanwhile is not_found
            subscription = next((key for key, member in live.subscribers.items()
                                 if member == actor), None) or uuid.uuid4().hex
            live.subscribers[subscription] = actor
            self.pending_closes.pop(ref.session_id, None)
            from tools.async_delegation import allow_for_session
            allow_for_session((self.profile_id, ref.session_id))  # a closed session's work may start again
            transport = self.events.get(actor.transport_id)
            if transport is not None:
                live.event_stream.fanout.attach(transport)
                live.event_stream.on_overflow = partial(self._retire_overflowed, ref.session_id)
            active_generation = handle.execution_generation if handle.execution_state == "running" else None
            prompts = live.controls.snapshot(ref.session_id, active_generation)
            epoch, sequence = live.event_stream.watermark()
            history = self.unless_deleted(ref, lambda: tuple(local_history(self, ref, prepared)))
            snapshot = SubscriptionSnapshot(subscription, handle, epoch, sequence, history,
                                        tuple(self._pending_receipt(r) for r in pending_rows(self, ref)), prompts)
        from gateway.session_viewer_close import cancel_persisted_close
        await asyncio.to_thread(cancel_persisted_close, self, ref.session_id)  # the viewer cancels it
        from gateway.session_deferred_pick import reask_on_attach
        await reask_on_attach(self, ref, live)
        return snapshot

    def _retire_overflowed(self, session_id, transport):
        """The fanout dropped this peer's backlog: its subscription is over even though the
        socket still answers RPCs. A later resume re-attaches it with a fresh snapshot."""
        live = self.sessions[session_id]
        for subscription, member in list(live.subscribers.items()):
            if self.events.get(member.transport_id) is transport:
                del live.subscribers[subscription]

    async def detach(self, actor, subscription_id, *, end_reason=None):
        for session_id, live in self.sessions.items():
            if subscription_id in live.subscribers:
                if live.subscribers[subscription_id] != actor:
                    raise RuntimeStoreError('permission_denied')
                del live.subscribers[subscription_id]
                transport = self.events.get(actor.transport_id)
                if transport is not None:
                    live.event_stream.fanout.detach(transport)
                if not live.subscribers:
                    from gateway.session_local_mcp import release_editor_mcp
                    await asyncio.to_thread(release_editor_mcp, self, [session_id])
                    if end_reason:
                        from gateway.session_viewer_close import close_viewer_session
                        await close_viewer_session(self, session_id, end_reason)
                        return
                    from gateway.session_acp_lifecycle import end_idle_acp_session, reopen_if_reattached
                    from gateway.session_runtime_workers import tracked_write
                    # A viewer that attached while the close was off the loop reopens it.
                    await tracked_write(self, partial(end_idle_acp_session, self, session_id),
                                        then=partial(reopen_if_reattached, self, session_id))
                return
        raise RuntimeStoreError('not_found')

    def _receipt(self, row):
        status, outcome = row['status'], row['outcome']
        if outcome == STEERING:  # handed to the running claim: it reads as that execution until settled
            status, outcome = 'started', None
        return AdmissionReceipt(row['admission_id'], SessionRef(self.profile_id, row['target_session_id']),
                                row['seq'], status, outcome, row['owner_epoch'] or self.epoch, row['generation'])

    def _pending_receipt(self, row):
        # Only public input text crosses the viewer boundary, never the
        # private native envelope's routing or authorization provenance.
        payload = row['payload']
        text = payload.get('text')
        if text is None:
            text = payload.get('native_text_v1', {}).get('event', {}).get('text', '')
        from gateway.session_queued_controls import queued_controls
        return PendingAdmission(**vars(self._receipt(row)), input_id=row['request_id'], text=text,
                                controls=queued_controls(self, row), steering=row['outcome'] == STEERING)

    def _publish_pending(self, ref):
        live = self.sessions[ref.session_id]
        from gateway.session_queued_controls import pending_rows
        with live.event_stream.lock:
            handle = self._handle(ref)
            pending = [asdict(self._pending_receipt(row)) for row in pending_rows(self, ref)]
            # Turns bump runtime_revision without any session.updated event, so
            # this is the only place a viewer learns the CAS revision a later
            # prepared mutation must present.
            live.event_stream.publish(ref.session_id, {
                'stored_session_id': ref.session_id, 'pending': pending, 'authority_epoch': self.epoch,
                'desktop_protocol': CANONICAL_GATEWAY_PROTOCOL,
                'running': handle.execution_state == 'running',
                'execution_generation': handle.execution_generation,
                'revision': handle.revision,
            }, event_type='session.info')
        from gateway.session_bot_mailbox import wake_bot_receipts
        wake_bot_receipts(self, ref.session_id)

    def _schedule(self, ref):
        live = self.sessions[ref.session_id]
        live.rescan = True
        if live.task is None or live.task.done():
            live.task = asyncio.create_task(self._drain(ref))
            live.task.add_done_callback(_log_drain_failure)
            from gateway.session_runtime_workers import persist_work_count
            live.task.add_done_callback(lambda task: persist_work_count(self, task))

    def wake_after_worker(self, session_id):
        """A worker execution on ``session_id`` (a logical owner or its compression continuation)
        ended: a drain that parked behind it re-runs. A no-op while a drain is running."""
        for sid in dict.fromkeys((session_id, self.logical_owner(session_id))):
            if sid in self.sessions:
                self._schedule(SessionRef(self.profile_id, sid))

    def _pause(self, ref, reason):
        """The FIFO stopped without claiming its head. Committed rows stay queued for a later
        drain; process-local delivery/HTTP/producer waiters on this session are released,
        with the reason instead of a reply, so an adapter loop is never parked on a turn that
        will not run. The ingress turns that refusal into one user-facing notice per episode.
        ``session_busy`` is transient (a registered worker finishes and wakes the FIFO), so only
        the messaging delivery waiters are released for it; an API/webhook/Bot observer keeps
        waiting for the answer instead of reporting accepted work as a conflict."""
        for row in list_session_admissions(self.db, session_id=ref.session_id):
            admission_id = row['admission_id']
            if reason == 'session_busy' and admission_id not in self.native_waiters:
                continue
            self.native_waiters.discard(admission_id)
            waiter = self.waiters.pop(admission_id, None)
            if waiter is not None and not waiter.done():
                waiter.set_exception(RuntimeStoreError(reason))

    async def admit_automation(self, adapter, event, identity, replaces=None, *,
                               _preservation_handle=None, _preservation_child_key=None,
                               _on_committed=None):
        from gateway.session_automation import admit_automation
        return await admit_automation(
            self, adapter, event, identity, replaces,
            _preservation_handle=_preservation_handle,
            _preservation_child_key=_preservation_child_key,
            _on_committed=_on_committed,
        )

    async def admit_native(self, event, *, _preservation_handle=None):
        """Await current connector policy, then commit before ACK or scheduling execution."""
        from gateway.session_ingress_media import abandon_capture, capture_lease
        reservation_id = str(getattr(event, 'message_id', None) or uuid.uuid4().hex)
        preservation_handle = _preservation_handle
        if preservation_handle is None:
            # Reserve the producer's I01 root before waiting for this route's FIFO turn.  The
            # route lock orders durable admissions, but it must not delay custody reservation for
            # a duplicate that is already waiting behind an in-flight writer.
            preservation_handle = self._preservation_root(
                'I01_native_inbound', f'native:{reservation_id}')
        route_entered = False
        try:
            async with self._route_turn(event) as fence:
                route_entered = True
                try:
                    self._require_fresh_admission_open()
                except BaseException:
                    # The producer reserved its I01 root before preparation; release an
                    # unaccepted root when the fresh gate closes before the durable writer.
                    if preservation_handle is not None:
                        self._preservation_bind_or_release(None, preservation_handle)
                    raise
                with capture_lease(self) as scope:
                    try:
                        return await self._admit_native(
                            event, _reservation_id=reservation_id,
                            _preservation_handle=preservation_handle, fence=fence)
                    except BaseException:
                        if not scope.write_started:
                            await abandon_capture(scope)
                        raise
        except BaseException:
            if not route_entered and preservation_handle is not None:
                self._preservation_bind_or_release(None, preservation_handle)
            raise

    @contextlib.asynccontextmanager
    async def _route_turn(self, event):
        """One route's inputs commit in the order they entered ``admit_native``: a photo's off-loop
        capture must not let the chat's next text take the earlier FIFO position. asyncio's Lock is
        FIFO and a cancelled waiter gives up only its own place, so a later input never overtakes an
        earlier one still in flight. Other routes never wait; an unroutable event (refused by
        ``prepare_native``) is not held. Yields the messaging input's fence check (else None)."""
        ingress = messaging_admission.get()
        messaging_admission.set(None)  # not inherited by the drain this admission schedules
        try:
            key = self.runner.session_store._generate_session_key(event.source)
        except AttributeError:  # no source, or a duck-typed one without routing fields
            key = None
        if key is None:
            yield None
            return
        arrived = self.route_epoch(key)

        def fence(_conn=None):
            if self.route_epoch(key) != arrived:
                raise RuntimeStoreError('stale_generation', field='route',
                                        detail='a /stop or /new of this chat came after this message')

        def queued():  # its FIFO position is fixed behind the earlier input
            if isinstance(ingress, asyncio.Future) and not ingress.done():
                ingress.set_result(None)
        async with keyed_turn(self._route_turns, key, queued=queued):
            if ingress is not None:
                fence()
            yield fence if ingress is not None else None

    def route_epoch(self, route):
        """How many messaging /stop and /new fenced *route* (``fence_route``)."""
        return self._route_epochs.get(route, 0)

    def fence_route(self, route):
        """/stop and /new: the route's messaging input still capturing or waiting, and a slash command's
        turn read before it (``_stop_epoch``), never commit."""
        self._route_epochs[route] = self.route_epoch(route) + 1

    async def _admit_native(self, event, *, _reservation_id=None, _preservation_handle=None,
                            fence=None):
        import json
        from gateway.session_envelope import prepare_native, restore_native_source
        if _preservation_handle is None:
            preservation_handle = self._preservation_root(
                'I01_native_inbound', f'native:{_reservation_id or uuid.uuid4().hex}')
        else:
            coordinator = self._preservation_coordinator()
            owned_lease = None if coordinator is None else coordinator._parent(_preservation_handle)
            if (getattr(_preservation_handle, 'owner_class', None) != 'I01_native_inbound'
                    or getattr(_preservation_handle, 'profile_id', None) != self.profile_id
                    or getattr(_preservation_handle, 'authority_epoch', None) != self.epoch
                    or owned_lease is None):
                self._preservation_bind_or_release(None, _preservation_handle)
                raise RuntimeStoreError('preservation_owner_unmapped')
            preservation_handle = _preservation_handle
        request_id = None
        principal_id = None
        session_id = None
        route_write_pending = False
        self._require_fresh_admission_open()
        try:
            payload = await prepare_native(self.runner, event)
            # Native Gateway admissions are a server-owned managed producer when the runtime
            # custody seam is installed. Keep the legacy route functional for lightweight
            # embedders that do not install that seam; they must not be mislabeled as managed
            # and then fail at execution for a binding that cannot exist in that runtime.
            producer_process_mode = None
            if self._preservation_coordinator() is not None:
                from gateway.session_classic_custody import classic_direct_route_marker
                producer_process_mode = (
                    classic_direct_route_marker(self, event) or 'managed_turn'
                )
            ingress_fingerprint = getattr(event, '_ingress_sha256', None)
            self._require_fresh_admission_open()
            if fence is not None:
                fence()
            source = restore_native_source(payload)  # no re-hash of the just-captured media on the loop
            from gateway.session_runtime_workers import tracked_write
            route_write_pending = True

            def route_finished(_result):
                # A cancelled caller is allowed to leave this worker running, but the producer root
                # is released only after the route worker has returned. No admission row exists yet.
                self._preservation_bind_or_release(None, preservation_handle)

            try:
                route_result = await tracked_write(
                    self, partial(self._reserve_route, source, fresh=True),
                    on_cancel=route_finished, on_failure=lambda _error: route_finished(None))
            except asyncio.CancelledError:
                # ``tracked_write`` owns the shielded worker and invokes route_finished at completion.
                raise
            except BaseException:
                route_write_pending = False
                self._preservation_bind_or_release(None, preservation_handle)
                raise
            route_write_pending = False
            ref = self._publish_route(source, *route_result, fresh=True)
            identity = json.dumps([source.profile, source.platform.value, source.chat_id,
                                   source.thread_id, source.user_id], separators=(',', ':'))
            event_row = payload['native_text_v1']['event']
            message_id = str(event_row['message_id'] or uuid.uuid4().hex)
            revision = event_row.get('edit_revision')
            if revision is not None and not isinstance(revision, str):
                raise RuntimeStoreError('invalid_params')
            # Edits keep the provider's message id but are separate durable inputs. Preserve the
            # legacy id for ordinary sends; the framed edit key makes redelivery exact per revision.
            request_id = (json.dumps(['native_edit', message_id, revision], separators=(',', ':'))
                          if revision is not None else message_id)
            session_id = ref.session_id
            principal_id = 'messaging:' + identity
            self._preservation_pending_root(
                request_id, preservation_handle, principal_id=principal_id, session_id=session_id)
            committed = [ref]  # the row's target: the write may retarget a rotated-away reservation
            # A /retry re-send carries its transcript rewrite: it commits in this admission's own write.
            from gateway import session_retry_admission
            rewrite = session_retry_admission.take(event, self.db)

            def admitted(row):
                self._preservation_bind_or_release(row, preservation_handle)
                committed[0] = SessionRef(self.profile_id, row.get('target_session_id') or ref.session_id)
                if rewrite is not None and rewrite.applied:
                    rewrite.on_commit()
                return self._admitted(committed[0], event, row)
            from gateway.session_ingress_media import _lease_scope
            if (scope := _lease_scope.get()) is not None:
                scope.write_started = True
            native_write_started.set(True)  # shielded from here: a cancelled caller may still commit
            try:
                return await tracked_write(self, partial(
                    self._admit_native_write, principal_id=principal_id, session_id=session_id,
                    request_id=request_id, payload=payload,
                    producer_process_mode=producer_process_mode,
                    ingress_fingerprint=ingress_fingerprint,
                    authorize=session_retry_admission.compose(rewrite, self._admission_gate(fence, fresh=True)),
                    raw=getattr(event, 'raw_message', None)),
                    then=admitted, ordered=True, after=lambda _receipt: self._schedule_admitted(committed[0]),
                    on_failure=partial(
                        self._preservation_reconcile_failed, preservation_handle,
                        principal_id=principal_id, session_id=session_id, request_id=request_id))
            except asyncio.CancelledError:
                # The tracked worker's late callback reconciles the exact row after the shielded
                # transaction completes. Releasing here would race a commit that is still unseen.
                raise
            except BaseException:
                self._preservation_reconcile_failed(
                    preservation_handle, principal_id=principal_id, session_id=session_id,
                    request_id=request_id)
                raise
        except BaseException:
            if preservation_handle is not None and request_id is None and not route_write_pending:
                coordinator = self._preservation_coordinator()
                if coordinator is not None:
                    result = coordinator.release_unaccepted(preservation_handle)
                    if result is not None and result.code != 'stale_lease':
                        coordinator.mark_unknown(preservation_handle, 'read_failed')
            raise

    def _admit_native_write(
        self, *, principal_id, session_id, request_id, payload,
        producer_process_mode=None, ingress_fingerprint=None, authorize, raw=None,
    ):
        # Off the loop and in admission order: the redelivery reconcile reads the ledger it decides
        # against, so a concurrent first delivery of the same message cannot slip between them.
        from gateway.session_ingress_media import drop_capture_lease, reconcile_native_retry, release_unheld_media
        fresh = payload.get('native_text_v1', {}).get('media', [])
        session_id = self._current_owner(session_id)
        try:
            existing = find_session_admission(
                self.db, principal_id=principal_id, session_id=session_id, request_id=request_id,
            )
            retained_payload = (existing or {}).get('payload')
            retained_native = retained_payload.get('native_text_v1') if isinstance(retained_payload, dict) else None
            legacy_identity = isinstance(retained_native, dict) and 'producer_process_mode' not in retained_native
            if not legacy_identity:
                descriptor = payload.get('native_text_v1')
                if not isinstance(descriptor, dict):
                    raise RuntimeStoreError('invalid_params')
                payload = {
                    **payload,
                    'native_text_v1': {**descriptor},
                }
                if producer_process_mode is not None:
                    payload['native_text_v1']['producer_process_mode'] = producer_process_mode
                if ingress_fingerprint:
                    # The fingerprint is the delivery before any rewrite; keep it alongside
                    # the custody marker on fresh rows and exact retries with that marker.
                    payload['native_text_v1']['ingress_sha256'] = ingress_fingerprint
            # A pre-custody row keeps its original payload shape. The ordinary retry
            # reconciliation and admission digest still decide exact replay or conflict;
            # this identity lookup cannot authorize changed content.
            payload = reconcile_native_retry(self.db, principal_id=principal_id, session_id=session_id,
                                             request_id=request_id, payload=payload, raw=raw)
            return admit_session_input(self.db, epoch=self.epoch, principal_id=principal_id, session_id=session_id,
                                       request_id=request_id, payload=payload, _authorize_write=authorize)
        except Exception:
            # A refused admission owns nothing it captured (unless another admission holds the same bytes).
            drop_capture_lease()
            release_unheld_media(self.db, fresh, retain_history=False)
            raise

    def _current_owner(self, session_id):
        """``session_id`` unless the exhaustion auto-reset rotated it away after this admission
        reserved it: then the live fresh session its route now names (the moved rows' home)."""
        route = self.rotated_owners.get(session_id)
        if route is None:
            return session_id
        entry = self.runner.session_store.lookup_by_session_key(route)
        current = self.logical_owner(entry.session_id) if entry is not None else session_id
        return current if current in self.sessions else session_id

    async def recover_native_sessions(self, bindings):
        """Bind only server-observed native routes; unknown work stays paused."""
        if self._preservation_coordinator() is not None:
            from gateway.session_recovery_custody import recover_native_sessions
            return await recover_native_sessions(self, bindings)
        from collections import Counter
        from gateway.session_envelope import check_native_route
        bindings = list(bindings)
        counts = Counter(sid for sid, _, _ in bindings)
        results = {}
        for sid, available_source, adapter in bindings:
            try:
                if counts[sid] != 1:
                    raise RuntimeStoreError('admission_conflict')
                rows = list_session_admissions(self.db, session_id=sid, pending_only=False)
                def recognized_automation(row):
                    payload = row.get('payload') or {}
                    principal = row.get('principal_id')
                    return (
                        row.get('status') == 'queued'
                        and isinstance(principal, str) and principal.startswith('automation:')
                        and ('local_automation_v1' in payload or 'native_text_v1' in payload)
                    )

                native = [row for row in rows if 'native_text_v1' in row['payload']]
                local_automation = [
                    row for row in rows if 'local_automation_v1' in row['payload']
                ]
                if not native and not local_automation:
                    raise RuntimeStoreError('not_found')
                target = self.physical_target(SessionRef(self.profile_id, sid)) if native else None
                last_native = native[-1] if native else local_automation[-1]
                if recognized_automation(last_native):
                    recovered = self._preservation_recover_automation(last_native)
                    if recovered is None:
                        raise RuntimeStoreError('preservation_owner_unmapped')
                if native:
                    source, route = await check_native_route(
                        self.runner, last_native['payload'], target, available_source, adapter)
                else:
                    from gateway.session_recovery_custody import _local_automation_route
                    source, route = _local_automation_route(self, sid, available_source)
                for row in rows:
                    if row['status'] != 'queued':
                        continue
                    payload = row['payload']
                    if recognized_automation(row):
                        recovered = self._preservation_recover_automation(row)
                        if recovered is None:
                            raise RuntimeStoreError('preservation_owner_unmapped')
                    if 'native_text_v1' in payload:
                        await check_native_route(self.runner, row['payload'], target, available_source, adapter)
                    elif 'local_automation_v1' in payload:
                        from gateway.session_automation import check_local_automation
                        check_local_automation(
                            self, SessionRef(self.profile_id, sid), row, route=route)
                    else:
                        from gateway.session_operator import check_local_input
                        check_local_input(
                            self, SessionRef(self.profile_id, sid), row, source=source)
                self._require_admission_open()
                self.sessions.setdefault(sid, LiveSession(source, route))
                if any(row['status'] == 'unknown' for row in rows):
                    raise RuntimeStoreError('unknown_execution')
                self._schedule(SessionRef(self.profile_id, sid))
                results[sid] = 'ready'
            except RuntimeStoreError as exc:
                results[sid] = exc.reason
        return results

    async def submit(self, actor: Principal, request: Submission, *, _authorize_write=None, queue_while_restarting=False,
                     _replay_guard=None, _kanban_card=None, _on_committed=None):
        """``queue_while_restarting``: the caller is an interactive client attached to a restart's
        after-turn wait, which commits the input queued for the next owner. Machine callers (a2a,
        kanban, hosted rooms) keep the plain ``runtime_draining`` refusal and retry against the next
        owner with their own stable request id; none of them reads a released waiter as queued.

        ``_kanban_card`` is the Kanban dispatcher's trusted producer marker (``session_kanban.run_task``
        only): committed as ``kanban_card_v1``, it lets a card attempt lost across an owner restart
        settle interrupted (#134889 KB-8). Never a payload field: a client cannot supply it."""
        self.authorize(actor, request.ref, 'session:submit')
        async with self.sessions[request.ref.session_id].mutation_lock:
            # Deletion can retire this session while the caller waits on its writer.
            self.authorize(actor, request.ref, 'session:submit')
            from gateway.session_ingress_media import capture_lease
            with capture_lease():  # captured bytes are held from capture until commit or refusal
                return await self._submit(actor, request, _authorize_write=_authorize_write,
                                          queue_only=queue_while_restarting is True, _replay_guard=_replay_guard,
                                          _kanban_card=_kanban_card, _on_committed=_on_committed)

    async def _submit(self, actor, request, *, _authorize_write=None, queue_only=False,
                      _replay_guard=None, _kanban_card=None, _on_committed=None):
        self._require_fresh_admission_open(queue_only=queue_only)
        if (request.intent != 'queue' or not {'text'} <= set(request.payload) <= {
                'text', 'attachments', 'finite', 'unattended', 'surface', 'voice_context', 'interrupted',
                'voice_turn', 'display_kind', 'title_preview', 'turn_author', 'api_text', 'model_override', 'clarify_batch'}
                or not isinstance(request.payload['text'], str)):
            raise RuntimeStoreError('invalid_params')
        from gateway.session_ingress_media import admit_attachments
        from gateway.session_finite import admit_finite
        from gateway.session_surface import admit_surface
        from gateway.session_display import admit_display
        from gateway.session_cli_turn import admit_cli_turn
        finite = admit_finite(request.payload)
        def existing_admission():
            return find_session_admission(self.db, principal_id=actor.subject,
                                          session_id=request.ref.session_id, request_id=request.request_id)
        payload = {'text': request.payload['text'], **finite, **admit_surface(request.payload),
                   **admit_display(request.payload), **admit_cli_turn(request.payload),
                   **admit_attachments(request.payload.get('attachments'), admitted=existing_admission)}
        # This is the actual local prompt producer, including text-only and image
        # submissions.  Stamp the managed route before the durable row exists so the
        # claim can recover the exact I04 binding; never infer it from an absent
        # ContextVar or from the attachment schema.
        from gateway.session_managed_worker import managed_policy
        from gateway.session_operator import MANAGED_WORKER_DESCRIPTOR, MANAGED_WORKER_FIELD
        # Minimal owner fixtures and hosted-room adapters may expose no local policy
        # resolver at all; absence of that server-owned resolver is a fail-closed
        # ordinary route, never permission to stamp a managed descriptor.
        if (callable(getattr(self.runner, '_adapter_for_source', None))
                and managed_policy(self, request.ref) is not None):
            payload[MANAGED_WORKER_FIELD] = dict(MANAGED_WORKER_DESCRIPTOR)
        if _kanban_card is not None:
            payload['kanban_card_v1'] = _kanban_card
        source = self.sessions[request.ref.session_id].source
        # A LOCAL viewer admission is the interactive session-turn producer for the same FIFO and
        # control path as the API door. Bind its producer root before the durable write so an
        # approval/clarify callback can retain an exact I05 child under the claimed I04 turn.
        # Hosted/A2A/Kanban callers use their own source-owned custody handoff and stay untouched.
        from gateway.config import Platform
        local_preservation_handle = None
        if source is not None and source.platform == Platform.LOCAL:
            local_preservation_handle = self._preservation_root(
                'I02_api_session_turn', f'local:{request.ref.session_id}:{request.request_id}')
            self._preservation_pending_root(
                request.request_id, local_preservation_handle,
                principal_id=actor.subject, session_id=request.ref.session_id,
                source_identity='local_operator')
        if source is not None and source.user_id != actor.subject:
            # Durable server authorization, not a client payload field. The original
            # principal remains the admission/retry identity across owner restarts.
            # Operator provenance is about who was authorized at this boundary, not
            # whether the bound session happens to use the LOCAL transport.
            payload['local_operator_v1'] = {
                'profile_id': self.profile_id, 'session_id': request.ref.session_id,
                'principal_id': actor.subject}
        def admit():
            try:
                return admit_session_input(self.db, epoch=self.epoch, principal_id=actor.subject,
                                           session_id=request.ref.session_id, request_id=request.request_id,
                                           payload=payload, intent=request.intent, _replay_guard=_replay_guard,
                                           _authorize_write=self._admission_gate(
                                               _authorize_write, queue_only=queue_only, fresh=True))
            except Exception:
                self._release_refused_capture(request, payload)
                raise
        from gateway.session_runtime_workers import tracked_write

        def bind_local_preservation(row):
            if local_preservation_handle is None:
                return
            # An exact retry of an already terminal row owns no new producer work. Release the
            # fresh retry root; nonterminal rows bind it to the durable admission for I04/I05.
            if row.get('status') == 'terminal':
                self._preservation_forget_pending_root(local_preservation_handle)
                self._preservation_release_unaccepted(local_preservation_handle)
            else:
                self._preservation_bind_or_release(row, local_preservation_handle)

        def reconcile_local_preservation(_error=None):
            if local_preservation_handle is None:
                return
            try:
                row = find_session_admission(
                    self.db, principal_id=actor.subject, session_id=request.ref.session_id,
                    request_id=request.request_id,
                )
            except BaseException:  # health: allow BLE001 -- an opaque store read cannot prove no local producer; retain its exact root UNKNOWN
                coordinator = self._preservation_coordinator()
                if coordinator is not None:
                    coordinator.mark_unknown(local_preservation_handle, 'read_failed')
                return
            if row is None:
                self._preservation_forget_pending_root(local_preservation_handle)
                self._preservation_release_unaccepted(local_preservation_handle)
            else:
                bind_local_preservation(row)

        def committed(row):
            bind_local_preservation(row)
            if _on_committed is not None: _on_committed(row)
            return self._admitted(request.ref, None, row)
        try:
            return await tracked_write(
                self, admit, then=committed, ordered=True,
                after=partial(self._schedule_admitted, request.ref),
                on_failure=reconcile_local_preservation,
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            reconcile_local_preservation()
            raise

    def _release_refused_capture(self, request, payload):
        """A refused submission's captured bytes have no owner unless another admission holds the
        same bytes. They become durable retirement candidates (collected again by delete/prune if
        this pass fails), then a holder-aware release runs. Its failure is logged and never replaces
        the refusal the client must see (e.g. ``admission_conflict``)."""
        media = payload.get('attachments_v1', {}).get('media', [])
        if not media:
            return
        from gateway.session_ingress_media import drop_capture_lease
        drop_capture_lease()  # its own lease must not keep the refused bytes from collection
        from hermes_state_media import collect_retired_media, retire_media
        try:
            self.db._execute_write(lambda conn: retire_media(conn, {'attachments_v1': {'media': media}}))
            collect_retired_media(self.db)
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                'Releasing refused attachments of request %s failed', request.request_id)

    async def receipt(self, actor, ref, admission_id):
        self.authorize(actor, ref, 'session:submit')
        row = get_session_admission(self.db, admission_id=admission_id)
        if row is None or row['target_session_id'] != ref.session_id:
            raise RuntimeStoreError('not_found')
        if row['principal_id'] != actor.subject and 'session:control' not in actor.capabilities:
            raise RuntimeStoreError('permission_denied')
        live = self.sessions[ref.session_id]
        with live.event_stream.lock:
            # Read after the row: a terminal row whose completion is not yet published reads as
            # its claim, so a receipt poller never replays events that lack the completion frame.
            row = live.settling.get(admission_id, row)
        return self._receipt(row)

    async def cancel_queued(self, actor, ref, admission_id):
        before = await self.receipt(actor, ref, admission_id)
        from gateway.session_runtime_workers import track_mutation
        # Write off the shared loop; commit and observer settlement are one tracked task the
        # caller cannot cancel between (an exact retry still pays a left-over obligation).
        row = await asyncio.shield(track_mutation(self, self._commit_cancel(ref, admission_id, before)))
        # Media collection is its own write txn after the committed, settled cancellation: its
        # failure surfaces to the caller, and an exact retry collects again without re-settling.
        from gateway.session_ingress_media import release_admission_media
        await asyncio.to_thread(release_admission_media, self.db, admission_id)
        return self._receipt(row)

    async def cancel_or_claimed(self, actor, ref, admission_id):
        """Cancel a row the caller read ``queued``: None once cancelled, else the started row the
        drain claimed between that read and the cancel commit (``stale_generation``), which the
        caller must stop instead of refusing a live execution."""
        try:
            await self.cancel_queued(actor, ref, admission_id)
            return None
        except RuntimeStoreError as exc:
            if exc.reason != 'stale_generation':
                raise
        row = get_session_admission(self.db, admission_id=admission_id)
        if row is None or row['status'] != 'started':
            raise RuntimeStoreError('stale_generation')
        return row

    async def _commit_cancel(self, ref, admission_id, before):
        if before.status == 'queued':
            # Owed from before the write: a commit that reports failure, or anything raising
            # between the commit and observer settlement, leaves it for an exact retry to pay.
            self.cancel_obligations.add(admission_id)
        try:
            row = await asyncio.to_thread(cancel_session_input, self.db, epoch=self.epoch, admission_id=admission_id)
        except RuntimeStoreError:
            self.cancel_obligations.discard(admission_id)  # a definite refusal: nothing committed
            raise
        if (row['status'], row['outcome']) == ('terminal', 'cancelled') and admission_id in self.cancel_obligations:
            self._settle_cancelled(ref, admission_id)
        else:
            if row['status'] != 'queued':
                self.cancel_obligations.discard(admission_id)
            self._publish_pending(ref)
        return row

    def _settle_cancelled(self, ref, admission_id, outcome='cancelled'):
        # The only place a queued row becomes terminal: every observer kind that waits on
        # the admission (native delivery, API/webhook/hosted waiters, ACP and viewer streams)
        # settles here, or a cancelled row that never reaches _drain blocks them forever.
        live = self.sessions[ref.session_id]
        with live.event_stream.lock:
            self._publish_pending(ref)
            running = live.event_stream.execution
            # A queued row owns no execution generation; stamp its own identity so the
            # completion is not attributed to the turn currently running ahead of it.
            live.event_stream.execution = {'authority_epoch': self.epoch, 'admission_id': admission_id}
            try:
                live.event_stream.publish(ref.session_id, {
                    'text': '', 'content': '', 'admission_id': admission_id, 'outcome': outcome})
            finally:
                live.event_stream.execution = running
        # Published once: a later exact retry re-collects media but never repeats the completion.
        self.cancel_obligations.discard(admission_id)
        self.native_waiters.discard(admission_id)
        waiter = self.waiters.pop(admission_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        # A paused drain (preclaim refusal on this head) ended its task; the successors
        # need a fresh drain that revalidates them on their own merits.
        self._schedule(ref)
        self._preservation_release_terminal(admission_id)

    async def resolve_unknown(self, actor, ref, admission_id, generation):
        """Operator acknowledgement that an ``unknown`` turn will not finish; the paused FIFO behind
        it resumes. Never requeues the input. A turn that finished in THIS owner but whose
        settlement write failed (its exact result is still in ``pending_results``) commits that
        result instead of discarding it: the answer was produced, only its receipt was lost."""
        self.authorize(actor, ref, 'session:control')
        await self.receipt(actor, ref, admission_id)
        return await self.resolve_unknown_as_owner(ref, admission_id, generation)

    async def resolve_unknown_as_owner(self, ref, admission_id, generation, *, notice=None):
        """``resolve_unknown`` past authorization: also the owner's own call when the last viewer
        closed a session whose turn recovered ``unknown`` (gateway/session_viewer_close), or when an
        unattended automation turn settles itself (``notice``: its transcript copy, GW-10)."""
        captured = self.pending_results.get(admission_id)
        before = get_session_admission(self.db, admission_id=admission_id)
        prepared = None
        if captured is not None and before is not None and before['status'] == 'unknown':
            from gateway.session_results import prepare_result
            row = get_session_admission(self.db, admission_id=admission_id)
            prepared = prepare_result(row, captured['result'].get('final_response') or '', 'completed', captured)
        # The transcript boundary commits WITH the terminal transition: a follower can never be
        # claimed with the discarded text left open to be merged into its request. With a captured
        # result the answer is normally already the transcript tail, so the closer is a no-op.
        from gateway.session_results import close_discarded_turn
        from gateway.session_runtime_workers import tracked_write
        return await tracked_write(self, partial(
            resolve_unknown_session_input, self.db, epoch=self.epoch, admission_id=admission_id,
            generation=generation, _captured=prepared,
            # ``notice`` says the turn was interrupted: only when that is the outcome, never beside a
            # captured result this resolution commits as the turn's own answer.
            _terminal_write=lambda conn, lost: close_discarded_turn(self.db, conn, lost, None if (
                prepared is not None and lost['owner_epoch'] == self.epoch) else notice)),
            then=partial(self._resolved_unknown, ref, admission_id, prepared, captured))

    async def _resolved_unknown(self, ref, admission_id, prepared, captured, row):
        self.pending_results.pop(admission_id, None)
        if prepared is not None and row['owner_epoch'] == self.epoch:
            # The captured result committed: viewers told the turn is unknown get its completion,
            # unless the drain still owns the claim (resolution raced its recovery stamp) and
            # publishes the committed result itself.
            from gateway.session_settlement_recovery import _publish_completion
            live = self.sessions[ref.session_id]
            with live.event_stream.lock:
                if live.event_stream.execution.get('admission_id') != admission_id:
                    _publish_completion(self, live, ref, row, row,
                                        captured['result'].get('final_response') or '', captured)
        # Its own write txn + unlink, so it runs after the resolution commits; a discarded image
        # would otherwise stay on disk forever (the drain releases only settled turns).
        from gateway.session_ingress_media import release_admission_media
        await asyncio.to_thread(release_admission_media, self.db, admission_id)
        # Before the successor can run: a committed captured answer reaches the chat it was
        # owed to (a turn fenced unknown already released its delivery waiter), exactly once.
        from gateway.session_ingress import deliver_resolved
        await deliver_resolved(self, admission_id, owed=prepared is not None and row['owner_epoch'] == self.epoch)
        self._publish_pending(ref)
        self._schedule(ref)
        return self._receipt(row)

    async def interrupt(self, actor, ref, generation):
        self.authorize(actor, ref, 'session:control')
        handle = self._handle(ref)
        if handle.execution_generation != generation:
            raise RuntimeStoreError('stale_generation')
        live, agent = self.sessions[ref.session_id], None
        if handle.execution_state == 'running':
            # Latches are written under the lock adopt_agent takes on the executor thread, BEFORE the
            # agent is called: an adoption racing the call consumes them. The agent is called outside it.
            with live.event_stream.lock:
                adopted_generation, agent = self.adopted.get(ref.session_id, (None, None))
                if adopted_generation != generation:
                    agent = self.agent(ref)
                    if not self._runs_own_agent(ref):
                        # Accepted for this exact claim before its turn adopted an agent: whatever agent
                        # the turn adopts for this generation must not run the work as if no Stop arrived.
                        self.pending_stops[ref.session_id] = generation
                if agent is not None:
                    self.delivered_stops[ref.session_id] = generation
        # Main's Stop: hard-cancel (aborts compression too) and end the session's own background
        # delegations (spawned by this session only: a viewer must not kill other gateway work).
        from agent.interrupt_compat import request_hard_interrupt
        from tools.async_delegation import interrupt_for_session
        if agent is not None:
            request_hard_interrupt(agent)
        for parent in dict.fromkeys((ref.session_id, self.physical_target(ref))):
            interrupt_for_session(session_key=live.route, parent_session_id=parent, reason='user_stop')
        return self._handle(ref)

    def _classic_turn_holds(self, route):
        """The route's running slot is held by a turn this FIFO did not claim (the drain is serial:
        its own previous turn released the slot before this claim). Leaked and reaped slots are
        evicted first, so they never block the FIFO; the reaped check reads state.db at most 1/s."""
        runner = self.runner
        running = getattr(runner, '_is_session_running', None)
        if running is None or not running(route):
            return False
        import time
        now = time.monotonic()
        if now - self._classic_evict_at.get(route, 0.0) >= 1.0:
            self._classic_evict_at[route] = now
            runner._hm_evict_idle_stale_agent(route)
            if running(route):
                runner._hm_evict_reaped_agent(route)
        return running(route)

    def _runs_own_agent(self, ref):
        """The route's running slot holds a real agent (not the claim's pending sentinel), so the
        Stop above reached the turn's own agent, not a cached one it may still replace."""
        from gateway.run import _AGENT_PENDING_SENTINEL
        live = self.sessions[ref.session_id]
        if live.superseded:
            return False
        running = getattr(self.runner, '_running_agents', None) or {}
        agent = running.get(live.route)
        return agent is not None and agent is not _AGENT_PENDING_SENTINEL

    def steer_agent(self, ref, generation, text):
        """The agent a steer for this running generation reaches; the caller holds the session's
        event_stream.lock after check_approval_generation. Before the turn adopts its agent the
        resident one is the previous turn's cached agent, which this turn may replace, so *text* is
        latched for the agent this generation adopts (adopt_agent, under the same lock):
        ``(None, True)``. A claim with no steer seam (cron, managed worker) latches nothing."""
        adopted_generation, agent = self.adopted.get(ref.session_id, (None, None))
        if adopted_generation != generation:
            agent = self.agent(ref) if self._runs_own_agent(ref) else None
        from gateway.session_busy_controls import adopts_owner_agent
        if agent is not None or text is None or not adopts_owner_agent(self, ref):
            return agent, False
        latched, texts = self.pending_steers.get(ref.session_id, (None, []))
        self.pending_steers[ref.session_id] = (generation, [*texts, text] if latched == generation else [text])
        return None, True

    def adopt_agent(self, session_id, generation, agent):
        """The turn installs its agent for the running claim; a Stop latched while there
        was no agent to deliver it to fires now, never against a later generation. A Stop
        delivered to the cached agent for an earlier generation may have landed after that
        turn's finalizer cleared it, so it is dropped rather than cancelling this turn. A managed
        worker is a fresh process per turn and carries no earlier flag to drop."""
        live = self.sessions.get(session_id)
        # The turn's executor thread runs this; the stream lock orders it against session.steer's latch.
        with live.event_stream.lock if live is not None else contextlib.nullcontext():
            self.adopted[session_id] = (generation, agent)
            clear = getattr(agent, 'clear_interrupt', None)
            if self.delivered_stops.pop(session_id, generation) != generation and clear is not None:
                clear()
            if self.pending_stops.get(session_id) == generation:
                del self.pending_stops[session_id]
                from agent.interrupt_compat import request_hard_interrupt
                request_hard_interrupt(agent)
            latched, steers = self.pending_steers.pop(session_id, (None, ()))
            for text in steers if latched == generation else ():
                agent.steer(text)

    def check_approval_generation(self, session_id, generation):
        handle = self._handle(SessionRef(self.profile_id, session_id))
        if handle.execution_generation != generation or handle.execution_state != "running":
            raise RuntimeStoreError("stale_generation")

    def publish_execution(self, session_id, generation, event_type, payload):
        """Worker callbacks never outlive their exact running claim."""
        live = self.sessions[session_id]
        with live.event_stream.lock:
            # The executing generation (set with the claim's stamp, cleared the moment its execution
            # returns, before settlement): no per-token SQLite read under this lock, and a late
            # callback (an agent thread outliving a timed-out turn) never follows the completion.
            if self.executing.get(session_id) != generation:
                return False
            delivered = live.event_stream.publish(session_id, payload, event_type=event_type)
            from gateway.session_api_turn import publish_api_event
            observed = publish_api_event(self, session_id, event_type, payload)
            return bool(delivered or observed)

    def register_approval(self, session_id, generation, route, data, *, timeout_s=None):
        live = self.sessions[session_id]
        with live.event_stream.lock:
            self.check_approval_generation(session_id, generation)
            live.controls.register(session_id, route, generation, data, timeout_s=timeout_s)

    def register_clarify(self, session_id, generation, entry, *, timeout_s=None):
        live = self.sessions[session_id]
        with live.event_stream.lock:
            self.check_approval_generation(session_id, generation)
            live.controls.register_clarify(session_id, generation, entry, timeout_s=timeout_s)

    async def respond(self, actor, ref, generation, prompt_id, response, *, kind="approval"):
        capability = {"approval": "session:approve", "clarify": "session:respond"}.get(kind)
        if capability is None:
            raise RuntimeStoreError("invalid_params")
        self.authorize(actor, ref, capability)
        live = self.sessions[ref.session_id]
        with live.event_stream.lock:
            if actor not in live.subscribers.values():
                raise RuntimeStoreError("permission_denied")
            if type(generation) is not int:
                raise RuntimeStoreError("stale_generation")
            self.check_approval_generation(ref.session_id, generation)
            if not isinstance(prompt_id, str) or not prompt_id:
                raise RuntimeStoreError("invalid_params")
            return live.controls.respond(ref.session_id, generation, prompt_id, response, kind=kind)

    async def _claim_next(self, ref, live):
        # False asks the pump to re-read after a queued head changed during preflight.
        async with live.claim_gate, live.mutation_lock:
            pending = list_session_admissions(self.db, session_id=ref.session_id)
            if any(row['status'] == 'unknown' for row in pending):
                raise RuntimeStoreError('unknown_execution')
            first = next((row for row in pending if row['status'] == 'queued'), None)
            # The child lease is reserved before any route/session preflight await.  If that
            # preflight changes the head, the exact reservation is released below; an ambiguous
            # claim write retains it as unknown instead of guessing that no row committed.
            claim_child = self._preservation_reserve_claim(ref, first)
            from gateway.config import Platform
            if live.source.platform == Platform.LOCAL:
                # A reasoning command or a model pick made while the previous turn ran lands here, at the
                # turn boundary.
                from gateway.session_deferred_pick import apply_deferred_pick
                from gateway.session_setting_commands import apply_pending_reasoning
                await apply_pending_reasoning(self, ref, live)
                await apply_deferred_pick(self, ref, live)
            if first is not None and live.source.platform == Platform.LOCAL:
                from gateway.session_local_recovery import restore_local_session
                restore_local_session(self, ref.session_id)
                if first['request_id'].startswith('hosted:'):
                    from gateway.session_hosted_transport import check_remote_hosted_admission
                    if not await asyncio.to_thread(check_remote_hosted_admission, self, ref, first):
                        service = getattr(self, 'hosted_room_service', None)
                        if service is None:
                            raise RuntimeStoreError('permission_denied')
                        await asyncio.to_thread(service.check_admission, ref, first)
                    # Cancellation may advance the FIFO while the source owner is awaited;
                    # the successor must earn its own reauthorization, not inherit this one.
                    current = get_session_admission(self.db, admission_id=first['admission_id'])
                    if current is None or current['status'] != 'queued':
                        self._preservation_drop_unclaimed(first['admission_id'], 'read_failed')
                        return False, first
                if 'local_automation_v1' in first['payload'] or '_automation_custody_v1' in first['payload']:
                    from gateway.session_automation import check_local_automation
                    check_local_automation(self, ref, first)
                else:
                    from gateway.session_operator import check_local_input
                    check_local_input(self, ref, first)
                # Editor MCP schema drift re-freezes here, at this admission's start (a turn boundary),
                # once the head is authorized: a refused head spawns no server and commits no change.
                from gateway.session_local_mcp import refreeze_editor_mcp
                await refreeze_editor_mcp(self, ref, live)
                # The first real turn reopens a finalized row (#85303): mounts are reads, and
                # SessionStore would route a stamped row as stale onto a FRESH session. A write: off-loop,
                # tracked so retirement and the shutdown close gate count it.
                from gateway.session_local_recovery import reopen_local_session
                from gateway.session_runtime_workers import tracked_write
                from gateway.session_completion_notes import noted_completion
                if not noted_completion(first):  # a note leaves a closed row closed (session_completion_notes)
                    await tracked_write(self, partial(reopen_local_session, self, ref))
            if first is not None and 'native_text_v1' in first['payload']:
                from gateway.session_envelope import check_native_route
                await check_native_route(self.runner, first['payload'], self.physical_target(ref), live.source,
                                   self.runner._adapter_for_source(live.source))
                # Cancellation may advance FIFO while the connector is awaited.
                # Never let the successor inherit this row's fresh verdict.
                current = get_session_admission(self.db, admission_id=first['admission_id'])
                if current is None or current['status'] != 'queued':
                    self._preservation_drop_unclaimed(first['admission_id'], 'read_failed')
                    return False, first
            if first is not None and live.source.platform == Platform.API_SERVER:
                from gateway.session_api_turn import check_api_turn
                check_api_turn(self, ref, first['payload'])
            if first is not None and self._classic_turn_holds(live.route):
                # A classic turn (fallback wake, CLI handoff, legacy resume) holds the route's slot:
                # a claim now would hand this row to its busy path and settle it unrun. The head
                # waits queued, as main's pending slot did; /stop and /new still cancel it.
                self._preservation_drop_unclaimed(first['admission_id'], 'read_failed')
                return 'classic_busy', first
            try:
                self._require_admission_open()
            except RuntimeStoreError:
                self._preservation_drop_unclaimed(first['admission_id'], 'runtime_draining')
                raise
            from gateway.session_runtime_workers import tracked_write
            live.rescan = False  # a _schedule from here on may not be visible to this claim's read
            # The row these checks validated, never whichever is first when the write runs: a
            # cancellation commits from a worker thread and can advance the FIFO at any point.
            try:
                row = await tracked_write(self, partial(claim_session_input, self.db, epoch=self.epoch,
                                                        session_id=ref.session_id, _guard=self._admission_gate(),
                                                        head=first['admission_id'] if first is not None else None),
                                          then=partial(self._stamp_claim, ref, live, claim_child=claim_child,
                                                       claim_admission_id=first['admission_id'] if first is not None else None),
                                          ordered=True)
            except BaseException:
                if claim_child is not None:
                    self._preservation_claiming.pop(first['admission_id'], None)
                    self._preservation_mark_unknown(first['admission_id'], 'read_failed')
                raise
            # Input scheduled while this claim's transaction ran may postdate its read: re-read
            # instead of letting the drain exit idle over a committed admission.
            return (False if row is None and live.rescan else row), first

    def _stamp_claim(self, ref, live, row=None, *, claim_child=None, claim_admission_id=None):
        """The claim's execution stamp, in the same tracked task as its commit: no observer (or
        retirement's join) ever sees a committed claim that no execution stamp names."""
        admission_id = row.get('admission_id') if row else claim_admission_id
        self._preservation_activate_claim(admission_id, row, claim_child)
        self._preservation_bind_claim(admission_id, row)
        if row and admission_id in getattr(self, '_i10_recovery_bindings', {}):
            # Recovery adopts the queued row before the claim assigns its execution generation.
            # Advance that exact source binding at the canonical claim boundary so callback,
            # retry, and terminal proof consumers authenticate the stamped row rather than the
            # pre-claim generation.
            from gateway.session_recovery_custody import refresh_binding
            if not refresh_binding(self, row):
                self._preservation_mark_unknown(admission_id, 'unmapped_writer')
                raise RuntimeStoreError('preservation_owner_unmapped')
        if row:  # None or False (the head changed under the write) claimed nothing
            with live.event_stream.lock:
                live.event_stream.execution = {
                    'authority_epoch': self.epoch, 'execution_generation': row['generation'],
                    'admission_id': row['admission_id']}
                self.executing[ref.session_id] = row['generation']
                from gateway.session_automation import admission_display
                live.event_stream.publish(ref.session_id, admission_display(row.get('payload') or {}),
                                          event_type='message.start')
                self._publish_pending(ref)
        return row

    def _drain_stopping(self, ref, live):
        """A drain about to wait (storage retry, classic slot) exits instead when the runtime drains,
        the profile retires or the live entry was replaced; committed rows stay queued."""
        if self.runner._draining or self.retiring or self.sessions.get(ref.session_id) is not live:
            self._pause(ref, 'runtime_draining')
            return True
        return False

    async def _claim_with_retry(self, ref, live):
        """The drain's next ``(row, first)``, waiting out transient storage and a classic turn on the
        route; None when the drain must exit (paused, or stopping with committed rows left queued)."""
        backoff = 0.0
        while True:
            try:
                row, first = await self._claim_next(ref, live)
            except RuntimeStoreError as exc:
                if exc.reason == 'unknown_execution':
                    # An automation turn nobody is waiting on settles interrupted (GW-10); only a
                    # person's lost turn keeps the FIFO paused for them to resolve.
                    from gateway.session_unattended_unknown import settle_unattended_unknown
                    if await settle_unattended_unknown(self, ref):
                        continue
                self._preservation_drop_for_ref(ref, exc.reason)
                import logging
                logging.getLogger(__name__).warning('Session %s paused: %s', ref.session_id, exc.reason)
                self._pause(ref, exc.reason)
                return None
            except (OSError, sqlite3.OperationalError) as exc:
                self._preservation_drop_for_ref(ref, 'read_failed')
                # _claim_next releases a preflight child by its exact admission id. Storage errors
                # after the claim write retain the row/lease as unknown and retry this drain.
                # A writer held past its patience or a full disk is transient, and every _schedule
                # caller is event-driven: a dead drain would strand the committed head and hang its
                # waiters until the next submit. Retry in place (like recover_failed_settlement),
                # keeping waiters: releasing them fails requests that would run a moment later.
                if self._drain_stopping(ref, live):
                    return None
                backoff = min(max(backoff * 2, _CLAIM_RETRY_MIN_S), _CLAIM_RETRY_MAX_S)
                import logging
                logging.getLogger(__name__).warning(
                    'Session %s claim deferred by storage (%r); retrying in %.1fs', ref.session_id, exc, backoff)
                await asyncio.sleep(backoff)
                continue
            backoff = 0.0
            if row != 'classic_busy':
                return row, first
            if self._drain_stopping(ref, live):
                return None
            await asyncio.sleep(_CLAIM_RETRY_MIN_S)

    async def _execute_claimed(self, ref, live, row):
        """Run the claimed admission: ``(response, outcome)``. The executing stamp clears the moment
        execution returns; a lost managed worker settles an automation turn (``settled_unknown``) or pauses
        the FIFO and re-raises ManagedExecutionUnknown."""
        from gateway.session_finite import execute_finite_admission
        from gateway.session_managed_worker import ManagedExecutionUnknown
        review = getattr(live.review, '_background_review_run', None)
        if review is not None and not review.request_done.is_set():
            # A cold /refine's review shares the session id, as the resident agent's does: cancel it
            # before the turn (its bounded wait runs off-loop).
            from agent.background_review import cancel_background_review_for_live_turn
            await asyncio.to_thread(cancel_background_review_for_live_turn, live.review)
        try:
            try:
                return await execute_finite_admission(self, ref, row), 'completed'
            finally:
                with live.event_stream.lock:
                    self.executing.pop(ref.session_id, None)
        except ManagedExecutionUnknown:
            with live.event_stream.lock:
                live.event_stream.execution = {}
            self.pending_stops.pop(ref.session_id, None)
            self.pending_stop_causes.pop(ref.session_id, None)
            self.pending_steers.pop(ref.session_id, None)
            self.adopted.pop(ref.session_id, None)
            # An automation turn nobody waits on settles here, at the exit that fenced it (GW-10),
            # so the followers already queued run in this drain; a person's turn still pauses.
            from gateway.session_unattended_unknown import settle_unattended_unknown
            if await settle_unattended_unknown(self, ref):
                return None, 'settled_unknown'
            self._pause(ref, 'unknown_execution')
            raise
        except Exception:
            import logging
            logging.getLogger(__name__).exception('Admitted turn %s failed', row['admission_id'])
            return 'The admitted turn failed.', 'failed'

    async def _settle_claimed(self, ref, live, row, response, outcome):
        """Commit one claimed result or classify the exact recovery action for the drain."""
        admission_id = row['admission_id']
        settled = None
        settlement = {}
        try:
            from gateway.session_settlement_recovery import settle_with_retry
            captured = self.pending_results.get(admission_id)
            try:
                await settle_with_retry(self, live, ref, row, response, outcome, captured, settlement)
            finally:
                if 'settled' in settlement:
                    settled, response = settlement['settled'], settlement['response']
                    self.pending_results.pop(admission_id, None)
            if self.sessions.get(ref.session_id) is live:
                self._publish_pending(ref)
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                'Settlement or publication of admission %s failed', admission_id)
            if settled is None:
                self.pending_results.setdefault(admission_id, {
                    'result': {'final_response': response, 'failed': outcome == 'failed'}, 'usage': {}})
                from gateway.session_settlement_recovery import recover_failed_settlement
                try:
                    await recover_failed_settlement(self, row)
                except RuntimeStoreError:
                    self._pause(ref, 'unknown_execution')
                    return settled, response, 'return'
                current = get_session_admission(self.db, admission_id=admission_id)
                if current['status'] == 'unknown':
                    self._preservation_mark_unknown(admission_id, 'join_failed')
                    from gateway.session_ingress import retain_unsettled_delivery
                    retain_unsettled_delivery(self, admission_id)
                    self._publish_pending(ref)
                    self._pause(ref, 'unknown_execution')
                    return settled, response, 'return'
                from gateway.session_results import admission_result
                from gateway.session_settlement_recovery import publish_terminal
                with live.event_stream.lock:
                    captured = admission_result(self.db, admission_id)
                    self.pending_results.pop(admission_id, None)
                    if captured is None:
                        self._preservation_mark_unknown(admission_id, 'read_failed')
                        waiter = self.waiters.pop(admission_id, None)
                        if waiter is not None and not waiter.done():
                            waiter.set_exception(RuntimeStoreError('unknown_execution'))
                        return settled, response, 'continue'
                    settled = current
                    response = captured['result'].get('final_response') or ''
                    publish_terminal(self, ref, row, settled, response, captured)
        return settled, response, 'next'

    async def _drain(self, ref):
        from gateway.session_managed_worker import ManagedExecutionUnknown
        live = self.sessions[ref.session_id]
        while True:
            claimed = await self._claim_with_retry(ref, live)
            if claimed is None:
                return
            row, first = claimed
            if row is False:
                continue
            if row is None and first is not None:
                # A live worker (or a claim this drain does not own) blocks the head: the same
                # pause episode, so its notice is not repeated for every message queued behind it.
                self._pause(ref, 'session_busy')
                return
            # The FIFO is moving again (or empty): the next pause is a new episode.
            live.pause_notified = False
            if row is None:
                from gateway.session_acp_lifecycle import end_idle_acp_session
                # A write (idle check + stamp in one txn): off-loop. Input scheduled meanwhile set
                # ``rescan`` on this still-live drain, which must claim it rather than exit.
                from gateway.session_runtime_workers import tracked_write
                await tracked_write(self, partial(end_idle_acp_session, self, ref.session_id))
                if live.rescan:
                    continue
                from gateway.session_viewer_close import settle_viewer_close
                await settle_viewer_close(self, ref.session_id)  # a close that arrived mid-turn
                if live.rescan:
                    continue
                return
            admission_id = row['admission_id']
            try:
                response, outcome = await self._execute_claimed(ref, live, row)
                if outcome == 'settled_unknown':
                    self.deferred_resets.pop(live.route, None)  # no settlement follows, as below
                    continue
            except BaseException as exc:
                # No settlement follows (a lost managed worker, a cancelled drain): the turn is never
                # replayed, so its deferred reset is dropped (the next turn re-detects the overflow),
                # never left to fire after a later settled turn with a stale entry.
                self.deferred_resets.pop(live.route, None)
                if isinstance(exc, ManagedExecutionUnknown):
                    self._preservation_mark_unknown(admission_id, 'join_failed')
                    return
                raise
            self._preservation_mark_finalizing(admission_id, f'settlement:{admission_id}')
            settled = None
            try:
                settled, response, settlement_action = await self._settle_claimed(
                    ref, live, row, response, outcome,
                )
                if settlement_action == 'return':
                    return
                if settlement_action == 'continue':
                    continue
            except asyncio.CancelledError:
                self._preservation_mark_unknown(admission_id, 'join_failed')
                raise
            finally:
                # The stamp names a claimed, unsettled execution. Left in place, idle
                # mutations (session.updated) would carry a terminal generation and
                # a versioned viewer fence would discard them as late frames.
                with live.event_stream.lock:
                    live.event_stream.execution = {}
                self.pending_stops.pop(ref.session_id, None)
                self.pending_stop_causes.pop(ref.session_id, None)
                self.pending_steers.pop(ref.session_id, None)
                self.adopted.pop(ref.session_id, None)
                from gateway.session_queued_controls import publish_transfers
                publish_transfers(self, ref, row['generation'])
                if settled is None:
                    # Uncommitted (unknown or discarded): the user is not told an answer the FIFO lost.
                    self.pending_deliveries.pop(admission_id, None)
                from gateway.session_ingress import waiter_replies
                waiter_replies(self).pop(admission_id, None)
                # Taken on every exit, so a turn that never settled leaves no reset for a later one.
                reset = self.deferred_resets.pop(live.route, None)
            await self._run_deferred_reset(live.route, reset)
            from gateway.session_ingress import deliver_settled
            delivery_receipt = await deliver_settled(self, admission_id)
            if not await self._preservation_finish_settled_delivery(
                admission_id, delivery_receipt, response,
            ):
                return

    @staticmethod
    async def _run_deferred_reset(route, reset):
        """The auto-reset the settled turn deferred (``_hmwa_defer_exhaustion_reset``), before its
        waiter wakes: a delivery retry or the next DM then reads the fresh target."""
        if reset is not None:
            try:
                await reset()
            except Exception:
                import logging
                logging.getLogger(__name__).exception('Deferred auto-reset of %s failed', route)

    async def _resolve_settled_waiter(self, admission_id, response):
        """Hand the settled reply to its waiter. The adapter sending it adopted the turn's crash marker
        and releases it once the reply is ledgered (seconds with auto-TTS); a follower claimed before
        that replaces the one-slot marker, so a kill in between would lose the persisted reply: the
        drain waits for that release (bounded; never while draining or retiring)."""
        adopter = getattr(self, 'marker_adopters', {}).get(admission_id)
        released = None
        if adopter is not None and getattr(adopter, '_gateway_active_turn_token', None):
            released = adopter._gateway_marker_released = asyncio.Event()
        waiter = self.waiters.pop(admission_id, None)
        if waiter is None or waiter.done():
            return False
        waiter.set_result(response)
        if released is not None and not (self.runner._draining or self.retiring):
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(released.wait(), _MARKER_RELEASE_WAIT_S)
            return released.is_set()
        return True


def backfill_legacy_session_owners(db):
    """Stamp this store's own profile onto pre-ownership NULL rows (#94724).

    The owner is the only writer while it holds the profile, so the offline REST backfill is
    refused for as long as it runs; doing it here is what migrates an upgraded install. Idempotent
    and never overwrites an owner, so every start runs it. A store outside the profile tree has
    no derivable owner and keeps NULL.
    """
    import logging
    stamp = db._own_profile_name()
    if not stamp:
        return 0
    try:
        stamped = db.backfill_null_session_profiles(stamp)
    except Exception:
        logging.getLogger(__name__).warning('Legacy session owner backfill failed for %s', db.db_path, exc_info=True)
        return 0
    if stamped:
        logging.getLogger(__name__).info('Stamped %d legacy session row(s) with owner profile %r', stamped, stamp)
    return stamped


def backfill_compression_created_sources(db):
    """List conversations compressed before write-time stamping under their surface again: a chain
    root without a created_source takes its nearest stamped continuation's. Idempotent and never
    overwrites a known value, so every start runs it, like the owner backfill above."""
    import logging
    try:
        stamped = db.stamp_legacy_compression_chains()
    except Exception:
        logging.getLogger(__name__).warning('Compression created_source repair failed for %s', db.db_path, exc_info=True)
        return 0
    if stamped:
        logging.getLogger(__name__).info('Stamped created_source on %d compressed session row(s)', stamped)
    return stamped


async def initialize_session_authority(runner, *, profile_id, instance_id, db=None, register=True):
    """Compatibility export for the runtime-owned authority bootstrap."""
    from gateway.run_runtime import initialize_session_authority as _initialize
    return await _initialize(
        runner, profile_id=profile_id, instance_id=instance_id, db=db, register=register,
    )
