"""Process-owned admission and close fence for cooperative Desktop backend retirement.

The fence is the process-owned admission gate. Work is represented by opaque,
process-local leases rather than an integer counter so an asynchronous finalizer can
release exactly the reservation it accepted. The reversible close API is only a
custody/barrier receipt: it stops new roots but never proves idle or drained state.
The legacy prepare/commit/cancel protocol keeps its existing idle-proof and 30-second
token semantics, with exact lease and pending-child rechecks before an irreversible
commit.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
import logging
import secrets
import threading
import time


_PROCESS_INSTANCE = secrets.token_urlsafe(24)


@dataclass(frozen=True)
class RetirementLease:
    """An exact process-local reservation for one accepted piece of work.

    The object is intentionally truthy so existing ``work()`` checks keep their
    boolean behavior. ``release`` accepts this exact object, never a count, owner
    string, or copied context value.
    """

    lease_id: str
    owner_kind: str
    owner_id: str
    profile_id: str | None
    parent_lease_id: str | None
    close_generation: int
    process_instance: str
    _fence_id: str = field(repr=False)

    def __bool__(self):
        return True


@dataclass(frozen=True)
class RetirementChildReservation:
    """A pre-close continuation reservation that can be activated after close."""

    reservation_id: str
    owner_kind: str
    owner_id: str
    profile_id: str | None
    parent_lease_id: str
    close_generation: int
    process_instance: str
    _fence_id: str = field(repr=False)

    def __bool__(self):
        return True


@dataclass(frozen=True)
class RetirementClose:
    """The exact owner receipt for one legacy or reversible close.

    ``reversible`` distinguishes the new custody barrier from the legacy idle-proof
    token. Only legacy receipts auto-expire; a reversible deadline bounds the owner's
    operation but never reopens the fence while accepted custody is unknown.
    """

    token: str = field(repr=False)
    owner_kind: str
    owner_id: str
    scope: str
    close_generation: int
    deadline: float
    nonce: str = field(repr=False)
    process_instance: str
    _fence_id: str = field(repr=False)
    reversible: bool = field(default=False, repr=False)


class RetirementFence:
    """A process-wide admission fence with reversible close ownership.

    ``begin_close`` is a lock-linearized barrier and custody receipt. It may be
    acquired while active leases or pending child reservations exist, and it does
    not call ``idle_proof`` or return a drained verdict. Existing accepted work may
    finish naturally, and a child reserved before the barrier may activate under it.
    ``release_close`` is an explicit owner abort; it may reopen a fence that still
    has accepted work, so callers must not describe that operation as drained.

    ``prepare`` remains the legacy idle-proof path. It refuses active leases and
    pending child reservations both before and after the probe, and its 30-second
    uncommitted token expiry is unchanged. ``commit`` and ``cancel`` recognize only
    the legacy receipt. A committed close is permanent for this fence instance.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._active: dict[str, RetirementLease] = {}
        self._child_reservations: dict[str, RetirementChildReservation] = {}
        self._preparing = False
        self._close: RetirementClose | None = None
        # Keep the legacy token carrier for callers that only observe the token-shaped
        # response; the structured receipt is authoritative for identity and generation.
        self._token: str | None = None
        self._committed = False
        self._generation = 0
        self._fence_id = secrets.token_urlsafe(16)
        self._process_instance = _PROCESS_INSTANCE
        self._now = time.monotonic

    def _expire(self):
        """Expire only the legacy 30-second token; reversible close never auto-reopens."""
        close = self._close
        if close is not None and not close.reversible and not self._committed and self._now() >= close.deadline:
            self._close = None
            self._token = None

    def _is_current_lease(self, lease):
        return (
            isinstance(lease, RetirementLease)
            and lease.process_instance == self._process_instance
            and lease._fence_id == self._fence_id
            and self._active.get(lease.lease_id) is lease
        )

    def _is_current_reservation(self, reservation):
        return (
            isinstance(reservation, RetirementChildReservation)
            and reservation.process_instance == self._process_instance
            and reservation._fence_id == self._fence_id
            and self._child_reservations.get(reservation.reservation_id) is reservation
        )

    @staticmethod
    def _identity(value, default):
        value = default if value is None else str(value).strip()
        return value or default

    def _new_close(self, *, owner_kind, owner_id, scope, deadline_s, reversible):
        self._generation += 1
        token = secrets.token_urlsafe(32)
        return RetirementClose(
            token=token,
            owner_kind=self._identity(owner_kind, "retirement"),
            owner_id=self._identity(owner_id, "retirement:close"),
            scope=self._identity(scope, "process"),
            close_generation=self._generation,
            deadline=self._now() + max(0.0, float(deadline_s)),
            nonce=secrets.token_urlsafe(18),
            process_instance=self._process_instance,
            _fence_id=self._fence_id,
            reversible=reversible,
        )

    def acquire(self, *, owner_kind="work", owner_id=None, profile_id=None, parent=None):
        """Return one exact lease, or ``None`` when new work is refused.

        A root admission is refused during legacy preparation, after commit, or
        while a close is held. During a reversible close, only a live parent lease
        created before that close may admit a direct child. A continuation whose
        parent has already finished must reserve before close and activate later.
        """
        with self._lock:
            self._expire()
            if self._preparing or self._committed:
                return None
            close = self._close
            if close is not None:
                if not close.reversible:
                    return None
                if parent is None or not self._is_current_lease(parent):
                    return None
                if parent.close_generation >= close.close_generation:
                    return None
            elif parent is not None and not self._is_current_lease(parent):
                return None

            lease_id = secrets.token_urlsafe(18)
            kind = self._identity(owner_kind, "work")
            owner = self._identity(owner_id, f"{kind}:{lease_id}")
            lease = RetirementLease(
                lease_id=lease_id,
                owner_kind=kind,
                owner_id=owner,
                profile_id=profile_id,
                parent_lease_id=parent.lease_id if parent is not None else None,
                close_generation=close.close_generation if close is not None else self._generation,
                process_instance=self._process_instance,
                _fence_id=self._fence_id,
            )
            self._active[lease_id] = lease
            return lease

    def release(self, lease):
        """Release exactly ``lease``; return ``False`` for stale or foreign values."""
        if not isinstance(lease, RetirementLease):
            return False
        with self._lock:
            if not self._is_current_lease(lease):
                return False
            del self._active[lease.lease_id]
            return True

    def run_with_lease(self, lease, fn, *args, **kwargs):
        """Run one accepted callable and release its exact lease in ``finally``.

        A Future's ``done`` callback is not the producer's terminal boundary: a
        callable can still be unwinding its cleanup when the Future becomes done,
        and callback registration can fail.  Callers use this wrapper for work that
        actually starts; a separate cancellation observer handles a Future that is
        cancelled before the callable starts.
        """
        try:
            return fn(*args, **kwargs)
        finally:
            self.release(lease)

    def observe_future_cancellation(self, future, lease, on_cancel=None):
        """Release ``lease`` only when an accepted Future is cancelled before start.

        Running callables release through :meth:`run_with_lease`.  The callback is
        therefore deliberately cancellation-only.  A callback attachment failure
        returns ``False`` and leaves the exact lease held: callers must keep the
        accepted work/unknown custody alive and must not pretend submission failed.
        If the Future is already cancelled when attachment fails, the cancellation
        is settled immediately.  The local once guard also protects an immediate
        callback plus a fallback cancellation check from double-finalizing.
        """
        settled = False
        settled_lock = threading.Lock()

        def settle_cancelled():
            nonlocal settled
            with settled_lock:
                if settled:
                    return
                settled = True
            try:
                if on_cancel is not None:
                    on_cancel()
            except Exception:  # health: allow BLE001 -- optional cancellation cleanup is an opaque callback; release remains in finally
                logging.getLogger(__name__).warning(
                    "Retirement cancellation cleanup failed; exact lease release continues"
                )
            finally:
                self.release(lease)

        def done(completed):
            try:
                if completed.cancelled():
                    settle_cancelled()
            except Exception:  # health: allow BLE001 -- Future.cancelled() is an opaque executor callback; unknown status retains custody
                logging.getLogger(__name__).warning(
                    "Retirement cancellation observation failed; retaining custody"
                )

        try:
            future.add_done_callback(done)
        except BaseException:  # health: allow BLE001 -- arbitrary Future implementations may raise cancellation/system exceptions; an unobservable observer retains the lease
            logging.getLogger(__name__).warning(
                "Retirement cancellation observer attachment failed; retaining custody"
            )
            try:
                already_cancelled = bool(future.cancelled())
            except BaseException:  # health: allow BLE001 -- cancellation status is an opaque Future boundary; unknown means custody remains held
                logging.getLogger(__name__).warning(
                    "Retirement cancellation status check failed; retaining custody"
                )
                already_cancelled = False
            if already_cancelled:
                settle_cancelled()
            return False
        return True

    def reserve_child(self, parent, *, owner_kind="child", owner_id=None, profile_id=None):
        """Reserve a continuation before close without copied context state."""
        with self._lock:
            self._expire()
            if self._preparing or self._close is not None or self._committed:
                return None
            if not self._is_current_lease(parent):
                return None
            reservation_id = secrets.token_urlsafe(18)
            kind = self._identity(owner_kind, "child")
            owner = self._identity(owner_id, f"{kind}:{reservation_id}")
            reservation = RetirementChildReservation(
                reservation_id=reservation_id,
                owner_kind=kind,
                owner_id=owner,
                profile_id=profile_id,
                parent_lease_id=parent.lease_id,
                close_generation=self._generation,
                process_instance=self._process_instance,
                _fence_id=self._fence_id,
            )
            self._child_reservations[reservation_id] = reservation
            return reservation

    def activate_child(self, reservation):
        """Activate one exact pre-close reservation after preparation has settled."""
        if not isinstance(reservation, RetirementChildReservation):
            return None
        with self._lock:
            self._expire()
            # A legacy idle proof owns a freeze. Refuse activation across it; the
            # final locked probe check also protects against future race changes.
            if self._preparing or self._committed or not self._is_current_reservation(reservation):
                return None
            close = self._close
            if close is not None:
                if not close.reversible or reservation.close_generation >= close.close_generation:
                    return None
            del self._child_reservations[reservation.reservation_id]
            lease_id = secrets.token_urlsafe(18)
            lease = RetirementLease(
                lease_id=lease_id,
                owner_kind=reservation.owner_kind,
                owner_id=reservation.owner_id,
                profile_id=reservation.profile_id,
                parent_lease_id=reservation.parent_lease_id,
                close_generation=close.close_generation if close is not None else self._generation,
                process_instance=self._process_instance,
                _fence_id=self._fence_id,
            )
            self._active[lease_id] = lease
            return lease

    def release_child_reservation(self, reservation):
        """Drop an unactivated reservation only with its exact object."""
        if not isinstance(reservation, RetirementChildReservation):
            return False
        with self._lock:
            if not self._is_current_reservation(reservation):
                return False
            del self._child_reservations[reservation.reservation_id]
            return True

    def active_count(self):
        """Diagnostic count of fence leases; this is not a runtime work census."""
        with self._lock:
            return len(self._active)

    def pending_child_count(self):
        """Diagnostic count of unresolved child custody; not a full runtime census."""
        with self._lock:
            return len(self._child_reservations)

    def _legacy_prepare_with_probe(self):
        from hermes_cli.web_server_idle_proof import idle_proof

        with self._lock:
            self._expire()
            if self._committed:
                return True, self._close
            if (
                self._preparing
                or self._close is not None
                or self._active
                or self._child_reservations
            ):
                return False, None
            self._preparing = True

        verdict = None
        try:
            verdict = idle_proof()["idle"]
        except Exception:
            logging.getLogger(__name__).warning("Retirement idle probe unavailable", exc_info=True)
        finally:
            with self._lock:
                self._preparing = False
                # The probe runs outside our lock. Recheck every fence-owned hold
                # under the lock before creating the legacy idle receipt.
                if (
                    verdict is True
                    and not self._committed
                    and self._close is None
                    and not self._active
                    and not self._child_reservations
                ):
                    receipt = self._new_close(
                        owner_kind="health-retirement",
                        owner_id="health-retirement",
                        scope="process",
                        deadline_s=30.0,
                        reversible=False,
                    )
                    self._close = receipt
                    self._token = receipt.token
                elif verdict is True:
                    # A child/lease that appeared before this locked finalization
                    # wins over the optimistic probe result.
                    verdict = False
        return verdict, self._close if verdict is True and self._close is not None else None

    def begin_close(
        self,
        *,
        owner_kind="retirement",
        owner_id="retirement:close",
        scope="process",
        deadline_s=30.0,
    ):
        """Claim a reversible close barrier and exact custody receipt.

        This is deliberately not an idle probe. Existing active leases and pending
        child reservations are allowed and remain in custody; only new roots are
        refused. A competing preparation/close or committed fence returns ``None``.
        Deadline expiry never clears this receipt; the owner must explicitly release
        or complete it, and release is not a drained verdict.
        """
        kind = self._identity(owner_kind, "retirement")
        owner = self._identity(owner_id, "retirement:close")
        with self._lock:
            self._expire()
            if self._committed or self._preparing or self._close is not None:
                return None
            receipt = self._new_close(
                owner_kind=kind,
                owner_id=owner,
                scope=scope,
                deadline_s=deadline_s,
                reversible=True,
            )
            self._close = receipt
            return receipt

    def release_close(self, receipt):
        """Explicitly abort one reversible close; reopening is not a drain result."""
        if not isinstance(receipt, RetirementClose) or not receipt.reversible:
            return False
        with self._lock:
            self._expire()
            if self._close is not receipt or self._committed:
                return False
            self._close = None
            self._token = None
            return True

    def commit_close(self, receipt):
        """Commit one exact barrier after this fence's local custody settles.

        A successful result makes this fence permanently closed; it is not a
        process-retired or whole-runtime-drained verdict. Active leases and
        pending child reservations therefore remain a hard refusal condition.
        """
        if not isinstance(receipt, RetirementClose) or not receipt.reversible:
            return False
        with self._lock:
            self._expire()
            if self._close is not receipt or self._committed:
                return False
            if self._now() > receipt.deadline:
                return False
            if self._active or self._child_reservations:
                return False
            self._committed = True
            self._token = receipt.token
            return True

    def prepare(self):
        """Legacy idle-proof/prepare response with unchanged token semantics."""
        verdict, receipt = self._legacy_prepare_with_probe()
        if verdict is not True or receipt is None:
            return {"ok": False, "idle": verdict}
        return {"ok": True, "idle": True, "token": receipt.token}

    def _matches_token(self, token):
        self._expire()
        close = self._close
        if close is None:
            return False
        # A reversible receipt is not a legacy commit token while its barrier
        # is still held. Once commit_close has durably committed this exact
        # fence, accepting its token is the legacy recovery/idempotency path.
        if close.reversible and not self._committed:
            return False
        if isinstance(token, RetirementClose):
            return close is token
        return isinstance(token, str) and token == close.token

    def commit(self, token):
        """Commit only the exact legacy receipt after a locked custody recheck."""
        with self._lock:
            if not self._matches_token(token):
                return {"ok": False}
            # A committed reversible receipt can be replayed through the
            # existing prepare/commit API. This is recovery of the same
            # committed fence, never a second idle or drain proof.
            if self._committed:
                return {"ok": True}
            if not self._active and not self._child_reservations:
                self._committed = True
                return {"ok": True}
            return {"ok": False}

    def cancel(self, token):
        with self._lock:
            if self._committed or not self._matches_token(token):
                return {"ok": False}
            if self._active or self._child_reservations:
                return {"ok": False}
            self._close = None
            self._token = None
            return {"ok": True}

    @contextmanager
    def work(self, *, owner_kind="work", owner_id=None, profile_id=None, parent=None):
        lease = self.acquire(
            owner_kind=owner_kind,
            owner_id=owner_id,
            profile_id=profile_id,
            parent=parent,
        )
        try:
            yield bool(lease)
        finally:
            if lease is not None:
                self.release(lease)


retirement = RetirementFence()
