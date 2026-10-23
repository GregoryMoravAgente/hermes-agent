"""Exact custody for tool workers that outlive their submitting turn.

The ordinary agent tool executors use daemon pools deliberately: a wedged tool must
not hold interpreter exit.  That does not make an accepted tool write disappear,
though.  This module keeps the exact submission handle and an opaque parent binding
in the task context so the gateway can retain the owning turn until the worker's
body and finalizers really finish.

The module is intentionally independent of ``gateway``.  A gateway parent is a
small source-owned object supplied through :func:`bind_tool_worker_parent`; the
generic agent and pool code only call its admission and settlement methods.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator

from tools.thread_context import propagate_context_to_thread


logger = logging.getLogger(__name__)


class ToolWorkerAdmissionError(RuntimeError):
    """The current turn cannot admit another nested worker."""


class ToolWorkerSubmissionError(RuntimeError):
    """A pool handoff was refused or could not be proven accepted.

    ``accepted`` is deliberately kept separate from the exception text.  A
    caller may wait and join when the pool refused before enqueue, but must
    abandon its join when an accepted/ambiguous queue item has no Future to
    observe.  In the latter case the custody handle remains attached to its
    exact parent and carries the UNKNOWN state until a source-owned observer
    can settle it.
    """

    def __init__(self, message: str, *, handle, accepted: str, future=None, error=None):
        super().__init__(message)
        self.handle = handle
        self.accepted = accepted
        self.future = future
        self.error = error


_PARENT: contextvars.ContextVar[Any] = contextvars.ContextVar("hermes_tool_worker_parent", default=None)


@dataclass
class ToolWorkerHandle:
    """One accepted or refused submission, identified by its own opaque token.

    ``future`` is only one part of the identity.  A queue handoff can be accepted
    before a Future is returned, and a broken worker factory can leave an accepted
    queue item with no running thread.  In both cases the handle remains UNKNOWN
    rather than being replaced by a TID, counter, or empty registry entry.
    """

    route: str
    owner: Any
    parent: Any = None
    handle_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    future: Any = None
    state: str = "admitted"
    worker_tid: int | None = None
    started: threading.Event = field(default_factory=threading.Event)
    worker_exited: threading.Event = field(default_factory=threading.Event)
    body_done: threading.Event = field(default_factory=threading.Event)
    finalizers_done: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _settled: bool = field(default=False, repr=False)
    _future_observer_bound: bool = field(default=False, repr=False)

    def bind_future(self, future: Any) -> None:
        with self._lock:
            if self.future is not None and self.future is not future:
                raise ToolWorkerAdmissionError("tool_worker_future_rebind")
            self.future = future

    def mark_queued(self) -> None:
        with self._lock:
            if not self._settled and self.state == "admitted":
                self.state = "queued"

    def mark_started(self, worker_tid: int | None = None) -> None:
        with self._lock:
            if self._settled:
                return
            self.worker_tid = worker_tid if worker_tid is not None else threading.get_ident()
            self.state = "running"
            self.started.set()

    def mark_worker_exited(self) -> None:
        """Record the worker wrapper's exit before the Future is completed."""
        self.worker_exited.set()

    def mark_submit_unknown(self) -> None:
        """Retain an ambiguous accepted handoff until its exact worker settles."""
        with self._lock:
            if not self._settled:
                self.state = "unknown"

    def mark_submit_refused(self) -> None:
        self._settle("refused")

    def mark_body_done(self) -> None:
        with self._lock:
            self.body_done.set()
        self._maybe_settle()

    def mark_finalizers_done(self) -> None:
        with self._lock:
            self.finalizers_done.set()
        self._maybe_settle()

    def _maybe_settle(self) -> None:
        with self._lock:
            if self._settled or not (self.body_done.is_set() and self.finalizers_done.is_set()):
                return
            self._settled = True
            self.state = "settled"
        parent = self.parent
        if parent is not None:
            try:
                parent.settle_tool_worker(self)
            except Exception:
                # Custody metadata must not change the tool's result.  The parent
                # remains the authority's retained object and will be reconciled by
                # its own exact-handle inspection if this observer fails.
                logger.debug("nested tool worker parent settlement observer failed", exc_info=True)

    def _settle(self, state: str) -> None:
        with self._lock:
            if self._settled:
                return
            self._settled = True
            self.state = state
        parent = self.parent
        if parent is not None:
            try:
                parent.settle_tool_worker(self)
            except Exception:
                logger.debug("nested tool worker refusal observer failed", exc_info=True)

    @property
    def pending(self) -> bool:
        with self._lock:
            return not self._settled


@contextmanager
def bind_tool_worker_parent(parent: Any) -> Iterator[Any]:
    """Bind one source-owned parent to this turn and propagated worker contexts."""
    token = _PARENT.set(parent)
    try:
        yield parent
    finally:
        _PARENT.reset(token)


def current_tool_worker_parent() -> Any:
    """Return the exact parent inherited by the current turn/task, if any."""
    return _PARENT.get()


def register_tool_worker_delegate(owner: Any) -> Any:
    """Bind one built delegate to the exact opaque parent in this context.

    Delegation creates a distinct ``AIAgent`` object, so identity with the
    outer agent is intentionally not the admission rule.  The gateway parent
    performs the source-owned lineage, profile and generation checks.  A
    context without a gateway parent is the direct CLI/TUI path and needs no
    registration.
    """
    parent = _PARENT.get()
    if parent is None:
        return None
    register = getattr(parent, "register_delegate_agent", None)
    # Require a real class-owned registration method.  Dynamic test doubles
    # (and legacy context holders) expose arbitrary callable attributes but do
    # not own a source lineage authority.
    if not callable(getattr(type(parent), "register_delegate_agent", None)) or not callable(register):
        # Legacy low-level parents do not own delegated-agent lineage.  Keep
        # their existing context contract; the production gateway parent
        # exposes register_delegate_agent and therefore takes the strict path.
        return None
    try:
        accepted = register(owner)
    except ToolWorkerAdmissionError:
        raise
    except Exception as exc:
        raise ToolWorkerAdmissionError("nested_tool_worker_delegate_registration_failed") from exc
    if accepted is not True:
        raise ToolWorkerAdmissionError("nested_tool_worker_delegate_not_current")
    # This is source-owned opaque state, not a caller-supplied parent name.  The
    # child runner re-binds this same object when a copied ContextVar is absent.
    try:
        existing = getattr(owner, "_tool_worker_parent", None)
    except Exception as exc:
        raise ToolWorkerAdmissionError("nested_tool_worker_delegate_owner_unreadable") from exc
    if existing is not None and existing is not parent:
        raise ToolWorkerAdmissionError("nested_tool_worker_delegate_parent_rebind")
    try:
        owner._tool_worker_parent = parent
    except Exception as exc:
        raise ToolWorkerAdmissionError("nested_tool_worker_delegate_owner_unbindable") from exc
    return parent


def tool_worker_parent_for(owner: Any) -> Any:
    """Return the exact source-owned parent recorded for a delegated child."""
    return getattr(owner, "_tool_worker_parent", None)


def admit_tool_worker(owner: Any, route: str) -> ToolWorkerHandle:
    """Admit one child before the pool receives its callable.

    A gateway parent may refuse because its turn was superseded or its authority is
    closing.  No executor submission is attempted after that refusal.
    """
    parent = _PARENT.get()
    handle = ToolWorkerHandle(route=route, owner=owner, parent=parent)
    if parent is None:
        # Direct CLI/TUI agents have no profile authority to retain.  They still
        # receive an exact handle so the submission wrapper never needs a second
        # path, and their normal daemon-pool behavior is unchanged.
        handle.state = "unbound"
        return handle
    try:
        parent.admit_tool_worker(handle)
    except Exception as exc:
        handle.mark_submit_refused()
        if isinstance(exc, ToolWorkerAdmissionError):
            raise
        raise ToolWorkerAdmissionError("nested tool worker admission refused") from exc
    return handle


def submit_tool_worker(executor: Any, handle: ToolWorkerHandle, fn, /, *args, **kwargs):
    """Submit a worker while retaining queue/start/finish identity.

    ``DaemonThreadPoolExecutor.submit_owned`` is the source-owned queue boundary.
    It distinguishes a refusal before enqueue from an accepted queue whose worker
    start path is ambiguous.  The ordinary ``submit`` fallback is retained for
    extension/test executors that do not implement the owned protocol; its raised
    exception is conservatively UNKNOWN because it cannot prove that no work was
    accepted.
    """
    native_owned = callable(getattr(executor, "submit_owned", None))

    def run_with_custody(*call_args, **call_kwargs):
        handle.mark_started(threading.get_ident())
        try:
            try:
                result = fn(*call_args, **call_kwargs)
            except BaseException:
                handle.mark_body_done()
                raise
            handle.mark_body_done()
            return result
        finally:
            # A foreign/legacy executor cannot prove the post-WorkItem
            # callback boundary.  Direct unbound callers retain the historical
            # local settlement; a bound parent stays UNKNOWN until a native
            # owned Future proves finalization.
            if not native_owned or handle.parent is None:
                handle.mark_finalizers_done()

    handle.mark_queued()
    submit_owned = getattr(executor, "submit_owned", None)
    if callable(submit_owned):
        submission = submit_owned(
            handle.handle_id,
            run_with_custody,
            *args,
            on_started=lambda _handoff_id: handle.mark_started(threading.get_ident()),
            on_exited=lambda _handoff_id: handle.mark_worker_exited(),
            on_finalized=lambda _handoff_id: handle.mark_finalizers_done(),
            **kwargs,
        )
        accepted = getattr(submission, "accepted", None)
        future = getattr(submission, "future", None)
        error = getattr(submission, "error", None)
        if accepted == "no":
            handle.mark_submit_refused()
            message = str(error) if error is not None else "tool worker submission refused before enqueue"
            raise ToolWorkerSubmissionError(
                message, handle=handle, accepted="no", future=None, error=error,
            ) from error
        if accepted not in {"yes", "unknown"}:
            handle.mark_submit_unknown()
            raise ToolWorkerSubmissionError(
                "tool worker submission returned an invalid acceptance state",
                handle=handle, accepted="unknown", future=future, error=error,
            ) from error
        if future is None:
            handle.mark_submit_unknown()
            message = str(error) if error is not None else "accepted tool worker submission returned no Future"
            raise ToolWorkerSubmissionError(
                message, handle=handle, accepted=accepted, future=None, error=error,
            ) from error
        handle.bind_future(future)
        from tools.daemon_pool import OwnedFuture
        if not isinstance(future, OwnedFuture):
            # A submit_owned-shaped foreign executor may accept the callback
            # keyword without implementing the native Future boundary.  Keep
            # the exact handle UNKNOWN rather than trusting its Future.done().
            handle.mark_submit_unknown()
        if accepted == "unknown" or error is not None:
            handle.mark_submit_unknown()
        return future

    try:
        future = executor.submit(propagate_context_to_thread(run_with_custody), *args, **kwargs)
    except BaseException as error:
        handle.mark_submit_unknown()
        raise ToolWorkerSubmissionError(
            str(error), handle=handle, accepted="unknown", error=error,
        ) from error
    handle.bind_future(future)
    return future


def bind_existing_tool_worker_future(handle: ToolWorkerHandle, future: Any) -> bool:
    """Bind a legacy prepared slot to its already-created Future.

    Prepared terminal workers created before custody was added have a Future but
    no ``ToolWorkerHandle``.  The handle must observe that exact Future; a new
    placeholder task, TID or completed boolean would be a different operation.
    A cancelled or callback-less Future remains UNKNOWN and is never released.
    """
    if future is None:
        handle.mark_submit_unknown()
        return False
    handle.mark_queued()
    try:
        handle.bind_future(future)
    except ToolWorkerAdmissionError:
        handle.mark_submit_unknown()
        raise
    if handle.parent is not None:
        # This slot predates admission custody.  Neither a plain Future nor an
        # independently created OwnedFuture proves our post-callback boundary:
        # a done callback can run before other accepted callbacks finish. Keep
        # the exact Future available to the tool awaiter, and retain UNKNOWN
        # under its parent instead of treating Future.done() as physical exit.
        handle.mark_submit_unknown()
        return True
    with handle._lock:
        if handle._future_observer_bound:
            return True
        handle._future_observer_bound = True

    def settle(done_future):
        try:
            if done_future.cancelled():
                handle.mark_submit_unknown()
                return
        except Exception:  # health: allow BLE001 -- an opaque Future cancellation probe must retain UNKNOWN custody
            handle.mark_submit_unknown()
            return
        handle.mark_body_done()
        handle.mark_finalizers_done()

    add_done_callback = getattr(future, "add_done_callback", None)
    if not callable(add_done_callback):
        handle.mark_submit_unknown()
        return False
    try:
        add_done_callback(settle)
    except Exception:  # health: allow BLE001 -- an opaque Future callback boundary must retain UNKNOWN custody
        handle.mark_submit_unknown()
        return False
    return True


__all__ = [
    "ToolWorkerAdmissionError",
    "ToolWorkerHandle",
    "ToolWorkerSubmissionError",
    "admit_tool_worker",
    "bind_existing_tool_worker_future",
    "bind_tool_worker_parent",
    "current_tool_worker_parent",
    "register_tool_worker_delegate",
    "submit_tool_worker",
    "tool_worker_parent_for",
]
