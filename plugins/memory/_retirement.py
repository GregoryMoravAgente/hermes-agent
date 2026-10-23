"""Private helpers for provider-owned hard-retirement receipts.

The Core retirement validator is intentionally not imported here: these bundled providers must
remain loadable on the frozen Core while they add the optional public capability.  The helpers
only construct the documented wire shape and compare a caller-supplied receipt with the identity
owned by the initialized provider instance.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from agent.memory_provider import spawn_context_thread


RECEIPT_KEYS = frozenset({"provider", "home_id", "incarnation"})
RESULT_KEYS = (
    "status",
    "provider",
    "home_id",
    "incarnation",
    "active_store_count",
    "pending_write_count",
    "pending_flush_count",
    "close_failure_count",
    "reason",
)
PREPARATION_KEYS = frozenset({"status", "receipt", "lease", "reason"})


def thread_is_closed(thread: object) -> bool:
    """Return true only after an admitted native thread is no longer running.

    Provider operation bodies can finish before the thread wrapper's native finalizer does.  A
    finish counter therefore cannot release the provider's custody lease.  Unstarted objects
    without a liveness API are the one definite pre-start case; an opaque live object remains
    unresolved so callers retain it for a later bounded join.
    """
    if thread is None or thread is threading.current_thread():
        return thread is None
    checker = getattr(thread, "is_alive", None)
    if not callable(checker):
        return getattr(thread, "ident", None) is None
    try:
        if checker():
            return False
    except (AttributeError, OSError, RuntimeError, TypeError):
        return False
    identity = getattr(thread, "ident", None)
    if identity is None:
        return True
    joiner = getattr(thread, "join", None)
    if not callable(joiner):
        return False
    try:
        joiner(timeout=0)
        return not checker()
    except (AttributeError, OSError, RuntimeError, TypeError):
        return False


def watch_thread_until_closed(thread: object, on_closed: Callable[[], None]) -> None:
    """Reconcile a live worker lease after its operation body has returned.

    The watcher owns no provider resource; it only joins the exact admitted thread and invokes
    the source-owned cleanup callback after the native thread has actually terminated.
    """
    joiner = getattr(thread, "join", None)
    if not callable(joiner):
        return

    def _watch() -> None:
        try:
            joiner()
        except (AttributeError, OSError, RuntimeError, TypeError):
            return
        if thread_is_closed(thread):
            on_closed()

    try:
        watcher = spawn_context_thread(_watch, name="provider-retirement-watch")
        watcher.start()
    except (OSError, RuntimeError):
        return


@dataclass(frozen=True, slots=True)
class ProviderIdentity:
    provider: str
    home_id: str
    incarnation: str

    @property
    def initialized(self) -> bool:
        return bool(self.provider and self.home_id and self.incarnation)

    def as_mapping(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "home_id": self.home_id,
            "incarnation": self.incarnation,
        }


class ProviderRetirementLease:
    """Opaque source-owned identity for one reversible provider retirement fence.

    The lease deliberately has no mapping or serialization protocol.  Core can carry the object
    returned by ``prepare_retirement`` back to the same provider, but it cannot manufacture a
    lease for another provider or incarnation.  Providers mutate only the phase flags below
    while holding their own retirement condition.
    """

    __slots__ = ("_commit_started", "_identity", "_prepared", "_resumed")

    def __init__(self, identity: ProviderIdentity):
        self._identity = identity
        self._prepared = False
        self._commit_started = False
        self._resumed = False


def preparation_result(
    identity: ProviderIdentity,
    *,
    status: str,
    lease: ProviderRetirementLease | None,
    reason: str,
) -> dict[str, object]:
    """Construct the exact reversible provider-preparation wire shape."""
    return {
        "status": status,
        "receipt": identity.as_mapping(),
        "lease": lease,
        "reason": reason,
    }


def new_identity(provider: str, home: str | Path | None) -> ProviderIdentity:
    """Create a source-owned identity for one initialized provider incarnation."""
    home_id = ""
    if home:
        home_id = str(Path(home).expanduser().resolve())
    return ProviderIdentity(provider, home_id, uuid4().hex if home_id else "")


def _valid_mapping(value: object) -> bool:
    return isinstance(value, Mapping) and set(value) == RECEIPT_KEYS and all(
        type(value[key]) is str for key in RECEIPT_KEYS
    )


def requested_identity(value: object, current: ProviderIdentity) -> ProviderIdentity:
    """Parse a requested receipt, falling back to current identity for malformed input."""
    if value is None:
        return current
    if not _valid_mapping(value):
        return current
    return ProviderIdentity(str(value["provider"]), str(value["home_id"]), str(value["incarnation"]))


def receipt_reason(value: object, current: ProviderIdentity) -> tuple[ProviderIdentity, str | None]:
    """Return the requested identity and a fail-closed reason, if it is not current."""
    requested = requested_identity(value, current)
    if not current.initialized:
        return requested, "not_initialized"
    if not _valid_mapping(value) and value is not None:
        return requested, "foreign_receipt"
    if requested.provider != current.provider or requested.home_id != current.home_id:
        return requested, "foreign_receipt"
    if requested.incarnation != current.incarnation:
        return requested, "stale_receipt"
    return requested, None


def retirement_result(
    identity: ProviderIdentity,
    *,
    status: str,
    reason: str,
    active_store_count: int = 0,
    pending_write_count: int = 0,
    pending_flush_count: int = 0,
    close_failure_count: int = 0,
) -> dict[str, object]:
    """Construct the exact provider result shape consumed by Core."""
    return {
        "status": status,
        "provider": identity.provider,
        "home_id": identity.home_id,
        "incarnation": identity.incarnation,
        "active_store_count": max(0, int(active_store_count)),
        "pending_write_count": max(0, int(pending_write_count)),
        "pending_flush_count": max(0, int(pending_flush_count)),
        "close_failure_count": max(0, int(close_failure_count)),
        "reason": reason,
    }
