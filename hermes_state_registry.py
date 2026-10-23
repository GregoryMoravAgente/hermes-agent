"""Process-wide shared SessionDB registry.

Each bare ``SessionDB()`` mints its own writer connection, lock, close-time WAL checkpoint
and token writer thread, and one connection's close-time checkpoint can race another's
growth (lost/reordered-page-write corruption). This module owns that boundary: one shared
``SessionDB`` per resolved path per process, refcounted, with generation-aware retirement
when the file is replaced (snapshot restore, recovery swap).

Lifecycle rules:
- ``acquire(path)`` returns the current generation for *path* and bumps its refcount.
- ``close()`` on a shared instance RELEASES one refcount instead of tearing the
  connection down: the registry owns the physical lifecycle and only closes on the
  final release, so legacy call sites return their reference instead of leaking it.
- ``release(db)`` decrements the generation *db was acquired from* (object-keyed, so an
  inode replacement cannot strand a still-owned generation); the final release of a
  retired generation tears it down.
- On inode change the old generation is RETIRED (never lent again) but stays alive until
  its holders release. If the replacement open fails the registry keeps NO path entry.
- All teardown happens OUTSIDE the registry lock: a final release's WAL checkpoint must
  never stall acquisition for every state.db.
- A final close/checkpoint is serialized with the next open for the same path; no new
  generation is published while the previous generation is still tearing down.
- A path can have SEVERAL closes admitted at once (the current generation's final release
  plus a retired generation's drain). The path barrier COUNTS them and is lifted only by
  the last one to settle, so neither ``acquire`` nor ``close_all`` / ``close_all_under``
  can escape while any handle for that path is still inside checkpoint/WAL-unlink.
- Maintenance callers borrow handles with a temporary registry reference instead of
  iterating an unpinned snapshot.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

from hermes_state_common import stat_db_file_identity as _stat_db_file_identity

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typed only
    from hermes_state import SessionDB

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionDbCloseReceipt:
    """Checked outcome for one physical SessionDB-handle sweep.

    Legacy close callers still receive their historical integer/``None`` results.  Lifecycle
    custody uses this receipt instead: a zero-failure, zero-pending receipt is the only evidence
    that every handle admitted to this sweep settled.  ``source`` is diagnostic only and never
    grants authority by itself.
    """

    source: str
    attempted: int
    succeeded: int
    failed: int = 0
    pending: int = 0

    @property
    def verified(self) -> bool:
        return (
            type(self.source) is str
            and bool(self.source)
            and type(self.attempted) is int
            and self.attempted >= 0
            and type(self.succeeded) is int
            and self.succeeded >= 0
            and type(self.failed) is int
            and self.failed >= 0
            and type(self.pending) is int
            and self.pending == 0
            and self.failed == 0
            and self.succeeded == self.attempted
        )

    @classmethod
    def combine(cls, source: str, receipts: list[SessionDbCloseReceipt]):
        """Combine checked sub-sweeps without treating a missing sub-sweep as success."""
        return cls(
            source=source,
            attempted=sum(receipt.attempted for receipt in receipts),
            succeeded=sum(receipt.succeeded for receipt in receipts),
            failed=sum(receipt.failed for receipt in receipts),
            pending=sum(receipt.pending for receipt in receipts),
        )


class _TeardownBarrier:
    """Accounting for every admitted-but-unfinished physical close of one path.

    A path can own more than one close at a time: the current generation's final
    release and a retired generation's drain are admitted independently under
    ``_lock`` and only meet at the lifecycle mutex. One event per path is honest
    only if the LAST admitted teardown settles it. Signalling on the first lets
    ``close_all()`` return and ``acquire()`` publish a replacement while an older
    handle is still inside ``PRAGMA wal_checkpoint``/sidecar unlink -- the exact
    overlap (#102827) this registry exists to forbid.
    """

    __slots__ = ("admitted", "completed", "event", "failures", "handles", "pending")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.pending = 0
        self.admitted = 0
        self.completed = 0
        self.failures = 0
        # Strong references to the exact handles admitted through this barrier.  A checked
        # sweep that meets a legacy in-flight close can inspect its source-owned receipt after
        # the event instead of treating an opaque successful return as proof.
        self.handles: list[tuple[SessionDB, bool]] = []


class _Generation:
    """One shared SessionDB generation: instance, refcount, file identity."""

    __slots__ = ("db", "identity", "path", "refcount", "retired")

    def __init__(self, path: Path, db: SessionDB, identity: Optional[tuple[int, int]]) -> None:
        self.path = path
        self.db = db
        self.refcount = 1
        self.identity = identity
        self.retired = False


_lock = threading.Lock()
# path → live generation; retired generations move to _retired (keyed by id(db)) until
# their last holder releases.
_generations: dict[Path, _Generation] = {}
_retired: dict[int, _Generation] = {}
# Paths whose next generation is being constructed. Construction runs outside _lock
# (schema reconciliation can take seconds), but peers for the SAME file must wait or
# every cold caller opens its own writer before a winner is chosen.
_opening: dict[Path, threading.Event] = {}
# A final close/checkpoint must finish before a replacement writer is opened
# for the same path. The barrier is admitted while holding _lock and lifted
# only after the LAST admitted physical teardown, so acquire cannot slip
# through the generation-removal/open gap and close_all cannot report a
# finished sweep over a close that is still running.
_tearing_down: dict[Path, _TeardownBarrier] = {}
# A failed physical close remains attached to its exact handle.  It is deliberately separate from
# live/retired generations: the path must not lend this object again, while the next checked sweep
# must retry this object rather than replacing it with an aggregate process-wide failure bit.
_failed_generations: dict[int, _Generation] = {}
# Open and close are both performed outside _lock. This per-path mutex closes
# the race between checking _tearing_down and entering sqlite3.connect(),
# including retired-generation drains after an inode replacement.
_path_lifecycle_locks: dict[Path, threading.Lock] = {}
_CHECKED_TEARDOWN_MARKER = "_registry_checked_teardown_required"


def _open_session_db(path: Path) -> SessionDB:
    """Construct the SessionDB for *path* (call-time import avoids cycles; tests patch this)."""
    from hermes_state import SessionDB

    return SessionDB(db_path=path)


def _teardown(db: SessionDB) -> bool:
    """Close a shared instance, clearing its registry-owned flag first."""
    checked = getattr(db, _CHECKED_TEARDOWN_MARKER, False) is True
    was_registry_owned = getattr(db, "_shared_registry_owned", False) is True
    with contextlib.suppress(Exception):
        db._shared_registry_owned = False
    succeeded = _close_quietly(db, "Error closing shared SessionDB", checked=checked)
    if not succeeded and was_registry_owned:
        # A failed close leaves the physical handle in custody.  Restore the ownership marker so
        # cache callers do not treat it as an unrelated bare handle while the exact generation is
        # retained for a checked retry.
        with contextlib.suppress(Exception):
            db._shared_registry_owned = True
    return succeeded


def _invoke_teardown(db: SessionDB, *, checked: bool) -> bool:
    """Call the one-argument teardown seam with an explicit checked receipt requirement."""
    sentinel = object()
    previous = getattr(db, _CHECKED_TEARDOWN_MARKER, sentinel)
    try:
        setattr(db, _CHECKED_TEARDOWN_MARKER, checked)
        return _teardown(db)
    finally:
        with contextlib.suppress(Exception):
            if previous is sentinel:
                delattr(db, _CHECKED_TEARDOWN_MARKER)
            else:
                setattr(db, _CHECKED_TEARDOWN_MARKER, previous)


def _close_quietly(db: SessionDB, debug_message: str, *, checked: bool = False) -> bool:
    """close() that never propagates. A lost WAL generation whose capture failed is data at risk,
    not teardown noise: the handle stays open and the operator has to act, so that one surfaces."""
    prover = getattr(db, "_checked_close_proven", None)
    if checked and not callable(prover):
        logger.debug("Checked SessionDB close has no source-owned physical receipt for %s", _db_path_of(db))
        return False
    marker_name = _CHECKED_TEARDOWN_MARKER
    marker_sentinel = object()
    previous_marker = marker_sentinel
    if checked:
        # The fallback path is used for bare handles that are not registry-managed.  It still
        # needs to enter the source-owned checked close mode; otherwise SessionDB.close() can
        # release the exact native connection while a token writer is in-flight and reopen it.
        previous_marker = getattr(db, marker_name, marker_sentinel)
        with contextlib.suppress(Exception):
            setattr(db, marker_name, True)
    if callable(prover):
        # An unchecked test seam returning None must not inherit a prior successful close marker.
        with contextlib.suppress(Exception):
            db._last_native_close_succeeded = None
    try:
        try:
            db.close()
        except Exception as exc:
            from hermes_state_dbfile import RetiredGenerationCaptureError
            if isinstance(exc, RetiredGenerationCaptureError):
                logger.error("SessionDB for %s did not settle at close: %s", _db_path_of(db), exc)
            else:
                logger.debug(debug_message, exc_info=True)
            return False
        if callable(prover):
            try:
                if prover() is not True:
                    logger.debug("SessionDB for %s returned without proving native closure", _db_path_of(db))
                    return False
            except Exception:
                logger.debug("SessionDB for %s did not expose a usable close receipt", _db_path_of(db), exc_info=True)
                return False
        return True
    finally:
        if checked:
            with contextlib.suppress(Exception):
                if previous_marker is marker_sentinel:
                    delattr(db, marker_name)
                else:
                    setattr(db, marker_name, previous_marker)


def _path_lifecycle_lock_locked(path: Path) -> threading.Lock:
    """Return the lifecycle mutex for *path* (caller holds ``_lock``)."""
    lock = _path_lifecycle_locks.get(path)
    if lock is None:
        lock = threading.Lock()
        _path_lifecycle_locks[path] = lock
    return lock


def _admit_teardown_locked(
    path: Path,
    db: Optional[SessionDB] = None,
    *,
    checked: bool = False,
) -> _TeardownBarrier:
    """Register one pending physical close for *path* (caller holds ``_lock``).

    Admission shares the lock section that removes the generation, so a peer
    release, ``acquire`` or ``close_all`` taking the lock next always sees this
    teardown accounted for.
    """
    barrier = _tearing_down.get(path)
    if barrier is None:
        barrier = _tearing_down[path] = _TeardownBarrier()
    barrier.pending += 1
    barrier.admitted += 1
    if db is not None:
        barrier.handles.append((db, checked))
    return barrier


def _finish_teardown(path: Path, barrier: _TeardownBarrier, *, succeeded: bool = True) -> None:
    """Settle one admitted teardown; only the last one lifts the path barrier."""
    with _lock:
        barrier.pending -= 1
        barrier.completed += 1
        if not succeeded:
            barrier.failures += 1
        if barrier.pending > 0:
            return
        if _tearing_down.get(path) is barrier:
            _tearing_down.pop(path, None)
        barrier.event.set()


def _teardown_generation(
    path: Path,
    db: SessionDB,
    *,
    barrier: Optional[_TeardownBarrier] = None,
    checked: bool = False,
) -> bool:
    """Close *db* under its path lifecycle mutex, then settle its barrier slot."""
    with _lock:
        lifecycle_lock = _path_lifecycle_lock_locked(path)
    succeeded = False
    try:
        with lifecycle_lock:
            result = _invoke_teardown(db, checked=checked)
            # Checked lifecycle custody needs literal source-owned proof.  A test seam or wrapper
            # returning None is unknown and must retain the exact handle for a later retry.
            succeeded = result is True
        return succeeded
    finally:
        if succeeded:
            _forget_failed_generation(db)
        else:
            _retain_failed_generation(path, db)
        if barrier is not None:
            _finish_teardown(path, barrier, succeeded=succeeded)


def _retain_failed_generation(path: Path, db: SessionDB) -> _Generation:
    """Retain the exact physical handle whose teardown was not proven successful."""
    key = id(db)
    with _lock:
        generation = _failed_generations.get(key)
        if generation is None:
            generation = _retired.get(key)
        if generation is None:
            candidate = _generations.get(path)
            if candidate is not None and candidate.db is db:
                generation = candidate
        if generation is None:
            generation = _Generation(path, db, _stat_db_file_identity(path))
        generation.retired = True
        generation.refcount = 0
        current = _generations.get(path)
        if current is not None and current.db is db:
            # A retired handle may fail after an inode replacement has already
            # registered a successor. Retaining the old native handle must not
            # remove that distinct live generation from the registry.
            _generations.pop(path, None)
        _retired.pop(key, None)
        _failed_generations[key] = generation
        return generation


def _forget_failed_generation(db: SessionDB) -> None:
    """Drop a failed-generation record only after its exact handle closes successfully."""
    with _lock:
        _failed_generations.pop(id(db), None)


def _failed_for_path_locked(path: Path) -> list[_Generation]:
    return [generation for generation in _failed_generations.values() if generation.path == path]


def _failed_handle_is_closed(generation: _Generation) -> bool:
    """Drop a failed record only after its source-owned native receipt proves closure."""
    prover = getattr(generation.db, "_checked_close_proven", None)
    if callable(prover):
        try:
            return prover() is True
        except Exception:  # health: allow BLE001 -- a failed checked receipt must retain custody
            return False
    # Legacy test doubles have no source-owned receipt; retain the old inert-handle fallback
    # only for those non-SessionDB objects.
    return hasattr(generation.db, "_conn") and getattr(generation.db, "_conn", object()) is None


def _db_path_of(db: SessionDB) -> Optional[Path]:
    """``Path(db.db_path)`` or None when absent/unconvertible."""
    path = getattr(db, "db_path", None)
    try:
        return None if path is None else Path(path)
    except (TypeError, ValueError):
        return None


def _finish_opening(path: Path, opening: threading.Event) -> None:
    """Drop the per-path construction marker and wake waiters (caller holds _lock)."""
    if _opening.get(path) is opening:
        _opening.pop(path, None)
    opening.set()


def acquire(db_path: Optional[Path] = None) -> SessionDB:
    """Return the shared SessionDB for *db_path*, incrementing its refcount. If the file was
    replaced (different inode) since the generation opened, that generation is RETIRED
    but stays alive for its holders, and a fresh one is opened in its place. Raises
    whatever ``SessionDB.__init__`` raises; on a replacement-open failure the registry
    holds NO entry for the path."""
    from hermes_state import _default_db_path

    raw_path = Path(db_path) if db_path is not None else Path(_default_db_path())
    try:
        path = raw_path.resolve()
    except OSError:
        path = raw_path

    while True:
        wait_for: Optional[threading.Event] = None
        with _lock:
            failed = _failed_for_path_locked(path)
            if failed:
                # A failed wrapper may have closed the underlying connection before raising.  It
                # is safe to discard that inert record; an open failed handle must block a new
                # writer until a checked lifecycle retry settles the exact object.
                for generation in failed:
                    if _failed_handle_is_closed(generation):
                        _failed_generations.pop(id(generation.db), None)
                failed = _failed_for_path_locked(path)
                if failed:
                    raise RuntimeError(
                        f"SessionDB close is unverified for {path}; retry checked teardown before acquire"
                    )
            generation = _generations.get(path)
            if generation is not None:
                current = _stat_db_file_identity(path)
                if current is not None and generation.identity is not None and current != generation.identity:
                    # File replaced: retire this generation so it is never lent again, then elect one
                    # caller to open the replacement. It stays alive for its holders, tracked in
                    # ``_retired`` by ``id(db)`` so their releases find it after the path remaps.
                    generation.retired = True
                    del _generations[path]
                    _retired[id(generation.db)] = generation
                else:
                    generation.refcount += 1
                    return generation.db
            teardown = _tearing_down.get(path)
            if teardown is not None:
                wait_for = teardown.event
            else:
                opening = _opening.get(path)
                if opening is None:
                    opening = _opening[path] = threading.Event()
                    lifecycle_lock = _path_lifecycle_lock_locked(path)
                else:
                    wait_for = opening
        if wait_for is not None:
            # Another caller is constructing or closing this path; wait without holding the
            # global lock. A failed opener signals too, so a waiter can retry.
            wait_for.wait()
            continue

        # Open OUTSIDE the registry lock; the per-path marker prevents redundant writers without
        # serialising other files, the lifecycle mutex keeps the open off a same-path close.
        try:
            with lifecycle_lock:
                db = _open_session_db(path)
                db._shared_registry_owned = True
                identity = _stat_db_file_identity(path)
        except BaseException:
            with _lock:
                _finish_opening(path, opening)
            raise

        with _lock:
            teardown = _tearing_down.get(path)
            if teardown is None:
                existing = _generations.get(path)
                if existing is not None:  # Defensive: installed by explicit registry manipulation mid-open.
                    existing.refcount += 1
                    winner = existing.db
                else:
                    _generations[path] = _Generation(path, db, identity)
                    winner = db
            _finish_opening(path, opening)
        if teardown is not None:
            # A shutdown or retired-generation final release was admitted while this opener was
            # constructing: never publish into that window — discard the speculative handle and
            # go round again once the barrier lifts.
            _teardown_generation(path, db)
            teardown.event.wait()
            continue
        if winner is not db:
            _teardown_generation(path, db)
        return winner


def _release_impl(db: SessionDB, *, checked: bool = False) -> tuple[bool, bool, bool]:
    """Release one reference and return ``(was_managed, physical_close_succeeded, attempted)``."""
    if db is None:
        return False, False, False
    key = id(db)
    teardown_barrier: Optional[_TeardownBarrier] = None
    with _lock:
        generation = _failed_generations.get(key)
        retry_failed = generation is not None
        if generation is None:
            generation = _retired.get(key)
        if generation is None:
            path = _db_path_of(db)
            if path is None:
                return False, False, False
            generation = _generations.get(path)
            if generation is None or generation.db is not db:
                # Not shared (bare SessionDB()); the caller owns close().
                return False, False, False
        needs_teardown = retry_failed
        if not retry_failed:
            generation.refcount -= 1
            needs_teardown = generation.refcount <= 0
        if needs_teardown:
            if not retry_failed:
                if generation.retired:
                    _retired.pop(key, None)
                elif _generations.get(generation.path) is generation:
                    _generations.pop(generation.path, None)
            # A retired generation's drain is admitted too: it checkpoints and unlinks the same
            # sidecars as the current one, so a replacement writer must not open on top of it.
            teardown_barrier = _admit_teardown_locked(
                generation.path, db, checked=checked,
            )
    # Teardown OUTSIDE the lock: stopping the token writer, WAL checkpoint and read-pool
    # drain must not block acquisition for every other state.db.
    close_succeeded = True
    if needs_teardown:
        if checked:
            close_succeeded = _teardown_generation(
                generation.path, db, barrier=teardown_barrier, checked=True,
            )
        else:
            # Keep the legacy one-argument seam patchable by existing callers/tests.
            close_succeeded = _teardown_generation(generation.path, db, barrier=teardown_barrier)
    return True, close_succeeded, needs_teardown


def release(db: SessionDB) -> bool:
    """Decrement the refcount of a shared SessionDB. ``True`` if *db* was shared; ``False``
    if it is not registry-managed (caller owns close()). The final release tears the
    generation down OUTSIDE the registry lock. Lookup is object-keyed, so holders of an
    old generation release into its retired record, not into whatever the path names."""
    managed, _close_succeeded, _attempted = _release_impl(db)
    return managed


def release_checked(db: SessionDB) -> SessionDbCloseReceipt:
    """Release one registry reference and report a checked physical-close outcome when final."""
    managed, close_succeeded, attempted = _release_impl(db, checked=True)
    if not managed:
        return SessionDbCloseReceipt("hermes_state_registry.release_checked", 0, 0)
    if not attempted:
        # A checked release that only returned a shared reference has not physically closed
        # anything yet. Mark the still-live generation pending instead of reporting a vacuous
        # zero-failure receipt as verified.
        return SessionDbCloseReceipt(
            "hermes_state_registry.release_checked",
            attempted=1,
            succeeded=0,
            pending=1,
        )
    return SessionDbCloseReceipt(
        "hermes_state_registry.release_checked",
        attempted=1,
        succeeded=1 if close_succeeded else 0,
        failed=0 if close_succeeded else 1,
    )


def _path_is_under(path: Path, root: Path) -> bool:
    """True when *path* is *root* or a file inside it (normcase, resolved)."""
    try:
        path_key = os.path.normcase(str(path.resolve()))
        root_key = os.path.normcase(str(root.resolve()))
    except OSError:
        path_key = os.path.normcase(str(path))
        root_key = os.path.normcase(str(root))
    return path_key == root_key or path_key.startswith(root_key + os.sep)


def _teardown_swept_generations(
    generations: list[_Generation],
    teardown_barriers: dict[Path, _TeardownBarrier],
    active_teardowns: list[_TeardownBarrier],
) -> int:
    """Close *generations* outside the registry lock; wait for already-admitted teardowns."""
    by_path: dict[Path, list[_Generation]] = {}
    for generation in generations:
        by_path.setdefault(generation.path, []).append(generation)
    for path, path_generations in by_path.items():
        with _lock:
            lifecycle_lock = _path_lifecycle_lock_locked(path)
        path_failed = False
        try:
            with lifecycle_lock:
                for generation in path_generations:
                    if _invoke_teardown(generation.db, checked=False) is not True:
                        path_failed = True
                        _retain_failed_generation(path, generation.db)
                    else:
                        _forget_failed_generation(generation.db)
        except BaseException:
            path_failed = True
            raise
        finally:
            _finish_teardown(path, teardown_barriers[path], succeeded=not path_failed)
    for barrier in active_teardowns:
        barrier.event.wait()
    return len(generations)


def _teardown_swept_generations_checked(
    generations: list[_Generation],
    teardown_barriers: dict[Path, _TeardownBarrier],
    active_teardowns: list[_TeardownBarrier],
) -> SessionDbCloseReceipt:
    """Close swept generations and retain failures from concurrent admitted teardowns."""
    attempted = len(generations)
    failed = 0
    by_path: dict[Path, list[_Generation]] = {}
    for generation in generations:
        by_path.setdefault(generation.path, []).append(generation)
    for path, path_generations in by_path.items():
        with _lock:
            lifecycle_lock = _path_lifecycle_lock_locked(path)
        path_failed = 0
        try:
            with lifecycle_lock:
                for generation in path_generations:
                    if _invoke_teardown(generation.db, checked=True) is not True:
                        path_failed += 1
                        _retain_failed_generation(path, generation.db)
                    else:
                        _forget_failed_generation(generation.db)
        except BaseException:
            path_failed += 1
            raise
        finally:
            _finish_teardown(path, teardown_barriers[path], succeeded=path_failed == 0)
        failed += path_failed
    for barrier in active_teardowns:
        barrier.event.wait()
        attempted += barrier.admitted
        active_failed = barrier.failures
        if active_failed == 0:
            if not barrier.handles:
                # An opaque admitted close has no source-owned object to inspect.
                active_failed += barrier.admitted
            else:
                for db, admitted_checked in barrier.handles:
                    if admitted_checked:
                        continue
                    prover = getattr(db, "_checked_close_proven", None)
                    try:
                        proven = callable(prover) and prover() is True
                    except Exception:  # health: allow BLE001 -- opaque legacy close stays unverified
                        proven = False
                    if not proven:
                        active_failed += 1
                        path = _db_path_of(db)
                        if path is not None:
                            _retain_failed_generation(path, db)
        failed += active_failed
    return SessionDbCloseReceipt(
        source="hermes_state_registry.close_all_checked",
        attempted=attempted,
        succeeded=max(0, attempted - failed),
        failed=failed,
    )


def close_all() -> int:
    """Close every shared SessionDB regardless of refcount; returns the count. For gateway
    shutdown, after all agents and cron jobs finished. Idempotent."""
    teardown_barriers: dict[Path, _TeardownBarrier] = {}
    with _lock:
        active_teardowns = list(_tearing_down.values())
        generations = (
            list(_generations.values())
            + list(_retired.values())
            + list(_failed_generations.values())
        )
        unique: dict[int, _Generation] = {}
        generations = [unique.setdefault(id(generation.db), generation) for generation in generations]
        for path in {generation.path for generation in generations}:
            barrier = teardown_barriers[path] = _admit_teardown_locked(path)
            barrier.handles.extend(
                (generation.db, False) for generation in generations if generation.path == path
            )
        _generations.clear()
        _retired.clear()
        for generation in generations:
            generation.retired = True
    return _teardown_swept_generations(generations, teardown_barriers, active_teardowns)


def close_all_checked() -> SessionDbCloseReceipt:
    """Close every shared SessionDB and return a fail-closed physical receipt.

    ``close_all()`` remains the compatibility API.  Lifecycle custody must call this checked
    variant because a suppressed close exception, or an already-admitted close racing the sweep,
    cannot be represented by the legacy integer count.
    """
    teardown_barriers: dict[Path, _TeardownBarrier] = {}
    with _lock:
        active_teardowns = list(_tearing_down.values())
        generations = (
            list(_generations.values())
            + list(_retired.values())
            + list(_failed_generations.values())
        )
        unique: dict[int, _Generation] = {}
        generations = [unique.setdefault(id(generation.db), generation) for generation in generations]
        for path in {generation.path for generation in generations}:
            barrier = teardown_barriers[path] = _admit_teardown_locked(path)
            barrier.handles.extend(
                (generation.db, True) for generation in generations if generation.path == path
            )
        _generations.clear()
        _retired.clear()
        for generation in generations:
            generation.retired = True
    return _teardown_swept_generations_checked(generations, teardown_barriers, active_teardowns)


def close_all_under(directory: str | Path) -> int:
    """Force-close every shared SessionDB whose file lives under *directory*; returns the count.

    Profile delete rmtree (and a same-name recreate) fails while this process still holds
    ``state.db``. Same contract as ``MemoryStore.release_all_under``: a live holder is
    expected to fail afterward; a process that holds none is a no-op returning 0.

    A final ``release()`` can drop the generation and admit teardown before the physical
    close finishes. Wait for those directory-matching barriers even when no generation
    remains, otherwise rmtree still sees the open handle.
    """
    try:
        root = Path(directory).expanduser().resolve()
    except OSError:
        root = Path(directory).expanduser()
    teardown_barriers: dict[Path, _TeardownBarrier] = {}
    with _lock:
        generations = [
            generation
            for generation in (
                list(_generations.values())
                + list(_retired.values())
                + list(_failed_generations.values())
            )
            if _path_is_under(generation.path, root)
        ]
        unique: dict[int, _Generation] = {}
        generations = [unique.setdefault(id(generation.db), generation) for generation in generations]
        # Collect by directory, not by remaining generations: a last release already
        # popped the generation and left only ``_tearing_down``.
        active_teardowns = [
            barrier for path, barrier in _tearing_down.items()
            if _path_is_under(path, root)
        ]
        selected_paths = {generation.path for generation in generations}
        for path in selected_paths:
            barrier = teardown_barriers[path] = _admit_teardown_locked(path)
            barrier.handles.extend(
                (generation.db, False) for generation in generations if generation.path == path
            )
        for generation in generations:
            generation.retired = True
            if _generations.get(generation.path) is generation:
                _generations.pop(generation.path, None)
            _retired.pop(id(generation.db), None)
    return _teardown_swept_generations(generations, teardown_barriers, active_teardowns)


def other_generations_for_path(
    db_path: Path, *, exclude: Optional[SessionDB] = None
) -> list[str]:
    """Describe every other held SessionDB generation THIS process has for *db_path*.

    The registry is path-keyed, so it can answer the in-process half of "is this store quiet?"
    that a ``/proc`` descriptor scan structurally cannot: that scan skips our own pid, so it only
    ever proves other PROCESSES are away.

    RETIRED generations are deliberately not holders here. A generation is retired only after its
    file was replaced, which is exactly when ``SessionDB`` fences it: every write raises
    ``StateDbReplacedError`` and the close-time checkpoint is disabled, so it is not the live writer
    this gate protects. It also leaves ``_retired`` only when its last holder releases, and a
    gateway handle does not release before shutdown — counting it made ONE inode replacement
    (recovery swap, backup restore, snapshot) skip auto-VACUUM for that path for the rest of the
    process lifetime, turning the unbounded growth this maintenance exists to bound into a
    permanent condition. Failed checked closes are different: they remain in
    ``_failed_generations`` precisely because their native handle is still unverified and may
    still be open, so maintenance must count them even though the record is marked retired.
    """
    try:
        path = Path(db_path).resolve()
    except OSError:
        path = Path(db_path)
    with _lock:
        live = [generation for generation in _generations.values() if not generation.retired]
        failed = list(_failed_generations.values())
        return [
            f"in-process live SessionDB generation (refcount {generation.refcount})"
            for generation in (*live, *failed)
            if generation.db is not exclude and generation.path == path
        ]


def live_shared_session_dbs() -> list[SessionDB]:
    """Snapshot of every live (non-retired) shared SessionDB (refcounts untouched), for
    in-process maintenance. A concurrent final release may close an instance, in which
    case the callee sees ``_conn is None``."""
    with _lock:
        return [g.db for g in _generations.values() if not g.retired]


@contextlib.contextmanager
def borrow_live_shared_session_dbs() -> Iterator[list[SessionDB]]:
    """Borrow live handles with registry references pinned for the whole block.

    Maintenance must not operate on the unowned snapshot returned by
    :func:`live_shared_session_dbs`: the last real owner could otherwise
    release and physically close the connection between the snapshot and the
    maintenance call. Each borrowed generation gets one temporary reference;
    the ``finally`` block releases it even when maintenance raises.
    """
    with _lock:
        borrowed_generations = [
            generation for generation in _generations.values() if not generation.retired
        ]
        borrowed = [generation.db for generation in borrowed_generations]
        for generation in borrowed_generations:
            generation.refcount += 1
    try:
        yield borrowed
    finally:
        for db in reversed(borrowed):
            release(db)


def stats() -> dict[str, int]:
    """Registry census for tests and diagnostics (no locks held long)."""
    with _lock:
        return {
            "live_generations": len(_generations), "retired_generations": len(_retired),
            "total_refcounts": sum(g.refcount for g in _generations.values()),
        }


def release_or_close(db: SessionDB) -> None:
    """Release a shared instance, or close it when it is not registry-managed. Drop-in for a
    plain ``db.close()``: read-only opens, CLI one-shots and test fakes fall back."""
    if not release(db):
        _close_quietly(db, "release_or_close fallback close failed")


def release_or_close_checked(db: SessionDB) -> SessionDbCloseReceipt:
    """Checked counterpart used by lifecycle shutdown handle sweeps."""
    receipt = release_checked(db)
    if receipt.attempted:
        return receipt
    if db is None:
        return SessionDbCloseReceipt("hermes_state_registry.release_or_close_checked", 0, 0, failed=1)
    succeeded = _close_quietly(db, "release_or_close fallback close failed", checked=True)
    return SessionDbCloseReceipt(
        "hermes_state_registry.release_or_close_checked",
        attempted=1,
        succeeded=1 if succeeded else 0,
        failed=0 if succeeded else 1,
    )
