"""Nonblocking, all-or-nothing reservations of canonical profile homes.

A held lock inode is never removed: unlinking it creates a second owner. The one exception is a
stale inode this user cannot write (left by a gateway that ran as root, #42685) that a flock through
a read-only descriptor proves unheld; it is replaced once, and every winner checks that the path
still names the inode it locked.
"""
from __future__ import annotations

from contextlib import contextmanager, suppress
import copy
from dataclasses import dataclass
import enum
import json
import os
from pathlib import Path
import stat
import threading
import uuid
from hermes_dir_acl import make_user_dir


class OwnershipConflict(RuntimeError):
    pass


class ProfileReleaseOutcome(enum.Enum):
    RELEASED = "released"
    ABSENT = "absent"
    UNKNOWN_LOST_EXCLUSION = "unknown-lost-exclusion"


@dataclass(frozen=True)
class ProfileReleaseSummary:
    """Per-home cleanup outcomes that remain actionable by the caller."""

    outcomes: dict[Path, ProfileReleaseOutcome]
    writers_alive: bool = False

    @property
    def failed(self) -> bool:
        return self.writers_alive or any(
            outcome is not ProfileReleaseOutcome.RELEASED for outcome in self.outcomes.values())

    def describe(self) -> str:
        details = ", ".join(f"{home}: {outcome.value}" for home, outcome in self.outcomes.items())
        if self.writers_alive:
            details = f"{details}, writers_alive" if details else "writers_alive"
        return details or "released"


@dataclass(frozen=True)
class ProfileLineage:
    """Typed relationship between a new profile origin and its current gateway owner."""

    status: str
    reason: str = ""
    generation: str | None = None
    birth_binding: tuple[int, int, int | None] | None = None
    owner_epoch: str | None = None
    record_binding: tuple[object, ...] | None = None
    prior_lock_present: bool = False

    @property
    def is_authenticated_new_profile(self) -> bool:
        return self.status == "new_profile_started"


class OwnershipReleaseError(OwnershipConflict):
    """A reservation cleanup did not prove that every OS exclusion was released."""

    def __init__(self, message: str, summary: ProfileReleaseSummary):
        self.summary = summary
        super().__init__(message)


def canonical_home(home: Path) -> Path:
    from hermes_cli.profile_provenance_bootstrap import ProfileProvenanceError, _safe_path

    try:
        safe_home = _safe_path(home, label="profile home")
    except ProfileProvenanceError as exc:
        raise OwnershipConflict(f"Gateway runtime ownership refused for linked profile {home}") from exc
    return Path(os.path.normcase(str(safe_home)))


def tighten_lock_mode(fd: int) -> None:
    """Validate an open ``gateway.lock`` descriptor and narrow an upgraded lock to 0600.

    Main's ``acquire_gateway_runtime_lock`` created the inode with ``open(path, "a+")`` under the
    umask (0644 on most hosts), and lock inodes are never replaced, so an upgraded home keeps that
    mode forever and discovery refuses it. Only a regular, single-link inode this user owns is
    chmodded, through the already-open NOFOLLOW descriptor (no path race); anything else is refused.
    """
    info = os.fstat(fd)
    if os.name == 'nt':
        if not stat.S_ISREG(info.st_mode):
            raise PermissionError('unsafe gateway lock owner or type')
        return
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():  # windows-footgun: ok — POSIX branch
        raise PermissionError('unsafe gateway lock owner or type')
    if stat.S_IMODE(info.st_mode) & 0o077:
        if info.st_nlink != 1:
            raise PermissionError('unsafe gateway lock link count')
        os.fchmod(fd, 0o600)  # windows-footgun: ok — POSIX branch


_LOCK_FLAGS = os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0)


def _open_checked(path: Path, flags: int) -> int:
    fd = os.open(path, flags, 0o600)
    try:
        tighten_lock_mode(fd)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _unlink_stale_lock(path: Path) -> bool:
    """Unlink a ``gateway.lock`` this user cannot write (another uid's) once a flock taken through a
    read-only descriptor proves it unheld. Symlinks, hardlinks, non-regular files, a home whose
    ``gateway.pid`` names a live process and anything not provably unheld are left in place (False).
    An inode this user cannot even open is refused with the recovery command: an unheld snapshot
    (``/proc/locks``) does not stop a holder locking it between the read and the unlink."""
    from gateway.status import _live_pid_from_record, _read_pid_record
    if os.name == 'nt':
        return False  # a sharing violation is a live holder there
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        return False
    if _live_pid_from_record(_read_pid_record(path.with_name('gateway.pid'))) is not None:
        return False
    try:
        probe = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK)
    except PermissionError:
        raise PermissionError(foreign_lock_recovery(path)) from None
    import fcntl
    try:
        if not _same_inode(os.fstat(probe), info):
            return False
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        # Unlinked while still locked: a late locker of the old inode never shares our lock.
        return _unlink_if_same(path, info)
    finally:
        os.close(probe)


def foreign_lock_recovery(path: Path) -> str:
    return (f'foreign_stale_lock: {path} is not readable by this user (left by a gateway run as root?) '
            f'and cannot be proven unheld. If `sudo lsof {path}` lists no process, run `sudo rm {path}`.')


def unreadable_lock_recovery(path: Path) -> str | None:
    """The recovery a client prints for a ``gateway.lock`` it cannot even open read-only."""
    try:
        os.close(os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK))
    except PermissionError:
        return foreign_lock_recovery(path)
    except OSError:
        pass
    return None


def _unlink_if_same(path: Path, info) -> bool:
    if not _same_inode(os.lstat(path), info):
        return False
    os.unlink(path)
    return True


def _same_inode(left, right) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _open_lock(path: Path) -> int:
    """Descriptor for *path*, replacing a provably stale inode this user cannot use once."""
    try:
        return _open_checked(path, _LOCK_FLAGS)
    except PermissionError:
        if not _unlink_stale_lock(path):
            raise
    # A racer may create the fresh inode first; both then contend for its flock as usual.
    return _open_checked(path, _LOCK_FLAGS)


def _still_names(path: Path, fd: int) -> bool:
    """True when *path* is still the inode *fd* locked (a stale-lock replacement unlinks it)."""
    if os.name == 'nt':
        return True
    try:
        return _same_inode(os.lstat(path), os.fstat(fd))
    except FileNotFoundError:
        return False


class ProfileOwnership:
    def __init__(self):
        self._handles: dict[Path, object] = {}
        self._unknown_homes: set[Path] = set()
        self._owner_epochs: dict[Path, str] = {}
        self._owner_record_bindings: dict[Path, tuple[object, str, tuple[object, ...], int]] = {}
        self._predecessor_records: dict[Path, dict | None] = {}
        self._lineages: dict[Path, ProfileLineage] = {}
        # Object-bound custody already used by the finalizer path.  An ambiguous physical
        # close keeps the same profile handle until that exact token is terminal; this is not
        # a second ledger and a successor cannot satisfy it with a copied object.
        self._finalizer_holds: dict[Path, object] = {}
        self._writers: list[threading.Thread] = []
        self._mutex = threading.RLock()

    @property
    def homes(self) -> tuple[Path, ...]:
        with self._mutex:
            return tuple(self._handles)

    def _record_reservation_lineage(
        self, home: Path, origin, owner_epoch: str, prior_lock: bool, record: dict,
    ) -> None:
        from hermes_cli.profile_provenance import mark_profile_started

        if origin.is_new_profile and not prior_lock:
            try:
                started = mark_profile_started(
                    home, owner_epoch=owner_epoch,
                    record_binding=(
                        record.get('owner_epoch'), record.get('pid'), record.get('kind'),
                        record.get('start_time'), record.get('hermes_home'),
                    ),
                )
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                self._lineages[home] = ProfileLineage(
                    "unknown", "origin_transition_failed", owner_epoch=owner_epoch,
                    prior_lock_present=prior_lock,
                )
                raise OwnershipConflict(
                    f"Profile origin transition was not authenticated for {home}"
                ) from exc
            else:
                if started.is_authenticated:
                    self._lineages[home] = ProfileLineage(
                        "new_profile_started",
                        generation=started.generation,
                        birth_binding=started.birth_binding,
                        owner_epoch=owner_epoch,
                        record_binding=started.started_record_binding,
                        prior_lock_present=prior_lock,
                    )
                else:
                    self._lineages[home] = ProfileLineage(
                        "unknown", "origin_transition_unproven",
                        owner_epoch=owner_epoch, prior_lock_present=prior_lock,
                    )
                    raise OwnershipConflict(
                        f"Profile origin transition was not proven for {home}"
                    )
        elif origin.status == "valid" and origin.state == "started":
            self._lineages[home] = ProfileLineage(
                "existing_profile_started", "prior_origin_started",
                generation=origin.generation, birth_binding=origin.birth_binding,
                owner_epoch=owner_epoch, prior_lock_present=prior_lock,
            )
        elif origin.status == "valid" and origin.state == "never_started":
            self._lineages[home] = ProfileLineage(
                "unknown", "prior_lock_before_first_start",
                generation=origin.generation, birth_binding=origin.birth_binding,
                owner_epoch=owner_epoch, prior_lock_present=prior_lock,
            )
        else:
            self._lineages[home] = ProfileLineage(
                "unknown", origin.reason or "origin_unavailable",
                owner_epoch=owner_epoch, prior_lock_present=prior_lock,
            )

    def _publish_reservation(
        self, home: Path, handle, origin, owner_epoch: str, prior_lock_present: bool,
        purpose: str | None, *, record: dict | None = None, lineage_prepared: bool = False,
    ) -> None:
        from gateway.status import _build_pid_record, _read_gateway_lock_record

        path = home / 'gateway.lock'
        if not _still_names(path, handle.fileno()):
            raise OwnershipConflict(f'Gateway runtime already owns profile {home}')
        predecessor = _read_gateway_lock_record(path)
        self._predecessor_records[home] = (
            copy.deepcopy(predecessor) if isinstance(predecessor, dict) else None
        )
        try:
            prior_payload = os.fstat(handle.fileno()).st_size > 0
        except OSError:
            prior_payload = True
        prior_lock = prior_lock_present or prior_payload or isinstance(predecessor, dict)
        if record is None:
            captured = _build_pid_record()
            record = {**captured, 'hermes_home': str(home), 'owner_epoch': owner_epoch}
        # Bind the acquisition tuple to this exact descriptor and nonce before publishing the
        # record. Later reads must match this tuple exactly; only the independent live-process
        # start reading may use the existing platform drift tolerance.
        self._owner_record_bindings[home] = (
            handle, owner_epoch,
            (record.get('pid'), record.get('kind'), record.get('start_time'), record.get('hermes_home')),
            handle.fileno(),
        )
        if purpose:
            record['purpose'] = purpose
        if not lineage_prepared:
            self._record_reservation_lineage(home, origin, owner_epoch, prior_lock, record)
        handle.seek(0)
        handle.truncate()
        json.dump(record, handle)
        handle.flush()
        os.fsync(handle.fileno())

    def reserve(self, homes, *, purpose: str | None = None) -> None:
        """Reserve the real profile lock and publish one fresh physical incarnation."""
        from gateway.status import _try_acquire_file_lock
        from hermes_cli.profile_provenance import read_profile_provenance
        with self._mutex:
            added = []
            try:
                for home in sorted({canonical_home(p) for p in homes}):
                    if home in self._unknown_homes:
                        raise OwnershipConflict(f'Gateway runtime ownership is unknown for profile {home}')
                    if home in self._handles:
                        continue
                    # Preserve the historical reservation ABI for a brand-new legacy home.  The
                    # directory birth is not an I15 witness; without the authenticated origin it
                    # remains UNKNOWN at the close fence.  Existing markerless homes are never
                    # touched by this compatibility path.
                    if not os.path.lexists(home):
                        make_user_dir(home, mode=0o700, parents=True, exist_ok=False)
                    path = home / 'gateway.lock'
                    prior_lock_present = os.path.lexists(path)
                    origin = read_profile_provenance(home)
                    if origin.is_new_profile and prior_lock_present:
                        raise OwnershipConflict(
                            f"Profile {home} has a first-run origin but a prior gateway lock"
                        )
                    # Generate the logical incarnation before any OS acquisition.  If UUID
                    # creation fails there is no descriptor to retain; once acquisition succeeds
                    # the already-created token can be registered without another fallible step.
                    owner_epoch = uuid.uuid4().hex
                    from gateway.status import _build_pid_record

                    captured = _build_pid_record()
                    record = {**captured, 'hermes_home': str(home), 'owner_epoch': owner_epoch}
                    if purpose:
                        record['purpose'] = purpose
                    lineage_prepared = origin.is_new_profile and not prior_lock_present
                    if lineage_prepared:
                        # Publish the one-way source transition before creating/opening
                        # gateway.lock.  A crash after this point is a started/unknown
                        # profile, never a replayable first-run claim.
                        self._record_reservation_lineage(
                            home, origin, owner_epoch, prior_lock_present, record,
                        )
                    fd = _open_lock(path)
                    try:
                        handle = os.fdopen(fd, 'r+', encoding='utf-8')  # windows-footgun: ok — write-only
                    except BaseException:
                        with suppress(OSError):
                            os.close(fd)
                        raise
                    if not _try_acquire_file_lock(handle):
                        handle.close()
                        raise OwnershipConflict(f'Gateway runtime already owns profile {home}')
                    # The kernel exclusion is now held. Register the descriptor and its fresh
                    # incarnation before any record construction or file I/O can fail.
                    self._handles[home] = handle
                    self._owner_epochs[home] = owner_epoch
                    self._predecessor_records[home] = None
                    if not lineage_prepared:
                        self._lineages[home] = ProfileLineage(
                            "unknown", "owner_record_unpublished", owner_epoch=owner_epoch,
                            prior_lock_present=prior_lock_present,
                        )
                    added.append(home)
                    self._publish_reservation(
                        home, handle, origin, owner_epoch, prior_lock_present, purpose,
                        record=record, lineage_prepared=lineage_prepared,
                    )
            except BaseException as exc:
                rollback = self._release_homes(added)
                if rollback.failed:
                    raise OwnershipReleaseError(
                        f'Profile ownership rollback is unknown: {rollback.describe()}', rollback) from exc
                raise

    def owns(self, home: Path) -> bool:
        with self._mutex:
            key = canonical_home(home)
            return key in self._handles and key not in self._unknown_homes

    def owner_epoch(self, home: Path) -> str | None:
        """Return the physical lock incarnation only after strict revalidation."""
        with self._mutex:
            key = canonical_home(home)
            if key not in self._handles or key in self._unknown_homes:
                return None
            if not self._revalidate_owner_locked(key):
                self._unknown_homes.add(key)
                return None
            return self._owner_epochs.get(key)

    def predecessor_record(self, home: Path) -> dict | None:
        """Return the record observed after acquiring this home's real exclusion."""
        with self._mutex:
            record = self._predecessor_records.get(canonical_home(home))
            return copy.deepcopy(record) if isinstance(record, dict) else None

    def reservation_lineage(self, home: Path) -> ProfileLineage | None:
        """Return the typed creation lineage bound to this retained owner descriptor."""
        with self._mutex:
            return self._lineages.get(canonical_home(home))

    def reservation_is_current(self, home: Path, expected_epoch: str) -> bool:
        """Validate a retained callback token without treating ``owns`` as proof."""
        if not isinstance(expected_epoch, str) or not expected_epoch:
            return False
        return self.owner_epoch(home) == expected_epoch

    def _read_bound_owner_record_locked(self, key: Path, handle, expected_epoch: str) -> dict | None:
        """Read and validate one lock record through the retained owner descriptor.

        The record, descriptor/path identity, and current process tuple are one witness.  A
        second path-opened record cannot replace it between validation and I20 sealing; Windows
        keeps the already-held mandatory byte-range lock and never unlocks/relocks to query it.
        """
        try:
            fd = handle.fileno()
            if (getattr(handle, 'closed', False) or not isinstance(expected_epoch, str)
                    or not expected_epoch or not _still_names(key / 'gateway.lock', fd)):
                return None
            from gateway.status import (
                _build_pid_record, _same_hermes_home, _try_acquire_file_lock,
                start_time_fingerprints_match,
            )
            if os.name != 'nt' and not _try_acquire_file_lock(handle):
                return None
            os.fstat(fd)
            if not _still_names(key / 'gateway.lock', fd):
                return None
            handle.seek(0)
            record = json.loads(handle.read())
            if not isinstance(record, dict):
                return None
            # The path is checked both before and after the coherent descriptor read.
            if not _still_names(key / 'gateway.lock', fd):
                return None
            current = _build_pid_record()
            binding = self._owner_record_bindings.get(key)
            if (
                not isinstance(binding, tuple) or len(binding) != 4
                or binding[1] != expected_epoch
                or not isinstance(binding[2], tuple) or len(binding[2]) != 4
                or not isinstance(binding[3], int) or isinstance(binding[3], bool)
            ):
                return None
            # Tests and diagnostics may wrap the retained object to inject a close fault; the
            # native descriptor remains the same physical handle in that case.  A different
            # descriptor cannot satisfy the binding even if it copied the JSON.
            captured_handle = binding[0]
            if (getattr(captured_handle, 'closed', False)
                    or captured_handle.fileno() != binding[3]
                    or handle.fileno() != binding[3]):
                return None
            captured_pid, captured_kind, captured_start, captured_home = binding[2]
            recorded_start = record.get('start_time')
            current_start = current.get('start_time')
            record_pid = record.get('pid')
            current_pid = current.get('pid')
            record_kind = record.get('kind')
            current_kind = current.get('kind')
            record_home = record.get('hermes_home')
            if (
                record.get('owner_epoch') != expected_epoch
                or not isinstance(record_pid, int) or isinstance(record_pid, bool)
                or record_pid != captured_pid
                or not isinstance(record_kind, str) or record_kind != current_kind
                or record_kind != captured_kind
                or not isinstance(record_home, str)
                or not _same_hermes_home(record_home, key)
                or record_home != captured_home
                or not isinstance(recorded_start, int) or isinstance(recorded_start, bool)
                or recorded_start != captured_start
                or not isinstance(captured_pid, int) or isinstance(captured_pid, bool)
                or not isinstance(captured_kind, str)
                or not isinstance(captured_home, str)
                or not _same_hermes_home(captured_home, key)
                or current_pid != captured_pid
                or current_kind != captured_kind
                or not isinstance(current_start, int) or isinstance(current_start, bool)
                or not start_time_fingerprints_match(captured_start, current_start)
            ):
                return None
            return record
        except BaseException:  # health: allow BLE001 -- interrupted physical query stays unknown
            return None

    def reservation_evidence(self, home: Path) -> dict | None:
        """Return source-owned I20 evidence from one retained descriptor-bound record.

        A boolean owner flag, PID-only record, or second path read cannot produce evidence here.
        """
        with self._mutex:
            key = canonical_home(home)
            epoch = self._owner_epochs.get(key)
            handle = self._handles.get(key)
            if handle is None or not isinstance(epoch, str):
                if handle is not None:
                    self._unknown_homes.add(key)
                return None
            record = self._read_bound_owner_record_locked(key, handle, epoch)
            if record is None:
                self._unknown_homes.add(key)
                return None
            try:
                fd = handle.fileno()
            except BaseException:  # health: allow BLE001 -- evidence stays unknown
                self._unknown_homes.add(key)
                return None
            return {
                'profile_home': str(key),
                'owner_epoch': epoch,
                'record': copy.deepcopy(record),
                'physical_binding': (str(key), epoch, fd),
                'record_binding': (
                    record['owner_epoch'], record['pid'], record['kind'],
                    record['start_time'], record['hermes_home'],
                ),
                'native_handle_retained': True,
                'native_descriptor': fd,
                'os_lock_owner': True,
                'cross_process_proof': True,
                'observed': True,
            }

    def _revalidate_owner_locked(self, key: Path) -> bool:
        handle = self._handles.get(key)
        expected_epoch = self._owner_epochs.get(key)
        if handle is None or not isinstance(expected_epoch, str) or not expected_epoch:
            return False
        return self._read_bound_owner_record_locked(key, handle, expected_epoch) is not None

    def revalidate_owner(self, home: Path) -> bool:
        with self._mutex:
            key = canonical_home(home)
            if key not in self._handles or key in self._unknown_homes:
                return False
            if not self._revalidate_owner_locked(key):
                self._unknown_homes.add(key)
                return False
            return True

    def release_outcome(self, home: Path) -> ProfileReleaseOutcome:
        """Return the typed physical result while retaining the legacy ``release`` ABI."""
        with self._mutex:
            key = canonical_home(home)
            if key in self._finalizer_holds:
                return ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
            handle = self._handles.get(key)
            if handle is None:
                return (ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
                        if key in self._unknown_homes else ProfileReleaseOutcome.ABSENT)
            if key in self._unknown_homes and not self._revalidate_owner_locked(key):
                return ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
            # A caller may have closed the exact retained descriptor behind this owner.  Calling
            # close() again can report success even though the OS exclusion was already lost (and
            # on POSIX the descriptor number may already have been reused).  Keep that exact
            # object in custody and classify the release as unknown; never replace or discard it.
            try:
                descriptor = handle.fileno()
                if getattr(handle, "closed", False) or type(descriptor) is not int or descriptor < 0:
                    self._unknown_homes.add(key)
                    return ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
            except BaseException:  # health: allow BLE001 -- invalid retained handle is ambiguous
                self._unknown_homes.add(key)
                return ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
            try:
                # Closing the original descriptor is the OS owner operation on both POSIX and
                # Windows. Do not unlock first and then pretend a failing close was harmless.
                handle.close()
            except BaseException:  # health: allow BLE001 -- ambiguous close must retain custody
                self._unknown_homes.add(key)
                return ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
            self._handles.pop(key, None)
            self._unknown_homes.discard(key)
            self._owner_epochs.pop(key, None)
            self._owner_record_bindings.pop(key, None)
            self._predecessor_records.pop(key, None)
            self._lineages.pop(key, None)
            return ProfileReleaseOutcome.RELEASED

    def release(self, home: Path) -> bool:
        """Release a profile using the historical boolean API.

        Typed callers must use :meth:`release_outcome`; a failed or ambiguous close remains
        retained and therefore returns ``False`` rather than a truthy enum.
        """
        return self.release_outcome(home) is ProfileReleaseOutcome.RELEASED

    def retain_finalizer_hold(self, home: Path, finalizer: object) -> None:
        """Keep one exact profile handle through an ambiguous physical finalizer."""
        if finalizer is None or getattr(finalizer, "state", None) == "terminal":
            raise OwnershipConflict("finalizer hold requires a nonterminal owner")
        with self._mutex:
            key = canonical_home(home)
            if key not in self._handles:
                raise OwnershipConflict(f"finalizer hold requires owned profile {key}")
            current = self._finalizer_holds.get(key)
            if current is not None and current is not finalizer:
                raise OwnershipConflict(f"profile {key} has another finalizer hold")
            self._finalizer_holds[key] = finalizer

    def release_finalizer_hold(self, home: Path, finalizer: object) -> bool:
        """Release only the same retained handle after its exact token is terminal."""
        with self._mutex:
            key = canonical_home(home)
            if self._finalizer_holds.get(key) is not finalizer:
                return False
            if getattr(finalizer, "state", None) != "terminal":
                return False
            self._finalizer_holds.pop(key, None)
            released = self.release_outcome(key) is ProfileReleaseOutcome.RELEASED
            if not released:
                # An ambiguous physical close must retain the exact token so the same owner can
                # retry it; a failed release may never silently discard finalizer custody.
                self._finalizer_holds[key] = finalizer
            return released

    def finalizer_hold(self, home: Path) -> object | None:
        with self._mutex:
            return self._finalizer_holds.get(canonical_home(home))

    def start_writer(self, thread: threading.Thread) -> None:
        """Register before starting so partial startup cannot forget a live writer."""
        with self._mutex:
            self._writers.append(thread)
            thread.start()

    def _release_homes(self, homes) -> ProfileReleaseSummary:
        outcomes = {}
        for home in reversed(tuple(homes)):
            key = canonical_home(home)
            outcomes[key] = self.release_outcome(key)
        return ProfileReleaseSummary(outcomes)

    def close(self) -> ProfileReleaseSummary:
        with self._mutex:
            # A timed-out daemon can still write during finally/atexit. Keep the
            # handles alive; process exit releases them atomically in the OS.
            if any(thread.is_alive() for thread in self._writers):
                return ProfileReleaseSummary({}, writers_alive=True)
            self._writers.clear()
            homes = self.homes
            for home in homes:
                finalizer = self.finalizer_hold(home)
                if finalizer is not None and getattr(finalizer, "state", None) == "terminal":
                    self.release_finalizer_hold(home, finalizer)
            return self._release_homes(home for home in homes if home in self.homes)


process_ownership = ProfileOwnership()
_maintenance = threading.local()


def _maintenance_owner(owner: ProfileOwnership | None = None) -> ProfileOwnership:
    if owner is not None:
        return owner
    state = getattr(_maintenance, "state", None)
    if not isinstance(state, tuple) or len(state) != 2 or not isinstance(state[1], ProfileOwnership):
        raise OwnershipConflict("no active maintenance owner")
    return state[1]


def maintenance_holder(home: Path, purpose: str) -> dict | None:
    """The live holder record of *home*'s ``gateway.lock`` when it is a ``purpose`` maintenance
    reservation (see :meth:`ProfileOwnership.reserve`), else None. Advisory only: it names the
    holder for a refusal or a short wait, it never grants the lock."""
    from gateway.status import _live_pid_from_record, _read_gateway_lock_record
    record = _read_gateway_lock_record(canonical_home(home) / 'gateway.lock')
    if not record or record.get('purpose') != purpose or _live_pid_from_record(record) is None:
        return None
    return record


def retain_maintenance(home: Path, finalizer: object, *, owner: ProfileOwnership | None = None) -> None:
    """Register the exact finalizer before an exclusive maintenance context unwinds."""
    _maintenance_owner(owner).retain_finalizer_hold(home, finalizer)


def release_maintenance_hold(
    home: Path, finalizer: object, *, owner: ProfileOwnership | None = None,
) -> bool:
    """Release a retained maintenance lock after same-owner terminal proof."""
    return _maintenance_owner(owner).release_finalizer_hold(home, finalizer)


def lock_record_unpublished(home: Path) -> bool:
    """True when *home*'s ``gateway.lock`` record names no live holder yet: empty, torn, or a dead
    previous holder's. :meth:`ProfileOwnership.reserve` takes the flock BEFORE it writes the record,
    so a contender that loses the flock in that window cannot tell maintenance from a gateway; it
    must retry (bounded) instead of reporting a runtime owner. Advisory only, never grants the lock."""
    from gateway.status import _live_pid_from_record, _read_gateway_lock_record
    record = _read_gateway_lock_record(canonical_home(home) / 'gateway.lock')
    return not record or _live_pid_from_record(record) is None


def live_runtime_owner(home: Path) -> int | None:
    """PID of a live runtime owner of *home* (a gateway, or a reservation that is not marked as
    maintenance), else None. A purpose-marked maintenance holder is never an owner."""
    from gateway.status import _live_pid_from_record, _read_gateway_lock_record, get_running_pid
    home = canonical_home(home)
    record = _read_gateway_lock_record(home / 'gateway.lock')
    live = _live_pid_from_record(record) if record else None
    if live is not None and record.get('purpose'):
        return None  # the flock holder is maintenance, so no gateway can hold it too
    return get_running_pid(home / 'gateway.pid', cleanup_stale=False) or live


@contextmanager
def exclusive_maintenance(homes, *, purpose: str | None = None):
    """Reserve the authority's exact lock before touching managed state.

    This is not an owner-presence check: the reservation remains held through
    publication, excluding startup even before its PID/control socket exists.
    Only nested synchronous maintenance in this thread may reuse a reservation;
    being the runtime owner (even in this process) never grants maintenance.
    Lock inodes must not be restored, removed, or replaced by callers.
    """
    state = getattr(_maintenance, 'state', None)
    same_owner = isinstance(state, tuple) and len(state) == 2 and state[0] == os.getpid()
    owner = state[1] if same_owner else ProfileOwnership()
    if not isinstance(owner, ProfileOwnership):
        raise OwnershipConflict('invalid maintenance owner state')
    outermost = not same_owner
    previous = set(owner.homes)
    try:
        owner.reserve(homes, purpose=purpose)
        if outermost:
            _maintenance.state = (os.getpid(), owner)
        yield owner
    except OwnershipReleaseError:
        raise
    except OwnershipConflict as exc:
        raise OwnershipConflict(
            f'Exclusive maintenance refused: {exc}. Drain and stop the gateway, then retry.'
        ) from exc
    finally:
        added = tuple(home for home in owner.homes if home not in previous)
        retained = set()
        for home in added:
            finalizer = owner.finalizer_hold(home)
            if finalizer is not None:
                if getattr(finalizer, "state", None) == "terminal":
                    # A terminal token may attempt its real physical close here.  If that
                    # close is ambiguous, release_finalizer_hold restores the exact token and
                    # the normal release pass below raises the typed unknown result.
                    owner.release_finalizer_hold(home, finalizer)
                else:
                    # Legacy context-manager callers may leave an exact nonterminal finalizer
                    # token behind.  Keep that physical handle for its same-owner retry rather
                    # than throwing from __exit__ or releasing a still-live exclusion.
                    retained.add(home)
        released = owner._release_homes(
            home for home in added if home in owner.homes and home not in retained
        )
        if released.failed:
            # Keep the owner reachable for a bounded retry; a close can have released the OS
            # exclusion before raising, so no caller may proceed as if the reservation was gone.
            _maintenance.state = (os.getpid(), owner)
            raise OwnershipReleaseError(
                f'Exclusive maintenance release is unknown: {released.describe()}', released)
        if outermost:
            _maintenance.state = None
        elif retained:
            _maintenance.state = (os.getpid(), owner)


def release_runtime_ownership():
    """Release this process's reservations and return the typed physical outcome.

    An unknown/partial close deliberately keeps the running-PID cache stale and actionable;
    callers must not turn an unresolved descriptor into clean shutdown or successor ownership.
    """
    from gateway import status as status_api
    import contextlib
    try:
        summary = process_ownership.close()
    except BaseException as exc:  # health: allow BLE001 -- interrupted close must fail closed
        # A partial or interrupted close is still physical custody.  Return a typed failed
        # summary so every shutdown caller invalidates clean state and retains its PID fence.
        status_api.logger.error("Gateway runtime ownership close was interrupted: %r", exc)
        try:
            homes = process_ownership.homes
        except BaseException:  # health: allow BLE001 -- custody snapshot must remain conservative
            homes = ()
        summary = ProfileReleaseSummary(
            {home: ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION for home in homes},
            writers_alive=True,
        )
    if not summary.failed:
        status_api._clear_running_pid_cache()
    else:
        status_api.logger.warning("Gateway runtime ownership remains unresolved: %s", summary.describe())
        # _stop_persist_exit_state may have written the marker before the final close.  A failed
        # physical release invalidates it centrally so an unknown descriptor can never look like
        # a clean successor handoff on the next boot.
        with contextlib.suppress(OSError):
            (status_api.get_process_hermes_home() / ".clean_shutdown").unlink(missing_ok=True)
    return summary



def remove_owned_pid_file_checked() -> bool:
    """Remove this process's PID marker and report whether the fence is resolved.

    A different live PID is a validated successor fence and is therefore successful for the
    caller's cleanup.  Any read/unlink Exception or BaseException is an unknown result.
    """
    from gateway import status as status_api
    try:
        path = status_api._get_pid_path()
        record = status_api._read_json_file(path, bare_pid_ok=True)
        if record is None and path.exists():
            return False
        file_pid = status_api._pid_from_record(record)
        if file_pid is not None and file_pid != os.getpid():
            if not isinstance(record, dict) or not status_api._record_matches_live_gateway_pid(
                    record, file_pid, expected_home=status_api._get_process_hermes_home()):
                return False
            return True  # A validated successor owns the marker; never remove its identity.
        if file_pid is None and record is not None:
            return False
        path.unlink(missing_ok=True)
        status_api._clear_running_pid_cache()
        return True
    except BaseException as exc:  # health: allow BLE001 -- unknown PID fence blocks clean custody
        status_api.logger.error("Could not resolve the gateway PID marker: %r", exc)
        return False
