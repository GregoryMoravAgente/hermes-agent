"""Stdlib-only creation provenance shared by the installer and runtime adapter.

The installer imports this module before PM dependencies exist.  Keep the source
and the on-disk contract here; the normal runtime adapter supplies its existing
install identity, while the installer mints that identity only after it has
exclusively created a new home.
"""

from __future__ import annotations

import argparse
import contextlib
import contextvars
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import sys
import uuid
import weakref


PROVENANCE_FILENAME = ".hermes-profile-origin.json"
PROVENANCE_SCHEMA = 1
PROVENANCE_KIND = "hermes-profile-origin"
INSTALL_ID_FILENAME = "install_id"
INSTALL_ID_LOCK_FILENAME = ".install_id.lock"
STATE_NEVER_STARTED = "never_started"
STATE_STARTED = "started"
_INSTALL_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_REPARSE_POINT = 0x0400


class ProfileProvenanceError(RuntimeError):
    """The creation receipt could not be published without weakening its proof."""


_BIRTH_CAPABILITY_TOKEN = object()
_ACTIVE_BIRTH_CAPABILITIES: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_ACTIVE_NATIVE_DIRECTORY: contextvars.ContextVar = contextvars.ContextVar(
    "hermes_profile_origin_native_directory", default=None,
)


class _WindowsDirectoryInformation:
    """Lazy marker for the native file-information layout (Windows only)."""

    @staticmethod
    def read(handle):
        import ctypes
        from ctypes import wintypes

        class _FileInformation(ctypes.Structure):
            _fields_ = [
                ("attributes", wintypes.DWORD),
                ("creation_time", wintypes.FILETIME),
                ("access_time", wintypes.FILETIME),
                ("write_time", wintypes.FILETIME),
                ("volume_serial", wintypes.DWORD),
                ("file_size_high", wintypes.DWORD),
                ("file_size_low", wintypes.DWORD),
                ("number_of_links", wintypes.DWORD),
                ("file_index_high", wintypes.DWORD),
                ("file_index_low", wintypes.DWORD),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetFileInformationByHandle.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(_FileInformation),
        ]
        kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
        info = _FileInformation()
        if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(info)):
            error = ctypes.get_last_error() or 1
            raise OSError(error, "GetFileInformationByHandle failed")
        if not info.attributes & 0x10 or info.attributes & _REPARSE_POINT:
            raise ProfileProvenanceError("native directory is not a plain directory")
        file_index = (int(info.file_index_high) << 32) | int(info.file_index_low)
        if file_index == 0:
            raise ProfileProvenanceError("native directory file identity is unavailable")
        # The volume serial and the 64-bit file index are the Windows identity.  The
        # attributes are retained as an additional reparse/type guard, not as identity.
        return int(info.volume_serial), file_index, int(info.attributes)


def _close_windows_handle(handle) -> None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    if not kernel32.CloseHandle(handle):
        error = ctypes.get_last_error() or 1
        raise OSError(error, "CloseHandle failed")


class _NativeDirectoryHandle:
    """One retained native handle for a directory birth capability."""

    __slots__ = ("_handle", "_path", "_windows")

    def __init__(self, handle, *, path: Path, windows: bool) -> None:
        self._handle = handle
        self._path = path
        self._windows = windows

    @classmethod
    def open(cls, path: Path, *, protect_name: bool = False) -> _NativeDirectoryHandle:
        if os.name != "nt":
            flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            try:
                return cls(os.open(path, flags), path=Path(path), windows=False)
            except OSError as exc:
                raise ProfileProvenanceError("native directory handle is unavailable") from exc

        # The CRT cannot reliably open a Windows directory as an fd.  Keep a real
        # reparse-safe kernel handle instead; this is the same directory-handle shape
        # used by hermes_dir_acl, but it remains private to the birth capability.
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
            wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
        ]
        kernel32.CreateFileW.restype = wintypes.HANDLE
        # Attribute-only handles do not prevent renaming. The retained birth
        # handle also requests FILE_LIST_DIRECTORY so denying delete sharing
        # holds the pathname until marker publication/readback has finished.
        desired_access = 0x0081 if protect_name else 0x0080
        share_mode = 0x00000003 if protect_name else 0x00000007
        open_existing = 3
        flags = 0x02000000 | 0x00200000  # BACKUP_SEMANTICS | OPEN_REPARSE_POINT
        handle = kernel32.CreateFileW(str(path), desired_access, share_mode, None,
                                      open_existing, flags, None)
        invalid = ctypes.c_void_p(-1).value
        if not handle or handle == invalid:
            raise ProfileProvenanceError("native directory handle is unavailable")
        result = cls(handle, path=Path(path), windows=True)
        try:
            result._validate_windows_directory()
        except BaseException:
            result.close()
            raise
        return result

    def _validate_windows_directory(self) -> tuple[int, int, int]:
        return _WindowsDirectoryInformation.read(self._handle)

    def identity(self) -> tuple[int, int, int | None]:
        """Read the identity from the retained native object, never a replacement path."""
        if self._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        if self._windows:
            return self._validate_windows_directory()
        try:
            info = os.fstat(self._handle)
        except OSError as exc:
            raise ProfileProvenanceError("native directory identity is unavailable") from exc
        if not stat.S_ISDIR(info.st_mode):
            raise ProfileProvenanceError("native object is not a directory")
        attributes = getattr(info, "st_file_attributes", None)
        return (
            getattr(info, "st_dev", None),
            getattr(info, "st_ino", None),
            attributes,
        )

    def matches(self, binding: tuple[int, int, int | None]) -> bool:
        if self._handle is None:
            return False
        if self._windows:
            try:
                return self.identity() == binding
            except (OSError, ProfileProvenanceError):
                return False
        try:
            return self.identity() == binding
        except (OSError, ProfileProvenanceError):
            return False

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        if self._windows:
            _close_windows_handle(handle)
        else:
            os.close(handle)
        # Clear custody only after the native close has returned success.  A failed or
        # ambiguous close must leave the exact object available for a same-owner retry.
        self._handle = None

    def read_child(self, name: str) -> bytes:
        """Read one child through the retained directory identity."""
        if self._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        if self._windows:
            child = _safe_path(self._path / name, label="native directory child")
            return child.read_bytes()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(name, flags, dir_fd=self._handle)
        try:
            with os.fdopen(fd, "rb") as stream:
                fd = -1
                return stream.read()
        finally:
            if fd >= 0:
                os.close(fd)

    def replace_child(self, name: str, payload: bytes) -> None:
        """Atomically replace one child relative to this exact directory."""
        if self._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        temporary = f".{name}.pending-{uuid.uuid4().hex}"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        moved = False
        if self._windows:
            temporary_path = self._path / temporary
            fd = os.open(temporary_path, flags, 0o600)
        else:
            temporary_path = None
            fd = os.open(temporary, flags, 0o600, dir_fd=self._handle)
        try:
            with os.fdopen(fd, "wb") as stream:
                fd = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if self._windows:
                os.replace(temporary_path, self._path / name)
            else:
                try:
                    os.rename(
                        temporary, name, src_dir_fd=self._handle, dst_dir_fd=self._handle,
                    )
                except (NotImplementedError, TypeError) as exc:
                    raise ProfileProvenanceError(
                        "relative marker replacement is unavailable"
                    ) from exc
            moved = True
            if not self._windows:
                os.fsync(self._handle)
        finally:
            if fd >= 0:
                os.close(fd)
            if not moved:
                with contextlib.suppress(FileNotFoundError, OSError):
                    self.unlink_child(temporary)

    def write_exclusive_child(self, name: str, payload: bytes) -> None:
        """Write one marker relative to this exact directory on descriptor-based hosts."""
        if self._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        token = _ACTIVE_NATIVE_DIRECTORY.set(self)
        try:
            _write_exclusive(self._path / name, payload)
        finally:
            _ACTIVE_NATIVE_DIRECTORY.reset(token)

    def unlink_child(self, name: str) -> None:
        """Remove a marker through the retained directory identity when possible."""
        if self._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        if self._windows:
            (self._path / name).unlink(missing_ok=True)
            return
        os.unlink(name, dir_fd=self._handle)

    def __del__(self) -> None:
        with contextlib.suppress(BaseException):
            self.close()


class ExclusiveDirectoryBirth:
    """Opaque, one-use proof issued by the real exclusive directory creator.

    A binding tuple or a copied marker is only an observation.  This capability also
    retains the creator process, the lexical path and its parent identity, the native
    directory handle, and a source-issued generation.  The private constructor token,
    active-object registry and single-use state prevent public tuple/boolean substitutes
    from reaching the origin publisher.
    """

    __slots__ = (
        "__weakref__",
        "_binding",
        "_consumed",
        "_creator_pid",
        "_generation",
        "_native",
        "_parent",
        "_parent_binding",
        "_path",
        "_token",
    )

    def __init__(
        self, token, *, path: Path, parent: Path,
        binding: tuple[int, int, int | None],
        parent_binding: tuple[int, int, int | None],
        generation: str, creator_pid: int, native: _NativeDirectoryHandle,
    ) -> None:
        if token is not _BIRTH_CAPABILITY_TOKEN:
            raise TypeError("directory birth capabilities are source-issued")
        self._token = token
        self._path = path
        self._parent = parent
        self._binding = binding
        self._parent_binding = parent_binding
        self._generation = generation
        self._creator_pid = creator_pid
        self._native = native
        self._consumed = False

    def __copy__(self):
        raise TypeError("directory birth capabilities cannot be copied")

    def __deepcopy__(self, _memo):
        raise TypeError("directory birth capabilities cannot be copied")

    def __reduce__(self):
        raise TypeError("directory birth capabilities cannot be serialized")

    def __reduce_ex__(self, _protocol):
        raise TypeError("directory birth capabilities cannot be serialized")


def _birth_failure(reason: str) -> ProfileProvenanceError:
    return ProfileProvenanceError(f"directory birth capability is unavailable: {reason}")


def create_exclusive_directory_birth(path: Path) -> ExclusiveDirectoryBirth:
    """Create one ACL-safe directory and retain the exact source-issued birth proof."""
    safe_path = _safe_path(path, label="profile home")
    parent = _safe_path(safe_path.parent, label="profile home parent")
    if os.path.lexists(safe_path):
        raise FileExistsError(f"directory already exists: {safe_path}")
    try:
        _make_user_dir(parent, parents=True, exist_ok=True)
        _make_user_dir(safe_path, parents=False, exist_ok=False)
    except FileExistsError:
        raise
    except OSError as exc:
        raise _birth_failure("exclusive creation failed") from exc
    try:
        current = _safe_path(safe_path, label="profile home")
        if current != safe_path:
            raise _birth_failure("created path changed")
        binding = directory_binding(safe_path)
        parent_binding = directory_binding(parent)
        if binding is None or parent_binding is None:
            raise _birth_failure("created directory identity is unavailable")
        native = _NativeDirectoryHandle.open(safe_path, protect_name=True)
        if not native.matches(binding):
            native.close()
            raise _birth_failure("created native identity does not read back")
        birth = ExclusiveDirectoryBirth(
            _BIRTH_CAPABILITY_TOKEN,
            path=safe_path,
            parent=parent,
            binding=binding,
            parent_binding=parent_binding,
            generation=uuid.uuid4().hex,
            creator_pid=os.getpid(),
            native=native,
        )
    except BaseException:
        with contextlib.suppress(OSError):
            safe_path.rmdir()
        raise
    _ACTIVE_BIRTH_CAPABILITIES[birth] = None
    return birth


def _validate_directory_birth(
    birth: ExclusiveDirectoryBirth, home: Path, *, consume: bool = False,
) -> tuple[int, int, int | None]:
    if not isinstance(birth, ExclusiveDirectoryBirth):
        raise _birth_failure("publisher requires the exact source-issued capability")
    if birth not in _ACTIVE_BIRTH_CAPABILITIES:
        raise _birth_failure("capability was copied, serialized, or already consumed")
    if birth._token is not _BIRTH_CAPABILITY_TOKEN or birth._consumed:
        raise _birth_failure("capability was copied, serialized, or already consumed")
    if birth._creator_pid != os.getpid():
        raise _birth_failure("capability belongs to another creator process")
    safe_home = _safe_path(home, label="profile home")
    if safe_home != birth._path:
        raise _birth_failure("capability path does not match the publisher target")
    current_parent = directory_binding(birth._parent)
    current = directory_binding(safe_home)
    if current_parent != birth._parent_binding or current != birth._binding:
        raise _birth_failure("directory identity changed after exclusive creation")
    if not birth._native.matches(birth._binding):
        raise _birth_failure("retained native directory handle is unavailable")
    if consume:
        birth._consumed = True
        _ACTIVE_BIRTH_CAPABILITIES.pop(birth, None)
    return birth._binding
@dataclass(frozen=True)
class ProfileProvenance:
    """Typed result of reading one profile's source-owned creation receipt."""

    status: str
    reason: str = ""
    generation: str | None = None
    profile_kind: str | None = None
    profile_name: str | None = None
    install_id: str | None = None
    state: str | None = None
    birth_binding: tuple[int, int, int | None] | None = None
    started_owner_epoch: str | None = None
    started_record_binding: tuple[object, ...] | None = None

    @property
    def is_authenticated(self) -> bool:
        return self.status == "valid"

    @property
    def is_new_profile(self) -> bool:
        return self.status == "valid" and self.state == STATE_NEVER_STARTED


def provenance_path(home: Path) -> Path:
    return Path(home) / PROVENANCE_FILENAME


def install_id_path(root: Path) -> Path:
    return Path(root) / INSTALL_ID_FILENAME


def _safe_path(path: Path, *, label: str) -> Path:
    """Return an absolute lexical path after rejecting links/reparse points in its prefix.

    ``Path.resolve`` is deliberately not used here.  A profile home is an ownership boundary;
    following a link before the source-owned birth/identity checks could authenticate another
    directory.  ``abspath`` only normalizes ``.``/``..`` and the lstat walk stops at the first
    missing component, so an exclusive creator can still validate a new leaf without touching it.
    """
    candidate = Path(path).expanduser()
    candidate = Path(os.path.abspath(os.fspath(candidate)))
    current = Path(candidate.anchor)
    for part in candidate.parts:
        if part == candidate.anchor:
            continue
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            break
        except OSError as exc:
            raise ProfileProvenanceError(f"{label} is unavailable") from exc
        attributes = getattr(info, "st_file_attributes", 0)
        if stat.S_ISLNK(info.st_mode) or (
            type(attributes) is int and attributes & _REPARSE_POINT
        ):
            raise ProfileProvenanceError(f"{label} is linked or reparse-backed")
    return candidate


def canonical_install_root(home: Path) -> Path:
    """Resolve the canonical install root only after the served home is link-free."""
    safe_home = _safe_path(home, label="profile home")
    try:
        from hermes_constants import get_default_hermes_root

        root = get_default_hermes_root(home=safe_home)
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        root = safe_home
    return _safe_path(root, label="install identity root")


def read_canonical_install_id(home: Path) -> str | None:
    """Read the existing install identity without creating a home or identity file."""
    try:
        root = canonical_install_root(home)
    except ProfileProvenanceError:
        return None
    return read_existing_install_id(root)


def directory_binding(path: Path) -> tuple[int, int, int | None] | None:
    """Return native directory identity without following a directory symlink."""
    try:
        safe_path = _safe_path(path, label="profile home")
        if os.name == "nt":
            native = _NativeDirectoryHandle.open(safe_path)
            try:
                return native.identity()
            finally:
                native.close()
        info = os.stat(safe_path, follow_symlinks=False)
    except (FileNotFoundError, OSError, ProfileProvenanceError):
        return None
    if not stat.S_ISDIR(info.st_mode):
        return None
    device = getattr(info, "st_dev", None)
    inode = getattr(info, "st_ino", None)
    attributes = getattr(info, "st_file_attributes", None)
    if type(device) is not int or type(inode) is not int:
        return None
    if attributes is not None and type(attributes) is not int:
        return None
    if os.name == "nt" and inode == 0:
        return None
    return device, inode, attributes


def _binding_from_raw(value) -> tuple[int, int, int | None] | None:
    if not isinstance(value, list) or len(value) != 3:
        return None
    device, inode, attributes = value
    if type(device) is not int or type(inode) is not int:
        return None
    if attributes is not None and type(attributes) is not int:
        return None
    return device, inode, attributes


def _valid_hex(value: object, length: int) -> bool:
    return isinstance(value, str) and len(value) == length and bool(re.fullmatch(r"[0-9a-f]+", value))


def _invalid(status: str, reason: str) -> ProfileProvenance:
    return ProfileProvenance(status=status, reason=reason)


def read_existing_install_id(root: Path) -> str | None:
    """Read an existing identity without creating a directory or identity file."""
    try:
        safe_root = _safe_path(root, label="install identity root")
        value = install_id_path(safe_root).read_text(encoding="utf-8-sig").strip().lower()
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError):
        return None
    return value if _INSTALL_ID_RE.fullmatch(value) else None


@contextlib.contextmanager
def _install_id_file_lock(root: Path):
    """Serialize identity publication using the existing POSIX/Windows contract."""
    fd = os.open(root / INSTALL_ID_LOCK_FILENAME, os.O_RDWR | os.O_CREAT, 0o600)
    windows = os.name == "nt"
    try:
        if windows:
            import msvcrt

            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
                os.fsync(fd)
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            if windows:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _fsync_directory(path: Path) -> None:
    native = _ACTIVE_NATIVE_DIRECTORY.get()
    if native is not None and not native._windows:
        if native._handle is None:
            raise ProfileProvenanceError("native directory handle is unavailable")
        os.fsync(native._handle)
        return
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_exclusive(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    native = _ACTIVE_NATIVE_DIRECTORY.get()
    if native is not None and not native._windows:
        safe_path = Path(path)
        if safe_path.parent != native._path:
            raise ProfileProvenanceError("native marker target is not bound to its birth directory")
        fd = os.open(safe_path.name, flags, 0o600, dir_fd=native._handle)
    else:
        fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            fd = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if fd >= 0:
            os.close(fd)


def _create_install_id(root: Path) -> str:
    root = Path(root)
    if directory_binding(root) is None:
        raise ProfileProvenanceError("install identity root is unavailable")
    with _install_id_file_lock(root):
        existing_path = install_id_path(root)
        try:
            existing = existing_path.read_text(encoding="utf-8-sig").strip().lower()
        except FileNotFoundError:
            existing = ""
        except (OSError, UnicodeDecodeError) as exc:
            raise ProfileProvenanceError("install identity is unreadable") from exc
        if existing:
            if not _INSTALL_ID_RE.fullmatch(existing):
                raise ProfileProvenanceError("install identity is malformed")
            return existing
        value = uuid.uuid4().hex
        try:
            _write_exclusive(existing_path, (value + "\n").encode("utf-8"))
            _fsync_directory(root)
            committed = existing_path.read_text(encoding="utf-8-sig").strip().lower()
        except (OSError, UnicodeDecodeError) as exc:
            raise ProfileProvenanceError("install identity publication failed") from exc
        if committed != value or not _INSTALL_ID_RE.fullmatch(committed):
            raise ProfileProvenanceError("install identity did not read back")
        return committed


def read_profile_provenance(
    home: Path, *, install_id: str | None, native: _NativeDirectoryHandle | None = None,
) -> ProfileProvenance:
    """Read and authenticate a profile origin against its current home and identity."""
    try:
        home = _safe_path(home, label="profile home")
    except ProfileProvenanceError:
        return _invalid("unknown", "home_path_linked")
    if native is not None:
        if native._path != home:
            return _invalid("unknown", "native_home_binding_mismatch")
        try:
            current_binding = native.identity()
        except (OSError, ProfileProvenanceError):
            return _invalid("unknown", "home_identity_unavailable")
    else:
        current_binding = directory_binding(home)
        if current_binding is None:
            return _invalid("unknown", "home_identity_unavailable")
    try:
        raw = (
            native.read_child(PROVENANCE_FILENAME)
            if native is not None else provenance_path(home).read_bytes()
        )
    except FileNotFoundError:
        return _invalid("missing", "origin_missing")
    except OSError:
        return _invalid("unknown", "origin_unreadable")
    try:
        record = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _invalid("malformed", "origin_malformed")
    if not isinstance(record, dict):
        return _invalid("malformed", "origin_not_object")
    if (
        record.get("schema") != PROVENANCE_SCHEMA
        or record.get("kind") != PROVENANCE_KIND
        or record.get("state") not in {STATE_NEVER_STARTED, STATE_STARTED}
        or record.get("profile_kind") not in {"default", "named"}
        or not _valid_hex(record.get("generation"), 32)
        or not _valid_hex(record.get("install_id"), 32)
    ):
        return _invalid("malformed", "origin_fields_invalid")
    if not isinstance(install_id, str) or install_id != record["install_id"]:
        return _invalid("invalid", "origin_install_binding_mismatch")
    profile_name = record.get("profile_name")
    if record["profile_kind"] == "default":
        if profile_name is not None:
            return _invalid("malformed", "default_profile_name_present")
    elif not isinstance(profile_name, str) or not profile_name:
        return _invalid("malformed", "named_profile_name_missing")
    binding = _binding_from_raw(record.get("birth_binding"))
    if binding is None or binding != current_binding:
        return _invalid("invalid", "origin_home_binding_mismatch")
    started_epoch = record.get("started_owner_epoch")
    started_binding = record.get("started_record_binding")
    if record["state"] == STATE_STARTED:
        if not isinstance(started_epoch, str) or not started_epoch:
            return _invalid("malformed", "started_owner_epoch_missing")
        if not isinstance(started_binding, list) or len(started_binding) != 5:
            return _invalid("malformed", "started_record_binding_missing")
        started_binding = tuple(started_binding)
    else:
        if started_epoch is not None or started_binding is not None:
            return _invalid("malformed", "never_started_has_owner")
        started_binding = None
    return ProfileProvenance(
        status="valid",
        generation=record["generation"],
        profile_kind=record["profile_kind"],
        profile_name=profile_name,
        install_id=install_id,
        state=record["state"],
        birth_binding=binding,
        started_owner_epoch=started_epoch,
        started_record_binding=started_binding,
    )


def publish_new_profile_origin(
    home: Path, *, profile_kind: str, install_id: str, profile_name: str | None = None,
    birth: ExclusiveDirectoryBirth | None = None,
    expected_binding: object | None = None,
    publish_target: Path | None = None, retain_birth: bool = False,
) -> ProfileProvenance:
    """Publish a create-only origin marker using the exact source birth capability."""
    home = _safe_path(home, label="profile home")
    if expected_binding is not None:
        raise _birth_failure("binding tuples and caller booleans are not birth proof")
    binding = _validate_directory_birth(birth, home)
    if profile_kind not in {"default", "named"}:
        raise ProfileProvenanceError("profile kind is invalid")
    if profile_kind == "default" and profile_name is not None:
        raise ProfileProvenanceError("default origin cannot carry a profile name")
    if profile_kind == "named" and (not isinstance(profile_name, str) or not profile_name):
        raise ProfileProvenanceError("named origin requires a profile name")
    if not isinstance(install_id, str) or not _INSTALL_ID_RE.fullmatch(install_id):
        raise ProfileProvenanceError("install identity is unavailable")
    if publish_target is not None:
        publish_target = _safe_path(publish_target, label="profile publication target")
        if os.path.lexists(publish_target):
            raise ProfileProvenanceError("profile publication target already exists")
    _validate_directory_birth(birth, home)
    generation = birth._generation
    record = {
        "schema": PROVENANCE_SCHEMA,
        "kind": PROVENANCE_KIND,
        "profile_kind": profile_kind,
        "profile_name": profile_name,
        "install_id": install_id,
        "generation": generation,
        "state": STATE_NEVER_STARTED,
        "birth_binding": list(binding),
    }
    path = provenance_path(home)
    _validate_directory_birth(birth, home, consume=not retain_birth)
    marker_written = False
    succeeded = False
    try:
        try:
            token = _ACTIVE_NATIVE_DIRECTORY.set(birth._native)
            try:
                _write_exclusive(path, (json.dumps(record, sort_keys=True) + "\n").encode("utf-8"))
                _fsync_directory(home)
            finally:
                _ACTIVE_NATIVE_DIRECTORY.reset(token)
            marker_written = True
        except FileExistsError as exc:
            raise ProfileProvenanceError("profile origin already exists") from exc
        except OSError as exc:
            raise ProfileProvenanceError(f"profile origin publication failed: {exc}") from exc
        # The retained native directory is authoritative.  Re-read the path identity after the
        # marker write so a rename/replacement interleaving cannot publish into an attacker-owned
        # replacement.  On descriptor-based hosts the marker was written relative to that exact
        # handle, so cleanup below cannot touch a replacement path.
        if directory_binding(home) != binding or not birth._native.matches(binding):
            raise _birth_failure("directory identity changed during publication")
        if publish_target is not None and os.path.lexists(publish_target):
            raise _birth_failure("publication target appeared during publication")
        result = read_profile_provenance(home, install_id=install_id, native=birth._native)
        if not result.is_new_profile or result.generation != generation:
            raise ProfileProvenanceError("published profile origin did not read back")
        succeeded = True
        return result
    except BaseException:
        if marker_written:
            with contextlib.suppress(OSError, ProfileProvenanceError):
                birth._native.unlink_child(PROVENANCE_FILENAME)
        raise
    finally:
        if not retain_birth or not succeeded:
            if not retain_birth:
                birth._native.close()
            elif not succeeded:
                # The caller owns cleanup of an unpublished staging tree.  Keep its exact
                # capability reachable so cleanup can refuse a replacement path.
                pass


def publish_profile_directory_noreplace(
    staging: Path, target: Path, *, birth: ExclusiveDirectoryBirth,
) -> Path:
    """Publish a born staging directory with the platform's native no-replace primitive."""
    staging = _safe_path(staging, label="profile staging directory")
    target = _safe_path(target, label="profile publication target")
    parent = _safe_path(target.parent, label="profile publication parent")
    if staging.parent != parent:
        raise ProfileProvenanceError("profile staging and publication parent differ")
    binding = _validate_directory_birth(birth, staging)
    if directory_binding(parent) != birth._parent_binding:
        raise ProfileProvenanceError("profile publication parent identity changed")

    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        # The birth handle protects the staging name while its marker is being written.  A
        # Windows directory handle opened without FILE_SHARE_DELETE cannot itself be renamed;
        # reopen the same native identity with delete sharing immediately before the native
        # no-replace move, and refuse if the path was exchanged during that handoff.
        birth._native.close()
        birth._native = _NativeDirectoryHandle.open(staging, protect_name=False)
        if not birth._native.matches(binding):
            raise ProfileProvenanceError("profile staging identity changed before publication")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.MoveFileExW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        kernel32.MoveFileExW.restype = wintypes.BOOL
        # Do not pass MOVEFILE_REPLACE_EXISTING.  A destination which appears after any
        # earlier observation is rejected by the native operation itself.
        if not kernel32.MoveFileExW(str(staging), str(target), 0x00000008):
            error = ctypes.get_last_error() or 1
            raise FileExistsError(error, "native no-replace profile publication failed", target)
    else:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        parent_fd = os.open(parent, flags)
        try:
            if sys.platform == "darwin":
                import ctypes

                renameatx_np = getattr(ctypes.CDLL(None, use_errno=True), "renameatx_np", None)
                if renameatx_np is None:
                    raise ProfileProvenanceError("native no-replace profile publication is unavailable")
                renameatx_np.argtypes = [
                    ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
                ]
                renameatx_np.restype = ctypes.c_int
                result = renameatx_np(
                    parent_fd, staging.name.encode(), parent_fd, target.name.encode(), 0x00000004,
                )
                if result != 0:
                    error = ctypes.get_errno() or 1
                    raise FileExistsError(error, "native no-replace profile publication failed", target)
            else:
                import ctypes

                renameat2 = getattr(ctypes.CDLL(None, use_errno=True), "renameat2", None)
                if renameat2 is None:
                    raise ProfileProvenanceError("native no-replace profile publication is unavailable")
                renameat2.argtypes = [
                    ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
                ]
                renameat2.restype = ctypes.c_int
                result = renameat2(
                    parent_fd, staging.name.encode(), parent_fd, target.name.encode(), 0x00000001,
                )
                if result != 0:
                    error = ctypes.get_errno() or 1
                    raise FileExistsError(error, "native no-replace profile publication failed", target)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    # The source handle is still the authority after the move; the target path is only
    # accepted when it has the exact pre-publish identity and the parent stayed fixed.
    if directory_binding(parent) != birth._parent_binding or directory_binding(target) != binding:
        raise ProfileProvenanceError("profile publication identity changed")
    if not birth._native.matches(binding):
        raise ProfileProvenanceError("profile publication native identity changed")
    birth._path = target
    birth._native.close()
    birth._consumed = True
    _ACTIVE_BIRTH_CAPABILITIES.pop(birth, None)
    return target


def discard_unpublished_directory_birth(
    staging: Path, *, birth: ExclusiveDirectoryBirth,
) -> bool:
    """Remove only the exact still-unpublished staging directory."""
    if not isinstance(birth, ExclusiveDirectoryBirth) or birth._consumed:
        return False
    try:
        staging = _safe_path(staging, label="profile staging directory")
        if staging != birth._path or directory_binding(staging) != birth._binding:
            return False
        if not birth._native.matches(birth._binding):
            return False
        birth._native.close()
        if directory_binding(staging) != birth._binding:
            return False
        import shutil

        shutil.rmtree(staging)
        birth._consumed = True
        _ACTIVE_BIRTH_CAPABILITIES.pop(birth, None)
        return True
    except (OSError, ProfileProvenanceError):
        return False


def _make_user_dir(path: Path, *, parents: bool, exist_ok: bool) -> None:
    """Load the ACL-safe directory helper without requiring site-packages."""
    try:
        from hermes_dir_acl import make_user_dir
    except ModuleNotFoundError as exc:
        if exc.name != "hermes_dir_acl":
            raise
        root = str(Path(__file__).resolve().parents[1])
        if root not in sys.path:
            sys.path.insert(0, root)
        from hermes_dir_acl import make_user_dir
    make_user_dir(path, parents=parents, exist_ok=exist_ok)


def bootstrap_profile_home(
    home: Path, *, profile_kind: str = "default", profile_name: str | None = None,
    identity_root: Path | None = None,
) -> ProfileProvenance:
    """Exclusively create a clean home, identity, and authenticated origin receipt.

    Existing homes are never upgraded.  A valid unstarted receipt may be resumed
    after an installer crash; all markerless, malformed, copied, or mismatched
    homes remain refused/unknown.
    """
    home = _safe_path(home, label="profile home")
    identity_root = home if identity_root is None else _safe_path(
        identity_root, label="install identity root",
    )
    if profile_kind == "default" and identity_root != home:
        raise ProfileProvenanceError("default identity root must be the home")
    if os.path.lexists(home):
        identity = read_existing_install_id(identity_root)
        if identity is None:
            raise ProfileProvenanceError("existing home has no authenticated install identity")
        current = read_profile_provenance(home, install_id=identity)
        if (
            current.is_new_profile
            and current.profile_kind == profile_kind
            and current.profile_name == profile_name
        ):
            return current
        raise ProfileProvenanceError(current.reason or "existing home is not resumable")
    try:
        birth = create_exclusive_directory_birth(home)
    except FileExistsError as exc:
        raise ProfileProvenanceError("home appeared during exclusive creation") from exc
    identity = _create_install_id(identity_root)
    return publish_new_profile_origin(
        home,
        profile_kind=profile_kind,
        profile_name=profile_name,
        install_id=identity,
        birth=birth,
    )


def mark_profile_started(
    home: Path, *, owner_epoch: str, record_binding: tuple[object, ...], install_id: str,
) -> ProfileProvenance:
    """Make the authenticated one-way ``never_started`` → ``started`` transition."""
    if not isinstance(owner_epoch, str) or not owner_epoch:
        raise ProfileProvenanceError("owner epoch is invalid")
    if not isinstance(record_binding, tuple) or len(record_binding) != 5:
        raise ProfileProvenanceError("owner record binding is invalid")
    home = _safe_path(home, label="profile home")
    native = _NativeDirectoryHandle.open(home, protect_name=True)
    marker_written = False
    try:
        # All reads and the replacement are bound to one retained native directory object.  A
        # path replacement can therefore never redirect the marker write into a new home.
        current = read_profile_provenance(home, install_id=install_id, native=native)
        if not current.is_new_profile:
            raise ProfileProvenanceError(current.reason or "profile origin is not new")
        record = {
            "schema": PROVENANCE_SCHEMA,
            "kind": PROVENANCE_KIND,
            "profile_kind": current.profile_kind,
            "profile_name": current.profile_name,
            "install_id": current.install_id,
            "generation": current.generation,
            "state": STATE_STARTED,
            "birth_binding": list(current.birth_binding),
            "started_owner_epoch": owner_epoch,
            "started_record_binding": list(record_binding),
        }
        native.replace_child(
            PROVENANCE_FILENAME, (json.dumps(record, sort_keys=True) + "\n").encode("utf-8"),
        )
        marker_written = True
        result = read_profile_provenance(home, install_id=install_id, native=native)
        if (
            directory_binding(home) != current.birth_binding
            or not native.matches(current.birth_binding)
            or result.status != "valid"
            or result.state != STATE_STARTED
            or result.generation != current.generation
        ):
            raise ProfileProvenanceError("profile origin transition did not read back")
        return result
    except BaseException:
        if marker_written:
            with contextlib.suppress(OSError, ProfileProvenanceError):
                native.unlink_child(PROVENANCE_FILENAME)
        raise
    finally:
        native.close()


def discard_copied_runtime_origin(
    staging: Path, *, birth: ExclusiveDirectoryBirth, publish_target: Path,
) -> None:
    """Remove copied runtime markers from a private clone staging directory."""
    staging = _safe_path(staging, label="clone staging directory")
    publish_target = _safe_path(publish_target, label="profile publication target")
    if os.path.lexists(publish_target):
        raise ProfileProvenanceError("profile publication target appeared during clone")
    _validate_directory_birth(birth, staging)
    for name in (PROVENANCE_FILENAME, "gateway.lock"):
        path = Path(staging) / name
        if path.is_dir() and not path.is_symlink():
            raise ProfileProvenanceError(f"copied runtime marker is a directory: {name}")
        token = _ACTIVE_NATIVE_DIRECTORY.set(birth._native)
        try:
            try:
                birth._native.unlink_child(name)
            except FileNotFoundError:
                pass
        except OSError as exc:
            raise ProfileProvenanceError(f"copied runtime marker could not be removed: {name}") from exc
        finally:
            _ACTIVE_NATIVE_DIRECTORY.reset(token)
    _validate_directory_birth(birth, staging)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish one authenticated Hermes profile origin")
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--profile-kind", choices=("default", "named"), default="default")
    parser.add_argument("--profile-name")
    parser.add_argument("--identity-root", type=Path)
    args = parser.parse_args(argv)
    try:
        result = bootstrap_profile_home(
            args.home,
            profile_kind=args.profile_kind,
            profile_name=args.profile_name,
            identity_root=args.identity_root,
        )
    except ProfileProvenanceError as exc:
        print(json.dumps({"status": "refused", "reason": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps({"status": result.status, "state": result.state, "generation": result.generation}, sort_keys=True))
    return 0


__all__ = [
    "INSTALL_ID_FILENAME",
    "INSTALL_ID_LOCK_FILENAME",
    "PROVENANCE_FILENAME",
    "STATE_NEVER_STARTED",
    "STATE_STARTED",
    "ExclusiveDirectoryBirth",
    "ProfileProvenance",
    "ProfileProvenanceError",
    "bootstrap_profile_home",
    "create_exclusive_directory_birth",
    "directory_binding",
    "discard_copied_runtime_origin",
    "install_id_path",
    "mark_profile_started",
    "provenance_path",
    "publish_new_profile_origin",
    "read_existing_install_id",
    "read_profile_provenance",
]


if __name__ == "__main__":
    raise SystemExit(_main())
