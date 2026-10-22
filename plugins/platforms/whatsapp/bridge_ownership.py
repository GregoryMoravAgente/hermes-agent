"""Non-destructive bridge allocation for multiplexed secondary profiles."""
import hashlib
import json
import os
import secrets
import socket
from contextlib import suppress
from pathlib import Path

from .bridge_state_lock import BridgeStateLock, BridgeStateLockError

# 3000 stays the default profile's historical port; secondaries allocate above it. 999 ports
# covers any realistic number of paired phones on one host.
SECONDARY_PORT_FIRST = 3001
SECONDARY_PORT_LAST = 3999
_AUTHORITY_FILE = "bridge.authority.json"
_AUTHORITY_CLAIM = "bridge.authority.claim"
_OWNER_LOCKS: dict[str, BridgeStateLock] = {}


def _authority_path(session: Path) -> Path:
    return Path(session).resolve() / _AUTHORITY_FILE


def _authority_digest(binding: dict) -> str:
    encoded = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _private_text_writer(path: Path, text: str) -> None:
    """Use the existing owner-only writer; this module never exposes authority bytes."""
    from gateway.host_rendezvous import _write_private_text
    _write_private_text(path, text)


def _acquire_owner_lock(session: Path) -> BridgeStateLock:
    """Serialize authority-file reads/creation with the queue mutation journal."""
    key = str(Path(session).resolve())
    existing = _OWNER_LOCKS.get(key)
    if existing is not None:
        return existing
    lock = BridgeStateLock(Path(session).resolve() / "inbound-queue" / "mutation.sqlite3")
    try:
        lock.acquire()
    except BridgeStateLockError as exc:
        raise RuntimeError(f"bridge owner lock is {exc.code}") from exc
    _OWNER_LOCKS[key] = lock
    return lock


def release_bridge_authority(session: Path) -> None:
    """Release a held authority transaction during an explicit shutdown."""
    key = str(Path(session).resolve())
    lock = _OWNER_LOCKS.pop(key, None)
    if lock is not None:
        lock.close()


def load_bridge_authority(
    session: Path, *, session_digest: str, profile_digest: str, canonical_port: int,
    script_sha256: str, process_start: int | None = None,
) -> dict:
    """Load one durable private bridge authority, creating it without overwriting a race winner.

    Raw keys are returned only to the managed bootstrap/attach caller. They are never suitable for
    health output or public event payloads; the queue still proves the current process/epoch before
    accepting an operation.
    """
    session = Path(session).resolve()
    path = _authority_path(session)
    session.mkdir(parents=True, exist_ok=True)
    lock = _acquire_owner_lock(session)
    try:
        if path.exists():
            if path.is_symlink():
                raise RuntimeError("bridge authority path is linked")
            try:
                authority = json.loads(path.read_text(encoding="utf-8-sig"))
            except (OSError, UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError):
                raise RuntimeError("bridge authority is unreadable") from None
            if not isinstance(authority, dict):
                raise RuntimeError("bridge authority is malformed")
            expected = {
                "session_digest": session_digest,
                "profile_digest": profile_digest,
                "canonical_port": canonical_port,
                "script_sha256": script_sha256,
            }
            if any(authority.get(key) != value for key, value in expected.items()):
                raise RuntimeError("bridge authority identity mismatch")
            if not all(isinstance(authority.get(key), str) and authority[key] for key in (
                "attach_key_b64url", "source_witness_key_b64url", "attach_key_id", "source_witness_key_id",
            )):
                raise RuntimeError("bridge authority key material is incomplete")
            binding = {key: authority.get(key) for key in (
                "session_digest", "profile_digest", "canonical_port", "script_sha256", "process_start",
                "bridge_instance", "owner_epoch", "attach_key_id", "source_witness_key_id",
            )}
            if authority.get("binding_digest") != _authority_digest(binding):
                raise RuntimeError("bridge authority binding proof is invalid")
            if (
                authority.get("attach_key_id")
                != hashlib.sha256(str(authority["attach_key_b64url"]).encode("ascii")).hexdigest()
                or authority.get("source_witness_key_id")
                != hashlib.sha256(str(authority["source_witness_key_b64url"]).encode("ascii")).hexdigest()
            ):
                raise RuntimeError("bridge authority key identity is invalid")
            return authority
        if path.with_name(_AUTHORITY_CLAIM).exists():
            raise RuntimeError("bridge authority claim is busy or ambiguous")
        attach_key = secrets.token_urlsafe(32)
        witness_key = secrets.token_urlsafe(32)
        binding = {
            "session_digest": session_digest,
            "profile_digest": profile_digest,
            "canonical_port": canonical_port,
            "script_sha256": script_sha256,
            "process_start": process_start,
            "bridge_instance": None,
            "owner_epoch": None,
            "attach_key_id": hashlib.sha256(attach_key.encode("ascii")).hexdigest(),
            "source_witness_key_id": hashlib.sha256(witness_key.encode("ascii")).hexdigest(),
        }
        authority = {
            "schema_version": 1,
            **binding,
            "attach_key_b64url": attach_key,
            "source_witness_key_b64url": witness_key,
            "binding_digest": _authority_digest(binding),
            "authority_state": "pending",
        }
        _private_text_writer(path, json.dumps(authority, sort_keys=True, separators=(",", ":")))
        return authority
    except Exception:
        _OWNER_LOCKS.pop(str(session), None)
        lock.close()
        raise
    finally:
        if _OWNER_LOCKS.get(str(session)) is lock:
            _OWNER_LOCKS.pop(str(session), None)
        lock.close()


def read_bridge_handoff(session: Path) -> dict | None:
    """Read only the durable predecessor envelope; callers must still prove its private authority."""
    meta = Path(session).resolve() / "inbound-queue" / "meta.json"
    try:
        value = json.loads(meta.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError):
        return None
    handoff = value.get("owner_handoff") if isinstance(value, dict) else None
    return dict(handoff) if isinstance(handoff, dict) else None


def port_is_free(port: int) -> bool:
    """Whether the WhatsApp Node bridge could bind ``port`` on 127.0.0.1 right now.

    Contract: this answers for the bridge's own bind semantics, not for an arbitrary binder. On
    POSIX the bridge (Express/libuv) sets SO_REUSEADDR, so a port holding only TIME-WAIT entries
    reads as free here even though a binder without SO_REUSEADDR would get EADDRINUSE; a live
    listener still reads as busy. Its only callers are this module's bridge-port allocation and
    ownership checks. Do not reuse it to gate a process that binds without SO_REUSEADDR.
    """
    with socket.socket() as sock:
        # A just-stopped bridge can leave its accepted TCP connections in TIME-WAIT. The Node
        # bridge enables this option too, so match its bind semantics while still rejecting a listener.
        # POSIX only, like libuv: on Windows SO_REUSEADDR lets a bind share a live listener's port,
        # which would read a foreign bridge as free (Windows binds past TIME-WAIT without it).
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _read_port_record(record: Path) -> int:
    try:
        return int(record.read_text(encoding="utf-8-sig").strip())
    except ValueError:
        raise ValueError(f"Bridge port record {record} is not a port number; delete it to allocate a new port") from None


def _sibling_claims(home: Path) -> set:
    """Ports other profiles' records claim. A sibling whose bridge is down leaves its port unbound, so a
    bind probe alone would hand it to this profile and the sibling would hit a conflict on its next start."""
    claims = set()
    for record in home.parent.glob("*/platforms/whatsapp/bridge_port"):
        if record.parents[2] != home:
            with suppress(OSError, ValueError):
                claims.add(_read_port_record(record))
    return claims


def secondary_bridge_port(home: Path, explicit) -> int:
    record = home / "platforms/whatsapp/bridge_port"
    if explicit is not None:
        port = int(explicit)
    elif record.exists():
        port = _read_port_record(record)
    else:
        claimed = _sibling_claims(home)
        port = next((p for p in range(SECONDARY_PORT_FIRST, SECONDARY_PORT_LAST + 1)
                     if p not in claimed and port_is_free(p)), None)
        if port is None:
            raise ValueError(f"No free bridge port in {SECONDARY_PORT_FIRST}..{SECONDARY_PORT_LAST}")
        record.parent.mkdir(parents=True, exist_ok=True)
        record.write_text(str(port), encoding="utf-8")
    if not 1 <= port <= 65535:
        raise ValueError(f"Invalid bridge port {port}")
    return port


def standalone_bridge_port(home: Path, explicit) -> int:
    """Port for the launch profile's startup and for out-of-process sends; never allocates.

    Explicit config wins, then the profile's persisted record (written when it served as a
    secondary), then the historical default 3000. Startup and delivery share this one
    resolution so they always target the same bridge.
    """
    if explicit is not None:
        port = int(explicit)
    else:
        record = home / "platforms/whatsapp/bridge_port"
        if not record.exists():
            return 3000
        port = _read_port_record(record)
    if not 1 <= port <= 65535:
        raise ValueError(f"Invalid bridge port {port}")
    return port


def check_secondary_ownership(session: Path, port: int) -> str:
    """``"free"`` when the port is unbound, ``"ours"`` when it is bound and this profile's own
    ``bridge.pid`` names a live process (pid plus kernel start time, the same fingerprint the default
    path trusts) that was spawned on this same port, so a bridge orphaned by a gateway crash can be
    adopted or reaped by identity. A live recorded bridge on a different port proves nothing about
    who holds this one. Anything else bound on the port belongs to someone else and is a fatal for
    this profile: no HTTP probe is safe before ownership is known, a healthy listener can still be
    another phone, and a secondary never signals a process it cannot prove is its own."""
    if port_is_free(port):
        return "free"
    from .adapter import _bridge_pid_is_ours, _read_bridge_pidfile
    pid, recorded_start, recorded_port = _read_bridge_pidfile(session)
    if pid is not None and recorded_port == port and _bridge_pid_is_ours(pid, recorded_start):
        return "ours"
    raise ValueError(f"Bridge port {port} is bound by a process this profile does not own")


def check_bridge_owner(
    session: Path, port: int, *, profile_digest: str | None = None,
    script_sha256: str | None = None, health: dict | None = None,
) -> dict:
    """Return a fail-closed physical owner proof for either primary or secondary.

    Health is only a confirmation after the durable PID/start/port record and
    listener have identified the process.  A healthy listener without that
    evidence is ambiguous and is never adopted or killed.
    """
    session = Path(session).resolve()
    from .adapter import _bridge_pid_is_ours, _read_bridge_pidfile
    pid, recorded_start, recorded_port = _read_bridge_pidfile(session)
    if pid is None or recorded_port != port or not _bridge_pid_is_ours(pid, recorded_start):
        return {"status": "foreign_or_ambiguous", "reason": "pid_start_port_proof_missing"}
    if port_is_free(port):
        return {"status": "foreign_or_ambiguous", "reason": "listener_missing"}
    lock_path = session / "inbound-queue" / "owner-lock.sqlite3"
    if not lock_path.exists():
        return {"status": "foreign_or_ambiguous", "reason": "owner_lock_missing"}
    probe = BridgeStateLock(lock_path)
    try:
        probe.acquire()
    except BridgeStateLockError as exc:
        if exc.code != "owner_busy":
            return {"status": "foreign_or_ambiguous", "reason": "owner_lock_unknown"}
        owner_lock = "busy"
    else:
        probe.release()
        return {"status": "foreign_or_ambiguous", "reason": "owner_lock_not_held"}
    if not isinstance(health, dict):
        return {"status": "owned_physical", "pid": pid, "process_start": recorded_start, "port": port,
                "owner_lock": owner_lock}
    if health.get("session") and Path(str(health["session"])).resolve() != session:
        return {"status": "foreign_or_ambiguous", "reason": "session_mismatch"}
    if script_sha256 and health.get("scriptHash"):
        expected = script_sha256.lower()
        observed = str(health["scriptHash"]).lower()
        if len(expected) == 64 and observed != expected[:16] and observed != expected:
            return {"status": "foreign_or_ambiguous", "reason": "script_mismatch"}
    observed_profile = health.get("profileDigest")
    if observed_profile is None and isinstance(health.get("queue"), dict):
        queue_owner = health["queue"].get("owner")
        if isinstance(queue_owner, dict):
            observed_profile = queue_owner.get("profile_digest")
    if profile_digest and observed_profile != profile_digest:
        return {"status": "foreign_or_ambiguous", "reason": "profile_mismatch"}
    return {
        "status": "owned",
        "pid": pid,
        "process_start": recorded_start,
        "port": port,
        "session": str(session),
        "script_digest": script_sha256,
        "owner_lock": owner_lock,
    }
