"""WhatsApp platform adapter (Baileys bridge): a Node.js bridge process runs the WhatsApp Web
client; messages are polled over a local HTTP API and responses are posted back through it."""

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import os
import platform
import re
import signal
import struct
import subprocess
from uuid import uuid4
from contextlib import suppress
from functools import wraps
from pathlib import Path
from typing import Dict, Optional, Any

from gateway.platforms._shared import (
    apply_yaml_bridge as _apply_yaml_bridge, extra_or_secret as _extra_or_secret, get_scoped_secret, send_error
)
from gateway.platforms.send_targets import StandaloneSendPolicy, regex_stripped_parser
from hermes_constants import (find_node_executable, get_hermes_dir, with_hermes_node_path)
from .bridge_connect import WhatsAppBridgeConnectMixin
from .bridge_preservation import BridgePreservationMixin

_IS_WINDOWS = platform.system() == "Windows"


def _wenv(name: str, default: str = "") -> str:
    """WHATSAPP_* env via the profile secret scope (multiplexed profiles see their own .env, not the process-global value)."""
    return get_scoped_secret(name, default)

logger = logging.getLogger(__name__)

# Owner-typed inbound text is prefixed at MessageEvent construction so transcripts stay disambiguated before silent_ingest.
_OWNER_REPLY_PREFIX = "[owner reply] "

_RUN_TEXT = dict(capture_output=True, text=True, encoding='utf-8', errors='replace', stdin=subprocess.DEVNULL)

_ACK_PAYLOAD_MAX_BYTES = 1024 * 1024


def _encode_ack_payload_snapshot(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Encode one bounded ACK body for same-occurrence response-loss replay."""
    try:
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > _ACK_PAYLOAD_MAX_BYTES:
            return None
        decoded = json.loads(encoded)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    return (encoded, decoded) if isinstance(decoded, dict) else None


def _load_ack_payload_snapshot(
    delivery: dict[str, Any], request_id: str,
) -> tuple[str, dict[str, Any]] | None:
    """Load only an intact source-owned ACK body for its original request id."""
    encoded = delivery.get("_ack_payload_snapshot")
    digest = delivery.get("_ack_payload_snapshot_digest")
    if (
        not isinstance(encoded, str)
        or len(encoded.encode("utf-8")) > _ACK_PAYLOAD_MAX_BYTES
        or not isinstance(digest, str)
        or delivery.get("_ack_payload_request_id") != request_id
    ):
        return None
    try:
        if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != digest:
            return None
        payload = json.loads(encoded)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    if (
        not isinstance(payload, dict)
        or payload.get("request_id") != request_id
        or payload.get("op") != "ack"
        or not isinstance(payload.get("settlement"), dict)
    ):
        return None
    return encoded, payload


def _bridge_boundary_errors():
    """Return only expected HTTP, transport, and response-decoding failures."""
    import aiohttp

    return (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        OSError,
        RuntimeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    )


def _listener_pids_on_port(port: int) -> list:
    """PIDs *listening* on ``port`` (POSIX), never clients — a bare ``lsof -i :PORT`` once killed the user's browser."""
    pids: list = []
    with suppress(FileNotFoundError):  # lsof not installed — fall through to ss
        pids = _safe_ints(subprocess.run(["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"], timeout=5, **_RUN_TEXT, check=False).stdout.strip().splitlines())
        if pids:
            return pids
    with suppress(FileNotFoundError):
        pids.extend(int(m.group(1)) for m in re.finditer(r"pid=(\d+)", subprocess.run(["ss", "-ltnHp", f"sport = :{port}"], timeout=5, **_RUN_TEXT, check=False).stdout))
    return pids


def _safe_ints(tokens) -> list:
    out: list = []
    for tok in tokens:
        try:
            out.append(int(tok))
        except ValueError:
            pass
    return out


def _session_dir(extra) -> Path:
    """Absolute session dir: --session, bridge.pid and every /health ``session`` comparison (gateway and standalone
    sends) share this one string, or a gateway restarted from another cwd would see its own bridge as foreign."""
    return Path(os.path.abspath(os.path.expanduser(str((extra or {}).get(
        "session_path", get_hermes_dir("platforms/whatsapp/session", "whatsapp/session"))))))


def _windows_listener_pids(port: int) -> list:
    """PIDs in LISTENING state on ``port`` via netstat (Windows)."""
    from hermes_cli._subprocess_compat import windows_hide_flags
    result = subprocess.run(["netstat", "-ano", "-p", "TCP"], timeout=5, creationflags=windows_hide_flags(), **_RUN_TEXT, check=False)
    rows = (line.split() for line in result.stdout.splitlines())
    return _safe_ints(p[4] for p in rows if len(p) >= 5 and p[3] == "LISTENING" and p[1].endswith(f":{port}"))


def _port_listener_pids(port: int) -> list:
    """Listening PIDs on ``port`` for this platform: netstat on Windows, lsof/ss elsewhere."""
    return _windows_listener_pids(port) if _IS_WINDOWS else _listener_pids_on_port(port)


def _pid_looks_like_node_bridge(pid: int) -> bool:
    """Fail-closed: the live process must be a ``node`` executable (a scan-time PID can be a stranger by kill time).

    ``_kill_port_process`` discovers PIDs from a netstat/lsof scan of a TCP port — a bare number naming a
    *stranger* process (#89614 class: an unverified scan-time PID force-killed later can be anything,
    including a critical system process). Before any kill, require the live process to actually look like
    our Baileys bridge: a ``node`` executable. Any ambiguity (process gone, unreadable cmdline) refuses the
    kill.
    """
    try:
        import psutil
        proc = psutil.Process(pid)
        return "node" in (proc.name() or "").lower() or "node" in " ".join(proc.cmdline() or []).lower().split(" ", 1)[0]
    except Exception:
        return False


def _kill_port_process(port: int) -> None:
    """Kill any node bridge *listening* on the given TCP port (never a client); SIGTERM on POSIX, taskkill /F on Windows."""
    with suppress(Exception):
        for pid in _port_listener_pids(port):
            # Killing a mistyped or recycled PID is unrecoverable — verify first.
            if pid <= 0 or not _pid_looks_like_node_bridge(pid):
                logger.warning("[whatsapp] Not killing PID %s on port %d: process is not a node bridge (or identity unverifiable)", pid, port)
                continue
            if _IS_WINDOWS:
                from hermes_cli._subprocess_compat import windows_hide_flags
                # Only SubprocessError is swallowed per-PID; an OSError (e.g. taskkill missing) aborts the scan.
                with suppress(subprocess.SubprocessError):
                    subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, stdin=subprocess.DEVNULL, timeout=5, creationflags=windows_hide_flags(), check=False)
            else:
                with suppress(OSError):  # ProcessLookupError/PermissionError are OSError subclasses
                    os.kill(pid, signal.SIGTERM)


def _bridge_pid_is_ours(pid: int, expected_start) -> bool:
    """``pid`` alive AND still our bridge: kernel start time (definitive); fail closed without it.

    Legacy pidfiles record only the PID. The old fallback accepted a ``node`` + session-path cmdline
    substring as kill evidence — but a log tail, editor, or grep that merely *mentions* the session
    path matches that same substring (#116883), so a legacy pidfile could signal a stranger. Without
    a start-time fingerprint the caller must reap via the bridge-port scan instead.
    """
    from gateway import status
    if not status._pid_exists(pid):
        return False
    if expected_start is None:
        return False
    return status.get_process_start_time(pid) == expected_start


def _unlink_quietly(path: Path) -> None:
    with suppress(OSError):
        path.unlink()


def _read_bridge_pidfile(session_path: Path) -> tuple:
    """``(pid, kernel_start_time, port)`` from ``bridge.pid``; all ``None`` when absent or unparseable.
    Line 1 = pid, optional line 2 = kernel start time, optional line 3 = the ``--port`` it was spawned
    with (legacy files: pid only, or pid + start time)."""
    pid_file = session_path / "bridge.pid"
    if not pid_file.exists():
        return None, None, None
    try:
        lines = [ln.strip() for ln in pid_file.read_text(encoding="utf-8-sig").split("\n")]
        pid = int(lines[0])
        recorded_start = int(lines[1]) if len(lines) > 1 and lines[1] else None
        recorded_port = int(lines[2]) if len(lines) > 2 and lines[2] else None
    except (ValueError, OSError, TypeError, IndexError):
        return None, None, None
    return pid, recorded_start, recorded_port


def _kill_stale_bridge_by_pidfile(session_path: Path) -> None:
    """Kill an orphaned bridge recorded in ``bridge.pid``, after :func:`_bridge_pid_is_ours`."""
    from gateway.status import _pid_exists
    pid_file = session_path / "bridge.pid"
    if not pid_file.exists():
        return
    pid, recorded_start, _ = _read_bridge_pidfile(session_path)
    if pid is None:
        _unlink_quietly(pid_file)
        return
    if _bridge_pid_is_ours(pid, recorded_start):
        with suppress(OSError):  # ProcessLookupError / PermissionError included
            os.kill(pid, signal.SIGTERM)
            logger.info("[whatsapp] Killed stale bridge PID %d from pidfile", pid)
    elif _pid_exists(pid):
        reason = ("legacy pidfile lacks a start-time fingerprint and cmdline substring evidence can name a stranger"
                  if recorded_start is None else "it is no longer the bridge (recycled onto an unrelated process)")
        logger.warning("[whatsapp] Not killing pidfile PID %d: %s; "
                       "skipping to avoid killing a stranger.", pid, reason)
    _unlink_quietly(pid_file)


def _write_bridge_pidfile(session_path: Path, pid: int, port: Optional[int] = None) -> None:
    """Write the bridge PID plus its kernel start time (line 2) for identity-checked cleanup, and the
    port it listens on (line 3) so a secondary adopts only the endpoint that process actually serves."""
    with suppress(OSError):
        from gateway.status import get_process_start_time
        start = get_process_start_time(pid)
        text = str(pid) if start is None else f"{pid}\n{start}" + ("" if port is None else f"\n{port}")
        (session_path / "bridge.pid").write_text(text, encoding="utf-8")


def _terminate_bridge_process(proc, *, force: bool = False) -> None:
    """Terminate the bridge process using process-tree semantics where possible."""
    action = "kill" if force else "terminate"
    if _IS_WINDOWS:
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T"] + (["/F"] if force else []),
                stdin=subprocess.DEVNULL, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=10,
                check=False,
            )
        except FileNotFoundError:
            return getattr(proc, action)()
        if result.returncode != 0:
            raise OSError((result.stderr or result.stdout or "").strip() or f"taskkill failed for PID {proc.pid}")
        return
    import psutil
    with suppress(psutil.NoSuchProcess):
        parent = psutil.Process(proc.pid)
        for child in parent.children(recursive=True):
            with suppress(psutil.NoSuchProcess):
                getattr(child, action)()
        getattr(parent, action)()

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from gateway.config import Platform, PlatformConfig
from gateway.platforms.whatsapp_common import WhatsAppBehaviorMixin
from gateway.whatsapp_identity import normalize_whatsapp_mention_jid, to_whatsapp_jid
from gateway.platforms.base import (
    BasePlatformAdapter, SendResult, SUPPORTED_DOCUMENT_TYPES, cache_image_from_url, cache_audio_from_url,
)
from gateway.platforms.helpers import cancel_task
from gateway.platforms.event import MessageEvent, MessageType
from utils import env_int


def _cache_dirs() -> tuple:
    """``(image, audio, video, document)`` cache dirs, resolved per call so a profile override's cache matches."""
    from gateway.platforms.base import get_audio_cache_dir, get_document_cache_dir, get_image_cache_dir, get_video_cache_dir
    return get_image_cache_dir(), get_audio_cache_dir(), get_video_cache_dir(), get_document_cache_dir()


def _is_allowed_bridge_path(url: str) -> bool:
    """Absolute bridge path resolves (symlinks included) inside a Hermes cache dir — a rogue bridge could hand back /etc/passwd."""
    try:
        resolved = Path(url).resolve()
    except (OSError, ValueError):
        return False
    for root in _cache_dirs():
        with suppress(OSError, ValueError):
            if resolved.is_relative_to(Path(root).resolve()):
                return True
    return False


def _file_content_hash(path: Path) -> str:
    """First 16 hex chars of SHA-256 of *path* ("" if unreadable); bridge.js reports its own as ``/health`` ``scriptHash``."""
    import hashlib
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        return ""


def check_whatsapp_requirements() -> bool:
    """
    Check if WhatsApp dependencies are available.

    WhatsApp requires a Node.js bridge for most implementations.
    """
    _node = find_node_executable("node")
    if not _node:
        from pm import lazy_installs_allowed

        # Let connect prepare a missing runtime, but never install during discovery.
        return lazy_installs_allowed()
    try:
        return subprocess.run([_node, "--version"], timeout=5, env=with_hermes_node_path(), **_RUN_TEXT, check=False).returncode == 0
    except Exception:
        return False


def _import_aiohttp():
    import aiohttp
    if not hasattr(aiohttp, "ClientSession"):  # half-removed install leaves a namespace shell (#71308)
        where = getattr(aiohttp, "__file__", None) or list(getattr(aiohttp, "__path__", ()))  # a plain aiohttp.py has no __path__
        raise ImportError(f"aiohttp at {where} is an incomplete install")
    return aiohttp


def ensure_aiohttp() -> Optional[tuple[str, bool]]:
    """None once aiohttp imports (PM installs the ``sms`` extra, which is exactly aiohttp, when it is missing), else
    (why not, retryable).

    Every bridge call imports aiohttp; a sealed env without it used to fail each /health poll silently and loop
    forever on "did not start in 15s" while the bridge was healthy (#126358).
    """
    try:
        _import_aiohttp()
        return None
    except ImportError as exc:
        if not isinstance(exc, ModuleNotFoundError):
            return str(exc), False  # present but broken: PM sees it as installed, reinstalling is the user's call
    from pm.environments import running_from_selected_environment
    from pm.environments_adopt import restart_needed
    from pm.extras import ensure_import
    from pm.install import lazy_installs_allowed
    from pm.paths import repo_root

    # Read before the sync moves the selection. A foreign interpreter (dev venv, Nix) can never lazy-install; one an
    # update left on an older generation can after a restart.
    root = repo_root()
    installable = lazy_installs_allowed() and (running_from_selected_environment(root) or restart_needed(root) is not None)
    try:
        ensure_import("sms")
    except (RuntimeError, OSError, ValueError) as exc:  # InstallError/DownloadError; anything else escapes as transient
        # Only policy or a foreign interpreter is final. A busy dependency lock, a failed download and "installed;
        # restart Hermes to activate it" stay retryable: as final errors they exit the gateway 78 and keep it down.
        return str(exc) or type(exc).__name__, installable
    try:
        _import_aiohttp()
        return None
    except ImportError as exc:
        return f"{exc} after installing the sms extra", False


# Env vars bridge.js consumes; injected because a multiplexed subprocess's os.environ lacks the secondary profile's .env.
_BRIDGE_PASSTHROUGH_ENV = (
    "WHATSAPP_ALLOWED_USERS", "WHATSAPP_ALLOW_FROM", "WHATSAPP_DM_POLICY", "WHATSAPP_GROUP_POLICY",
    "WHATSAPP_GROUP_ALLOWED_USERS", "WHATSAPP_GROUP_ALLOW_FROM", "WHATSAPP_REQUIRE_MENTION",
    "WHATSAPP_MENTION_PATTERNS", "WHATSAPP_FREE_RESPONSE_CHATS", "WHATSAPP_DEBUG",
    "WHATSAPP_FORWARD_OWNER_MESSAGES", "WHATSAPP_MAX_MESSAGE_LENGTH",
    "WHATSAPP_CHUNK_DELAY_MS", "WHATSAPP_SEND_TIMEOUT_MS",
)
_TEXT_INJECT_EXTS = {".txt", ".md", ".csv", ".json", ".xml", ".yaml", ".yml", ".log", ".py", ".js", ".ts", ".html", ".css"}
_MAX_TEXT_INJECT_BYTES = 100 * 1024  # matches Telegram/Discord/Slack
_NATIVE_MEDIA_TYPES = {"location": MessageType.LOCATION, "live_location": MessageType.LOCATION, "sticker": MessageType.STICKER}
# Inbound mediaType substring → kind; ptt = WhatsApp voice note, so "ptt" must precede "audio".
_MEDIA_NEEDLES = (("image", MessageType.PHOTO), ("video", MessageType.VIDEO), ("ptt", MessageType.VOICE), ("audio", MessageType.AUDIO))
# MessageType → (bridge label, default mime); documents take their mime from SUPPORTED_DOCUMENT_TYPES instead.
_MEDIA_INFO = {
    MessageType.PHOTO: ("image", "image/jpeg"), MessageType.VOICE: ("audio", "audio/ogg"), MessageType.AUDIO: ("audio", "audio/mpeg"),
    MessageType.VIDEO: ("video", "video/mp4"), MessageType.DOCUMENT: ("document", ""),
}
_MEDIA_TYPE_BY_BRIDGE_KIND = {"image": MessageType.PHOTO, "video": MessageType.VIDEO, "audio": MessageType.VOICE, "document": MessageType.DOCUMENT}
_QUOTED_MIME_BY_BRIDGE_KIND = {
    "image": "image/jpeg", "video": "video/mp4", "gif": "video/mp4", "audio": "audio/ogg", "ptt": "audio/ogg",
    "document": "application/octet-stream", "sticker": "image/webp",
}


def _needs_bridge(method):
    """Adapter-method decorator: return ``SendResult(error=...)`` when ``_bridge_unavailable()`` says so."""
    @wraps(method)
    async def _guarded(self, *args, **kwargs):
        unavailable = await self._bridge_unavailable()
        return SendResult(success=False, error=unavailable) if unavailable else await method(self, *args, **kwargs)
    return _guarded


class WhatsAppAdapter(
    WhatsAppBehaviorMixin, WhatsAppBridgeConnectMixin, BridgePreservationMixin, BasePlatformAdapter
):
    """Transport over a local Node.js (Baileys) HTTP bridge; behavior lives in ``WhatsAppBehaviorMixin``. config.extra: bridge_script /
    bridge_port (3000) / session_path, dm_policy / group_policy (open|allowlist|disabled|pairing), allow_from / group_allow_from, send_read_receipts."""

    def _preservation_source_contract(self):
        from gateway.preservation_source_close import SourceCloseContract
        return SourceCloseContract.for_adapter(self, name="whatsapp-poll-source")

    _DEFAULT_BRIDGE_DIR = None  # resolved in __init__
    splits_long_messages = True  # send() chunks via truncate_message()
    serial_receive_loop = True  # the bridge poll loop awaits each media message before the next

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WHATSAPP)
        if WhatsAppAdapter._DEFAULT_BRIDGE_DIR is None:
            from gateway.platforms.whatsapp_common import resolve_whatsapp_bridge_dir
            WhatsAppAdapter._DEFAULT_BRIDGE_DIR = resolve_whatsapp_bridge_dir()
        extra = config.extra
        from hermes_constants import get_hermes_home
        self._profile_home = get_hermes_home()
        self._bridge_process: Optional[subprocess.Popen] = None
        self._foreign_bridge_session: Optional[str] = None  # set by _reuse_running_bridge when /health names another profile's session
        self._bridge_probe_timed_out = False  # set by _reuse_running_bridge when the port's holder gave no /health answer
        from .bridge_ownership import standalone_bridge_port
        try:
            # Same resolution as out-of-process sends, so a profile that recorded a port while serving as a
            # secondary keeps it when it launches; a secondary re-resolves (and may allocate) in connect().
            self._bridge_port: int = standalone_bridge_port(self._profile_home, extra.get("bridge_port"))
        except ValueError as exc:
            logger.warning("[whatsapp] %s; using port 3000", exc)
            self._bridge_port = 3000
        self._bridge_script: str = extra.get("bridge_script", str(self._DEFAULT_BRIDGE_DIR / "bridge.js"))
        self._session_path = _session_dir(extra)
        self._reply_prefix: Optional[str] = extra.get("reply_prefix")
        self._dm_policy = str(_extra_or_secret(extra, "dm_policy", "WHATSAPP_DM_POLICY", "pairing")).strip().lower()
        self._allow_from = self._coerce_allow_list(self._select_dm_allowlist(extra, ("WHATSAPP_ALLOWED_USERS",), _wenv))
        self._group_policy = str(_extra_or_secret(extra, "group_policy", "WHATSAPP_GROUP_POLICY", "pairing")).strip().lower()
        # Same precedence as the DM list. Until #72529 the env carrier only reached the Node bridge, so
        # env-only installs gated groups on an empty allowlist.
        _, raw_groups = self._select_allowlist(
            extra, ("group_allow_from", "groupAllowFrom"), ("WHATSAPP_GROUP_ALLOW_FROM", "WHATSAPP_GROUP_ALLOWED_USERS"), _wenv)
        self._group_allow_from = self._coerce_allow_list(raw_groups)
        rr = extra.get("send_read_receipts", False)
        self._send_read_receipts = rr if isinstance(rr, bool) else str(rr or "").strip().lower() in {"1", "true", "yes", "on"}
        self._mention_patterns = self._compile_mention_patterns()
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._bridge_log_fh = self._bridge_log = self._poll_task = self._http_session = None
        self._bridge_log_open_task = None
        self._bridge_log_open_close_requested = False
        self._bridge_binding = None
        self._bridge_authority = None
        self._bridge_capability: Optional[str] = None
        self._bridge_witness_key: Optional[bytes] = None
        self._bridge_capability_required = False
        self._consumer_id = uuid4().hex
        self._consumer_epoch = 1
        self._preservation_bridge_close_task = None
        self._preservation_bridge_resume_task = None
        self._preservation_bridge_resume_result = False
        self._preservation_bridge_snapshot = None
        self._preservation_bridge_close_generation = None
        # Set by disconnect() before SIGTERMing so _check_managed_bridge_exit() can tell an intentional exit (-15/-2/0) from a crash.
        self._shutting_down = False
        # Text debounce batching: rapid bursts (forwards, paste-splits) would otherwise each trigger a separate agent turn.
        # Telegram cadence and ceilings (#44883); ``0`` dispatches each message immediately.
        self._configure_text_batch_delays()

    def _bridge_url(self, path: str) -> str:
        return f"http://127.0.0.1:{self._bridge_port}/{path}"

    def _bridge_identity(self) -> dict[str, Any]:
        return {
            "session_digest": hashlib.sha256(str(self._session_path.resolve()).encode()).hexdigest(),
            "profile_digest": hashlib.sha256(str(self._profile_home.resolve()).encode()).hexdigest(),
            "canonical_port": self._bridge_port,
            "script_sha256": _file_content_hash(Path(self._bridge_script)),
        }

    def _load_bridge_authority(self, process_start: int | None = None) -> dict[str, Any] | None:
        from .bridge_ownership import load_bridge_authority
        identity = self._bridge_identity()
        try:
            authority = load_bridge_authority(
                self._session_path,
                **identity,
                process_start=process_start,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            logger.warning("[%s] Managed WhatsApp bridge authority is unavailable", self.name)
            return None
        self._bridge_authority = authority
        self._bridge_capability = authority.get("attach_key_b64url")
        witness = authority.get("source_witness_key_b64url")
        self._bridge_witness_key = witness.encode("utf-8") if isinstance(witness, str) else None
        self._bridge_capability_required = True
        return authority

    def _bridge_req(self, method: str, path: str, timeout: float, **kwargs):
        """``session.<method>`` context manager for a bridge endpoint (caller must ``async with``)."""
        import aiohttp
        headers = dict(kwargs.pop("headers", {}) or {})
        capability = getattr(self, "_bridge_capability", None)
        if capability:
            headers.setdefault("X-Hermes-Bridge-Capability", capability)
        if headers:
            kwargs["headers"] = headers
        return getattr(self._http_session, method)(self._bridge_url(path), **kwargs, timeout=aiohttp.ClientTimeout(total=timeout))

    async def _probe_bridge_health(self) -> tuple[bool, Any]:
        """GET /health with a fresh session → ``(http_200, json)``; unparseable 200 body → ``(True, None)``; connection errors and timeouts propagate."""
        import aiohttp
        async with aiohttp.ClientSession() as session, session.get(self._bridge_url("health"), timeout=aiohttp.ClientTimeout(total=2)) as resp:
            if resp.status != 200:
                return False, None
            try:
                return True, await resp.json()
            except TimeoutError:
                raise  # a body that never arrives is no answer, the same as headers that never arrive
            except Exception:
                return True, None

    def _ensure_bridge_deps(self, bridge_dir: Path) -> bool:
        """npm install when node_modules is missing OR package.json hash != stamp file. False = fatal error set."""
        _dep_stamp = bridge_dir / "node_modules" / ".hermes-pkg-hash"  # holds the package.json hash of the last install
        _pkg_hash = _file_content_hash(bridge_dir / "package.json")
        try:
            if (bridge_dir / "node_modules").exists() and _dep_stamp.read_text(encoding="utf-8-sig").strip() == _pkg_hash and bool(_pkg_hash):
                return True
        except OSError:
            pass
        print(f"[{self.name}] Installing WhatsApp bridge dependencies...")
        detail = ""
        try:  # Default 300s accommodates slow systems like an Unraid NAS.
            import pm

            _npm_bin = find_node_executable("npm")
            env = with_hermes_node_path()
            if _npm_bin is None:
                env = pm.ensure("npm").env
                installed = pm.installed_package("npm")
                if installed is None or installed.binary is None:
                    raise pm.InstallError("npm", "ensured but no selected binary was recorded")
                _npm_bin = str(installed.binary)
            install_result = subprocess.run([_npm_bin, "install", "--silent"], cwd=str(bridge_dir), timeout=env_int("WHATSAPP_NPM_INSTALL_TIMEOUT", 300),
                                            env=env, **_RUN_TEXT, check=False)
            if install_result.returncode == 0:
                print(f"[{self.name}] Dependencies installed")
                with suppress(OSError):  # Stamp is an optimization; install still succeeded
                    if _pkg_hash:
                        _dep_stamp.write_text(_pkg_hash, encoding="utf-8")
                return True
            print(f"[{self.name}] npm install failed: {install_result.stderr}")
            detail = f" ({install_result.stderr.strip()[-500:]})" if install_result.stderr else ""
        except Exception as e:
            print(f"[{self.name}] Failed to install dependencies: {e}")
            detail = f" ({e})"
        self._set_fatal_error("whatsapp_npm_install_failed", f"WhatsApp bridge npm install failed{detail}. "
                              "Run `hermes whatsapp`, then restart `hermes gateway`.", retryable=False)
        return False

    def _start_bridge_polling(self) -> None:
        poll_task = getattr(self, "_poll_task", None)
        if poll_task is None or poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_messages())

    def _attach_to_bridge(self, managed_process, *, start_polling: bool = True) -> None:
        import aiohttp
        self._bridge_process = managed_process
        self._http_session = aiohttp.ClientSession()
        self._consumer_id = uuid4().hex
        self._consumer_epoch += 1
        self._bridge_binding = None
        self._bridge_producer_generation = None
        self._preservation_bridge_resume_result = False
        self._poll_task = None
        if start_polling:
            self._start_bridge_polling()

    def _queue_binding(self) -> Optional[dict[str, Any]]:
        binding = getattr(self, "_bridge_binding", None)
        if not isinstance(binding, dict):
            return None
        return {
            **binding,
            "session_path": str(self._session_path.resolve()),
            "profile": getattr(self, "_preservation_source_profile", None),
            "consumer_id": getattr(self, "_consumer_id", None),
            "consumer_epoch": getattr(self, "_consumer_epoch", None),
        }

    async def _ensure_queue_binding(self) -> Optional[dict[str, Any]]:
        binding = self._queue_binding()
        if binding is not None:
            return binding
        if not getattr(self, "_http_session", None):
            return None
        ok, data = await self._probe_bridge_health()
        queue = data.get("queue") if isinstance(data, dict) else None
        if not ok or not isinstance(queue, dict):
            return None
        if (
            not queue.get("queue_epoch")
            or not queue.get("bridge_instance")
            or not isinstance(queue.get("owner_epoch"), int)
            or queue.get("owner_epoch") < 1
        ):
            return None
        self._bridge_binding = {
            "queue_epoch": str(queue["queue_epoch"]),
            "bridge_instance": str(queue["bridge_instance"]),
            "owner_epoch": queue.get("owner_epoch"),
        }
        if queue.get("capability_required"):
            self._bridge_capability_required = True
            capability = getattr(self, "_bridge_capability", None)
            if not isinstance(capability, str) or len(capability) < 24:
                return None
            if queue.get("capability_digest") and hashlib.sha256(capability.encode("utf-8")).hexdigest() != queue.get("capability_digest"):
                return None
            if isinstance(queue.get("consumer_id"), str) and isinstance(queue.get("consumer_epoch"), int):
                self._consumer_id = queue["consumer_id"]
                self._consumer_epoch = queue["consumer_epoch"]
        else:
            self._bridge_capability_required = False
            self._bridge_capability = None
        self._bridge_binding.update({
            "session_digest": queue.get("session_digest"),
            "profile_digest": queue.get("profile_digest"),
            "canonical_port": queue.get("canonical_port"),
            "script_sha256": queue.get("script_sha256"),
            "process_start": queue.get("process_start"),
            "producer_generation": queue.get("producer_generation"),
            "listener_ready": queue.get("listener_ready") is True,
            "owner_ready": queue.get("owner_ready") is True,
        })
        # Close snapshots and source finalizer receipts must be checked against
        # the generation observed from this authenticated bridge, never a
        # predecessor cached on the adapter object.
        self._bridge_producer_generation = queue.get("producer_generation")
        return self._queue_binding()

    async def _bridge_json(self, method: str, path: str, *, timeout: float, payload=None):
        bridge_errors = _bridge_boundary_errors()
        kwargs = {} if payload is None else {"json": payload}
        async with self._bridge_req(method, path, timeout, **kwargs) as resp:
            try:
                data = await resp.json()
            except bridge_errors:
                data = None
            return resp.status, data

    async def _bridge_operation(self, path: str, payload: dict[str, Any], *, timeout: float = 30) -> Optional[dict[str, Any]]:
        request_id = payload.get("request_id")
        bridge_errors = _bridge_boundary_errors()
        try:
            status, data = await self._bridge_json("post", path, timeout=timeout, payload=payload)
            if status == 200 and isinstance(data, dict):
                if request_id and data.get("request_id") != request_id:
                    return None
                if data.get("status") == "request_result":
                    result = data.get("result")
                    return result if isinstance(result, dict) and (not request_id or result.get("request_id") == request_id) else None
                return data
            # Retry only a decoded-empty 200 body or retryable transport statuses; a
            # malformed successful body (list/string) and 409 identity conflicts remain
            # fail-closed rather than looking up foreign input.
            if status == 200:
                if data is not None:
                    return None
            elif status not in {408, 503}:
                return None
        except bridge_errors:
            status = None
            data = None
        if not request_id or not getattr(self, "_http_session", None):
            return None
        try:
            status, data = await self._bridge_json(
                "get", f"messages/requests/{request_id}", timeout=min(timeout, 10),
            )
            if (
                status == 200
                and isinstance(data, dict)
                and data.get("status") == "request_result"
                and data.get("request_id") == request_id
                and isinstance(data.get("result"), dict)
                and data["result"].get("request_id") == request_id
            ):
                return data["result"]
        except bridge_errors:
            return None
        return None

    async def _bridge_control(self, operation: str, generation: int) -> Optional[dict[str, Any]]:
        binding = await self._ensure_queue_binding()
        if binding is None:
            return None
        operation_identity = {
            "operation": operation,
            "close_generation": generation,
            "binding": binding,
        }
        request_id = "control-" + hashlib.sha256(json.dumps(
            operation_identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")).hexdigest()
        return await self._bridge_operation(
            "messages/control",
            {"request_id": request_id, "op": operation, "close_generation": generation, "binding": binding},
        )

    async def _bridge_transition(
        self, operation: str, delivery: dict[str, Any], *, expected_state: str,
        custody_ref: Optional[str] = None, reason: Optional[str] = None,
        settlement: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        binding = await self._ensure_queue_binding()
        if binding is None:
            return None
        request_key = f"{operation}_request_id"
        if request_key not in delivery:
            operation_identity = {
                "operation": operation,
                "queue_epoch": delivery.get("queue_epoch"),
                "bridge_instance": delivery.get("bridge_instance"),
                "owner_epoch": delivery.get("owner_epoch"),
                "item_id": delivery.get("item_id"),
                "sequence": delivery.get("sequence"),
                "claim_id": delivery.get("claim_id"),
                "custody_ref": custody_ref or delivery.get("custody_ref"),
                "settlement_challenge": delivery.get("settlement_challenge"),
                "finalizer_receipt_digest": delivery.get("finalizer_receipt_digest"),
                "reason": reason,
            }
            operation_digest = hashlib.sha256(json.dumps(
                operation_identity, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")).hexdigest()
            request_id = f"{operation}-{operation_digest}"
            delivery[request_key] = request_id
        else:
            request_id = delivery[request_key]
        snapshot_present = operation == "ack" and any(
            key in delivery
            for key in (
                "_ack_payload_snapshot",
                "_ack_payload_snapshot_digest",
                "_ack_payload_request_id",
            )
        )
        stored_snapshot = _load_ack_payload_snapshot(delivery, request_id) if snapshot_present else None
        if snapshot_present and stored_snapshot is None:
            return None
        replaying_snapshot = stored_snapshot is not None and settlement is None
        if replaying_snapshot:
            settlement = stored_snapshot[1]["settlement"]
        payload = {
            "request_id": request_id,
            "op": operation,
            "binding": binding,
            "item_id": delivery.get("item_id"),
            "sequence": delivery.get("sequence"),
            "claim_id": delivery.get("claim_id"),
            "expected_state": expected_state,
            "source_identity": delivery.get("source_identity"),
            "registration_generation": delivery.get("registration_generation"),
            "custody_ref": custody_ref or delivery.get("custody_ref"),
            "settlement_challenge": delivery.get("settlement_challenge"),
            "reason": reason,
            "settlement": settlement,
            "settlement_fields": delivery.get("_settlement_fields"),
            "producer_generation": delivery.get("producer_generation"),
            "finalizer_receipt_digest": delivery.get("finalizer_receipt_digest"),
            "finalizer_status": delivery.get("finalizer_status"),
        }
        if operation == "ack":
            encoded_payload = _encode_ack_payload_snapshot(payload)
            if encoded_payload is None:
                return None
            if replaying_snapshot and encoded_payload[0] != stored_snapshot[0]:
                # An omitted-settlement replay is valid only for the intact
                # same-occurrence body.
                return None
            # An explicit changed body is never replaced by the retained snapshot.
            # Let the durable journal see the same request id and return its
            # request_conflict; only the omitted-settlement replay may reuse the
            # exact source-owned body above.
            if stored_snapshot is None and isinstance(settlement, dict) and not snapshot_present:
                encoded = encoded_payload[0]
                delivery["_ack_payload_snapshot"] = encoded
                delivery["_ack_payload_snapshot_digest"] = hashlib.sha256(
                    encoded.encode("utf-8")
                ).hexdigest()
                delivery["_ack_payload_request_id"] = request_id
        return await self._bridge_operation("messages/transition", payload)

    @staticmethod
    def _source_identity(event: object) -> object:
        source = getattr(event, "source", None)
        to_dict = getattr(source, "to_dict", None)
        if callable(to_dict):
            try:
                return tuple(sorted(to_dict().items()))
            except (AttributeError, TypeError, ValueError):
                pass
        return source

    async def _settle_bridge_delivery(self, token, custody_ref: Optional[str]) -> object:
        delivery = getattr(token, "transport_receipt", None)
        if not isinstance(delivery, dict):
            return False
        finalizer = getattr(token, "finalizer_receipt", None)
        finalizer_digest = getattr(finalizer, "finalizer_digest", None)
        if getattr(finalizer, "status", None) != "success" or not isinstance(finalizer_digest, str):
            return False
        settlement = {
            "producer_done": bool(getattr(token, "producer_done", False)),
            "native_exited": bool(getattr(token, "native_exited", False)),
            "transport_settled": True,
            "producer_generation": getattr(token, "producer_generation", None)
            or delivery.get("producer_generation"),
            "finalizer_receipt_digest": finalizer_digest,
            "finalizer_status": "success",
        }
        witness_key = getattr(self, "_bridge_witness_key", None)
        witness = None
        delivery["producer_generation"] = settlement["producer_generation"]
        settle_identity = {
            "operation": "settle",
            "queue_epoch": delivery.get("queue_epoch"),
            "bridge_instance": delivery.get("bridge_instance"),
            "owner_epoch": delivery.get("owner_epoch"),
            "item_id": delivery.get("item_id"),
            "sequence": delivery.get("sequence"),
            "claim_id": delivery.get("claim_id"),
            "custody_ref": custody_ref or delivery.get("custody_ref"),
            "settlement_challenge": delivery.get("settlement_challenge"),
            "finalizer_receipt_digest": finalizer_digest,
            "reason": None,
        }
        delivery.setdefault("settle_request_id", "settle-" + hashlib.sha256(
            json.dumps(settle_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest())
        if getattr(self, "_bridge_capability_required", False):
            if not isinstance(witness_key, bytes) or len(witness_key) < 16:
                return False
            if not isinstance(settlement["producer_generation"], str) or not settlement["producer_generation"]:
                return False
            try:
                token.transport_status = "success"
                captured = getattr(token, "_settlement_fields", None)
                if isinstance(captured, dict):
                    captured["transport_status"] = "success"
                    captured["producer_generation"] = settlement["producer_generation"]
                    captured["finalizer_receipt_digest"] = finalizer_digest
                    captured["finalizer_status"] = "success"
                    delivery["_settlement_fields"] = captured
                witness = token.build_settlement_witness(
                    witness_key,
                    receipt={"status": "success", "request_id": delivery.get("settle_request_id")},
                ).envelope()
            except (TypeError, ValueError):
                return False
            settlement["witness"] = witness
        delivery["finalizer_receipt_digest"] = finalizer_digest
        delivery["finalizer_status"] = "success"
        settled = await self._bridge_transition(
            "settle",
            delivery,
            expected_state="accepted",
            custody_ref=custody_ref,
            settlement=settlement,
        )
        if not isinstance(settled, dict) or settled.get("status") != "transitioned":
            return False
        settlement["settlement_receipt"] = settled.get("item", {}).get("settlement_record")
        result = await self._bridge_transition(
            "ack", delivery, expected_state="accepted", custody_ref=custody_ref, settlement=settlement,
        )
        if isinstance(result, dict) and result.get("status") == "transitioned":
            if witness:
                return {"settled": True, "witness_digest": witness.get("witness_digest")}
            if not getattr(self, "_bridge_capability_required", False):
                return True
        return False

    async def _hold_bridge_delivery(self, delivery: dict[str, Any], reason: str) -> bool:
        state = str(delivery.get("state") or "claimed")
        result = await self._bridge_transition(
            "hold", delivery, expected_state=state, reason=reason,
        )
        return isinstance(result, dict) and result.get("status") == "transitioned"

    @staticmethod
    def _delivery_payload(msg_data: dict[str, Any]) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
        delivery = msg_data.get("_bridge_delivery")
        payload = msg_data.get("_bridge_payload")
        if isinstance(delivery, dict) and isinstance(payload, dict):
            return payload, delivery
        return msg_data, delivery if isinstance(delivery, dict) else None

    async def _reuse_running_bridge(self, bridge_path: Path) -> bool:
        """Adopt a connected bridge serving the on-disk bridge.js + same read-receipt config; else say why it restarts."""
        self._foreign_bridge_session = None
        self._bridge_probe_timed_out = False
        try:
            from .bridge_ownership import check_bridge_owner, port_is_free
            if port_is_free(self._bridge_port):
                return False
            # A running process is attachable only when this profile already has its
            # durable private authority.  Do not create a new key in response to a
            # listener that has not first proved the session/profile identity.
            authority_path = self._session_path.resolve() / "bridge.authority.json"
            if not authority_path.exists():
                self._foreign_bridge_session = "private_authority_missing"
                return False
            authority = self._load_bridge_authority()
            if authority is None:
                self._foreign_bridge_session = "private_authority_unavailable"
                return False
            physical = check_bridge_owner(
                self._session_path,
                self._bridge_port,
                profile_digest=hashlib.sha256(str(self._profile_home.resolve()).encode()).hexdigest(),
                script_sha256=_file_content_hash(bridge_path),
            )
            if physical.get("status") not in {"owned", "owned_physical"}:
                self._foreign_bridge_session = "foreign_or_ambiguous"
                return False
            ok, data = await self._probe_bridge_health()
            if not ok or data is None:
                return False
            physical = check_bridge_owner(
                self._session_path,
                self._bridge_port,
                profile_digest=hashlib.sha256(str(self._profile_home.resolve()).encode()).hexdigest(),
                script_sha256=_file_content_hash(bridge_path),
                health=data,
            )
            if physical.get("status") != "owned":
                self._foreign_bridge_session = str(data.get("session") or "foreign_or_ambiguous")
                return False
            # Another profile's bridge (same default port) is neither adopted, which would answer as that
            # profile's phone, nor killed: connect() reports it. Checked before status because a bridge in
            # startup, reconnect or QR wait reports "disconnected". A bridge without ``session`` predates the
            # field, so its scriptHash no longer matches bridge.js and it restarts as stale below.
            reported_session = data.get("session")
            if reported_session and reported_session != str(self._session_path):
                self._foreign_bridge_session = reported_session
                return False
            bridge_status = data.get("status", "unknown")
            if bridge_status != "connected":
                print(f"[{self.name}] Bridge found but not connected (status: {bridge_status}), restarting")
                return False
            running_hash, disk_hash = data.get("scriptHash", ""), _file_content_hash(bridge_path)
            if running_hash and disk_hash and running_hash == disk_hash and bool(data.get("sendReadReceipts", False)) == self._send_read_receipts:
                if not await self._adopt_running_bridge(data, authority):
                    self._foreign_bridge_session = "private_attach_refused"
                    return False
                print(f"[{self.name}] Using existing bridge (status: {bridge_status})")
                self._mark_connected()
                self._attach_to_bridge(None)  # Not managed by us
                self._wire_plugin_handlers(None)
                return True
            stale_reason = f"running={running_hash or 'unversioned'}, disk={disk_hash}" if running_hash != disk_hash else "send_read_receipts config changed"
            print(f"[{self.name}] Running bridge is stale ({stale_reason}), restarting")
        except TimeoutError:
            self._bridge_probe_timed_out = True  # something holds the port but gave no identity; connect() leaves it
        except Exception:
            pass  # Bridge not running, start a new one
        return False

    async def _adopt_running_bridge(self, health: dict[str, Any], authority: dict[str, Any]) -> bool:
        """Attach and, when present, consume the durable predecessor handoff."""
        from .bridge_ownership import read_bridge_handoff

        queue = health.get("queue") if isinstance(health, dict) else None
        if not isinstance(queue, dict):
            return False
        binding = {
            "session_digest": authority.get("session_digest"),
            "profile_digest": authority.get("profile_digest"),
            "canonical_port": authority.get("canonical_port"),
            "script_sha256": authority.get("script_sha256"),
            "process_start": queue.get("process_start") or queue.get("owner_start")
            or (queue.get("owner_binding") or {}).get("owner_start"),
            "queue_epoch": queue.get("queue_epoch"),
            "bridge_instance": queue.get("bridge_instance"),
            "owner_epoch": queue.get("owner_epoch"),
            "attach_key_id": authority.get("attach_key_id"),
            "source_witness_key_id": authority.get("source_witness_key_id"),
            "listener_ready": queue.get("listener_ready") is True,
            "owner_ready": queue.get("owner_ready") is True,
        }
        if not all(isinstance(binding.get(key), (str, int, bool)) for key in (
            "session_digest", "profile_digest", "canonical_port", "script_sha256",
            "queue_epoch", "bridge_instance", "owner_epoch", "attach_key_id",
            "source_witness_key_id",
        )):
            return False
        handoff = read_bridge_handoff(self._session_path)
        predecessor_digest = handoff.get("predecessor_digest") if isinstance(handoff, dict) else None
        attach_identity = {
            "queue_epoch": binding.get("queue_epoch"),
            "bridge_instance": binding.get("bridge_instance"),
            "owner_epoch": binding.get("owner_epoch"),
            "process_start": binding.get("process_start"),
            "predecessor_digest": predecessor_digest,
        }
        attach_id = "attach-" + hashlib.sha256(
            json.dumps(attach_identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        nonce = base64.urlsafe_b64encode(hashlib.sha256(
            json.dumps({**attach_identity, "attach_request_id": attach_id},
                       sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).digest()[:18]).decode().rstrip("=")
        digest_binding = {key: binding.get(key) for key in (
            "session_digest", "profile_digest", "canonical_port", "script_sha256", "process_start",
            "queue_epoch", "bridge_instance", "owner_epoch", "attach_key_id", "source_witness_key_id",
        )}
        attach_payload = {
            "protocol": "hermes.whatsapp.attach.v2",
            "request_id": attach_id,
            "challenge_nonce": nonce,
            "binding": binding,
            "binding_digest": hashlib.sha256(json.dumps(digest_binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "predecessor_digest": predecessor_digest,
            "attach_key_id": authority.get("attach_key_id"),
            "source_witness_key_id": authority.get("source_witness_key_id"),
        }
        attached = await self._bridge_operation("messages/attach", attach_payload)
        if not isinstance(attached, dict) or attached.get("status") != "attached":
            return False
        if isinstance(handoff, dict) and handoff.get("state") == "pending":
            adoption_identity = {
                "queue_epoch": handoff.get("queue_epoch") or queue.get("queue_epoch"),
                "predecessor_digest": handoff.get("predecessor_digest"),
                "adoption_nonce": handoff.get("adoption_nonce"),
            }
            adoption_id = "adopt-" + hashlib.sha256(
                json.dumps(adoption_identity, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            adoption = await self._bridge_operation("messages/adopt", {
                "protocol": "hermes.whatsapp.adoption.v1",
                "request_id": adoption_id,
                "predecessor_digest": handoff.get("predecessor_digest"),
                "adoption_nonce": handoff.get("adoption_nonce"),
                "queue_epoch": handoff.get("queue_epoch") or queue.get("queue_epoch"),
                "binding": binding,
                "attach_proof": attached.get("attach_proof"),
            })
            if not isinstance(adoption, dict) or adoption.get("status") != "adopted":
                return False
        self._bridge_binding = {
            "queue_epoch": queue.get("queue_epoch"),
            "bridge_instance": queue.get("bridge_instance"),
            "owner_epoch": queue.get("owner_epoch"),
            "session_digest": authority.get("session_digest"),
            "profile_digest": authority.get("profile_digest"),
            "canonical_port": authority.get("canonical_port"),
            "script_sha256": authority.get("script_sha256"),
            "process_start": binding.get("process_start"),
            "producer_generation": queue.get("producer_generation"),
            "listener_ready": binding.get("listener_ready"),
            "owner_ready": binding.get("owner_ready"),
        }
        self._bridge_producer_generation = queue.get("producer_generation")
        self._consumer_id = str(queue.get("consumer_id") or uuid4().hex)
        self._consumer_epoch = int(queue.get("consumer_epoch") or (self._consumer_epoch + 1))
        self._bridge_capability_required = True
        return True

    async def _prepare_spawned_bridge_startup(self, authority: dict[str, Any], startup_mode: str) -> bool:
        """Authenticate a spawned queue owner before its poller can claim anything."""
        ok, health = await self._probe_bridge_health()
        if not ok or not isinstance(health, dict):
            return False
        queue = health.get("queue")
        handoff = queue.get("owner_handoff") if isinstance(queue, dict) else None
        handoff_pending = isinstance(handoff, dict) and handoff.get("state") == "pending"
        if startup_mode == "adopt" and not handoff_pending:
            return False
        # A successor creates the durable predecessor envelope while it opens the
        # existing queue.  Consume it before polling even when the bootstrap had
        # to be sent as ``fresh`` because no envelope existed before spawn.
        if handoff_pending and not await self._adopt_running_bridge(health, authority):
            return False
        return True

    def _bridge_env(self) -> dict:
        """Subprocess env: the adapter's EFFECTIVE profile policy + profile-resolved WHATSAPP_* values + cache dirs."""
        # with_hermes_node_path() copies os.environ when called with no arg: under a multiplexed secondary
        # that copy carries the DEFAULT profile's WHATSAPP_* values, so every bridge-consumed key is
        # re-resolved from this profile (dropped on a scoped miss), never inherited from the launch env.
        bridge_env = with_hermes_node_path()
        reply_prefix = _wenv("WHATSAPP_REPLY_PREFIX")
        if reply_prefix:
            bridge_env["WHATSAPP_REPLY_PREFIX"] = reply_prefix
        elif self._reply_prefix is not None:
            bridge_env["WHATSAPP_REPLY_PREFIX"] = self._reply_prefix
        else:
            bridge_env.pop("WHATSAPP_REPLY_PREFIX", None)
        bridge_env["WHATSAPP_SEND_READ_RECEIPTS"] = "true" if self._send_read_receipts else "false"
        for _key, _v in [("WHATSAPP_MODE", _wenv("WHATSAPP_MODE", "self-chat"))] + [(k, _wenv(k)) for k in _BRIDGE_PASSTHROUGH_ENV]:
            if _v:
                bridge_env[_key] = _v
            else:
                bridge_env.pop(_key, None)
        # bridge.js gates DMs BEFORE Python sees them: it must run the same dm_policy / allow_from the
        # adapter resolved (scoped env → this profile's YAML → default), or a secondary's YAML
        # ``dm_policy: pairing`` runs under the default profile's allowlist and drops valid pairing DMs.
        bridge_env["WHATSAPP_DM_POLICY"] = self._dm_policy
        bridge_env["WHATSAPP_GROUP_POLICY"] = self._group_policy
        for env_key, ids in (("WHATSAPP_ALLOWED_USERS", self._allow_from), ("WHATSAPP_GROUP_ALLOWED_USERS", self._group_allow_from)):
            if ids:
                bridge_env[env_key] = ",".join(sorted(ids))
            else:
                bridge_env.pop(env_key, None)
        # Without these the bridge hardcodes ~/.hermes/{image,audio,document}_cache (wrong under HERMES_HOME/profiles/cache layout).
        img_dir, audio_dir, _video_dir, doc_dir = _cache_dirs()
        bridge_env.update(HERMES_IMAGE_CACHE_DIR=str(img_dir), HERMES_AUDIO_CACHE_DIR=str(audio_dir), HERMES_DOCUMENT_CACHE_DIR=str(doc_dir))
        return bridge_env

    def _send_bridge_bootstrap(self, process: subprocess.Popen) -> tuple[dict[str, Any], str]:
        """Send the managed owner frame after the kernel start token exists."""
        start = None
        try:
            from gateway.status import get_process_start_time
            start = get_process_start_time(process.pid)
        except (OSError, RuntimeError, TypeError, ValueError):
            start = None
        if start is None:
            raise RuntimeError("WhatsApp bridge process-start proof unavailable")
        script_bytes = Path(self._bridge_script).read_bytes()
        authority = self._load_bridge_authority(start)
        if authority is None:
            raise RuntimeError("WhatsApp bridge private authority unavailable")
        from .bridge_ownership import read_bridge_handoff
        handoff = read_bridge_handoff(self._session_path)
        startup_mode = "adopt" if isinstance(handoff, dict) and handoff.get("state") == "pending" else "fresh"
        attach_key = authority["attach_key_b64url"]
        witness_key = authority["source_witness_key_b64url"]
        self._bridge_capability = attach_key
        self._bridge_witness_key = witness_key.encode("utf-8")
        identity = self._bridge_identity()
        frame = {
            "protocol": "hermes.whatsapp.bridge.bootstrap.v1",
            "session_digest": identity["session_digest"],
            "profile_digest": identity["profile_digest"],
            "canonical_port": self._bridge_port,
            "script_sha256": hashlib.sha256(script_bytes).hexdigest(),
            "startup_mode": startup_mode,
            "predecessor_digest": handoff.get("predecessor_digest", "") if startup_mode == "adopt" else "",
            "adoption_nonce": handoff.get("adoption_nonce", "") if startup_mode == "adopt" else "",
            "key_id": authority["attach_key_id"],
            "attach_key_b64url": attach_key,
            "source_witness_key_id": authority["source_witness_key_id"],
            "source_witness_key_b64url": witness_key,
            "process_start": start,
        }
        encoded = json.dumps(frame, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) > 16384 or process.stdin is None:
            raise RuntimeError("WhatsApp bridge bootstrap frame is unavailable")
        process.stdin.write(struct.pack(">I", len(encoded)) + encoded)
        process.stdin.flush()
        process.stdin.close()
        return authority, startup_mode

    def _bridge_died(self, detail: str, code: str = "whatsapp_bridge_exited") -> bool:
        print(f"[{self.name}] {detail}")
        print(f"[{self.name}] Check log: {self._bridge_log}")
        self._close_bridge_log()
        # Named so the reconnect status and connect telemetry say why instead of an unclassified failure.
        self._set_fatal_error(code, f"{detail} (see {self._bridge_log})", retryable=True)
        return False

    async def _poll_bridge_health(self, died_msg: str) -> tuple[Optional[bool], bool, dict]:
        """Poll /health up to 15×1s → ``(connected, http_ready, data)``; connected False = process died (reported), None = timeout."""
        http_ready = False
        data: dict = {}
        for attempt in range(15):
            await asyncio.sleep(1)
            if self._bridge_process.poll() is not None:
                return self._bridge_died(died_msg.format(code=self._bridge_process.returncode)), http_ready, data
            try:
                ok, d = await self._probe_bridge_health()
                if ok:
                    http_ready = True
                    if d is not None:
                        data = d
                        if data.get("status") == "connected":
                            print(f"[{self.name}] Bridge ready (status: connected)")
                            return True, http_ready, data
            except Exception:
                continue
        return None, http_ready, data

    async def _wait_for_bridge(self) -> bool:
        """Phase 1: HTTP up (≤15s). Phase 2: ``status: connected`` (≤15s more; warns but proceeds if still connecting)."""
        connected, http_ready, data = await self._poll_bridge_health("Bridge process died (exit code {code})")
        if connected is False:
            return False
        if not http_ready:
            return self._bridge_died("Bridge HTTP server did not start in 15s", "whatsapp_bridge_timeout")
        if data.get("status") != "connected":
            print(f"[{self.name}] Bridge HTTP ready, waiting for WhatsApp connection...")
            connected, _, _ = await self._poll_bridge_health("Bridge process died during connection")
            if connected is False:
                return False
            if connected is None:
                print(f"[{self.name}] ⚠ WhatsApp not connected after 30s")
                print(f"[{self.name}]   Bridge log: {self._bridge_log}")
                print(f"[{self.name}]   If session expired, re-pair: hermes whatsapp")
        return True

    def _preflight(self) -> bool:
        """Node + bridge script + creds.json + aiohttp present, else a named fatal error (an unpaired bridge only prints QR codes; retries would pay 30s each)."""
        bridge_path = Path(self._bridge_script)
        creds_path = self._session_path / "creds.json"
        checks = (
            (check_whatsapp_requirements, ("[%s] Node.js not found. WhatsApp requires Node.js.", self.name),
             "whatsapp_node_missing", "Node.js is not installed — install Node.js and re-run `hermes gateway`."),
            (bridge_path.exists, ("[%s] Bridge script not found: %s", self.name, bridge_path),
             "whatsapp_bridge_missing", f"WhatsApp bridge script missing at {bridge_path}."),
            (creds_path.exists, ("[%s] WhatsApp is enabled but not paired (no creds.json at %s). Pair from the dashboard or run "
                                 "`hermes whatsapp`; remove WHATSAPP_ENABLED from your .env to disable.", self.name, creds_path),
             "whatsapp_not_paired", "WhatsApp enabled but not paired — pair from the dashboard or run `hermes whatsapp`."),
        )
        for ok, warn_args, code, message in checks:
            if not ok():
                logger.warning(*warn_args)
                self._set_fatal_error(code, message, retryable=False)
                return False
        if (missing := ensure_aiohttp()) is not None:  # may install it: connect runs this off the event loop
            why, retryable = missing
            message = (f"aiohttp is unavailable ({why}); the WhatsApp bridge client needs it. "
                       "Run `hermes pm install --extra sms` (it ships aiohttp; `hermes pm repair` for a damaged install), "
                       "then restart `hermes gateway`.")
            logger.warning("[%s] %s", self.name, message)
            self._set_fatal_error("whatsapp_aiohttp_missing", message, retryable=retryable)
            return False
        logger.info("[%s] Bridge found at %s", self.name, bridge_path)
        return True

    def _close_bridge_log(self) -> None:
        # Mark the tracked worker before closing the current handle.  If the
        # worker completes later, its callback closes that late handle instead
        # of publishing it after disconnect/finalizer cleanup.
        self._request_bridge_log_open_close()
        if self._bridge_log_fh:
            with suppress(Exception):
                self._bridge_log_fh.close()
            self._bridge_log_fh = None

    async def _check_managed_bridge_exit(self) -> Optional[str]:
        returncode = self._bridge_process.poll() if self._bridge_process is not None else None
        if returncode is None:
            return None
        # getattr-with-default: tests build the adapter via ``__new__`` without __init__.
        # A container stop (e.g. s6-overlay's stage-3 broadcast) reaches the bridge child (its own session) before the
        # gateway's stop flow reaches this adapter's disconnect(), so ``_shutting_down`` alone
        # misses that window and a normal -15/-2 exit reads as a fatal crash (#127047). The
        # runner flips ``_stop_requested_by_signal`` in its signal handler — the first thing
        # that runs on any signal-driven stop — so consult it too. Un-guarded it stays fatal:
        # a bridge killed while the gateway keeps running still needs the reconnect watcher.
        runner_stop = getattr(getattr(self, "gateway_runner", None), "_stop_requested_by_signal", False)
        if (getattr(self, "_shutting_down", False) or runner_stop) and returncode in {0, -2, -15}:
            logger.info("[%s] Bridge exited during shutdown (code %d).", self.name, returncode)
            return None
        message = f"WhatsApp bridge process exited unexpectedly (code {returncode})."
        if not self.has_fatal_error:
            logger.error("[%s] %s", self.name, message)
            self._set_fatal_error("whatsapp_bridge_exited", message, retryable=True)
            self._close_bridge_log()
            await self._notify_fatal_error()
        return self.fatal_error_message or message

    def _terminate_bridge(self, *, force: bool) -> None:
        try:
            _terminate_bridge_process(self._bridge_process, force=force)
        except (ProcessLookupError, PermissionError):
            getattr(self._bridge_process, "kill" if force else "terminate")()

    async def disconnect(self) -> None:
        """Stop the WhatsApp bridge and clean up any orphaned processes."""
        self._shutting_down = True  # flip BEFORE signalling so send()/poll loop don't report the intentional exit as fatal
        if not self._bridge_process:
            print(f"[{self.name}] Disconnecting (external bridge left running)")
        else:
            try:
                self._terminate_bridge(force=False)
                await asyncio.sleep(1)
                if self._bridge_process.poll() is None:
                    self._terminate_bridge(force=True)
            except Exception as e:
                print(f"[{self.name}] Error stopping bridge: {e}")
        if self._bridge_process or not getattr(self, "_runtime_status_platform_key", ""):
            _unlink_quietly(self._session_path / "bridge.pid")
        await cancel_task(self._poll_task)
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        self._poll_task = self._http_session = self._bridge_process = None
        self._release_platform_lock()
        self._mark_disconnected()
        self._close_bridge_log()
        print(f"[{self.name}] Disconnected")

    async def _bridge_unavailable(self) -> Optional[str]:
        return "Not connected" if not self._running or not self._http_session else (await self._check_managed_bridge_exit() or None)

    async def _post_bridge_message(self, path: str, payload: dict[str, Any], *, timeout: float) -> SendResult:
        """POST to the bridge; 200 → SendResult(messageId, raw_response), else the error text."""
        try:
            async with self._bridge_req("post", path, timeout, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return SendResult(success=True, message_id=data.get("messageId"), raw_response=data)
                return SendResult(success=False, error=await resp.text())
        except Exception as e:
            return SendResult(success=False, error=str(e))

    @_needs_bridge
    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> SendResult:
        """Format markdown for WhatsApp, chunk preserving code blocks, send sequentially."""
        if not content or not content.strip():
            return SendResult(success=True, message_id=None)
        chat_id = to_whatsapp_jid(chat_id)
        try:
            chunks = self.truncate_message(self.format_message(content), self._outgoing_chunk_limit())
            sent_message_ids: list[str] = []
            last_message_id = None
            for idx, chunk in enumerate(chunks):
                payload: dict[str, Any] = {"chatId": chat_id, "message": chunk}
                if reply_to and idx == 0:
                    payload["replyTo"] = reply_to  # Reply-to on the first chunk only.
                result = await self._post_bridge_message("send", payload, timeout=30)
                if not result.success:
                    return SendResult(success=False, error=result.error)
                last_message_id = result.message_id
                if last_message_id:
                    sent_message_ids.append(str(last_message_id))
                if len(chunks) > 1:
                    await asyncio.sleep(0.3)  # avoid rate limiting between chunks
            return SendResult(success=True, message_id=last_message_id, continuation_message_ids=tuple(sent_message_ids[:-1]),
                              raw_response={"message_ids": sent_message_ids})
        except Exception as e:
            return SendResult(success=False, error=str(e))

    @_needs_bridge
    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False) -> SendResult:
        try:
            async with self._bridge_req("post", "edit", 15, json={"chatId": to_whatsapp_jid(chat_id), "messageId": message_id, "message": content}) as resp:
                return SendResult(success=True, message_id=message_id) if resp.status == 200 else SendResult(success=False, error=await resp.text())
        except Exception as e:
            return SendResult(success=False, error=str(e))

    @_needs_bridge
    async def _send_media_to_bridge(self, chat_id: str, file_path: str, media_type: str, caption: Optional[str] = None, file_name: Optional[str] = None) -> SendResult:
        if not os.path.exists(file_path):
            return SendResult(success=False, error=f"File not found: {file_path}")
        jid = to_whatsapp_jid(chat_id)
        payload: dict[str, Any] = {"chatId": jid, "filePath": file_path, "mediaType": media_type}
        payload.update({k: v for k, v in (("caption", caption), ("fileName", file_name)) if v})
        result = await self._post_bridge_message("send-media", payload, timeout=120)
        if result.success and result.message_id:
            # A later quote of this attachment carries only a thumbnail stub; the bridge's cache
            # knows inbound media only, so index our own sends (the cron-delivered image case).
            from gateway import rich_sent_store
            mime = mimetypes.guess_type(file_path)[0] or _MEDIA_INFO.get(_MEDIA_TYPE_BY_BRIDGE_KIND.get(media_type), ("", ""))[1]
            await rich_sent_store.record_media_async(jid, result.message_id, [(file_path, mime or "application/octet-stream")])
        return result

    @_needs_bridge
    async def send_poll(self, chat_id: str, question: str, options: list[str], *, selectable_count: int = 1) -> SendResult:
        """Native WhatsApp poll (low-level transport primitive; approval UX stays gateway-owned)."""
        payload: dict[str, Any] = {"chatId": to_whatsapp_jid(chat_id), "question": question, "options": list(options or []), "selectableCount": selectable_count}
        return await self._post_bridge_message("send-poll", payload, timeout=30)

    async def send_clarify(self, chat_id: str, question: str, choices: Optional[list], clarify_id: str, session_key: str,
                           metadata: Optional[dict[str, Any]] = None) -> SendResult:
        """Multiple-choice clarify as a native poll (the pick arrives as message text for the normal intercept); else text prompt."""
        clean_choices = [str(choice).strip() for choice in (choices or []) if str(choice).strip()]
        if 2 <= len(clean_choices) <= 12:
            result = await self.send_poll(chat_id, str(question or "").strip(), clean_choices, selectable_count=1)
            if result.success:
                return result
            logger.warning("[%s] Native WhatsApp clarify poll failed; falling back to text: %s", self.name, result.error)
        return await super().send_clarify(chat_id=chat_id, question=question, choices=choices, clarify_id=clarify_id, session_key=session_key, metadata=metadata)

    @_needs_bridge
    async def send_location(self, chat_id: str, latitude: float, longitude: float, *, name: Optional[str] = None, address: Optional[str] = None,
                            reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> SendResult:
        try:
            payload: dict[str, Any] = {"chatId": to_whatsapp_jid(chat_id), "latitude": float(latitude), "longitude": float(longitude)}
        except Exception as e:
            return SendResult(success=False, error=str(e))
        payload.update({k: v for k, v in (("name", name), ("address", address)) if v})
        return await self._post_bridge_message("send-location", payload, timeout=30)

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None, reply_to: Optional[str] = None,
                         metadata: Optional[dict[str, Any]] = None) -> SendResult:
        """Download image URL to cache, send natively via bridge (``metadata`` honors the base contract)."""
        try:
            local_path = await cache_image_from_url(image_url)
            return await self._send_media_to_bridge(chat_id, local_path, "image", caption)
        except Exception:
            return await super().send_image(chat_id, image_url, caption, reply_to, metadata)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_media_to_bridge(chat_id, image_path, "image", caption)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_media_to_bridge(chat_id, video_path, "video", caption)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_media_to_bridge(chat_id, audio_path, "audio", caption)

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None, file_name: Optional[str] = None,
                            reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_media_to_bridge(chat_id, file_path, "document", caption, file_name or os.path.basename(file_path))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if await self._bridge_unavailable():
            return
        with suppress(Exception):
            import aiohttp
            # ``async with`` — a bare ``await session.post(...)`` leaves the response (and its CLOSE_WAIT socket) alive until GC.
            async with self._http_session.post(self._bridge_url("typing"), json={"chatId": to_whatsapp_jid(chat_id)}, timeout=aiohttp.ClientTimeout(total=5)):
                pass

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        if not self._running or not self._http_session:
            return {"name": "Unknown", "type": "dm"}
        if not await self._check_managed_bridge_exit():
            try:
                async with self._bridge_req("get", f"chat/{to_whatsapp_jid(chat_id)}", 10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {"name": data.get("name", chat_id), "type": "group" if data.get("isGroup") else "dm", "participants": data.get("participants", [])}
            except Exception as e:
                logger.debug("Could not get WhatsApp chat info for %s: %s", chat_id, e)
        return {"name": chat_id, "type": "dm"}

    async def _report_bridge_exit(self) -> bool:
        bridge_exit = await self._check_managed_bridge_exit()
        if bridge_exit:
            print(f"[{self.name}] {bridge_exit}")
        return bool(bridge_exit)

    async def _poll_messages(self) -> None:
        while self._running:
            if not self._http_session or await self._report_bridge_exit():
                break
            try:
                binding = await self._ensure_queue_binding()
                if binding is None:
                    await asyncio.sleep(1)
                    continue
                request_id = getattr(self, "_bridge_claim_request_id", None)
                if not request_id:
                    request_id = uuid4().hex
                    self._bridge_claim_request_id = request_id
                result = await self._bridge_operation(
                    "messages/claim",
                    {"request_id": request_id, "binding": binding, "limit": 1},
                )
                if isinstance(result, dict) and result.get("status") in {"claimed", "empty", "blocked"}:
                    self._bridge_claim_request_id = None
                if isinstance(result, dict) and result.get("status") == "claimed":
                    item = result.get("item") or {}
                    payload = item.get("payload") if isinstance(item, dict) else None
                    if not isinstance(payload, dict):
                        await asyncio.sleep(1)
                        continue
                    delivery = {
                        "queue_epoch": result.get("binding", {}).get("queue_epoch", binding.get("queue_epoch")),
                        "bridge_instance": result.get("binding", {}).get("bridge_instance", binding.get("bridge_instance")),
                        "owner_epoch": result.get("binding", {}).get("owner_epoch", binding.get("owner_epoch")),
                        "consumer_id": result.get("binding", {}).get("consumer_id", binding.get("consumer_id")),
                        "consumer_epoch": result.get("binding", {}).get("consumer_epoch", binding.get("consumer_epoch")),
                        "producer_generation": result.get("binding", {}).get("producer_generation", binding.get("producer_generation")),
                        "item_id": item.get("item_id"),
                        "sequence": item.get("sequence"),
                        "close_generation": item.get("close_generation"),
                        "claim_id": result.get("claim_id"),
                        "custody_ref": result.get("custody_ref"),
                        "settlement_challenge": result.get("settlement_challenge", item.get("settlement_challenge")),
                        "wire_identity": item.get("wire_identity"),
                        "state": "claimed",
                    }
                    await self._dispatch_polled_message({
                        **payload,
                        "_bridge_payload": payload,
                        "_bridge_delivery": delivery,
                    })
            except asyncio.CancelledError:
                break
            except Exception as e:
                if await self._report_bridge_exit():
                    break
                print(f"[{self.name}] Poll error: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(1)  # Poll interval

    @staticmethod
    def _poll_source_marker(msg_data: dict[str, Any]) -> tuple[str, ...] | None:
        message_id = str(msg_data.get("messageId") or "").strip()
        if not message_id:
            return None
        chat_id = str(msg_data.get("chatId") or "").strip()
        delivery = msg_data.get("_bridge_delivery")
        if isinstance(delivery, dict):
            return (
                "whatsapp-poll",
                str(delivery.get("queue_epoch") or ""),
                str(delivery.get("sequence") or ""),
                str(delivery.get("item_id") or ""),
                chat_id,
                message_id,
            )
        return ("whatsapp-poll", chat_id, message_id)

    async def _dispatch_polled_message(self, msg_data: dict[str, Any]) -> None:
        wire_data, delivery = self._delivery_payload(msg_data)
        event = await self._build_message_event(wire_data)
        if event is None:
            if delivery is not None:
                await self._hold_bridge_delivery(delivery, "event_build_failed")
            return
        from hermes_state_runtime import RuntimeStoreError

        source_marker = self._poll_source_marker(msg_data)
        prior_event_tokens = self._preservation_source_tokens(event)
        try:
            token = self._preservation_source_admit(
                source_marker or event, event=event, phase="whatsapp-poll-callback",
            )
            if prior_event_tokens:
                self._preservation_source_unknown(token, "ambiguous_event_source_tokens")
                if delivery is not None:
                    await self._hold_bridge_delivery(delivery, "ambiguous_event_source_tokens")
                return
            if delivery is not None:
                delivery["source_identity"] = self._source_identity(event)
                delivery["registration_generation"] = getattr(token, "registration_generation", None)
                custody_ref = self._preservation_source_attach_transport(
                    token, delivery, self._settle_bridge_delivery, delivery.get("custody_ref"),
                )
                if not custody_ref:
                    self._preservation_source_unknown(token, "transport_custody_missing")
                    await self._hold_bridge_delivery(delivery, "transport_custody_missing")
                    return
                delivery["custody_ref"] = custody_ref
                capture = getattr(token, "capture_bridge_delivery", None)
                if callable(capture):
                    capture(delivery)
                event._preservation_bridge_delivery = delivery
                event._preservation_bridge_deliveries = [delivery]
                accepted = await self._bridge_transition(
                    "accept", delivery, expected_state="claimed", custody_ref=custody_ref,
                )
                if not isinstance(accepted, dict) or accepted.get("status") != "transitioned":
                    self._preservation_source_unknown(token, "bridge_accept_failed")
                    await self._hold_bridge_delivery(delivery, "bridge_accept_failed")
                    return
                delivery["state"] = "accepted"
        except RuntimeStoreError as exc:
            if str(exc) == "runtime_draining" and source_marker is not None:
                self._preservation_source_refused(
                    event, marker=source_marker, reason="runtime_draining",
                )
                if delivery is not None:
                    released = await self._bridge_transition(
                        "release", delivery, expected_state="claimed", reason="runtime_draining",
                    )
                    if not isinstance(released, dict) or released.get("status") != "transitioned":
                        await self._hold_bridge_delivery(delivery, "runtime_draining_release_ambiguous")
            else:
                self._preservation_source_unknown(event, "whatsapp_poll_source_refused")
                if delivery is not None:
                    await self._hold_bridge_delivery(delivery, "whatsapp_poll_source_refused")
            logger.debug("[%s] Source-close refused polled message: %s", self.name, exc)
            return
        read_receipt_task = asyncio.create_task(self._send_read_receipt(msg_data))
        self._background_tasks.add(read_receipt_task)
        read_receipt_task.add_done_callback(self._background_tasks.discard)
        try:
            if event.message_type == MessageType.TEXT:
                self._enqueue_text_event(event)
                if getattr(event, "_preservation_source_deferred", False):
                    self._preservation_source_producer_done(event)
                else:
                    self._preservation_source_unknown(event, "whatsapp_text_enqueue_refused")
                    if delivery is not None:
                        await self._hold_bridge_delivery(delivery, "whatsapp_text_enqueue_refused")
            else:
                await self.handle_message(
                    event, source_tokens=(token,) if token is not None else (),
                )
                self._preservation_source_producer_done(event)
        except (asyncio.CancelledError, Exception):
            self._preservation_source_unknown(event, "whatsapp_dispatch_failed")
            if delivery is not None:
                await self._hold_bridge_delivery(delivery, "whatsapp_dispatch_failed")
            raise

    async def _dispatch_text_batch(self, event: MessageEvent) -> None:
        """Finalize the poll-source token after a buffered text reaches the base handler."""
        event._preservation_source_deferred = False
        source_tokens = self._preservation_source_tokens(event)
        try:
            await self.handle_message(event, source_tokens=source_tokens)
        except (asyncio.CancelledError, Exception):
            self._preservation_source_unknown(event, "whatsapp_text_batch_failed")
            deliveries = getattr(event, "_preservation_bridge_deliveries", None)
            if not deliveries:
                deliveries = (getattr(event, "_preservation_bridge_delivery", None),)
            for delivery in deliveries:
                if isinstance(delivery, dict):
                    await self._hold_bridge_delivery(delivery, "whatsapp_text_batch_failed")
            raise
        finally:
            self._preservation_source_producer_done(source_tokens or event)

    async def _send_read_receipt(self, data: dict[str, Any]) -> None:
        key = data.get("readReceiptKey")
        if not self._send_read_receipts or not self._http_session or not isinstance(key, dict):
            return
        try:
            async with self._bridge_req("post", "read", 5, json={"key": key}) as resp:
                if resp.status != 200:
                    logger.warning("[%s] WhatsApp read receipt failed with HTTP %s", self.name, resp.status)
        except Exception as exc:
            logger.warning("[%s] WhatsApp read receipt failed: %s", self.name, exc)

    _SPLIT_THRESHOLD = 6000  # WhatsApp supports ~65K chars; generous threshold

    @staticmethod
    def _classify_bridge_message(data: dict[str, Any]) -> MessageType:
        media_type = str(data.get("mediaType", "") or "")
        if media_type in _NATIVE_MEDIA_TYPES:
            return _NATIVE_MEDIA_TYPES[media_type]
        if not data.get("hasMedia"):
            return MessageType.TEXT
        return next((kind for needle, kind in _MEDIA_NEEDLES if needle in media_type), MessageType.DOCUMENT)

    async def _collect_bridge_media(self, data: dict[str, Any], msg_type: MessageType) -> tuple[list, list]:
        """``mediaUrls`` → ``(cached_urls, media_types)``: remote image/audio cached locally; absolute paths only inside a cache dir."""
        accepted: list[tuple] = []  # (url_or_path, mime)
        label, default_mime = _MEDIA_INFO.get(msg_type, (None, ""))
        bridge_mime = str(data.get("mime") or "").strip()
        for url in data.get("mediaUrls", []):
            mime = bridge_mime or (SUPPORTED_DOCUMENT_TYPES.get(Path(url).suffix.lower(), "application/octet-stream") if msg_type == MessageType.DOCUMENT else default_mime)
            if url.startswith(("http://", "https://")) and msg_type in {MessageType.PHOTO, MessageType.VOICE, MessageType.AUDIO}:
                cacher, ext = (cache_image_from_url, ".jpg") if msg_type == MessageType.PHOTO else (cache_audio_from_url, ".ogg")
                try:
                    url = await cacher(url, ext=ext)
                    print(f"[{self.name}] Cached user {label}: {url}", flush=True)
                except Exception as e:
                    print(f"[{self.name}] Failed to cache {label}: {e}", flush=True)
                accepted.append((url, mime))
            elif label is not None and os.path.isabs(url):
                if _is_allowed_bridge_path(url):
                    accepted.append((url, mime))
                    print(f"[{self.name}] Using bridge-cached {label}: {url}", flush=True)
                else:
                    print(f"[{self.name}] Rejected bridge {label} path outside cache dir: {url}", flush=True)
            else:
                accepted.append((url, "unknown"))
        return [u for u, _ in accepted], [m for _, m in accepted]

    def _inject_document_text(self, cached_urls: list, body: str) -> tuple[str, list[bool]]:
        """Prepend text-readable document contents (≤100KB) so the agent reads them inline; returns
        ``(body, media_text_inlined)`` with one flag per ``cached_urls`` entry (True = injected)."""
        inlined = [False] * len(cached_urls)
        for i, doc_path in enumerate(cached_urls):
            p = Path(doc_path)
            if p.suffix.lower() not in _TEXT_INJECT_EXTS:
                continue
            try:
                file_size = p.stat().st_size
                if file_size > _MAX_TEXT_INJECT_BYTES:
                    print(f"[{self.name}] Skipping text injection for {doc_path} ({file_size} bytes > {_MAX_TEXT_INJECT_BYTES})", flush=True)
                    continue
                content = p.read_text(encoding="utf-8-sig", errors="replace")
                parts = p.name.split("_", 2)  # strip the doc_<hex>_ prefix for display
                injection = f"[Content of {parts[2] if len(parts) >= 3 else p.name}]:\n{content}"
                body = f"{injection}\n\n{body}" if body else injection
                inlined[i] = True
                print(f"[{self.name}] Injected text content from: {doc_path}", flush=True)
            except Exception as e:
                print(f"[{self.name}] Failed to read document text: {e}", flush=True)
        return body, inlined

    def _quoted_media(self, data: dict[str, Any], raw_reply_id: Any) -> list[tuple[str, str]]:
        """``(path, mime)`` for the quoted message's attachment, folded into this event's own media so the
        vision/audio pipeline sees it like a direct send. ``contextInfo.quotedMessage`` carries only a
        thumbnail stub, so the bridge resolves INBOUND quotes from its download cache (``quotedMediaUrls``,
        guarded like direct media: a rogue bridge could hand back /etc/passwd); quotes of OUR media (cron
        chart, generated image — any path we chose) resolve from the outbound index written by
        ``_send_media_to_bridge``."""
        quoted_type = str(data.get("quotedMediaType") or "").strip()
        accepted: list[tuple[str, str]] = []
        for path in data.get("quotedMediaUrls") or []:
            if not (isinstance(path, str) and os.path.isabs(path) and _is_allowed_bridge_path(path)):
                print(f"[{self.name}] Rejected quoted-media path outside cache dir: {path}", flush=True)
                continue
            accepted.append((path, _QUOTED_MIME_BY_BRIDGE_KIND.get(quoted_type, "application/octet-stream")))
        if not accepted and raw_reply_id is not None:
            from gateway import rich_sent_store
            accepted = rich_sent_store.lookup_media(data.get("chatId", ""), str(raw_reply_id))
        for path, _ in accepted:
            print(f"[{self.name}] Attached quoted-reply media: {path}", flush=True)
        return accepted

    async def _build_message_event(self, data: dict[str, Any]) -> Optional[MessageEvent]:
        """Build a MessageEvent from bridge message data, downloading images to cache."""
        try:
            if not self._should_process_message(data):
                return None
            msg_type = self._classify_bridge_message(data)
            source = self.build_source(chat_id=data.get("chatId", ""), chat_name=data.get("chatName"), chat_type="group" if data.get("isGroup", False) else "dm",
                                       user_id=data.get("senderId"), user_name=data.get("senderName"),
                                       message_id=data.get("messageId"))
            cached_urls, media_types = await self._collect_bridge_media(data, msg_type)
            body = data.get("body", "")
            if data.get("isGroup"):
                body = self._clean_bot_mention_text(body, data)
            if msg_type == MessageType.VOICE and cached_urls and str(body).strip().lower() == "[ptt received]":
                body = ""  # Bridge placeholder for captionless voice notes; the audio is the payload.
            # Quoted message stays in structured fields only — GatewayRunner renders the "[Replying to: ...]" pointer.
            quoted = bool(data.get("hasQuotedMessage"))
            raw_reply_id = data.get("quotedMessageId") if quoted else None
            if quoted:
                for path, mime in self._quoted_media(data, raw_reply_id):
                    cached_urls.append(path)
                    media_types.append(mime)
            media_text_inlined: list[bool] = []
            if msg_type == MessageType.DOCUMENT and cached_urls:
                body, media_text_inlined = self._inject_document_text(cached_urls, body)
            native_metadata = data.get("nativeMetadata")
            metadata: dict[str, Any] = {k: v for k, v in (
                ("whatsapp_native_type", str(data.get("nativeType") or "").strip()),
                ("whatsapp_native", native_metadata if isinstance(native_metadata, dict) else None),
            ) if v}
            # ``fromOwner`` = owner-typed inbound fromMe (gated by WHATSAPP_FORWARD_OWNER_MESSAGES at the bridge); surfaced as
            # metadata AND a text prefix so the marker survives downstream failures before silent_ingest.
            if data.get("fromOwner"):
                metadata["whatsapp_from_owner"] = True
                if not body.startswith(_OWNER_REPLY_PREFIX):
                    body = f"{_OWNER_REPLY_PREFIX}{body}"
            return MessageEvent(
                text=body, message_type=msg_type, source=source, raw_message=data, message_id=data.get("messageId"),
                media_urls=cached_urls, media_types=media_types, media_text_inlined=media_text_inlined, metadata=metadata,
                reply_to_message_id=str(raw_reply_id) if raw_reply_id is not None else None,
                reply_to_text=str(data.get("quotedText") or "").strip() or None,
                reply_to_author_id=(self._normalize_whatsapp_id(data.get("quotedParticipant")) or None) if quoted else None,
                reply_to_is_own_message=self._message_is_reply_to_bot(data) if quoted else False,
            )
        except Exception as e:
            print(f"[{self.name}] Error building event: {e}")
            return None


# ── Plugin glue: register(ctx) plus the hooks for gateway/run.py, gateway/config.py, hermes_cli/gateway.py, send_message_tool.py.

_WA_EXT_MEDIA_TYPE = {
    **dict.fromkeys((".jpg", ".jpeg", ".png", ".webp", ".gif"), "image"),
    **dict.fromkeys((".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp"), "video"),
    **dict.fromkeys((".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac"), "audio"),
}


def _bridge_media_type(file_path: str, is_voice: bool, force_document: bool) -> str:
    """Local file → bridge ``mediaType`` (image|video|audio|document); ``force_document`` = the [[as_document]] directive."""
    return "document" if force_document else "audio" if is_voice else _WA_EXT_MEDIA_TYPE.get(os.path.splitext(file_path)[1].lower(), "document")


def _normalize_outbound_mentions(mentions: list[str] | None) -> list[str]:
    """Valid participant JIDs, deduplicated, order preserved."""
    return list(dict.fromkeys(jid for m in mentions or () if (jid := normalize_whatsapp_mention_jid(m))))


async def _standalone_send(pconfig, chat_id, message, *, thread_id=None, media_files=None, force_document=False,
                           caption=None, mentions=None):
    """Out-of-process delivery via the bridge HTTP API (standalone_sender_fn: cron apart from the gateway); ``caption`` rides on the media bubble."""
    try:
        import aiohttp
    except ImportError:
        return send_error("aiohttp not installed. Run: pip install aiohttp")
    try:
        from hermes_constants import get_hermes_home
        from .bridge_ownership import standalone_bridge_port
        bridge_port = standalone_bridge_port(
            get_hermes_home(), (getattr(pconfig, "extra", {}) or {}).get("bridge_port")
        )
        own_session = str(_session_dir(getattr(pconfig, "extra", {}) or {}))
        normalized_chat_id = to_whatsapp_jid(chat_id)
        media = media_files or []
        # A caption only applies to a single media file — never repeat it across a multi-file send.
        media_caption = caption if (caption and len(media) == 1) else None
        pending_mentions = _normalize_outbound_mentions(mentions)

        def _mention_first_payload(payload):
            nonlocal pending_mentions
            if pending_mentions:
                payload["mentions"] = pending_mentions
                pending_mentions = []
            return payload

        last_message_id = None
        async with aiohttp.ClientSession() as session:
            # Whose bridge holds the port is checked before anything is posted: on a shared port another profile's
            # bridge would deliver from that profile's WhatsApp account. A bridge without ``session`` predates the field.
            async with session.get(
                f"http://localhost:{bridge_port}/health",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return send_error(
                        f"WhatsApp bridge on port {bridge_port} answered /health with HTTP {resp.status}; nothing was sent.")
                health = await resp.json()
            reported_session = health.get("session")
            if reported_session and reported_session != own_session:
                return send_error(
                    f"WhatsApp bridge on port {bridge_port} serves another profile's session ({reported_session}); "
                    "nothing was sent. Give each profile its own platforms.whatsapp.extra.bridge_port.")
            if pending_mentions:
                if not (health.get("capabilities") or {}).get("outboundMentions"):
                    return {"error": (
                        "WhatsApp bridge does not support native mentions; "
                        "restart it from the same Hermes version.")}

            async def _post(path, payload, total, error_label=None):
                """``(messageId, None)`` on 200, else ``(None, error_dict)`` (body read only when labelled)."""
                url = f"http://localhost:{bridge_port}/{path}"
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=total)) as resp:
                    if resp.status == 200:
                        return (await resp.json()).get("messageId"), None
                    return None, {} if error_label is None else send_error(f"WhatsApp {error_label} error ({resp.status}): {await resp.text()}")
            # 1) Text first (skipped when media-only or when the text rides as the caption).
            if (message or "").strip() and not media_caption:
                payload = _mention_first_payload({"chatId": normalized_chat_id, "message": message})
                last_message_id, err = await _post("send", payload, 30, "bridge")
                if err:
                    return err
            # 2) Each media file as a native attachment (mediaType picks the WhatsApp kind).
            for media_path, is_voice in media:
                if not os.path.exists(media_path):
                    # In caption mode the words would vanish with the missing file — deliver the caption as a plain message.
                    if media_caption:
                        try:
                            payload = _mention_first_payload({"chatId": normalized_chat_id, "message": media_caption})
                            await _post("send", payload, 30)
                        except Exception:
                            logger.warning("WhatsApp caption-fallback send failed for missing media")
                    return send_error(f"WhatsApp media file not found: {media_path}")
                media_type = _bridge_media_type(media_path, is_voice, force_document)
                payload: dict[str, Any] = {"chatId": normalized_chat_id, "filePath": media_path, "mediaType": media_type}
                payload.update({k: v for k, v in (("fileName", os.path.basename(media_path) if media_type == "document" else None), ("caption", media_caption)) if v})
                payload = _mention_first_payload(payload)
                mid, err = await _post("send-media", payload, 120, "media")
                if err:
                    return err
                last_message_id = mid or last_message_id
        return {"success": True, "platform": "whatsapp", "chat_id": normalized_chat_id, "message_id": last_message_id}
    except Exception as e:
        return send_error(f"WhatsApp send failed: {e}")


def interactive_setup() -> None:
    """Guide the user through WhatsApp setup (CLI helpers lazy-imported)."""
    from hermes_cli.config import get_env_value, remove_env_value, save_env_value
    from hermes_cli.cli_output import prompt, prompt_yes_no, print_header, print_info, print_success
    print_header("WhatsApp")
    print_info("WhatsApp uses a local Node.js bridge (WhatsApp Web client).")
    print_info("Start the bridge separately; the gateway connects to it over HTTP.")
    if (get_env_value("WHATSAPP_ENABLED") or "").lower() in {"true", "1", "yes"}:
        print_info("WhatsApp: already enabled")
        if not prompt_yes_no("Reconfigure WhatsApp?", False):
            return
    if not prompt_yes_no("Enable WhatsApp?", True):
        save_env_value("WHATSAPP_ENABLED", "false")
        print_info("WhatsApp left disabled")
        return
    save_env_value("WHATSAPP_ENABLED", "true")
    print_success("WhatsApp enabled")
    allowed_users = prompt("Allowed user IDs (comma-separated, leave empty for no allowlist)")
    if allowed_users:
        save_env_value("WHATSAPP_ALLOWED_USERS", allowed_users.replace(" ", ""))
        print_success("WhatsApp allowlist configured")
    home_channel = prompt("Home chat ID for cron delivery (leave empty to skip)").strip()
    if home_channel:
        save_env_value("WHATSAPP_HOME_CHANNEL", home_channel)
    elif remove_env_value("WHATSAPP_HOME_CHANNEL"):
        print_info("Home channel cleared.")


# config.yaml whatsapp: key → env var. Env vars take precedence over YAML.
_YAML_BRIDGE = (  # (yaml key, env var, kind) for apply_yaml_bridge
    ("require_mention", "WHATSAPP_REQUIRE_MENTION", "lower"), ("dm_policy", "WHATSAPP_DM_POLICY", "lower"),
    ("group_policy", "WHATSAPP_GROUP_POLICY", "lower"), ("mention_patterns", "WHATSAPP_MENTION_PATTERNS", "json"),
    ("free_response_chats", "WHATSAPP_FREE_RESPONSE_CHATS", "csv"), ("allow_from", "WHATSAPP_ALLOWED_USERS", "csv"),
    ("group_allow_from", "WHATSAPP_GROUP_ALLOWED_USERS", "csv"),
)


def _apply_yaml_config(yaml_cfg: dict, whatsapp_cfg: dict) -> dict | None:
    """``apply_yaml_config_fn`` (#24849): config.yaml whatsapp: keys → WHATSAPP_* env (env wins; skipped under
    a multiplexed secondary profile's scope, #80099) + ``PlatformConfig.extra`` (extra-first readers)."""
    return _apply_yaml_bridge(whatsapp_cfg, _YAML_BRIDGE)



def _is_connected(config) -> bool:
    """Connected == WHATSAPP_ENABLED opt-in (or an enabled PlatformConfig with extras); auth lives in the bridge."""
    if config is not None and getattr(config, "enabled", False) and (getattr(config, "extra", {}) or {}):
        return True
    # Via hermes_cli.gateway.get_env_value (not os.getenv) so setup-status callers that patch it observe the same value.
    import hermes_cli.gateway as gateway_mod
    return (gateway_mod.get_env_value("WHATSAPP_ENABLED") or "").strip().lower() in {"true", "1", "yes"}


# System-prompt platform hint (PlatformEntry.platform_hint); cached-prompt bytes, keep stable.
_PLATFORM_HINT = (
    "You are on WhatsApp. Standard markdown auto-converts to WhatsApp syntax (*bold*, _italic_, ~strike~, "
    "monospace) \u2014 write markdown freely, bullets included. No tables \u2014 use bullets or labeled lines. "
    "You can send files natively: write MEDIA:/absolute/path/to/file in your response. "
    "Images (.jpg, .png, .webp) send as photos, videos (.mp4, .mov) play "
    "inline, other files arrive as documents; image URLs via ![alt](url) send as photos."
)


# WhatsApp JIDs (@g.us, @s.whatsapp.net, @lid, broadcast/newsletter) are native targets the adapter
# accepts verbatim — explicit, never home-channel. E.164 numbers use core's phone rule.
_SEND_JID_RE = re.compile(r"^\s*[\w-]+@(?:g\.us|s\.whatsapp\.net|lid|broadcast|newsletter)\s*$", re.IGNORECASE)


def register(ctx) -> None:
    from . import gateway_env  # env -> GatewayConfig bridge (gateway.config_env)
    ctx.register_platform(
        target_ref_syntax_fn=regex_stripped_parser(_SEND_JID_RE),
        # Media goes through Baileys /send-media; ``mentions`` force the standalone route too.
        standalone_send=StandaloneSendPolicy(label="WhatsApp", media=True, captionable=True,
                                             forward_force_document=True, mentions=True, text_direct=True),
        name="whatsapp", label="WhatsApp", adapter_factory=WhatsAppAdapter, check_fn=check_whatsapp_requirements,
        dashboard_description="Use Hermes through the bundled WhatsApp bridge with QR-based auth.",
        docs_url="https://github.com/tulir/whatsmeow",
        dashboard_env_vars=(
            "WHATSAPP_ENABLED", "WHATSAPP_MODE", "WHATSAPP_DM_POLICY", "WHATSAPP_ALLOWED_USERS",
        ),
        dashboard_required_env=(),
        is_connected=_is_connected, required_env=["WHATSAPP_ENABLED"],
        install_hint="WhatsApp requires a Node.js bridge — see the WhatsApp messaging docs",
        setup_fn=interactive_setup, apply_yaml_config_fn=_apply_yaml_config, cron_deliver_env_var="WHATSAPP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send, max_message_length=4096, emoji="💬", allow_update_command=True,
        platform_hint=_PLATFORM_HINT,
        # Baileys bridge supports /edit.
        display_tier="medium",
        env_overrides=gateway_env.ENV_OVERRIDES,
        env_enable_credentials=gateway_env.ENV_ENABLE_CREDENTIALS, **gateway_env.AUTHZ,
    )
