"""OpenAI-compatible shim that forwards Hermes requests to `copilot --acp`.

Each request starts a short-lived ACP session, sends the formatted conversation
as one prompt, collects text chunks, and returns the minimal OpenAI-client shape.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import queue
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from agent.acp_openai_bridge import (
    completion_to_stream_chunks as _completion_to_stream_chunks,
    extract_tool_calls_from_text as _extract_tool_calls_from_text,
    render_tool_bridge_sections as _render_tool_bridge_sections,
)
from agent.copilot_acp_durable_receipt import (
    _ManagedDurableReceiptWitness,
    capture_managed_durable_receipt_witness as _capture_managed_durable_receipt_witness,
    validate_managed_durable_receipt_witness as _validate_managed_durable_receipt_witness,
    mark_managed_durable_receipt_unknown as _mark_managed_durable_receipt_unknown,
)
from agent.file_safety import (
    get_nt_namespace_error, get_read_block_error, get_write_denied_error, is_write_approval_required)
from agent.redact import redact_sensitive_text
from agent.runtime_cwd import resolve_agent_cwd
from tools.environments.local import hermes_subprocess_env

ACP_MARKER_BASE_URL = "acp://copilot"
logger = logging.getLogger(__name__)
_DEFAULT_TIMEOUT_SECONDS = 900.0
# Stderr fingerprint of the deprecated `gh copilot` extension. Require BOTH the product name
# AND a deprecation marker: the NEW `@github/copilot` CLI legitimately mentions "copilot-cli".
_DEPRECATION_REQUIRED = ("gh-copilot",)
_DEPRECATION_MARKERS = ("has been deprecated", "no commands will be executed")
_ROLE_LABELS = {"system": "System", "user": "User", "assistant": "Assistant", "tool": "Tool", "context": "Context"}
# Probe verdicts per binary path (~50ms --help paid once per process). Only definitive
# True/False is cached, so a CLI installed mid-session is picked up.
_ACP_PROBE_CACHE: dict[str, bool] = {}
_PROMPT_PREAMBLE = (
    "You are being used as the active ACP agent backend for Hermes.",
    "Use ACP capabilities to complete tasks.",
    "IMPORTANT: If you take an action with a tool, you MUST output tool calls using <tool_call>{...}</tool_call> blocks with JSON exactly in OpenAI function-call shape.",
    "If no tool is needed, answer normally.",
)
_INITIALIZE_PARAMS = {
    "protocolVersion": 1,
    "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}},
    "clientInfo": {"name": "hermes-agent", "title": "Hermes Agent", "version": "0.0.0"},
}
_DEPRECATED_CLI_ERROR = (
    "Hermes ACP mode requires the NEW GitHub Copilot CLI (github.com/github/copilot-cli), but the binary it just "
    "spawned is the deprecated `gh copilot` extension.\n\n"
    "Install the new CLI:\n  npm install -g @github/copilot\n  # then verify with: copilot --help\n\n"
    "If `copilot` already resolves to the new CLI but you still see this,\npoint Hermes at it explicitly:\n"
    "  export HERMES_COPILOT_ACP_COMMAND=/path/to/new/copilot\n\n"
    "Alternative: use the `copilot` provider (no ACP, hits the Copilot API\ndirectly with a Copilot subscription "
    "token) via `hermes setup`.\n\nOriginal error:\n"
)


def _is_gh_copilot_deprecation_message(stderr_text: str) -> bool:
    """True iff stderr looks like the deprecated gh-copilot extension's banner."""
    lower = stderr_text.lower()
    return any(req in lower for req in _DEPRECATION_REQUIRED) and any(m in lower for m in _DEPRECATION_MARKERS)


def _resolve_command() -> str:
    return os.getenv("HERMES_COPILOT_ACP_COMMAND", "").strip() or os.getenv("COPILOT_CLI_PATH", "").strip() or "copilot"


def _resolve_args() -> list[str]:
    return shlex.split(os.getenv("HERMES_COPILOT_ACP_ARGS", "").strip()) or ["--acp", "--stdio"]


def _acp_supported(command: str, args: list[str]) -> bool | None:
    """Tri-state ``--acp`` probe (a CLI without the flag exits 1 and the parent would wait the
    full child timeout for stdout that never arrives). True = help advertises --acp; False =
    help ran cleanly without it (caller fast-fails); None = inconclusive (binary missing /
    --help failed → normal spawn error). Skipped when ``--acp`` is not in ``args`` (custom transport)."""
    if "--acp" not in args:
        return True
    if (cached := _ACP_PROBE_CACHE.get(command)) is not None:
        return cached
    try:
        probe = subprocess.run(
            [command, "--help"],
            # Explicit codec because text=True alone decodes with the
            # locale default and crashes on non-ASCII help text under
            # GBK/CP932 locales.
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=5,
            stdin=subprocess.DEVNULL,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if probe.returncode != 0:
        return None
    # ``--acp`` as a flag token; tolerate spacing and ``[--acp]`` variants.
    verdict = _ACP_PROBE_CACHE[command] = bool(re.search(r"(?:^|[\s\[])--acp(?:[\s=\],]|$)", probe.stdout, re.MULTILINE))
    return verdict


def _resolve_home_dir() -> str:
    """Stable HOME for child ACP processes; the temp dir as a last resort so the child never starts HOME-less."""
    if home := os.environ.get("HOME", "").strip():
        return home
    if (expanded := os.path.expanduser("~")) and expanded != "~":
        return expanded
    try:
        import pwd

        return pwd.getpwuid(os.getuid()).pw_dir.strip() or tempfile.gettempdir()  # windows-footgun: ok — POSIX fallback inside try/except (pwd import fails on Windows)
    except Exception:
        return tempfile.gettempdir()


def _build_subprocess_env() -> dict[str, str]:
    from hermes_constants import apply_subprocess_home_env

    # Copilot ACP drives a model and needs LLM provider credentials; the central helper still
    # strips Tier-1 secrets (bot tokens, GitHub auth, infra).
    # See #29157.
    env = hermes_subprocess_env(inherit_credentials=True)
    env["HOME"] = _resolve_home_dir()
    apply_subprocess_home_env(env)
    return env


def _jsonrpc_result(message_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _jsonrpc_error(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


def _enabled_id_list(entries: Any, key: str) -> list[str]:
    """Ordered ids whose ``_meta.copilotEnablement`` is not ``disabled``."""
    seen: set[str] = set()
    result: list[str] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        value = str(entry.get(key) or "").strip()
        if (not value or value in seen
                or str((entry.get("_meta") or {}).get("copilotEnablement") or "").strip().lower() == "disabled"):
            continue
        seen.add(value)
        result.append(value)
    return result


def _model_config_option(session: dict[str, Any]) -> dict[str, Any] | None:
    return next((option for option in (session.get("configOptions") or []) if isinstance(option, dict)
                 and "model" in (option.get("category"), option.get("id"))), None)


def _session_model_ids(session: dict[str, Any]) -> list[str]:
    """Account-authorized model ids advertised by ``session/new`` in ACP v1 or its legacy extension."""
    if option := _model_config_option(session):
        return _enabled_id_list(option.get("options"), "value")
    return _legacy_session_model_ids(session)


def _pump_acp_stream(
    stream: Any, sink: Callable[[str], None], receipt: Any, name: str,
    setup_gate: threading.Event | None = None,
) -> None:
    if setup_gate is not None:
        setup_gate.wait()
    try:
        for line in stream or ():
            sink(line)
    except Exception:
        if receipt is not None:
            receipt.reader_failed(f"{name}_reader_error")
        raise
    finally:
        if receipt is not None:
            receipt.reader_eof(name, threading.current_thread())


def _start_acp_pumps(client: Any, proc: Any, inbox: queue.Queue, stderr_tail: deque[str], decode) -> None:
    from agent.memory_provider import spawn_context_thread
    parent = getattr(getattr(client, "_producer_policy", None), "parent", None)
    receipt = getattr(client, "_process_receipts", {}).get(proc)
    bind_reader = None
    if parent is not None and receipt is not None:
        from gateway.runtime_service_operations import bind_reader as bind_receipt_reader
        bind_reader = bind_receipt_reader
    readers: list[tuple[str, Any]] = []
    setup_gate = threading.Event()
    setup_done = threading.Event()
    with client._active_process_lock:
        if proc not in client._active_processes:
            raise RuntimeError("ACP process was released before reader setup")
        client._process_setup_fences[proc] = setup_done
        if receipt is not None:
            receipt.setup_fence = setup_done
    try:
        stdout_pump = spawn_context_thread(
            target=_pump_acp_stream, name=f"acp-stdout-{proc.pid}",
            args=(proc.stdout, lambda line: inbox.put(decode(line)), receipt, "stdout", setup_gate), daemon=True,
        )
        readers.append(("stdout", stdout_pump))
        with client._active_process_lock:
            client._process_readers[proc] = tuple(reader for _, reader in readers)
        stderr_pump = spawn_context_thread(
            target=_pump_acp_stream, name=f"acp-stderr-{proc.pid}",
            args=(proc.stderr, lambda line: stderr_tail.append(line.rstrip("\n")), receipt, "stderr", setup_gate), daemon=True,
        )
        readers.append(("stderr", stderr_pump))
        # Publish both identities before either thread can run or any close path can consume
        # the process.  A setup failure leaves this complete created set in the local map.
        with client._active_process_lock:
            client._process_readers[proc] = tuple(reader for _, reader in readers)
        if bind_reader is not None:
            for stream, reader in readers:
                bind_reader(parent, stream, reader)
        for _, reader in readers:
            reader.start()
    except BaseException:
        # A wrapper or binder may fail before it reaches the receipt.  Retain each created
        # identity directly so reconciliation can see every reader, including an unstarted
        # or partly-started thread, while the original exception remains the caller's error.
        if receipt is not None:
            for stream, reader in readers:
                if stream not in getattr(receipt, "readers", {}):
                    try:
                        receipt.bind_reader(stream, reader)
                    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                        logger.warning("ACP reader identity could not be retained", exc_info=True)
        raise
    finally:
        setup_gate.set()
        setup_done.set()


def _legacy_session_model_ids(session: dict[str, Any]) -> list[str]:
    return _enabled_id_list((session.get("models") or {}).get("availableModels"), "modelId")


def _model_selection_request(session: dict[str, Any], requested_model: str) -> tuple[str, dict[str, str]] | None:
    """ACP request selecting ``requested_model`` for ``session``: stable v1
    ``session/set_config_option``, else Copilot's pre-stabilization ``session/set_model``
    when no model config option is advertised. A reported model list is authoritative:
    unknown and policy-disabled ids return None instead of being sent."""
    session_id = str(session.get("sessionId") or "").strip()
    requested_model = str(requested_model or "").strip()
    if not session_id or not requested_model or requested_model == "copilot-acp":
        return None
    option = _model_config_option(session)
    if option:
        if requested_model not in _enabled_id_list(option.get("options"), "value"):
            return None
        return "session/set_config_option", {"sessionId": session_id, "configId": str(option.get("id") or "model"), "value": requested_model}
    available = _legacy_session_model_ids(session)
    return None if available and requested_model not in available else ("session/set_model", {"sessionId": session_id, "modelId": requested_model})


def _format_messages_as_prompt(
    messages: list[dict[str, Any]], model: str | None = None, tools: list[dict[str, Any]] | None = None, tool_choice: Any = None,
) -> str:
    # Deliberately no "requested model" line: the model is applied for real via ACP session/set_model;
    # a prompt-text mention makes a substituted backend model FALSELY self-identify as the requested
    # one. Copilot has no tools of its own that collide with Hermes', so forward the whole toolset.
    sections: list[str] = [*_PROMPT_PREAMBLE, *_render_tool_bridge_sections(tools, tool_choice)]
    transcript: list[str] = []
    for message in (m for m in messages if isinstance(m, dict)):
        role = str(message.get("role") or "unknown").strip().lower()
        if rendered := _render_message_content(message.get("content")):
            transcript.append(f"{_ROLE_LABELS.get(role, 'Context')}:\n{rendered}")
    if transcript:
        sections.append("Conversation transcript:\n\n" + "\n\n".join(transcript))
    sections.append("Continue the conversation from the latest user request.")
    return "\n\n".join(section.strip() for section in sections if section and section.strip())


def _render_message_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, dict):
        if "text" in content:
            return str(content.get("text") or "").strip()
        return content["content"].strip() if isinstance(content.get("content"), str) else json.dumps(content, ensure_ascii=True)
    if isinstance(content, list):
        parts = [item if isinstance(item, str) else item["text"].strip() for item in content if isinstance(item, str)
                 or (isinstance(item, dict) and isinstance(item.get("text"), str) and item["text"].strip())]
        return "\n".join(parts).strip()
    return str(content).strip()


def _ensure_path_within_cwd(path_text: str, cwd: str, *, verb: str) -> Path:
    # Raw-string check BEFORE resolve(): resolving an NT-namespace path is the NTLM-leak trigger.
    if nt_error := get_nt_namespace_error(path_text, verb=verb):
        raise PermissionError(nt_error)
    if not Path(path_text).is_absolute():
        raise PermissionError("ACP file-system paths must be absolute.")
    resolved, root = Path(path_text).resolve(), Path(cwd).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PermissionError(f"Path '{resolved}' is outside the session cwd '{root}'.") from exc
    return resolved


def _effective_timeout(timeout: Any) -> float:
    """Normalise a float or httpx.Timeout-like object to wall-clock seconds (largest component wins)."""
    if isinstance(timeout, (int, float)):
        return float(timeout)
    candidates = [getattr(timeout, attr, None) for attr in ("read", "write", "connect", "pool", "timeout")]
    return max((float(v) for v in candidates if isinstance(v, (int, float))), default=_DEFAULT_TIMEOUT_SECONDS)


def _fs_read_text_file(params: dict[str, Any], cwd: str) -> Any:
    path = _ensure_path_within_cwd(str(params.get("path") or ""), cwd, verb="Read")
    if block_error := get_read_block_error(str(path)):
        raise PermissionError(block_error)
    try:
        content = path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        content = ""
    line, limit = params.get("line"), params.get("limit")
    if isinstance(line, int) and line > 1:
        end = line - 1 + limit if isinstance(limit, int) and limit > 0 else None
        content = "".join(content.splitlines(keepends=True)[line - 1:end])
    return {"content": redact_sensitive_text(content, force=True) if content else content}


def _fs_write_text_file(params: dict[str, Any], cwd: str) -> Any:
    path = _ensure_path_within_cwd(str(params.get("path") or ""), cwd, verb="Write")
    if denied := get_write_denied_error(str(path)):
        raise PermissionError(denied)
    if is_write_approval_required(str(path)):  # soft-gated for interactive tools; the ACP shim has no human channel → fail closed
        raise PermissionError(f"Write denied: '{path}' requires interactive approval and cannot be written through the ACP file bridge.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(params.get("content") or ""), encoding="utf-8")
    return None


_FS_HANDLERS = {"fs/read_text_file": _fs_read_text_file, "fs/write_text_file": _fs_write_text_file}


class CopilotACPClient:
    """Minimal OpenAI-client-compatible facade for Copilot ACP."""

    # Declared for agent/auxiliary_client.py: this shim drives an ACP subprocess over stdio, so it is
    # already a complete client (never re-dispatch through a wire adapter) and async-safe as-is.
    HERMES_SKIP_TRANSPORT_WRAP = True
    HERMES_SKIP_ASYNC_WRAP = True

    def __init__(
        self, *, api_key: str | None = None, base_url: str | None = None, default_headers: dict[str, str] | None = None,
        acp_command: str | None = None, acp_args: list[str] | None = None, acp_cwd: str | None = None, command: str | None = None,
        args: list[str] | None = None, producer_policy: Any = None, **_: Any,
    ):
        self.api_key, self.base_url = api_key or "copilot-acp", base_url or ACP_MARKER_BASE_URL
        self._default_headers = dict(default_headers or {})
        self._acp_command = acp_command or command or _resolve_command()
        self._acp_args = list(acp_args or args or _resolve_args())
        # The session's workspace, not the process cwd: under the gateway every turn runs in
        # the daemon (or a managed worker) whose own cwd is unrelated to the client's.
        self._acp_cwd = str(Path(acp_cwd or resolve_agent_cwd()).resolve())
        self._producer_policy = producer_policy
        if producer_policy is not None:
            from gateway.runtime_service_operations import validate_producer_policy
            validate_producer_policy(producer_policy, spawning=True)
            parent = getattr(producer_policy, "parent", None)
            if parent is not None:
                parent._catalog_client = self
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create_chat_completion))
        self.is_closed = False
        # Clients are cached and shared across concurrent callers (auxiliary tasks, async
        # dispatch), so several ACP sessions can be live on one instance. Track every live
        # child — a single slot would let one session's teardown kill a sibling's process
        # while its own leaked.
        self._active_processes: set[subprocess.Popen[str]] = set()
        self._active_process_lock = threading.Lock()
        self._process_receipts: dict[subprocess.Popen[str], Any] = {}
        self._process_readers: dict[subprocess.Popen[str], tuple[threading.Thread, ...]] = {}
        self._process_setup_fences: dict[subprocess.Popen[str], threading.Event] = {}
        # Catalog discovery keeps its cache proof separate; managed turns retain a
        # non-durable child witness until SessionDB readback and I04 delivery finish.
        self._process_proofs: dict[subprocess.Popen[str], Any] = {}
        # Immutable binding captured when each managed child is admitted.  The provider's
        # ACP session id is a different namespace and is recorded separately in the witness.
        self._process_binding_snapshots: dict[subprocess.Popen[str], dict[str, Any]] = {}
        self._pending_managed_witnesses: deque[dict[str, Any]] = deque()

    @staticmethod
    def _terminate_process(proc: subprocess.Popen[str]) -> None:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()

    def _forget_process_record(self, proc: subprocess.Popen[str]) -> None:
        """Drop local child maps only after the owning custody receipt is settled."""
        with self._active_process_lock:
            self._active_processes.discard(proc)
            self._process_proofs.pop(proc, None)
            self._process_binding_snapshots.pop(proc, None)
            self._process_readers.pop(proc, None)
            self._process_setup_fences.pop(proc, None)
            self._process_receipts.pop(proc, None)

    def _register_spawned_process(
        self, proc: subprocess.Popen[str], receipt: Any, binding_snapshot: dict[str, Any] | None,
    ) -> None:
        with self._active_process_lock:
            self._active_processes.add(proc)
            if receipt is not None:
                self._process_receipts[proc] = receipt
            self.is_closed = False
        if binding_snapshot is not None:
            self._process_binding_snapshots[proc] = dict(binding_snapshot)

    def _release_spawn_failure(
        self, proc: Any, *, receipt: Any, transaction: Any, parent: Any,
        binding_snapshot: dict[str, Any] | None,
        reason: str, no_effect_verified: bool = False,
        original_error: BaseException | None = None,
    ) -> None:
        # A transactionless constructor exception after the launch boundary is opaque: no
        # process object or native transaction can prove that no child was created.  Keep
        # that receipt sticky; only an explicit no-effect proof may enter the retry path.
        retained_reason = (
            "opaque_launch_effect"
            if proc is None and transaction is None and not no_effect_verified
            else reason
        )
        try:
            if proc is not None:
                if proc not in self._process_receipts:
                    self._register_spawned_process(proc, receipt, binding_snapshot)
                if receipt is not None:
                    retain_partial = getattr(receipt, "retain_partial_launch", None)
                    if callable(retain_partial):
                        retain_partial(proc, transaction, reason=retained_reason)
                self._release_process(
                    proc, success=False, failure_reason=retained_reason, original_error=original_error,
                )
                return
            if receipt is None or parent is None:
                return
            if no_effect_verified is True:
                from gateway.runtime_service_operations import release_before_effect
                if release_before_effect(parent) is True:
                    self._refresh_closed_state()
                    return
            else:
                retain_partial = getattr(receipt, "retain_partial_launch", None)
                if callable(retain_partial):
                    retain_partial(None, transaction, reason=retained_reason)
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, retained_reason)
            if getattr(parent, "mode", None) == "managed_turn":
                self._retain_managed_failure(
                    None, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                    reason=retained_reason,
                )
            self._refresh_closed_state()
        except BaseException:
            # Keep a launch/cleanup failure from replacing the original spawn error.
            if original_error is None:
                raise
            logger.warning("ACP spawn cleanup could not settle its receipt", exc_info=True)

    def _retry_managed_intermediate_failure(
        self, parent: Any, service: Any, binding: Any, request: Any,
    ) -> bool | None:
        """Recover an existing typed prefix through the original child's cleanup path."""
        from agent.session_persistence_proof import (
            ManagedIntermediateWriterFence, _managed_intermediate_prefix_identity_matches,
            mark_managed_intermediate_writer_fence_unknown,
            recover_managed_intermediate_writer_fence,
        )
        owner = getattr(parent, "_owner_agent", None)
        fence = getattr(owner, "_classic_writer_intermediate_fence", None)
        if fence is None:
            return None
        if (type(fence) is not ManagedIntermediateWriterFence
                or fence.parent is not parent or fence.service is not service
                or fence.binding is not binding or fence.request is not request
                or not _managed_intermediate_prefix_identity_matches(owner, self, fence)):
            return False
        receipt = getattr(parent, "_physical", None)
        failed_process = getattr(receipt, "process", None)
        if not self._managed_failure_queue_is_exact(receipt, failed_process):
            return False
        if fence.state == "pending":
            if not mark_managed_intermediate_writer_fence_unknown(
                owner, self, "managed_intermediate_child_failed",
            ):
                return False
        if not recover_managed_intermediate_writer_fence(owner, self):
            return False
        if failed_process is not None:
            self._forget_process_record(failed_process)
        self._refresh_closed_state()
        return True

    def _retry_managed_process_failure(
        self, parent: Any, service: Any, binding: Any, request: Any,
    ) -> bool:
        intermediate_recovery = self._retry_managed_intermediate_failure(parent, service, binding, request)
        if intermediate_recovery is not None:
            return intermediate_recovery
        owner = getattr(parent, "_owner_agent", None)
        writer_recovery = None
        if owner is not None:
            from agent.session_persistence_proof import prepare_gateway_writer_proof_recovery
            writer_recovery = prepare_gateway_writer_proof_recovery(
                owner, self, parent, service, binding, request,
            )
            if writer_recovery is False:
                return False
        receipt = getattr(parent, "_physical", None)
        if receipt is None:
            return False
        old_process = getattr(receipt, "process", None)
        if not service.reconcile_process_failure(binding, request):
            return False
        retire_failed = getattr(self, "_retire_managed_failure_witness_after_retry", None)
        if not callable(retire_failed) or retire_failed(receipt, old_process) is not True:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "managed_failure_queue_residual")
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                logger.warning("managed retry residual queue could not mark custody unknown", exc_info=True)
            return False
        if old_process is not None:
            self._forget_process_record(old_process)
        receipt.managed_witness = None
        receipt.managed_client = None
        if writer_recovery is not None:
            from agent.session_persistence_proof import complete_gateway_writer_proof_recovery
            if not complete_gateway_writer_proof_recovery(
                owner, self, parent, service, binding, request, writer_recovery,
            ):
                return False
        self._refresh_closed_state()
        return True

    def _retry_managed_writer_proof(
        self, agent: Any, parent: Any, service: Any, binding: Any, request: Any,
        messages: list[dict[str, Any]],
    ) -> bool:
        """Recover one post-commit writer proof without admitting or writing again."""
        if getattr(parent, "_owner_agent", None) is not agent or getattr(agent, "_managed_acp_client", None) is not self:
            return False
        from agent.session_persistence_proof import (
            complete_gateway_writer_postcommit_recovery,
            prepare_gateway_writer_postcommit_recovery,
        )
        recovery = prepare_gateway_writer_postcommit_recovery(
            agent, self, parent, service, binding, request, messages,
        )
        if recovery is None:
            return False
        return complete_gateway_writer_postcommit_recovery(
            agent, self, parent, service, binding, request, recovery, messages,
        )

    def _managed_pending_for_process(self, proc: subprocess.Popen[str]) -> bool:
        return any(item.get("_process") is proc for item in self._pending_managed_witnesses)

    def _managed_failure_queue_is_exact(self, receipt: Any, process: Any) -> bool:
        """Refuse a second failed-child claim before changing native or queue custody."""
        failures = tuple(item for item in self._pending_managed_witnesses
                         if item.get("receipt") is receipt and item.get("_failure_reason"))
        return bool(failures) and all(item.get("_process") is process for item in failures)

    def _retire_managed_failure_witness_after_retry(
        self, receipt: Any, process: Any = None,
    ) -> bool:
        """Retire only the exact failed-child entry after same-owner recovery.

        A different failure entry on the same receipt is a live ownership claim.  It
        must remain visible and refuse the retry rather than being swept with the
        reconciled child.
        """
        if not self._managed_failure_queue_is_exact(receipt, process):
            return False
        retained = tuple(self._pending_managed_witnesses)
        removed = False
        remaining = []
        for item in retained:
            exact = (
                item.get("receipt") is receipt
                and item.get("_failure_reason")
                and item.get("_process") is process
            )
            if exact:
                removed = True
                continue
            remaining.append(item)
        self._pending_managed_witnesses = deque(remaining)
        if not removed:
            return False
        return not any(
            item.get("receipt") is receipt
            and item.get("_failure_reason")
            for item in self._pending_managed_witnesses
        )

    def has_pending_managed_custody(self) -> bool:
        """Keep a cached client live while any managed witness still owns physical custody."""
        if self._pending_managed_witnesses:
            return True
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        receipt = getattr(parent, "_physical", None)
        return bool(
            receipt is not None
            and getattr(receipt, "managed_client", None) is self
            and isinstance(getattr(receipt, "managed_witness", None), dict)
        )

    def _managed_service_is_reusable(self) -> bool:
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        service = getattr(parent, "_service", None)
        return bool(
            getattr(parent, "mode", None) == "managed_turn"
            and getattr(parent, "_catalog_client", None) is self
            and getattr(parent, "state", None) in {"parent_reserved", "request_reserved"}
            and service is not None
            and getattr(service, "external_parent", None) is parent
            and getattr(service, "unknown", False) is False
        )

    def _refresh_closed_state(self) -> None:
        with self._active_process_lock:
            self.is_closed = (
                not self._active_processes
                and not self.has_pending_managed_custody()
                and not self._managed_service_is_reusable()
            )

    def _retain_managed_failure(
        self, proc: subprocess.Popen[str], *, parent: Any, receipt: Any,
        binding_snapshot: dict[str, Any] | None, reason: str,
    ) -> None:
        """Retain an exact failed child witness until same-owner reconciliation."""
        if self._managed_pending_for_process(proc):
            return
        witness = dict(binding_snapshot or {})
        witness.update({"_process": proc, "parent": parent, "receipt": receipt, "_failure_reason": reason})
        self._pending_managed_witnesses.append(witness)
        if receipt is not None:
            receipt.managed_witness = dict(witness)
            receipt.managed_client = self
        owner = getattr(parent, "_owner_agent", None)
        if owner is not None:
            owner._managed_acp_client = self

    def _retain_managed_success(
        self, proc: subprocess.Popen[str], *, parent: Any, receipt: Any,
        binding_snapshot: dict[str, Any], proof: dict[str, Any],
    ) -> None:
        if self._managed_pending_for_process(proc):
            return
        witness = {**proof, "_process": proc, "parent": parent, "receipt": receipt,
                   "binding_snapshot": binding_snapshot}
        receipt.managed_witness = dict(witness)
        receipt.managed_client = self
        self._pending_managed_witnesses.append(witness)
        owner = getattr(parent, "_owner_agent", None)
        if owner is not None:
            owner._managed_acp_client = self

    def _retain_cleanup_failure(
        self, proc: Any, receipt: Any, parent: Any, binding_snapshot: dict[str, Any] | None,
        reason: str,
    ) -> None:
        if receipt is not None:
            receipt.mark_unknown(reason)
        if receipt is not None and parent is not None and getattr(parent, "mode", None) == "managed_turn":
            from gateway.runtime_service_operations import mark_unknown
            mark_unknown(parent, reason)
            self._retain_managed_failure(
                proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot, reason=reason,
            )

    def _setup_fence_ready(
        self, proc: Any, receipt: Any, parent: Any, binding_snapshot: dict[str, Any] | None,
    ) -> bool:
        with self._active_process_lock:
            setup_fence = self._process_setup_fences.get(proc)
        if setup_fence is None or setup_fence.wait(timeout=2.0):
            return True
        if receipt is not None:
            receipt.mark_unknown("reader_setup_failed")
            if getattr(parent, "mode", None) == "managed_turn":
                self._retain_managed_failure(
                    proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                    reason="reader_setup_failed",
                )
        return False

    def _run_cleanup_step(
        self, call: Callable[[], bool], proc: Any, receipt: Any, parent: Any,
        binding_snapshot: dict[str, Any] | None, reason: str, original_error: BaseException | None,
    ) -> bool | None:
        try:
            return call()
        except BaseException:
            self._retain_cleanup_failure(
                proc, receipt, parent, binding_snapshot, reason,
            )
            self._refresh_closed_state()
            if original_error is None:
                raise
            return None

    def _terminate_registered_process(self, proc: Any, transaction: Any) -> bool:
        """Attempt only the original owner's termination and retain opaque failures."""
        if transaction is None:
            self._terminate_process(proc)
            return False
        if getattr(transaction, "process", None) is not proc:
            return True
        try:
            process_live = proc.poll() is None
        except (OSError, RuntimeError, TypeError, ValueError):
            process_live = True
        if not process_live:
            return False
        terminate_owned_tree = getattr(transaction, "terminate_owned_tree", None)
        if not callable(terminate_owned_tree):
            return True
        try:
            terminate_owned_tree()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return True
        return False

    @staticmethod
    def _join_registered_readers(readers: tuple[Any, ...]) -> bool:
        """Join each retained started reader without replacing an unstarted identity."""
        failed = False
        for reader in readers:
            if getattr(reader, "ident", object()) is None:
                continue
            try:
                reader.join(timeout=2.0)
            except (OSError, RuntimeError, TypeError, ValueError):
                failed = True
        return failed

    def _release_process(
        self, proc: subprocess.Popen[str], *, success: bool = False, proof: Any = None,
        failure_reason: str | None = None, original_error: BaseException | None = None,
    ) -> None:
        """Reap one session's own child after setup is terminal and preserve opaque cleanup failures."""
        if proof is None:
            proof = self._process_proofs.get(proc)
        binding_snapshot = self._process_binding_snapshots.get(proc)
        policy = self._producer_policy
        parent = getattr(policy, "parent", None)
        with self._active_process_lock:
            receipt = self._process_receipts.get(proc)
        if not self._setup_fence_ready(proc, receipt, parent, binding_snapshot):
            self._refresh_closed_state()
            return
        with self._active_process_lock:
            self._active_processes.discard(proc)
            readers = self._process_readers.get(proc, ())
        transaction = getattr(receipt, "transaction", None) if receipt is not None else None
        native_termination_failed = self._run_cleanup_step(
            lambda: self._terminate_registered_process(proc, transaction),
            proc, receipt, parent, binding_snapshot, "native_cleanup_interrupted", original_error,
        )
        if native_termination_failed is None:
            return
        reader_join_failed = self._run_cleanup_step(
            lambda: self._join_registered_readers(readers),
            proc, receipt, parent, binding_snapshot, "reader_cleanup_interrupted", original_error,
        )
        if reader_join_failed is None:
            return
        if receipt is None or parent is None:
            self._forget_process_record(proc)
            self._refresh_closed_state()
            return
        from gateway.runtime_service_operations import mark_unknown, publish_catalog
        if len(readers) != 2 or reader_join_failed or native_termination_failed:
            reason = failure_reason or "reader_join_failed"
            mark_unknown(parent, reason)
            if getattr(parent, "mode", None) == "managed_turn":
                self._retain_managed_failure(
                    proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                    reason=reason,
                )
            elif not (native_termination_failed or transaction is not None):
                self._forget_process_record(proc)
            self._refresh_closed_state()
            return
        receipt.reader_joined("stdout", readers[0])
        receipt.reader_joined("stderr", readers[1])
        if not success:
            reason = failure_reason or "acp_session_failed"
            mark_unknown(parent, reason)
            if getattr(parent, "mode", None) == "managed_turn":
                self._retain_managed_failure(
                    proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                    reason=reason,
                )
            else:
                self._forget_process_record(proc)
            self._refresh_closed_state()
            return
        if getattr(parent, "mode", None) == "managed_turn":
            # A prompt response is only an authenticated child witness.  The managed
            # service cannot close or publish it until the canonical SessionDB writer
            # returns a readback proof and the existing I04 delivery finalizer settles.
            if (
                not isinstance(proof, dict)
                or not isinstance(binding_snapshot, dict)
                or proof.get("hermes_session_id") != binding_snapshot.get("hermes_session_id")
                or not proof.get("provider_session_id")
                or proof.get("admission_id") != binding_snapshot.get("admission_id")
                or proof.get("execution_generation") != binding_snapshot.get("execution_generation")
            ):
                mark_unknown(parent, "acp_result_unwitnessed")
                self._retain_managed_failure(
                    proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                    reason="acp_result_unwitnessed",
                )
                self._refresh_closed_state()
                return
            self._retain_managed_success(
                proc, parent=parent, receipt=receipt, binding_snapshot=binding_snapshot,
                proof=proof,
            )
            self._refresh_closed_state()
            return
        if proof is None:
            self._forget_process_record(proc)
            self._refresh_closed_state()
            return
        try:
            publish_catalog(parent, proof)
            if receipt.close_after_delivery() is not True:
                raise RuntimeError("ACP physical receipt did not settle")
            parent._physical = None
            parent.state = "request_reserved"
            self._forget_process_record(proc)
        except Exception:
            logger.exception("ACP physical receipt finalization failed")
            mark_unknown(parent, "physical_close_failed")
            self._forget_process_record(proc)
        self._refresh_closed_state()

    def _record_prompt_proof(self, proc: Any, session_id: str, result: Any, text: str) -> None:
        """Bind a successful prompt to the exact child/session before its release."""
        policy = self._producer_policy
        parent = getattr(policy, "parent", None)
        if parent is None or not session_id:
            return
        binding_snapshot = self._process_binding_snapshots.get(proc)
        payload = json.dumps(
            {"session_id": session_id, "result": result, "text": text},
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str,
        ).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        if getattr(parent, "mode", None) == "managed_turn":
            # This digest identifies the ACP child response only.  It is deliberately
            # not a durable result proof: the writer must prove the actual DB rows.
            if not isinstance(binding_snapshot, dict):
                return
            self._process_proofs[proc] = {
                "operation_id": binding_snapshot["operation_id"], "profile_id": binding_snapshot["profile_id"],
                "process_instance": binding_snapshot["process_instance"],
                "authority_epoch": binding_snapshot["authority_epoch"],
                "source_identity": binding_snapshot["source_identity"],
                "admission_id": binding_snapshot["admission_id"],
                "execution_generation": binding_snapshot["execution_generation"],
                "hermes_session_id": binding_snapshot["hermes_session_id"],
                "provider_session_id": session_id,
                "result_identity": f"acp:{session_id}:{digest}", "payload_digest": digest,
            }
            return
        from gateway.runtime_service_operations import ExternalPublicationProof
        self._process_proofs[proc] = ExternalPublicationProof(
            operation_id=parent.operation_id, profile_id=parent.profile_id,
            process_instance=parent.process_instance, authority_epoch=parent.authority_epoch,
            source_identity=parent.source_identity,
            result_identity=f"acp:{session_id}:{digest}", durable_result=True,
            payload_digest=digest,
        )

    def has_pending_managed_witness(self) -> bool:
        return bool(self._pending_managed_witnesses)

    def has_managed_durable_receipt(self, session_id: str) -> bool:
        """Require the writer proof to remain on this same physical receipt after handoff."""
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        receipt = getattr(parent, "_physical", None)
        witness = getattr(receipt, "managed_witness", None)
        proof = witness.get("durable_proof") if isinstance(witness, dict) else None
        return (
            isinstance(session_id, str)
            and isinstance(witness, dict)
            and proof is not None
            and getattr(proof, "session_id", None) == session_id
            and getattr(receipt, "managed_client", None) is self
        )

    def capture_managed_durable_receipt_witness(
        self, agent: Any, writer_proof: Any,
    ) -> _ManagedDurableReceiptWitness | None:
        return _capture_managed_durable_receipt_witness(self, agent, writer_proof)

    def validate_managed_durable_receipt_witness(
        self, agent: Any, writer_proof: Any, witness: Any,
    ) -> bool:
        return _validate_managed_durable_receipt_witness(self, agent, writer_proof, witness)

    def mark_managed_durable_receipt_unknown(self, witness: Any) -> bool:
        return _mark_managed_durable_receipt_unknown(self, witness)

    @staticmethod
    def _managed_result_rows(proof: Any, messages: list[dict[str, Any]]) -> tuple[int, ...]:
        """Select the ordered assistant/tool suffix after this admission's user row."""
        anchor = next(
            (index for index in range(len(messages) - 1, -1, -1)
             if isinstance(messages[index], dict) and messages[index].get("role") == "user"),
            None,
        )
        if anchor is None:
            return ()
        anchor_message = messages[anchor]
        if not isinstance(anchor_message, dict) or type(anchor_message.get("_row_id")) is not int:
            return ()
        anchor_id = anchor_message["_row_id"]
        if (
            anchor_id <= 0
            or type(proof.row_ids) is not tuple
            or type(proof.row_roles) is not tuple
            or len(proof.row_ids) != len(proof.row_roles)
        ):
            return ()
        try:
            proof_index = proof.row_ids.index(anchor_id)
        except ValueError:
            return ()
        if proof.row_roles[proof_index] != "user":
            return ()
        suffix_ids: list[int] = []
        suffix_roles: list[str] = []
        for message in messages[anchor:]:
            if not isinstance(message, dict) or type(message.get("_row_id")) is not int:
                return ()
            row_id = message["_row_id"]
            role = message.get("role")
            if row_id <= 0 or type(role) is not str or role not in {"user", "assistant", "tool"}:
                return ()
            suffix_ids.append(row_id)
            suffix_roles.append(role)
        if (
            not suffix_ids
            or len(set(suffix_ids)) != len(suffix_ids)
            or tuple(proof.row_ids[proof_index:]) != tuple(suffix_ids)
            or tuple(proof.row_roles[proof_index:]) != tuple(suffix_roles)
        ):
            return ()
        return tuple(row_id for row_id, role in zip(suffix_ids[1:], suffix_roles[1:])
                     if role in {"assistant", "tool"})

    @staticmethod
    def _managed_final_message(messages: list[dict[str, Any]]) -> dict[str, Any] | None:
        last = next((item for item in reversed(messages) if isinstance(item, dict)), None)
        if last is None or last.get("role") != "assistant" or last.get("tool_calls"):
            return None
        return last

    @staticmethod
    def _managed_intermediate_message(messages: list[dict[str, Any]]) -> dict[str, Any] | None:
        last = next((item for item in reversed(messages) if isinstance(item, dict)), None)
        if last is None:
            return None
        if last.get('role') == 'assistant' and isinstance(last.get('tool_calls'), list) and last['tool_calls']:
            return last
        if last.get('role') == 'tool' and isinstance(last.get('tool_call_id'), str) and last['tool_call_id']:
            return last
        return None

    def _commit_managed_intermediate_writer_proof(self, agent: Any, proof: Any, messages: list[dict[str, Any]]) -> bool:
        """Prove an ACP child's tool envelope without declaring the enclosing turn final."""
        return self._commit_managed_writer_proof(agent, proof, messages, intermediate=True)

    def _managed_intermediate_prefix_matches(
        self, agent: Any, fence: Any, current_durable: Any, capture: Any,
    ) -> bool:
        """Authenticate the existing receipt prefix before extending a terminal tail."""
        from agent.session_persistence_proof import SessionDBWriteProof, _managed_writer_prefix_extends
        from gateway.runtime_service_custody import DurableResultProof
        current_witness = None
        current_writer_proof = None
        if current_durable is getattr(fence.physical_witness, "durable_proof", None):
            current_witness = fence.physical_witness
            current_writer_proof = fence.writer_proof
        elif isinstance(current_durable, DurableResultProof):
            current_writer_proof = SessionDBWriteProof(
                store_path=current_durable.store_path, session_id=current_durable.session_id,
                row_ids=current_durable.row_ids, row_snapshots=current_durable.row_snapshots,
                row_sessions=current_durable.row_sessions, row_roles=current_durable.row_roles,
                payload_digest=current_durable.payload_digest,
            )
            current_witness = capture(agent, current_writer_proof)
        if current_witness is None:
            return False
        if current_writer_proof is not fence.writer_proof:
            if not _managed_writer_prefix_extends(
                agent, fence.writer_proof, fence.physical_witness,
                current_writer_proof, current_witness,
            ):
                return False
        if (
            current_witness.client is not fence.client
            or current_witness.parent is not fence.parent
            or current_witness.service is not fence.service
            or current_witness.request is not fence.request
            or current_witness.binding is not fence.binding
            or current_witness.receipt is not fence.receipt
        ):
            return False
        return True

    def _record_managed_intermediate_terminal(
        self, fence: Any, service: Any, request: Any, receipt: Any,
        current_durable: Any, durable: Any,
    ) -> bool:
        """Record a validated terminal extension on the existing physical receipt."""
        if current_durable != durable:
            try:
                service.record_result_durable(fence.binding, request, durable)
                # ServiceProcessReceipt.record_result_durable updates this in current
                # runtimes; keep the receipt-local managed witness explicit for the
                # same-owner capture path and preserve every provider-child field.
                managed_witness = getattr(receipt, "managed_witness", None)
                if not isinstance(managed_witness, dict):
                    return False
                receipt.managed_witness = {**managed_witness, "durable_proof": durable}
                release_sidecar = getattr(fence.binding.authority, "_preservation_release_sidecar", None)
                if callable(release_sidecar):
                    release_sidecar(fence.binding.admission_id, request, durable=True)
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                return False
        elif isinstance(getattr(receipt, "managed_witness", None), dict):
            receipt.managed_witness = {
                **receipt.managed_witness, "durable_proof": current_durable,
            }
        return True

    @staticmethod
    def _managed_request_durable(receipt: Any, request: Any):
        """Read only this request's durable object after a child witness is cleared."""
        request_id = getattr(getattr(request, "lease", None), "lease_id", None)
        receipt_durable = getattr(receipt, "durable_proofs", None)
        current_durable = (
            getattr(receipt, "managed_witness", {}).get("durable_proof")
            if isinstance(getattr(receipt, "managed_witness", None), dict) else None
        )
        # The receipt's durable map is the canonical fallback after a cleanup path
        # has cleared only the transient managed witness.  Read exactly this request's
        # object; never borrow another child or reconstruct a proof from fields.
        if current_durable is None and isinstance(receipt_durable, dict) and type(request_id) is str:
            current_durable = receipt_durable.get(request_id)
        return current_durable

    def _commit_managed_intermediate_terminal_proof(
        self, agent: Any, fence: Any, proof: Any, messages: list[dict[str, Any]],
    ) -> bool:
        """Extend an existing child proof for a finalizer-shaped no-next-child tail.

        The child has already been recorded on the receipt.  This path may only append the
        shaped terminal rows to that same SessionDB/receipt proof; it cannot create a provider
        child, replace a physical handle, or project the intermediate checkpoint as final.
        """
        from agent.session_persistence_proof import (
            ManagedIntermediateWriterFence, SessionDBWriteProof,
            _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF,
            _gateway_proof_rows_match,
            _managed_intermediate_prefix_handoff_matches,
            consume_managed_intermediate_writer_fence, remember_gateway_writer_proof,
            retain_managed_intermediate_writer_fence,
        )
        from gateway.runtime_service_custody import DurableResultProof
        if (
            type(fence) is not ManagedIntermediateWriterFence
            or fence.agent is not agent
            or fence.client is not self
            or (proof is not None and type(proof) is not SessionDBWriteProof)
        ):
            return False
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        service = getattr(parent, "_service", None)
        request = (getattr(service, "requests", {}) or {}).get(getattr(fence.binding, "admission_id", None))
        receipt = getattr(parent, "_physical", None)
        if (
            parent is not fence.parent
            or service is not fence.service
            or request is not fence.request
            or receipt is not fence.receipt
            or getattr(parent, "_owner_agent", None) is not agent
            or getattr(parent, "turn_binding", None) is not fence.binding
            or getattr(service, "physical", None) is not receipt
            or getattr(receipt, "service", None) is not service
            or getattr(receipt, "managed_client", None) is not self
            or getattr(service, "unknown", True) is not False
            or getattr(parent, "state", None) in {"unknown", "terminal"}
            or getattr(receipt, "state", None) in {"unknown", "closed", "released"}
            or (
                proof is not None
                and (
                    getattr(proof, "session_id", None) != getattr(agent, "session_id", None)
                    or getattr(proof, "store_path", None) != fence.writer_proof.store_path
                )
            )
        ):
            return False
        capture = getattr(self, "capture_managed_durable_receipt_witness", None)
        validator = getattr(self, "validate_managed_durable_receipt_witness", None)
        if not callable(capture) or not callable(validator):
            return False
        current_durable = self._managed_request_durable(receipt, fence.request)
        prefix_handoff = (
            getattr(receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, None) is fence
            and _managed_intermediate_prefix_handoff_matches(agent, self, fence)
        )
        recovered_terminal = proof is None
        recovered_prefix = (
            prefix_handoff
            and getattr(receipt, "managed_witness", None) is None
            and current_durable is getattr(fence.physical_witness, "durable_proof", None)
            and proof is not None
            and proof is fence.writer_proof
        )
        if proof is None:
            # A post-record retry may have no fresh intended messages.  Reuse only the
            # exact fence proof whose receipt-local durable witness is unchanged; never
            # treat a false/None flush as permission for a new DB write or child.
            if current_durable is not getattr(fence.physical_witness, "durable_proof", None):
                return False
            proof = fence.writer_proof
        recovered_terminal = recovered_terminal or recovered_prefix
        if type(proof) is not SessionDBWriteProof or not _gateway_proof_rows_match(
            agent, messages, proof, proof.session_id,
        ):
            return False
        result_rows = self._managed_result_rows(proof, messages)
        final_message = self._managed_final_message(messages)
        if (
            not result_rows
            or final_message is None
            or final_message.get("_row_id") not in result_rows
            or (
                recovered_terminal
                and getattr(current_durable, "result_row_ids", None) != tuple(result_rows)
            )
        ):
            return False
        if not self._managed_intermediate_prefix_matches(agent, fence, current_durable, capture):
            return False
        # The exact exited child must have completed native cleanup before the terminal proof
        # is eligible for the existing Gateway handoff.  This is the same receipt method used
        # by prepare_next_managed_child, with no replacement process or PID lookup.
        close_native = getattr(receipt, "close_native_after_process", None)
        if not callable(close_native) or close_native() is not True:
            return False
        if recovered_terminal:
            if not isinstance(current_durable, DurableResultProof):
                return False
            durable = current_durable
        else:
            durable = DurableResultProof(
                store_path=proof.store_path, session_id=proof.session_id,
                admission_id=fence.binding.admission_id,
                execution_generation=fence.binding.execution_generation,
                row_ids=proof.row_ids, row_snapshots=proof.row_snapshots,
                row_sessions=proof.row_sessions, row_roles=proof.row_roles,
                payload_digest=proof.payload_digest, result_row_ids=result_rows,
            )
        if not self._record_managed_intermediate_terminal(
            fence, service, request, receipt, current_durable, durable,
        ):
            return False
        prefix_only = (
            prefix_handoff
            and getattr(receipt, "managed_witness", None) is None
            and current_durable is getattr(fence.physical_witness, "durable_proof", None)
            and proof is fence.writer_proof
        )
        # Prefix-only recovery reuses the exact witness object and does not reattach
        # the previous provider child witness to the receipt.  A changed durable map
        # must be captured and validated as a strict same-receipt extension.
        next_witness = fence.physical_witness if prefix_only else capture(agent, proof)
        if next_witness is None:
            return False
        if prefix_only:
            if not _managed_intermediate_prefix_handoff_matches(agent, self, fence):
                return False
        elif validator(agent, proof, next_witness) is not True:
            return False
        if remember_gateway_writer_proof(
            agent, proof, fence.binding, True, next_witness,
        ) is not True:
            return False
        if fence.writer_proof is proof and current_durable is durable:
            # A previous attempt may have committed the exact terminal rows before its
            # readback failed.  Reuse that same immutable proof rather than writing again.
            return consume_managed_intermediate_writer_fence(
                agent, self, proof, next_witness, messages,
            ) is True
        if not retain_managed_intermediate_writer_fence(agent, self, proof, next_witness):
            return False
        return consume_managed_intermediate_writer_fence(
            agent, self, proof, next_witness, messages,
        ) is True

    def _managed_witness_index(self, session_id: str) -> int | None:
        parent = getattr(self._producer_policy, "parent", None)
        binding = getattr(parent, "turn_binding", None)
        fields = (
            "operation_id", "profile_id", "process_instance", "authority_epoch", "source_identity",
            "admission_id", "execution_generation", "hermes_session_id",
        )
        identity = (
            tuple(getattr(parent, field, None) for field in fields[:5])
            + (getattr(binding, "admission_id", None), getattr(binding, "execution_generation", None),
               getattr(binding, "session_id", None))
        )
        return next(
            (index for index, item in enumerate(self._pending_managed_witnesses)
             if isinstance(item.get("provider_session_id"), str)
             and not item.get("_failure_reason")
             and tuple(item.get(field) for field in fields) == identity
             and item.get("hermes_session_id") == session_id),
            None,
        )

    def _commit_managed_writer_proof(
        self, agent: Any, proof: Any, messages: list[dict[str, Any]], *, intermediate: bool = False,
    ) -> bool:
        """Bind one writer readback to the exact ACP child, then stage I04 durability."""
        if not self._pending_managed_witnesses:
            return False
        from agent.session_persistence_proof import SessionDBWriteProof
        from gateway.runtime_service_custody import DurableResultProof, ServiceCustodyError
        if not isinstance(proof, SessionDBWriteProof):
            raise ServiceCustodyError("durable_result_proof_missing")
        result_rows = self._managed_result_rows(proof, messages)
        last = (self._managed_intermediate_message(messages) if intermediate
                else self._managed_final_message(messages))
        last_row_id = None if last is None else last.get("_row_id")
        if not result_rows or type(last_row_id) is not int or last_row_id not in result_rows:
            return False
        witness_index = self._managed_witness_index(proof.session_id)
        if witness_index is None:
            raise ServiceCustodyError("managed_acp_witness_missing")
        witness = self._pending_managed_witnesses[witness_index]
        parent = witness.get("parent")
        receipt = witness.get("receipt")
        binding = getattr(parent, "turn_binding", None)
        if (
            binding is None
            or witness.get("admission_id") != binding.admission_id
            or witness.get("execution_generation") != binding.execution_generation
            or witness.get("hermes_session_id") != binding.session_id
        ):
            raise ServiceCustodyError("managed_acp_witness_stale")
        service = getattr(parent, "_service", None)
        request = (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
        if (getattr(parent, "mode", None) != "managed_turn" or binding is None or service is None
                or request is None or getattr(parent, "_physical", None) is not receipt
                or getattr(service, "physical", None) is not receipt
                or getattr(parent, "_owner_agent", agent) is not agent):
            raise ServiceCustodyError("managed_acp_owner_mismatch")
        from gateway.runtime_service_custody import TurnCustodyBinding
        if isinstance(binding, TurnCustodyBinding) or proof.session_id != binding.session_id:
            from gateway.session_turn_physical_tip import resolve_physical_tip_witness
            if resolve_physical_tip_witness(binding, proof.session_id, proof.store_path) is None:
                raise ServiceCustodyError("managed_acp_physical_tip_unknown")
        durable = DurableResultProof(
            store_path=proof.store_path, session_id=proof.session_id,
            admission_id=binding.admission_id, execution_generation=binding.execution_generation,
            row_ids=proof.row_ids, row_snapshots=proof.row_snapshots,
            row_sessions=proof.row_sessions, row_roles=proof.row_roles,
            payload_digest=proof.payload_digest, result_row_ids=result_rows,
        )
        try:
            service.record_result_durable(binding, request, durable)
            binding.authority._preservation_release_sidecar(binding.admission_id, request, durable=True)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "managed_durable_handoff_failed")
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                logger.exception("managed ACP custody retention failed")
            raise ServiceCustodyError("managed_durable_handoff_failed") from exc
        receipt.managed_witness = {
            **(receipt.managed_witness if isinstance(getattr(receipt, "managed_witness", None), dict) else witness),
            "durable_proof": durable,
        }
        del self._pending_managed_witnesses[witness_index]
        process = witness.get("_process")
        if process is not None:
            self._forget_process_record(process)
        self._refresh_closed_state()
        return True

    def close(self) -> None:
        with self._active_process_lock:
            procs = tuple(self._active_processes)
        for proc in procs:
            self._release_process(proc)
        if self.has_pending_managed_custody():
            self.is_closed = False
            return
        self.is_closed = True
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        if parent is not None:
            if (
                getattr(parent, "mode", None) == "managed_turn"
                and getattr(parent, "_catalog_client", None) is not self
            ):
                return
            from gateway.runtime_service_operations import close_after_finalizers
            if close_after_finalizers(parent):
                owner = getattr(parent, "_owner_agent", None)
                if owner is not None:
                    from gateway.runtime_service_operations import release_agent_process_parent
                    release_agent_process_parent(owner)

    def _create_chat_completion(
        self, *, model: str | None = None, messages: list[dict[str, Any]] | None = None, timeout: float | None = None,
        tools: list[dict[str, Any]] | None = None, tool_choice: Any = None, stream: bool = False, **_: Any,
    ) -> Any:
        prompt_text = _format_messages_as_prompt(messages or [], model=model, tools=tools, tool_choice=tool_choice)
        response_text, reasoning = self._run_prompt(prompt_text, timeout_seconds=_effective_timeout(timeout), model=model)
        tool_calls, cleaned_text = _extract_tool_calls_from_text(response_text)
        message = SimpleNamespace(
            content=cleaned_text, tool_calls=tool_calls, reasoning=reasoning or None, reasoning_content=reasoning or None,
            reasoning_details=None,
        )
        completion = SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="tool_calls" if tool_calls else "stop")],
            usage=SimpleNamespace(prompt_tokens=0, completion_tokens=0, total_tokens=0, prompt_tokens_details=SimpleNamespace(cached_tokens=0)),
            model=model or "copilot-acp",
        )
        return _completion_to_stream_chunks(completion) if stream else completion

    def _spawn(self) -> subprocess.Popen[str]:
        receipt = None
        parent = getattr(getattr(self, "_producer_policy", None), "parent", None)
        managed_binding = None
        managed_service = None
        managed_request = None
        binding_snapshot = None
        proc = None
        transaction = None
        # Fast-fail when the CLI rejects --acp (else the parent waits the full child timeout for stdout that
        # never arrives). ``None`` falls through to the spawn's established start error.
        if _acp_supported(self._acp_command, self._acp_args) is False:
            preview = " ".join(self._acp_args[:3]) if self._acp_args else "(none)"
            raise RuntimeError(
                f"ACP transport not supported by '{self._acp_command}': `{preview}` is rejected as an unknown option. This "
                "usually means the CLI is an older release (e.g. Claude Code v2.x) or a different tool than expected. Either "
                "install a CLI that ships with --acp support (e.g. `@github/copilot` late 2025+), or set "
                "HERMES_COPILOT_ACP_COMMAND / HERMES_COPILOT_ACP_ARGS to a working pair."
            )
        if getattr(parent, "mode", None) == "managed_turn":
            from gateway.runtime_service_custody import ServiceCustodyError
            managed_binding = getattr(parent, "turn_binding", None)
            managed_service = getattr(parent, "_service", None)
            managed_request = (getattr(managed_service, "requests", {}) or {}).get(
                getattr(managed_binding, "admission_id", None),
            )
            if managed_binding is None or managed_service is None or managed_request is None:
                raise ServiceCustodyError("managed_binding_missing")
            bound_request = getattr(parent, "_request", None)
            if bound_request is not None and bound_request is not managed_request:
                raise ServiceCustodyError("managed_request_mismatch")
            # The service request map is authoritative for parent views created by the
            # existing managed adapter; bind that exact object before any effect.
            parent._request = managed_request
            if (
                getattr(managed_service, "unknown", False)
                or getattr(parent, "state", None) == "unknown"
                or getattr(getattr(managed_service, "physical", None), "state", None) == "unknown"
            ) and not self._retry_managed_process_failure(
                parent, managed_service, managed_binding, managed_request,
            ):
                raise ServiceCustodyError("managed_process_unknown")
            if (getattr(parent, 'state', None) != 'request_reserved'
                    and not self.has_pending_managed_witness()):
                # An intermediate response proved this child's rows, not the whole turn.
                # Advance only through the same service's EOF/join/native-close fence;
                # it retains the physical handle and never adopts a replacement owner.
                if not managed_service.prepare_next_managed_child(managed_binding, managed_request):
                    raise ServiceCustodyError('physical_custody_pending')
            if getattr(parent, "state", None) != "request_reserved":
                raise ServiceCustodyError("managed_request_not_reserved")
            if getattr(parent, "_request", None) is not managed_request:
                raise ServiceCustodyError("managed_request_mismatch")
            if getattr(parent, "turn_binding", None) is not managed_binding:
                raise ServiceCustodyError("managed_binding_mismatch")
            managed_service.validate_spawn_binding(managed_binding, managed_request)
            binding_snapshot = {
                "operation_id": parent.operation_id, "profile_id": parent.profile_id,
                "process_instance": parent.process_instance, "authority_epoch": parent.authority_epoch,
                "source_identity": parent.source_identity, "admission_id": managed_binding.admission_id,
                "execution_generation": managed_binding.execution_generation, "hermes_session_id": managed_binding.session_id,
            }
        if self._producer_policy is not None:
            from gateway.runtime_service_operations import reserve_physical
            receipt = reserve_physical(self._producer_policy.parent)
            receipt.ephemeral_process = True
        try:
            from hermes_cli._subprocess_compat import windows_hide_flags  # hide the Windows console flash (#56747); pipes intact for the ACP wire

            # Hide the console the CLI child would otherwise flash on Windows (#56747). Hide-only — stdio
            # pipes stay intact for the ACP wire.
            if self._producer_policy is not None and sys.platform == "win32":
                from tools.process_registry_windows import begin_windows_launch
                from gateway.runtime_service_operations import bind_windows_launch
                transaction = begin_windows_launch(receipt)
                if receipt is None or receipt.retain_launch_transaction(transaction) is not True:
                    raise RuntimeError("ACP launch transaction could not be retained")
                proc = transaction.launch(
                    [self._acp_command] + self._acp_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace', bufsize=1,
                    cwd=self._acp_cwd, env=_build_subprocess_env(), creationflags=windows_hide_flags(),
                )
                if proc is False or proc is None:
                    raise RuntimeError("ACP launch did not return a process")
                self._register_spawned_process(proc, receipt, binding_snapshot)
                bind_windows_launch(self._producer_policy.parent, transaction, proc)
            else:
                proc = subprocess.Popen(
                    [self._acp_command] + self._acp_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, encoding='utf-8', errors='replace', bufsize=1, cwd=self._acp_cwd, env=_build_subprocess_env(),
                    creationflags=windows_hide_flags(),
                )
                self._register_spawned_process(proc, receipt, binding_snapshot)
                if self._producer_policy is not None:
                    from gateway.runtime_service_operations import bind_physical
                    bind_physical(
                        self._producer_policy.parent, None, proc, pid=proc.pid, process_started_at=None,
                        scope="posix_direct", scope_identity="posix_direct",
                    )
        except FileNotFoundError as exc:
            self._release_spawn_failure(
                proc, receipt=receipt, transaction=transaction, parent=parent,
                binding_snapshot=binding_snapshot, reason="spawn_not_started",
                no_effect_verified=True, original_error=exc,
            )
            raise RuntimeError(f"Could not start Copilot ACP command '{self._acp_command}'. Install GitHub Copilot CLI or set "
                               "HERMES_COPILOT_ACP_COMMAND/COPILOT_CLI_PATH.") from exc
        except BaseException as exc:
            process = proc if proc is not None else getattr(transaction, "process", None)
            self._release_spawn_failure(
                process, receipt=receipt, transaction=transaction, parent=parent,
                binding_snapshot=binding_snapshot, reason="spawn_bind_failed",
                original_error=exc,
            )
            raise
        if proc.stdin is None or proc.stdout is None:
            if receipt is not None:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "pipe_missing")
            self._release_process(proc, success=False, failure_reason="pipe_missing")
            raise RuntimeError("Copilot ACP process did not expose stdin/stdout pipes.")
        return proc

    @contextlib.contextmanager
    def _session(
        self, timeout_seconds: float, *, allow_file_requests: bool = True
    ) -> Iterator[tuple[dict[str, Any], Callable[..., Any], Any]]:
        """Start one ACP process and yield its session, request callable, and exact child."""
        proc = self._spawn()
        inbox: queue.Queue[dict[str, Any]] = queue.Queue()
        stderr_tail: deque[str] = deque(maxlen=40)

        def _decode(line: str) -> dict[str, Any]:
            try:
                return json.loads(line)
            except Exception:
                return {"raw": line.rstrip("\n")}

        request_ids = iter(range(1, 1 << 62))
        # One budget for the WHOLE session (initialize + session/new + any prompt), not per
        # request: a hung CLI must not get 2x the caller's timeout on the foreground /model path.
        session_deadline = time.monotonic() + timeout_seconds

        def _request(method: str, params: dict[str, Any], *, text_parts: list[str] | None = None,
                     reasoning_parts: list[str] | None = None) -> Any:
            request_id = next(request_ids)
            proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}) + "\n")
            proc.stdin.flush()
            deadline = session_deadline
            while time.monotonic() < deadline and proc.poll() is None:
                try:
                    msg = inbox.get(timeout=0.1)
                except queue.Empty:
                    continue
                if self._handle_server_message(
                    msg, process=proc, cwd=self._acp_cwd, text_parts=text_parts,
                    reasoning_parts=reasoning_parts, allow_file_requests=allow_file_requests,
                ) or msg.get("id") != request_id:
                    continue
                if "error" in msg:
                    err = msg.get("error") or {}
                    raise RuntimeError(f"Copilot ACP {method} failed: {err.get('message') or err}")
                return msg.get("result")
            if proc.poll() is not None:
                # The pump can still hold the crash text when poll() first sees the exit; reading
                # it too early turned a dead CLI into a TimeoutError, which retries differently.
                stderr_pump = getattr(self, "_process_readers", {}).get(proc, (None, None))[1]
                if stderr_pump is not None:
                    stderr_pump.join(timeout=1.0)
                stderr_text = "\n".join(stderr_tail).strip()
                if _is_gh_copilot_deprecation_message(stderr_text):
                    raise RuntimeError(_DEPRECATED_CLI_ERROR + stderr_text)
                raise RuntimeError(f"Copilot ACP process exited early: {stderr_text or f'exit code {proc.returncode}'}")
            raise TimeoutError(f"Timed out waiting for Copilot ACP response to {method}.")

        settled = False
        pump_failed = False
        session: dict[str, Any] = {}
        try:
            try:
                _start_acp_pumps(self, proc, inbox, stderr_tail, _decode)
            except BaseException:
                pump_failed = True
                raise
            _request("initialize", _INITIALIZE_PARAMS)
            session = _request("session/new", {"cwd": self._acp_cwd, "mcpServers": []}) or {}
            if not str(session.get("sessionId") or "").strip():
                raise RuntimeError("Copilot ACP did not return a sessionId.")
            yield session, _request, proc
            settled = True
        finally:
            self._release_process(
                proc, success=settled, proof=self._process_proofs.get(proc),
                failure_reason="reader_setup_failed" if pump_failed else None,
                original_error=sys.exc_info()[1],
            )

    def list_models(self, *, timeout_seconds: float = 15.0) -> list[str]:
        """Return the enabled models advertised by a short-lived authenticated ACP session."""
        with self._session(timeout_seconds, allow_file_requests=False) as (session, _, _proc):
            return _session_model_ids(session)

    def finalize_external_catalog(
        self, *, result_identity: str, cache_identity: str, payload_digest: str,
    ) -> None:
        """Close a discovery child only after the caller's actual cache readback proof."""
        policy = self._producer_policy
        parent = getattr(policy, "parent", None)
        receipt = getattr(parent, "_physical", None)
        if parent is None or receipt is None:
            raise RuntimeError("external catalog custody is not pending")
        from gateway.runtime_service_operations import (
            ExternalPublicationProof, publish_catalog,
        )
        proof = ExternalPublicationProof(
            operation_id=parent.operation_id, profile_id=parent.profile_id,
            process_instance=parent.process_instance, authority_epoch=parent.authority_epoch,
            source_identity=parent.source_identity, result_identity=result_identity,
            cache_readback=True, cache_identity=cache_identity, payload_digest=payload_digest,
        )
        publish_catalog(parent, proof)
        if receipt.close_after_delivery() is not True:
            # Keep the exact external parent/finalizer link when the physical close is a
            # retryable unknown.  The existing owner finalizer will call close_after_delivery
            # again on this same receipt before it can release the parent root.
            receipt.mark_unknown("physical_close_failed")
            if not getattr(receipt, "can_retry_external_close", lambda: False)():
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "physical_close_failed")
            raise RuntimeError("external catalog custody did not settle")
        parent._physical = None
        parent.state = "request_reserved"

    def _run_prompt(self, prompt_text: str, *, timeout_seconds: float, model: str | None = None) -> tuple[str, str]:
        # The CLI's `--model` spawn flag is deliberately NOT used: `copilot --acp` validates it (unknown id
        # aborts the spawn) but ignores it for the session; the model is applied after session/new instead.
        requested_model = str(model or "").strip()
        with self._session(timeout_seconds) as (session, _request, proc):
            session_id = str(session.get("sessionId") or "").strip()
            if requested_model and requested_model != "copilot-acp":
                try:
                    if (selection := _model_selection_request(session, requested_model)) is not None:
                        _request(*selection)
                    else:
                        logger.warning("Copilot ACP does not offer model %r; using the session default.", requested_model)
                except Exception as exc:
                    logger.warning("Copilot ACP model selection for %r failed; continuing with the session default: %s", requested_model, exc)
            text_parts: list[str] = []
            reasoning_parts: list[str] = []
            prompt = {"sessionId": session_id, "prompt": [{"type": "text", "text": prompt_text}]}
            prompt_result = _request("session/prompt", prompt, text_parts=text_parts, reasoning_parts=reasoning_parts)
            text = "".join(text_parts)
            self._record_prompt_proof(proc, session_id, prompt_result, text)
            return text, "".join(reasoning_parts)

    def _handle_server_message(
        self, msg: dict[str, Any], *, process: subprocess.Popen[str], cwd: str, text_parts: list[str] | None, reasoning_parts: list[str] | None,
        allow_file_requests: bool = True,
    ) -> bool:
        """Consume a server->client message; True when handled (notification or request answered)."""
        method = msg.get("method")
        if not isinstance(method, str):
            return False
        if method == "session/update":
            update = (msg.get("params") or {}).get("update") or {}
            content = update.get("content") or {}
            chunk_text = str(content.get("text") or "") if isinstance(content, dict) else ""
            sinks = {"agent_message_chunk": text_parts, "agent_thought_chunk": reasoning_parts}
            if chunk_text and (sink := sinks.get(str(update.get("sessionUpdate") or "").strip())) is not None:
                sink.append(chunk_text)
            return True
        if process.stdin is None:
            return True
        message_id = msg.get("id")
        if method == "session/request_permission":
            response = _jsonrpc_result(message_id, {"outcome": {"outcome": "cancelled"}})
        elif method in _FS_HANDLERS:
            if not allow_file_requests:
                response = _jsonrpc_error(message_id, -32601, "File access is unavailable during model discovery.")
            else:
                try:
                    response = _jsonrpc_result(message_id, _FS_HANDLERS[method](msg.get("params") or {}, cwd))
                except Exception as exc:
                    response = _jsonrpc_error(message_id, -32602, str(exc))
        else:
            response = _jsonrpc_error(message_id, -32601, f"ACP client method '{method}' is not supported by Hermes yet.")
        process.stdin.write(json.dumps(response) + "\n")
        process.stdin.flush()
        return True
