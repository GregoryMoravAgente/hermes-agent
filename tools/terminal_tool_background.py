"""Background-process launch path: ``terminal(background=true)`` spawns a tracked
process via the process registry (Popen locally, ``env.execute`` in a sandbox),
stamps gateway routing metadata for completion / watch-pattern notifications and
returns the JSON result. Lazy ``tools.terminal_tool`` imports keep the origin's
monkeypatch points authoritative.
"""

import json
import logging
import sys
import time
from typing import Any, List, Optional

logger = logging.getLogger("tools.terminal_tool")

# A silent background process (no notify_on_complete / watch_patterns) is right
# only for servers/watchers; for bounded tasks the agent almost always wanted a
# notification and forgot the flag, so nudge it (cheap false positive).
_SILENT_BACKGROUND_HINT = (
    'background=true without notify_on_complete=true means this process runs SILENTLY — you '
    'will not be told when it exits. If this is a bounded task (test suite, build, CI poller, '
    'deploy, anything with a defined end), you almost certainly wanted notify_on_complete=true '
    'so the system pings you on exit. Re-launch with notify_on_complete=true, or call '
    "process(action='poll') / process(action='wait') yourself to learn the outcome. Only "
    'ignore this hint for genuine long-lived processes that never exit (servers, watchers, '
    'daemons).'
)

# Homebrewed CI pollers built on `gh pr view --json statusCheckRollup` or
# `gh pr checks | jq` fail silently in known ways (block-buffered stdout never
# reaches capture, jq null-key edge cases exit the loop, conclusion-vs-status
# confusion declares all-green early, TTY-only banners never appear piped).
# Detector is deliberately narrow: the canonical column-2 awk poller is fine.
_HOMEBREW_CI_POLLER_HINT = (
    'This looks like a homebrewed CI poller built from `gh pr view --json statusCheckRollup` '
    'and/or `gh pr checks | jq`. That shape has burned us repeatedly in hermes-agent dev work '
    '(PRs #31329, #31448, #31695, #31709, #31745, #32264, #33131) — stdout buffering kills '
    'output capture, jq null-key edge cases silently exit the loop, conclusion-vs-status field '
    'confusion exits early with bogus all-green verdicts, TTY-only summary banners never '
    'appear when piped. Use the canonical snippets in the green-ci-policy skill instead: the '
    'exit-code-driven `gh pr checks $PR >/dev/null` (rc 0 = green, 8 = pending, else fail) for '
    'exit-on-first-fail behavior, or the column-2 awk-on-tabs poller (`awk -F"\\t" '
    '"$2==\\"pending\\""`) for sharded matrices. Load '
    "skill_view(name='github/hermes-agent-dev', file_path='references/green-ci-policy.md') for "
    'the verbatim snippets. If you must roll a custom loop with rich structured output, write '
    "each tick to a known file (`tee -a $TMPDIR/ci.log`) and rely on `process(action='log')` to "
    'read THAT file — do not rely on background-process stdout capture for line-buffered shell '
    'loops.'
)

# A foreground call whose only backgrounding was a trailing `&` is spawned as background=true.
_AUTO_BACKGROUND_NOTE = (
    "Ran in the background (the trailing & was converted to background=true; Hermes tracks the "
    "process). Do NOT re-run it. Use process_manage (poll/log/wait/kill, session_id above) to check it."
)

_ASYNC_UNSUPPORTED_NOTE = (
    'notify_on_complete / watch_patterns are not available in this session — it cannot receive '
    'an async completion after the turn ends (a one-shot runner such as `hermes -z`, a cron '
    'job, a Kanban worker, or a stateless HTTP endpoint). The process is running in the '
    "background; retrieve its result with process(action='poll') or process(action='wait')."
)

# proc_session attribute -> HERMES_SESSION_* env var carrying it.
_ROUTING_FIELDS = (
    ("watcher_chat_id", "HERMES_SESSION_CHAT_ID"),
    ("watcher_user_id", "HERMES_SESSION_USER_ID"),
    ("watcher_user_name", "HERMES_SESSION_USER_NAME"),
    ("watcher_thread_id", "HERMES_SESSION_THREAD_ID"),
    ("watcher_message_id", "HERMES_SESSION_MESSAGE_ID"),
    # The spawning conversation's session-db id lets the gateway's completion
    # pre-flight drop the notification if the user closed this session (/new)
    # before the process finished, instead of injecting it into the NEW one.
    ("parent_session_id", "HERMES_SESSION_ID"),
)


def _looks_like_homebrew_ci_poller(command: str) -> bool:
    has_gh = "gh pr view" in command or "gh pr checks" in command
    has_jq = " jq " in command or "| jq" in command or "$(jq" in command
    # `gh pr checks` doesn't emit JSON, so piping it to jq is confused intent.
    return "statusCheckRollup" in command or (has_gh and has_jq)


def _stamp_gateway_routing(proc_session, get_session_env) -> None:
    """Copy the spawning chat's routing metadata onto the process session so
    completion / watch notifications reach the right chat/thread."""
    platform = get_session_env("HERMES_SESSION_PLATFORM", "")
    if not platform:
        return
    proc_session.watcher_platform = platform
    for attr, var in _ROUTING_FIELDS:
        setattr(proc_session, attr, get_session_env(var, ""))


def _spawn(process_registry, *, env, env_type, command, cwd, effective_task_id, task_id,
           session_key, effective_pty, persist_on_release: bool = False,
           notify_on_complete: bool = False):
    from gateway.session_context import get_i03_custody_binding

    binding = get_i03_custody_binding()
    parent_handle = None
    scope_fence = None
    owner_task_id = task_id or effective_task_id
    if binding is not None:
        if effective_task_id != getattr(binding, "process_task_id", None):
            raise RuntimeError("I03 terminal task identity mismatch")
        parent_handle = getattr(binding, "handle", None)
        scope_fence = getattr(binding, "process_scope", None)
        if parent_handle is None or scope_fence is None:
            raise RuntimeError("I03 terminal custody binding is incomplete")
        owner_task_id = (
            getattr(binding, "process_owner_task_id", None)
            or binding.process_task_id
        )
    common = dict(command=command, cwd=cwd, task_id=effective_task_id,
                  owner_task_id=owner_task_id, session_key=session_key,
                  persist_on_release=persist_on_release, parent_handle=parent_handle,
                  scope_fence=scope_fence, notify_on_complete=notify_on_complete)
    if env_type == "local":
        return process_registry.spawn_local(
            env_vars=env.env if hasattr(env, 'env') else None, use_pty=effective_pty, **common)
    return process_registry.spawn_via_env(env=env, **common)


def _async_delivery_state(notify_on_complete, watch_patterns):
    """Read async capability before launch so a fast child cannot finish un-notified."""
    if not (notify_on_complete or watch_patterns):
        return None
    from gateway.session_context import async_delivery_supported
    return bool(async_delivery_supported())


def _apply_async_support(
    proc_session, result_data, notify_on_complete, watch_patterns, *, async_supported=None,
):
    """Finite sessions (stateless HTTP, one-shot Kanban workers) can't route a
    completion back after the turn ends: drop the flags and tell the agent to
    poll. Otherwise stamp gateway routing. Returns (notify, watch_patterns)."""
    if not (notify_on_complete or watch_patterns):
        return notify_on_complete, watch_patterns
    from gateway.session_context import get_session_env

    if async_supported is None:
        async_supported = _async_delivery_state(notify_on_complete, watch_patterns)
    if async_supported:
        _stamp_gateway_routing(proc_session, get_session_env)
        return notify_on_complete, watch_patterns
    result_data["notify_on_complete"] = False
    result_data["notify_unsupported"] = _ASYNC_UNSUPPORTED_NOTE
    logger.info("background proc %s: async delivery unsupported on this "
                "session; notify_on_complete/watch_patterns disabled", proc_session.id)
    return False, None


def _register_completion_watcher(process_registry, proc_session, session_key) -> None:
    """Gateway mode: register a fast watcher so completion triggers a new
    agent turn (CLI mode uses the completion_queue directly).

    Armed on the live gateway loop right away: the post-turn drain alone leaves a
    process that finishes while its launching turn is still running unwatched, and
    the chat mute for as long as that turn lasts (#112033). Before the gateway
    serves, or while it stops, the descriptor waits in ``pending_watchers`` for the
    startup / post-turn drain instead."""
    proc_session.watcher_interval = 5
    watcher = {
        "session_id": proc_session.id, "check_interval": 5, "session_key": session_key,
        "platform": proc_session.watcher_platform,
        **{attr.removeprefix("watcher_"): getattr(proc_session, attr)
           for attr, _ in _ROUTING_FIELDS[:-1]},
        "notify_on_complete": True, "parent_session_id": proc_session.parent_session_id,
    }
    runner_ref = getattr(sys.modules.get("gateway.run"), "_gateway_runner_ref", None)
    runner = runner_ref() if callable(runner_ref) else None
    if runner is not None and runner.arm_process_watcher(watcher):
        process_registry.mark_completion_watcher_registered(proc_session.id)
        return
    process_registry.pending_watchers.append(watcher)
    process_registry.mark_completion_watcher_registered(proc_session.id)


def _managed_reader_complete(evidence: Any) -> bool:
    """Require the original native reader to finish before custody can be staged."""
    if not isinstance(evidence, dict):
        return False
    reader_state = evidence.get("reader_state")
    reader_thread = evidence.get("reader_thread")
    is_alive = getattr(reader_thread, "is_alive", None)
    return (
        isinstance(reader_state, dict)
        and reader_state.get("done") is True
        and callable(is_alive)
        and not is_alive()
        and evidence.get("deadline_expired") is not True
        and evidence.get("status") == "true_eof"
        and evidence.get("eof_kind") == "win32_broken_pipe"
    )


def spawn_background_process(
    *, command: str, env: Any, env_type: str, effective_task_id: str, task_id: Optional[str],
    session_key: str, workdir: Optional[str], cwd: str, effective_pty: bool,
    notify_on_complete: bool, watch_patterns: Optional[list[str]], approval_note: Optional[str],
    completion_output_chars: int = 0,
    pty_disabled_reason: Optional[str],
    heartbeat_seconds: int = 0,
    persist_on_release: bool = False,
    mounted_host: Optional[str] = None,
    auto_backgrounded: bool = False,
) -> str:
    """Spawn *command* as a tracked background process and return the JSON result.

    ``auto_backgrounded`` marks a trailing-`&` conversion: the result carries the conversion note
    and skips the silent-background hint, whose "re-launch" advice would start a second copy.

    Never inline-polls ``is_interrupted()``: the spawn detaches and returns
    exit_code 0 immediately, so the stale-interrupt kill cannot occur here.
    """
    from tools.process_registry import process_registry
    from tools.terminal_tool import (
        _redact_terminal_error_text, _resolve_command_cwd, _resolve_notification_flag_conflict,
    )

    effective_cwd = _resolve_command_cwd(
        workdir=workdir, default_cwd=cwd, session_key=session_key, env_type=env_type,
        mounted_host=mounted_host if mounted_host is not None else getattr(env, "host_cwd", None),
        env=env,
    )
    # The session flag must be present before Popen/env.execute.  A child that exits in the
    # launch call otherwise reaches _move_to_finished before the caller can arm notification,
    # and the durable completion queue has no second chance to publish it.
    try:
        async_supported = _async_delivery_state(notify_on_complete, watch_patterns)
        launch_notify_on_complete = bool(notify_on_complete and async_supported is not False)
        proc_session = _spawn(
            process_registry, env=env, env_type=env_type, command=command, cwd=effective_cwd,
            effective_task_id=effective_task_id, task_id=task_id, session_key=session_key,
            effective_pty=effective_pty, persist_on_release=persist_on_release,
            notify_on_complete=launch_notify_on_complete,
        )
        result_data = {"output": "Background process started", "session_id": proc_session.id,
                       "pid": proc_session.pid, "exit_code": 0, "error": None}
        if persist_on_release:
            result_data["persist_on_release"] = True
        if approval_note:
            result_data["approval"] = approval_note
        if pty_disabled_reason:
            result_data["pty_note"] = pty_disabled_reason
        if auto_backgrounded:
            result_data["auto_backgrounded"] = _AUTO_BACKGROUND_NOTE
        elif not notify_on_complete and not watch_patterns:
            result_data["hint"] = _SILENT_BACKGROUND_HINT
        if command and _looks_like_homebrew_ci_poller(command):
            existing = result_data.get("hint", "")
            result_data["hint"] = (existing + "\n\n" + _HOMEBREW_CI_POLLER_HINT if existing
                                   else _HOMEBREW_CI_POLLER_HINT)

        notify_on_complete, watch_patterns = _apply_async_support(
            proc_session, result_data, notify_on_complete, watch_patterns,
            async_supported=async_supported,
        )
        watch_patterns, conflict_note = _resolve_notification_flag_conflict(
            notify_on_complete=bool(notify_on_complete), watch_patterns=watch_patterns, background=True,
        )
        if conflict_note:
            logger.warning("background proc %s: %s", proc_session.id, conflict_note)
            result_data["watch_patterns_ignored"] = conflict_note
        if notify_on_complete:
            proc_session.notify_on_complete = True
            result_data["notify_on_complete"] = True
            if completion_output_chars:
                proc_session.completion_output_chars = int(completion_output_chars)
            if proc_session.watcher_platform:
                _register_completion_watcher(process_registry, proc_session, session_key)
            from agent.delegation_context import is_delegated_child_context
            if is_delegated_child_context():
                result_data["notify_on_complete"] = False
                result_data["subagent_note"] = _SUBAGENT_NOTIFY_NOTE
            elif heartbeat_seconds:
                # Heartbeats ride the same delivery path as the completion notice, so they are
                # only armed where that notice can actually reach the agent.
                result_data["heartbeat_seconds"] = process_registry.arm_heartbeat(proc_session, heartbeat_seconds)
        elif heartbeat_seconds:
            result_data["heartbeat_ignored"] = "heartbeat needs notify=true delivery, which this session cannot receive"
        if watch_patterns:
            proc_session.watch_patterns = list(watch_patterns)
            result_data["watch_patterns"] = proc_session.watch_patterns
        return json.dumps(result_data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({
            "output": "", "exit_code": -1,
            "error": _redact_terminal_error_text(f"Failed to start background process: {e}"),
        }, ensure_ascii=False)


_SUBAGENT_NOTIFY_NOTE = (
    "You are a subagent: this process's completion notice will NOT reach your parent, and the process is killed when "
    "you finish. Before you finish, either wait for it (process_manage wait), kill it, or hand it to your parent with "
    "process_manage(action='handoff', session_id=..., data='<purpose>') so the parent receives its completion. For CI "
    "watchers prefer returning the fact (PR number, SHA) and letting the parent watch."
)

_YIELDED_NOTE = (
    "The user sent a message while this command was running, so it was moved to the "
    "background WITHOUT being killed and is still running. You will be notified when it "
    "exits (notify_on_complete). Read the user's message and respond to it now; use "
    "process(action='poll'|'wait'|'log', session_id=...) to check on this command."
)


def yield_to_background_handler(
    *, command: str, env_type: str, cwd: Optional[str], effective_task_id: str,
    task_id: Optional[str], session_key: str, session_id: Optional[str] = None,
    tool_call_id: Optional[str] = None,
    turn_id: Optional[str] = None, run_generation: Optional[int] = None,
):
    """Build the ``yield_handler`` a foreground ``env.execute`` calls when the tool thread is
    asked to yield (a user message arrived mid-command). Local backend only: an ordinary
    reader is adopted by the process registry; a managed Windows reader is transferred with
    its existing Popen/Job/reader/collector objects and the partial output is returned to the
    model right away. Other backends return None (no adoptable host process) and the foreground
    wait continues."""
    if env_type != "local":
        return None

    custody = None
    launch_transaction = None
    prepare_failure = False
    foreground_pending = False
    foreground_evidence = None
    foreground_binding = None
    i03_binding = None
    i03_owner_task_id = None
    i03_parent_handle = None
    i03_scope_fence = None
    sidecar_owner = None
    from gateway.runtime_service_custody import current_turn_binding
    managed_turn_binding = current_turn_binding()
    from gateway.session_context import get_i03_custody_binding
    i03_binding = get_i03_custody_binding()
    if i03_binding is not None:
        if effective_task_id != getattr(i03_binding, "process_task_id", None):
            raise RuntimeError("I03 foreground task identity mismatch")
        i03_parent_handle = getattr(i03_binding, "handle", None)
        i03_scope_fence = getattr(i03_binding, "process_scope", None)
        if i03_parent_handle is None or i03_scope_fence is None:
            raise RuntimeError("I03 foreground custody binding is incomplete")
        i03_owner_task_id = (
            getattr(i03_binding, "process_owner_task_id", None)
            or i03_binding.process_task_id
        )
    if (
        isinstance(session_key, str) and session_key
        and isinstance(tool_call_id, str) and tool_call_id
        and isinstance(turn_id, str) and turn_id
        and type(run_generation) is int and run_generation >= 0
    ):
        from tools.process_registry_custody import ForegroundParentBinding
        foreground_binding = ForegroundParentBinding(
            session_id=session_id or session_key,
            session_key=session_key,
            tool_call_id=tool_call_id,
            turn_id=turn_id,
            run_generation=run_generation,
        )

    def _prepare_custody() -> None:
        nonlocal custody, launch_transaction, prepare_failure, sidecar_owner
        if custody is not None:
            return
        from tools.process_registry import process_registry
        # Called by BaseEnvironment immediately before the real local Popen. A bound runtime
        # coordinator that is closing/refusing must stop here; falling through to an ordinary
        # Popen would create a live child with no authenticated owner. Non-Windows keeps the
        # existing host path and remains explicitly unknown until a proven scope exists.
        try:
            from tools.environments.local_custody import prepare_managed_local_sidecar
            sidecar_owner = prepare_managed_local_sidecar(
                source_identity=f"terminal:{effective_task_id}",
                binding=managed_turn_binding,
            )
            process_parent = sidecar_owner.handle if sidecar_owner is not None else i03_parent_handle
            process_profile = (
                sidecar_owner.parent.profile_id if sidecar_owner is not None else None
            )
            if foreground_binding is None:
                custody = process_registry.reserve_foreground_custody(
                    profile_id=process_profile, parent_handle=process_parent, scope_fence=i03_scope_fence,
                    owner_task_id=i03_owner_task_id or effective_task_id,
                )
            else:
                custody = process_registry.reserve_foreground_custody(
                    binding=foreground_binding, profile_id=process_profile, parent_handle=process_parent,
                    scope_fence=i03_scope_fence,
                    owner_task_id=i03_owner_task_id or effective_task_id,
                )
            if custody is not None and getattr(custody, "state", "accepted") != "accepted":
                raise RuntimeError("foreground custody reservation is not accepted")
            if custody is not None and sys.platform == "win32":
                from tools.process_registry_windows import begin_windows_launch
                launch_transaction = begin_windows_launch(custody)
                custody.native_scope = launch_transaction
                # Retain the exact transaction before any Popen/Job side effect.  A later
                # ambiguous release must be recoverable by the same-generation owner.
                if foreground_binding is None:
                    process_registry.retain_foreground_custody(launch_transaction)
                else:
                    process_registry.retain_foreground_custody(
                        launch_transaction, binding=foreground_binding,
                    )
                # BaseEnvironment reads this after the reservation hook returns. Keep the exact
                # transaction object visible so the native launcher can bind the same suspended
                # Popen before it resumes; do not reconstruct identity from PID.
                _handler._launch_transaction = launch_transaction
                _handler._retain_reader_until_eof = True
        except BaseException:
            prepare_failure = True
            if sidecar_owner is not None:
                from tools.environments.local_custody import finish_local_process
                finish_local_process(sidecar_owner, reason="local_reservation_failed")
            raise

    def _release_custody() -> None:
        nonlocal custody, launch_transaction, sidecar_owner
        def _finish_sidecar() -> None:
            nonlocal sidecar_owner
            if sidecar_owner is None:
                return
            # Managed API turns hand the exact sidecar receipt to the durable-result
            # finalizer.  On POSIX there is no I15 native transaction to retain, so the
            # base environment reaches this teardown hook before the I04 writer proof;
            # marking the sidecar UNKNOWN here poisons that authenticated finalizer.
            if (
                foreground_binding is not None
                and getattr(sidecar_owner, "process", None) is not None
                and getattr(getattr(sidecar_owner, "physical", None), "state", None)
                not in {"unknown", "closed", "released"}
                and getattr(sidecar_owner, "reader", None) is not None
            ):
                return
            if sidecar_owner.process is None and not sidecar_owner.effect_observed:
                from gateway.runtime_sidecar_sources import release_before_effect
                if release_before_effect(sidecar_owner):
                    sidecar_owner = None
                    return
            from tools.environments.local_custody import finish_local_process
            finish_local_process(sidecar_owner)
        if custody is None:
            _finish_sidecar()
            return
        from tools.process_registry import process_registry
        if launch_transaction is not None:
            if launch_transaction.has_process_effect:
                custody.mark_unknown("partial_launch")
                process_registry.retain_foreground_custody(launch_transaction)
                _finish_sidecar()
                return
            if not launch_transaction.release_before_effect():
                # ``release_before_effect`` may fail after a native object was created or
                # closed ambiguously. Keep the exact transaction reachable; never create a
                # second Job/Popen on the strength of an exception string.
                process_registry.retain_foreground_custody(launch_transaction)
                _finish_sidecar()
                return
            if custody.foreground_reserved:
                if custody.scope_fence is None or not custody.scope_fence.release_foreground(
                    custody.owner_id
                ):
                    custody.mark_unknown("process_scope_unavailable")
                    process_registry.retain_foreground_custody(launch_transaction)
                    _finish_sidecar()
                    return
                custody.foreground_reserved = False
            process_registry.consume_foreground_custody(custody)
            _handler._retain_reader_until_eof = False
        else:
            if not custody.release_unaccepted():
                _finish_sidecar()
                return
            if custody.foreground_reserved:
                if custody.scope_fence is None or not custody.scope_fence.release_foreground(
                    custody.owner_id
                ):
                    custody.mark_unknown("process_scope_unavailable")
                    _finish_sidecar()
                    return
                custody.foreground_reserved = False
        custody = None
        launch_transaction = None
        _finish_sidecar()

    def _handoff_managed_reader(proc, output, reader_thread, reader_stop, evidence):
        """Register the already-running native reader without reopening its pipe."""
        nonlocal custody, launch_transaction, foreground_pending, foreground_evidence
        if custody is None or launch_transaction is None:
            return None
        deadline = evidence.get("deadline")
        if isinstance(deadline, (int, float)) and time.monotonic() >= deadline:
            evidence["deadline_expired"] = True
            return None
        if reader_thread is None or not reader_thread.is_alive():
            return None
        if reader_stop is None or reader_stop.is_set():
            return None
        evidence["process"] = proc
        evidence["pid"] = getattr(proc, "pid", None)
        evidence["host_start_time"] = launch_transaction.host_start_time
        from tools.process_registry import process_registry

        def _before_reader_finisher(session):
            # The registry invokes this only after the exact session/transaction is
            # installed and before the reader finisher can publish or close handles.
            _stamp_routing_if_gateway(process_registry, session, session_key)

        session = process_registry.transfer_foreground_reader(
            launch_transaction,
            proc=proc,
            reader_thread=reader_thread,
            reader_stop=reader_stop,
            output=output,
            evidence=evidence,
            command=command,
            cwd=cwd,
            task_id=effective_task_id,
            owner_task_id=i03_owner_task_id or task_id or effective_task_id,
            session_key=session_key,
            notify_on_complete=True,
            before_finisher=_before_reader_finisher,
        )
        if session is None:
            return None
        foreground_evidence = evidence
        custody = None
        launch_transaction = None
        foreground_pending = False
        _handler._foreground_reader_transferred = True
        _handler._foreground_pending = False
        _handler._retain_reader_until_eof = False
        if sidecar_owner is not None:
            from tools.environments.local_custody import finish_local_process
            finish_local_process(sidecar_owner, reason="local_reader_transferred")
        logger.info(
            "foreground command yielded to background as %s (pid %s) with its live reader",
            session.id, session.pid,
        )
        return {
            "output": output.render(),
            "returncode": None,
            "yielded_session_id": session.id,
            "pid": session.pid,
        }

    def _bind_source_process(proc) -> None:
        if sidecar_owner is None or sidecar_owner.process is not None:
            return
        from tools.environments.local_custody import bind_local_process
        bind_local_process(sidecar_owner, proc)

    def _retry_disposition() -> str:
        if prepare_failure:
            return "refuse"
        if foreground_pending:
            return "completed"
        return "retry" if custody is None and launch_transaction is None else "refuse"

    def _record_foreground_evidence(proc, evidence: dict) -> None:
        nonlocal foreground_evidence, foreground_pending
        # A completed no-yield handoff is only supported by the native transaction path.  The
        # POSIX/legacy path keeps its existing release semantics until a real scope adapter exists.
        foreground_pending = launch_transaction is not None and custody is not None
        foreground_evidence = (
            evidence if foreground_pending and evidence.get("reader_lease") is True
            else dict(evidence)
        )
        if foreground_binding is not None and sidecar_owner is not None:
            reader_state = evidence.get("reader_state")
            reader = evidence.get("reader_thread")
            reader_complete = (
                isinstance(reader_state, dict)
                and reader_state.get("done") is True
                and callable(getattr(reader, "is_alive", None))
                and not reader.is_alive()
                and evidence.get("status") == "true_eof"
                and evidence.get("drain_joined") is True
                and evidence.get("stdout_closed") is True
            )
            if reader_complete:
                from tools.environments.local_custody import record_local_readback
                if not record_local_readback(sidecar_owner, reader):
                    from tools.environments.local_custody import finish_local_process
                    finish_local_process(sidecar_owner, reason="local_readback_unavailable")
            else:
                from tools.environments.local_custody import finish_local_process
                finish_local_process(sidecar_owner, reason="local_readback_unavailable")
        _handler._foreground_pending = foreground_pending
        if foreground_pending:
            foreground_evidence["process"] = proc
            foreground_evidence["pid"] = getattr(proc, "pid", None)
            foreground_evidence["host_start_time"] = launch_transaction.host_start_time
            from tools.process_registry import process_registry
            process_registry.retain_foreground_custody(
                launch_transaction, binding=foreground_binding, evidence=foreground_evidence,
            )

    def _finalize_foreground(result: dict, finalized_result: Any = None) -> bool:
        nonlocal custody, launch_transaction, foreground_pending, foreground_evidence
        if not foreground_pending:
            return True
        from tools.process_registry import process_registry
        transaction = launch_transaction
        evidence = foreground_evidence
        if foreground_binding is not None and transaction is not None:
            if not _managed_reader_complete(evidence):
                if custody is not None:
                    custody.mark_unknown("foreground_reader_pending")
                process_registry.retain_foreground_custody(
                    transaction, binding=foreground_binding,
                )
                return False
            staged = process_registry.stage_foreground_result(
                transaction,
                result=result,
                finalized_result=finalized_result,
                evidence=evidence,
                command=command,
                cwd=cwd,
                task_id=effective_task_id,
                owner_task_id=i03_owner_task_id or task_id or effective_task_id,
                session_key=session_key,
            )
            if not staged:
                if custody is not None:
                    custody.mark_unknown("foreground_delivery_record_missing")
                if transaction is not None:
                    process_registry.retain_foreground_custody(
                        transaction, binding=foreground_binding,
                    )
                if sidecar_owner is not None:
                    from tools.environments.local_custody import finish_local_process
                    finish_local_process(sidecar_owner, reason="local_delivery_record_missing")
                return False
            # The parent turn's durable callback is the only authority that may admit and
            # settle this staged process. Keep the original transaction and evidence alive.
            if sidecar_owner is not None:
                from tools.environments.local_custody import finish_local_process
                finish_local_process(sidecar_owner, reason="local_readback_unavailable")
            return True
        if transaction is not None and not _managed_reader_complete(evidence):
            if custody is not None:
                custody.mark_unknown("foreground_reader_pending")
            process_registry.retain_foreground_custody(transaction)
            return False
        try:
            accepted = transaction is not None and process_registry.admit_completed_foreground(
                transaction,
                command=command,
                cwd=cwd,
                task_id=effective_task_id,
                owner_task_id=i03_owner_task_id or task_id or effective_task_id,
                session_key=session_key,
                output=result.get("output", ""),
                returncode=result.get("returncode"),
                evidence=evidence,
            )
        except Exception:  # health: allow BLE001 -- finalizer failure retains exact custody
            accepted = False
        if accepted:
            process_registry.consume_foreground_custody(custody)
            custody = None
            launch_transaction = None
            _handler._retain_reader_until_eof = False
            if sidecar_owner is not None:
                from tools.environments.local_custody import finish_local_process
                finish_local_process(sidecar_owner, reason="local_readback_unavailable")
        else:
            if custody is not None:
                custody.mark_unknown("foreground_evidence_unavailable")
            if transaction is not None:
                process_registry.retain_foreground_custody(transaction)
            if sidecar_owner is not None:
                from tools.environments.local_custody import finish_local_process
                finish_local_process(sidecar_owner, reason="local_readback_unavailable")
        foreground_pending = False
        _handler._foreground_pending = False
        foreground_evidence = None
        return bool(accepted)

    def _retain_foreground_finalization_failure(result: Optional[dict] = None) -> None:
        """Keep the native transaction unknown when result delivery cannot finish.

        ``finalize_foreground_result`` has already crossed the synchronous result boundary
        when this hook runs.  Its exception is therefore not evidence that the child was
        durably delivered; calling ``_finalize_foreground`` here would admit a transaction
        from a raw, unfinalized result.  Keep the same transaction and evidence reachable for
        the authenticated recovery consumer instead.
        """
        nonlocal foreground_pending
        if not foreground_pending:
            return
        from tools.process_registry import process_registry
        transaction = launch_transaction
        if transaction is not None:
            process_registry.retain_foreground_finalization_failure(
                transaction, result=result, evidence=foreground_evidence,
            )
        foreground_pending = True
        _handler._foreground_pending = True

    def _handler(proc, output_so_far: str) -> dict:
        nonlocal custody, launch_transaction, foreground_pending, foreground_evidence
        from tools.process_registry import process_registry
        session = process_registry.adopt_local(
            proc, command=command, cwd=cwd, task_id=effective_task_id,
            owner_task_id=i03_owner_task_id or task_id or effective_task_id,
            session_key=session_key,
            output_so_far=output_so_far, custody=custody)
        if launch_transaction is not None:
            if not launch_transaction.matches(session.process, session.pid, session.host_start_time):
                custody.mark_unknown("foreign_custody")
                raise RuntimeError("foreground process does not match its reserved native custody")
            session.custody.native_scope = launch_transaction
            process_registry.consume_foreground_custody(custody)
        custody = None
        launch_transaction = None
        _handler._retain_reader_until_eof = False
        if sidecar_owner is not None:
            from tools.environments.local_custody import finish_local_process
            finish_local_process(sidecar_owner, reason="local_reader_transferred")
        foreground_pending = False
        foreground_evidence = None
        _handler._foreground_pending = False
        _stamp_routing_if_gateway(process_registry, session, session_key)
        logger.info("foreground command yielded to background as %s (pid %s)", session.id, session.pid)
        return {
            "output": output_so_far, "returncode": None, "yielded_session_id": session.id, "pid": session.pid,
        }
    _handler._prepare_custody = _prepare_custody
    _handler._release_custody = _release_custody
    _handler._retry_disposition = _retry_disposition
    _handler._record_foreground_evidence = _record_foreground_evidence
    _handler._finalize_foreground = _finalize_foreground
    _handler._retain_foreground_finalization_failure = _retain_foreground_finalization_failure
    _handler._foreground_pending = False
    _handler._foreground_reader_transferred = False
    _handler._launch_transaction = None
    _handler._retain_reader_until_eof = False
    _handler._handoff_managed_reader = _handoff_managed_reader
    _handler._bind_source_process = _bind_source_process
    _handler._foreground_binding = foreground_binding
    return _handler


def _stamp_routing_if_gateway(process_registry, session, session_key) -> None:
    """Route the adopted session's completion like a normal notify_on_complete spawn."""
    from gateway.session_context import async_delivery_supported, get_session_env
    if not async_delivery_supported():
        session.notify_on_complete = False
        return
    _stamp_gateway_routing(session, get_session_env)
    if session.watcher_platform:
        _register_completion_watcher(process_registry, session, session_key)
