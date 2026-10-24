"""Admission-owned production exec; observers never own the worker lifetime."""
import asyncio
from contextlib import contextmanager, suppress
from collections import deque
from dataclasses import asdict, replace
from functools import partial
import json
import os
import queue
import threading
from types import SimpleNamespace
from pathlib import Path
import subprocess
import sys
import time

from agent.managed_worker import accept_result, encode_frame, read_frame
from gateway.session_worker_reservation import reserve_admission_worker
from hermes_state_runtime import RuntimeStoreError

# The interpreter only imports psutil before it introduces itself; a child silent this long
# is wedged (loader/stdio stall), not slow.
HELLO_SECONDS = 60
# After Stop the child interrupts its agent and emits its result; the owner terminates a
# child that has not acknowledged within this window instead of waiting on the pipe forever.
STOP_ACK_SECONDS = 30
# Own session (POSIX): a worker whose owner died kills its whole group (managed_worker
# _die_with_owner); on Windows it exits itself.
WORKER_POPEN = {'start_new_session': True, 'close_fds': True}


class ManagedExecutionUnknown(RuntimeError):
    """The committed unknown verdict stops the FIFO without forging failed settlement."""


def managed_policy(authority, ref):
    """Bypass (safe / config-only) sessions always execute out of process; other local
    sessions only under the explicit custom-provider opt-in.

    The frozen creation snapshot, not current profile config, chooses execution.
    """
    from gateway.session_policy import policy_for_source
    policy = policy_for_source(authority.runner, authority.sessions[ref.session_id].source)
    if policy is None:
        return None
    if policy.ignore_user_config or policy.kanban_json is not None:
        return policy
    if policy.config().get('gateway', {}).get('managed_workers') is not True:
        return None
    request = json.loads(policy.request_json)
    if policy.platform != 'cli' or request.get('provider') != 'custom' or not request.get('base_url'):
        raise RuntimeStoreError('unsupported_managed_policy')
    return policy


def _bootstrap(authority, ref, row, policy, scope, text=None):
    from gateway.session_ingress import row_turn_author
    from gateway.session_policy import route_launch_key
    from gateway.session_policy_credentials import recover_config_secrets
    terminal = json.loads(policy.terminal_json)
    if policy.config_secret_ref:
        for path, value in recover_config_secrets(authority, policy).items():
            if path[0] is None:
                terminal[path[1]] = value
    live = authority.sessions[ref.session_id]
    # This turn's facts ride the per-turn hydrated request (the bootstrap field set is closed):
    # the admission's one-shot flags, the route's YOLO as of now (never the frozen launch flag) and
    # the committed surface (HUD / live voice / voice turn) when admission recorded one.
    request = dict(json.loads(policy.request_json), turn_v1={
        'finite': row['payload'].get('finite', False), 'unattended': row['payload'].get('unattended') is True,
        'yolo': _session_yolo(authority, live.route, policy),
        **({'surface_v1': row['payload']['surface_v1']} if row['payload'].get('surface_v1') else {}),
        **({'display_v1': row['payload']['display_v1']} if row['payload'].get('display_v1') else {})})
    from gateway.session_worker_construct import construct_inputs
    request['construct_v1'] = construct_inputs(authority, policy, live.source.chat_id)
    config = _worker_config(authority, policy)
    hydrated = replace(policy, config_json=json.dumps(config), request_json=json.dumps(request),
                       terminal_json=json.dumps(terminal), credential_ref=None, config_secret_ref=None)
    return {'version': 1, 'home': authority.profile_id, 'scope': scope,
            'policy': asdict(hydrated), 'api_key': route_launch_key(authority, policy, config),
            'text': row['payload']['text'] if text is None else text, 'route': live.route,
            **({'attachments_v1': row['payload']['attachments_v1']} if 'attachments_v1' in row['payload'] else {}),
            'user_id': live.source.user_id, 'chat_id': live.source.chat_id,
            'turn_author': row_turn_author(policy, row),
            'safe_mode': policy.safe_mode, 'ignore_user_config': policy.ignore_user_config}


def _worker_config(authority, policy):
    """The frozen session config the child binds as its only config (``bind_worker_policy``).
    ``command_allowlist`` is approval state rather than launch policy: an ``always`` grant an
    earlier worker of an ordinary session persisted, or one the operator removed by hand, comes
    from the live profile file, as it did before the child's config was frozen. Bypass sessions
    never read the profile."""
    config = policy.config(authority)
    home = Path(str(authority.profile_id))
    if policy.ignore_user_config or not home.is_absolute():
        return config
    from gateway.run import _load_gateway_config
    live = _load_gateway_config(home / 'config.yaml')
    config.pop('command_allowlist', None)
    if live.get('command_allowlist') is not None:
        config['command_allowlist'] = live['command_allowlist']
    return config


def _session_yolo(authority, route, policy):
    """The route's bypass as the in-process turn arms it on the owner, the one place a revocation
    is recorded: a ``--yolo`` launch seeded once per boundary, then the persisted ``/yolo`` copy."""
    from tools.approval import is_session_yolo_enabled
    from tools.approval_yolo import restore_gateway_yolo
    store = getattr(authority.runner, 'session_store', None)
    entry = store.lookup_by_session_key(route) if store is not None else None
    restore_gateway_yolo(route, getattr(entry, 'yolo', None), launch=bool(policy.yolo))
    return is_session_yolo_enabled(route)


@contextmanager
def worker_turn_scope(frame):
    """Child side of ``turn_v1``: bind what the in-process turn binds on the owner
    (execute_finite_admission, the route's YOLO, the committed surface), so ``chat -q``/``-z``
    never park a prompt, the session's current YOLO governs this child and a HUD / live-voice /
    voice turn reaches the model as it does in process. Frames without it bind nothing."""
    from gateway.session_finite import finite_turn_scope
    from gateway.session_display import display_turn_scope, restore_display
    from gateway.session_surface import restore_surface, surface_turn_scope
    turn = json.loads(frame['policy'].get('request_json') or '{}').get('turn_v1')
    if turn is None:
        yield
        return
    flags = {k: v for k, v in turn.items() if k not in ('surface_v1', 'display_v1')} if isinstance(turn, dict) else None
    if (flags is None or set(flags) != {'finite', 'unattended', 'yolo'}
            or any(type(v) is not bool for v in flags.values()) or (turn['unattended'] and not turn['finite'])):
        raise ValueError('invalid_managed_worker_bootstrap')
    try:
        surface = restore_surface(turn['surface_v1']) if 'surface_v1' in turn else None
        display = restore_display(turn['display_v1']) if 'display_v1' in turn else None
    except RuntimeStoreError as exc:
        raise ValueError('invalid_managed_worker_bootstrap') from exc
    if turn['yolo']:
        from tools.approval import enable_session_yolo
        enable_session_yolo(frame['route'])
    with finite_turn_scope(turn['finite'], turn['unattended']), surface_turn_scope(surface), \
            display_turn_scope(frame['scope']['execution_id'], display):
        yield


class ManagedWorker:
    def __init__(self, process):
        self.process = process
        # Verified interpreter behind the handle (a launcher trampoline may sit between).
        self.worker = None
        self.write_lock = threading.Lock()
        self.commands = queue.Queue(maxsize=16)
        self.closed = threading.Event()
        # Latched the moment Stop is admitted, before any pipe write: the owner's read loop
        # supervises it even while the child has not yet said hello or read its bootstrap.
        self.stop = asyncio.Event()
        # Monotonic instant of the FIRST Stop: one acknowledgment budget runs from here, never
        # renewed by the child's later output or by a repeated Stop.
        self.stopped_at = None
        # The gateway restart/shutdown reason when that stop (not a user's) signalled this worker.
        self.stop_cause = None
        self.writer = threading.Thread(target=self._write_controls, name='managed-control-writer', daemon=True)
        # One dedicated reader thread per worker (never the loop's shared default executor, which
        # parked pipe reads would exhaust). It reads one frame per demand, so the pipe keeps its
        # backpressure, and posts it here; a cancelled next_frame leaves the frame for the next.
        self.reader = None
        self._demand = threading.Semaphore(0)
        self._requested = False
        self._frames = deque()
        self._arrived = None
        self.stderr_drain = None
        # Exact secrets this worker was handed; the stderr sink scrubs them besides the patterns.
        self.secrets = []
        # Optional I16 source receipt.  The default worker path stays byte-for-byte
        # compatible; the managed producer adapter installs these only after its
        # physical receipt is reserved and before any reader starts.
        self._managed_worker_receipt = None
        self._managed_worker_native_terminate = None

    def _write_controls(self):
        try:
            while not self.closed.is_set():
                try:
                    frame = self.commands.get(timeout=.5)
                except queue.Empty:
                    continue
                self.send(frame)
        except (OSError, ValueError):
            self.closed.set()

    def _read_frames(self, loop):
        """Reader thread body. It owns ``stdout``: no other thread closes it under a blocked read."""
        observed_eof = False
        reader_failed = False
        try:
            while self._demand.acquire() and not self.closed.is_set():
                try:
                    item = read_frame(self.process.stdout)
                except (EOFError, ValueError, OSError, RecursionError) as exc:
                    observed_eof = isinstance(exc, EOFError)
                    reader_failed = not observed_eof
                    if reader_failed:
                        receipt = self._managed_worker_receipt
                        if receipt is not None:
                            receipt.reader_failed('reader_failed')
                    item = exc  # EOF / invalid or oversized frame / closed pipe: the owner raises it
                except BaseException:  # health: allow BLE001 -- opaque reader faults retain the native receipt
                    # A reader body is an accepted native worker operation.  An
                    # opaque BaseException must become an owner-visible failure,
                    # not an unobserved thread death that later looks joined.
                    reader_failed = True
                    receipt = self._managed_worker_receipt
                    if receipt is not None:
                        receipt.reader_failed('reader_failed')
                    item = RuntimeStoreError('managed_worker_reader_failed')
                try:
                    loop.call_soon_threadsafe(self._deliver, item)
                except RuntimeError:
                    reader_failed = True
                    return  # the owner loop has closed; nobody is left to read for
                if isinstance(item, Exception):
                    return
        finally:
            receipt = self._managed_worker_receipt
            reader = self.reader
            if receipt is not None and observed_eof:
                try:
                    receipt.reader_eof('stdout', reader)
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    receipt.mark_unknown('reader_state_unknown')
            elif receipt is not None and reader_failed:
                receipt.reader_failed('reader_failed')
            try:
                self.process.stdout.close()
            except BaseException:  # health: allow BLE001 -- a reader close fault retains native custody
                if receipt is not None:
                    receipt.reader_error = 'reader_close_failed'
                    receipt.mark_unknown('reader_close_failed')

    def _deliver(self, item):
        self._frames.append(item)
        self._arrived.set()

    def _take(self):
        item = self._frames[0]
        if isinstance(item, Exception):
            raise item  # stays queued: the reader has exited, every later read fails the same way
        self._frames.popleft()
        self._requested = False
        if not self._frames:
            self._arrived.clear()
        return item

    def drain_stderr(self, path):
        """Route the child's stderr (its tracebacks and redirected prints) into the profile's
        bounded, redacted, private log instead of discarding it."""
        from agent.memory_provider import spawn_context_thread
        from gateway.session_managed_worker_log import drain_worker_stderr
        def drain():
            observed_eof = False
            try:
                drain_worker_stderr(self.process.stderr, path, self.process.pid, lambda: self.secrets)
                observed_eof = True
            except BaseException:  # health: allow BLE001 -- opaque stderr faults retain the native receipt
                receipt = self._managed_worker_receipt
                if receipt is not None:
                    receipt.reader_failed('reader_failed')
                raise
            finally:
                receipt = self._managed_worker_receipt
                if receipt is not None and observed_eof:
                    reader = threading.current_thread()
                    try:
                        receipt.reader_eof('stderr', reader)
                    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                        receipt.mark_unknown('reader_state_unknown')

        self.stderr_drain = spawn_context_thread(drain, name='managed-stderr-drain')
        receipt = self._managed_worker_receipt
        if receipt is not None:
            receipt.bind_reader('stderr', self.stderr_drain)
            if not receipt.reader_started('stderr', self.stderr_drain):
                raise RuntimeStoreError('reader_start_unknown')
        try:
            self.stderr_drain.start()
        except BaseException:  # health: allow BLE001 -- opaque reader start retains the native receipt
            receipt = self._managed_worker_receipt
            if receipt is not None:
                receipt.reader_start_failed('stderr', self.stderr_drain, 'reader_start_unknown')
            raise

    def control(self, frame):
        if self.closed.is_set():
            raise RuntimeStoreError('managed_worker_lost')
        if frame == {'type': 'stop'}:
            if self.stopped_at is None:
                self.stopped_at = time.monotonic()
            self.stop.set()
        try:
            self.commands.put_nowait(frame)
        except queue.Full as exc:
            raise RuntimeStoreError('worker_control_backpressure') from exc

    def _stop_budget(self, ack):
        """Seconds left of the ``ack`` window that opened at the first Stop. Pure: only
        ``control`` stamps ``stopped_at``, so a read before any Stop (the hello) never starts
        the window a later Stop must receive in full."""
        return self.stopped_at + ack - time.monotonic()

    async def next_frame(self, timeout, ack):
        """Read one frame. A requested Stop bounds supervision to ``ack`` seconds TOTAL from the
        first Stop: a child that keeps emitting valid frames is escalated on the same deadline as a
        silent one, instead of leaving the turn started behind it."""
        if self.reader is None:
            self._arrived = asyncio.Event()
            from agent.memory_provider import spawn_context_thread
            self.reader = spawn_context_thread(self._read_frames, name='managed-frame-reader',
                                               args=(asyncio.get_running_loop(),))
            receipt = self._managed_worker_receipt
            if receipt is not None:
                receipt.bind_reader('stdout', self.reader)
                if not receipt.reader_started('stdout', self.reader):
                    raise RuntimeStoreError('reader_start_unknown')
            try:
                self.reader.start()
            except BaseException:  # health: allow BLE001 -- opaque reader start retains the native receipt
                receipt = self._managed_worker_receipt
                if receipt is not None:
                    receipt.reader_start_failed('stdout', self.reader, 'reader_start_unknown')
                raise
        if not self._frames and not self._requested:
            self._requested = True
            self._demand.release()
        arrived = asyncio.ensure_future(self._arrived.wait())
        stopper = asyncio.ensure_future(self.stop.wait())
        try:
            if not self._frames and not self.stop.is_set():
                await asyncio.wait({arrived, stopper}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                if not self._frames and not self.stop.is_set():
                    raise RuntimeStoreError('managed_worker_hello_timeout')
            if not self._frames:
                # Only a latched Stop gets here; its window runs from the first Stop.
                remaining = self._stop_budget(ack)
                if remaining > 0:
                    await asyncio.wait({arrived}, timeout=remaining)
            if not self._frames:
                raise RuntimeStoreError('managed_worker_stopped')
            return self._take()
        finally:
            stopper.cancel()
            arrived.cancel()

    def interrupt(self):
        self.control({'type': 'stop'})

    def respond(self, kind, prompt_id, value):
        self.control({'type': kind, 'prompt_id': prompt_id, 'value': value})

    def send(self, frame):
        with self.write_lock:
            self.process.stdin.write(encode_frame(frame))
            self.process.stdin.flush()

    def _signal_worker(self, kill):
        """Signal the verified interpreter, not only the handle: a launcher that exec-chained
        or exited leaves the real worker outside the Popen's reach."""
        import psutil
        if self.worker is None or self.worker[0] == self.process.pid:
            return
        try:
            proc = psutil.Process(self.worker[0])
            from gateway.session_worker import _canonical_worker_birth
            _canonical_worker_birth(self.worker[0], self.worker[1])
            (proc.kill if kill else proc.terminate)()
        except (RuntimeError, RuntimeStoreError, psutil.Error):
            pass

    def _signal_group(self, kill):
        """POSIX: signal the worker's whole process group (it leads one, WORKER_POPEN), so the tool
        children a killed worker never reaps with _die_with_owner go too. Only while the leader is
        unreaped and still leads its group; otherwise (Windows, gone) the handle alone."""
        native_terminate = self._managed_worker_native_terminate
        if callable(native_terminate):
            native_terminate(kill)
            return
        if os.name != 'nt' and self.process.poll() is None:
            import signal
            try:
                if os.getpgid(self.process.pid) == self.process.pid:
                    os.killpg(self.process.pid, signal.SIGKILL if kill else signal.SIGTERM)  # windows-footgun: ok — POSIX only
                    return
            except (ProcessLookupError, PermissionError):
                pass
        (self.process.kill if kill else self.process.terminate)()

    def _managed_cleanup_failure(self, reason):
        receipt = self._managed_worker_receipt
        if receipt is not None:
            receipt.mark_unknown(reason)
        self.closed.set()
        self._demand.release()

    def close(self):
        receipt = self._managed_worker_receipt
        if receipt is not None:
            from gateway.session_managed_worker_custody import close_managed_worker

            close_managed_worker(self, receipt)
            return

        self.closed.set()
        self._demand.release()  # a reader parked between frames exits; one mid-read ends at EOF
        if self.process.poll() is None:
            self._signal_worker(kill=False)
            self._signal_group(kill=False)
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._signal_worker(kill=True)
            self._signal_group(kill=True)
            with suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=5)
        else:
            self._signal_worker(kill=True)
        if self.writer.ident is not None:
            self.writer.join(timeout=5)
        self.process.stdin.close()
        if self.reader is None:
            self.process.stdout.close()
        else:
            self.reader.join(timeout=5)
        if self.stderr_drain is not None:
            self.stderr_drain.join(timeout=5)
        elif self.process.stderr is not None:
            self.process.stderr.close()


def interrupt_managed(authority, actor, ref, generation):
    worker = getattr(authority, '_managed_workers', {}).get(ref.session_id)
    if worker is None:
        return False
    authority.authorize(actor, ref, 'session:control')
    authority.check_approval_generation(ref.session_id, generation)
    worker.control({'type': 'stop'})
    return True


def _prompt_frame(authority, ref, row, worker, frame):
    live = authority.sessions[ref.session_id]
    controls = live.controls
    kind = frame.get('type')
    if kind == 'prompt_settled' and set(frame) == {'type', 'prompt_id'}:
        prompt_id = frame['prompt_id']
        controls.remote_responders.pop(prompt_id, None)
        saved = controls.pending.pop(prompt_id, None)
        if saved:
            live.event_stream.publish(ref.session_id, {'prompt_id': prompt_id,
                'execution_generation': row['generation']}, event_type=saved[1]['kind'] + '.settled')
        return True
    if kind not in {'approval', 'clarify'}:
        return False
    if len(controls.remote_responders) >= 16:
        raise RuntimeStoreError('worker_control_backpressure')
    if kind == 'approval':
        fields = {'request_id', 'command', 'description', 'allow_session', 'allow_permanent', 'smart_denied', 'edit'}
        data = frame.get('data')
        if set(frame) != {'type', 'data'} or not isinstance(data, dict) or set(data) - fields:
            raise RuntimeStoreError('invalid_worker_frame')
        prompt_id = data.get('request_id')
    else:
        if (set(frame) != {'type', 'prompt_id', 'question', 'choices', 'multi_select'}
                or not isinstance(frame['question'], str) or not isinstance(frame['choices'], list)
                or any(not isinstance(c, str) for c in frame['choices']) or type(frame['multi_select']) is not bool):
            raise RuntimeStoreError('invalid_worker_frame')
        prompt_id = frame['prompt_id']
    if not isinstance(prompt_id, str) or not prompt_id or prompt_id in controls.pending:
        raise RuntimeStoreError('invalid_worker_frame')
    controls.remote_responders[prompt_id] = worker.respond
    if kind == 'approval':
        authority.register_approval(ref.session_id, row['generation'], live.route, data)
    else:
        entry = SimpleNamespace(clarify_id=prompt_id, question=frame['question'], choices=frame['choices'],
                                multi_select=frame['multi_select'], event=threading.Event())
        authority.register_clarify(ref.session_id, row['generation'], entry)
    return True


def _worker_env(authority):
    """Child env for the OWNING profile under multiplex: its HERMES_HOME plus its ``.env``
    secrets over a scrubbed base, never the launch profile's process environment (the same
    rule MCP stdio children and shell hooks follow) — except for the launch profile's own
    worker, whose scope legitimately includes its frozen launch env. Single-profile gateways
    inherit the process env byte-for-byte, exactly as before."""
    from pathlib import Path
    from agent.secret_scope import is_multiplex_active
    from tools.environments.local import _is_routed_home
    home = Path(str(authority.profile_id))
    if not home.is_absolute():
        return None
    # Keyed on the worker's OWNING profile, not only the process-wide multiplex flag: a worker for
    # another profile must never inherit the launch environ even when that flag reads False.
    routed = _is_routed_home(home)
    if not routed and not is_multiplex_active():
        return None
    from agent.secret_scope import build_profile_secret_scope
    from tools.environments.local import _scrub_credentials, build_subprocess_env, strip_launch_profile_env
    # The scrub removes credentials, not settings: the launch profile's TERMINAL_* policy and
    # its ``.env`` settings would otherwise reach the secondary's worker (cron/kanban rule).
    # Strip the RAW environ before the constructor injects this turn's own session/bridge context:
    # a launch ``.env`` name (HERMES_SESSION_ID...) must not erase a value derived for this turn.
    env = build_subprocess_env(base=strip_launch_profile_env(os.environ.copy(), home), scrub_secrets=True)
    secrets = build_profile_secret_scope(home)
    if routed:
        # Same rule as served_profile_child_env: env_passthrough / first-party carve-outs must not
        # forward launch-process provider credentials that no .env or source snapshot recorded.
        _scrub_credentials(env, inherit_credentials=False)
    else:
        secrets = {**_launch_env_only_credentials(home, env), **secrets}
    env.update({k: v for k, v in secrets.items() if v is not None})
    env['HERMES_HOME'] = str(home)
    from hermes_constants import apply_subprocess_home_env
    apply_subprocess_home_env(env)
    return env


def _launch_env_only_credentials(home, env):
    """Credentials the LAUNCH profile's owner resolves from its frozen launch env
    (``launch_secret_scope``: systemd ``Environment=`` / ``op run`` keys with no ``.env`` line) that
    the scrub removed from *env* — the same mapping the owner's own ``get_secret`` reads for this
    profile. Credential names only: non-credential names the constructor removed on purpose
    (unbound session context, venv markers) stay removed, and Hermes-internal secrets
    (``AUXILIARY_*_API_KEY``, relay auth) never reach a child. Only the launch home's worker calls
    this; a routed home's worker sees its own files only."""
    from tools.environments.local import _scrub_credentials
    from tools.environments.local_env_policy import _is_hermes_internal_secret
    from tui_gateway.launch_profile_policy import launch_secret_scope
    missing = {k: v for k, v in launch_secret_scope(home).items()
               if k not in env and not _is_hermes_internal_secret(k)}
    plain = _scrub_credentials(dict(missing), inherit_credentials=False)
    return {k: v for k, v in missing.items() if k not in plain}


def _interrupted_before_bootstrap(authority, row, cause=None):
    authority.pending_results[row['admission_id']] = {
        'result': {'final_response': '', 'interrupted': True, **({'interrupt_message': cause} if cause else {})},
        'usage': {}}
    return ''


def _latched_stop_cause(authority, session_id, generation):
    """The gateway restart/shutdown cause latched for this generation before its worker existed
    (``run_runtime._stop_spawning_managed``), consumed once; None for a user's Stop."""
    latched = getattr(authority, 'pending_stop_causes', {}).pop(session_id, None)
    return latched[1] if latched and latched[0] == generation else None


def _with_stop_cause(accepted, worker):
    """The wire result carries no interruption cause; the owner recorded which stop it sent."""
    if accepted.get('interrupted') and worker.stop_cause:
        return {**accepted, 'interrupt_message': worker.stop_cause}
    return accepted


def _settled_session_usage(authority, session_id, accepted, execution_id=None):
    """A managed turn's ``message.complete.usage``: the worker exits with its turn, so the session-cumulative counters come from the rows its usage writes committed. A worker that compresses mid-turn ends on a child of this reservation's session; the owner's execution row records that tip (compression moves it in the same transaction), so the path from the tip back to the reservation plus the reservation's own compression ancestors is summed, and stale siblings or the conversation a branch came from never count."""
    import sqlite3
    db = authority.db
    try:
        tip = _execution_tip(db, execution_id)
        path = _continuation_path(db, session_id, tip) if tip and tip != session_id else None
        ids = list(dict.fromkeys([*_compression_ancestors(db, session_id), *(path or [session_id])]))
        rows = [db.get_session(sid) or {} for sid in ids]
    except sqlite3.Error:  # usage is display-only; settlement never waits on it
        return {}
    counts = {key: sum(int(row.get(key) or 0) for row in rows) for key in (
        'input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens')}
    prompt = counts['input_tokens'] + counts['cache_read_tokens'] + counts['cache_write_tokens']
    return {'session_usage': {**counts, 'prompt_tokens': prompt, 'completion_tokens': counts['output_tokens'],
                              'total_tokens': prompt + counts['output_tokens'],
                              'api_calls': sum(int(row.get('api_call_count') or 0) for row in rows),
                              'model': str(accepted.get('model') or rows[-1].get('model') or '')}}


def _execution_tip(db, execution_id):
    """The session this worker execution last wrote to, as the owner's ledger records it."""
    if not execution_id:
        return None
    with db._read_ctx() as conn:
        row = conn.execute('SELECT session_id FROM worker_executions WHERE execution_id=?', (execution_id,)).fetchone()
    return row[0] if row else None


def _preflight_managed_compression_route(custody, proof):
    """Run the pure proof gate while the source-owned service/receipt is still live."""
    service = getattr(custody, 'service', None)
    if service is None or getattr(service, 'unknown', False) is True:
        raise RuntimeStoreError('managed_compression_route_unknown')
    preflight = getattr(custody, 'preflight_finish_proof', None)
    if not callable(preflight) or preflight(proof) is None:
        raise RuntimeStoreError('managed_compression_route_unknown')


def _managed_compression_route_headers_valid(
    authority, admission, scope, proof, expected_session, execution_id, generation, epoch,
    admission_id, store_path,
):
    """Check the immutable route/proof header fields without touching live custody."""
    if not isinstance(admission, dict):
        return False
    return all((
        scope.get('profile_id') == authority.profile_id,
        type(execution_id) is str,
        bool(execution_id),
        type(generation) is int,
        generation >= 1,
        type(epoch) is int,
        epoch == authority.epoch,
        type(admission_id) is str,
        bool(admission_id),
        type(store_path) is str,
        bool(store_path),
        type(proof.get('store_path')) is str,
        proof.get('store_path') == store_path,
        admission.get('target_session_id') == expected_session,
        type(admission.get('generation')) is int,
        admission.get('generation') == generation,
        type(admission.get('owner_epoch')) is int,
        admission.get('owner_epoch') == epoch,
        admission.get('status') == 'started',
        type(proof.get('admission_id')) is str,
        proof.get('admission_id') == admission_id,
        type(proof.get('execution_id')) is str,
        proof.get('execution_id') == execution_id,
        type(proof.get('authority_epoch')) is int,
        proof.get('authority_epoch') == epoch,
        type(proof.get('execution_generation')) is int,
        proof.get('execution_generation') == generation,
    ))


def _managed_compression_lineage_valid(
    authority, ref, expected, target, execution_id, generation, epoch, admission_id,
):
    """Read the durable worker/admission lineage without changing the live route."""
    import sqlite3
    try:
        with authority.db._read_ctx() as conn:
            worker = conn.execute(
                'SELECT session_id,kind,owner_epoch,generation,status '
                'FROM worker_executions WHERE execution_id=?', (execution_id,),
            ).fetchone()
            admission_row = conn.execute(
                'SELECT target_session_id,lineage_json,owner_epoch,generation,status '
                'FROM session_admissions WHERE admission_id=?', (admission_id,),
            ).fetchone()
        if worker is None or admission_row is None:
            return False
        if not all((
            worker['session_id'] == target,
            worker['kind'] == 'compute',
            type(worker['owner_epoch']) is int,
            worker['owner_epoch'] == epoch,
            type(worker['generation']) is int,
            worker['generation'] == generation,
            worker['status'] in {'running', 'terminal'},
            admission_row['target_session_id'] == ref.session_id,
            type(admission_row['owner_epoch']) is int,
            admission_row['owner_epoch'] == epoch,
            type(admission_row['generation']) is int,
            admission_row['generation'] == generation,
            admission_row['status'] == 'started',
        )):
            return False
        admission_lineage = json.loads(admission_row['lineage_json'])
        if not isinstance(admission_lineage, list) or any(
            type(item) is not str or not item for item in admission_lineage
        ):
            return False
        lineage = tuple(authority.db.get_compression_lineage(ref.session_id))
        continuation = _continuation_path(authority.db, expected, target)
        return all((
            lineage,
            lineage[0] == ref.session_id,
            lineage[-1] == target,
            tuple(admission_lineage) == lineage,
            authority.db.get_compression_tip(ref.session_id) == target,
            continuation is not None,
            continuation[0] == expected,
            continuation[-1] == target,
        ))
    except (OSError, TypeError, ValueError, KeyError, sqlite3.Error):
        return False


def _advance_managed_compression_route(
    authority, ref, admission, scope, proof, *, binding=None, custody=None,
):
    """CAS-advance the owner route for an authenticated managed compression child.

    The worker-side ``compression.publish`` transaction moves the durable execution and
    admission lineage, but it cannot mutate the gateway process's in-memory route.  The
    physical-tip witness therefore needs one owner-side reconciliation before durable proof
    settlement.  Every check here is source-owned identity; a stale, foreign, or replaced
    child remains UNKNOWN rather than becoming a route mutation.
    """
    if not isinstance(proof, dict):
        raise RuntimeStoreError('managed_compression_route_unknown')
    if not isinstance(scope, dict):
        raise RuntimeStoreError('managed_compression_route_unknown')
    expected = scope.get('session_id')
    target = proof.get('session_id')
    if type(expected) is not str or not expected or type(target) is not str or not target:
        raise RuntimeStoreError('managed_compression_route_unknown')
    if target == expected:
        return
    execution_id = scope.get('execution_id')
    generation = scope.get('generation')
    epoch = scope.get('epoch')
    admission_id = admission.get('admission_id') if isinstance(admission, dict) else None
    try:
        from gateway.runtime_service_custody import (
            ServiceCustodyError, TurnCustodyBinding, admission_binding, validate_turn_binding,
        )
        if binding is None:
            binding = admission_binding(authority, ref, admission)
        if (
            not isinstance(binding, TurnCustodyBinding)
            or binding.authority is not authority
            or binding.admission_id != admission_id
            or binding.session_id != ref.session_id
        ):
            raise RuntimeStoreError('managed_compression_route_unknown')
        validate_turn_binding(binding)
        if custody is None:
            raise RuntimeStoreError('managed_compression_route_unknown')
        service = getattr(custody, 'service', None)
        if service is None or getattr(service, 'binding', None) is not binding:
            raise RuntimeStoreError('managed_compression_route_unknown')
        service.validate_binding(binding)
    except (AttributeError, KeyError, RuntimeError, ServiceCustodyError, TypeError, ValueError):
        raise RuntimeStoreError('managed_compression_route_unknown') from None
    from gateway.session_turn_physical_tip import canonical_store_path
    store_path = canonical_store_path(authority)
    if not _managed_compression_route_headers_valid(
        authority, admission, scope, proof, ref.session_id, execution_id, generation, epoch,
        admission_id, store_path,
    ):
        raise RuntimeStoreError('managed_compression_route_unknown')

    # The route CAS is an effect.  Authenticate the worker's exact source-produced row
    # snapshots, digest, binding, and native receipt before changing the in-memory tip.  This
    # preflight is pure; malformed/copy proofs therefore leave both the route and receipt maps
    # untouched.  The final physical receipt check still runs during ordinary settlement.
    try:
        _preflight_managed_compression_route(custody, proof)
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        raise RuntimeStoreError('managed_compression_route_unknown') from None

    if not _managed_compression_lineage_valid(
        authority, ref, expected, target, execution_id, generation, epoch, admission_id,
    ):
        raise RuntimeStoreError('managed_compression_route_unknown')

    store = getattr(getattr(authority, 'runner', None), 'session_store', None)
    live = (getattr(authority, 'sessions', {}) or {}).get(ref.session_id)
    route = getattr(live, 'route', None)
    if store is None or type(route) is not str or not route:
        raise RuntimeStoreError('managed_compression_route_unknown')
    entry = store.lookup_by_session_key(route)
    current = getattr(entry, 'session_id', None) if entry is not None else None
    if current == target:
        return
    if current != expected:
        raise RuntimeStoreError('managed_compression_route_unknown')
    # The durable lineage/readback above may yield to a source retirement or receipt
    # replacement.  Re-run the pure proof gate synchronously at the final effect boundary so
    # the CAS never relies on an earlier live-service/native snapshot.
    try:
        _preflight_managed_compression_route(custody, proof)
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        raise RuntimeStoreError('managed_compression_route_unknown') from None
    switched = store.advance_compression_session(route, expected, target)
    if switched is None or getattr(switched, 'session_id', None) != target:
        raise RuntimeStoreError('managed_compression_route_unknown')


def _continuation_path(db, start, tip):
    """Ids from *start* to *tip* when *tip* reaches *start* through compression continuations only, else None."""
    path, current = [tip], tip
    while current != start and len(path) <= 200:
        row = db.get_session(current)
        if not row or not db._is_compression_child_row(row):
            return None
        current = row['parent_session_id']
        path.append(current)
    return path[::-1] if current == start else None


def _compression_ancestors(db, session_id):
    """Root-first ids of the sessions *session_id* continues by compression, excluding itself."""
    ancestors, current = [], db.get_session(session_id)
    while current and db._is_compression_child_row(current) and len(ancestors) < 200:
        parent = current['parent_session_id']
        if parent in ancestors or parent == session_id:
            break
        ancestors.append(parent)
        current = db.get_session(parent)
    return ancestors[::-1]


def _mark_spawning(authority, ref, row):
    """Mark the generation pre-worker: until the worker registers, a gateway stop has nothing to
    signal, so it latches for this generation instead (#134889 GW-22). Set before ``@`` expansion,
    which may await the network, so a Stop there is never lost."""
    spawning = getattr(authority, '_managed_spawning', None)
    if spawning is None:
        spawning = authority._managed_spawning = {}
    spawning[ref.session_id] = row.get('generation')
    return spawning


def _stopped_before_spawn(authority, ref, row):
    """A Stop latched for this generation while it had no worker (a gateway stop during ``@``
    expansion, or a user's Stop): consume it and record the interrupted result, never spawning."""
    generation = row.get('generation')
    stops = getattr(authority, 'pending_stops', {})
    if generation is None or stops.get(ref.session_id) != generation:
        return False
    del stops[ref.session_id]
    authority._managed_spawning.pop(ref.session_id, None)
    _interrupted_before_bootstrap(authority, row, _latched_stop_cause(authority, ref.session_id, generation))
    return True


async def _spawn_registered_worker(authority, ref, row, custody=None):
    """Spawn the turn's child and register it for Stops: ``(worker, process)``, or None when the spawn was cancelled (the
    interrupted result is already recorded). The generation stays marked spawning until registration."""
    spawning = _mark_spawning(authority, ref, row)
    generation = row.get('generation')
    try:
        env = await asyncio.to_thread(_worker_env, authority)
        cwd = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
        from gateway.session_worker_spawn import acquire_process
        from gateway.session_managed_worker_log import worker_log_path
        log_path = worker_log_path(authority.profile_id)
        if custody is not None:
            custody.configure_streams({'stdout'} if log_path is None else {'stdout', 'stderr'})
            if sys.platform == 'win32':
                def spawn_windows(*args, **kwargs):
                    kwargs = dict(kwargs)
                    kwargs.setdefault('text', False)
                    kwargs.setdefault('encoding', None)
                    kwargs.setdefault('errors', None)
                    kwargs.setdefault('bufsize', -1)
                    kwargs.setdefault('creationflags', 0)
                    return custody.launch_windows(*args, **kwargs)

                process = await acquire_process(
                    spawn_windows,
                    [sys.executable, '-m', 'agent.managed_worker'],
                    cwd=cwd, stdin=subprocess.PIPE, env=env, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL if log_path is None else subprocess.PIPE,
                    cleanup=custody.cleanup_spawned_process, **WORKER_POPEN,
                )
            else:
                process = await acquire_process(
                    subprocess.Popen,
                    [sys.executable, '-m', 'agent.managed_worker'],
                    cwd=cwd, stdin=subprocess.PIPE, env=env, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL if log_path is None else subprocess.PIPE,
                    cleanup=custody.cleanup_spawned_process, **WORKER_POPEN,
                )
        else:
            process = await acquire_process(subprocess.Popen, [sys.executable, '-m', 'agent.managed_worker'],
                cwd=cwd, stdin=subprocess.PIPE, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL if log_path is None else subprocess.PIPE, **WORKER_POPEN)
    except asyncio.CancelledError:
        # acquire_process owns late-child cleanup; no bootstrap or reservation was sent.
        spawning.pop(ref.session_id, None)
        _interrupted_before_bootstrap(authority, row, _latched_stop_cause(authority, ref.session_id, generation))
        return None
    except BaseException:  # health: allow BLE001 -- spawn faults retain the exact admission boundary
        spawning.pop(ref.session_id, None)
        raise
    try:
        worker = ManagedWorker(process)
    except BaseException:  # health: allow BLE001 -- an opaque constructor fault retains the exact native child
        spawning.pop(ref.session_id, None)
        if custody is not None:
            custody.cleanup_spawned_process(process)
        raise
    if custody is not None:
        try:
            if sys.platform == 'win32':
                custody.bind_windows_transaction(custody.physical.transaction, process)
            else:
                custody.bind_process(process)
            custody.attach_worker(worker)
        except BaseException:  # health: allow BLE001 -- bind faults retain the exact native child
            # Keep the exact worker/process/receipt reachable for the owner-side
            # UNKNOWN path; never replace it with a fresh Popen.
            spawning.pop(ref.session_id, None)
            custody.cleanup_spawned_process(process)
            raise
    if log_path is not None:
        try:
            worker.drain_stderr(log_path)
        except BaseException:  # health: allow BLE001 -- reader construction/start faults retain native custody
            spawning.pop(ref.session_id, None)
            if custody is not None:
                custody.cleanup_spawned_process(process)
            raise
    workers = getattr(authority, '_managed_workers', None)
    if workers is None:
        workers = authority._managed_workers = {}
    workers[ref.session_id] = worker
    spawning.pop(ref.session_id, None)
    worker.stop_cause = _latched_stop_cause(authority, ref.session_id, generation)
    return worker, process


async def admit_managed(authority, ref, row, policy, binding=None):
    """The ingress route into a managed worker. A removed folder is refused before a worker exists,
    like the in-process runner: a worker failing in its policy scope reads as a LOST execution
    (``unknown``) and parks every follower behind it."""
    if binding is None:
        # A managed child is an I04->I16 producer.  There is no safe legacy
        # untracked fallback once the managed policy has selected this route.
        from gateway.runtime_service_custody import ServiceCustodyError
        raise ServiceCustodyError('preservation_owner_unmapped')
    from gateway.session_policy import workspace_ready, workspace_unavailable_result
    if not workspace_ready(policy):
        result = workspace_unavailable_result(policy)
        authority.pending_results[row['admission_id']] = {'result': result, 'usage': {}}
        return result['final_response']
    return await execute_managed(authority, ref, row, policy, binding)


async def _prepare_managed_execution(authority, ref, row, policy):
    """Run pre-spawn work and return an early response or the child text."""
    from gateway.session_context_refs import managed_turn_text, refused_turn_result

    _mark_spawning(authority, ref, row)
    try:
        text, refusal = await managed_turn_text(authority, row, policy)
    except BaseException:  # health: allow BLE001 -- pre-spawn faults retain the exact admission boundary
        authority._managed_spawning.pop(ref.session_id, None)
        raise
    if refusal is not None:
        authority._managed_spawning.pop(ref.session_id, None)
        authority.pending_results[row['admission_id']] = {
            'result': refused_turn_result(row, refusal), 'usage': {},
        }
        return refusal, None
    if _stopped_before_spawn(authority, ref, row):
        return '', None
    return None, text


async def execute_managed(authority, ref, row, policy, binding=None):
    if binding is None:
        # Keep direct/internal callers fail-closed too; the public ingress normally
        # rejects this earlier, but no caller may reach preflight or Popen without
        # the authenticated I04 binding.
        from gateway.runtime_service_custody import ServiceCustodyError
        raise ServiceCustodyError('preservation_owner_unmapped')
    from gateway.run_turn_progress import publish_worker_tool_event
    early_response, text = await _prepare_managed_execution(authority, ref, row, policy)
    if early_response is not None:
        return early_response
    custody = None
    worker = None
    process = None
    worker_closed = False
    from gateway.session_managed_worker_custody import (
        attach_bootstrap_metadata, retain_after_failure, reserve_execution_custody,
        settle_managed_worker,
    )
    custody = reserve_execution_custody(authority, binding)
    spawned = await _spawn_registered_worker(authority, ref, row, custody)
    if spawned is None:
        retain_after_failure(custody, None)
        return ''
    worker, process = spawned
    workers = authority._managed_workers
    # A Stop acknowledged during the env/spawn awaits found no worker and was latched for this
    # generation; consume it in the same step that makes the worker reachable to interrupt_managed.
    authority.adopt_agent(ref.session_id, row['generation'], worker)
    accepted = usage = None
    scope = None
    try:
        # The interpreter behind the handle introduces itself first; the owner verifies that
        # identity (alive, same birth, descends from the handle) before reserving for it.
        hello = await worker.next_frame(HELLO_SECONDS, ack=0)
        if worker.stop.is_set():
            # Stopped before the child could receive controls: hello raced the latch; never bootstrap.
            raise RuntimeStoreError('managed_worker_stopped')
        # psutil + BEGIN IMMEDIATE: off-loop and retirement-tracked, like admission and claim.
        from gateway.session_runtime_workers import tracked_write
        scope = await tracked_write(authority, partial(reserve_admission_worker, authority,
                    admission_id=row['admission_id'], process=process, principal_id=row['principal_id'], hello=hello))
        worker.worker = (scope['pid'], scope['birth'])
        # The child reads nothing else until the exact reservation has committed.
        frame = await asyncio.to_thread(_bootstrap, authority, ref, row, policy, scope, text)
        attach_bootstrap_metadata(custody, frame, process, scope)
        worker.secrets.extend(v for v in (frame['api_key'], scope['secret']) if isinstance(v, str) and v)
        await asyncio.to_thread(worker.send, frame)
        worker.writer.start()
        while True:
            frame = await worker.next_frame(None, ack=STOP_ACK_SECONDS)
            # The claim's execution stamp, not a ledger read per frame (publish_execution fences the same way).
            if authority.executing.get(ref.session_id) != row['generation']:
                raise RuntimeStoreError('stale_generation')
            with authority.sessions[ref.session_id].event_stream.lock:
                if _prompt_frame(authority, ref, row, worker, frame):
                    continue
            if frame == {'type': 'error', 'reason': 'managed_worker_failed'}:
                raise RuntimeStoreError('managed_worker_failed')
            kind = frame.get('type')
            if kind == 'ready' and set(frame) == {'type', 'pid'} and frame['pid'] == scope['pid']:
                continue
            if kind == 'delta' and set(frame) == {'type', 'text'} and isinstance(frame['text'], str):
                authority.publish_execution(ref.session_id, row['generation'], 'message.delta', {'text': frame['text']})
                continue
            if publish_worker_tool_event(authority, ref.session_id, row['generation'], frame):
                continue
            if kind == 'result' and set(frame) == {'type', 'result'} and accepted is None:
                try:
                    accepted, usage = accept_result(frame['result'])
                except ValueError as exc:
                    raise RuntimeStoreError('invalid_worker_result') from exc
                authority.sessions[ref.session_id].controls.snapshot(ref.session_id, None)
                await asyncio.to_thread(worker.send, {'type': 'finish'})
                continue
            if (kind == 'finished'
                    and set(frame).issubset({'type', 'managed_durable_proof'})
                    and accepted is not None):
                # 'finished' follows the worker's durable execution.finish: the turn is complete.
                # A slow or non-zero interpreter exit after it is close()'s cleanup, not a loss.
                proof = frame.get('managed_durable_proof') if isinstance(frame, dict) else None
                # The receipt remains attached to the same process/readers until
                # native scope close is observable.  The authority finalizer later
                # performs complete_delivery after I04 terminalization.
                await asyncio.to_thread(
                    _advance_managed_compression_route,
                    authority, ref, row, scope, proof, binding=binding, custody=custody,
                )
                await settle_managed_worker(worker, process, custody, proof)
                worker_closed = True
                accepted = _with_stop_cause(accepted, worker)
                # Like in-process execution, settlement belongs to the drain's stream lock.
                authority.pending_results[row['admission_id']] = {
                    'result': {**accepted, 'local_viewer': True}, 'usage': usage,
                    **await asyncio.to_thread(_settled_session_usage, authority, scope['session_id'], accepted, scope['execution_id'])}
                return accepted['final_response']
            raise RuntimeStoreError('invalid_worker_frame')
    except (Exception, asyncio.CancelledError) as exc:
        import logging
        logging.getLogger(__name__).warning('Managed worker lost: %s',
            exc.reason if isinstance(exc, RuntimeStoreError) else type(exc).__name__)
        if scope is None:
            retain_after_failure(custody, scope)
            if isinstance(exc, asyncio.CancelledError) or (
                    isinstance(exc, RuntimeStoreError) and exc.reason == 'managed_worker_stopped'):
                # Stopped before the child ever received its bootstrap: nothing executed, so
                # this settles like an ordinary interrupted turn (the finally kills the child).
                return _interrupted_before_bootstrap(authority, row, worker.stop_cause)
            raise
        # The I04 row may already have accepted the managed effect.  Any
        # bootstrap/reader/result failure after that boundary retains the
        # exact I16 objects as UNKNOWN before the ordinary worker finalizer.
        retain_after_failure(custody, scope)
        from gateway.session_worker_reservation import lose_admission_worker
        from gateway.session_runtime_workers import tracked_write
        await tracked_write(authority, partial(lose_admission_worker, authority, row, scope))
        live = authority.sessions[ref.session_id]
        with live.event_stream.lock:
            live.controls.snapshot(ref.session_id, None)
            authority._publish_pending(ref)
            live.event_stream.publish(ref.session_id, {'text': 'Worker execution is unknown.',
                'content': 'Worker execution is unknown.', 'admission_id': row['admission_id'], 'outcome': 'unknown',
                'status': 'error'})  # the message.complete TurnStatus: never painted as a normal finish
        waiter = authority.waiters.pop(row['admission_id'], None)
        if waiter is not None and not waiter.done():
            waiter.set_result('Worker execution is unknown.')
        # Stop this drain without its ordinary Exception→failed settlement. The
        # committed unknown row deliberately pauses every accepted follower.
        raise ManagedExecutionUnknown('managed_worker_unknown') from exc
    finally:
        workers.pop(ref.session_id, None)
        if worker is not None and not worker_closed:
            await asyncio.to_thread(worker.close)
