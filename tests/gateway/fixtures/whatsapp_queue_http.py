"""Authenticated queue/binder fixture for real WhatsApp source-close coverage.

The child process runs the production inbound queue, HTTP routes, producer lifecycle binder,
and wire-event enqueue seam.  Python only supplies the private authority through the managed
adapter bootstrap path and drives Baileys-shaped lifecycle observations through the binder.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
from typing import Any
from uuid import uuid4

from tests.waiting import async_wait_until


BRIDGE_DIR = Path(__file__).resolve().parents[3] / "scripts" / "whatsapp-bridge"


_NODE_AUTHENTICATED_QUEUE = r"""
import express from 'express';
import http from 'node:http';
import { createInterface } from 'node:readline';
import { mkdirSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { EventEmitter } from 'node:events';
import { InboundQueue, mountInboundQueueRoutes } from './inbound_queue.js';
import { bindInboundProducerLifecycle, enqueueInboundWireEvent } from './bridge_helpers.js';

const port = Number(process.argv.at(-4));
const sessionDir = process.argv.at(-3);
const readyPath = process.argv.at(-2);
const resultDir = process.argv.at(-1);
mkdirSync(resultDir, { recursive: true });

let queue = null;
let server = null;
let serverClosePromise = null;
let producerEvents = null;
let producerEmitter = null;
let connectionState = 'connecting';
let started = false;
const MAX_COMMAND_BYTES = 1024 * 1024;

function queueBinding() {
  if (!queue) {
    return {
      session_path: sessionDir,
      listener_ready: false,
      owner_ready: false,
    };
  }
  const health = queue.health();
  return {
    session_path: sessionDir,
    queue_epoch: queue.queueEpoch,
    bridge_instance: queue.bridgeInstance,
    owner_epoch: queue.ownerEpoch,
    capability_required: queue.capabilityRequired,
    owner_pid: process.pid,
    owner_start: health.owner_start,
    process_start: health.process_start,
    canonical_port: health.canonical_port,
    session_digest: health.session_digest,
    profile_digest: health.profile_digest,
    script_sha256: health.script_sha256,
    attach_key_id: health.attach_key_id,
    source_witness_key_id: health.source_witness_key_id,
    listener_ready: health.listener_ready,
    owner_ready: health.owner_ready,
  };
}

function writeCommandResult(command, value) {
  if (!command?.result_path) return;
  const temporaryPath = `${command.result_path}.tmp-${process.pid}`;
  try {
    writeFileSync(temporaryPath, JSON.stringify(value));
    renameSync(temporaryPath, command.result_path);
  } catch (error) {
    try {
      unlinkSync(temporaryPath);
    } catch (_cleanupError) {
      // Preserve the original publication error for the command owner.
    }
    throw error;
  }
}

function writeReady(value) {
  const temporaryPath = `${readyPath}.tmp-${process.pid}`;
  try {
    writeFileSync(temporaryPath, JSON.stringify(value));
    renameSync(temporaryPath, readyPath);
  } catch (error) {
    try {
      unlinkSync(temporaryPath);
    } catch (_cleanupError) {
      // Preserve the original publication error for the fixture owner.
    }
    throw error;
  }
}

function closeServerBounded() {
  if (serverClosePromise) return serverClosePromise;
  serverClosePromise = new Promise((resolve) => {
    if (!server || !server.listening) {
      resolve();
      return;
    }
    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(settle, 1000);
    try {
      server.close(settle);
    } catch (_error) {
      settle();
    }
  });
  return serverClosePromise;
}

async function start(bootstrap) {
  const eventEmitter = new EventEmitter();
  const app = express();
  app.use(express.json());
  app.get('/health', (req, res) => {
    if (!queue) {
      return res.status(503).json({
        status: 'startup_pending',
        queue: { listener_ready: false, owner_ready: false },
      });
    }
    let snapshot;
    try {
      snapshot = queue.health();
    } catch (error) {
      return res.status(503).json({ status: 'storage_fault', queue: { storage_fault: error?.code || 'storage_fault' } });
    }
    return res.json({
      status: connectionState === 'open' ? 'connected' : connectionState,
      queueLength: snapshot.pending || 0,
      queue: snapshot,
    });
  });
  app.post('/test/enqueue', async (req, res) => {
    try {
      const result = await enqueueInboundWireEvent(
        queue,
        req.body.payload,
        req.body.wire_identity,
        req.body.request_id,
      );
      if (req.body.drop_response === true) {
        // The production enqueue commits first; this seam only drops the
        // transport response so the adapter must recover the durable result.
        res.socket.destroy();
        return;
      }
      if (req.body.truncate_response === true) {
        // Send committed headers and a partial JSON body, then lose the transport.
        // The adapter must recover this exact request by its durable request_id.
        const body = JSON.stringify(result);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.flushHeaders();
        res.write(body.slice(0, -1), () => res.socket.destroy());
        return;
      }
      return res.json(result);
    } catch (error) {
      return res.status(error.status || 503).json({ error: error.code || 'storage_fault' });
    }
  });
  mountInboundQueueRoutes(app, () => queue, { binding: () => queueBinding() });
  server = http.createServer(app);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  try {
    queue = new InboundQueue({
      sessionDir,
      owner: {
        pid: process.pid,
        startedAt: bootstrap.process_start,
        canonicalPort: port,
        sessionPath: sessionDir,
        scriptHash: bootstrap.script_sha256,
        processStart: bootstrap.process_start,
        sessionDigest: bootstrap.session_digest,
        profileDigest: bootstrap.profile_digest,
        attachKeyId: bootstrap.key_id,
        sourceWitnessKeyId: bootstrap.source_witness_key_id,
        startupMode: bootstrap.startup_mode || 'fresh',
        predecessorDigest: bootstrap.predecessor_digest || null,
        adoptionNonce: bootstrap.adoption_nonce || null,
        capability: bootstrap.attach_key_b64url,
        witnessKey: bootstrap.source_witness_key_b64url,
        capabilityRequired: true,
        holdOwnerLock: true,
        listenerReady: true,
        ownerReady: true,
      },
    });
    producerEmitter = eventEmitter;
    producerEvents = bindInboundProducerLifecycle(queue, eventEmitter);
  } catch (error) {
    await closeServerBounded();
    throw error;
  }
  const health = queue.health();
  writeReady({
    base_url: `http://127.0.0.1:${server.address().port}`,
    session_path: sessionDir,
    queue_epoch: queue.queueEpoch,
    bridge_instance: queue.bridgeInstance,
    owner_epoch: queue.ownerEpoch,
    process_start: health.process_start,
    owner_start: health.owner_start,
  });
  started = true;
}

async function handle(command) {
  if (command?.connection && producerEmitter) {
    connectionState = command.connection;
    // The only readiness transition is the real binder's EventEmitter seam.
    // This is a Baileys-shaped connection.update observation, never a direct
    // queue readiness mutation.
    producerEmitter.emit('connection.update', { connection: command.connection });
    return;
  }
  if (command?.wire) {
    try {
      const result = await enqueueInboundWireEvent(
        queue,
        command.wire.payload,
        command.wire.identity,
        command.wire.request_id,
      );
      const item = result?.item || result || {};
      writeCommandResult(command, {
        status: result?.status || 'queued',
        item_id: item.item_id || null,
        sequence: item.sequence || null,
        wire_identity: item.wire_identity || command.wire.identity,
      });
    } catch (error) {
      writeCommandResult(command, { status: 'error', error: error?.code || 'enqueue_failed' });
    }
    return;
  }
  if (command?.stop) {
    await closeServerBounded();
    process.exit(0);
  }
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
let bootstrapped = false;
let commandTail = Promise.resolve();

async function processLine(line) {
  let command = null;
  try {
    if (Buffer.byteLength(line, 'utf8') > MAX_COMMAND_BYTES) {
      const error = new Error('fixture command is too large');
      error.code = 'command_too_large';
      throw error;
    }
    command = JSON.parse(line);
    if (!bootstrapped) {
      bootstrapped = true;
      await start(command);
      return;
    }
    await handle(command);
  } catch (error) {
    if (!started || !command) {
      await closeServerBounded();
      process.exit(1);
      return;
    }
    try {
      writeCommandResult(command, { status: 'error', error: error?.code || 'fixture_failed' });
    } catch (_publicationError) {
      await closeServerBounded();
      process.exit(1);
    }
  }
}

rl.on('line', (line) => {
  rl.pause();
  commandTail = commandTail
    .then(() => processLine(line))
    .catch(async () => {
      await closeServerBounded();
      process.exit(1);
    })
    .finally(() => rl.resume());
});
process.on('SIGTERM', () => {
  closeServerBounded().then(() => process.exit(0));
});
"""


def reserve_port() -> int:
    """Reserve a loopback port for one fixture profile; the child binds it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _spawn_queue_process(
    command: list[str], *, cwd: Path, env: dict[str, str],
) -> subprocess.Popen[str]:
    """Spawn the exact owned child synchronously so cancellation cannot orphan it."""
    return subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )


class WhatsAppQueueHttpFixture:
    """A production queue/binder HTTP owner attached through WhatsAppAdapter."""

    def __init__(self, adapter: Any, *, tmp_path: Path, name: str):
        self.adapter = adapter
        self.tmp_path = Path(tmp_path)
        self.name = name
        self.process: subprocess.Popen[str] | None = None
        self.ready_path = self.tmp_path / f"{name}-ready.json"
        self.result_dir = self.tmp_path / f"{name}-results"
        self.base_url = ""
        self._stdin_lock = asyncio.Lock()
        self._closed = False
        self._authority: dict[str, Any] | None = None
        self._io_tasks: set[asyncio.Task[Any]] = set()
        self._pending_adapter_tasks: set[asyncio.Task[Any]] = set()
        self._pending_session_closes: dict[asyncio.Task[Any], Any] = {}

    @classmethod
    async def start(cls, adapter: Any, tmp_path: Path, *, name: str = "whatsapp"):
        fixture = cls(adapter, tmp_path=tmp_path, name=name)
        try:
            await fixture._start_child()
        except BaseException:
            try:
                await fixture.close()
            except BaseException:
                pass
            raise
        return fixture

    async def _start_child(self) -> None:
        from gateway.status import get_process_start_time
        from hermes_cli._subprocess_compat import resolve_node_command
        from plugins.platforms.whatsapp.bridge_ownership import read_bridge_handoff

        self.ready_path.unlink(missing_ok=True)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        port = int(self.adapter._bridge_port)
        self.process = _spawn_queue_process(
            resolve_node_command(
                "node",
                [
                    "--input-type=module",
                    "-e",
                    _NODE_AUTHENTICATED_QUEUE,
                    str(port),
                    str(Path(self.adapter._session_path).resolve()),
                    str(self.ready_path),
                    str(self.result_dir),
                ],
            ),
            cwd=BRIDGE_DIR,
            env=dict(os.environ, NODE_NO_WARNINGS="1"),
        )
        process_start = get_process_start_time(self.process.pid)
        if not isinstance(process_start, int) or process_start < 1:
            raise RuntimeError("queue fixture process start is unavailable")
        self._authority = self.adapter._load_bridge_authority(process_start)
        if not isinstance(self._authority, dict):
            raise RuntimeError("managed queue fixture authority is unavailable")
        handoff = read_bridge_handoff(self.adapter._session_path)
        startup_mode = "adopt" if isinstance(handoff, dict) and handoff.get("state") == "pending" else "fresh"
        bootstrap = {
            "startup_mode": startup_mode,
            "predecessor_digest": handoff.get("predecessor_digest") if startup_mode == "adopt" else None,
            "adoption_nonce": handoff.get("adoption_nonce") if startup_mode == "adopt" else None,
            "session_digest": self._authority["session_digest"],
            "profile_digest": self._authority["profile_digest"],
            "canonical_port": self._authority["canonical_port"],
            "script_sha256": self._authority["script_sha256"],
            "process_start": process_start,
            "key_id": self._authority["attach_key_id"],
            "attach_key_b64url": self._authority["attach_key_b64url"],
            "source_witness_key_id": self._authority["source_witness_key_id"],
            "source_witness_key_b64url": self._authority["source_witness_key_b64url"],
        }
        await self._send_command(bootstrap)
        metadata = await self._wait_for_ready()
        self.base_url = str(metadata["base_url"])
        self.adapter._attach_to_bridge(self.process, start_polling=False)
        if not await self.adapter._prepare_spawned_bridge_startup(self._authority, startup_mode):
            raise RuntimeError("managed queue fixture owner adoption was not authenticated")
        # Mirror connect(): polling is a connected-adapter lifecycle, not merely
        # an attached HTTP session.  Without this existing state transition,
        # _poll_messages() exits at its _running guard before claiming the wire.
        self.adapter._mark_connected()
        self.adapter._start_bridge_polling()
        binding = await self.adapter._ensure_queue_binding()
        if not isinstance(binding, dict):
            raise RuntimeError("managed queue fixture binding is unavailable")

    @staticmethod
    def _read_complete_json(path: Path, required: tuple[str, ...]) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(value, dict) or any(
            key not in value or value[key] is None for key in required
        ):
            return None
        return value

    async def _wait_for_ready(self) -> dict[str, Any]:
        def _ready_metadata() -> dict[str, Any] | None:
            metadata = self._read_complete_json(
                self.ready_path,
                (
                    "base_url", "session_path", "queue_epoch", "bridge_instance",
                    "owner_epoch", "process_start", "owner_start",
                ),
            )
            if (
                metadata is not None
                and isinstance(metadata.get("base_url"), str)
                and metadata["base_url"]
            ):
                return metadata
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(f"queue fixture exited with {self.process.returncode}")
            return None

        return await async_wait_until(
            _ready_metadata,
            timeout=10,
            what="authenticated WhatsApp queue HTTP binding",
        )

    async def _send_command(self, command: dict[str, Any]) -> None:
        line = json.dumps(command, separators=(",", ":")) + "\n"
        async with self._stdin_lock:
            process = self.process
            if process is None or process.stdin is None:
                raise RuntimeError("queue fixture stdin is closed")

            def _write_line() -> None:
                process.stdin.write(line)
                process.stdin.flush()

            task = asyncio.create_task(asyncio.to_thread(_write_line))
            self._io_tasks.add(task)
            done, _pending = await asyncio.wait({task}, timeout=5)
            if not done:
                await self._terminate_child()
                raise TimeoutError("queue fixture stdin write timed out")
            try:
                task.result()
            finally:
                self._io_tasks.discard(task)

    async def health(self) -> dict[str, Any]:
        status, body = await self.adapter._bridge_json("get", "health", timeout=5)
        if status != 200 or not isinstance(body, dict):
            raise AssertionError(f"queue health failed: {status}")
        return body

    async def queue_request(
        self, method: str, route: str, *, payload: dict[str, Any] | None = None,
        bind: bool = True,
    ) -> tuple[int, Any]:
        """Drive an authenticated child-process HTTP route through the adapter client."""
        body = dict(payload or {})
        if bind:
            binding = await self.adapter._ensure_queue_binding()
            if not isinstance(binding, dict):
                raise RuntimeError("managed queue fixture binding is unavailable")
            body.setdefault("binding", binding)
        return await self.adapter._bridge_json(
            method, route.lstrip("/"), timeout=5, payload=body if body else None,
        )

    async def enqueue_http(
        self, payload: dict[str, Any], identity: dict[str, Any], *, request_id: str,
        truncate_response: bool = False,
    ) -> dict[str, Any] | None:
        request = {
            "request_id": request_id,
            "payload": payload,
            "wire_identity": identity,
        }
        if truncate_response:
            request["truncate_response"] = True
        return await self.adapter._bridge_operation(
            "test/enqueue", request,
            timeout=5,
        )

    async def enqueue_http_raw(
        self, payload: dict[str, Any], identity: dict[str, Any], *, request_id: str,
    ) -> tuple[int, Any] | None:
        import aiohttp

        try:
            return await self.adapter._bridge_json(
                "post",
                "test/enqueue",
                timeout=5,
                payload={
                    "request_id": request_id,
                    "payload": payload,
                    "wire_identity": identity,
                    "drop_response": True,
                },
            )
        except (aiohttp.ClientError, TimeoutError, OSError, RuntimeError):
            return None

    async def wait_for_producer(self, state: str) -> dict[str, Any]:
        request: asyncio.Task[dict[str, Any]] | None = None

        def _producer_snapshot() -> dict[str, Any] | None:
            nonlocal request
            if request is None:
                request = asyncio.create_task(self.health())
            if not request.done():
                return None
            body = request.result()
            request = None
            queue = body.get("queue")
            if isinstance(queue, dict) and queue.get("producer_state") == state:
                return body
            return None

        try:
            return await async_wait_until(
                _producer_snapshot,
                timeout=5,
                what=f"producer state {state}",
            )
        finally:
            if request is not None and not request.done():
                request.cancel()
                await asyncio.gather(request, return_exceptions=True)

    async def open(self) -> dict[str, Any]:
        await self._send_command({"connection": "open"})
        return await self.wait_for_producer("connected")

    async def disconnect(self) -> dict[str, Any]:
        await self._send_command({"connection": "close"})
        return await self.wait_for_producer("disconnected")

    async def enqueue_wire(
        self, payload: dict[str, Any], identity: dict[str, Any], *, request_id: str | None = None,
    ) -> dict[str, Any]:
        request_id = request_id or uuid4().hex
        result_path = self.result_dir / f"{request_id}.json"
        # A same-ID retry must not read the previous command's published result
        # before the child has processed this new wire identity.  The durable
        # queue operation remains the source of truth and will replay or reject
        # the request after this observation file is removed.
        result_path.unlink(missing_ok=True)
        await self._send_command({
            "wire": {
                "payload": payload,
                "identity": identity,
                "request_id": request_id,
            },
            "result_path": str(result_path),
        })
        def _wire_result() -> dict[str, Any] | None:
            result = self._read_complete_json(result_path, ("status",))
            if result is None:
                return None
            if result.get("status") == "error":
                return result if result.get("error") else None
            if any(
                key not in result or result[key] is None
                for key in ("item_id", "sequence", "wire_identity")
            ):
                return None
            return result

        return await async_wait_until(
            _wire_result,
            timeout=5,
            what="production wire enqueue acknowledgement",
        )

    async def foreign_capability_status(self, foreign: WhatsAppQueueHttpFixture) -> int:
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/messages",
                headers={"X-Hermes-Bridge-Capability": foreign.adapter._bridge_capability},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                await response.read()
                return response.status

    async def restart(self) -> dict[str, Any]:
        errors: list[BaseException] = []
        try:
            await self._detach_adapter()
        except BaseException as error:
            errors.append(error)
        try:
            await self._terminate_child()
        except BaseException as error:
            errors.append(error)
        try:
            errors.extend(await self._reap_pending_cleanup())
        except BaseException as error:
            errors.append(error)
        if errors:
            raise errors[0]
        self._closed = False
        self.ready_path.unlink(missing_ok=True)
        await self._start_child()
        return await self.health()

    async def _reap_pending_cleanup(self) -> list[BaseException]:
        errors: list[BaseException] = []
        pending_tasks = set(self._pending_adapter_tasks)
        pending_tasks.update(self._pending_session_closes)
        pending_tasks.update(self._io_tasks)
        if pending_tasks:
            await asyncio.wait(pending_tasks, timeout=1)

        for task in tuple(self._pending_adapter_tasks):
            if not task.done():
                continue
            self._pending_adapter_tasks.discard(task)
            if getattr(self.adapter, "_poll_task", None) is task:
                self.adapter._poll_task = None
            if not task.cancelled():
                try:
                    task.result()
                except BaseException as error:
                    errors.append(error)

        for task, session in tuple(self._pending_session_closes.items()):
            if not task.done():
                continue
            self._pending_session_closes.pop(task, None)
            if not task.cancelled():
                try:
                    task.result()
                except BaseException as error:
                    errors.append(error)
            if getattr(session, "closed", False) and getattr(self.adapter, "_http_session", None) is session:
                self.adapter._http_session = None

        for task in tuple(self._io_tasks):
            if not task.done():
                continue
            self._io_tasks.discard(task)
            if not task.cancelled():
                try:
                    task.result()
                except BaseException as error:
                    errors.append(error)
        return errors

    async def _detach_adapter(self) -> None:
        errors: list[BaseException] = []
        task = getattr(self.adapter, "_poll_task", None)
        if task is not None:
            try:
                if task.done():
                    self.adapter._poll_task = None
                else:
                    task.cancel()
                    done, pending = await asyncio.wait({task}, timeout=2)
                    if pending:
                        self._pending_adapter_tasks.add(task)
                        errors.append(TimeoutError("WhatsApp poll task cancellation timed out"))
                    else:
                        self.adapter._poll_task = None
                        if task in done and not task.cancelled():
                            task.exception()
            except BaseException as error:
                errors.append(error)
                if not task.done():
                    self._pending_adapter_tasks.add(task)
        session = getattr(self.adapter, "_http_session", None)
        if session is not None:
            if getattr(session, "closed", False):
                self.adapter._http_session = None
            elif any(value is session for value in self._pending_session_closes.values()):
                errors.append(TimeoutError("WhatsApp HTTP session close is still pending"))
            else:
                close_task = asyncio.create_task(session.close())
                self._pending_session_closes[close_task] = session
                try:
                    done, pending = await asyncio.wait({close_task}, timeout=2)
                    if pending:
                        errors.append(TimeoutError("WhatsApp HTTP session close timed out"))
                    else:
                        self._pending_session_closes.pop(close_task, None)
                        if close_task in done and not close_task.cancelled():
                            close_task.result()
                        if getattr(session, "closed", False):
                            self.adapter._http_session = None
                        else:
                            errors.append(RuntimeError("WhatsApp HTTP session remained open"))
                except BaseException as error:
                    errors.append(error)
        self.adapter._bridge_binding = None
        if errors:
            raise errors[0]

    async def _terminate_child(self) -> None:
        process = self.process
        if process is None:
            return
        try:
            if process.poll() is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.to_thread(process.wait, 5)
            except subprocess.TimeoutExpired:
                if process.poll() is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                await asyncio.to_thread(process.wait, 2)
        finally:
            if process.poll() is not None:
                if process.stdin is not None:
                    try:
                        process.stdin.close()
                    except OSError:
                        pass
                self.process = None

    async def close(self) -> None:
        session = getattr(self.adapter, "_http_session", None)
        if self._closed and self.process is None and (session is None or session.closed):
            return
        errors: list[BaseException] = []
        try:
            await self._detach_adapter()
        except BaseException as error:
            errors.append(error)
        try:
            await self._terminate_child()
        except BaseException as error:
            errors.append(error)
        try:
            errors.extend(await self._reap_pending_cleanup())
        except BaseException as error:
            errors.append(error)
        session = getattr(self.adapter, "_http_session", None)
        self._closed = (
            self.process is None
            and (session is None or session.closed)
            and not self._pending_adapter_tasks
            and not self._pending_session_closes
            and not self._io_tasks
        )
        if errors:
            raise errors[0]
