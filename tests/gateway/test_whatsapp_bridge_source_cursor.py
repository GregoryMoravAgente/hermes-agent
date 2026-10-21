"""Consumer-level cursor checks against the real Node WhatsApp queue HTTP seam.

The harness starts the source queue module with its actual Express routes.  Python only supplies
wire payloads and consumer requests, so a fake list or a direct Python queue twin cannot make the
positive path pass.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest


BRIDGE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "whatsapp-bridge"

_NODE_QUEUE_SERVER = r"""
import express from 'express';
import http from 'node:http';
import { writeFileSync } from 'node:fs';
import { InboundQueue, mountInboundQueueRoutes } from './inbound_queue.js';

const sessionDir = process.argv.at(-2);
const readyPath = process.argv.at(-1);
const queue = new InboundQueue({ sessionDir, owner: {
  pid: process.pid,
  startedAt: new Date().toISOString(),
  holdOwnerLock: true,
} });
const app = express();
app.use(express.json());
const binding = () => ({
  session_path: queue.sessionDir,
  queue_epoch: queue.queueEpoch,
  bridge_instance: queue.bridgeInstance,
  owner_epoch: queue.ownerEpoch,
});
app.get('/health', (req, res) => res.json(queue.health()));
app.post('/test/enqueue', (req, res) => {
  try {
    const result = queue.enqueue(req.body.payload, {
      requestId: req.body.request_id,
      wireIdentity: req.body.wire_identity,
    });
    if (req.body.drop_response === true) {
      // Commit the real queue operation, then close the transport before any
      // response bytes reach the client.  The retry must use the durable
      // request record rather than inventing a second enqueue.
      res.socket.destroy();
      return;
    }
    return res.json(result);
  } catch (error) {
    return res.status(error.status || 503).json({ error: error.code || 'storage_fault' });
  }
});
mountInboundQueueRoutes(app, queue, { binding });
const server = http.createServer(app);
server.listen(0, '127.0.0.1', () => {
  const health = queue.health();
  writeFileSync(readyPath, JSON.stringify({
    base_url: `http://127.0.0.1:${server.address().port}`,
    session_path: queue.sessionDir,
    queue_epoch: queue.queueEpoch,
    bridge_instance: queue.bridgeInstance,
    owner_epoch: queue.ownerEpoch,
    process_start: health.process_start,
    owner_start: health.owner_start,
  }));
});
process.on('SIGTERM', () => server.close(() => process.exit(0)));
"""


def _request(
    base_url: str, path: str, method: str = "GET", payload: dict | None = None,
    extra_headers: dict[str, str] | None = None,
):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        f"{base_url}{path}",
        data=body,
        method=method,
        headers={"content-type": "application/json", **(extra_headers or {})},
    )
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def _request_with_response_loss(base_url: str, path: str, method: str, payload: dict):
    """Return no result when the server commits then drops the HTTP connection."""
    try:
        return _request(base_url, path, method, payload)
    except (URLError, TimeoutError, OSError):
        return None


def _start_node_queue(tmp_path: Path):
    ready = tmp_path / "ready.json"
    session = tmp_path / "session"
    session.mkdir(exist_ok=True)
    ready.unlink(missing_ok=True)
    from hermes_cli._subprocess_compat import resolve_node_command

    process = subprocess.Popen(
        resolve_node_command("node", ["--input-type=module", "-e", _NODE_QUEUE_SERVER, str(session), str(ready)]),
        cwd=BRIDGE_DIR,
        env=dict(os.environ, NODE_NO_WARNINGS="1"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            if ready.exists():
                return process, json.loads(ready.read_text(encoding="utf-8")), session
            if process.poll() is not None:
                raise AssertionError(f"Node queue server exited with {process.returncode}")
            time.sleep(0.02)
        raise AssertionError("Node queue server did not publish its HTTP binding")
    except BaseException:
        process.terminate()
        process.wait(timeout=5)
        raise


def _stop_node_queue(process):
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=5)


def _consumer_binding(metadata: dict, consumer_id: str = "python-consumer", epoch: int = 1):
    return {
        "session_path": metadata["session_path"],
        "queue_epoch": metadata["queue_epoch"],
        "bridge_instance": metadata["bridge_instance"],
        "owner_epoch": metadata["owner_epoch"],
        "process_start": metadata["process_start"],
        "consumer_id": consumer_id,
        "consumer_epoch": epoch,
        "profile": "default",
    }


@pytest.mark.platforms("posix")
def test_real_node_queue_keeps_more_than_100_wire_items_and_closes_with_retained_backlog(tmp_path):
    process, metadata, _session = _start_node_queue(tmp_path)
    try:
        base_url = metadata["base_url"]
        binding = _consumer_binding(metadata)
        for index in range(101):
            status, body = _request(base_url, "/test/enqueue", "POST", {
                "request_id": f"enqueue-{index}",
                "payload": {"messageId": f"wire-{index}", "chatId": "chat@s.whatsapp.net", "body": f"body-{index}"},
                "wire_identity": {"chatId": "chat@s.whatsapp.net", "messageId": f"wire-{index}"},
            })
            assert status == 200, body
        status, health = _request(base_url, "/health")
        assert status == 200 and health["pending"] == 101

        status, claim = _request(base_url, "/messages/claim", "POST", {
            "request_id": "claim-first", "binding": binding, "limit": 1,
        })
        assert status == 200 and claim["status"] == "claimed"
        assert claim["item"]["sequence"] == 1
        assert len(claim["custody_ref"]) > 16
        _status, retry = _request(base_url, "/messages/claim", "POST", {
            "request_id": "claim-first", "binding": binding, "limit": 1,
        })
        assert retry == claim

        transition = {
            "binding": binding,
            "item_id": claim["item"]["item_id"],
            "sequence": claim["item"]["sequence"],
            "claim_id": claim["claim_id"],
            "expected_state": "claimed",
            "source_identity": [["chat_id", "chat@s.whatsapp.net"], ["message_id", "wire-0"]],
            "registration_generation": 1,
            "custody_ref": claim["custody_ref"],
        }
        status, accepted = _request(base_url, "/messages/transition", "POST", {
            **transition, "request_id": "accept-first", "op": "accept",
        })
        assert status == 200 and accepted["status"] == "transitioned"
        status, ack = _request(base_url, "/messages/transition", "POST", {
            **transition,
            "request_id": "ack-first",
            "op": "ack",
            "expected_state": "accepted",
            "settlement": {"producer_done": True, "native_exited": True, "transport_settled": True},
        })
        assert status == 200 and ack["status"] == "transitioned"

        status, closing = _request(base_url, "/messages/control", "POST", {
            "request_id": "close-17", "op": "close", "binding": binding, "close_generation": 17,
        })
        assert status == 200 and closing["status"] == "closing"
        assert closing["snapshot"]["preclose_pending"] == 100
        # Closing records the watermark, but validation waits for the
        # authenticated pre-close drain to settle the pending prefix.
        assert closing["snapshot"]["validated"] is False
        status, blocked = _request(base_url, "/messages/claim", "POST", {
            "request_id": "claim-while-closed", "binding": binding, "limit": 1,
        })
        assert status == 200 and blocked["reason"] == "source_closed"
        status, resumed = _request(base_url, "/messages/control", "POST", {
            "request_id": "resume-17", "op": "resume", "binding": binding, "close_generation": 17,
        })
        assert status == 200 and resumed["status"] == "open"
    finally:
        _stop_node_queue(process)


@pytest.mark.platforms("posix")
def test_real_node_queue_restart_keeps_uncertain_claim_held_and_fences_profiles(tmp_path):
    process, metadata, _session = _start_node_queue(tmp_path)
    try:
        base_url = metadata["base_url"]
        binding = _consumer_binding(metadata, "before-restart")
        status, _body = _request(base_url, "/test/enqueue", "POST", {
            "request_id": "restart-enqueue",
            "payload": {"messageId": "restart-wire", "chatId": "chat@s.whatsapp.net", "body": "held"},
            "wire_identity": {"chatId": "chat@s.whatsapp.net", "messageId": "restart-wire"},
        })
        assert status == 200
        status, claim = _request(base_url, "/messages/claim", "POST", {
            "request_id": "restart-claim", "binding": binding, "limit": 1,
        })
        assert status == 200 and claim["status"] == "claimed"
    finally:
        _stop_node_queue(process)

    process, restarted, _ = _start_node_queue(tmp_path)
    try:
        assert restarted["queue_epoch"] == metadata["queue_epoch"]
        assert restarted["owner_epoch"] != metadata["owner_epoch"]
        new_binding = _consumer_binding(restarted, "after-restart")
        status, blocked = _request(restarted["base_url"], "/messages/claim", "POST", {
            "request_id": "new-claim", "binding": new_binding, "limit": 1,
        })
        assert status == 200 and blocked["reason"] == "head_claimed"
        status, result = _request(restarted["base_url"], "/messages/requests/restart-claim")
        assert status == 200 and result["result"]["claim_id"] == claim["claim_id"]
        status, foreign = _request(restarted["base_url"], "/messages/transition", "POST", {
            "request_id": "foreign-release",
            "op": "release",
            "binding": new_binding,
            "item_id": claim["item"]["item_id"],
            "sequence": claim["item"]["sequence"],
            "claim_id": claim["claim_id"],
            "expected_state": "claimed",
            "reason": "runtime_draining",
        })
        assert status == 409 and foreign["error"] == "binding_conflict"
    finally:
        _stop_node_queue(process)


@pytest.mark.platforms("posix")
def test_real_node_queue_http_response_loss_retries_by_request_identity_after_restart(tmp_path):
    process, metadata, _session = _start_node_queue(tmp_path)
    payload = {
        "messageId": "response-loss-wire",
        "chatId": "chat@s.whatsapp.net",
        "body": "response-loss",
    }
    identity = {"messageId": "response-loss-wire", "chatId": "chat@s.whatsapp.net"}
    try:
        status, first = _request(metadata["base_url"], "/test/enqueue", "POST", {
            "request_id": "response-loss-enqueue",
            "payload": payload,
            "wire_identity": identity,
        })
        assert status == 200 and first["status"] == "enqueued"
    finally:
        _stop_node_queue(process)

    process, restarted, _session = _start_node_queue(tmp_path)
    try:
        status, retry = _request(restarted["base_url"], "/test/enqueue", "POST", {
            "request_id": "response-loss-enqueue",
            "payload": payload,
            "wire_identity": identity,
        })
        assert status == 200
        assert retry == first, "a lost HTTP response must replay the durable operation result"

        status, stored = _request(
            restarted["base_url"], "/messages/requests/response-loss-enqueue",
        )
        assert status == 200
        assert stored["status"] == "request_result"
        assert stored["result"] == first

        status, conflict = _request(restarted["base_url"], "/test/enqueue", "POST", {
            "request_id": "response-loss-enqueue",
            "payload": {**payload, "body": "different"},
            "wire_identity": identity,
        })
        assert status == 409
        assert conflict["error"] == "request_conflict"
    finally:
        _stop_node_queue(process)


@pytest.mark.platforms("posix")
def test_real_node_queue_http_postcommit_response_drop_replays_exact_result(tmp_path):
    process, metadata, _session = _start_node_queue(tmp_path)
    payload = {
        "messageId": "postcommit-drop-wire",
        "chatId": "chat@s.whatsapp.net",
        "body": "postcommit-drop",
    }
    identity = {"messageId": "postcommit-drop-wire", "chatId": "chat@s.whatsapp.net"}
    try:
        lost = _request_with_response_loss(metadata["base_url"], "/test/enqueue", "POST", {
            "request_id": "postcommit-drop-enqueue",
            "payload": payload,
            "wire_identity": identity,
            "drop_response": True,
        })
        assert lost is None, "the server must drop the response after committing the request"
        assert process.poll() is None, "response loss must not kill the owning queue process"
    finally:
        _stop_node_queue(process)

    process, restarted, _session = _start_node_queue(tmp_path)
    try:
        status, replay = _request(restarted["base_url"], "/test/enqueue", "POST", {
            "request_id": "postcommit-drop-enqueue",
            "payload": payload,
            "wire_identity": identity,
        })
        assert status == 200 and replay["status"] == "enqueued"
        status, stored = _request(
            restarted["base_url"], "/messages/requests/postcommit-drop-enqueue",
        )
        assert status == 200 and stored["result"] == replay
    finally:
        _stop_node_queue(process)


@pytest.mark.platforms("posix")
def test_real_node_queue_two_profile_idle_close_isolated_by_session_binding(tmp_path):
    first_dir = tmp_path / "profile-default"
    second_dir = tmp_path / "profile-alt"
    first_dir.mkdir()
    second_dir.mkdir()
    first_process, first, _ = _start_node_queue(first_dir)
    second_process, second, _ = _start_node_queue(second_dir)
    try:
        assert first["queue_epoch"] != second["queue_epoch"]
        first_binding = _consumer_binding(first, "default-consumer")
        second_binding = _consumer_binding(second, "alt-consumer")
        status, _body = _request(first["base_url"], "/test/enqueue", "POST", {
            "request_id": "profile-one-wire",
            "payload": {"messageId": "profile-one", "chatId": "one@s.whatsapp.net", "body": "one"},
            "wire_identity": {"chatId": "one@s.whatsapp.net", "messageId": "profile-one"},
        })
        assert status == 200
        status, empty = _request(second["base_url"], "/messages/claim", "POST", {
            "request_id": "profile-two-claim", "binding": second_binding, "limit": 1,
        })
        assert status == 200 and empty["status"] == "empty"
        status, first_close = _request(first["base_url"], "/messages/control", "POST", {
            "request_id": "profile-one-close", "op": "close", "binding": first_binding, "close_generation": 21,
        })
        status_two, second_close = _request(second["base_url"], "/messages/control", "POST", {
            "request_id": "profile-two-close", "op": "close", "binding": second_binding, "close_generation": 22,
        })
        assert status == status_two == 200
        assert first_close["status"] == second_close["status"] == "closing"
        assert first_close["snapshot"]["pending"] == 1
        assert second_close["snapshot"]["pending"] == 0
    finally:
        _stop_node_queue(first_process)
        _stop_node_queue(second_process)



def _json_digest(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _managed_bridge_bootstrap(script: Path, session: Path, port: int, label: str) -> tuple[bytes, str]:
    """Build the real bridge.js managed bootstrap frame with synthetic test capabilities."""
    attach_key = base64.urlsafe_b64encode(
        (f"attach-{label}-" + "x" * 32).encode("utf-8")
    ).rstrip(b"=").decode("ascii")
    witness_key = base64.urlsafe_b64encode(
        (f"witness-{label}-" + "y" * 32).encode("utf-8")
    ).rstrip(b"=").decode("ascii")
    frame = {
        "protocol": "hermes.whatsapp.bridge.bootstrap.v1",
        "session_digest": hashlib.sha256(str(session.resolve()).encode("utf-8")).hexdigest(),
        "profile_digest": hashlib.sha256(b"source-close-r2-profile").hexdigest(),
        "canonical_port": port,
        "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        "startup_mode": "fresh",
        "key_id": hashlib.sha256(attach_key.encode("utf-8")).hexdigest(),
        "attach_key_b64url": attach_key,
        "source_witness_key_id": hashlib.sha256(witness_key.encode("utf-8")).hexdigest(),
        "source_witness_key_b64url": witness_key,
        # Keep this inside the JavaScript safe-integer range and stable per child.
        "process_start": 1_700_000_000 + int.from_bytes(
            hashlib.sha256(label.encode("utf-8")).digest()[:4], "big"
        ) % 1_000_000,
    }
    encoded = json.dumps(frame, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded, attach_key


def _spawn_managed_bridge(session: Path, port: int, label: str):
    from hermes_cli._subprocess_compat import resolve_node_command

    session.mkdir(parents=True, exist_ok=True)
    script = BRIDGE_DIR / "bridge.js"
    frame, capability = _managed_bridge_bootstrap(script, session, port, label)
    process = subprocess.Popen(
        resolve_node_command("node", [
            str(script), "--managed", "--port", str(port), "--session", str(session),
        ]),
        cwd=BRIDGE_DIR,
        env=dict(os.environ, NODE_NO_WARNINGS="1", WHATSAPP_DEBUG="0"),
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert process.stdin is not None
        process.stdin.write(frame)
        process.stdin.close()
    except BaseException:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)
        raise
    return process, capability


def _wait_managed_bridge(process, port: int):
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"real bridge.js exited before health: {process.returncode}")
        try:
            status, body = _request(base_url, "/health")
            queue = body.get("queue") if isinstance(body, dict) else None
            if (status == 200 and isinstance(queue, dict)
                    and queue.get("listener_ready") is True
                    and queue.get("owner_ready") is True):
                return base_url, body
        except (HTTPError, OSError, URLError, TimeoutError, ValueError):
            pass
        time.sleep(0.05)
    raise AssertionError("real managed bridge.js did not publish its health endpoint")


def _stop_managed_bridge(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _owner_fence_from_health(health: dict) -> dict:
    queue = health["queue"]
    fields = {
        "session_digest": queue["session_digest"],
        "profile_digest": queue["profile_digest"],
        "canonical_port": queue["canonical_port"],
        "script_sha256": queue["script_sha256"],
        "process_start": queue["process_start"],
        "queue_epoch": queue["queue_epoch"],
        "bridge_instance": queue["bridge_instance"],
        "owner_epoch": queue["owner_epoch"],
        "attach_key_id": queue["attach_key_id"],
        "source_witness_key_id": queue["source_witness_key_id"],
    }
    return {**fields, "binding_digest": _json_digest(fields)}


def _real_bridge_attach(
    base_url: str, health: dict, capability: str, request_id: str,
    predecessor_digest: str | None = None,
) -> dict:
    fence = _owner_fence_from_health(health)
    status, body = _request(
        base_url,
        "/messages/attach",
        "POST",
        {
            "request_id": request_id,
            "protocol": "hermes.whatsapp.attach.v2",
            "challenge_nonce": f"source-close-r2-{request_id}",
            "predecessor_digest": predecessor_digest,
            "binding_digest": fence["binding_digest"],
        },
        {"x-hermes-bridge-capability": capability},
    )
    assert status == 200, body
    assert body["status"] == "attached"
    return body


def test_real_managed_bridge_listener_failure_does_not_replace_live_owner(tmp_path):
    """A real bridge listener bind failure leaves the already-owned profile untouched."""
    port = _free_loopback_port()
    first_session = tmp_path / "listener-owner"
    second_session = tmp_path / "listener-collision"
    first = second = None
    try:
        first, _capability = _spawn_managed_bridge(first_session, port, "listener-owner")
        first_base, first_health = _wait_managed_bridge(first, port)
        second, _ = _spawn_managed_bridge(second_session, port, "listener-collision")
        deadline = time.monotonic() + 10
        while second.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert second.poll() is not None, "a colliding real bridge listener must fail closed"
        status, health = _request(first_base, "/health")
        assert status == 200
        assert health["queue"]["listener_ready"] is True
        assert health["queue"]["owner_ready"] is True
        assert health["queue"]["owner_epoch"] == first_health["queue"]["owner_epoch"]
    finally:
        if second is not None:
            _stop_managed_bridge(second)
        if first is not None:
            _stop_managed_bridge(first)


def test_real_managed_bridge_adoption_authenticates_first_request_after_restart(tmp_path):
    """The actual managed bridge bootstrap, attach proof, and adoption route form one restart."""
    session = tmp_path / "managed-adoption"
    first_port = _free_loopback_port()
    second_port = _free_loopback_port()
    first = second = None
    try:
        first, first_capability = _spawn_managed_bridge(session, first_port, "adoption-predecessor")
        first_base, first_health = _wait_managed_bridge(first, first_port)
        first_attach = _real_bridge_attach(
            first_base, first_health, first_capability, "adoption-predecessor-attach",
        )
        assert first_attach["binding"]["owner_ready"] is True
        _stop_managed_bridge(first)
        first = None

        second, second_capability = _spawn_managed_bridge(session, second_port, "adoption-successor")
        second_base, second_health = _wait_managed_bridge(second, second_port)
        predecessor = second_health["queue"]["owner_handoff"]
        assert isinstance(predecessor, dict) and predecessor["state"] == "pending"
        attach = _real_bridge_attach(
            second_base, second_health, second_capability, "adoption-successor-attach",
            predecessor_digest=predecessor["predecessor_digest"],
        )
        handoff = attach.get("owner_handoff")
        assert isinstance(handoff, dict) and handoff["state"] == "pending"
        predecessor_digest = handoff["predecessor_digest"]
        adoption_nonce = handoff["adoption_nonce"]
        queue_epoch = second_health["queue"]["queue_epoch"]
        adoption_request = "adopt-" + _json_digest({
            "adoption_nonce": adoption_nonce,
            "predecessor_digest": predecessor_digest,
            "queue_epoch": queue_epoch,
        })
        status, adopted = _request(
            second_base,
            "/messages/adopt",
            "POST",
            {
                "request_id": adoption_request,
                "protocol": "hermes.whatsapp.adoption.v1",
                "predecessor_digest": predecessor_digest,
                "adoption_nonce": adoption_nonce,
                "queue_epoch": queue_epoch,
                "attach_proof": attach["attach_proof"],
            },
            {"x-hermes-bridge-capability": second_capability},
        )
        assert status == 200, adopted
        assert adopted["status"] == "adopted"
        status, snapshot = _request(
            second_base,
            "/messages",
            extra_headers={"x-hermes-bridge-capability": second_capability},
        )
        assert status == 200
        assert snapshot["owner_epoch"] == second_health["queue"]["owner_epoch"]
    finally:
        if second is not None:
            _stop_managed_bridge(second)
        if first is not None:
            _stop_managed_bridge(first)
