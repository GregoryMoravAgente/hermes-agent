import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import express from 'express';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { mkdtempSync, rmSync } from 'node:fs';

import { InboundQueue, mountInboundQueueRoutes } from './inbound_queue.js';
import {
  ATTACH_PROTOCOL,
  ADOPT_PROTOCOL,
  CAPABILITY_HEADER,
  adoptionOperationId,
} from './bridge_protocol.js';
import { bindInboundProducerLifecycle } from './bridge_helpers.js';

const MANAGED_CAPABILITY = 'm'.repeat(43);

function managedOwner() {
  return {
    pid: 51,
    startedAt: 'http-managed-test',
    processStart: 51,
    canonicalPort: 3013,
    sessionDigest: 'j'.repeat(64),
    profileDigest: 'k'.repeat(64),
    scriptHash: 'l'.repeat(64),
    attachKeyId: 'm'.repeat(64),
    sourceWitnessKeyId: 'n'.repeat(64),
    capability: MANAGED_CAPABILITY,
    capabilityRequired: true,
    listenerReady: true,
    ownerReady: true,
  };
}

function startQueueServer(sessionDir, owner = { pid: 51, startedAt: 'http-test' }) {
  const queue = new InboundQueue({ sessionDir, owner });
  const app = express();
  app.use(express.json());
  app.get('/health', (req, res) => {
    const health = queue.health();
    res.json({ ...health, queue: health });
  });
  app.post('/enqueue-test', (req, res) => res.json(queue.enqueue(req.body.payload, {
    requestId: req.body.request_id,
    wireIdentity: req.body.wire_identity,
  })));
  mountInboundQueueRoutes(app, queue, {
    binding: () => ({
      ...queue.currentOwnerFence(),
      session_path: queue.sessionDir,
      listener_ready: owner.listenerReady === true,
      owner_ready: owner.ownerReady === true,
    }),
  });
  const server = http.createServer(app);
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        queue,
        server,
        capability: queue.capability,
        baseUrl: `http://127.0.0.1:${server.address().port}`,
      });
    });
  });
}

async function request(baseUrl, pathName, method, body, extraHeaders = {}) {
  const response = await fetch(`${baseUrl}${pathName}`, {
    method,
    headers: { 'content-type': 'application/json', ...extraHeaders },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
}

test('HTTP cursor preserves request idempotency, exact binding, and close/resume state', async () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-inbound-http-'));
  const { queue, server, baseUrl } = await startQueueServer(sessionDir);
  const binding = {
    session_path: queue.sessionDir,
    queue_epoch: queue.queueEpoch,
    bridge_instance: queue.bridgeInstance,
    owner_epoch: queue.ownerEpoch,
    consumer_id: 'http-consumer',
    consumer_epoch: 2,
    profile: 'default',
  };
  try {
    const enqueued = await request(baseUrl, '/enqueue-test', 'POST', {
      request_id: 'http-enqueue',
      payload: { messageId: 'http-wire', chatId: 'chat@s.whatsapp.net', body: 'wire' },
      wire_identity: { chatId: 'chat@s.whatsapp.net', messageId: 'http-wire' },
    });
    assert.equal(enqueued.status, 200);

    const claimBody = { request_id: 'http-claim', binding, limit: 1 };
    const claimed = await request(baseUrl, '/messages/claim', 'POST', claimBody);
    assert.equal(claimed.status, 200);
    assert.equal(claimed.body.status, 'claimed');
    assert.equal(claimed.body.item.payload.messageId, 'http-wire');
    assert.equal(claimed.body.item.wire_identity.messageId, 'http-wire');
    const retried = await request(baseUrl, '/messages/claim', 'POST', claimBody);
    assert.deepEqual(retried.body, claimed.body);
    const stale = await request(baseUrl, '/messages/claim', 'POST', {
      request_id: 'http-stale-claim',
      binding: { ...binding, queue_epoch: 'stale-queue-epoch' },
      limit: 1,
    });
    assert.equal(stale.status, 409);
    assert.equal(stale.body.error, 'stale_queue_epoch');

    const accepted = await request(baseUrl, '/messages/transition', 'POST', {
      request_id: 'http-accept',
      op: 'accept',
      binding,
      item_id: claimed.body.item.item_id,
      sequence: claimed.body.item.sequence,
      claim_id: claimed.body.claim_id,
      expected_state: 'claimed',
      source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'http-wire']],
      registration_generation: 3,
      custody_ref: claimed.body.custody_ref,
    });
    assert.equal(accepted.body.status, 'transitioned');

    const foreignAck = await request(baseUrl, '/messages/transition', 'POST', {
      request_id: 'http-foreign-ack',
      op: 'ack',
      binding: { ...binding, consumer_id: 'foreign' },
      item_id: claimed.body.item.item_id,
      sequence: claimed.body.item.sequence,
      claim_id: claimed.body.claim_id,
      expected_state: 'accepted',
      custody_ref: claimed.body.custody_ref,
      settlement: { producer_done: true, native_exited: true, transport_settled: true },
    });
    assert.equal(foreignAck.status, 409);
    assert.equal(foreignAck.body.error, 'binding_conflict');

    const ackBody = {
      request_id: 'http-ack',
      op: 'ack',
      binding,
      item_id: claimed.body.item.item_id,
      sequence: claimed.body.item.sequence,
      claim_id: claimed.body.claim_id,
      expected_state: 'accepted',
      custody_ref: claimed.body.custody_ref,
      settlement: { producer_done: true, native_exited: true, transport_settled: true },
    };
    const acked = await request(baseUrl, '/messages/transition', 'POST', ackBody);
    assert.equal(acked.body.status, 'transitioned');
    const requestResult = await request(baseUrl, '/messages/requests/http-ack', 'GET');
    assert.equal(requestResult.body.status, 'request_result');
    assert.deepEqual(requestResult.body.result, acked.body);

    const closing = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-close', op: 'close', binding, close_generation: 11,
    });
    assert.equal(closing.body.status, 'closing');
    assert.equal(closing.body.snapshot.validated, true);
    const resumed = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-resume', op: 'resume', binding, close_generation: 11,
    });
    assert.equal(resumed.body.status, 'open');
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('managed HTTP control refuses an empty queue without a producer-ready receipt', async () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-empty-control-http-'));
  const { queue, server, baseUrl, capability } = await startQueueServer(sessionDir, managedOwner());
  const headers = { [CAPABILITY_HEADER]: capability };
  try {
    const ownerFence = queue.currentOwnerFence();
    const attached = await request(baseUrl, '/messages/attach', 'POST', {
      request_id: 'http-empty-control-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'http-empty-control-challenge',
      binding_digest: ownerFence.binding_digest,
    }, headers);
    assert.equal(attached.status, 200);
    assert.equal(attached.body.status, 'attached');
    const empty = await request(baseUrl, '/messages', 'GET', undefined, headers);
    assert.equal(empty.status, 200);
    assert.equal(empty.body.pending, 0);
    assert.equal(empty.body.active_claims, 0);
    assert.equal(empty.body.accepted, 0);

    const closing = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-empty-control-close',
      op: 'close',
      close_generation: 1,
    }, headers);
    assert.equal(closing.status, 200, 'the negative must inspect the normal serialized close response');
    assert.equal(closing.body?.status, 'closing');
    const snapshot = closing.status === 200 ? closing.body.snapshot : null;
    assert.ok(snapshot && typeof snapshot === 'object');
    assert.equal(Object.prototype.hasOwnProperty.call(snapshot, 'validated'), true);
    assert.equal(Object.prototype.hasOwnProperty.call(snapshot, 'drain_complete'), true);
    assert.equal(snapshot.producer_state, 'unknown');
    assert.equal(snapshot.validated, false, 'without a producer-ready receipt the close remains unknown');
    assert.equal(snapshot.drain_complete, false, 'without a producer-ready receipt the drain remains unknown');
    assert.ok(
      closing.status !== 200
        || snapshot?.validated !== true
        || snapshot?.drain_complete !== true
        || snapshot?.ready !== true,
      'an enabled managed queue without a producer-ready receipt must not report a clean validated drain',
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('managed HTTP control closes an empty queue only after the producer lifecycle opens', async () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-ready-control-http-'));
  const { queue, server, baseUrl, capability } = await startQueueServer(sessionDir, managedOwner());
  const headers = { [CAPABILITY_HEADER]: capability };
  const producerEvents = new EventEmitter();
  bindInboundProducerLifecycle(queue, producerEvents);
  try {
    const ownerFence = queue.currentOwnerFence();
    const binding = {
      ...ownerFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'http-ready-consumer',
      consumer_epoch: 1,
    };
    const attached = await request(baseUrl, '/messages/attach', 'POST', {
      request_id: 'http-ready-control-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'http-ready-control-challenge',
      binding_digest: ownerFence.binding_digest,
      binding,
    }, headers);
    assert.equal(attached.status, 200);

    producerEvents.emit('connection.update', { connection: 'open' });
    const ready = await request(baseUrl, '/health', 'GET', undefined, headers);
    assert.equal(ready.status, 200);
    assert.equal(ready.body.queue.producer_state, 'connected');
    assert.equal(typeof ready.body.queue.producer_receipt_digest, 'string');

    const closing = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-ready-control-close', op: 'close', binding, close_generation: 2,
    }, headers);
    assert.equal(closing.status, 200);
    assert.equal(closing.body.status, 'closing');
    assert.equal(closing.body.snapshot.producer_state, 'connected');
    assert.equal(closing.body.snapshot.validated, true);
    assert.equal(closing.body.snapshot.drain_complete, true);

    const resumed = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-ready-control-resume', op: 'resume', binding, close_generation: 2,
    }, headers);
    assert.equal(resumed.status, 200);
    producerEvents.emit('connection.update', { connection: 'close' });
    const disconnected = await request(baseUrl, '/health', 'GET', undefined, headers);
    assert.equal(disconnected.body.queue.producer_state, 'disconnected');

    const held = await request(baseUrl, '/messages/control', 'POST', {
      request_id: 'http-disconnected-control-close', op: 'close', binding, close_generation: 3,
    }, headers);
    assert.equal(held.status, 200);
    assert.equal(held.body.status, 'closing');
    assert.equal(held.body.snapshot.validated, false);
    assert.equal(held.body.snapshot.drain_complete, false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('HTTP owner adoption is deterministic and response-loss retries do not create a successor claim', async () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-adoption-http-'));
  const first = await startQueueServer(sessionDir, managedOwner());
  let second = null;
  try {
    const firstHeaders = { [CAPABILITY_HEADER]: first.capability };
    const firstFence = first.queue.currentOwnerFence();
    const firstBinding = {
      ...firstFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'predecessor-consumer',
      consumer_epoch: 1,
    };
    const attached = await request(first.baseUrl, '/messages/attach', 'POST', {
      request_id: 'adoption-predecessor-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'predecessor-challenge',
      binding_digest: firstFence.binding_digest,
      binding: firstBinding,
    }, firstHeaders);
    assert.equal(attached.status, 200);

    first.queue.enqueue(
      { messageId: 'adoption-wire', chatId: 'chat@s.whatsapp.net', body: 'held' },
      {
        requestId: 'adoption-wire',
        wireIdentity: { messageId: 'adoption-wire', chatId: 'chat@s.whatsapp.net' },
        ownerFence: firstFence,
      },
    );
    const firstClaim = first.queue.claim({ request_id: 'adoption-claim', binding: firstBinding, limit: 1 });
    assert.equal(firstClaim.status, 'claimed');

    await new Promise((resolve, reject) => first.server.close((error) => error ? reject(error) : resolve()));
    second = await startQueueServer(sessionDir, { ...managedOwner(), pid: 52, startedAt: 'http-adoption-successor' });
    const headers = { [CAPABILITY_HEADER]: second.capability };
    const predecessorDigest = second.queue.health().owner_handoff.predecessor_digest;
    const successorFence = second.queue.currentOwnerFence();
    const successorBinding = {
      ...successorFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'successor-consumer',
      consumer_epoch: 2,
    };
    const successorAttach = await request(second.baseUrl, '/messages/attach', 'POST', {
      request_id: 'adoption-successor-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'successor-challenge',
      predecessor_digest: predecessorDigest,
      binding_digest: successorFence.binding_digest,
      binding: successorBinding,
    }, headers);
    assert.equal(successorAttach.status, 200);
    const handoff = successorAttach.body.owner_handoff;
    assert.equal(handoff.state, 'pending');
    const adoption = {
      request_id: adoptionOperationId({
        queue_epoch: second.queue.queueEpoch,
        predecessor_digest: handoff.predecessor_digest,
        adoption_nonce: handoff.adoption_nonce,
      }),
      protocol: ADOPT_PROTOCOL,
      predecessor_digest: handoff.predecessor_digest,
      adoption_nonce: handoff.adoption_nonce,
      queue_epoch: second.queue.queueEpoch,
      attach_proof: successorAttach.body.attach_proof,
    };
    const adopted = await request(second.baseUrl, '/messages/adopt', 'POST', adoption, headers);
    assert.equal(adopted.status, 200);
    assert.equal(adopted.body.status, 'adopted');
    const retried = await request(second.baseUrl, '/messages/adopt', 'POST', adoption, headers);
    assert.deepEqual(retried.body, adopted.body);
    const conflicting = await request(second.baseUrl, '/messages/adopt', 'POST', {
      ...adoption,
      request_id: 'adoption-different-request',
    }, headers);
    assert.equal(conflicting.status, 409);
    assert.equal(conflicting.body.error, 'adoption_request_conflict');

    const held = await request(second.baseUrl, '/messages/claim', 'POST', {
      request_id: 'adoption-successor-claim', binding: successorBinding, limit: 1,
    }, headers);
    assert.equal(held.status, 200);
    assert.equal(held.body.reason, 'head_claimed');
    assert.equal(held.body.item.claim_id, firstClaim.claim_id);
  } finally {
    await new Promise((resolve) => {
      if (second?.server) second.server.close(() => resolve());
      else resolve();
    });
    if (first.server.listening) await new Promise((resolve) => first.server.close(resolve));
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

console.log('inbound_queue_http.test.mjs: all assertions passed');
