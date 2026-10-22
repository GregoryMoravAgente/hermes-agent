import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import os from 'node:os';
import path from 'node:path';
import {
  mkdtempSync,
  readdirSync,
  rmSync,
  unlinkSync,
} from 'node:fs';

import { InboundQueue, InboundQueueError } from './inbound_queue.js';
import { bindInboundProducerLifecycle } from './bridge_helpers.js';
import {
  ATTACH_PROTOCOL,
  DRAIN_PROTOCOL,
  sha256,
  signSettlement,
} from './bridge_protocol.js';

function makeBinding(queue, consumerId = 'consumer-a', consumerEpoch = 1) {
  return {
    ...queue.currentOwnerFence(),
    session_path: queue.sessionDir,
    consumer_id: consumerId,
    consumer_epoch: consumerEpoch,
  };
}

function temporaryQueue() {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-inbound-'));
  const queue = new InboundQueue({ sessionDir, owner: { pid: 42, startedAt: 'test' } });
  return { queue, sessionDir };
}

test('durable cursor keeps FIFO identity, source-issued custody, and idempotent response loss', () => {
  const { queue, sessionDir } = temporaryQueue();
  try {
    const binding = makeBinding(queue);
    for (let index = 0; index < 105; index += 1) {
      queue.enqueue(
        { messageId: `wire-${index}`, chatId: 'chat@s.whatsapp.net', body: `message-${index}` },
        { requestId: `enqueue-${index}`, wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: `wire-${index}` } },
      );
    }
    assert.equal(queue.snapshot().pending, 105);

    const first = queue.claim({ request_id: 'claim-1', binding, limit: 1 });
    assert.equal(first.status, 'claimed');
    assert.equal(first.item.sequence, 1);
    assert.equal(typeof first.custody_ref, 'string');
    assert.ok(first.custody_ref.length > 16);
    assert.deepEqual(queue.claim({ request_id: 'claim-1', binding, limit: 1 }), first);

    const accepted = queue.transition({
      request_id: 'accept-1',
      op: 'accept',
      binding,
      item_id: first.item.item_id,
      sequence: first.item.sequence,
      claim_id: first.claim_id,
      expected_state: 'claimed',
      source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'wire-0']],
      registration_generation: 7,
      custody_ref: first.custody_ref,
    });
    assert.equal(accepted.status, 'transitioned');
    assert.throws(
      () => queue.transition({
        request_id: 'ack-incomplete',
        op: 'ack',
        binding,
        item_id: first.item.item_id,
        sequence: first.item.sequence,
        claim_id: first.claim_id,
        expected_state: 'accepted',
        custody_ref: first.custody_ref,
        settlement: { producer_done: true, native_exited: false, transport_settled: true },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_incomplete',
    );
    const acked = queue.transition({
      request_id: 'ack-1',
      op: 'ack',
      binding,
      item_id: first.item.item_id,
      sequence: first.item.sequence,
      claim_id: first.claim_id,
      expected_state: 'accepted',
      custody_ref: first.custody_ref,
      settlement: { producer_done: true, native_exited: true, transport_settled: true },
    });
    assert.equal(acked.status, 'transitioned');
    assert.equal(queue.snapshot().acked, 1);
    assert.equal(queue.snapshot().pending, 104);

    const stale = { ...binding, owner_epoch: binding.owner_epoch - 1 };
    assert.throws(
      () => queue.claim({ request_id: 'stale-claim', binding: stale, limit: 1 }),
      (error) => error instanceof InboundQueueError && error.code === 'stale_owner_epoch',
    );
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('managed owner fence and private attach are required before a producer sequence', () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-managed-'));
  const capability = 'a'.repeat(43);
  const witnessKey = 'w'.repeat(32);
  const queue = new InboundQueue({
    sessionDir,
    owner: {
      pid: 52,
      startedAt: 'managed-test',
      processStart: 52,
      canonicalPort: 3012,
      sessionDigest: 's'.repeat(64),
      profileDigest: 'p'.repeat(64),
      scriptHash: 'h'.repeat(64),
      attachKeyId: 'a'.repeat(64),
      sourceWitnessKeyId: 'b'.repeat(64),
      capability,
      witnessKey,
      capabilityRequired: true,
      listenerReady: true,
      ownerReady: true,
    },
  });
  try {
    const ownerFence = queue.currentOwnerFence();
    const binding = {
      ...ownerFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'managed-consumer',
      consumer_epoch: 1,
    };
    const attached = queue.attach({
      request_id: 'managed-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'managed-challenge',
      binding_digest: ownerFence.binding_digest,
      binding,
    });
    assert.equal(attached.status, 'attached');
    assert.equal(typeof attached.attach_proof.mac_hex, 'string');
    const health = queue.health();
    assert.equal('capability' in health, false);
    assert.equal('attach_key_b64url' in health, false);
    assert.equal('source_witness_key_b64url' in health, false);
    assert.throws(
      () => queue.enqueue({ messageId: 'missing-fence', chatId: 'chat@s.whatsapp.net' }, {
        requestId: 'managed-missing-fence',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'missing-fence' },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'owner_fence_missing',
    );
    const enqueued = queue.enqueue(
      { messageId: 'managed-wire', chatId: 'chat@s.whatsapp.net', body: 'managed' },
      {
        requestId: 'managed-enqueue',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'managed-wire' },
        ownerFence,
      },
    );
    assert.equal(enqueued.status, 'enqueued');
    assert.equal(queue.snapshot().pending, 1);
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('managed producer readiness is bound to the current connection generation', () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-producer-readiness-'));
  const owner = {
    pid: 53,
    startedAt: 'producer-readiness-test',
    processStart: 53,
    canonicalPort: 3014,
    sessionDigest: 's'.repeat(64),
    profileDigest: 'p'.repeat(64),
    scriptHash: 'h'.repeat(64),
    attachKeyId: 'a'.repeat(64),
    sourceWitnessKeyId: 'b'.repeat(64),
    capability: 'c'.repeat(43),
    witnessKey: 'w'.repeat(32),
    capabilityRequired: true,
    listenerReady: true,
    ownerReady: true,
  };
  const queue = new InboundQueue({ sessionDir, owner });
  try {
    const binding = {
      ...makeBinding(queue, 'producer-consumer', 1),
      listener_ready: true,
      owner_ready: true,
    };
    queue.attach({
      request_id: 'producer-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'producer-challenge',
      binding_digest: queue.currentOwnerFence().binding_digest,
      binding,
    });

    const firstEvents = new EventEmitter();
    bindInboundProducerLifecycle(queue, firstEvents);
    firstEvents.emit('connection.update', { connection: 'open' });
    assert.equal(queue.health().producer_state, 'connected');

    const firstClose = queue.control({
      request_id: 'producer-close-1', op: 'close', binding, close_generation: 1,
    });
    assert.equal(firstClose.snapshot.validated, true);
    assert.equal(queue.control({
      request_id: 'producer-resume-1', op: 'resume', binding, close_generation: 1,
    }).status, 'open');

    const secondEvents = new EventEmitter();
    bindInboundProducerLifecycle(queue, secondEvents);
    secondEvents.emit('connection.update', { connection: 'open' });
    firstEvents.emit('connection.update', { connection: 'close' });
    assert.equal(queue.health().producer_state, 'connected');
    const secondClose = queue.control({
      request_id: 'producer-close-2', op: 'close', binding, close_generation: 2,
    });
    assert.equal(secondClose.snapshot.validated, true);

    assert.equal(queue.control({
      request_id: 'producer-resume-2', op: 'resume', binding, close_generation: 2,
    }).status, 'open');
    const thirdEvents = new EventEmitter();
    bindInboundProducerLifecycle(queue, thirdEvents);
    thirdEvents.emit('connection.update', { connection: 'connecting' });
    assert.equal(queue.health().producer_state, 'unknown');
    const thirdClose = queue.control({
      request_id: 'producer-close-before-open', op: 'close', binding, close_generation: 3,
    });
    assert.equal(thirdClose.snapshot.validated, false);
    thirdEvents.emit('connection.update', { connection: 'open' });
    assert.equal(queue.snapshot(binding).validated, false);

    const restarted = new InboundQueue({
      sessionDir,
      owner: { ...owner, pid: 54, startedAt: 'producer-readiness-restart', processStart: 54 },
    });
    assert.equal(restarted.health().producer_state, 'unknown');
    assert.equal(restarted.snapshot(makeBinding(restarted, 'producer-consumer', 1)).validated, false);
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('restart changes the owner fence and retains an uncertain claim without adoption or expiry requeue', () => {
  const { queue, sessionDir } = temporaryQueue();
  try {
    const binding = makeBinding(queue, 'consumer-before-restart', 1);
    queue.enqueue({ messageId: 'restart-wire', chatId: 'chat@s.whatsapp.net', body: 'held' }, {
      requestId: 'restart-enqueue',
      wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'restart-wire' },
    });
    const claim = queue.claim({ request_id: 'restart-claim', binding, limit: 1 });
    const restarted = new InboundQueue({ sessionDir, owner: { pid: 43, startedAt: 'restart' } });
    const afterRestart = makeBinding(restarted, 'consumer-after-restart', 1);
    assert.notEqual(restarted.bridgeInstance, queue.bridgeInstance);
    assert.notEqual(restarted.ownerEpoch, queue.ownerEpoch);
    assert.equal(restarted.snapshot().active_claims, 1);
    assert.equal(restarted.claim({ request_id: 'new-claim', binding: afterRestart, limit: 1 }).reason, 'head_claimed');
    assert.deepEqual(restarted.requestResult('restart-claim').result, claim);
    assert.throws(
      () => restarted.transition({
        request_id: 'foreign-transition',
        op: 'release',
        binding: afterRestart,
        item_id: claim.item.item_id,
        sequence: claim.item.sequence,
        claim_id: claim.claim_id,
        expected_state: 'claimed',
        reason: 'runtime_draining',
      }),
      (error) => error instanceof InboundQueueError && error.code === 'binding_conflict',
    );
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('close retains post-close durable backlog and resume requires the exact owner binding', () => {
  const { queue, sessionDir } = temporaryQueue();
  try {
    const binding = makeBinding(queue);
    queue.enqueue({ messageId: 'before-close', chatId: 'chat@s.whatsapp.net', body: 'before' }, {
      requestId: 'close-enqueue-before',
      wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'before-close' },
    });
    const closing = queue.control({ request_id: 'close-1', op: 'close', binding, close_generation: 9 });
    assert.equal(closing.status, 'closing');
    assert.equal(closing.snapshot.validated, false);
    assert.equal(closing.snapshot.preclose_pending, 1);

    const drainedClaim = queue.drainClaim({
      request_id: 'drain-1', protocol: DRAIN_PROTOCOL, close_generation: 9,
      watermark: closing.receipt.watermark, close_receipt_digest: closing.receipt.receipt_digest,
      binding, limit: 1,
    });
    assert.equal(drainedClaim.status, 'claimed');
    const drainAccept = queue.transition({
      request_id: 'drain-accept-1', op: 'accept', binding,
      item_id: drainedClaim.item.item_id, sequence: drainedClaim.item.sequence,
      claim_id: drainedClaim.claim_id, expected_state: 'claimed',
      source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'before-close']],
      registration_generation: 1, custody_ref: drainedClaim.custody_ref,
    });
    assert.equal(drainAccept.status, 'transitioned');
    const drainAck = queue.transition({
      request_id: 'drain-ack-1', op: 'ack', binding,
      item_id: drainedClaim.item.item_id, sequence: drainedClaim.item.sequence,
      claim_id: drainedClaim.claim_id, expected_state: 'accepted',
      custody_ref: drainedClaim.custody_ref,
      settlement: { producer_done: true, native_exited: true, transport_settled: true },
    });
    assert.equal(drainAck.status, 'transitioned');
    assert.equal(queue.snapshot(binding).drain_complete, true);

    queue.enqueue({ messageId: 'after-close', chatId: 'chat@s.whatsapp.net', body: 'after' }, {
      requestId: 'close-enqueue-after',
      wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'after-close' },
    });
    const retained = queue.snapshot(binding);
    assert.equal(retained.postclose_retained, 1);
    assert.equal(retained.ready, true);

    const foreign = { ...binding, consumer_id: 'foreign-consumer' };
    assert.throws(
      () => queue.control({ request_id: 'foreign-resume', op: 'resume', binding: foreign, close_generation: 9 }),
      (error) => error instanceof InboundQueueError && error.code === 'resume_conflict',
    );
    assert.equal(queue.control({ request_id: 'resume-1', op: 'resume', binding, close_generation: 9 }).status, 'open');
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('a missing durable sequence becomes an explicit storage fault instead of an empty queue', () => {
  const { queue, sessionDir } = temporaryQueue();
  try {
    queue.enqueue({ messageId: 'corrupt-wire', chatId: 'chat@s.whatsapp.net', body: 'corrupt' }, {
      requestId: 'corrupt-enqueue',
      wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'corrupt-wire' },
    });
    const itemDirectory = path.join(sessionDir, 'inbound-queue', 'items');
    const [item] = readdirSync(itemDirectory);
    unlinkSync(path.join(itemDirectory, item));
    assert.throws(
      () => queue.snapshot(),
      (error) => error instanceof InboundQueueError && error.code === 'storage_fault',
    );
    assert.throws(
      () => queue.health(),
      (error) => error instanceof InboundQueueError && error.code === 'storage_fault',
    );
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('the durable request record is idempotent and rejects a different operation identity', () => {
  const { queue, sessionDir } = temporaryQueue();
  try {
    const first = queue.enqueue(
      { messageId: 'same-request-wire', chatId: 'chat@s.whatsapp.net', body: 'first' },
      {
        requestId: 'same-request',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'same-request-wire' },
      },
    );
    assert.deepEqual(queue.enqueue(
      { messageId: 'same-request-wire', chatId: 'chat@s.whatsapp.net', body: 'first' },
      {
        requestId: 'same-request',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'same-request-wire' },
      },
    ), first);
    assert.throws(
      () => queue.enqueue(
        { messageId: 'different-wire', chatId: 'chat@s.whatsapp.net', body: 'second' },
        {
          requestId: 'same-request',
          wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'different-wire' },
        },
      ),
      (error) => error instanceof InboundQueueError && error.code === 'request_conflict',
    );
    assert.deepEqual(queue.requestResult('same-request').result, first);
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

test('managed settlement refuses missing or caller-forged finalizer evidence', () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-settlement-'));
  const queue = new InboundQueue({
    sessionDir,
    owner: {
      pid: 57,
      startedAt: 'settlement-test',
      processStart: 57,
      canonicalPort: 3015,
      sessionDigest: 's'.repeat(64),
      profileDigest: 'p'.repeat(64),
      scriptHash: 'h'.repeat(64),
      attachKeyId: 'a'.repeat(64),
      sourceWitnessKeyId: 'b'.repeat(64),
      capability: 'c'.repeat(43),
      witnessKey: 'w'.repeat(32),
      capabilityRequired: true,
      listenerReady: true,
      ownerReady: true,
    },
  });
  try {
    const ownerFence = queue.currentOwnerFence();
    const binding = {
      ...ownerFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'settlement-consumer',
      consumer_epoch: 1,
    };
    queue.attach({
      request_id: 'settlement-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'settlement-challenge',
      binding_digest: ownerFence.binding_digest,
      binding,
    });
    const lifecycle = new EventEmitter();
    bindInboundProducerLifecycle(queue, lifecycle);
    lifecycle.emit('connection.update', { connection: 'open' });
    const generation = queue.snapshot(binding).producer_generation;
    assert.equal(typeof generation, 'string');

    queue.enqueue(
      { messageId: 'settlement-wire', chatId: 'chat@s.whatsapp.net', body: 'settle' },
      {
        requestId: 'settlement-enqueue',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'settlement-wire' },
        ownerFence,
      },
    );
    const claim = queue.claim({ request_id: 'settlement-claim', binding, limit: 1 });
    queue.transition({
      request_id: 'settlement-accept',
      op: 'accept',
      binding,
      item_id: claim.item.item_id,
      sequence: claim.item.sequence,
      claim_id: claim.claim_id,
      expected_state: 'claimed',
      source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'settlement-wire']],
      registration_generation: 4,
      custody_ref: claim.custody_ref,
      settlement_fields: { producer_generation: generation },
    });
    const settleBase = {
      request_id: 'settlement-missing-finalizer',
      op: 'settle',
      binding,
      item_id: claim.item.item_id,
      sequence: claim.item.sequence,
      claim_id: claim.claim_id,
      expected_state: 'accepted',
      custody_ref: claim.custody_ref,
    };
    assert.throws(
      () => queue.transition({
        ...settleBase,
        settlement: { producer_done: true, native_exited: true, transport_settled: true },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_incomplete',
    );
    assert.throws(
      () => queue.transition({
        ...settleBase,
        request_id: 'settlement-forged-finalizer',
        settlement: {
          producer_done: true,
          native_exited: true,
          transport_settled: true,
          producer_generation: generation,
          finalizer_status: 'success',
          finalizer_receipt_digest: 'f'.repeat(64),
        },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_witness_invalid',
    );
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});


test('managed settlement preserves exact signed occurrence and rejects malformed source fields', () => {
  const sessionDir = mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-occurrence-'));
  const witnessKey = 'w'.repeat(32);
  const queue = new InboundQueue({
    sessionDir,
    owner: {
      pid: 58,
      startedAt: 'occurrence-test',
      processStart: 58,
      canonicalPort: 3016,
      sessionDigest: 's'.repeat(64),
      profileDigest: 'p'.repeat(64),
      scriptHash: 'h'.repeat(64),
      attachKeyId: 'a'.repeat(64),
      sourceWitnessKeyId: sha256(Buffer.from(witnessKey, 'utf8')),
      capability: 'c'.repeat(43),
      witnessKey,
      capabilityRequired: true,
      listenerReady: true,
      ownerReady: true,
    },
  });
  try {
    const ownerFence = queue.currentOwnerFence();
    const binding = {
      ...ownerFence,
      listener_ready: true,
      owner_ready: true,
      consumer_id: 'occurrence-consumer',
      consumer_epoch: 1,
    };
    queue.attach({
      request_id: 'occurrence-attach',
      protocol: ATTACH_PROTOCOL,
      challenge_nonce: 'occurrence-challenge',
      binding_digest: ownerFence.binding_digest,
      binding,
    });
    const lifecycle = new EventEmitter();
    bindInboundProducerLifecycle(queue, lifecycle);
    lifecycle.emit('connection.update', { connection: 'open' });
    const generation = queue.snapshot(binding).producer_generation;
    const enqueued = queue.enqueue(
      { messageId: 'occurrence-wire', chatId: 'chat@s.whatsapp.net', body: 'occurrence' },
      {
        requestId: 'occurrence-enqueue',
        wireIdentity: { chatId: 'chat@s.whatsapp.net', messageId: 'occurrence-wire' },
        ownerFence,
      },
    );
    const claim = queue.claim({ request_id: 'occurrence-claim', binding, limit: 1 });
    const sourceFields = {
      source_event_digest: 'e'.repeat(64),
      source_identity_digest: 'a'.repeat(64),
      transport_owner_digest: 'b'.repeat(64),
      transport_receipt_digest: 'c'.repeat(64),
      native_occurrence_id: '4:1',
    };
    assert.throws(
      () => queue.transition({
        request_id: 'occurrence-accept-wrong-type',
        op: 'accept',
        binding,
        item_id: claim.item.item_id,
        sequence: claim.item.sequence,
        claim_id: claim.claim_id,
        expected_state: 'claimed',
        source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'occurrence-wire']],
        registration_generation: 4,
        custody_ref: claim.custody_ref,
        settlement_fields: { ...sourceFields, native_occurrence_id: 4, producer_generation: generation },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_witness_invalid',
    );
    const accepted = queue.transition({
      request_id: 'occurrence-accept',
      op: 'accept',
      binding,
      item_id: claim.item.item_id,
      sequence: claim.item.sequence,
      claim_id: claim.claim_id,
      expected_state: 'claimed',
      source_identity: [['chat_id', 'chat@s.whatsapp.net'], ['message_id', 'occurrence-wire']],
      registration_generation: 4,
      custody_ref: claim.custody_ref,
      settlement_fields: { ...sourceFields, producer_generation: generation },
    });
    assert.equal(accepted.item.item_id, enqueued.item.item_id);

    const finalizerDigest = 'f'.repeat(64);
    const baseFields = {
      ...accepted.item.delivery.settlement_fields,
      source_witness_key_id: sha256(Buffer.from(witnessKey, 'utf8')),
      transport_status: 'success',
      settlement_challenge: accepted.item.settlement_challenge,
      finalizer_receipt_digest: finalizerDigest,
      finalizer_status: 'success',
    };
    const makeSettlement = (requestId, fieldOverrides = {}) => {
      const fields = {
        ...baseFields,
        request_id_digest: sha256(requestId),
        ...fieldOverrides,
      };
      return {
        producer_done: true,
        native_exited: true,
        transport_settled: true,
        producer_generation: generation,
        finalizer_status: 'success',
        finalizer_receipt_digest: finalizerDigest,
        witness: signSettlement(fields, Buffer.from(witnessKey, 'utf8')),
      };
    };
    const settleBase = {
      op: 'settle',
      binding,
      item_id: claim.item.item_id,
      sequence: claim.item.sequence,
      claim_id: claim.claim_id,
      expected_state: 'accepted',
      custody_ref: claim.custody_ref,
    };
    assert.throws(
      () => queue.transition({
        ...settleBase,
        request_id: 'occurrence-missing',
        settlement: makeSettlement('occurrence-missing', { native_occurrence_id: null }),
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_witness_invalid',
    );
    assert.throws(
      () => queue.transition({
        ...settleBase,
        request_id: 'occurrence-oversized',
        settlement: makeSettlement('occurrence-oversized', { native_occurrence_id: 'x'.repeat(513) }),
      }),
      (error) => error instanceof Error && error.message === 'invalid witness field: native_occurrence_id',
    );
    assert.throws(
      () => makeSettlement('occurrence-separator', { native_occurrence_id: '\x1f' }),
      /invalid witness field/,
    );
    const hmacSettlement = makeSettlement('occurrence-hmac');
    const tamperedMac = hmacSettlement.witness.mac_hex.startsWith('0')
      ? `1${hmacSettlement.witness.mac_hex.slice(1)}`
      : `0${hmacSettlement.witness.mac_hex.slice(1)}`;
    assert.throws(
      () => queue.transition({
        ...settleBase,
        request_id: 'occurrence-hmac',
        settlement: {
          ...hmacSettlement,
          witness: { ...hmacSettlement.witness, mac_hex: tamperedMac },
        },
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_witness_invalid',
    );
    assert.throws(
      () => queue.transition({
        ...settleBase,
        request_id: 'occurrence-exact-mismatch',
        settlement: makeSettlement('occurrence-exact-mismatch', { source_event_digest: 'd'.repeat(64) }),
      }),
      (error) => error instanceof InboundQueueError && error.code === 'settlement_witness_invalid',
    );
    const settled = queue.transition({
      ...settleBase,
      request_id: 'occurrence-valid',
      settlement: makeSettlement('occurrence-valid'),
    });
    assert.equal(settled.status, 'transitioned');
    assert.equal(settled.item.state, 'accepted');
  } finally {
    rmSync(sessionDir, { recursive: true, force: true });
  }
});

console.log('inbound_queue.test.mjs: all assertions passed');
