import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeSync,
} from 'fs';
import path from 'path';
import { createHash, randomBytes, randomUUID } from 'crypto';
import {
  ADOPT_PROTOCOL,
  ATTACH_PROTOCOL,
  CAPABILITY_HEADER,
  DRAIN_PROTOCOL,
  capabilityDigest,
  digestBinding,
  ownerBindingDigest,
  requestFileName,
  safeEqualHex,
  settlementFieldsMatch,
  sha256,
  signAttachProof,
  verifyAttachProof,
  validateRequestId,
  verifySettlement,
} from './bridge_protocol.js';
import { nextMutationMeta, QueueLockError, QueueMutationLock } from './queue_lock.js';

const QUEUE_DIR = 'inbound-queue';
const META_FILE = 'meta.json';
const ITEMS_DIR = 'items';
const OPERATIONS_DIR = 'operations';
const PREPARED_DIR = 'prepared';
const ITEM_STATES = new Set(['pending', 'claimed', 'accepted', 'unknown', 'acked']);
const PRODUCER_STATES = new Set(['unknown', 'connected', 'disconnected', 'not_required']);

const SOURCE_DIGEST_FIELDS = Object.freeze([
  'source_event_digest', 'source_identity_digest', 'transport_owner_digest',
  'transport_receipt_digest',
]);

function isValidSourceOccurrence(value) {
  // Python's source projection requires a nonempty string; boundedField uses
  // '-' only as the wire sentinel for a missing value and limits fields to 512
  // characters while rejecting the separators/control characters below.
  return typeof value === 'string' && value.length > 0 && value !== '-'
    && value.length <= 512 && !/[\x00\x1e\x1f\r\n]/.test(value);
}

export class InboundQueueError extends Error {
  constructor(code, message, status = 409, details = {}) {
    super(message);
    this.name = 'InboundQueueError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableValue(value[key])]));
  }
  return value;
}

function fingerprint(value) {
  return createHash('sha256')
    .update(JSON.stringify(stableValue(value)))
    .digest('hex');
}

function metadataFingerprint(value) {
  const normalized = { ...(value || {}) };
  // Producer readiness is its own source lifecycle journal.  Prepared queue
  // operations written before this field existed must still be recoverable;
  // readiness itself is validated from the current owner and generation.
  delete normalized.producer_readiness;
  delete normalized.durability;
  return fingerprint(normalized);
}

function requiredString(value, name) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new InboundQueueError('invalid_request', `${name} is required`, 400);
  }
  return value.trim();
}

function requiredInteger(value, name) {
  if (!Number.isInteger(value) || value < 0) {
    throw new InboundQueueError('invalid_request', `${name} is required`, 400);
  }
  return value;
}

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function now() {
  return new Date().toISOString();
}

export class SourcePendingSnapshot {
  constructor(fields = {}) {
    Object.assign(this, {
      status: 'snapshot',
      validated: false,
      drain_complete: false,
      close_generation: null,
      queue_epoch: null,
      bridge_instance: null,
      owner_epoch: null,
      watermark: 0,
      pending: 0,
      preclose_pending: 0,
      postclose_retained: 0,
      active_claims: 0,
      accepted: 0,
      unknown: 0,
      acked: 0,
      storage_fault: null,
      hidden_head: false,
      owner_binding: null,
      producer_state: 'not_required',
      producer_receipt_digest: null,
      ...fields,
    });
  }

  get ready() {
    return Boolean(
      this.validated
      && this.drain_complete
      && !this.storage_fault
      && !this.hidden_head
      && this.active_claims === 0
      && this.accepted === 0
      && this.unknown === 0
      && (
        this.producer_state === 'not_required'
        || (this.producer_state === 'connected' && typeof this.producer_receipt_digest === 'string'
          && this.producer_receipt_digest.length > 0)
      )
    );
  }

  toJSON() {
    return { ...this };
  }
}

export class InboundQueue {
  constructor({ sessionDir, owner = {} } = {}) {
    if (typeof sessionDir !== 'string' || !sessionDir.trim()) {
      throw new TypeError('sessionDir is required');
    }
    this.sessionDir = path.resolve(sessionDir);
    this.root = path.join(this.sessionDir, QUEUE_DIR);
    this.itemsDir = path.join(this.root, ITEMS_DIR);
    this.operationsDir = path.join(this.root, OPERATIONS_DIR);
    this.preparedDir = path.join(this.root, PREPARED_DIR);
    this.metaPath = path.join(this.root, META_FILE);
    this._fault = null;
    this.owner = { ...owner };
    this.capabilityRequired = owner.capabilityRequired === true;
    this._capability = typeof owner.capability === 'string' && owner.capability.length >= 24
      ? owner.capability
      : randomBytes(32).toString('base64url');
    this._witnessKey = Buffer.from(String(owner.witnessKey || this._capability), 'utf8');
    if (this._witnessKey.length < 16) throw new TypeError('source witness key is required');
    this._capabilityDigest = capabilityDigest(this._capability);
    this._serverConsumerId = randomUUID();
    this._serverConsumerEpoch = 1;
    this._producerGeneration = null;
    this._listenerReady = owner.listenerReady === true;
    this._ownerReady = owner.ownerReady === true;
    // Production holds this owner transaction for its lifetime; fixtures may opt out.
    this._holdOwnerLock = owner.holdOwnerLock === true;
    this._mutationDepth = 0;
    this._mutationLock = new QueueMutationLock(path.join(this.root, 'mutation.sqlite3'));
    this._ownerLock = new QueueMutationLock(path.join(this.root, 'owner-lock.sqlite3'));
    this._initialise(owner);
  }

  _withMutationLock(callback) {
    if (this._mutationDepth > 0) {
      this._mutationDepth += 1;
      try { return callback(); } finally { this._mutationDepth -= 1; }
    }
    try {
      return this._mutationLock.run(() => {
        this._mutationDepth = 1;
        try { return callback(); } finally { this._mutationDepth = 0; }
      });
    } catch (error) {
      if (error instanceof QueueLockError) {
        throw new InboundQueueError(error.code, error.message, error.code === 'queue_busy' ? 409 : 503);
      }
      throw error;
    }
  }

  _withConsistentRead(callback) {
    return this._withMutationLock(callback);
  }

  _defaultProducerReadiness() {
    return {
      source: this.capabilityRequired ? 'baileys' : null,
      state: this.capabilityRequired ? 'unknown' : 'not_required',
      generation: null,
      receipt: null,
    };
  }

  _producerReadiness(metadata = this.meta) {
    if (!this.capabilityRequired) return this._defaultProducerReadiness();
    const value = metadata?.producer_readiness;
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return this._defaultProducerReadiness();
    }
    if (value.source !== 'baileys') return this._defaultProducerReadiness();
    const state = PRODUCER_STATES.has(value.state) && value.state !== 'not_required'
      ? value.state
      : 'unknown';
    const generation = typeof value.generation === 'string' && value.generation ? value.generation : null;
    const receipt = value.receipt && typeof value.receipt === 'object' && !Array.isArray(value.receipt)
      ? value.receipt
      : null;
    if (!generation || (state !== 'connected' && receipt !== null)) {
      return this._defaultProducerReadiness();
    }
    if (state === 'connected') {
      const current = this.currentOwnerFence();
      if (!receipt || receipt.source !== 'baileys' || receipt.generation !== generation
        || receipt.owner_pid !== (this.meta.owner_pid ?? null)
        || receipt.owner_start !== (this.meta.owner_start ?? null)
        || receipt.session_digest !== current.session_digest
        || receipt.canonical_port !== current.canonical_port
        || receipt.script_sha256 !== current.script_sha256
        || receipt.binding_digest !== current.binding_digest
        || receipt.queue_epoch !== current.queue_epoch
        || receipt.bridge_instance !== current.bridge_instance
        || receipt.owner_epoch !== current.owner_epoch
        || receipt.profile_digest !== current.profile_digest
        || receipt.process_start !== current.process_start
        || receipt.attach_key_id !== current.attach_key_id
        || receipt.source_witness_key_id !== current.source_witness_key_id
        || typeof receipt.receipt_digest !== 'string' || !receipt.receipt_digest
        || receipt.receipt_digest !== fingerprint({
          source: receipt.source,
          generation: receipt.generation,
          owner_pid: receipt.owner_pid,
          owner_start: receipt.owner_start,
          session_digest: receipt.session_digest,
          profile_digest: receipt.profile_digest,
          canonical_port: receipt.canonical_port,
          script_sha256: receipt.script_sha256,
          process_start: receipt.process_start,
          queue_epoch: receipt.queue_epoch,
          bridge_instance: receipt.bridge_instance,
          owner_epoch: receipt.owner_epoch,
          attach_key_id: receipt.attach_key_id,
          source_witness_key_id: receipt.source_witness_key_id,
          binding_digest: receipt.binding_digest,
        })) {
        return this._defaultProducerReadiness();
      }
    }
    return { source: 'baileys', state, generation, receipt };
  }

  _commitProducerReadiness(readiness) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this._commitProducerReadiness(readiness));
    }
    if (!this.capabilityRequired) return true;
    try {
      this.meta = nextMutationMeta({ ...this.meta, producer_readiness: readiness });
      this._writeMeta();
      return true;
    } catch (error) {
      this.meta = {
        ...this.meta,
        producer_readiness: {
          source: this.capabilityRequired ? 'baileys' : null,
          state: this.capabilityRequired ? 'unknown' : 'not_required',
          generation: readiness.generation ?? null,
          receipt: null,
        },
      };
      this._setFault('producer_readiness_commit_failed', error.message);
      return false;
    }
  }

  _recordProducerConnection(generation, connection) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this._recordProducerConnection(generation, connection));
    }
    if (!this.capabilityRequired || generation !== this._producerGeneration) return false;
    let state = 'unknown';
    if (connection === 'open') state = 'connected';
    else if (connection === 'close') state = 'disconnected';

    let receipt = null;
    if (state === 'connected') {
      const fence = this.currentOwnerFence();
      const identity = {
        source: 'baileys',
        generation,
        owner_pid: this.meta.owner_pid ?? null,
        owner_start: this.meta.owner_start ?? null,
        ...fence,
      };
      receipt = { ...identity, receipt_digest: fingerprint(identity), observed_at: now() };
    }
    return this._commitProducerReadiness({
      source: 'baileys',
      state,
      generation,
      receipt,
    });
  }

  createProducerLifecycle() {
    const generation = randomUUID();
    this._producerGeneration = generation;
    if (!this.capabilityRequired) {
      return Object.freeze({ generation, update: () => true });
    }
    this._commitProducerReadiness({
      source: 'baileys',
      state: 'unknown',
      generation,
      receipt: null,
    });
    return Object.freeze({
      generation,
      update: (connection) => this._recordProducerConnection(generation, connection),
    });
  }

  _producerReceiptMatches(metadata) {
    if (!this.capabilityRequired) return true;
    const readiness = this._producerReadiness(metadata);
    const receipt = readiness.receipt;
    const current = this.currentOwnerFence();
    const closeReceipt = metadata?.close_receipt;
    return readiness.state === 'connected'
      && readiness.generation
      && readiness.generation === this._producerGeneration
      && receipt
      && receipt.source === 'baileys'
      && receipt.generation === readiness.generation
      && receipt.owner_pid === (this.meta.owner_pid ?? null)
      && receipt.owner_start === (this.meta.owner_start ?? null)
      && receipt.session_digest === current.session_digest
      && receipt.canonical_port === current.canonical_port
      && receipt.script_sha256 === current.script_sha256
      && receipt.binding_digest === current.binding_digest
      && receipt.queue_epoch === current.queue_epoch
      && receipt.bridge_instance === current.bridge_instance
      && receipt.owner_epoch === current.owner_epoch
      && receipt.profile_digest === current.profile_digest
      && receipt.process_start === current.process_start
      && receipt.attach_key_id === current.attach_key_id
      && receipt.source_witness_key_id === current.source_witness_key_id
      && receipt.receipt_digest === fingerprint({
        source: receipt.source,
        generation: receipt.generation,
        owner_pid: receipt.owner_pid,
        owner_start: receipt.owner_start,
        session_digest: receipt.session_digest,
        profile_digest: receipt.profile_digest,
        canonical_port: receipt.canonical_port,
        script_sha256: receipt.script_sha256,
        process_start: receipt.process_start,
        queue_epoch: receipt.queue_epoch,
        bridge_instance: receipt.bridge_instance,
        owner_epoch: receipt.owner_epoch,
        attach_key_id: receipt.attach_key_id,
        source_witness_key_id: receipt.source_witness_key_id,
        binding_digest: receipt.binding_digest,
      })
      && closeReceipt?.producer_generation === readiness.generation
      && closeReceipt?.producer_receipt_digest === receipt.receipt_digest;
  }

  _producerSnapshotFields(metadata = this.meta) {
    const readiness = this._producerReadiness(metadata);
    return {
      producer_state: readiness.state,
      producer_generation: readiness.generation,
      producer_receipt_digest: readiness.receipt?.receipt_digest ?? null,
    };
  }

  get queueEpoch() {
    return this.meta.queue_epoch;
  }

  get bridgeInstance() {
    return this.meta.bridge_instance;
  }

  get ownerEpoch() {
    return this.meta.owner_epoch;
  }

  get capability() {
    return this._capability;
  }

  get capabilityDigest() {
    return this._capabilityDigest;
  }

  requireCapability(value) {
    if (!this.capabilityRequired) return true;
    if (typeof value !== 'string') throw new InboundQueueError('capability_missing', 'bridge capability is required', 401);
    let digest;
    try { digest = capabilityDigest(value); } catch {
      throw new InboundQueueError('capability_stale', 'bridge capability is stale or foreign', 409);
    }
    if (!safeEqualHex(digest, this._capabilityDigest)) {
      throw new InboundQueueError('capability_stale', 'bridge capability is stale or foreign', 409);
    }
    return true;
  }

  authenticatedBinding(serverBinding = {}, capability) {
    this.requireCapability(capability);
    return {
      ...serverBinding,
      consumer_id: this._serverConsumerId,
      consumer_epoch: this._serverConsumerEpoch,
    };
  }

  currentOwnerFence() {
    if (this._mutationDepth === 0) {
      return this._withConsistentRead(() => this.currentOwnerFence());
    }
    const binding = {
      session_digest: this.meta.session_digest ?? null,
      profile_digest: this.meta.profile_digest ?? null,
      canonical_port: this.meta.owner_port ?? null,
      script_sha256: this.meta.script_sha256 ?? null,
      process_start: this.meta.process_start ?? this.meta.owner_start ?? null,
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      attach_key_id: this.meta.attach_key_id ?? null,
      source_witness_key_id: this.meta.source_witness_key_id ?? null,
    };
    return { ...binding, binding_digest: ownerBindingDigest(binding) };
  }

  _validateOwnerFence(ownerFence) {
    if (!this.capabilityRequired) return;
    if (!ownerFence || typeof ownerFence !== 'object') {
      throw new InboundQueueError('owner_fence_missing', 'source owner fence is required', 409);
    }
    const current = this.currentOwnerFence();
    for (const field of [
      'session_digest', 'profile_digest', 'canonical_port', 'script_sha256', 'process_start',
      'queue_epoch', 'bridge_instance', 'owner_epoch', 'attach_key_id', 'source_witness_key_id',
    ]) {
      if (String(ownerFence[field] ?? '') !== String(current[field] ?? '')) {
        throw new InboundQueueError('owner_fence_stale', 'source owner fence is stale or foreign', 409);
      }
    }
    if (!safeEqualHex(String(ownerFence.binding_digest || ''), current.binding_digest)) {
      throw new InboundQueueError('owner_fence_invalid', 'source owner fence is malformed', 409);
    }
    let durable;
    try { durable = JSON.parse(readFileSync(this.metaPath, 'utf8')); } catch {
      throw new InboundQueueError('owner_fence_unknown', 'durable owner metadata is unavailable', 503);
    }
    if (!durable || durable.queue_epoch !== current.queue_epoch
      || durable.bridge_instance !== current.bridge_instance
      || durable.owner_epoch !== current.owner_epoch
      || durable.owner_ready !== true || durable.listener_ready !== true) {
      throw new InboundQueueError('owner_fence_stale', 'durable owner fence is stale', 409);
    }
  }

  _initialise(owner) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this._initialise(owner));
    }
    return this._initialiseUnlocked(owner);
  }

  _initialiseUnlocked(owner) {
    try {
      mkdirSync(this.sessionDir, { recursive: true });
      mkdirSync(this.root, { recursive: true });
      mkdirSync(this.itemsDir, { recursive: true });
      mkdirSync(this.operationsDir, { recursive: true });
      mkdirSync(this.preparedDir, { recursive: true });
      if (this._holdOwnerLock) this._ownerLock.acquire();
    } catch (error) {
      throw new InboundQueueError('storage_fault', `queue directory unavailable: ${error.message}`, 503);
    }

    const defaults = {
      version: 2,
      queue_epoch: randomUUID(),
      bridge_instance: randomUUID(),
      owner_epoch: 1,
      owner_pid: owner.pid ?? process.pid,
      owner_start: owner.startedAt ?? now(),
      next_sequence: 1,
      last_sequence: 0,
      compacted_through: 0,
      intake_state: 'open',
      close_generation: null,
      close_receipt: null,
      mutation_version: 0,
      storage_fault: null,
      owner_port: owner.canonicalPort ?? null,
      process_start: owner.processStart ?? null,
      session_digest: owner.sessionDigest ?? null,
      profile_digest: owner.profileDigest ?? null,
      script_sha256: owner.scriptHash ?? null,
      attach_key_id: owner.attachKeyId ?? null,
      source_witness_key_id: owner.sourceWitnessKeyId ?? null,
      listener_ready: owner.listenerReady === true,
      owner_ready: owner.ownerReady === true,
      capability_digest: this._capabilityDigest,
      producer_readiness: this._defaultProducerReadiness(),
      durability: { file_fsync: 'performed', rename: 'performed', directory_sync: 'unknown' },
      owner_handoff: null,
    };

    if (existsSync(this.metaPath)) {
      try {
        const previous = JSON.parse(readFileSync(this.metaPath, 'utf8'));
        this.meta = { ...defaults, ...previous };
        if (typeof this.meta.queue_epoch !== 'string' || !this.meta.queue_epoch) throw new Error('queue epoch missing');
        // Recover prepared work against the exact metadata durable at shutdown.
        this._recoverPrepared();
        if (this._fault || this.meta.storage_fault) return;
        const priorHandoff = this.meta.owner_handoff;
        const pendingHandoff = priorHandoff && typeof priorHandoff === 'object'
          && priorHandoff.protocol === ADOPT_PROTOCOL
          && priorHandoff.state === 'pending'
          && typeof priorHandoff.predecessor_digest === 'string'
          && /^[0-9a-f]{64}$/.test(priorHandoff.predecessor_digest)
          && typeof priorHandoff.adoption_nonce === 'string'
          && priorHandoff.adoption_nonce.length >= 24
          ? { ...priorHandoff } : null;
        if (priorHandoff && !pendingHandoff && priorHandoff.state !== 'adopted') {
          throw new Error('owner handoff is malformed');
        }
        if (!pendingHandoff) {
          const predecessor = {
            queue_epoch: this.meta.queue_epoch,
            bridge_instance: this.meta.bridge_instance,
            owner_epoch: this.meta.owner_epoch,
            owner_pid: this.meta.owner_pid,
            owner_start: this.meta.owner_start,
            owner_port: this.meta.owner_port ?? null,
            session_digest: this.meta.session_digest ?? null,
            profile_digest: this.meta.profile_digest ?? null,
            script_sha256: this.meta.script_sha256 ?? null,
          };
          this.meta.owner_handoff = {
            protocol: ADOPT_PROTOCOL,
            state: 'pending',
            predecessor_digest: fingerprint(predecessor),
            predecessor,
            adoption_nonce: randomBytes(18).toString('base64url'),
            created_at: now(),
          };
        } else {
          // A previous owner may have prepared this exact adoption envelope
          // before the process stopped. Preserve it until the new owner proves
          // the attach and adoption MAC; generating a replacement here would
          // strand the predecessor nonce and make a valid restart unrecoverable.
          this.meta.owner_handoff = pendingHandoff;
        }
        this.meta.bridge_instance = randomUUID();
        this.meta.owner_epoch = Number.isInteger(this.meta.owner_epoch) ? this.meta.owner_epoch + 1 : 1;
        this.meta.owner_pid = owner.pid ?? process.pid;
        this.meta.owner_start = owner.startedAt ?? now();
        this.meta.process_start = owner.processStart ?? this.meta.process_start ?? this.meta.owner_start;
        this.meta.version = Math.max(2, Number(this.meta.version) || 1);
        this.meta.owner_port = owner.canonicalPort ?? this.meta.owner_port ?? null;
        this.meta.session_digest = owner.sessionDigest ?? this.meta.session_digest ?? null;
        this.meta.profile_digest = owner.profileDigest ?? this.meta.profile_digest ?? null;
        this.meta.script_sha256 = owner.scriptHash ?? this.meta.script_sha256 ?? null;
        this.meta.capability_digest = this._capabilityDigest;
        this.meta.attach_key_id = owner.attachKeyId ?? this.meta.attach_key_id ?? null;
        this.meta.source_witness_key_id = owner.sourceWitnessKeyId ?? this.meta.source_witness_key_id ?? null;
        this.meta.listener_ready = owner.listenerReady === true;
        this.meta.owner_ready = owner.ownerReady === true;
        // A new bridge instance owns a new producer generation.  A durable
        // connected receipt from the predecessor is never reused on restart.
        this.meta.producer_readiness = this._defaultProducerReadiness();
        this.meta = nextMutationMeta(this.meta);
        this._writeMeta();
      } catch (error) {
        this.meta = { ...defaults, intake_state: 'storage_fault' };
        this._setFault('metadata_corrupt', error.message, false);
      }
    } else {
      this.meta = defaults;
      this._writeMeta();
    }
  }

  _directorySync(directory) {
    let fd;
    try {
      fd = openSync(directory, 'r');
    } catch (error) {
      if (process.platform === 'win32' && ['EISDIR', 'EINVAL', 'EPERM', 'ENOTSUP'].includes(error?.code)) {
        return 'unsupported';
      }
      throw error;
    }
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    return 'performed';
  }

  _writeAtomic(file, value) {
    const directory = path.dirname(file);
    mkdirSync(directory, { recursive: true });
    const temporary = `${file}.tmp-${process.pid}-${randomUUID()}`;
    let fd;
    try {
      fd = openSync(temporary, 'wx', 0o600);
      const bytes = Buffer.from(`${JSON.stringify(value)}\n`, 'utf8');
      writeSync(fd, bytes, 0, bytes.length);
      fsyncSync(fd);
      closeSync(fd);
      fd = undefined;
      renameSync(temporary, file);
      const directorySync = this._directorySync(directory);
      if (this.meta?.durability) this.meta.durability.directory_sync = directorySync;
    } catch (error) {
      if (fd !== undefined) {
        try { closeSync(fd); } catch {}
      }
      try { unlinkSync(temporary); } catch {}
      throw error;
    }
  }

  _writeMeta() {
    this._writeAtomic(this.metaPath, this.meta);
  }

  _setFault(code, detail, persist = true) {
    this._fault = { code, detail: String(detail || code), at: now() };
    this.meta = this.meta || {
      version: 1,
      queue_epoch: randomUUID(),
      bridge_instance: randomUUID(),
      owner_epoch: 1,
      next_sequence: 1,
      last_sequence: 0,
      compacted_through: 0,
      intake_state: 'storage_fault',
    };
    this.meta.storage_fault = this._fault;
    this.meta.intake_state = 'storage_fault';
    if (persist) {
      try { this._writeMeta(); } catch {}
    }
  }

  _ensureHealthy() {
    if (this._fault || this.meta.storage_fault || this.meta.intake_state === 'storage_fault') {
      throw new InboundQueueError(
        'storage_fault',
        this._fault?.detail || this.meta.storage_fault?.detail || 'inbound queue storage fault',
        503,
        { storage_fault: clone(this._fault || this.meta.storage_fault) },
      );
    }
  }

  _readJson(file) {
    try {
      const value = JSON.parse(readFileSync(file, 'utf8'));
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('JSON object required');
      return value;
    } catch (error) {
      this._setFault('record_corrupt', `${file}: ${error.message}`);
      throw new InboundQueueError('storage_fault', `queue record is corrupt: ${path.basename(file)}`, 503);
    }
  }

  _itemPath(sequence, itemId) {
    return path.join(this.itemsDir, `${String(sequence).padStart(16, '0')}-${itemId}.json`);
  }

  _operationPath(requestId) {
    validateRequestId(requestId);
    return path.join(this.operationsDir, requestFileName(requestId));
  }

  _legacyOperationPath(requestId) {
    validateRequestId(requestId);
    return path.join(this.operationsDir, `${requestId}.json`);
  }

  _preparedPath(requestId) {
    validateRequestId(requestId);
    return path.join(this.preparedDir, requestFileName(requestId));
  }

  _legacyPreparedPath(requestId) {
    validateRequestId(requestId);
    return path.join(this.preparedDir, `${requestId}.json`);
  }

  _readOperation(requestId) {
    const file = this._operationPath(requestId);
    if (existsSync(file)) return this._readJson(file);
    const legacy = this._legacyOperationPath(requestId);
    return existsSync(legacy) ? this._readJson(legacy) : null;
  }

  _beginRequest(requestId, operation) {
    let id;
    try {
      id = validateRequestId(requiredString(requestId, 'request_id'));
    } catch {
      throw new InboundQueueError('invalid_request', 'request_id has an invalid format', 400);
    }
    const existing = this._readOperation(id);
    const requestFingerprint = fingerprint(operation);
    if (existing) {
      if (existing.fingerprint !== requestFingerprint) {
        throw new InboundQueueError('request_conflict', 'request_id was reused for a different operation', 409);
      }
      return { id, requestFingerprint, result: clone(existing.result) };
    }
    return { id, requestFingerprint, result: null };
  }

  _finishRequest(request, result) {
    this._writeAtomic(this._operationPath(request.id), {
      request_id: request.id,
      fingerprint: request.requestFingerprint,
      result: clone(result),
      committed_at: now(),
    });
    return result;
  }

  _writePrepared(request, journal) {
    this._writeAtomic(this._preparedPath(request.id), {
      request_id: request.id,
      fingerprint: request.requestFingerprint,
      journal_version: 2,
      evidence_version: 1,
      prior_meta_digest: metadataFingerprint(this.meta),
      next_mutation_version: Number.isInteger(journal.next_meta?.mutation_version)
        ? journal.next_meta.mutation_version : null,
      next_owner_handoff_digest: journal.next_meta?.owner_handoff
        ? fingerprint(journal.next_meta.owner_handoff) : null,
      ...journal,
      prepared_at: now(),
    });
  }

  _prepareRequest(request, journal) {
    try {
      this._writePrepared(request, journal);
    } catch (error) {
      this._setFault('prepare_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable operation preparation failed', 503);
    }
  }

  _removePrepared(requestId) {
    const file = this._preparedPath(requestId);
    const target = existsSync(file) ? file : this._legacyPreparedPath(requestId);
    if (!existsSync(target)) return;
    unlinkSync(target);
    this._directorySync(this.preparedDir);
  }

  _recoverPrepared() {
    for (const directory of [this.operationsDir, this.preparedDir, this.itemsDir]) {
      for (const name of readdirSync(directory)) {
        if (name.includes('.tmp-') || (!name.endsWith('.json') && name !== '.DS_Store')) {
          this._setFault('unresolved_queue_evidence', `${directory}/${name}`);
          return;
        }
      }
    }
    for (const fileName of readdirSync(this.preparedDir).filter((name) => name.endsWith('.json'))) {
      const file = path.join(this.preparedDir, fileName);
      let journal;
      try {
        journal = this._readJson(file);
      } catch {
        return;
      }
      const operation = this._readOperation(journal.request_id);
      if (operation) {
        if (operation.fingerprint !== journal.fingerprint) {
          this._setFault('prepared_request_conflict', journal.request_id);
          return;
        }
        this._removePrepared(journal.request_id);
        continue;
      }
      if (journal.evidence_version !== 1 || typeof journal.prior_meta_digest !== 'string'
        || journal.prior_meta_digest.length !== 64) {
        this._setFault('prepared_evidence_missing', journal.request_id);
        return;
      }
      const currentMetaDigest = metadataFingerprint(this.meta);
      const priorMetaMatches = journal.prior_meta_digest === currentMetaDigest;
      const nextMetaMatches = typeof journal.next_meta_digest === 'string'
        && journal.next_meta_digest.length === 64
        && journal.next_meta_digest === currentMetaDigest;
      if (!priorMetaMatches && !nextMetaMatches) {
        this._setFault('prepared_meta_mismatch', journal.request_id);
        return;
      }
      const nextMeta = journal.next_meta && typeof journal.next_meta === 'object'
        && !Array.isArray(journal.next_meta) ? journal.next_meta : null;
      if (nextMeta && metadataFingerprint(nextMeta) !== journal.next_meta_digest) {
        this._setFault('prepared_next_meta_mismatch', journal.request_id);
        return;
      }
      if (nextMeta && journal.next_mutation_version !== null
        && journal.next_mutation_version !== undefined
        && journal.next_mutation_version !== nextMeta.mutation_version) {
        this._setFault('prepared_mutation_version_mismatch', journal.request_id);
        return;
      }
      const commitNextMeta = () => {
        if (!priorMetaMatches) return true;
        if (!nextMeta) {
          this._setFault('prepared_next_meta_missing', journal.request_id);
          return false;
        }
        this.meta = clone(nextMeta);
        this._writeMeta();
        return true;
      };
      if (journal.kind === 'enqueue') {
        const expectedItem = journal.sequence && journal.item_id
          ? this._itemPath(journal.sequence, journal.item_id) : null;
        if (!journal.item_path || !expectedItem || path.resolve(journal.item_path) !== path.resolve(expectedItem)) {
          this._setFault('prepared_enqueue_missing', journal.request_id);
          return;
        }
        if (!existsSync(expectedItem) && priorMetaMatches && journal.next_item
          && typeof journal.next_item === 'object' && !Array.isArray(journal.next_item)) {
          this._writeAtomic(expectedItem, journal.next_item);
        }
        if (!existsSync(expectedItem)) {
          this._setFault('prepared_enqueue_missing', journal.request_id);
          return;
        }
        const item = this._readJson(journal.item_path);
        if (item.item_id !== journal.item_id || item.sequence !== journal.sequence
          || item.created_request_id !== journal.request_id) {
          this._setFault('prepared_enqueue_mismatch', journal.request_id);
          return;
        }
        if (!commitNextMeta()) return;
        this.meta.last_sequence = Math.max(this.meta.last_sequence || 0, journal.sequence);
        this.meta.next_sequence = Math.max(this.meta.next_sequence || 1, journal.next_sequence);
        if (priorMetaMatches) this._writeMeta();
        this._finishRequest({ id: journal.request_id, requestFingerprint: journal.fingerprint }, journal.result);
        this._removePrepared(journal.request_id);
        continue;
      }
      if (journal.kind === 'transition' || journal.kind === 'control') {
        if (journal.kind === 'transition' && journal.item_path && !existsSync(journal.item_path)
          && priorMetaMatches && journal.next_item && typeof journal.next_item === 'object'
          && !Array.isArray(journal.next_item)) {
          this._writeAtomic(journal.item_path, journal.next_item);
        }
        if (!commitNextMeta()) return;
        const item = journal.item_path && existsSync(journal.item_path) ? this._readJson(journal.item_path) : null;
        const stateRecovered = journal.kind === 'control'
          ? (this.meta.close_generation === journal.close_generation || this.meta.intake_state === journal.next_intake_state)
            && (!this.meta.close_receipt || this.meta.close_receipt.request_id === journal.request_id
              || journal.next_intake_state === 'open')
          : item && item.state === journal.next_state
            && Array.isArray(item.history)
            && item.history.some((entry) => entry.request_id === journal.request_id && entry.state === journal.next_state);
        if (!stateRecovered) {
          this._setFault('prepared_transition_incomplete', journal.request_id);
          return;
        }
        this._finishRequest({ id: journal.request_id, requestFingerprint: journal.fingerprint }, journal.result);
        this._removePrepared(journal.request_id);
        continue;
      }
      if (journal.kind === 'adopt') {
        if (!commitNextMeta()) return;
        const recovered = this.meta.owner_handoff?.adopted_predecessor_digest
          === journal.adopted_predecessor_digest
          && ['pending', 'adopted'].includes(this.meta.owner_handoff?.state);
        if (!recovered) {
          this._setFault('prepared_adoption_incomplete', journal.request_id);
          return;
        }
        this._finishRequest({ id: journal.request_id, requestFingerprint: journal.fingerprint }, journal.result);
        this._removePrepared(journal.request_id);
        continue;
      }
      this._setFault('prepared_kind_unknown', journal.request_id);
      return;
    }
  }

  _scanItems() {
    this._ensureHealthy();
    const items = [];
    const sequences = new Set();
    const ids = new Set();
    for (const fileName of readdirSync(this.itemsDir).filter((name) => name.endsWith('.json'))) {
      const item = this._readJson(path.join(this.itemsDir, fileName));
      if (!Number.isInteger(item.sequence) || item.sequence < 1 || typeof item.item_id !== 'string' || !item.item_id) {
        this._setFault('item_identity_invalid', fileName);
        throw new InboundQueueError('storage_fault', `invalid queue item: ${fileName}`, 503);
      }
      if (sequences.has(item.sequence) || ids.has(item.item_id)) {
        this._setFault('item_identity_duplicate', fileName);
        throw new InboundQueueError('storage_fault', `duplicate queue item: ${fileName}`, 503);
      }
      if (item.queue_epoch !== this.meta.queue_epoch) {
        this._setFault('item_epoch_mismatch', fileName);
        throw new InboundQueueError('storage_fault', `queue item belongs to another epoch: ${fileName}`, 503);
      }
      if (!ITEM_STATES.has(item.state)) {
        this._setFault('item_state_invalid', fileName);
        throw new InboundQueueError('storage_fault', `invalid queue item state: ${fileName}`, 503);
      }
      sequences.add(item.sequence);
      ids.add(item.item_id);
      items.push(item);
    }
    items.sort((left, right) => left.sequence - right.sequence);
    let expected = Math.max(1, (this.meta.compacted_through || 0) + 1);
    for (const item of items) {
      if (item.sequence !== expected) {
        this._setFault('sequence_gap', `expected ${expected}, found ${item.sequence}`);
        throw new InboundQueueError('storage_fault', 'queue sequence has a hidden head', 503);
      }
      expected += 1;
    }
    const expectedLast = Math.max(this.meta.last_sequence || 0, this.meta.compacted_through || 0);
    if (expected !== expectedLast + 1) {
      this._setFault('sequence_gap', `expected last ${expectedLast}, scanned through ${expected - 1}`);
      throw new InboundQueueError('storage_fault', 'queue sequence has a missing durable item', 503);
    }
    return items;
  }

  _validateBinding(binding = {}, { requireConsumer = false } = {}) {
    if (!binding || typeof binding !== 'object') {
      throw new InboundQueueError('invalid_binding', 'queue binding is required', 400);
    }
    if (binding.session_path && path.resolve(String(binding.session_path)) !== this.sessionDir) {
      throw new InboundQueueError('foreign_session', 'queue session binding is foreign', 409);
    }
    if (binding.queue_epoch !== this.meta.queue_epoch) {
      throw new InboundQueueError('stale_queue_epoch', 'queue epoch is stale', 409);
    }
    if (binding.bridge_instance !== this.meta.bridge_instance) {
      throw new InboundQueueError('stale_bridge_instance', 'bridge instance is stale', 409);
    }
    if (binding.owner_epoch !== this.meta.owner_epoch) {
      throw new InboundQueueError('stale_owner_epoch', 'queue owner epoch is stale', 409);
    }
    for (const [field, expected] of [
      ['session_digest', this.meta.session_digest],
      ['profile_digest', this.meta.profile_digest],
      ['canonical_port', this.meta.owner_port],
      ['script_sha256', this.meta.script_sha256],
      ['process_start', this.meta.process_start ?? this.meta.owner_start],
    ]) {
      if (expected !== null && expected !== undefined && String(binding[field] ?? '') !== String(expected)) {
        throw new InboundQueueError('foreign_binding', `${field} does not match the durable bridge owner`, 409);
      }
    }
    if (this.capabilityRequired && (binding.listener_ready !== true || binding.owner_ready !== true)) {
      throw new InboundQueueError('owner_not_ready', 'bridge owner is not ready', 409);
    }
    if (requireConsumer) {
      requiredString(binding.consumer_id, 'consumer_id');
      if (!Number.isInteger(binding.consumer_epoch) || binding.consumer_epoch < 1) {
        throw new InboundQueueError('invalid_binding', 'consumer_epoch is required', 400);
      }
    }
  }

  attach({ request_id: requestId, protocol, challenge_nonce: challengeNonce,
    binding_digest: bindingDigest, binding = {}, predecessor_digest: predecessorDigest = null } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.attach({ request_id: requestId, protocol,
        challenge_nonce: challengeNonce, binding_digest: bindingDigest, binding, predecessor_digest: predecessorDigest }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, {
      op: 'attach', protocol, challengeNonce, bindingDigest, binding, predecessorDigest,
    });
    if (request.result) return request.result;
    if (protocol !== ATTACH_PROTOCOL) {
      throw new InboundQueueError('invalid_request', 'attach protocol is required', 400);
    }
    this._validateBinding(binding, { requireConsumer: false });
    if (!this.meta.listener_ready || !this.meta.owner_ready) {
      throw new InboundQueueError('owner_not_ready', 'bridge owner is not ready to attach', 409);
    }
    const ownerBinding = this.currentOwnerFence();
    if (!safeEqualHex(String(bindingDigest || ''), ownerBinding.binding_digest)
      || binding.attach_key_id !== ownerBinding.attach_key_id
      || binding.source_witness_key_id !== ownerBinding.source_witness_key_id
      || binding.listener_ready !== true || binding.owner_ready !== true) {
      throw new InboundQueueError('attach_refused', 'private owner binding is stale or foreign', 409);
    }
    requiredString(challengeNonce, 'challenge_nonce');
    const fields = {
      protocol: ATTACH_PROTOCOL,
      request_id: request.id,
      challenge_nonce: challengeNonce,
      binding_digest: ownerBinding.binding_digest,
      predecessor_digest: predecessorDigest,
      attach_key_id: ownerBinding.attach_key_id,
      source_witness_key_id: ownerBinding.source_witness_key_id,
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      process_start: ownerBinding.process_start,
      proof_schema: 2,
      listener_ready: true,
      owner_ready: true,
    };
    const proof = signAttachProof(fields, Buffer.from(this._capability, 'utf8'));
    return this._finishRequest(request, {
      status: 'attached',
      request_id: request.id,
      binding: this._bindingForResult(binding),
      attach_proof: proof,
      owner_handoff: this.meta.owner_handoff ? {
        state: this.meta.owner_handoff.state,
        predecessor_digest: this.meta.owner_handoff.predecessor_digest,
        adoption_nonce: this.meta.owner_handoff.adoption_nonce,
      } : null,
    });
  }

  adopt({ request_id: requestId, protocol, predecessor_digest: predecessorDigest,
    adoption_nonce: adoptionNonce, queue_epoch: queueEpoch, binding = {}, attach_proof: attachProof } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.adopt({ request_id: requestId, protocol,
        predecessor_digest: predecessorDigest, adoption_nonce: adoptionNonce,
        queue_epoch: queueEpoch, binding, attach_proof: attachProof }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, {
      op: 'adopt', protocol, predecessor_digest: predecessorDigest,
      adoption_nonce: adoptionNonce, queue_epoch: queueEpoch, binding, attach_proof: attachProof,
    });
    if (request.result) return request.result;
    if (protocol !== ADOPT_PROTOCOL) throw new InboundQueueError('invalid_request', 'adoption protocol is required', 400);
    this._validateBinding(binding, { requireConsumer: true });
    const semantic = { queue_epoch: queueEpoch, predecessor_digest: predecessorDigest, adoption_nonce: adoptionNonce };
    const expectedRequestId = `adopt-${fingerprint(semantic)}`;
    if (request.id !== expectedRequestId) {
      throw new InboundQueueError('adoption_request_conflict', 'adoption request identity is not deterministic', 409);
    }
    const handoff = this.meta.owner_handoff;
    if (!handoff || handoff.state !== 'pending' || handoff.predecessor_digest !== predecessorDigest
      || handoff.adoption_nonce !== adoptionNonce || queueEpoch !== this.meta.queue_epoch) {
      throw new InboundQueueError('adoption_refused', 'owner handoff is not admissible', 409);
    }
    const expected = {
      predecessor_digest: handoff.predecessor_digest,
      queue_epoch: this.queueEpoch,
      bridge_instance: this.bridgeInstance,
      owner_epoch: this.ownerEpoch,
      process_start: this.meta.process_start ?? this.meta.owner_start,
      attach_key_id: this.meta.attach_key_id,
      source_witness_key_id: this.meta.source_witness_key_id,
      listener_ready: true,
      owner_ready: true,
      proof_schema: 2,
    };
    if (!verifyAttachProof(attachProof, Buffer.from(this.capability, 'utf8'), expected)) {
      throw new InboundQueueError('adoption_refused', 'private attach proof is invalid', 409);
    }
    const acceptedPredecessorDigest = handoff.predecessor_digest;
    const predecessor = {
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      owner_pid: this.meta.owner_pid,
      owner_start: this.meta.owner_start,
      owner_port: this.meta.owner_port ?? null,
      session_digest: this.meta.session_digest ?? null,
      profile_digest: this.meta.profile_digest ?? null,
      script_sha256: this.meta.script_sha256 ?? null,
      process_start: this.meta.process_start ?? null,
    };
    const nextHandoff = {
      protocol: ADOPT_PROTOCOL,
      state: 'adopted',
      predecessor_digest: fingerprint(predecessor),
      predecessor,
      adoption_nonce: randomBytes(18).toString('base64url'),
      created_at: now(),
      adopted_predecessor_digest: acceptedPredecessorDigest,
    };
    const nextMeta = nextMutationMeta({ ...this.meta, owner_handoff: nextHandoff });
    const result = { status: 'adopted', request_id: request.id,
      predecessor_digest: acceptedPredecessorDigest, queue_epoch: this.queueEpoch,
      snapshot: this.snapshot(binding, nextMeta) };
    this._prepareRequest(request, { kind: 'adopt', next_meta_digest: metadataFingerprint(nextMeta),
      adopted_predecessor_digest: acceptedPredecessorDigest, result });
    try {
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('adoption_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'owner adoption could not be committed', 503);
    }
  }

  markListenerReady({ binding = {}, listener_ready: listenerReady = true, owner_ready: ownerReady = true } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.markListenerReady({ binding,
        listener_ready: listenerReady, owner_ready: ownerReady }));
    }
    this._ensureHealthy();
    if (listenerReady !== true || ownerReady !== true) {
      throw new InboundQueueError('owner_not_ready', 'listener and owner readiness must be asserted together', 409);
    }
    this._validateBinding({ ...this._bindingForResult(binding), ...binding,
      listener_ready: true, owner_ready: true }, { requireConsumer: false });
    this.meta = nextMutationMeta({ ...this.meta, listener_ready: true, owner_ready: true,
      readiness_generation: this.meta.readiness_generation ? this.meta.readiness_generation + 1 : 1 });
    this._listenerReady = true;
    this._ownerReady = true;
    this._writeMeta();
    return { status: 'ready', queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance, owner_epoch: this.meta.owner_epoch,
      binding: this._bindingForResult(binding) };
  }

  _bindingForResult(binding) {
    return {
      session_path: this.sessionDir,
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      profile: binding?.profile ?? null,
      consumer_id: binding?.consumer_id ?? null,
      consumer_epoch: binding?.consumer_epoch ?? null,
      capability_digest: this._capabilityDigest,
      owner_pid: this.meta.owner_pid,
      owner_start: this.meta.owner_start,
      owner_port: this.meta.owner_port ?? null,
      session_digest: this.meta.session_digest ?? null,
      profile_digest: this.meta.profile_digest ?? null,
      script_sha256: this.meta.script_sha256 ?? null,
      process_start: this.meta.process_start ?? this.meta.owner_start ?? null,
      listener_ready: this.meta.listener_ready === true,
      owner_ready: this.meta.owner_ready === true,
      producer_generation: this._producerReadiness().generation,
    };
  }

  _itemResult(item) {
    if (!item) return null;
    const result = {
      sequence: item.sequence,
      item_id: item.item_id,
      state: item.state,
      payload: clone(item.payload),
      delivery: clone(item.delivery),
      wire_identity: clone(item.wire_identity),
      settlement_challenge: item.settlement_challenge ?? item.delivery?.settlement_challenge ?? null,
      identity_digest: item.wire_descriptor_digest || fingerprint(item.wire_identity || {}),
      queue_epoch: this.meta.queue_epoch,
      close_generation: item.close_generation ?? null,
    };
    // JSON persistence omits undefined fields.  Keep the in-memory response
    // byte/value shape equal to its durable replay, while retaining the durable
    // claim identity and settlement record whenever the item has one.
    if (item.claim_id !== undefined) {
      result.claim_id = item.claim_id;
    }
    if (item.settlement_record !== undefined) {
      result.settlement_record = clone(item.settlement_record);
    }
    return result;
  }

  enqueue(payload, { requestId = randomUUID(), wireIdentity = null, ownerFence = null } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.enqueue(payload, { requestId, wireIdentity, ownerFence }));
    }
    this._ensureHealthy();
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      throw new InboundQueueError('invalid_payload', 'wire payload is required', 400);
    }
    this._validateOwnerFence(ownerFence);
    const request = this._beginRequest(requestId, { op: 'enqueue', payload, wireIdentity });
    if (request.result) return request.result;
    const sequence = this.meta.next_sequence;
    const itemId = randomUUID();
    const item = {
      version: 1,
      sequence,
      item_id: itemId,
      queue_epoch: this.meta.queue_epoch,
      state: 'pending',
      payload: clone(payload),
      wire_identity: clone(wireIdentity),
      wire_descriptor_digest: fingerprint(wireIdentity || {}),
      source_identity_digest: null,
      occurrence: null,
      created_request_id: request.id,
      created_at: now(),
      close_generation: this.meta.intake_state === 'closing' ? this.meta.close_generation : null,
      post_close: this.meta.intake_state === 'closing',
      delivery: null,
      history: [{ state: 'pending', request_id: request.id, at: now() }],
    };
    const itemPath = this._itemPath(sequence, itemId);
    const result = {
      status: 'enqueued',
      request_id: request.id,
      item: this._itemResult(item),
      queue_epoch: this.meta.queue_epoch,
      owner_epoch: this.meta.owner_epoch,
    };
    const nextMeta = nextMutationMeta({
      ...this.meta, last_sequence: sequence, next_sequence: sequence + 1,
    });
    this._prepareRequest(request, {
      kind: 'enqueue',
      sequence,
      item_id: itemId,
      item_path: itemPath,
      next_sequence: sequence + 1,
      next_meta_digest: metadataFingerprint(nextMeta),
      next_meta: nextMeta,
      next_item: item,
      result,
    });
    try {
      this._writeAtomic(itemPath, item);
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('enqueue_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable enqueue failed', 503);
    }
  }

  claim({ request_id: requestId, binding, limit = 1 } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.claim({ request_id: requestId, binding, limit }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, {
      op: 'claim', binding, limit,
    });
    if (request.result) return request.result;
    this._validateBinding(binding, { requireConsumer: true });
    if (!Number.isInteger(limit) || limit !== 1) {
      throw new InboundQueueError('invalid_request', 'only one FIFO item may be claimed', 400);
    }
    if (this.meta.intake_state !== 'open') {
      const result = {
        status: 'blocked',
        reason: this.meta.intake_state === 'storage_fault' ? 'storage_fault' : 'source_closed',
        request_id: request.id,
        snapshot: this.snapshot(binding),
      };
      return this._finishRequest(request, result);
    }
    const items = this._scanItems();
    const item = items.find((candidate) => candidate.state !== 'acked');
    if (!item) {
      return this._finishRequest(request, {
        status: 'empty',
        request_id: request.id,
        snapshot: this.snapshot(binding),
      });
    }
    if (item.state !== 'pending') {
      return this._finishRequest(request, {
        status: 'blocked',
        reason: `head_${item.state}`,
        request_id: request.id,
        item: this._itemResult(item),
        snapshot: this.snapshot(binding),
      });
    }
    const claimId = randomUUID();
    const custodyRef = randomUUID();
    const next = {
      ...item,
      state: 'claimed',
      claim_id: claimId,
      claim_custody_ref: custodyRef,
      claim_request_id: request.id,
      claim_binding: this._bindingForResult(binding),
      claim_mode: 'normal',
      settlement_challenge: randomBytes(24).toString('base64url'),
      claimed_at: now(),
      history: [...(item.history || []), { state: 'claimed', request_id: request.id, at: now() }],
    };
    const result = {
      status: 'claimed',
      request_id: request.id,
      item: this._itemResult(next),
      claim_id: claimId,
      custody_ref: custodyRef,
      binding: this._bindingForResult(binding),
    };
    const itemPath = this._itemPath(item.sequence, item.item_id);
    const nextMeta = nextMutationMeta(this.meta);
    this._prepareRequest(request, {
      kind: 'transition',
      item_path: itemPath,
      expected_state: 'pending',
      next_state: 'claimed',
      next_meta_digest: metadataFingerprint(nextMeta),
      next_meta: nextMeta,
      next_item: next,
      result,
    });
    try {
      this._writeAtomic(itemPath, next);
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('claim_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable claim failed', 503);
    }
  }

  drainClaim({ request_id: requestId, protocol, close_generation: closeGeneration,
    watermark, close_receipt_digest: receiptDigest, limit = 1, binding } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.drainClaim({ request_id: requestId, protocol,
        close_generation: closeGeneration, watermark, close_receipt_digest: receiptDigest, limit, binding }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, {
      op: 'drain-claim', protocol, close_generation: closeGeneration, watermark, receiptDigest, limit,
    });
    if (request.result) return request.result;
    this._validateBinding(binding, { requireConsumer: true });
    if (protocol !== DRAIN_PROTOCOL) throw new InboundQueueError('invalid_request', 'drain protocol is required', 400);
    if (this.meta.intake_state !== 'closing') throw new InboundQueueError('source_open', 'pre-close drain requires a closing source', 409);
    if (closeGeneration !== this.meta.close_generation || watermark !== this.meta.close_receipt?.watermark
      || receiptDigest !== this.meta.close_receipt?.receipt_digest || limit !== 1) {
      throw new InboundQueueError('close_conflict', 'drain request does not match the durable close receipt', 409);
    }
    const items = this._scanItems();
    const head = items.find((candidate) => candidate.state !== 'acked' && candidate.sequence <= watermark);
    if (!head) {
      const snapshot = this.snapshot(binding);
      const result = { status: 'drained', request_id: request.id, close_generation: closeGeneration, watermark, snapshot };
      this._finishRequest(request, result);
      return result;
    }
    if (head.state !== 'pending') {
      const result = {
        status: 'blocked', request_id: request.id, reason: `head_${head.state}`,
        item: this._itemResult(head), snapshot: this.snapshot(binding),
      };
      this._finishRequest(request, result);
      return result;
    }
    const claimId = randomUUID();
    const custodyRef = randomUUID();
    const challenge = randomBytes(24).toString('base64url');
    const next = {
      ...head,
      state: 'claimed',
      claim_id: claimId,
      claim_custody_ref: custodyRef,
      claim_request_id: request.id,
      claim_mode: 'preclose_drain',
      close_generation: closeGeneration,
      drain_watermark: watermark,
      settlement_challenge: challenge,
      claim_binding: this._bindingForResult(binding),
      claimed_at: now(),
      history: [...(head.history || []), { state: 'claimed', op: 'preclose_drain', request_id: request.id, at: now() }],
    };
    const result = {
      status: 'claimed', request_id: request.id, claim_mode: 'preclose_drain',
      item: this._itemResult(next), claim_id: claimId, custody_ref: custodyRef,
      settlement_challenge: challenge, binding: this._bindingForResult(binding),
    };
    const itemPath = this._itemPath(head.sequence, head.item_id);
    const nextMeta = nextMutationMeta(this.meta);
    this._prepareRequest(request, { kind: 'transition', item_path: itemPath,
      expected_state: 'pending', next_state: 'claimed', next_meta_digest: metadataFingerprint(nextMeta),
      next_meta: nextMeta, next_item: next, result });
    try {
      this._writeAtomic(itemPath, next);
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('drain_claim_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable drain claim failed', 503);
    }
  }

  _validatedSettlement(item, settlement, requestId) {
    const hasFinalizerEvidence = settlement && (
      settlement.finalizer_status !== undefined
      || settlement.finalizer_receipt_digest !== undefined
      || settlement.producer_generation !== undefined
    );
    const requiresFinalizer = this.capabilityRequired || hasFinalizerEvidence;
    if (!settlement || settlement.producer_done !== true || settlement.native_exited !== true
      || settlement.transport_settled !== true
      || (requiresFinalizer && (
        settlement.finalizer_status !== 'success'
        || !/^[0-9a-f]{64}$/.test(String(settlement.finalizer_receipt_digest || ''))
      ))) {
      throw new InboundQueueError('settlement_incomplete', 'source finalizer settlement is required', 409);
    }
    if (!this.capabilityRequired) return { witness: null, fields: null };
    const witness = settlement.witness || settlement;
    if (typeof witness.witness_digest !== 'string') {
      throw new InboundQueueError('settlement_witness_invalid', 'typed source settlement witness is required', 409);
    }
    const fields = verifySettlement(witness, this._witnessKey);
    const canonicalFields = item.delivery?.settlement_fields || {};
    const canonicalProducerGeneration = canonicalFields.producer_generation;
    if (typeof canonicalProducerGeneration !== 'string' || !canonicalProducerGeneration
      || settlement.producer_generation !== canonicalProducerGeneration) {
      throw new InboundQueueError('settlement_witness_invalid', 'producer generation is stale or foreign', 409);
    }
    const expectedFields = {
      ...item.delivery?.settlement_fields,
      source_witness_key_id: sha256(this._witnessKey),
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      consumer_id: item.claim_binding?.consumer_id,
      consumer_epoch: item.claim_binding?.consumer_epoch,
      claim_id: item.claim_id,
      item_id: item.item_id,
      sequence: item.sequence,
      custody_ref: item.claim_custody_ref,
      settlement_challenge: String(item.settlement_challenge || item.delivery?.settlement_challenge || '-'),
      transport_status: 'success',
      request_id_digest: sha256(requestId),
      producer_generation: canonicalProducerGeneration,
      finalizer_receipt_digest: settlement.finalizer_receipt_digest,
      finalizer_status: 'success',
    };
    for (const name of [
      'bridge_binding_digest', 'wire_descriptor_digest', 'source_event_digest',
      'source_identity_digest', 'transport_owner_digest', 'transport_receipt_digest',
      'native_occurrence_id',
    ]) {
      const value = fields?.[name];
      const valid = name === 'native_occurrence_id'
        ? isValidSourceOccurrence(value)
        : typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
      const expected = expectedFields[name];
      if (!valid || typeof expected !== 'string' || expected === '-' || value !== expected) {
        throw new InboundQueueError('settlement_witness_invalid', `typed source field ${name} is malformed or does not match admission`, 409);
      }
      // These source-owned digests and the exact occurrence are authenticated
      // by the witness MAC and must equal the producer projection retained at
      // admission. Missing admission fields remain missing and fail closed;
      // the queue never replaces canonical source values with callback input.
    }
    if (!fields || fields.item_id !== item.item_id || fields.claim_id !== item.claim_id
      || fields.custody_ref !== item.claim_custody_ref
      || fields.queue_epoch !== this.meta.queue_epoch
      || fields.settlement_challenge !== expectedFields.settlement_challenge
      || fields.finalizer_status !== 'success'
      || !settlementFieldsMatch(fields, expectedFields)) {
      throw new InboundQueueError('settlement_witness_invalid', 'typed source settlement witness is invalid', 409);
    }
    return { witness, fields };
  }

  _validateAcceptedSettlementFields(_fields, item, binding) {
    if (!this.capabilityRequired) return null;
    const producerGeneration = this._producerReadiness().generation;
    if (typeof producerGeneration !== 'string' || !producerGeneration) {
      throw new InboundQueueError('producer_not_ready', 'source producer generation is required', 409);
    }
    const expected = {
      source_witness_key_id: null,
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      consumer_id: binding.consumer_id,
      consumer_epoch: binding.consumer_epoch,
      claim_id: item.claim_id,
      item_id: item.item_id,
      sequence: item.sequence,
      custody_ref: item.claim_custody_ref,
      settlement_challenge: item.settlement_challenge,
      close_generation: item.close_generation ?? null,
      registration_generation: item.delivery?.registration_generation ?? null,
      transport_status: 'pending',
      producer_generation: producerGeneration,
      bridge_binding_digest: digestBinding({
        queue_epoch: this.meta.queue_epoch,
        bridge_instance: this.meta.bridge_instance,
        owner_epoch: this.meta.owner_epoch,
        consumer_id: binding.consumer_id,
        consumer_epoch: binding.consumer_epoch,
        close_generation: item.close_generation ?? null,
      }),
      wire_descriptor_digest: item.wire_descriptor_digest,
    };
    if (_fields !== undefined && _fields !== null
      && (typeof _fields !== 'object' || Array.isArray(_fields))) {
      throw new InboundQueueError('settlement_witness_invalid', 'source settlement fields are malformed', 409);
    }
    const sourceFields = _fields && typeof _fields === 'object' && !Array.isArray(_fields) ? _fields : {};
    const sourceProjection = {};
    for (const name of SOURCE_DIGEST_FIELDS) {
      const value = sourceFields[name];
      if (value === undefined) continue;
      if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) {
        throw new InboundQueueError('settlement_witness_invalid', `source settlement field ${name} is malformed`, 409);
      }
      sourceProjection[name] = value;
    }
    if (sourceFields.native_occurrence_id !== undefined) {
      if (!isValidSourceOccurrence(sourceFields.native_occurrence_id)) {
        throw new InboundQueueError('settlement_witness_invalid', 'source settlement field native_occurrence_id is malformed', 409);
      }
      sourceProjection.native_occurrence_id = sourceFields.native_occurrence_id;
    }
    // Retain producer-owned fields only when supplied and valid. Missing fields
    // remain absent and therefore cannot satisfy the later exact settlement match.
    return { ...expected, ...sourceProjection };
  }

  transition({ request_id: requestId, op, binding, item_id: itemId, sequence, claim_id: claimId,
    expected_state: expectedState, source_identity: sourceIdentity, registration_generation: registrationGeneration,
    custody_ref: custodyRef, reason, settlement, settlement_fields: settlementFields,
    producer_generation: producerGeneration, finalizer_receipt_digest: finalizerReceiptDigest,
    finalizer_status: finalizerStatus } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.transition({ request_id: requestId, op, binding,
        item_id: itemId, sequence, claim_id: claimId, expected_state: expectedState,
        source_identity: sourceIdentity, registration_generation: registrationGeneration,
        custody_ref: custodyRef, reason, settlement, settlement_fields: settlementFields,
        producer_generation: producerGeneration, finalizer_receipt_digest: finalizerReceiptDigest,
        finalizer_status: finalizerStatus }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, {
      op, binding, item_id: itemId, sequence, claim_id: claimId, expected_state: expectedState,
      source_identity: sourceIdentity, registration_generation: registrationGeneration,
      custody_ref: custodyRef, reason, settlement, settlement_fields: settlementFields,
      producer_generation: producerGeneration, finalizer_receipt_digest: finalizerReceiptDigest,
      finalizer_status: finalizerStatus,
    });
    if (request.result) return request.result;
    this._validateBinding(binding, { requireConsumer: true });
    requiredString(op, 'op');
    requiredString(itemId, 'item_id');
    requiredInteger(sequence, 'sequence');
    const item = this._scanItems().find((candidate) => candidate.sequence === sequence && candidate.item_id === itemId);
    if (!item) throw new InboundQueueError('item_missing', 'queue item identity is unknown', 409);
    if (item.state !== expectedState) {
      throw new InboundQueueError('state_conflict', `expected ${expectedState}, found ${item.state}`, 409, { item: this._itemResult(item) });
    }
    if (item.claim_id !== claimId) {
      throw new InboundQueueError('claim_conflict', 'claim identity is foreign', 409);
    }
    if (item.claim_binding?.queue_epoch !== binding.queue_epoch
      || item.claim_binding?.bridge_instance !== binding.bridge_instance
      || item.claim_binding?.owner_epoch !== binding.owner_epoch
      || item.claim_binding?.consumer_id !== binding.consumer_id
      || item.claim_binding?.consumer_epoch !== binding.consumer_epoch) {
      throw new InboundQueueError('binding_conflict', 'consumer binding is foreign or stale', 409);
    }

    let nextState;
    let next = { ...item };
    if (op === 'accept') {
      nextState = 'accepted';
      if (sourceIdentity === undefined || sourceIdentity === null) {
        throw new InboundQueueError('source_identity_missing', 'source identity is required', 409);
      }
      if (!Number.isInteger(registrationGeneration) || registrationGeneration < 1) {
        throw new InboundQueueError('registration_generation_missing', 'registration generation is required', 409);
      }
      if (custodyRef !== item.claim_custody_ref) {
        throw new InboundQueueError('custody_conflict', 'source custody reference is not the live claim witness', 409);
      }
      next.delivery = {
        ...(item.delivery || {}),
        source_identity: clone(sourceIdentity),
        registration_generation: registrationGeneration ?? null,
        custody_ref: requiredString(custodyRef, 'custody_ref'),
        settlement_challenge: item.settlement_challenge || randomBytes(24).toString('base64url'),
        accepted_request_id: request.id,
      };
      if (this.capabilityRequired) {
        next.delivery.settlement_fields = this._validateAcceptedSettlementFields(settlementFields, next, binding);
      }
      next.settlement_challenge = next.delivery.settlement_challenge;
    } else if (op === 'release') {
      if (item.state !== 'claimed' || reason !== 'runtime_draining') {
        throw new InboundQueueError('release_forbidden', 'only an exact pre-admission draining refusal may release', 409);
      }
      nextState = 'pending';
      next.claim_id = null;
      next.claim_custody_ref = null;
      next.claim_request_id = null;
      next.claim_binding = null;
      next.released_request_id = request.id;
      next.release_reason = reason;
    } else if (op === 'hold') {
      nextState = 'unknown';
      next.unknown_reason = requiredString(reason, 'reason');
    } else if (op === 'settle') {
      if (item.state !== 'accepted') throw new InboundQueueError('settle_forbidden', 'only an accepted item may settle', 409);
      if (!custodyRef || item.delivery?.custody_ref !== custodyRef) {
        throw new InboundQueueError('custody_conflict', 'source custody reference is foreign', 409);
      }
      const verified = this._validatedSettlement(item, settlement, request.id);
      next.settlement_record = {
        request_id: request.id,
        request_id_digest: sha256(request.id),
        witness_digest: verified.witness?.witness_digest ?? null,
        finalizer_receipt_digest: settlement.finalizer_receipt_digest,
        finalizer_status: settlement.finalizer_status,
        producer_generation: settlement.producer_generation ?? null,
        settled_at: now(),
      };
      next.settlement = clone(settlement);
      nextState = 'accepted';
    } else if (op === 'ack') {
      if (item.state !== 'accepted') throw new InboundQueueError('ack_forbidden', 'only an accepted item may ACK', 409);
      if (!custodyRef || item.delivery?.custody_ref !== custodyRef) {
        throw new InboundQueueError('custody_conflict', 'source custody reference is foreign', 409);
      }
      if (this.capabilityRequired && (
        !item.settlement_record || item.settlement_record.finalizer_status !== 'success'
      )) {
        throw new InboundQueueError('settlement_incomplete', 'durable source settlement is required before ACK', 409);
      }
      if (item.settlement_record || (settlement !== undefined && settlement !== null)) {
        const verified = this._validatedSettlement(
          item, settlement, item.settlement_record?.request_id || request.id,
        );
        if (item.settlement_record && verified.witness
          && verified.witness.witness_digest !== item.settlement_record.witness_digest) {
          throw new InboundQueueError('settlement_witness_invalid', 'ACK witness does not match durable settlement', 409);
        }
      }
      next.settlement_witness_digest = item.settlement_record?.witness_digest ?? null;
      nextState = 'acked';
      next.acked_request_id = request.id;
      next.acked_at = now();
      next.settlement = clone(settlement);
    } else {
      throw new InboundQueueError('invalid_transition', `unsupported queue transition: ${op}`, 400);
    }
    next.state = nextState;
    next.history = [...(item.history || []), { state: nextState, op, request_id: request.id, at: now() }];
    const result = {
      status: 'transitioned',
      request_id: request.id,
      op,
      item: this._itemResult(next),
      binding: this._bindingForResult(binding),
    };
    const itemPath = this._itemPath(item.sequence, item.item_id);
    const nextMeta = nextMutationMeta(this.meta);
    this._prepareRequest(request, {
      kind: 'transition',
      item_path: itemPath,
      expected_state: expectedState,
      next_state: nextState,
      next_meta_digest: metadataFingerprint(nextMeta),
      next_meta: nextMeta,
      next_item: next,
      result,
    });
    try {
      this._writeAtomic(itemPath, next);
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('transition_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable transition failed', 503);
    }
  }

  control({ request_id: requestId, op, binding, close_generation: closeGeneration } = {}) {
    if (this._mutationDepth === 0) {
      return this._withMutationLock(() => this.control({ request_id: requestId, op, binding, close_generation: closeGeneration }));
    }
    this._ensureHealthy();
    const request = this._beginRequest(requestId, { op, binding, close_generation: closeGeneration });
    if (request.result) return request.result;
    this._validateBinding(binding, { requireConsumer: true });
    if (op !== 'close' && op !== 'resume') {
      throw new InboundQueueError('invalid_transition', 'control op must be close or resume', 400);
    }
    requiredInteger(closeGeneration, 'close_generation');
    if (op === 'close') {
      if (this.meta.intake_state === 'closing') {
        if (this.meta.close_generation !== closeGeneration) {
          throw new InboundQueueError('close_conflict', 'another close generation owns the queue', 409);
        }
        return this._finishRequest(request, {
          status: 'closing', request_id: request.id, receipt: clone(this.meta.close_receipt), snapshot: this.snapshot(binding),
        });
      }
      if (this.meta.intake_state !== 'open') throw new InboundQueueError('storage_fault', 'queue is not open', 503);
      const items = this._scanItems();
      const watermark = this.meta.last_sequence || (items.length ? items[items.length - 1].sequence : 0);
      const receipt = {
        close_generation: closeGeneration,
        queue_epoch: this.meta.queue_epoch,
        bridge_instance: this.meta.bridge_instance,
        owner_epoch: this.meta.owner_epoch,
        producer_generation: this._producerReadiness().generation,
        producer_receipt_digest: this._producerReadiness().receipt?.receipt_digest ?? null,
        consumer_binding: this._bindingForResult(binding),
        watermark,
        request_id: request.id,
        closed_at: now(),
      };
      receipt.receipt_digest = fingerprint(receipt);
      const nextMeta = nextMutationMeta({
        ...this.meta,
        intake_state: 'closing',
        close_generation: closeGeneration,
        close_receipt: receipt,
      });
      const result = {
        status: 'closing', request_id: request.id, receipt, snapshot: this.snapshot(binding, nextMeta),
      };
      this._prepareRequest(request, {
        kind: 'control',
        next_intake_state: 'closing',
        close_generation: closeGeneration,
        next_meta_digest: metadataFingerprint(nextMeta),
        next_meta: nextMeta,
        result,
      });
      try {
        this.meta = nextMeta;
        this._writeMeta();
        this._finishRequest(request, result);
        this._removePrepared(request.id);
        return result;
      } catch (error) {
        this._setFault('close_commit_failed', error.message);
        throw new InboundQueueError('storage_fault', 'durable close failed', 503);
      }
    }

    if (this.meta.intake_state !== 'closing' || this.meta.close_generation !== closeGeneration) {
      throw new InboundQueueError('resume_conflict', 'close generation is not resumable', 409);
    }
    const closeBinding = this.meta.close_receipt?.consumer_binding;
    if (closeBinding?.consumer_id !== binding.consumer_id || closeBinding?.consumer_epoch !== binding.consumer_epoch
      || closeBinding?.queue_epoch !== binding.queue_epoch || closeBinding?.bridge_instance !== binding.bridge_instance
      || closeBinding?.owner_epoch !== binding.owner_epoch) {
      throw new InboundQueueError('resume_conflict', 'resume owner binding is foreign', 409);
    }
    const nextMeta = nextMutationMeta({
      ...this.meta, intake_state: 'open', close_generation: null, close_receipt: null,
    });
    const result = {
      status: 'open', request_id: request.id, queue_epoch: this.meta.queue_epoch,
      owner_epoch: this.meta.owner_epoch, snapshot: this.snapshot(binding, nextMeta),
    };
    this._prepareRequest(request, {
      kind: 'control',
      next_intake_state: 'open',
      close_generation: closeGeneration,
      next_meta_digest: metadataFingerprint(nextMeta),
      next_meta: nextMeta,
      result,
    });
    try {
      this.meta = nextMeta;
      this._writeMeta();
      this._finishRequest(request, result);
      this._removePrepared(request.id);
      return result;
    } catch (error) {
      this._setFault('resume_commit_failed', error.message);
      throw new InboundQueueError('storage_fault', 'durable resume failed', 503);
    }
  }

  requestResult(requestId) {
    if (this._mutationDepth === 0) {
      return this._withConsistentRead(() => this.requestResult(requestId));
    }
    this._ensureHealthy();
    const id = requiredString(requestId, 'request_id');
    const operation = this._readOperation(id);
    if (!operation) throw new InboundQueueError('request_missing', 'request result is unknown', 404);
    return { status: 'request_result', request_id: id, result: clone(operation.result), fingerprint: operation.fingerprint };
  }

  snapshot(binding = {}, metadata = this.meta) {
    if (this._mutationDepth === 0) {
      return this._withConsistentRead(() => this.snapshot(binding, metadata));
    }
    const items = this._scanItems();
    const counts = { pending: 0, claimed: 0, accepted: 0, unknown: 0, acked: 0 };
    for (const item of items) counts[item.state] = (counts[item.state] || 0) + 1;
    const watermark = metadata.close_receipt?.watermark ?? metadata.last_sequence ?? 0;
    const pendingItems = items.filter((item) => item.state === 'pending');
    const precloseItems = items.filter((item) => item.sequence <= watermark);
    const hiddenHead = items.some((item, index) => index > 0 && item.sequence !== items[index - 1].sequence + 1);
    const producer = this._producerSnapshotFields(metadata);
    const baseValidated = metadata.intake_state === 'closing'
      && !this._fault
      && !metadata.storage_fault
      && !hiddenHead
      && precloseItems.every((item) => item.state === 'acked')
      && items.every((item) => item.queue_epoch === metadata.queue_epoch);
    const producerReady = this._producerReceiptMatches(metadata);
    return new SourcePendingSnapshot({
      close_generation: metadata.close_generation,
      close_receipt_digest: metadata.close_receipt?.receipt_digest ?? null,
      queue_epoch: metadata.queue_epoch,
      bridge_instance: metadata.bridge_instance,
      owner_epoch: metadata.owner_epoch,
      watermark,
      pending: counts.pending,
      preclose_pending: pendingItems.filter((item) => item.sequence <= watermark).length,
      preclose_active: precloseItems.filter((item) => item.state === 'claimed').length,
      preclose_accepted: precloseItems.filter((item) => item.state === 'accepted').length,
      preclose_unknown: precloseItems.filter((item) => item.state === 'unknown').length,
      postclose_retained: pendingItems.filter((item) => item.sequence > watermark).length,
      active_claims: counts.claimed,
      accepted: counts.accepted,
      unknown: counts.unknown,
      acked: counts.acked,
      storage_fault: this._fault || metadata.storage_fault || null,
      hidden_head: hiddenHead,
      owner_binding: this._bindingForResult(binding),
      ...producer,
      validated: baseValidated && producerReady,
      drain_complete: baseValidated && producerReady,
    });
  }

  health() {
    if (this._mutationDepth === 0) {
      return this._withConsistentRead(() => this.health());
    }
    const snapshot = this.snapshot();
    return {
      ...snapshot.toJSON(),
      queue_epoch: this.meta.queue_epoch,
      bridge_instance: this.meta.bridge_instance,
      owner_epoch: this.meta.owner_epoch,
      process_start: this.meta.process_start ?? this.meta.owner_start ?? null,
      owner_start: this.meta.owner_start ?? null,
      session_digest: this.meta.session_digest ?? null,
      profile_digest: this.meta.profile_digest ?? null,
      canonical_port: this.meta.owner_port ?? null,
      script_sha256: this.meta.script_sha256 ?? null,
      intake_state: this.meta.intake_state,
      owner: {
        pid: this.meta.owner_pid,
        start: this.meta.owner_start,
        port: this.meta.owner_port ?? null,
        session_digest: this.meta.session_digest ?? null,
        profile_digest: this.meta.profile_digest ?? null,
        script_sha256: this.meta.script_sha256 ?? null,
      },
      capability_required: this.capabilityRequired,
      capability_digest: this._capabilityDigest,
      attach_key_id: this.meta.attach_key_id ?? null,
      source_witness_key_id: this.meta.source_witness_key_id ?? null,
      listener_ready: this.meta.listener_ready === true,
      owner_ready: this.meta.owner_ready === true,
      consumer_id: this.capabilityRequired ? this._serverConsumerId : null,
      consumer_epoch: this.capabilityRequired ? this._serverConsumerEpoch : null,
      durability: this.meta.durability || { file_fsync: 'performed', rename: 'performed', directory_sync: 'unknown' },
      owner_handoff: this.meta.owner_handoff ? {
        protocol: this.meta.owner_handoff.protocol,
        state: this.meta.owner_handoff.state,
        predecessor_digest: this.meta.owner_handoff.predecessor_digest,
        queue_epoch: this.meta.queue_epoch,
        adoption_nonce_digest: fingerprint(this.meta.owner_handoff.adoption_nonce),
      } : null,
    };
  }
}

function sendQueueError(res, error) {
  const status = error instanceof InboundQueueError ? error.status : 503;
  const body = error instanceof InboundQueueError
    ? { status: 'error', error: error.code, message: error.message, ...error.details }
    : { status: 'error', error: 'storage_fault', message: 'inbound queue operation failed' };
  return res.status(status).json(body);
}

export function mountInboundQueueRoutes(app, queue, { binding } = {}) {
  const resolveQueue = () => (typeof queue === 'function' ? queue() : queue);
  const requireQueue = () => {
    const active = resolveQueue();
    if (!active) throw new InboundQueueError('startup_pending', 'bridge owner is not ready', 503);
    return active;
  };
  const requestBinding = (req, body = {}) => {
    const active = requireQueue();
    const supplied = body?.binding && typeof body.binding === 'object' ? body.binding : {};
    const server = typeof binding === 'function' ? binding(req) : {};
    const capability = req?.headers?.[CAPABILITY_HEADER] || req?.headers?.[CAPABILITY_HEADER.toLowerCase()];
    if (active.capabilityRequired) {
      return active.authenticatedBinding(server, capability);
    }
    return {
      ...server,
      ...supplied,
      session_path: supplied.session_path ?? server.session_path,
      queue_epoch: supplied.queue_epoch ?? server.queue_epoch,
      bridge_instance: supplied.bridge_instance ?? server.bridge_instance,
      owner_epoch: supplied.owner_epoch ?? server.owner_epoch,
    };
  };

  app.get('/messages', (req, res) => {
    try {
      const active = requireQueue();
      return res.json(active.snapshot(requestBinding(req)));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/drain-claim', (req, res) => {
    try {
      const active = requireQueue();
      return res.json(active.drainClaim({ ...req.body, binding: requestBinding(req, req.body) }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/claim', (req, res) => {
    try {
      const active = requireQueue();
      return res.json(active.claim({ ...req.body, binding: requestBinding(req, req.body) }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/transition', (req, res) => {
    try {
      const active = requireQueue();
      return res.json(active.transition({ ...req.body, binding: requestBinding(req, req.body) }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.get('/messages/requests/:requestId', (req, res) => {
    try {
      const active = requireQueue();
      if (active.capabilityRequired) active.requireCapability(req.headers?.[CAPABILITY_HEADER]);
      return res.json(active.requestResult(req.params.requestId));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/control', (req, res) => {
    try {
      const active = requireQueue();
      return res.json(active.control({ ...req.body, binding: requestBinding(req, req.body) }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/attach', (req, res) => {
    try {
      const active = requireQueue();
      const current = requestBinding(req, req.body);
      return res.json(active.attach({
        ...req.body,
        binding: current,
      }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });

  app.post('/messages/adopt', (req, res) => {
    try {
      const active = requireQueue();
      const current = requestBinding(req, req.body);
      return res.json(active.adopt({ ...req.body, binding: current }));
    } catch (error) {
      return sendQueueError(res, error);
    }
  });
}
