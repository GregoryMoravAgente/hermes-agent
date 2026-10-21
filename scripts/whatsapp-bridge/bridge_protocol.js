/**
 * Small, dependency-free protocol primitives shared by the WhatsApp queue and
 * its bridge.  These functions deliberately keep identity in server-owned
 * records; callers can authenticate an operation but cannot supply a new
 * queue owner, occurrence, or terminal state.
 */
import { createHash, createHmac, timingSafeEqual } from 'crypto';

export const CAPABILITY_HEADER = 'x-hermes-bridge-capability';
export const BOOTSTRAP_PROTOCOL = 'hermes.whatsapp.bridge.bootstrap.v1';
export const ATTACH_PROTOCOL = 'hermes.whatsapp.attach.v2';
export const DRAIN_PROTOCOL = 'hermes.whatsapp.preclose-drain.v1';
export const ADOPT_PROTOCOL = 'hermes.whatsapp.adoption.v1';
export const SETTLEMENT_PROTOCOL = 'hermes.whatsapp.settlement.v1';
export const SOURCE_FIXTURE_PROTOCOL = 'hermes.whatsapp.source-fixture.v1';
export const UNIT_SEPARATOR = '\x1f';
export const RECORD_SEPARATOR = '\x1e';

export const OWNER_BINDING_FIELDS = Object.freeze([
  'session_digest', 'profile_digest', 'canonical_port', 'script_sha256',
  'process_start', 'queue_epoch', 'bridge_instance', 'owner_epoch',
  'attach_key_id', 'source_witness_key_id',
]);

export const ATTACH_PROOF_FIELDS = Object.freeze([
  'protocol', 'request_id', 'challenge_nonce', 'binding_digest',
  'predecessor_digest', 'attach_key_id', 'source_witness_key_id',
  'queue_epoch', 'bridge_instance', 'owner_epoch', 'process_start',
  'proof_schema', 'listener_ready', 'owner_ready',
]);

const REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const B64URL = /^[A-Za-z0-9_-]+$/;

function stable(value) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError('protocol identity must contain finite JSON numbers');
    return value;
  }
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError('protocol identity must contain JSON values');
    }
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  }
  throw new TypeError('protocol identity must contain JSON values');
}

export function canonicalJson(value) {
  return JSON.stringify(stable(value));
}

export function sha256(value) {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
  return createHash('sha256').update(bytes).digest('hex');
}

export function requestDigest(requestId) {
  validateRequestId(requestId);
  return sha256(Buffer.from(requestId, 'utf8'));
}

export function semanticOperationId(key, prefix = 'op') {
  if (!key || typeof key !== 'object' || Array.isArray(key)) throw new Error('operation identity is required');
  return `${prefix}-${sha256(canonicalJson(key))}`;
}

export function adoptionOperationId({ queue_epoch: queueEpoch, predecessor_digest: predecessorDigest, adoption_nonce: adoptionNonce } = {}) {
  if (![queueEpoch, predecessorDigest, adoptionNonce].every((value) => typeof value === 'string' && value)) {
    throw new Error('adoption identity is incomplete');
  }
  return semanticOperationId({ queue_epoch: queueEpoch, predecessor_digest: predecessorDigest, adoption_nonce: adoptionNonce }, 'adopt');
}

export function attachOperationId({ queue_epoch: queueEpoch, bridge_instance: bridgeInstance,
  owner_epoch: ownerEpoch, process_start: processStart, predecessor_digest: predecessorDigest = null } = {}) {
  if (![queueEpoch, bridgeInstance, ownerEpoch, processStart].every((value) => value !== null && value !== undefined && String(value))) {
    throw new Error('attach identity is incomplete');
  }
  return semanticOperationId({
    queue_epoch: queueEpoch, bridge_instance: bridgeInstance, owner_epoch: ownerEpoch,
    process_start: processStart, predecessor_digest: predecessorDigest,
  }, 'attach');
}

export function requestFileName(requestId) {
  return `${requestDigest(requestId)}.json`;
}

export function validateRequestId(requestId) {
  if (typeof requestId !== 'string' || !REQUEST_ID.test(requestId)) {
    throw new Error('invalid request_id');
  }
  if (requestId.includes('/') || requestId.includes('\\') || requestId.includes('\0')
    || requestId === '.' || requestId === '..' || /^[A-Za-z]:/.test(requestId)) {
    throw new Error('invalid request_id');
  }
  return requestId;
}

export function digestBinding(binding) {
  return sha256(canonicalJson(binding));
}

export function ownerBindingDigest(binding) {
  const selected = Object.fromEntries(OWNER_BINDING_FIELDS.map((name) => [name, binding?.[name] ?? null]));
  return digestBinding(selected);
}

export function proofBytes(fields) {
  return Buffer.from(canonicalJson(fields), 'utf8');
}

export function signAttachProof(fields, key) {
  if (!Buffer.isBuffer(key) || key.length < 16) throw new Error('attach proof key is required');
  const bytes = proofBytes(fields);
  return {
    protocol: ATTACH_PROTOCOL,
    proof_b64url: bytes.toString('base64url'),
    mac_hex: createHmac('sha256', key).update(bytes).digest('hex'),
    proof_digest: sha256(bytes),
  };
}

export function verifyAttachProof(envelope, key, expected = {}) {
  if (!envelope || typeof envelope !== 'object' || !Buffer.isBuffer(key) || key.length < 16) return null;
  if (envelope.protocol !== ATTACH_PROTOCOL || typeof envelope.proof_b64url !== 'string'
    || !B64URL.test(envelope.proof_b64url) || !HEX64.test(envelope.mac_hex)) return null;
  let bytes;
  try { bytes = Buffer.from(envelope.proof_b64url, 'base64url'); } catch { return null; }
  if (bytes.length < 2 || bytes.length > 4096) return null;
  const mac = createHmac('sha256', key).update(bytes).digest('hex');
  if (!safeEqualHex(mac, envelope.mac_hex)) return null;
  if (envelope.proof_digest && !safeEqualHex(sha256(bytes), envelope.proof_digest)) return null;
  let fields;
  try { fields = JSON.parse(bytes.toString('utf8')); } catch { return null; }
  if (!fields || typeof fields !== 'object' || Array.isArray(fields)) return null;
  for (const name of ATTACH_PROOF_FIELDS) {
    if (expected[name] !== undefined && String(fields[name] ?? '') !== String(expected[name] ?? '')) return null;
  }
  return fields;
}

export function capabilityDigest(capability) {
  if (typeof capability !== 'string' || capability.length < 24 || !B64URL.test(capability)) {
    throw new Error('invalid capability');
  }
  return sha256(capability);
}

export function safeEqualHex(left, right) {
  if (typeof left !== 'string' || typeof right !== 'string' || !HEX64.test(left) || !HEX64.test(right)) return false;
  return timingSafeEqual(Buffer.from(left, 'hex'), Buffer.from(right, 'hex'));
}

export const SETTLEMENT_FIELDS = Object.freeze([
  'source_witness_key_id', 'queue_epoch', 'bridge_instance', 'owner_epoch',
  'consumer_id', 'consumer_epoch', 'bridge_binding_digest', 'close_generation', 'claim_id', 'item_id',
  'sequence', 'custody_ref', 'source_event_digest', 'wire_descriptor_digest',
  'source_identity_digest', 'registration_generation', 'transport_owner_digest',
  'native_occurrence_id', 'transport_receipt_digest', 'transport_status',
  'settlement_challenge', 'request_id_digest', 'producer_generation',
  'finalizer_receipt_digest', 'finalizer_status',
]);

function boundedField(value, name) {
  const text = String(value ?? '');
  if (!text || text.length > 512 || /[\x00\x1e\x1f\r\n]/.test(text)) throw new Error(`invalid witness field: ${name}`);
  return text;
}

export function settlementBytes(fields) {
  const values = SETTLEMENT_FIELDS.map((name) => {
    const value = fields?.[name];
    return boundedField(value === null || value === undefined ? '-' : value, name);
  });
  return Buffer.from(`${values.join(UNIT_SEPARATOR)}${RECORD_SEPARATOR}`, 'utf8');
}

export function signSettlement(fields, key) {
  if (!Buffer.isBuffer(key) || key.length < 16) throw new Error('source witness key is required');
  const bytes = settlementBytes(fields);
  const mac = createHmac('sha256', key).update(bytes).digest('hex');
  return {
    protocol: SETTLEMENT_PROTOCOL,
    witness_b64url: bytes.toString('base64url'),
    mac_hex: mac,
    witness_digest: sha256(bytes),
  };
}

export function verifySettlement(envelope, key) {
  if (!envelope || typeof envelope !== 'object' || Object.keys(envelope).some((keyName) => !['protocol', 'witness_b64url', 'mac_hex', 'witness_digest'].includes(keyName))) return null;
  if (envelope.protocol !== SETTLEMENT_PROTOCOL || typeof envelope.witness_b64url !== 'string' || !B64URL.test(envelope.witness_b64url) || !HEX64.test(envelope.mac_hex)) return null;
  if (!Buffer.isBuffer(key) || key.length < 16) return null;
  let bytes;
  try { bytes = Buffer.from(envelope.witness_b64url, 'base64url'); } catch { return null; }
  if (bytes.length < 8 || bytes.length > 4096 || bytes.toString('utf8').at(-1) !== RECORD_SEPARATOR) return null;
  const expected = createHmac('sha256', key).update(bytes).digest('hex');
  if (!safeEqualHex(expected, envelope.mac_hex)) return null;
  if (envelope.witness_digest !== undefined && (!HEX64.test(envelope.witness_digest) || !safeEqualHex(sha256(bytes), envelope.witness_digest))) return null;
  const parts = bytes.toString('utf8').slice(0, -1).split(UNIT_SEPARATOR);
  if (parts.length !== SETTLEMENT_FIELDS.length) return null;
  return Object.fromEntries(SETTLEMENT_FIELDS.map((name, index) => [name, parts[index]]));
}

export function settlementFieldsMatch(fields, expected) {
  if (!fields || !expected) return false;
  return SETTLEMENT_FIELDS.every((name) => String(fields[name] ?? '-') === String(expected[name] ?? '-'));
}

export function parseJsonFrame(buffer, maxBytes = 16384) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 1 || buffer.length > maxBytes) throw new Error('invalid frame');
  const text = buffer.toString('utf8');
  const value = JSON.parse(text);
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('frame object required');
  return value;
}
