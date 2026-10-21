/**
 * Kernel-backed queue exclusion for the synchronous bridge process.
 *
 * The JSON files remain the source of truth.  This database contains only a
 * schema marker and is held in a BEGIN IMMEDIATE transaction while one
 * complete JSON mutation/read is performed.  It is deliberately not a
 * process/PID lock and has no stale-owner cleanup path.
 */
import { mkdirSync } from 'fs';
import path from 'path';
import { DatabaseSync } from 'node:sqlite';

export class QueueLockError extends Error {
  constructor(code, message, cause = undefined) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'QueueLockError';
    this.code = code;
  }
}

export function nextMutationMeta(metadata = {}) {
  return {
    ...metadata,
    mutation_version: (Number.isInteger(metadata?.mutation_version)
      ? metadata.mutation_version : 0) + 1,
  };
}

function classifyLockError(error) {
  const text = String(error?.message || error || '').toLowerCase();
  if (text.includes('busy') || text.includes('locked')) return 'queue_busy';
  return 'storage_fault';
}

export class QueueMutationLock {
  constructor(filename) {
    if (typeof filename !== 'string' || !filename) throw new TypeError('queue lock filename is required');
    this.filename = path.resolve(filename);
    this._database = null;
    this._depth = 0;
  }

  _open() {
    if (this._database) return this._database;
    mkdirSync(path.dirname(this.filename), { recursive: true });
    try {
      this._database = new DatabaseSync(this.filename);
      this._database.exec('PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0;');
      this._database.exec('CREATE TABLE IF NOT EXISTS hermes_lock_schema (version INTEGER NOT NULL);');
      const row = this._database.prepare('SELECT version FROM hermes_lock_schema LIMIT 1').get();
      if (row && row.version !== 1) throw new Error('unsupported queue lock schema');
      if (!row) this._database.prepare('INSERT INTO hermes_lock_schema(version) VALUES (1)').run();
      return this._database;
    } catch (error) {
      this.close();
      throw new QueueLockError(classifyLockError(error), 'queue lock database is unavailable', error);
    }
  }

  run(callback) {
    if (typeof callback !== 'function') throw new TypeError('queue lock callback is required');
    if (this._depth > 0) {
      this._depth += 1;
      try { return callback(); } finally { this._depth -= 1; }
    }
    const database = this._open();
    let callbackActive = false;
    try {
      database.exec('BEGIN IMMEDIATE');
      this._depth = 1;
      callbackActive = true;
      const result = callback();
      callbackActive = false;
      database.exec('COMMIT');
      return result;
    } catch (error) {
      try { database.exec('ROLLBACK'); } catch {}
      // Preserve application errors (including the queue's typed 4xx
      // refusals).  Only failures in the SQLite transaction boundary itself
      // are lock/storage errors; wrapping callback failures would turn stale,
      // foreign, and invalid requests into misleading storage faults.
      if (callbackActive || error instanceof QueueLockError) throw error;
      throw new QueueLockError(classifyLockError(error), 'queue mutation could not be serialized', error);
    } finally {
      this._depth = 0;
      this.close();
    }
  }

  acquire() {
    const database = this._open();
    if (this._depth > 0) {
      this._depth += 1;
      return this;
    }
    try {
      database.exec('BEGIN IMMEDIATE');
      this._depth = 1;
      return this;
    } catch (error) {
      this.close();
      throw new QueueLockError(classifyLockError(error), 'queue owner lock could not be acquired', error);
    }
  }

  release() {
    if (!this._depth) return;
    this._depth -= 1;
    if (this._depth) return;
    try {
      this._database?.exec('COMMIT');
    } catch (error) {
      try { this._database?.exec('ROLLBACK'); } catch {}
      throw new QueueLockError('storage_fault', 'queue owner lock commit failed', error);
    } finally {
      this.close();
    }
  }

  close() {
    if (!this._database) return;
    try { this._database.close(); } catch {}
    this._database = null;
    this._depth = 0;
  }
}
