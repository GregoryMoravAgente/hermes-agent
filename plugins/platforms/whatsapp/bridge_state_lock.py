"""Small stdlib SQLite transaction helper shared by bridge authority readers."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path


logger = logging.getLogger(__name__)


class BridgeStateLockError(RuntimeError):
    """A busy or unreadable bridge state lock is ambiguous, never stale."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class BridgeStateLock:
    """Hold one SQLite BEGIN IMMEDIATE transaction for an owner lifetime."""

    def __init__(self, filename: Path) -> None:
        self.filename = Path(filename).resolve()
        self._connection: sqlite3.Connection | None = None
        self._depth = 0

    def _open(self) -> sqlite3.Connection:
        if self._connection is not None:
            return self._connection
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        try:
            connection = sqlite3.connect(self.filename, timeout=0, isolation_level=None)
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA busy_timeout=0")
            connection.execute("CREATE TABLE IF NOT EXISTS hermes_lock_schema (version INTEGER NOT NULL)")
            row = connection.execute("SELECT version FROM hermes_lock_schema LIMIT 1").fetchone()
            if row and row[0] != 1:
                raise BridgeStateLockError("storage_fault", "unsupported bridge lock schema")
            if row is None:
                connection.execute("INSERT INTO hermes_lock_schema(version) VALUES (1)")
            self._connection = connection
            return connection
        except sqlite3.OperationalError as exc:
            self.close()
            code = "owner_busy" if "locked" in str(exc).lower() or "busy" in str(exc).lower() else "storage_fault"
            raise BridgeStateLockError(code, "bridge state lock is unavailable") from exc
        except Exception:
            self.close()
            raise

    def acquire(self) -> BridgeStateLock:
        if self._depth:
            self._depth += 1
            return self
        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._depth = 1
            return self
        except sqlite3.OperationalError as exc:
            self.close()
            raise BridgeStateLockError("owner_busy", "bridge state is owned by another process") from exc
        except Exception as exc:
            self.close()
            raise BridgeStateLockError("storage_fault", "bridge state lock could not be acquired") from exc

    def release(self) -> None:
        if not self._depth:
            return
        self._depth -= 1
        if self._depth:
            return
        try:
            if self._connection is not None:
                self._connection.execute("COMMIT")
        except sqlite3.Error as exc:
            if self._connection is not None:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error as rollback_error:
                    logger.warning("bridge state rollback after commit failure failed: %s", rollback_error)
            raise BridgeStateLockError("storage_fault", "bridge state lock commit failed") from exc
        finally:
            self.close()

    def close(self) -> None:
        connection, self._connection = self._connection, None
        self._depth = 0
        if connection is not None:
            connection.close()

    @contextmanager
    def transaction(self):
        self.acquire()
        try:
            yield self
        except Exception:
            logger.exception("bridge state transaction body failed")
            if self._connection is not None:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error as rollback_error:
                    logger.warning("bridge state transaction rollback failed: %s", rollback_error)
            self.close()
            raise
        else:
            self.release()
