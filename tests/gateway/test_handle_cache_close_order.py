"""A verified cache close accounts for every accepted native SQLite connection."""
from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path

import pytest

from gateway.session_db_recovery import RecoverableHandleCache


CONTROLS = (
    "checked_failure",
    "legacy_noop",
    "legacy_physicalclose",
    "checkedsuccess",
    "openA",
    "openB",
)


def _cases():
    for base in (("openA",), ("openA", "openB")):
        for controls in itertools.product(CONTROLS, repeat=2):
            yield base + controls + ("checkedsuccess",)


def _assert_physically_closed(connections: list[sqlite3.Connection], sequence: tuple[str, ...]) -> None:
    for index, connection in enumerate(connections):
        try:
            connection.execute("SELECT 1").fetchone()
        except sqlite3.ProgrammingError:
            continue
        raise AssertionError(
            f"{sequence}: checked receipt claimed closure while accepted connection {index} remained usable"
        )


@pytest.mark.parametrize("sequence", list(_cases()), ids=lambda sequence: "+".join(sequence))
def test_checked_receipt_is_an_oracle_for_every_accepted_sqlite_connection(tmp_path, sequence):
    cache = RecoverableHandleCache()
    paths = {"openA": tmp_path / "a.db", "openB": tmp_path / "b.db"}
    accepted: list[sqlite3.Connection] = []

    def open_key(key: str) -> None:
        def opener():
            connection = sqlite3.connect(":memory:")
            accepted.append(connection)
            return connection

        returned = cache.get(paths[key], opener)
        assert returned is not None, sequence
        if returned not in accepted:
            accepted.append(returned)

    def checked_failure(handle) -> bool:
        return False

    def checked_success(handle) -> bool:
        handle.close()
        return True

    def legacy_noop(handle) -> None:
        return None

    def legacy_physicalclose(handle) -> None:
        handle.close()

    operations = {
        "openA": lambda: open_key("openA"),
        "openB": lambda: open_key("openB"),
        "checked_failure": lambda: cache.close_all_checked(checked_failure),
        "checkedsuccess": lambda: cache.close_all_checked(checked_success),
        "legacy_noop": lambda: cache.close_all(legacy_noop),
        "legacy_physicalclose": lambda: cache.close_all(legacy_physicalclose),
    }

    try:
        for action in sequence:
            receipt = operations[action]()
            if receipt is not None and receipt.verified:
                _assert_physically_closed(accepted, sequence)
        final = cache.close_all_checked(checked_success)
        assert final.verified is True, sequence
        _assert_physically_closed(accepted, sequence)
    finally:
        for connection in accepted:
            try:
                connection.close()
            except Exception:
                pass
