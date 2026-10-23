"""The lifecycle journal must fail closed when an existing record cannot be read."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway import lifecycle_handoff, lifecycle_ledger
from gateway.control_socket import identify_gateway, pause_gateway_for_update
from tests.gateway.test_lifecycle_start_gateway_successor import (
    _assert_exit_75,
    _handoff_state,
    _launch_gateway,
    _stop_if_running,
)
from tests.waiting import wait_until


class _Runner:
    session_runtime_descriptor = {"state": "starting"}


@pytest.mark.parametrize(
    ("journal_body", "expected_status"),
    [
        ("{", "malformed"),
        ("not-json", "malformed"),
        (json.dumps([]), "malformed"),
        (json.dumps({"handoff": "not-a-record"}), "ok"),
        (json.dumps({"handoff": {"state": "released"}}), "ok"),
        (json.dumps({"handoff": {"state": "future"}}), "ok"),
        (json.dumps({"lifecycle_record_read_failure": []}), "ok"),
    ],
)
def test_existing_malformed_journal_refuses_successor_boot(
    tmp_path: Path, journal_body: str, expected_status: str,
) -> None:
    home = tmp_path / "hermes-home"
    journal = home / "state" / "gateway.lifecycle.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(journal_body, encoding="utf-8")

    _record, status = lifecycle_ledger._read_json_status(journal)
    assert status == expected_status
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is False
    assert lifecycle_handoff.verify_successor_ready(_Runner(), home) is False


def test_existing_unreadable_journal_refuses_successor_boot(tmp_path: Path) -> None:
    home = tmp_path / "hermes-home"
    journal = home / "state" / "gateway.lifecycle.json"
    journal.parent.mkdir(parents=True)
    journal.mkdir()

    _record, status = lifecycle_ledger._read_json_status(journal)
    assert status == "unreadable"
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is False
    assert lifecycle_handoff.verify_successor_ready(_Runner(), home) is False


def test_record_startup_keeps_unreadable_journal_fail_closed(tmp_path: Path) -> None:
    home = tmp_path / "hermes-home"
    journal = home / "state" / "gateway.lifecycle.json"
    journal.parent.mkdir(parents=True)
    journal.mkdir()

    lifecycle_ledger.record_startup(home)

    assert lifecycle_ledger._read_json_status(journal)[1] == "unreadable"
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is False


def test_absent_journal_keeps_ordinary_boot_path(tmp_path: Path) -> None:
    home = tmp_path / "hermes-home"

    assert lifecycle_ledger._read_json_status(home / "state" / "gateway.lifecycle.json")[1] == "absent"
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is True
    assert lifecycle_handoff.verify_successor_ready(_Runner(), home) is True


def test_record_startup_preserves_journal_read_failure_for_the_boot_gate(tmp_path: Path) -> None:
    home = tmp_path / "hermes-home"
    journal = home / "state" / "gateway.lifecycle.json"
    journal.parent.mkdir(parents=True)
    journal.write_text("{", encoding="utf-8")

    lifecycle_ledger.record_startup(home)

    claim = json.loads(journal.read_text(encoding="utf-8"))
    assert claim["lifecycle_record_read_failure"] == "malformed"
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is False


def test_record_startup_preserves_malformed_handoff_shape_for_the_boot_gate(tmp_path: Path) -> None:
    home = tmp_path / "hermes-home"
    journal = home / "state" / "gateway.lifecycle.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(json.dumps({"handoff": "not-a-record"}), encoding="utf-8")

    lifecycle_ledger.record_startup(home)

    claim = json.loads(journal.read_text(encoding="utf-8"))
    assert claim["lifecycle_record_read_failure"] == "malformed"
    assert lifecycle_handoff.adopt_successor_or_refuse(_Runner(), home) is False


def test_real_successor_refuses_existing_truncated_journal_before_public_ready(tmp_path: Path) -> None:
    """A real successor cannot turn an unreadable predecessor journal into ordinary READY."""
    home = (tmp_path / "hermes-home").resolve()
    predecessor = successor = None
    try:
        predecessor, _predecessor_stdout, predecessor_stderr = _launch_gateway(home, "predecessor")
        predecessor_identity = wait_until(
            lambda: identify_gateway(home, timeout=1.0),
            timeout=90,
            what="journal-failure predecessor identify",
        )
        pause = wait_until(
            lambda: pause_gateway_for_update(home, timeout=2.0),
            timeout=30,
            what="journal-failure predecessor pause",
        )
        assert pause.get("pausing") is True
        _assert_exit_75(predecessor, predecessor_stderr, what="journal-failure predecessor")
        released = wait_until(
            lambda: _handoff_state(home, "released"),
            timeout=30,
            what="journal-failure predecessor release",
        )
        assert released["predecessor"]["pid"] == predecessor_identity["pid"]

        (home / "state" / "gateway.lifecycle.json").write_text("{", encoding="utf-8")
        successor, _successor_stdout, successor_stderr = _launch_gateway(home, "successor")
        wait_until(
            lambda: successor.poll() is not None,
            timeout=90,
            what="journal-failure successor refusal",
        )
        assert successor.returncode != 0
        assert identify_gateway(home, timeout=1.0) is None
        assert "successor handoff" in successor_stderr.read_text(encoding="utf-8", errors="replace")
    finally:
        if successor is not None:
            _stop_if_running(successor)
        if predecessor is not None:
            _stop_if_running(predecessor)

