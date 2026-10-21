"""Standalone Base callback compatibility, separate from managed authority custody.

These controls use the public Base adapter ingress and its real background owner.  They do not
claim an I04/I06 authority row and therefore cannot mask a managed-route ownership failure.
The callback runs in the original owner task, while a real queued successor may already have
been handed to a drain task before the owner reaches its final session reconciliation.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource, build_session_key


class _StandaloneProbeAdapter(BasePlatformAdapter):
    """Concrete legacy adapter with no GatewayRunner or SessionAuthority."""

    def __init__(self) -> None:
        super().__init__(PlatformConfig(enabled=True, token="x"), Platform.SLACK)
        self.sent: list[str] = []

    async def start(self):  # pragma: no cover - compatibility surface
        pass

    async def stop(self):  # pragma: no cover - compatibility surface
        pass

    async def connect(self):  # pragma: no cover - compatibility surface
        pass

    async def disconnect(self):  # pragma: no cover - compatibility surface
        pass

    async def get_chat_info(self, chat_id):  # pragma: no cover - compatibility surface
        return {}

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(str(content))
        return SendResult(success=True, message_id="standalone-message")

    async def send_typing(self, chat_id, metadata=None):  # pragma: no cover - compatibility surface
        pass


def _source() -> SessionSource:
    return SessionSource(
        platform=Platform.SLACK,
        user_id="U1",
        chat_id="C1",
        user_name="tester",
        chat_type="channel",
    )


def _event(source: SessionSource, message_id: str) -> MessageEvent:
    return MessageEvent(
        text=message_id,
        message_type=MessageType.TEXT,
        source=source,
        message_id=message_id,
    )


async def _run_standalone_callback_case(control: str, *, proof_path: Path | None = None):
    adapter = _StandaloneProbeAdapter()
    source = _source()
    first = _event(source, "standalone-first")
    second = _event(source, "standalone-second")
    session_key = build_session_key(source)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    callback_done = asyncio.Event()
    callback_task = None
    callback_is_initial_owner_task = False
    finish_observation = {}

    async def handler(event):
        if event is first:
            first_started.set()
            await release_first.wait()
            return "first reply"
        assert event is second
        second_started.set()
        return None

    adapter.set_message_handler(handler)

    def callback():
        nonlocal callback_task, callback_is_initial_owner_task
        callback_task = asyncio.current_task()
        callback_is_initial_owner_task = callback_task is owner_task
        callback_done.set()
        if control == "runtime_error":
            raise RuntimeError("standalone callback failure")
        if control == "cancelled":
            raise asyncio.CancelledError()
        if control == "keyboard_interrupt":
            raise KeyboardInterrupt("standalone callback interrupt")
        raise AssertionError(f"unknown standalone control: {control}")

    adapter.register_post_delivery_callback(session_key, callback)
    original_finish_session_task = adapter._finish_session_task

    def observe_finish_session_task(key, event):
        finish_observation.update({
            "called": True,
            "same_session": key == session_key,
            "pending_is_second_before": adapter._pending_messages.get(key) is second,
            "owner_is_callback_task": adapter._session_tasks.get(key) is asyncio.current_task(),
            "finalizer_is_callback_task": asyncio.current_task() is callback_task,
            "task": asyncio.current_task(),
        })
        if proof_path is not None:
            proof_path.write_text(json.dumps({
                "control": control,
                "callback_exception_type": {
                    "runtime_error": "RuntimeError",
                    "cancelled": "CancelledError",
                    "keyboard_interrupt": "KeyboardInterrupt",
                }[control],
                "finish_called": True,
                "callback_is_initial_owner_task": callback_is_initial_owner_task,
                "same_session": key == session_key,
                "finalizer_is_callback_task": asyncio.current_task() is callback_task,
                "pending_is_second_before": adapter._pending_messages.get(key) is second,
                "owner_is_callback_task": adapter._session_tasks.get(key) is asyncio.current_task(),
                "successor_started": second_started.is_set(),
            }, sort_keys=True) + "\n")
        return original_finish_session_task(key, event)

    adapter._finish_session_task = observe_finish_session_task
    owner_task = None
    owner_error_type = None
    try:
        await adapter.handle_message(first)
        await asyncio.wait_for(first_started.wait(), timeout=5)
        owner_task = adapter._session_tasks.get(session_key)
        assert owner_task is not None

        await adapter.handle_message(second)
        assert second._gateway_accepted is True
        assert adapter._pending_messages.get(session_key) is second

        release_first.set()
        await asyncio.wait_for(callback_done.wait(), timeout=5)
        await asyncio.wait_for(second_started.wait(), timeout=5)
        if owner_task is not None:
            try:
                await owner_task
            except BaseException as exc:
                owner_error_type = type(exc).__name__

        assert finish_observation["called"] is True
        assert finish_observation["same_session"] is True
        assert finish_observation["finalizer_is_callback_task"] is True
        # The real owner hands the queued event to a drain task before callback cleanup.  The
        # finalizer therefore sees the pending slot empty and a different task in the owner map;
        # second_started is the source-faithful proof that the successor was retained and drained.
        assert finish_observation["pending_is_second_before"] is False
        assert finish_observation["owner_is_callback_task"] is False
        assert second_started.is_set() is True
        assert adapter._pending_messages.get(session_key) is not second
        assert adapter.gateway_runner is None
        return {
            "callback_task": callback_task,
            "finalizer_task": finish_observation["task"],
            "callback_is_initial_owner_task": callback_is_initial_owner_task,
            "caught_type": owner_error_type,
            "pending_observed_before_finalizer": finish_observation["pending_is_second_before"],
            "owner_observed_before_finalizer": finish_observation["owner_is_callback_task"],
            "finalizer_is_callback_task": finish_observation["finalizer_is_callback_task"],
            "successor_started": second_started.is_set(),
            "authority_present": getattr(adapter, "gateway_runner", None) is not None,
        }
    finally:
        release_first.set()
        await adapter.cancel_background_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("control", ("runtime_error", "cancelled"))
async def test_standalone_base_callback_fault_contract_drains_real_pending_successor(control):
    observation = await _run_standalone_callback_case(control)
    assert observation["callback_task"] is observation["finalizer_task"]
    assert observation["pending_observed_before_finalizer"] is False
    assert observation["owner_observed_before_finalizer"] is False
    assert observation["finalizer_is_callback_task"] is True
    assert observation["callback_is_initial_owner_task"] is True
    assert observation["successor_started"] is True
    assert observation["authority_present"] is False
    assert observation["caught_type"] == {
        "runtime_error": None,
        "cancelled": "CancelledError",
    }[control]


@pytest.mark.asyncio
async def test_standalone_base_keyboardinterrupt_preserves_real_callback_boundary(tmp_path):
    proof_path = tmp_path / "standalone-base-keyboardinterrupt-proof.json"
    repo_root = Path(__file__).resolve().parents[2]
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.gateway.test_classic_callback_base_compat",
        "--run-keyboardinterrupt",
        "--proof",
        str(proof_path),
        cwd=repo_root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await child.communicate()
    output = stdout.decode() + stderr.decode()
    assert child.returncode != 0
    assert "KeyboardInterrupt" in output
    assert proof_path.is_file()
    proof = json.loads(proof_path.read_text())
    assert proof == {
        "callback_exception_type": "KeyboardInterrupt",
        "callback_is_initial_owner_task": True,
        "control": "keyboard_interrupt",
        "finish_called": True,
        "finalizer_is_callback_task": False,
        "owner_is_callback_task": True,
        "pending_is_second_before": False,
        "same_session": True,
        "successor_started": True,
    }


def _run_isolated_keyboardinterrupt() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-keyboardinterrupt", action="store_true")
    parser.add_argument("--proof", required=True)
    args = parser.parse_args()
    assert args.run_keyboardinterrupt is True
    asyncio.run(
        _run_standalone_callback_case(
            "keyboard_interrupt", proof_path=Path(args.proof),
        )
    )


if __name__ == "__main__":
    _run_isolated_keyboardinterrupt()
