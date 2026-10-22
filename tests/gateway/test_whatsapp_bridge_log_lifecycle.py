"""Cancellation and close ownership for the WhatsApp bridge log boundary."""

import asyncio
import threading

import pytest

from plugins.platforms.whatsapp import adapter as whatsapp_adapter
from plugins.platforms.whatsapp import bridge_connect


def _new_owner():
    owner = object.__new__(whatsapp_adapter.WhatsAppAdapter)
    owner._bridge_log_fh = None
    owner._bridge_log_open_task = None
    owner._bridge_log_open_state = None
    owner._bridge_log_open_close_requested = False
    owner._shutting_down = False
    return owner


@pytest.mark.asyncio
async def test_cancelled_log_open_drains_worker_and_closes_late_real_file(tmp_path, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    handles = []

    def gated_open(path):
        started.set()
        assert release.wait(2)
        handle = open(path, "a", encoding="utf-8")
        handles.append(handle)
        return handle

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", gated_open)
    owner = _new_owner()
    path = tmp_path / "bridge.log"
    opening = asyncio.create_task(owner._open_bridge_log(path))
    assert await asyncio.to_thread(started.wait, 2)

    opening.cancel()
    await asyncio.sleep(0)
    owner._close_bridge_log()
    opening.cancel()  # repeated cancellation must not abandon the worker
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await opening
    assert handles and handles[0].closed
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None


@pytest.mark.asyncio
async def test_inner_open_task_cancellation_closes_late_worker_handle(tmp_path, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    handles = []

    def gated_open(path):
        started.set()
        assert release.wait(2)
        handle = open(path, "a", encoding="utf-8")
        handles.append(handle)
        return handle

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", gated_open)
    owner = _new_owner()
    real_create_task = asyncio.create_task
    inner_tasks = []

    def capture_inner_task(coro, *args, **kwargs):
        task = real_create_task(coro, *args, **kwargs)
        inner_tasks.append(task)
        return task

    monkeypatch.setattr(bridge_connect.asyncio, "create_task", capture_inner_task)
    opening = real_create_task(owner._open_bridge_log(tmp_path / "bridge.log"))
    assert await asyncio.to_thread(started.wait, 2)
    assert inner_tasks
    state = owner._bridge_log_open_state
    assert state is not None and not state.done()

    inner_tasks[-1].cancel("inner-open-cancel")
    release.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await opening
    assert caught.value.args == ("inner-open-cancel",)
    assert await asyncio.to_thread(state._done.wait, 2)
    assert handles and handles[0].closed
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None
    owner_state = owner._bridge_log_open_state
    assert owner_state is None or owner_state is state
    if owner_state is not None:
        assert owner_state.done()


@pytest.mark.asyncio
async def test_cancelled_log_open_preserves_cancellation_over_late_worker_error(tmp_path, monkeypatch):
    started = threading.Event()
    release = threading.Event()

    def failing_open(_path):
        started.set()
        assert release.wait(2)
        raise OSError("late open failure")

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", failing_open)
    owner = _new_owner()
    opening = asyncio.create_task(owner._open_bridge_log(tmp_path / "bridge.log"))
    assert await asyncio.to_thread(started.wait, 2)

    opening.cancel()
    await asyncio.sleep(0)
    opening.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await opening
    assert isinstance(caught.value.__cause__, OSError)
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None


@pytest.mark.asyncio
async def test_close_request_before_log_open_completion_closes_without_publication(tmp_path, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    handles = []

    def gated_open(path):
        started.set()
        assert release.wait(2)
        handle = open(path, "a", encoding="utf-8")
        handles.append(handle)
        return handle

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", gated_open)
    owner = _new_owner()
    opening = asyncio.create_task(owner._open_bridge_log(tmp_path / "bridge.log"))
    assert await asyncio.to_thread(started.wait, 2)

    owner._close_bridge_log()
    release.set()

    with pytest.raises(RuntimeError, match="closed before publication"):
        await opening
    assert handles and handles[0].closed
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None


@pytest.mark.asyncio
async def test_worker_error_without_cancellation_is_re_raised(tmp_path, monkeypatch):
    def failing_open(_path):
        raise OSError("open failed")

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", failing_open)
    owner = _new_owner()

    with pytest.raises(OSError, match="open failed"):
        await owner._open_bridge_log(tmp_path / "bridge.log")
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None


@pytest.mark.asyncio
async def test_baseexception_worker_error_without_cancellation_is_re_raised(tmp_path, monkeypatch):
    class WorkerAbort(BaseException):
        pass

    def aborting_open(_path):
        raise WorkerAbort("worker aborted")

    monkeypatch.setattr(bridge_connect, "_open_bridge_log_file", aborting_open)
    owner = _new_owner()

    with pytest.raises(WorkerAbort, match="worker aborted"):
        await owner._open_bridge_log(tmp_path / "bridge.log")
    assert owner._bridge_log_fh is None
    assert owner._bridge_log_open_task is None


@pytest.mark.asyncio
async def test_successful_real_log_is_published_then_closed(tmp_path):
    owner = _new_owner()

    handle = await owner._open_bridge_log(tmp_path / "bridge.log")
    assert handle is owner._bridge_log_fh
    assert not handle.closed

    owner._close_bridge_log()
    assert handle.closed
    assert owner._bridge_log_fh is None
