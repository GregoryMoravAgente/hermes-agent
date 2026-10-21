"""Bind the MCP SDK's real stdio child to the existing I16 service receipt.

The SDK deliberately exposes only message streams.  Its private process factory is the
construction boundary that owns the actual anyio process, so this adapter observes that
object for the duration of one context entry and restores the SDK module immediately after
the entry.  The adapter never derives ownership from a PID snapshot or creates a second
process/receipt ledger.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import inspect
import os
import sys
from typing import Any

from gateway.runtime_service_operations import ExternalProcessOperationError
from gateway.runtime_sidecar_sources import SidecarOwner, bind_process, bind_reader, mark_process_finished, mark_unknown


_CAPTURE_LOCK: asyncio.Lock | None = None


def _capture_lock() -> asyncio.Lock:
    global _CAPTURE_LOCK
    if _CAPTURE_LOCK is None:
        _CAPTURE_LOCK = asyncio.Lock()
    return _CAPTURE_LOCK


class MCPProcessAdapter:
    """Transparent receipt adapter around the exact anyio process returned by the SDK."""

    __slots__ = ("raw",)

    def __init__(self, raw: Any):
        if raw is None or type(getattr(raw, "pid", None)) is not int or raw.pid <= 0:
            raise ExternalProcessOperationError("physical_launch_identity_mismatch")
        self.raw = raw

    @property
    def pid(self) -> int:
        return self.raw.pid

    @property
    def returncode(self):
        return getattr(self.raw, "returncode", None)

    def poll(self):
        return self.returncode

    async def wait(self):
        return await self.raw.wait()

    def terminate(self):
        terminate = getattr(self.raw, "terminate", None)
        if not callable(terminate):
            raise ExternalProcessOperationError("physical_process_termination_unknown")
        return terminate()

    def kill(self):
        kill = getattr(self.raw, "kill", None)
        if not callable(kill):
            raise ExternalProcessOperationError("physical_process_termination_unknown")
        return kill()


class MCPStdioReaderWitness:
    """The SDK's exact receive stream plus its context-owned join boundary."""

    __slots__ = ("joined", "process", "stream")

    def __init__(self, stream: Any, process: Any):
        if stream is None or process is None:
            raise ExternalProcessOperationError("reader_identity_missing")
        self.stream = stream
        self.process = process
        self.joined = False

    def is_alive(self) -> bool:
        return not self.joined

    def mark_joined(self) -> None:
        self.joined = True


def _stdio_factory(stdio_client: Any):
    module = inspect.getmodule(stdio_client)
    factory = getattr(module, "_create_platform_compatible_process", None) if module else None
    if module is None or not callable(factory):
        raise ExternalProcessOperationError("mcp_process_factory_unavailable")
    return module, factory


@asynccontextmanager
async def serialized_stdio_client(stdio_client: Any, server_params: Any, *, errlog: Any):
    """Enter an unowned SDK stdio transport at the same construction fence as managed ones."""
    cm = stdio_client(server_params, errlog=errlog)
    entered = False
    async with _capture_lock():
        try:
            streams = await cm.__aenter__()
            entered = True
        except BaseException:
            if entered:
                await cm.__aexit__(*sys.exc_info())
            raise
    try:
        yield streams
    except BaseException:
        await cm.__aexit__(*sys.exc_info())
        raise
    else:
        await cm.__aexit__(None, None, None)


@asynccontextmanager
async def captured_stdio_client(
    stdio_client: Any,
    server_params: Any,
    *,
    errlog: Any,
    owner: SidecarOwner | None = None,
):
    """Enter the SDK transport while observing its exact process and streams.

    The SDK's factory is patched only while ``__aenter__`` constructs the process; the
    original function is restored before yielding to the caller.  This avoids a global
    permanent hook and makes concurrent connection attempts serialize at the one SDK
    construction boundary.
    """
    captured: dict[str, Any] = {}
    cm = stdio_client(server_params, errlog=errlog)
    entered = False
    witness = None
    async with _capture_lock():
        # Resolve the SDK module/factory only after taking the same construction lock that
        # also protects standalone stdio.  Reading the factory before the lock let a second
        # task capture or restore another task's process boundary.
        module, original_factory = _stdio_factory(stdio_client)

        async def capture_factory(*args, **kwargs):
            process = await original_factory(*args, **kwargs)
            captured["process"] = process
            return process

        current = getattr(module, "_create_platform_compatible_process", None)
        if current is not original_factory:
            raise ExternalProcessOperationError("mcp_process_factory_replaced")
        module._create_platform_compatible_process = capture_factory
        try:
            try:
                streams = await cm.__aenter__()
                entered = True
                process = captured.get("process")
                if process is None:
                    raise ExternalProcessOperationError("mcp_process_identity_missing")
                if owner is not None:
                    witness = bind_stdio_owner(owner, process, streams[0])
            except BaseException:
                if entered:
                    await cm.__aexit__(*sys.exc_info())
                raise
        finally:
            module._create_platform_compatible_process = original_factory
    process = captured["process"]
    try:
        if owner is None:
            yield streams, process
        else:
            yield streams, process, witness
    except BaseException:
        await cm.__aexit__(*sys.exc_info())
        raise
    else:
        await cm.__aexit__(None, None, None)


def bind_stdio_owner(owner: SidecarOwner, process: Any, read_stream: Any) -> MCPStdioReaderWitness:
    """Bind the SDK process and receive stream before the first protocol request."""
    existing_process = getattr(getattr(owner, "process", None), "raw", None)
    existing_reader = getattr(owner, "reader", None)
    if existing_process is process and existing_reader is not None:
        if getattr(existing_reader, "stream", None) is read_stream:
            return existing_reader
        mark_unknown(owner, "reader_identity_replaced")
        raise ExternalProcessOperationError("reader_identity_replaced")
    raw_pid = getattr(process, "pid", None)
    if type(raw_pid) is not int or raw_pid <= 0:
        mark_unknown(owner, "process_identity_rejected")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    try:
        pgid = os.getpgid(raw_pid)
    except (AttributeError, OSError, ProcessLookupError) as exc:
        mark_unknown(owner, "physical_scope_unknown")
        raise ExternalProcessOperationError("physical_scope_unknown") from exc
    if type(pgid) is not int or pgid <= 0:
        mark_unknown(owner, "physical_scope_unknown")
        raise ExternalProcessOperationError("physical_scope_unknown")
    adapter = MCPProcessAdapter(process)
    bind_process(
        owner,
        adapter,
        scope="posix_process_group",
        scope_identity=f"posix_process_group:{raw_pid}:{pgid}",
        process_group_id=pgid,
    )
    witness = MCPStdioReaderWitness(read_stream, process)
    try:
        bind_reader(owner, "stdout", witness)
    except BaseException:
        mark_unknown(owner, "reader_identity_missing")
        raise
    return witness


def finish_stdio_owner(owner: SidecarOwner, witness: MCPStdioReaderWitness, *, reason: str) -> None:
    """Record SDK context teardown, retaining UNKNOWN where POSIX group emptiness is unproven."""
    witness.mark_joined()
    mark_process_finished(owner, reader=witness, reason=reason)
