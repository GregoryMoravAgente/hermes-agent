"""Refusal and stdio construction races for I16 sidecar producers."""

import asyncio
import sys
from dataclasses import replace

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


@pytest.mark.asyncio
async def test_stale_managed_binding_refuses_before_sidecar_root(tmp_path, monkeypatch):
    """A stale real admission cannot create an I16 root during a retry race."""
    from gateway.runtime_service_custody import ServiceCustodyError
    from gateway.runtime_sidecar_sources import reserve_managed_sidecar

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("sidecar-stale-race")
        before = fixture.authority._preservation_coordinator().status()
        stale = replace(binding, execution_generation=binding.execution_generation + 1)
        with pytest.raises(ServiceCustodyError, match="preservation_owner_mismatch"):
            reserve_managed_sidecar(
                stale,
                family="local_terminal",
                source_identity="terminal:sidecar-stale-race",
                producer_key="environment",
                operation_id="stale-sidecar-race",
            )
        after = fixture.authority._preservation_coordinator().status()
        assert after == before
        assert "I16_turn_sidecars" not in after["active_owner_classes"]
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_mcp_stdio_capture_binds_sdk_process_and_restores_factory(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """The real MCP SDK child is bound before protocol use and remains exact on teardown."""
    pytest.importorskip("mcp.client.stdio")
    from mcp.client.stdio import StdioServerParameters, stdio_client

    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tools.mcp_tool_custody import (
        captured_stdio_client,
        finish_stdio_owner,
    )
    from gateway.runtime_sidecar_sources import reserve_managed_sidecar

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("sidecar-mcp-stdio")
        token = bind_turn(binding)
        try:
            owner = reserve_managed_sidecar(
                binding,
                family="mcp_stdio",
                source_identity="mcp:sidecar-mcp-stdio",
                producer_key="mcp",
                operation_id="mcp-stdio-sidecar",
            )
        finally:
            reset_turn(token)

        module = __import__("mcp.client.stdio", fromlist=["_create_platform_compatible_process"])
        original_factory = module._create_platform_compatible_process
        params = StdioServerParameters(
            command=sys.executable,
            args=["-c", "import time; time.sleep(30)"],
        )
        async with captured_stdio_client(
            stdio_client, params, errlog=None, owner=owner,
        ) as transport:
            (_read_stream, _write_stream), process, witness = transport
            assert owner.process.raw is process
            assert owner.physical.pid == process.pid
            assert owner.physical.scope == "posix_process_group"
            assert owner.physical.readers["stdout"] is witness
        assert module._create_platform_compatible_process is original_factory
        finish_stdio_owner(owner, witness, reason="mcp_stdio_readback_unavailable")
        assert owner.physical.state == "unknown"
        assert owner.physical.unknown_reason == "mcp_stdio_readback_unavailable"
        assert owner.parent.state == "unknown"
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_public_mcp_discovery_binds_real_sdk_child(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """Discovery owns the real SDK child before handshake/tool discovery starts."""
    pytest.importorskip("mcp.client.stdio")
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    captured_servers = []
    original_adopt = mcp_tool_discovery._adopt_server

    def observe_adopt(name, server):
        captured_servers.append(server)
        return original_adopt(name, server)

    monkeypatch.setattr(mcp_tool_discovery, "_adopt_server", observe_adopt)
    server_code = (
        "import json,sys\n"
        "for line in sys.stdin:\n"
        " try: request=json.loads(line)\n"
        " except Exception: continue\n"
        " method=request.get('method')\n"
        " if method == 'notifications/initialized': continue\n"
        " if method == 'initialize': result={'protocolVersion':'2025-03-26','capabilities':{'tools':{}},'serverInfo':{'name':'i16-public','version':'1'}}\n"
        " elif method == 'tools/list': result={'tools':[{'name':'i16_public_tool','description':'fixture','inputSchema':{'type':'object','properties':{}}}]}\n"
        " elif method == 'ping': result={}\n"
        " else: result={}\n"
        " if 'id' in request: print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)\n"
    )
    try:
        binding = await fixture.claim("sidecar-public-mcp")
        token = bind_turn(binding)
        try:
            names = await asyncio.wait_for(
                asyncio.to_thread(
                    mcp_tool_discovery.register_mcp_servers,
                    {
                        "i16-public-mcp": {
                            "command": sys.executable,
                            "args": ["-c", server_code],
                            "connect_timeout": 5,
                        },
                    },
                ),
                timeout=15,
            )
        finally:
            reset_turn(token)

        assert "mcp__i16_public_mcp__i16_public_tool" in names
        assert len(captured_servers) == 1
        server = captured_servers[0]
        owner = server._i16_source_owner
        assert owner is not None
        assert owner.parent.profile_authority is binding.authority
        assert owner.parent.profile_id == binding.profile_id
        assert owner.process is not None
        assert owner.physical is not None
        assert owner.physical.process.raw is owner.process.raw
        assert owner.physical.readers["stdout"] is owner.reader
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-public-mcp"}, timeout=15)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_invalid_required_streams_refuse_without_empty_reader_proof(tmp_path, monkeypatch):
    """A malformed/empty stream declaration must retain the exact real receipt UNKNOWN."""
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from gateway.runtime_sidecar_sources import reserve_managed_sidecar, require_physical
    from gateway.runtime_sidecar_transport import RequiredStreamsError, required_reader_streams

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("sidecar-invalid-streams")
        token = bind_turn(binding)
        try:
            owner = reserve_managed_sidecar(
                binding,
                family="local_terminal",
                source_identity="terminal:sidecar-invalid-streams",
                producer_key="environment",
                operation_id="invalid-streams-sidecar",
            )
        finally:
            reset_turn(token)

        with pytest.raises(RuntimeError, match="required_streams_invalid"):
            require_physical(owner, streams=())
        physical = owner.physical
        assert physical is not None
        assert physical.state == "unknown"
        assert physical.unknown_reason == "required_streams_invalid"
        with pytest.raises(RequiredStreamsError, match="required_streams_invalid"):
            required_reader_streams(physical)
        assert physical.eof_streams == set()
        assert physical.joined_streams == set()
        assert physical.readers == {}
    finally:
        fixture.cleanup()
