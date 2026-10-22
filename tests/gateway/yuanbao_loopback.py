"""Protocol-faithful loopback WebSocket used by Yuanbao custody tests."""

from __future__ import annotations

import asyncio

import plugins.platforms.yuanbao.adapter as yuanbao_module
from plugins.platforms.yuanbao import yuanbao_proto


class RecordingWebSocket:
    def __init__(self):
        self.sent = []
        self.auth_response = None
        self._frames = asyncio.Queue()
        self.response_frames = set()
        self.decode_outbound = None
        self._closed = False

    async def send(self, value):
        self.sent.append(value)
        decoder = self.decode_outbound
        if decoder is None:
            return
        try:
            decoded = decoder(value)
        except (KeyError, TypeError, ValueError):
            return
        head = decoded.get("head", {})
        if head.get("cmd_type") != yuanbao_module.CMD_TYPE["Request"]:
            return
        if head.get("cmd") == "auth-bind":
            return
        response = yuanbao_proto.encode_conn_msg_full(
            cmd_type=yuanbao_module.CMD_TYPE["Response"],
            cmd=head.get("cmd", ""), seq_no=head.get("seq_no", 0),
            msg_id=head.get("msg_id", ""), module=head.get("module", ""), data=b"",
        )
        self.response_frames.add(response)
        await self._frames.put(response)

    async def recv(self):
        if self.auth_response is not None:
            response, self.auth_response = self.auth_response, None
            return response
        return await self._frames.get()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
        return await self.recv()

    async def close(self):
        self._closed = True
        await self._frames.put(None)

    async def wait_closed(self):
        return None
