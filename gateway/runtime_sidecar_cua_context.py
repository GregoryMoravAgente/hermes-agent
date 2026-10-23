"""Small async-context adapter for the CUA stdio source."""

from __future__ import annotations

import asyncio
from typing import Any


class ObservedCuaContext:
    """Observe one SDK context while preserving its exact same-task exit."""

    def __init__(self, context: Any, owner: Any):
        self._context, self._owner = context, owner
        self._entered = False
        self.value = None

    async def __aenter__(self):
        self.value = await self._context.__aenter__()
        self._entered = True
        self._owner.receipt.bind_session(self.value, asyncio.current_task())
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        try:
            result = await self._context.__aexit__(exc_type, exc, traceback)
        except BaseException as close_error:
            self._owner.receipt.close_session(exc, close_error)
            raise
        self._owner.receipt.close_session(exc, None)
        return result
