"""Source-owned receipt for MCP HTTP/SSE sidecars.

The process-backed I16 adapter lives in :mod:`gateway.runtime_sidecar_sources`.  A
remote MCP transport has no child PID or pipe EOF, so it needs a separate typed
receipt for the SDK context manager and ClientSession objects that actually own
the connection.  The receipt is attached to the existing
``ExternalProcessParent``; it does not create a process-global registry.
"""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

from gateway.runtime_sidecar_sources import SidecarOwner

class RemoteSourceError(RuntimeError):
    """A remote source cannot prove the requested ownership transition."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


_STREAMABLE_HTTP_REJECTION_STATUSES = frozenset((400, 405, 406, 411))
_REMOTE_SCOPE_CONTEXT: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "i16_remote_scope_context", default=None,
)
_REMOTE_RESPONSE_MARKER = object()
_REMOTE_REQUEST_MARKER = object()


def _contains_only_exception_identity(container: BaseException, target: BaseException) -> bool:
    """Recognize a pure SDK TaskGroup wrapper of the same body failure."""
    if container is target:
        return True
    if isinstance(container, BaseExceptionGroup):
        return bool(container.exceptions) and all(
            _contains_only_exception_identity(child, target) for child in container.exceptions
        )
    return False


class _RemoteObservedContext:
    """Source-owned async-context observation; callers cannot supply exit flags."""

    __slots__ = (
        "__body_error", "__close_error", "__closed", "__context", "__entered", "__value",
    )

    def __init__(self, context: Any):
        self.__context = context
        self.__entered = False
        self.__closed = False
        self.__close_error = None
        self.__body_error = None
        self.__value = None

    async def enter(self) -> Any:
        self.__value = await self.__context.__aenter__()
        self.__entered = True
        return self.__value

    async def exit(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> Any:
        if exc is not None:
            self.__body_error = exc
        try:
            suppressed = await self.__context.__aexit__(exc_type, exc, traceback)
        except BaseException as close_error:
            if exc is not None and _contains_only_exception_identity(close_error, exc):
                self.__closed = True
                return False
            self.__close_error = close_error
            raise
        self.__closed = True
        return suppressed

    @property
    def entered(self) -> bool:
        return self.__entered

    @property
    def closed(self) -> bool:
        return self.__closed

    @property
    def value(self) -> Any:
        return self.__value

    @property
    def body_error(self) -> BaseException | None:
        return self.__body_error

    @property
    def close_error(self) -> BaseException | None:
        return self.__close_error


@dataclass(frozen=True, slots=True)
class _RemoteHTTPResponseEvent:
    """Immutable response event captured by the source-owned httpx hook."""

    response: Any
    request: Any
    status: int
    method: str
    url: str
    body: str
    ordinal: int
    task: Any = field(repr=False, compare=False)
    scope: Any = field(repr=False, compare=False)
    attempt: Any = field(repr=False, compare=False)
    server: Any = field(repr=False, compare=False)
    request_marker: Any = field(repr=False, compare=False)
    response_marker: Any = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class _RemoteHTTPRejectionWitness:
    """Private fallback proof tied to one source-owned scope and response event."""

    event: _RemoteHTTPResponseEvent = field(repr=False, compare=False)
    exception: BaseException = field(repr=False, compare=False)
    scope: Any = field(repr=False, compare=False)


class _RemoteHTTPWireState:
    """Private identity for one request in an overlapping source-owned HTTP attempt."""

    __slots__ = (
        "callback_tasks", "request", "response_marker", "transport_request",
        "transport_response", "transport_task",
    )

    def __init__(self, request: Any):
        self.request = request
        self.transport_request = None
        self.transport_response = None
        self.transport_task = None
        self.response_marker = None
        self.callback_tasks: set[Any] = set()


class _RemoteHTTPAttempt:
    """Private wire attempt installed only on the source-created HTTP client.

    The request/response hooks are deliberately kept on this object and are never
    returned by the public owner API.  The marking transport records the exact
    objects it received before the SDK's response hook runs; a copied request or
    shaped response therefore cannot manufacture an event.
    """

    __slots__ = (
        "ambiguous", "callback_tasks", "context", "hook_request", "receipt",
        "request_states", "response", "response_hook_token", "response_marker", "server", "task", "token",
        "transport_request", "transport_response", "transport_task",
    )

    def __init__(self, receipt: RemoteSourceReceipt, server: Any, task: Any):
        self.receipt = receipt
        self.server = server
        self.task = task
        self.token = object()
        self.context = object()
        self.hook_request = None
        self.transport_request = None
        self.transport_task = None
        self.transport_response = None
        self.response_marker = None
        self.response = None
        self.response_hook_token = object()
        self.callback_tasks: set[Any] = set()
        self.request_states: dict[int, _RemoteHTTPWireState] = {}
        self.ambiguous = False

    def _wire_state(self, request: Any) -> _RemoteHTTPWireState | None:
        """Resolve one retained request by id and exact object identity."""
        state = self.request_states.get(id(request))
        if state is None:
            return None
        if state.request is not request:
            self.ambiguous = True
            self.receipt.mark_unknown("remote_http_request_identity_mismatch")
            return None
        return state

    async def record_request(self, request: Any) -> None:
        """Record the exact request emitted by the source-created HTTPX client."""
        if (
            _REMOTE_SCOPE_CONTEXT.get() is not self.context
            or self.receipt._http_attempt is not self
        ):
            self.receipt.mark_unknown("remote_http_request_foreign")
            return
        extensions = getattr(request, "extensions", None)
        if not isinstance(extensions, dict):
            self.receipt.mark_unknown("remote_http_request_identity_missing")
            return
        try:
            extensions[_REMOTE_REQUEST_MARKER] = self.token
        except (AttributeError, TypeError):
            self.receipt.mark_unknown("remote_http_request_identity_missing")
            return
        state = self.request_states.get(id(request))
        if state is not None:
            if state.request is not request:
                self.ambiguous = True
                self.receipt.mark_unknown("remote_http_request_identity_mismatch")
            else:
                self.ambiguous = True
                self.receipt.mark_unknown("remote_http_request_reused")
            return
        state = _RemoteHTTPWireState(request)
        self.request_states[id(request)] = state
        # These aliases retain the source-owned diagnostic view used by existing
        # consumers.  Validation below always resolves the exact request entry.
        self.transport_request = None
        self.transport_response = None
        self.response_marker = None
        self.transport_task = None
        self.hook_request = request

    def mark_transport_request(self, request: Any) -> None:
        """Record the request reaching the source-owned underlying HTTP transport."""
        current_task = asyncio.current_task()
        if (
            _REMOTE_SCOPE_CONTEXT.get() is not self.context
            or self.receipt._http_attempt is not self
            or current_task is None
        ):
            self.receipt.mark_unknown("remote_http_transport_foreign")
            return
        state = self._wire_state(request)
        if state is None or state.transport_request is not None:
            self.ambiguous = True
            self.receipt.mark_unknown("remote_http_request_identity_mismatch")
            return
        state.transport_request = request
        state.transport_task = current_task
        self.transport_request = state.transport_request
        self.transport_task = state.transport_task
        self.callback_tasks.add(current_task)
        state.callback_tasks.add(current_task)

    def mark_transport_response(self, response: Any, *, request: Any = None) -> None:
        """Stamp the exact wire response before HTTPX invokes response hooks."""
        current_task = asyncio.current_task()
        request = getattr(response, "request", None) if request is None else request
        state = self._wire_state(request) if request is not None else None
        # HTTPX attaches ``response.request`` after the transport returns.  The
        # source transport identity is therefore established here by the exact
        # response object, and the request identity is checked by the later hook.
        if (
            _REMOTE_SCOPE_CONTEXT.get() is not self.context
            or self.receipt._http_attempt is not self
            or state is None
            or current_task is not state.transport_task
            or state.transport_request is not request
        ):
            self.ambiguous = True
            self.receipt.mark_unknown("remote_http_response_foreign")
            return
        extensions = getattr(response, "extensions", None)
        if not isinstance(extensions, dict):
            self.receipt.mark_unknown("remote_http_response_identity_missing")
            return
        marker = object()
        try:
            extensions[_REMOTE_RESPONSE_MARKER] = marker
        except (AttributeError, TypeError):
            self.receipt.mark_unknown("remote_http_response_identity_missing")
            return
        state.response_marker = marker
        state.transport_response = response
        self.response_marker = marker
        self.transport_response = response

    def source_response_hook(self):
        """Return the private response callback installed on this source-owned HTTPX client."""
        hook_token = self.response_hook_token

        def _record(response: Any, body: str) -> None:
            current_task = asyncio.current_task()
            self._record_source_response(
                hook_token, current_task, response, body,
            )

        return _record

    def _record_source_response(
        self, hook_token: Any, callback_task: Any, response: Any, body: str,
    ) -> None:
        """Record the exact response from the private HTTPX hook task."""
        current_task = asyncio.current_task()
        if (
            hook_token is not self.response_hook_token
            or callback_task is None
            or current_task is not callback_task
            or _REMOTE_SCOPE_CONTEXT.get() is not self.context
            or self.receipt._http_attempt is not self
            or self.ambiguous
        ):
            self.receipt.mark_unknown("remote_http_response_foreign")
            return
        request = getattr(response, "request", None)
        state = self._wire_state(request) if request is not None else None
        if (
            state is None
            or callback_task is not state.transport_task
            or state.transport_request is not request
            or state.transport_response is not response
            or getattr(response, "extensions", {}).get(_REMOTE_RESPONSE_MARKER) is not state.response_marker
        ):
            self.receipt.mark_unknown("remote_http_response_foreign")
            return
        self.callback_tasks.add(callback_task)
        state.callback_tasks.add(callback_task)
        event_task = callback_task
        status = getattr(response, "status_code", None)
        method = getattr(request, "method", None)
        url = str(getattr(request, "url", ""))
        if type(status) is not int or type(method) is not str or not url:
            self.receipt.mark_unknown("remote_http_response_identity_missing")
            return
        if status not in _STREAMABLE_HTTP_REJECTION_STATUSES:
            return
        if self.response is not None:
            self.ambiguous = True
            self.receipt._http_event_ambiguous = True
            self.receipt.mark_unknown("remote_http_response_replaced")
            return
        self.response = _RemoteHTTPResponseEvent(
            response=response,
            request=request,
            status=status,
            method=method,
            url=url,
            body=str(body),
            ordinal=1,
            task=event_task,
            scope=self.receipt._active_scope,
            attempt=self,
            server=self.server,
            request_marker=self.token,
            response_marker=state.response_marker,
        )
        self.receipt._http_event = self.response

    def record_response(self, response: Any, body: str) -> None:
        """Reject direct callers; only the private source hook can authenticate an event."""
        self.receipt.mark_unknown("remote_http_response_foreign")


class _RemoteHTTPMarkingTransport:
    """Source-owned transport wrapper that records actual wire identities."""

    __slots__ = ("_attempt", "_transport")

    def __init__(self, transport: Any, attempt: _RemoteHTTPAttempt):
        self._transport = transport
        self._attempt = attempt

    async def handle_async_request(self, request: Any) -> Any:
        self._attempt.mark_transport_request(request)
        response = await self._transport.handle_async_request(request)
        self._attempt.mark_transport_response(response, request=request)
        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


class _RemoteTransportScope:
    """Own and await one exact SDK transport/session pair for a remote receipt."""

    __slots__ = (
        "__body_error", "__context_token", "__finished", "__receipt", "__session", "__session_context",
        "__session_kwargs", "__streams", "__task", "__transport", "__transport_context",
    )

    def __init__(self, receipt: RemoteSourceReceipt, transport: Any, session_kwargs: dict, task: Any):
        self.__receipt = receipt
        self.__transport = transport
        self.__session_kwargs = dict(session_kwargs)
        self.__task = task
        self.__transport_context = _RemoteObservedContext(transport)
        self.__session_context = None
        self.__body_error = None
        self.__streams = None
        self.__session = None
        self.__finished = False
        self.__context_token = None

    @property
    def task(self) -> Any:
        return self.__task

    @property
    def transport(self) -> Any:
        return self.__transport

    @property
    def streams(self) -> Any:
        return self.__streams

    @property
    def session(self) -> Any:
        return self.__session

    @property
    def transport_entered(self) -> bool:
        return self.__transport_context.entered

    @property
    def transport_closed(self) -> bool:
        return self.__transport_context.closed

    @property
    def session_entered(self) -> bool:
        return self.__session_context is not None and self.__session_context.entered

    @property
    def session_closed(self) -> bool:
        return self.__session_context is not None and self.__session_context.closed

    @property
    def body_error(self) -> BaseException | None:
        if self.__body_error is not None:
            return self.__body_error
        if self.__transport_context.body_error is not None:
            return self.__transport_context.body_error
        if self.__session_context is not None:
            return self.__session_context.body_error
        return None

    @property
    def close_error(self) -> BaseException | None:
        if self.__session_context is not None and self.__session_context.close_error is not None:
            return self.__session_context.close_error
        return self.__transport_context.close_error

    @property
    def clean(self) -> bool:
        return (
            self.transport_entered
            and self.transport_closed
            and self.session_entered
            and self.session_closed
            and self.close_error is None
        )

    async def __aenter__(self) -> Any:
        try:
            streams = await self.__transport_context.enter()
            if not isinstance(streams, (tuple, list)) or len(streams) < 2:
                raise RemoteSourceError("remote_transport_stream_identity_missing")
            self.__streams = streams
            self.__receipt._begin_transport_scope(self)
            self.__context_token = _REMOTE_SCOPE_CONTEXT.set(self.__receipt._http_context)
            from tools.mcp_tool_common import _core
            session_context = _RemoteObservedContext(
                _core.ClientSession(streams[0], streams[1], **self.__session_kwargs),
            )
            self.__session_context = session_context
            self.__session = await session_context.enter()
            self.__receipt._bind_transport_scope_session(self)
            return self.__session
        except BaseException as enter_error:
            try:
                await self.__close(None, enter_error, None)
            except (Exception, asyncio.CancelledError) as close_error:
                # Preserve the source-enter failure while retaining the close failure as its cause;
                # _finish_transport_scope has already marked the exact parent UNKNOWN.
                raise enter_error from close_error
            raise

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return await self.__close(exc_type, exc, traceback)

    async def __close(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> bool:
        if self.__finished:
            return False
        self.__finished = True
        self.__body_error = exc
        close_error = None
        try:
            if self.__session_context is not None and self.__session_context.entered:
                try:
                    await self.__session_context.exit(exc_type, exc, traceback)
                except BaseException as session_error:  # health: allow BLE001 -- retain first SDK exit error while awaiting the outer transport
                    close_error = session_error
            if self.__transport_context.entered:
                try:
                    await self.__transport_context.exit(exc_type, exc, traceback)
                except BaseException as transport_error:  # health: allow BLE001 -- retain first SDK exit error after the session exit
                    if close_error is None:
                        close_error = transport_error
        finally:
            if self.__context_token is not None:
                _REMOTE_SCOPE_CONTEXT.reset(self.__context_token)
                self.__context_token = None
            self.__receipt._finish_transport_scope(self, close_error=close_error)
        if close_error is not None:
            raise close_error
        return False


@dataclass
class RemoteSourceReceipt:
    """The exact SDK transport/session receipt for one accepted remote route."""

    parent: Any
    family: str
    source_identity: str
    operation_id: str
    close_generation: int = 0
    _transport: Any = field(default=None, repr=False)
    _streams: Any = field(default=None, repr=False)
    _session: Any = field(default=None, repr=False)
    _task: Any = field(default=None, repr=False)
    _last_transport: Any = field(default=None, repr=False)
    _last_session: Any = field(default=None, repr=False)
    _resolved_identity: str | None = None
    _transport_closed: bool = False
    _accepted: bool = False
    _close_requested: bool = False
    _state: str = "reserved"
    _unknown_reason: str | None = None
    _transport_closes: int = 0
    _last_scope: _RemoteTransportScope | None = field(default=None, repr=False)
    _active_scope: _RemoteTransportScope | None = field(default=None, repr=False)
    _server: Any = field(default=None, repr=False)
    _owner_ref: Any = field(default=None, repr=False)
    _pending_transport: Any = field(default=None, repr=False)
    _http_attempt: _RemoteHTTPAttempt | None = field(default=None, repr=False)
    _http_attempt_task: Any = field(default=None, repr=False)
    _http_attempt_token: Any = field(default=None, repr=False)
    _http_event: _RemoteHTTPResponseEvent | None = field(default=None, repr=False)
    _http_event_ambiguous: bool = field(default=False, repr=False)
    _http_witness_issued: bool = field(default=False, repr=False)

    @property
    def state(self) -> str:
        return self._state

    @property
    def accepted(self) -> bool:
        return self._accepted

    @property
    def transport(self) -> Any:
        if self._transport is not None:
            return self._transport
        if self._state == "unknown" and self._last_scope is not None:
            return self._last_transport
        return None

    @property
    def session(self) -> Any:
        if self._session is not None:
            return self._session
        if self._state == "unknown" and self._last_scope is not None:
            return self._last_session
        return None

    @property
    def task(self) -> Any:
        return self._task

    @property
    def _http_context(self) -> Any:
        attempt = self._http_attempt
        return None if attempt is None else attempt.context

    @property
    def resolved_identity(self) -> str | None:
        return self._resolved_identity

    @property
    def unknown_reason(self) -> str | None:
        return self._unknown_reason

    @property
    def source_object(self) -> Any:
        """The currently bound SDK object, for source-owned diagnostics only."""
        return (
            self._session if self._session is not None else
            self._transport if self._transport is not None else
            self._last_session if self._last_session is not None else self._last_transport
        )

    def identity(self) -> tuple[Any, ...]:
        parent = self.parent
        return (
            self.family,
            self.source_identity,
            self.operation_id,
            getattr(parent, "profile_id", None),
            getattr(parent, "process_instance", None),
            getattr(parent, "authority_epoch", None),
            getattr(parent, "physical_owner_epoch", None),
            getattr(parent, "root_handle", None),
        )

    def _owner_is_current(self) -> bool:
        parent = self.parent
        authority = getattr(parent, "profile_authority", None)
        if authority is None or getattr(parent, "profile_id", None) != getattr(authority, "profile_id", None):
            return False
        if getattr(parent, "process_instance", None) != getattr(authority, "instance_id", None):
            return False
        if getattr(parent, "authority_epoch", None) != getattr(authority, "epoch", None):
            return False
        try:
            from gateway.runtime_ownership import process_ownership
            return process_ownership.reservation_is_current(
                parent.profile_home, parent.physical_owner_epoch,
            ) is True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False

    def _mark_unknown_local(self, reason: str) -> None:
        if self._state == "terminal":
            return
        self._state = "unknown"
        self._unknown_reason = str(reason)

    def mark_unknown(self, reason: str) -> None:
        """Retain this exact source and parent after an ambiguous close or bind."""
        self._mark_unknown_local(reason)
        parent = self.parent
        if getattr(parent, "state", None) != "unknown":
            from gateway.runtime_service_operations import mark_unknown as mark_parent_unknown
            mark_parent_unknown(parent, reason)

    def bind_task(self, task: Any) -> None:
        """Bind the actual MCPServerTask that owns the source scope lifetime."""
        if task is None or not callable(getattr(task, "done", None)):
            self.mark_unknown("remote_task_identity_missing")
            raise RemoteSourceError("remote_task_identity_missing")
        if self._state in {"unknown", "terminal"} or not self._owner_is_current():
            self.mark_unknown("foreign_custody")
            raise RemoteSourceError("foreign_custody")
        if self._task is not None and self._task is not task:
            self.mark_unknown("remote_task_replaced")
            raise RemoteSourceError("remote_task_replaced")
        self._task = task

    def _source_validate_server(self, server: Any, task: Any) -> None:
        """Validate the actual MCPServerTask before any source SDK object is built."""
        if (
            server is None
            or getattr(server, "_i16_source_owner", None) is not self._owner
            or getattr(server, "_task", None) is not task
            or task is not asyncio.current_task()
            or self._task is not task
            or self._state in {"unknown", "terminal"}
            or not self._owner_is_current()
        ):
            self.mark_unknown("remote_server_capability_unavailable")
            raise RemoteSourceError("remote_server_capability_unavailable")
        if self._server is not None and self._server is not server:
            self.mark_unknown("remote_server_replaced")
            raise RemoteSourceError("remote_server_replaced")
        self._server = server

    @property
    def _owner(self) -> Any:
        return self._owner_ref

    def _source_begin_http_attempt(self, server: Any, task: Any) -> None:
        """Start a private attempt for the actual MCPServerTask HTTP client."""
        self._source_validate_server(server, task)
        self._http_attempt_task = task
        self._http_attempt = _RemoteHTTPAttempt(self, server, task)
        self._http_attempt_token = self._http_attempt.token
        self._http_event = None
        self._http_event_ambiguous = False
        self._http_witness_issued = False

    def _source_register_transport(self, server: Any, task: Any, transport: Any) -> Any:
        """Register exactly one source-created SDK transport context for this server."""
        self._source_validate_server(server, task)
        if (
            transport is None
            or not callable(getattr(transport, "__aenter__", None))
            or not callable(getattr(transport, "__aexit__", None))
            or self._pending_transport is not None
            or self._active_scope is not None
        ):
            self.mark_unknown("remote_transport_factory_unavailable")
            raise RemoteSourceError("remote_transport_factory_unavailable")
        self._pending_transport = transport
        return transport

    def _source_open_transport_scope(
        self, server: Any, transport: Any, *, session_kwargs: dict[str, Any], task: Any,
    ) -> _RemoteTransportScope:
        """Open only the context previously built by this source's SDK factory."""
        self._source_validate_server(server, task)
        if (
            transport is None
            or transport is not self._pending_transport
            or self._active_scope is not None
        ):
            self.mark_unknown("remote_transport_scope_unavailable")
            raise RemoteSourceError("remote_transport_scope_unavailable")
        self._pending_transport = None
        return _RemoteTransportScope(self, transport, session_kwargs, task)

    def _source_streamable_http_transport(
        self, server: Any, *, url: str, headers: dict[str, str], connect_timeout: float,
        ssl_verify: bool, client_cert: Any, oauth_auth: Any, strict_cfg_headers: bool,
        configured_header_names: set[str],
    ) -> Any:
        """Build the HTTP context and HTTPX client inside the source boundary."""
        task = asyncio.current_task()
        self._source_validate_server(server, task)
        attempt = self._http_attempt
        if attempt is None or attempt.server is not server or attempt.task is not task:
            self.mark_unknown("remote_http_attempt_unavailable")
            raise RemoteSourceError("remote_http_attempt_unavailable")
        from tools import mcp_tool_transport as transport_module
        from tools.mcp_tool_common import _core
        if not _core._MCP_NEW_HTTP:
            if strict_cfg_headers:
                raise ImportError(
                    f"MCP server '{server.name}' requires mcp >= 1.24.0 to enforce "
                    "the portable redirect-header boundary (strict_redirect_headers)."
                )
            return self._source_register_transport(
                server,
                task,
                _core.streamablehttp_client(
                    url, headers=headers, timeout=float(connect_timeout), verify=ssl_verify,
                    **transport_module._present(auth=oauth_auth),
                ),
            )
        httpx = _core.sdk_httpx()
        build_client = transport_module._make_redirect_header_stripper(
            httpx,
            httpx.URL(url),
            strict=strict_cfg_headers,
            configured_header_names=configured_header_names,
        )
        inner_transport = _RemoteHTTPMarkingTransport(
            httpx.AsyncHTTPTransport(verify=ssl_verify, **transport_module._present(cert=client_cert)),
            attempt,
        )
        mounts = transport_module._mcp_proxy_mounts(
            httpx, url, ssl_verify, client_cert, server.name,
        )
        if mounts:
            mounts = {
                key: _RemoteHTTPMarkingTransport(value, attempt)
                for key, value in mounts.items()
            }
        sink = getattr(server, "_http_rejection", None)
        if not isinstance(sink, dict):
            sink = {}
            server._http_rejection = sink
        client_kwargs: dict[str, Any] = {
            "follow_redirects": True,
            "timeout": httpx.Timeout(float(connect_timeout), read=300.0),
            **({"headers": headers} if headers else {}),
            "event_hooks": {
                "request": [attempt.record_request],
                "response": [transport_module._make_http_rejection_recorder(
                    sink, on_response=attempt.source_response_hook(),
                )],
            },
            "transport": transport_module._make_mcp_body_cap_transport(httpx, inner_transport),
            **transport_module._present(
                mounts=mounts,
                auth=oauth_auth,
            ),
        }

        @asynccontextmanager
        async def source_owned_client_streams():
            async with build_client(**client_kwargs) as http_client:
                async with _core.streamable_http_client(url, http_client=http_client) as streams:
                    yield streams

        return self._source_register_transport(server, task, source_owned_client_streams())

    def _source_sse_transport(
        self, server: Any, *, url: str, headers: dict[str, str], connect_timeout: float,
        ssl_verify: bool, client_cert: Any, oauth_auth: Any, strict_cfg_headers: bool,
    ) -> Any:
        """Build the source-owned SSE context with the canonical SDK factory."""
        task = asyncio.current_task()
        self._source_validate_server(server, task)
        if strict_cfg_headers:
            raise ValueError(
                f"MCP server '{server.name}': strict_redirect_headers is not supported on the SSE transport."
            )
        from tools import mcp_tool_transport as transport_module
        from tools.mcp_tool_common import _core
        if _core.sse_client is None:
            raise ImportError(
                f"MCP server '{server.name}' requires SSE transport but mcp.client.sse.sse_client "
                "is not available. Upgrade the mcp package to get SSE support."
            )
        httpx_mod = _core.sdk_httpx()

        def source_sse_client_factory(headers=None, timeout=None, auth=None):
            inner = httpx_mod.AsyncHTTPTransport(
                verify=ssl_verify, **transport_module._present(cert=client_cert),
            )
            return httpx_mod.AsyncClient(
                follow_redirects=True,
                timeout=timeout if timeout is not None else httpx_mod.Timeout(30.0, read=300.0),
                transport=transport_module._make_mcp_body_cap_transport(httpx_mod, inner),
                **transport_module._present(
                    mounts=transport_module._mcp_proxy_mounts(
                        httpx_mod, url, ssl_verify, client_cert, server.name,
                    ),
                    headers=headers,
                    auth=auth,
                ),
            )

        return self._source_register_transport(
            server,
            task,
            _core.sse_client(
                url=url,
                headers=headers or None,
                timeout=float(connect_timeout),
                sse_read_timeout=300.0,
                httpx_client_factory=source_sse_client_factory,
                **transport_module._present(auth=oauth_auth),
            ),
        )

    def open_transport_scope(
        self, transport: Any, *, session_kwargs: dict[str, Any], task: Any,
    ) -> _RemoteTransportScope:
        """Reject the old public arbitrary-context capability."""
        self.mark_unknown("remote_transport_scope_capability_required")
        raise RemoteSourceError("remote_transport_scope_capability_required")

    def _begin_transport_scope(self, scope: _RemoteTransportScope) -> None:
        if (
            scope is None
            or self._active_scope is not None
            or self._state in {"unknown", "terminal"}
            or scope.task is not self._task
            or scope.task is not asyncio.current_task()
            or not self._owner_is_current()
            or not isinstance(scope.streams, (tuple, list))
            or len(scope.streams) < 2
        ):
            self.mark_unknown("remote_transport_scope_identity_missing")
            raise RemoteSourceError("remote_transport_scope_identity_missing")
        self._active_scope = scope
        self._transport = scope.transport
        self._streams = scope.streams
        self._session = None
        self._transport_closed = False
        self._last_scope = None
        self._accepted = True
        self._state = "accepted"
        self.parent.state = "physical_bound"

    def _bind_transport_scope_session(self, scope: _RemoteTransportScope) -> None:
        if (
            scope is not self._active_scope
            or self._transport is not scope.transport
            or self._streams is not scope.streams
            or scope.session is None
            or self._state in {"unknown", "terminal"}
            or not self._owner_is_current()
        ):
            self.mark_unknown("remote_session_scope_identity_missing")
            raise RemoteSourceError("remote_session_scope_identity_missing")
        self._session = scope.session

    def _finish_transport_scope(
        self, scope: _RemoteTransportScope, *, close_error: BaseException | None,
    ) -> None:
        """Consume only the source-created scope after both actual exits have run."""
        if scope is not self._active_scope or scope.task is not asyncio.current_task():
            self.mark_unknown("remote_transport_scope_identity_mismatch")
            return
        self._last_transport = scope.transport
        self._last_session = scope.session
        self._transport = None
        self._streams = None
        self._session = None
        self._transport_closed = True
        self._transport_closes += 1
        self._last_scope = scope
        if (
            close_error is not None
            or not scope.clean
            or not scope.transport_entered
            or not scope.transport_closed
            or not scope.session_entered
            or not scope.session_closed
            or not self._owner_is_current()
        ):
            self.mark_unknown("remote_transport_close_failed")
            return
        if scope.body_error is not None:
            # The initial HTTP rejection is still ambiguous until the exact source
            # event is authenticated and this same coordinator root is reconciled.
            self.mark_unknown("remote_transport_rejected")
            return
        self._active_scope = None
        if self._state not in {"unknown", "terminal"}:
            self._state = "accepted"

    def bind_identity(self, identity: Any) -> None:
        """Retain the transport's resolved, secret-free connection digest."""
        if type(identity) is not str or not identity:
            self.mark_unknown("remote_source_identity_missing")
            raise RemoteSourceError("remote_source_identity_missing")
        owner_current = self._owner_is_current()
        if self._state in {"unknown", "terminal"} or not owner_current:
            self.mark_unknown("foreign_custody")
            raise RemoteSourceError("foreign_custody")
        if self._resolved_identity is not None and self._resolved_identity != identity:
            self.mark_unknown("remote_source_identity_replaced")
            raise RemoteSourceError("remote_source_identity_replaced")
        self._resolved_identity = identity

    def begin_http_attempt(self, task: Any) -> Any:
        """Reject the old public shaped-response hook capability."""
        self.mark_unknown("remote_http_attempt_capability_required")
        raise RemoteSourceError("remote_http_attempt_capability_required")

    @staticmethod
    def _is_opaque_sdk_rejection(exception: BaseException) -> BaseException:
        try:
            from tools.mcp_tool_errors import _unwrap_exception_group
            root = _unwrap_exception_group(exception)
        except (AttributeError, ImportError, TypeError, ValueError):
            root = exception
        if (
            getattr(getattr(root, "error", None), "code", None) == -32603
            and "server returned an error response" in str(root).lower()
        ):
            return root
        raise RemoteSourceError("remote_http_rejection_unproven")

    def http_rejection_witness(self, exception: BaseException) -> _RemoteHTTPRejectionWitness:
        """Authenticate one actual response event inside the source boundary."""
        event = self._http_event
        scope = self._last_scope
        attempt = self._http_attempt
        if (
            not isinstance(exception, BaseException)
            or self._http_witness_issued
            or self._http_event_ambiguous
            or attempt is None
            or event is None
            or scope is None
            or event.scope is not scope
            or event.attempt is not attempt
            or event.server is not self._server
            or event.task not in attempt.callback_tasks
            or self._http_attempt_task is not asyncio.current_task()
            or self._state != "unknown"
            or self._unknown_reason != "remote_transport_rejected"
            or not scope.clean
            or not self._owner_is_current()
            or type(event.status) is not int
            or event.status not in _STREAMABLE_HTTP_REJECTION_STATUSES
            or event.method != "POST"
            or not event.url
            or event.request_marker is not attempt.token
            or event.response_marker is None
            or getattr(event.request, "extensions", {}).get(_REMOTE_REQUEST_MARKER) is not event.request_marker
            or getattr(event.response, "extensions", {}).get(_REMOTE_RESPONSE_MARKER) is not event.response_marker
        ):
            self.mark_unknown("remote_http_rejection_unproven")
            raise RemoteSourceError("remote_http_rejection_unproven")
        try:
            from tools.mcp_tool_errors import _unwrap_exception_group
            root = _unwrap_exception_group(exception)
        except (AttributeError, ImportError, TypeError, ValueError):
            root = exception
        response = getattr(root, "response", None)
        if response is not None:
            response_request = getattr(response, "request", None)
            if (
                response is not event.response
                or getattr(response, "status_code", None) != event.status
                or response_request is not event.request
                or getattr(response_request, "method", None) != event.method
                or str(getattr(response_request, "url", "")) != event.url
            ):
                self.mark_unknown("remote_http_rejection_unproven")
                raise RemoteSourceError("remote_http_rejection_unproven")
        else:
            self._is_opaque_sdk_rejection(exception)
        self._http_witness_issued = True
        return _RemoteHTTPRejectionWitness(event=event, exception=exception, scope=scope)

    def request_close(self) -> None:
        self._close_requested = True

    def prepare_replacement(self, reason: str, *, rejection: _RemoteHTTPRejectionWitness) -> None:
        """Re-arm this same root after an authenticated initial HTTP fallback.

        Streamable HTTP can reject its first request before an SSE-only endpoint is
        known. The SDK context has already exited in that path; a replacement is
        permitted only for that one typed rejection, after the exact prior transport
        has been observed closed. A close/readback failure remains UNKNOWN.
        """
        if reason != "streamable_http_rejected" or not isinstance(rejection, _RemoteHTTPRejectionWitness):
            self.mark_unknown("remote_replacement_reason_invalid")
            raise RemoteSourceError("remote_replacement_reason_invalid")
        event = self._http_event
        scope = self._last_scope
        attempt = self._http_attempt
        current_task = asyncio.current_task()
        parent = self.parent
        root = getattr(parent, "root_handle", None)
        service = getattr(parent, "_service", None)
        if (
            self._state != "unknown"
            or self._transport is not None
            or self._session is not None
            or not self._transport_closed
            or self._last_transport is None
            or scope is None
            or scope is not rejection.scope
            or scope.transport is not self._last_transport
            or scope.session is not self._last_session
            or scope.task is not current_task
            or scope.body_error is not rejection.exception
            or not scope.clean
            or self._unknown_reason != "remote_transport_rejected"
            or (self._task is not None and scope.task is not self._task)
            or event is None
            or attempt is None
            or rejection.event is not event
            or event.scope is not scope
            or event.attempt is not attempt
            or event.server is not self._server
            or event.task not in attempt.callback_tasks
            or event.request_marker is not attempt.token
            or event.response_marker is None
            or getattr(event.request, "extensions", {}).get(_REMOTE_REQUEST_MARKER) is not event.request_marker
            or getattr(event.response, "extensions", {}).get(_REMOTE_RESPONSE_MARKER) is not event.response_marker
            or event.status not in _STREAMABLE_HTTP_REJECTION_STATUSES
            or event.method.upper() != "POST"
            or self._http_attempt_task is not current_task
            or not self._http_witness_issued
            or type(self.source_identity) is not str
            or not self.source_identity
            or getattr(parent, "source_identity", None) != self.source_identity
            or type(self._resolved_identity) is not str
            or not self._resolved_identity
            or root is None
            or getattr(root, "profile_id", None) != getattr(parent, "profile_id", None)
            or getattr(root, "process_instance", None) != getattr(parent, "process_instance", None)
            or getattr(root, "authority_epoch", None) != getattr(parent, "authority_epoch", None)
            or getattr(root, "physical_owner_epoch", None) != getattr(parent, "physical_owner_epoch", None)
            or service is None
            or (
                getattr(service, "external_parent", None) is not None
                and getattr(service, "external_parent", None) is not parent
            )
            or getattr(service, "handle", None) is not root
            or getattr(service, "unknown", False)
            or getattr(service, "physical", None) is not None
            or getattr(parent, "_physical", None) is not None
            or getattr(service, "requests", None)
            or getattr(service, "durable_requests", None)
            or getattr(service, "delivery_unknown_reasons", None)
            or getattr(service, "delivery_terminalized", None)
            or not self._owner_is_current()
        ):
            self.mark_unknown("remote_replacement_unproven")
            raise RemoteSourceError("remote_replacement_unproven")
        try:
            restored = parent.coordinator.reconcile_unknown(
                root,
                owner_id=root.owner_id,
                profile_id=parent.profile_id,
                process_instance=parent.process_instance,
                authority_epoch=parent.authority_epoch,
            )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self.mark_unknown("remote_replacement_unproven")
            raise RemoteSourceError("remote_replacement_unproven") from exc
        if restored is not None:
            self.mark_unknown("remote_replacement_unproven")
            raise RemoteSourceError("remote_replacement_unproven")
        self._state = "accepted"
        self._unknown_reason = None
        self._close_requested = False
        self._active_scope = None
        self._last_scope = None
        self._http_attempt = None
        self._http_attempt_task = None
        self._http_attempt_token = None
        self._http_event = None
        self._http_event_ambiguous = False
        self._http_witness_issued = False
        parent.state = "request_reserved"

    def close_after_shutdown(self, task: Any = None) -> bool:
        """Prove task/transport shutdown before the existing parent is terminalized."""
        self.request_close()
        if task is not None and not getattr(task, "done", lambda: False)():
            self.mark_unknown("remote_task_join_failed")
            return False
        if self._task is not None:
            if task is not self._task or not self._task.done():
                self.mark_unknown("remote_task_identity_mismatch")
                return False
        if self._transport is not None or self._session is not None:
            self.mark_unknown("remote_transport_still_active")
            return False
        if self._state == "unknown":
            return False
        if self._accepted and not self._transport_closed:
            self.mark_unknown("remote_close_readback_missing")
            return False
        if not self._accepted:
            self.mark_unknown("remote_effect_unproven")
            return False
        if self._task is None:
            self.mark_unknown("remote_task_identity_missing")
            return False
        if self._resolved_identity is None:
            self.mark_unknown("remote_source_identity_missing")
            return False
        if not self._owner_is_current():
            self.mark_unknown("foreign_custody")
            return False
        self._state = "closed"
        return True

    def readback(self) -> dict[str, Any] | None:
        """Return a typed readback only after the exact source context has closed."""
        if self._state != "closed" or self._transport is not None or self._session is not None:
            return None
        return {
            "operation_id": self.operation_id,
            "family": self.family,
            "source_identity": self.source_identity,
            "resolved_identity": self._resolved_identity,
            "profile_id": self.parent.profile_id,
            "process_instance": self.parent.process_instance,
            "authority_epoch": self.parent.authority_epoch,
            "physical_owner_epoch": self.parent.physical_owner_epoch,
            "root_handle": self.parent.root_handle,
            "task": self._task,
            "close_generation": self.close_generation,
            "transport_closes": self._transport_closes,
        }


@dataclass
class RemoteSidecarOwner(SidecarOwner):
    """I16 source view carrying the exact remote receipt."""

    receipt: RemoteSourceReceipt | None = None

    def __post_init__(self) -> None:
        if self.receipt is not None:
            self.receipt._owner_ref = self

    @property
    def source_object(self) -> Any:
        return None if self.receipt is None else self.receipt.source_object

    @property
    def remote_state(self) -> str:
        return "unknown" if self.receipt is None else self.receipt.state

    def open_transport_scope(
        self, transport: Any, *, session_kwargs: dict[str, Any], task: Any,
    ) -> _RemoteTransportScope:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt.open_transport_scope(transport, session_kwargs=session_kwargs, task=task)

    def bind_task(self, task: Any) -> None:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        self.receipt.bind_task(task)

    def bind_identity(self, identity: Any) -> None:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        self.receipt.bind_identity(identity)

    def _source_begin_http_attempt(self, server: Any, task: Any) -> None:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        self.receipt._source_begin_http_attempt(server, task)

    def _source_streamable_http_transport(self, server: Any, **kwargs: Any) -> Any:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt._source_streamable_http_transport(server, **kwargs)

    def _source_sse_transport(self, server: Any, **kwargs: Any) -> Any:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt._source_sse_transport(server, **kwargs)

    def _source_open_transport_scope(
        self, server: Any, transport: Any, *, session_kwargs: dict[str, Any], task: Any,
    ) -> _RemoteTransportScope:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt._source_open_transport_scope(
            server, transport, session_kwargs=session_kwargs, task=task,
        )

    def begin_http_attempt(self, task: Any) -> Any:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt.begin_http_attempt(task)

    def http_rejection_witness(self, exception: BaseException) -> _RemoteHTTPRejectionWitness:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        return self.receipt.http_rejection_witness(exception)

    def prepare_replacement(self, reason: str, *, rejection: _RemoteHTTPRejectionWitness) -> None:
        if self.receipt is None:
            raise RemoteSourceError("remote_receipt_missing")
        self.receipt.prepare_replacement(reason, rejection=rejection)
