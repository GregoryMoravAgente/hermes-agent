"""Shared real adapter fixtures for API custody tests."""

import types

from aiohttp import web

from gateway.config import PlatformConfig
from gateway.platforms.api_server import (
    APIServerAdapter, cors_middleware, security_headers_middleware,
)


def make_adapter(
    api_key: str = "", cors_origins=None, *, process_instance: str = "test-api-server",
    monkeypatch=None,
):
    extra = {"key": api_key} if api_key else {}
    if cors_origins is not None:
        extra["cors_origins"] = cors_origins
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra=extra))
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence
    import tools.process_registry as process_registry_module
    from tools.process_registry import ProcessRegistry

    coordinator = RuntimeCustodyCoordinator(
        process_instance=process_instance, authority_epoch=1, fence=RetirementFence())
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    profile_id = registry._process_profile_id()
    if monkeypatch is not None:
        monkeypatch.setattr(process_registry_module, "process_registry", registry)
    adapter.gateway_runner = types.SimpleNamespace(
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": process_instance, "authority_epoch": 1,
            "profile_id": profile_id, "served_profiles": [profile_id]},
        session_authority=None,
    )
    adapter._fixture_process_registry = registry
    return adapter


def create_runs_app(adapter: APIServerAdapter) -> web.Application:
    middleware = [mw for mw in (cors_middleware, security_headers_middleware) if mw is not None]
    app = web.Application(middlewares=middleware)
    app["api_server_adapter"] = adapter
    app.router.add_post("/v1/runs", adapter._handle_runs)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    app.router.add_get("/v1/runs/{run_id}/events", adapter._handle_run_events)
    app.router.add_post("/v1/runs/{run_id}/approval", adapter._handle_run_approval)
    app.router.add_post("/v1/runs/{run_id}/steer", adapter._handle_steer_run)
    app.router.add_post("/v1/runs/{run_id}/stop", adapter._handle_stop_run)
    return app


def use_idempotency_db(adapter, path):
    from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
    adapter._run_idempotency_store.close()
    adapter._run_idempotency_store = RunIdempotencyStore(str(path))
