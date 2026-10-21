"""Loopback HTTP + real API executor/session bindings, with inference replaced.

No paid model calls. Captures what the runtime hands the model, not provider behavior.
"""
import asyncio
import copy
import json
import os
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer


def _observe_real_agent_runs(captured):
    """Observe canonical AIAgent entry and restore the class in the enclosing finally."""
    from contextlib import contextmanager
    from functools import wraps
    import copy
    import inspect

    import gateway.session_context as sc
    from gateway.session_context import get_session_env
    from run_agent import AIAgent
    from tools.delegate_tool_dispatch import _resolve_async_wake_sid

    original = AIAgent.run_conversation
    had_local_method = "run_conversation" in AIAgent.__dict__
    local_method = AIAgent.__dict__.get("run_conversation")

    @contextmanager
    def installed():
        @wraps(original)
        def observe(self, *args, **kwargs):
            bound = inspect.signature(original).bind(self, *args, **kwargs)
            bound.apply_defaults()
            session_id = get_session_env("HERMES_SESSION_CHAT_ID", "")
            history = copy.deepcopy(bound.arguments.get("conversation_history"))
            wake_args = [session_id]
            if len(inspect.signature(_resolve_async_wake_sid).parameters) > 1:
                wake_args.append(sc.session_history_delivery_supported())
            captured.append({
                "session_id": session_id,
                "target": _resolve_async_wake_sid(*wake_args),
                "history": history,
            })
            return original(self, *args, **kwargs)

        AIAgent.run_conversation = observe
        try:
            yield
        finally:
            if had_local_method:
                AIAgent.run_conversation = local_method
            else:
                del AIAgent.run_conversation

    return installed()


async def _join_real_run_tasks(tasks):
    """Join exact retained /v1/runs tasks while preserving the first worker failure."""
    first_error = None
    for task in tasks:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if first_error is None:
                first_error = exc
        except Exception as exc:  # health: allow BLE001 -- preserve the first source-task failure across cleanup joins
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _capture_real_run_tasks(adapter, retained):
    """Retain exact source-owned asyncio tasks and return newly retained objects."""
    added = []
    for task in getattr(adapter, "_active_run_tasks", {}).values():
        if not isinstance(task, asyncio.Task):
            continue
        if not any(task is previous for previous in retained):
            retained.append(task)
            added.append(task)
    return tuple(added)


async def _join_session_run_tasks(tasks):
    """Require and join the exact source tasks before the next chat is accepted."""
    if not tasks:
        raise AssertionError("session /v1/runs accepted without a source-owned task")
    await _join_real_run_tasks(tasks)
    return all(task.done() for task in tasks)


async def probe():
    import hermes_state
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner, _build_gateway_agent_history
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from gateway.wake import persist_delegation_delivery
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home

    home = await asyncio.to_thread(Path(os.environ["HERMES_HOME"]).resolve)
    await asyncio.to_thread(home.mkdir, parents=True, exist_ok=True)
    home_token = set_hermes_home_override(home)
    # The probe is a subprocess entrypoint rather than the pytest fixture. Reset the
    # import-time DB constant so the existing call-time HERMES_HOME scope selects this
    # reserved profile's state.db before GatewayRunner opens its store.
    hermes_state.DEFAULT_DB_PATH = hermes_state._IMPORT_DEFAULT_DB_PATH
    runner = None
    adapter = None
    reserved = False

    async def exercise():
        db.create_session("parent", source="api_server")
        db.append_message("parent", "user", "request")
        db.append_message("parent", "assistant", "acknowledged")
        db.end_session("parent", "compression")
        db.create_session("child", source="api_server", parent_session_id="parent")
        captured = []
        run_tasks = []

        app = web.Application()
        app.router.add_post("/v1/chat/completions", adapter._handle_chat_completions)
        app.router.add_post("/v1/runs", adapter._handle_runs)
        records = []
        runs = []
        session_run_input = "session-run-accepted"
        session_run_reply = "delegation-http-probe"
        session_run_raw_history = None
        session_run_expected_history = None
        session_run_resolved_session_id = None
        chat_resumed_history = None
        session_run_task_joined = False
        with _observe_real_agent_runs(captured):
            try:
                async with TestClient(TestServer(app)) as client:
                    for stream in (False, True):
                        for explicit in (False, True):
                            headers = {"Authorization": "Bearer fixture-api-key"}
                            if explicit:
                                headers["X-Hermes-Session-Id"] = "parent"
                            response = await client.post("/v1/chat/completions", headers=headers, json={"messages": [{"role": "user", "content": "continue"}], "stream": stream})
                            body = await response.text()
                            records.append({"stream": stream, "explicit": explicit, "status": response.status, "header": response.headers.get("X-Hermes-Session-Id"), "runtime": captured[-1] if captured else None, "body": body[:120]})
                    calls_before = len(captured)
                    evt = {"type": "async_delegation", "delegation_id": "unit-http"}
                    delivery_error = None
                    try:
                        await asyncio.gather(*(persist_delegation_delivery(adapter, text="DELIVERY_RESULT", session_id="parent", evt=evt) for _ in range(2)))
                    except Exception as exc:  # health: allow BLE001 -- diagnostic preserves the opaque delivery failure class alongside the remaining behavioral observations
                        delivery_error = type(exc).__name__
                    calls_after = len(captured)
                    # The /v1/runs session path must be compared with the canonical durable
                    # history it will load immediately before admission. Keep this snapshot
                    # independent of the AIAgent observer; the later chat turn is intentionally
                    # accepted after the first run and is checked separately.
                    session_run_resolved_session_id = await asyncio.to_thread(
                        db.resolve_resume_session_id, "parent")
                    # Keep the durable DB projection independent from the model-history
                    # projection.  The latter is derived by the same canonical serializer
                    # used by GatewayRunner, never from the observed runtime history.
                    session_run_raw_history = copy.deepcopy(await asyncio.to_thread(
                        db.get_messages_as_conversation, session_run_resolved_session_id))
                    session_run_expected_history, observed_context = _build_gateway_agent_history(
                        copy.deepcopy(session_run_raw_history),
                        channel_prompt=None,
                        inject_timestamps=False,
                    )
                    assert observed_context is None
                    resumed_history = copy.deepcopy(session_run_expected_history)
                    snapshot = [{"role": "user", "content": "caller snapshot"}]
                    adapter._response_store.put("resp-seed", {"conversation_history": snapshot, "session_id": "parent"})
                    db.create_session("declared", source="api_server", session_key="fixture-key")
                    db.append_message("declared", "assistant", "DECLARED_HISTORY")
                    session_run_tasks = ()
                    for name, body, extra_headers in (
                        ("session", {"session_id": "parent"}, {}),
                        ("caller_history", {"session_id": "parent", "conversation_history": snapshot}, {}),
                        ("response_chain", {"previous_response_id": "resp-seed"}, {}),
                        ("declared_key", {}, {"X-Hermes-Session-Key": "fixture-key"}),
                    ):
                        count = len(captured)
                        source_tasks = []
                        run_input = session_run_input if name == "session" else "continue"
                        response = await client.post("/v1/runs", headers={"Authorization": "Bearer fixture-api-key", **extra_headers}, json={"input": run_input, **body})
                        if response.status == 202:
                            source_tasks.extend(_capture_real_run_tasks(adapter, run_tasks))
                        await response.read()
                        async def wait_for_run():
                            while len(captured) == count:
                                await asyncio.sleep(0.01)
                        if response.status == 202:
                            await asyncio.wait_for(wait_for_run(), 10)
                            source_tasks.extend(_capture_real_run_tasks(adapter, run_tasks))
                        runs.append({"name": name, "status": response.status, "runtime": captured[-1] if len(captured) > count else None})
                        if name == "session":
                            # Join only the exact source tasks created for this /v1/runs
                            # request; an empty capture is a fixture failure, never proof of
                            # a joined worker. The final flag is read from each task itself.
                            session_run_tasks = tuple(source_tasks)
                            if response.status == 202:
                                session_run_task_joined = await _join_session_run_tasks(session_run_tasks)
                            response = await client.post("/v1/chat/completions", headers={"Authorization": "Bearer fixture-api-key", "X-Hermes-Session-Id": "parent"}, json={"messages": [{"role": "user", "content": "read result"}]})
                            await response.read()
                            chat_resumed_history = copy.deepcopy(captured[-1]["history"])
            finally:
                await _join_real_run_tasks(tuple(run_tasks))
        durable_child_rows = sum(
            1 for message in db.get_messages("child")
            if message.get("display_kind") == "async_delegation_complete"
            and message.get("content") == "DELIVERY_RESULT"
        )
        result = {
            "requests": records, "runs": runs, "delivery_error": delivery_error,
            "unsolicited_calls": calls_after-calls_before, "resumed_history": resumed_history,
            "session_run_raw_history": session_run_raw_history,
            "session_run_expected_history": session_run_expected_history,
            "session_run_resolved_session_id": session_run_resolved_session_id,
            "session_run_input": session_run_input, "session_run_reply": session_run_reply,
            "session_run_task_count": len(session_run_tasks),
            "session_run_task_joined": session_run_task_joined,
            "chat_resumed_history": chat_resumed_history,
            "durable_child_rows": durable_child_rows,
        }
        return result

    try:
        with FakeLLMServer(default_text="delegation-http-probe") as provider:
            write_hermes_home(home, provider.base_url)
            process_ownership.reserve([home])
            reserved = True
            runner = GatewayRunner(GatewayConfig(sessions_dir=home / "sessions"))
            adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "fixture-api-key"}))
            adapter.gateway_runner = runner
            runner.adapters = {Platform.API_SERVER: adapter}
            await initialize_gateway_runtime(runner)
            runner._wire_adapter_handlers(adapter)
            db = runner.session_authority.db
            return await exercise()
    finally:
        authority_retired = runner is None
        try:
            if runner is not None:
                from gateway.run_runtime import _retire_profile_authority
                authority = getattr(runner, "session_authority", None)
                authority_retired = authority is None or await _retire_profile_authority(authority)
            if authority_retired:
                if adapter is not None:
                    adapter._response_store.close()
                    adapter._run_idempotency_store.close()
                if runner is not None:
                    runner.session_store.close_all_db_handles()
                    runner.close_all_session_db_handles()
                if reserved:
                    process_ownership.close()
        finally:
            reset_hermes_home_override(home_token)

if __name__ == "__main__":
    print(json.dumps(asyncio.run(probe()), indent=2, default=str))
