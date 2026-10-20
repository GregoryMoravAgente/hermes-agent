"""Execution finalization for one admitted API run."""

from contextlib import suppress
from typing import Any, Optional

from gateway.runtime_custody import CustodyRefusal


def arm_legacy_process_custody(
    custody: Any, task_id: str, *, owner_task_id: Optional[str] = None,
):
    """Arm the I03 scope with public task and private per-run owner identities."""
    from gateway.platforms.api_server_run_custody import ApiRunCustodyError
    from gateway.session_context import set_i03_custody_binding

    refusal = custody.arm_process_scope(
        task_id, owner_task_id=owner_task_id if owner_task_id is not None else task_id)
    if refusal is not None:
        raise ApiRunCustodyError(refusal)
    return set_i03_custody_binding(custody)


def reset_legacy_process_custody(binding_token: Any) -> None:
    """Reset the I03 context after the source-owned process scope has been closed."""
    if binding_token is not None:
        from gateway.session_context import reset_i03_custody_binding
        reset_i03_custody_binding(binding_token)


def close_legacy_process_custody(custody: Any):
    """Close the direct scope and require the existing process-owner proof before release."""
    refusal = custody.close_process_scope()
    if refusal is not None:
        custody.mark_unknown(getattr(refusal, "code", "process_scope_unknown"))
        return refusal
    refusal = custody.observe_process_owner()
    if refusal is not None:
        custody.mark_unknown(getattr(refusal, "code", "cross_process_unproven"))
    return refusal


def finalize_legacy_stream_custody(custody: Any, task_id: str):
    """Close one stream's original process scope after its durable terminal readback."""
    refusal = custody.close_process_scope()
    if refusal is not None:
        return refusal
    if custody.worker_submitted or custody.worker_done:
        return custody.finish(task_id)
    return custody.mark_no_worker()


def legacy_stream_profile(adapter: Any, request_profile: Optional[str]) -> Optional[str]:
    """Use the request profile only when a live runtime authority can authenticate it."""
    runner = getattr(adapter, "gateway_runner", None)
    if (getattr(runner, "session_authority", None) is not None
            or getattr(runner, "session_authorities", None) is not None):
        return request_profile
    return None


def reserve_legacy_stream(adapter: Any, request_profile: Optional[str], run_id: str):
    """Reserve a stream under the live authority or the fixture's descriptor-owned profile."""
    from gateway.platforms.api_server_run_custody import reserve_legacy
    return reserve_legacy(adapter, legacy_stream_profile(adapter, request_profile), run_id)


def begin_legacy_stream_turn(
    adapter: Any, custody: Any, *, task_id: str, profile_id: Optional[str], session_id: Optional[str],
):
    """Arm and write the stream's source boundary before it creates worker effects."""
    binding_token = arm_legacy_process_custody(custody, task_id)
    try:
        begin_legacy_direct_turn(adapter, custody, profile_id=profile_id, session_id=session_id)
    except BaseException:
        # The caller only receives the token after this boundary succeeds.  Reset it here when
        # the durable writer refuses, so the outer finalizer cannot accidentally reset ``None``
        # and leak the authenticated custody binding into the next stream task.
        reset_legacy_process_custody(binding_token)
        raise
    return binding_token


def submit_legacy_stream_worker(custody: Any, worker_task: Any) -> None:
    """Retain and admit the stream Task before its result can be consumed."""
    bind_legacy_worker(custody, worker_task)
    refusal = custody.mark_worker_submitted()
    if refusal is not None:
        custody.mark_unknown(getattr(refusal, "code", "worker_submission_unknown"))
        raise RuntimeError("API session stream worker custody became unknown")


def finish_legacy_stream_custody(custody: Any, task_id: str, binding_token: Any) -> None:
    """Apply the terminal scope receipt after status, delivery, and control receipts."""
    try:
        refusal = finalize_legacy_stream_custody(custody, task_id)
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, "code", "stream_custody_finalization_unknown"))
    finally:
        reset_legacy_process_custody(binding_token)


def begin_legacy_direct_turn(
    adapter: Any, custody: Any, *, profile_id: Optional[str], session_id: Optional[str],
) -> None:
    """Establish the durable writer before a supported pre-authority worker starts.

    The caller invokes this inside the source-owned failure boundary so a refused running
    receipt still reaches its no-worker finalizer.
    """
    # A session SSE wrapper may establish the writer before it creates its retained worker task;
    # the canonical _run_agent path reaches this helper again after that task has started.  The
    # existing durable status/readback is the same reservation, so do not rewrite it or create a
    # second writer boundary on the second entry.
    if getattr(custody, "reservation_key", None) is not None and getattr(custody, "status_persisted", False):
        return
    from gateway.platforms.api_server_run_custody import (
        ApiRunCustodyError, prepare_legacy_store, record_legacy_status)

    refusal = prepare_legacy_store(adapter, custody, profile_id=profile_id)
    if refusal is not None:
        release_refusal = custody.release_unaccepted()
        if release_refusal is not None:
            custody.mark_unknown(getattr(release_refusal, "code", "legacy_writer_unknown"))
        else:
            adapter._run_custody.pop(custody.run_id, None)
            getattr(adapter, "_run_launches", {}).pop(custody.run_id, None)
        raise ApiRunCustodyError(refusal)
    refusal = record_legacy_status(adapter, custody, "running", session_id=session_id or "")
    if refusal is not None:
        raise ApiRunCustodyError(refusal)


def bind_legacy_worker(custody: Any, worker_future: Any) -> None:
    """Retain the exact submitted Future on custody and its source-owned launch."""
    custody.worker_future = worker_future
    custody.execution_task = worker_future
    launch = getattr(custody, "source_launch", None)
    if launch is not None:
        launch.worker_future = worker_future
        launch.execution_task = worker_future


def record_legacy_direct_result(
    adapter: Any, custody: Any, result: Any, usage: Any, *, active_run_id: Optional[str],
) -> None:
    """Persist the direct worker's terminal result before its source handle is consumed."""
    if custody.reservation_key is None:
        return
    from gateway.platforms.api_server_run_custody import (
        ApiRunCustodyError, complete_legacy_source, record_legacy_status)
    from gateway.platforms.api_server_runs import terminal_run_status

    terminal_status, terminal_fields = terminal_run_status(
        result if isinstance(result, dict) else {"failed": True})
    if isinstance(result, dict) and terminal_status == "completed":
        terminal_fields["output"] = result.get("final_response", "")
    elif isinstance(result, dict) and result.get("error"):
        terminal_fields["error"] = str(result["error"])
    terminal_fields["usage"] = usage
    refusal = record_legacy_status(adapter, custody, terminal_status, **terminal_fields)
    if refusal is not None:
        raise ApiRunCustodyError(refusal)
    if not active_run_id:
        refusal = complete_legacy_source(custody, active_run_id=active_run_id)
        if refusal is not None:
            raise ApiRunCustodyError(refusal)


def finalize_api_run(
    adapter: Any,
    run: Any,
    api_server: Any,
    loop: Any,
    status: str,
    extra: Optional[dict] = None,
    **fields: Any,
) -> None:
    """Persist one terminal decision and publish its matching event.

    The custody receipt is read back before the event is queued.  Keeping this phase in a
    topical sibling leaves the worker driver responsible for execution order while this helper
    owns the status/receipt/event agreement.
    """
    from gateway.platforms.api_server_runs import (
        _enqueue_terminal_event, _run_event, _source_status_receipt,
        canonical_terminal_decision,
    )

    extra = extra or {}
    if run.run_id in adapter._shutdown_interrupted_run_ids:
        status = "interrupted"
        fields = {"error": "Gateway shutdown interrupted the run."}
        extra = {}
    _, status_receipt = adapter._set_run_status(
        run.run_id, status, _return_receipt=True, **fields, last_event=f"run.{status}", **extra)
    event_status = status
    event_fields = {**fields, **extra}
    if run.custody is not None:
        status_receipt = _source_status_receipt(run, status_receipt)
        run.status_receipt = status_receipt
        run.owner._run_launches[run.run_id].status_receipt = status_receipt
        if status_receipt is None:
            run.custody.mark_unknown("status_commit_unproven")
        else:
            event_status, event_fields = canonical_terminal_decision(
                status, event_fields, status_receipt)
            if event_status == "unknown":
                run.custody.mark_unknown("canonical_terminal_outcome_inconsistent")
            elif event_status != status:
                _, status_receipt = adapter._set_run_status(
                    run.run_id, event_status, _return_receipt=True,
                    **event_fields, last_event=f"run.{event_status}")
                status_receipt = _source_status_receipt(run, status_receipt)
                run.status_receipt = status_receipt
                run.owner._run_launches[run.run_id].status_receipt = status_receipt
        status_refusal = run.custody.mark_status_receipt(
            status_receipt, require_store=run.run_id in adapter._run_idempotency_ids)
        if status_refusal is None and run.custody.unknown_reason:
            status_refusal = CustodyRefusal("custody_unknown", "I03_api_run_direct")
        if status_refusal is not None:
            from gateway.platforms.api_server_run_recovery import publish_unknown_status
            event_status = "unknown"
            event_fields = publish_unknown_status(
                adapter, run.run_id, run.custody, status_refusal)
    with suppress(Exception):
        # A worker can finish before its Future is wrapped, so awaiting it need not yield to
        # already-scheduled commentary/tool callbacks.
        loop.call_soon(_enqueue_terminal_event, run, _run_event(
            run.run_id, f"run.{event_status}", **event_fields))
