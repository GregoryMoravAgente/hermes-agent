"""Sequential tool Future polling kept outside the dispatch facade."""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any


# Keep /stop responsive while a non-cooperative tool is running.
_SEQUENTIAL_INTERRUPT_POLL_SECONDS = 1.0


def poll_sequential_future(
    agent,
    future,
    function_name: str,
    deadline: float | None,
    started: float,
    authorization_gate,
    *,
    poll_seconds: float = _SEQUENTIAL_INTERRUPT_POLL_SECONDS,
) -> tuple[str, Any]:
    """Wait in interrupt-poll slices and return the existing sequential states."""
    last_heartbeat = 0
    while True:
        wait_slice = poll_seconds
        if deadline is not None:
            remaining = deadline + authorization_gate.excluded_seconds() - time.monotonic()
            if remaining <= 0:
                return "timeout", None
            wait_slice = min(wait_slice, remaining)
        try:
            return "done", future.result(timeout=wait_slice)
        except concurrent.futures.TimeoutError:
            # A settled Future never unsettles. Re-waiting after a worker-death
            # TimeoutError would otherwise spin until the deadline.
            if future.done():
                return "done", future.result()
            if agent._interrupt_requested:
                return "interrupted", None
            elapsed = int(time.monotonic() - started)
            if elapsed - last_heartbeat >= 30:
                last_heartbeat = elapsed
                agent._touch_activity(f"sequential tool running ({elapsed}s): {function_name}")


def submit_concurrent_workers(batch, executor, runnable: list[int]) -> tuple[list, dict]:
    """Submit a concurrent batch through the owned worker boundary.

    Kept beside the wait loop so the dispatch facade stays below its per-file
    health cap. Imports the facade at call time: tests and extension surfaces
    that patch its submission hooks still patch the binding the route reads.
    """
    import agent.tool_executor as te

    futures = []
    future_to_index = {}
    for submit_index, i in enumerate(runnable):
        try:
            lease = te.admit_tool_worker(batch.agent, "concurrent")
        except te.ToolWorkerAdmissionError as admission_error:
            ref = batch.parsed_calls[i].ref(batch.effective_task_id)
            batch.results[i] = te._ToolOutcome(
                ref, f"Error executing tool '{ref.name}': nested worker admission refused: {admission_error}",
                0.0, True, False,
            )
            continue
        try:
            future = te.submit_tool_worker(executor, lease, batch.run_worker, i, submit_index)
        except te.ToolWorkerSubmissionError as submit_error:
            if submit_error.accepted in {"no", "unknown"} and isinstance(submit_error.error, RuntimeError) \
                    and te._is_interpreter_shutdown_submit_error(submit_error.error):
                submit_error.handle.mark_submit_refused()
                skipped = runnable[submit_index:]
                te.logger.warning(
                    "interpreter shutdown while scheduling concurrent tools; skipping %d unsubmitted tool(s)",
                    len(skipped),
                )
                for skipped_i in skipped:
                    ref = batch.parsed_calls[skipped_i].ref(batch.effective_task_id)
                    if batch.results[skipped_i] is None:
                        result = f"Error executing tool '{ref.name}': Python interpreter is shutting down; tool was not started"
                        batch.results[skipped_i] = te._ToolOutcome(ref, result, 0.0, True, False)
                break
            if submit_error.accepted in {"yes", "unknown"}:
                # The queue handoff is accepted or ambiguous but has no Future
                # to wait on. Keep the exact child under custody, synthesize only
                # this slot, and let the caller detach the daemon pool.
                batch.submission_abandoned = True
                ref = batch.parsed_calls[i].ref(batch.effective_task_id)
                batch.results[i] = te._ToolOutcome(
                    ref,
                    f"Error executing tool '{ref.name}': worker submission remained unknown",
                    0.0,
                    True,
                    False,
                )
                for skipped_i in runnable[submit_index + 1:]:
                    skipped_ref = batch.parsed_calls[skipped_i].ref(batch.effective_task_id)
                    if batch.results[skipped_i] is None:
                        batch.results[skipped_i] = te._ToolOutcome(
                            skipped_ref,
                            f"Error executing tool '{skipped_ref.name}': worker was not started after an unknown submission",
                            0.0,
                            True,
                            False,
                        )
                break
            raise
        futures.append(future)
        future_to_index[future] = i
    return futures, future_to_index


__all__ = ["_SEQUENTIAL_INTERRUPT_POLL_SECONDS", "poll_sequential_future", "submit_concurrent_workers"]
