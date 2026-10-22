"""Real Windows foreground-child controllers shared by native custody tests."""

import contextlib
import threading
from types import SimpleNamespace

from tests.tools.foreground_custody_diagnostics import failure_details
from tests.waiting import wait_until


def _controller_transaction_snapshot(transaction):
    """Expose bounded child/Job identity when a controller phase fails."""
    process = getattr(transaction, "process", None)
    job = getattr(transaction, "job", None)
    job_handle = getattr(job, "_handle", None) if job is not None else None
    return {
        "process_object_id": id(process) if process is not None else None,
        "process_pid": getattr(process, "pid", None),
        "process_returncode": getattr(process, "returncode", None),
        "process_host_start_time": getattr(transaction, "host_start_time", None),
        "job_object_id": id(job) if job is not None else None,
        "job_handle_object_id": id(job_handle) if job_handle is not None else None,
        "assigned": getattr(transaction, "assigned", None),
        "resumed": getattr(transaction, "resumed", None),
        "process_handle_closed": getattr(transaction, "process_handle_closed", None),
        "job_handle_closed": getattr(transaction, "job_handle_closed", None),
    }



def _start_late_writer_controller(
    handler, listener, *, invalidate_reader: bool = False, diagnostic_dir=None,
):
    """Control one real inherited writer without sleeping or fabricating reader evidence."""
    errors = []
    released = threading.Event()
    state = {}

    def _control():
        connection = None
        transaction = None
        state["connection_state"] = "not_connected"
        try:
            state["phase"] = "transaction_wait"
            transaction = wait_until(
                lambda: getattr(handler, "_launch_transaction", None),
                timeout=10,
                what="the retained Windows launch transaction",
            )
            state["transaction"] = transaction
            state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
            state["phase"] = "listener_accept"
            connection, _ = listener.accept()
            state["connection_state"] = "connected"
            state["phase"] = "direct_exit_wait"
            wait_until(
                lambda: transaction.process is not None and transaction.process.poll() is not None,
                timeout=10,
                what="the direct child to exit while its inherited writer remains",
            )
            state["direct_exited"] = True
            state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
            if invalidate_reader:
                state["phase"] = "reader_invalidate"
                stdout = transaction.process.stdout
                assert stdout is not None
                stdout.close()
            state["phase"] = "release_send"
            connection.sendall(b"release")
            released.set()
            state["phase"] = "released"
        except BaseException as exc:  # surfaced by the owning test after the real wait
            state["error"] = {
                "phase": state.get("phase", "unknown"),
                "connection_state": state.get("connection_state", "unknown"),
                "type": type(exc).__name__,
                "message": str(exc)[:512],
            }
            if transaction is not None:
                state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
                state["error"]["transaction_snapshot"] = state["transaction_snapshot"]
            if diagnostic_dir is not None:
                state["diagnostics"] = failure_details(
                    diagnostic_dir, transaction, controller_state=state,
                )
            errors.append(exc)
        finally:
            if connection is not None:
                with contextlib.suppress(OSError):
                    connection.close()
            state["connection_state"] = "closed"

    thread = threading.Thread(target=_control, daemon=True)
    thread.start()
    return thread, errors, released, state



def _start_blocking_inherited_writer_controller(handler, listener):
    """Hold one real descendant socket open until the retained Job is stopped."""
    errors = []
    connected = threading.Event()
    release = threading.Event()
    state = {}

    def _control():
        connection = None
        transaction = None
        state["connection_state"] = "not_connected"
        try:
            state["phase"] = "transaction_wait"
            transaction = wait_until(
                lambda: getattr(handler, "_launch_transaction", None),
                timeout=10,
                what="the retained Windows launch transaction",
            )
            state["transaction"] = transaction
            state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
            state["phase"] = "listener_accept"
            connection, _ = listener.accept()
            state["connection_state"] = "connected"
            state["phase"] = "release_wait"
            connected.set()
            release.wait()
        except BaseException as exc:  # surfaced by the owning test after the real wait
            state["error"] = {
                "phase": state.get("phase", "unknown"),
                "connection_state": state.get("connection_state", "unknown"),
                "type": type(exc).__name__,
                "message": str(exc)[:512],
            }
            if transaction is not None:
                state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
                state["error"]["transaction_snapshot"] = state["transaction_snapshot"]
            errors.append(exc)
        finally:
            if connection is not None:
                with contextlib.suppress(OSError):
                    connection.close()
            state["connection_state"] = "closed"

    thread = threading.Thread(target=_control, daemon=True)
    thread.start()
    return thread, errors, connected, release, state



def _wire_managed_turn_callback(agent, session_id: str, session_key: str, generation: int):
    """Install the real TurnRunner callback on a real AIAgent used by custody controls."""
    from gateway.run_turn_runner import TurnRunner
    from gateway.turn_context import TurnContext

    ctx = TurnContext(
        source=SimpleNamespace(platform=None),
        session_id=session_id,
        session_key=session_key,
        run_generation=generation,
        user_config={},
        mute_notification_reply=False,
        progress_callback=None,
        native_tool_start_callback=None,
        native_tool_complete_callback=None,
        voice_ack_callback=None,
        _native_slack_task_cards=False,
        _step_callback_sync=None,
        _status_callback_sync=None,
        _event_callback_sync=None,
        _status_adapter=None,
        process_task_id=None,
        process_baseline=None,
        voice_turn=False,
    )
    ctx._hooks_ref = SimpleNamespace(loaded_hooks=[])
    holder = SimpleNamespace(
        _ctx=ctx,
        _approval_owner=None,
        _runner=SimpleNamespace(
            _service_tier=None,
            _consume_pending_turn_sidecar_notes=lambda _key: [],
        ),
        _make_bg_review_callbacks=lambda: (lambda _message: None, lambda: None),
        _merge_turn_request_overrides=TurnRunner._merge_turn_request_overrides,
        _clarify_callback_sync=lambda *_args, **_kwargs: None,
        _notice_callback_sync=lambda *_args, **_kwargs: None,
        _attach_session_title_callback=lambda *_args, **_kwargs: None,
        combined_tool_start_callback=None,
        combined_tool_complete_callback=None,
    )
    TurnRunner._wire_turn_agent_callbacks(holder, agent, {}, None, None, None, False)
    return agent.tool_result_committed_callback
