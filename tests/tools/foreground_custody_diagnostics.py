"""Bounded, observer-only diagnostics for native foreground custody tests.

The helpers in this module never decide custody state.  They record the exact
retained transaction and Job observations, plus child phases written under the
test's temporary directory, so a failed assertion carries its first missing
boundary in the test output.
"""

from __future__ import annotations

import base64
import ctypes
import hashlib
import json
from pathlib import Path
import shlex
import sys
import tempfile
from typing import Any


_MAX_PHASE_BYTES = 4096
_MAX_ATTACHED_BYTES = 8192
_MAX_JOB_QUERY_BYTES = 1024 * 1024


def encoded_python_source(source: str) -> str:
    """Return an argv-safe Python wrapper preserving generated source bytes."""
    payload = base64.b64encode(source.encode("utf-8")).decode("ascii")
    return (
        "import base64;exec(compile(base64.b64decode("
        + repr(payload)
        + ",validate=True).decode('utf-8'),'<hermes-custody-generated>','exec'))"
    )


def shell_python_command(source: str) -> str:
    """Build the existing Bash command with an encoded Python source payload."""
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(encoded_python_source(source))}"



def child_phase_prefix(path: Path) -> str:
    """Return a bounded source prefix for a real child-phase JSONL witness.

    The generated code writes only phase, pid, and bounded error/status fields
    into the caller's pytest temporary directory.  A failed write cannot alter
    the child result because the observer is deliberately best effort.
    """
    path_literal = str(path)
    return (
        "import json,os\n"
        f"_hermes_custody_diag_path={path_literal!r}\n"
        "def _hermes_custody_phase(name, **fields):\n"
        "    try:\n"
        "        from pathlib import Path\n"
        "        target=Path(_hermes_custody_diag_path)\n"
        "        if target.exists() and target.stat().st_size >= 4096:\n"
        "            return\n"
        "        record={'phase':name,'pid':os.getpid(),**fields}\n"
        "        with target.open('a',encoding='utf-8') as stream:\n"
        "            if target.stat().st_size < 4096:\n"
        "                stream.write(json.dumps(record,sort_keys=True)[:480]+'\\n')\n"
        "    except BaseException as error:\n"
        "        try:\n"
        "            failure={'phase':name,'error_type':type(error).__name__,"
        "'error_message':str(error)[:256]}\n"
        "            os.write(2, ('HERMES_CUSTODY_PHASE_WRITE_ERROR '+"
        "json.dumps(failure,sort_keys=True)[:480]+'\\n').encode('utf-8','replace'))\n"
        "        except OSError:\n"
        "            pass\n"
        "import sys\n"
        "_hermes_custody_default_excepthook=sys.excepthook\n"
        "def _hermes_custody_excepthook(exc_type, exc, traceback):\n"
        "    _hermes_custody_phase('child_exception', "
        "error_type=exc_type.__name__, error_message=str(exc)[:256])\n"
        "    _hermes_custody_default_excepthook(exc_type, exc, traceback)\n"
        "sys.excepthook=_hermes_custody_excepthook\n"
    )


def stdin_child_code(path: Path, output: str) -> str:
    """Build the unchanged one-child stdin control with observer phases."""
    return (
        child_phase_prefix(path)
        + f"import sys; _hermes_custody_phase('child_start'); print({output!r}, flush=True); "
        + "sys.stdin.read(); _hermes_custody_phase('child_stdin_eof'); "
        + "_hermes_custody_phase('child_exit');"
    )


def blocking_child_code(path: Path, output: str) -> str:
    """Build the unchanged blocking reader child with observer phases."""
    return (
        child_phase_prefix(path)
        + "import sys,threading; _hermes_custody_phase('descendant_start'); "
        + f"sys.stdout.write({output!r}+'\\n'); sys.stdout.flush(); "
        + "_hermes_custody_phase('descendant_ready'); threading.Event().wait()"
    )


def socket_descendant_code(
    path: Path,
    host: str,
    port: int,
    ready_source: str,
    tail_source: str,
) -> str:
    """Build the existing socket-held descendant with phase-only instrumentation."""
    return (
        child_phase_prefix(path)
        + "import socket,sys; _hermes_custody_phase('descendant_start'); "
        + f"peer=socket.create_connection(({host!r}, {port})); "
        + "_hermes_custody_phase('descendant_connected'); "
        + ready_source
        + " peer.recv(7); _hermes_custody_phase('descendant_release_received'); "
        + tail_source
        + " _hermes_custody_phase('descendant_before_close'); peer.close(); "
        + "_hermes_custody_phase('descendant_exit')"
    )


def inherited_descendant_parent_code(path: Path, descendant: str, output: str) -> str:
    """Build the existing inherited-stdio parent with its original Popen contract."""
    return (
        child_phase_prefix(path)
        + "import subprocess,sys; _hermes_custody_phase('parent_before_popen'); "
        + f"_hermes_custody_child_pid=subprocess.Popen([sys.executable, '-c', {descendant!r}], "
        + "stdin=sys.stdin.buffer, stdout=sys.stdout.buffer, stderr=sys.stdout.buffer, close_fds=False).pid; "
        + "_hermes_custody_phase('parent_after_popen', child_pid=_hermes_custody_child_pid); "
        + f"print({output!r}, flush=True)"
    )


def _parent_write_probe(directory: str | None) -> dict[str, Any]:
    """Observe the test parent's temporary-directory access without changing child inputs."""
    try:
        with tempfile.TemporaryFile(dir=directory) as stream:
            stream.write(b"custody-observer")
            stream.seek(0)
            readable = stream.read() == b"custody-observer"
        return {"parent_write_readback": readable}
    except (OSError, TypeError, ValueError) as exc:
        return {"parent_write_readback": False, "error_type": type(exc).__name__,
                "error_message": str(exc)[:256]}


def _emit_execute_observation(phase: str, observation: dict[str, Any]) -> None:
    """Keep observer output on the pytest stream, outside the real child reader."""
    try:
        print("HERMES_CUSTODY_EXECUTE " + json.dumps({"phase": phase, **observation},
                                                   sort_keys=True), flush=True)
    except (OSError, TypeError, ValueError):
        # A broken harness output stream must not replace the native test outcome.
        return


def execute_with_diagnostics(environment: Any, command: str, **kwargs: Any):
    """Return the actual environment result unchanged, exposing the missing child boundary.

    Used only by the five fixed synthetic native controls. The command and bounded result
    fields contain their test scripts, never environment snapshots or credentials. This adds
    no launch, retry, reader, wait, release, or decision to the actual custody transaction.
    """
    observation = {
        "command_sha256": hashlib.sha256(command.encode()).hexdigest(),
        "command_chars": len(command), "command": command[:32768],
        "python_executable": sys.executable, "cwd": str(kwargs.get("cwd", ""))[:1024],
        **_parent_write_probe(kwargs.get("cwd")),
    }
    _emit_execute_observation("before_execute", observation)
    try:
        result = environment.execute(command, **kwargs)
    except BaseException as exc:
        _emit_execute_observation("execute_exception", {
            "command_sha256": observation["command_sha256"],
            "error_type": type(exc).__name__, "error_message": str(exc)[:1024],
        })
        raise
    fields = {}
    if isinstance(result, dict):
        for key in ("output", "error", "stdin_error", "returncode", "exit_code",
                    "yielded_session_id", "timed_out", "timeout"):
            value = result.get(key)
            if value is None or isinstance(value, (bool, int, float)):
                fields[key] = value
            elif isinstance(value, str):
                fields[key] = value[:8192]
    _emit_execute_observation("after_execute", {
        "command_sha256": observation["command_sha256"], "result_fields": fields,
    })
    return result


def _safe_call(function, *, operation: str) -> dict[str, Any]:
    try:
        return {"operation": operation, "result": function()}
    except BaseException as exc:  # diagnostics must not replace the test's assertion
        return {
            "operation": operation,
            "result": None,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:256],
        }


def _safe_attr(obj: Any, name: str, errors: dict[str, Any]) -> Any:
    try:
        return getattr(obj, name)
    except BaseException as exc:
        errors[name] = {
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:256],
        }
        return None


def _job_process_ids(job: Any) -> dict[str, Any]:
    """Query process IDs from this exact retained Job handle only."""
    try:
        handle = getattr(job, "_handle", None)
        if handle is None:
            return {"result": None, "error_type": "job_handle_unavailable"}
        from hermes_cli.local_runtime import processes as native_processes
        from ctypes import wintypes

        header_type = native_processes._BasicProcessIdListHeader
        header_size = ctypes.sizeof(header_type)
        pid_size = ctypes.sizeof(ctypes.c_size_t)
        buffer_size = header_size + pid_size
        query = job._api.QueryInformationJobObject
        for _ in range(8):
            if buffer_size > _MAX_JOB_QUERY_BYTES:
                return {"result": None, "error_type": "job_pid_query_too_large"}
            buffer = (ctypes.c_ubyte * buffer_size)()
            returned = wintypes.DWORD()
            ok = query(
                handle,
                3,
                ctypes.byref(buffer),
                buffer_size,
                ctypes.byref(returned),
            )
            if not ok:
                required = int(returned.value)
                if required <= buffer_size or required > _MAX_JOB_QUERY_BYTES:
                    return {
                        "result": None,
                        "error_type": "job_pid_query_failed",
                        "required_bytes": required,
                    }
                buffer_size = required
                continue
            if int(returned.value) > buffer_size:
                return {"result": None, "error_type": "job_pid_query_truncated"}
            header = header_type.from_buffer(buffer)
            assigned = int(header.NumberOfAssignedProcesses)
            count = int(header.NumberOfProcessIdsInList)
            capacity = (buffer_size - header_size) // pid_size
            if count < 0 or count > assigned or count > capacity:
                return {"result": None, "error_type": "job_pid_query_invalid_count"}
            values = (ctypes.c_size_t * count).from_buffer(buffer, header_size)
            return {
                "result": {
                    "assigned_count": assigned,
                    "member_count": count,
                    "pids": [int(value) for value in values],
                },
            }
        return {"result": None, "error_type": "job_pid_query_retry_exhausted"}
    except BaseException as exc:
        return {
            "result": None,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:256],
        }


def transaction_snapshot(transaction: Any) -> dict[str, Any]:
    """Read exact transaction/process/Job observations without changing custody."""
    attribute_errors: dict[str, Any] = {}
    process = _safe_attr(transaction, "process", attribute_errors)
    job = _safe_attr(transaction, "job", attribute_errors)
    process_ref = _safe_attr(transaction, "process_ref", attribute_errors)
    job_handle = _safe_attr(job, "_handle", attribute_errors) if job is not None else None
    snapshot = {
        "transaction_object_id": id(transaction),
        "process_object_id": id(process) if process is not None else None,
        "process_pid": _safe_attr(process, "pid", attribute_errors) if process is not None else None,
        "process_returncode": _safe_attr(process, "returncode", attribute_errors) if process is not None else None,
        "process_poll": (
            _safe_call(
                lambda: _safe_attr(process, "poll", attribute_errors)(),
                operation="process.poll",
            )
            if process is not None
            else None
        ),
        "process_host_start_time": _safe_attr(transaction, "host_start_time", attribute_errors),
        "job_object_id": id(job) if job is not None else None,
        "job_handle_object_id": id(job_handle) if job_handle is not None else None,
        "assigned": _safe_attr(transaction, "assigned", attribute_errors),
        "resumed": _safe_attr(transaction, "resumed", attribute_errors),
        "state": _safe_attr(transaction, "state", attribute_errors),
        "scope_observation": _safe_attr(transaction, "scope_observation", attribute_errors),
        "native_empty_observed": _safe_attr(transaction, "native_empty_observed", attribute_errors),
        "process_handle_closed": _safe_attr(transaction, "process_handle_closed", attribute_errors),
        "job_handle_closed": _safe_attr(transaction, "job_handle_closed", attribute_errors),
    }
    if attribute_errors:
        snapshot["attribute_errors"] = attribute_errors
    if job is None:
        snapshot["job_queries"] = []
        snapshot["job_process_ids"] = {"result": None, "error_type": "job_unavailable"}
        return snapshot
    snapshot["job_queries"] = [
        _safe_call(
            lambda: _safe_attr(job, "active_processes", attribute_errors)(),
            operation="job.active_processes",
        ),
        _safe_call(
            lambda: job.contains_process_handle(process_ref),
            operation="job.contains_process_handle",
        ),
        _safe_call(
            lambda: job.contains_pid(getattr(transaction, "pid", None)),
            operation="job.contains_pid(root)",
        ),
    ]
    snapshot["job_process_ids"] = _job_process_ids(job)
    return snapshot


def _phase_files(directory: Path) -> list[dict[str, Any]]:
    attached: list[dict[str, Any]] = []
    for path in sorted(directory.glob("hermes-custody-*.jsonl")):
        try:
            data = path.read_bytes()[:_MAX_PHASE_BYTES]
            attached.append({
                "path": path.name,
                "truncated": path.stat().st_size > _MAX_PHASE_BYTES,
                "content": data.decode("utf-8", "replace")[:_MAX_PHASE_BYTES],
            })
        except BaseException as exc:
            attached.append({
                "path": path.name,
                "read_error_type": type(exc).__name__,
                "read_error_message": str(exc)[:256],
            })
        if sum(len(json.dumps(item, sort_keys=True)) for item in attached) >= _MAX_ATTACHED_BYTES:
            break
    return attached


def failure_details(directory: Path, transaction: Any = None, **extra: Any) -> str:
    """Attach bounded observer output to an existing assertion message."""
    payload: dict[str, Any] = {
        "custody_diagnostics": {
            "directory": directory.name,
            "phase_files": _phase_files(directory),
        }
    }
    if transaction is not None:
        payload["custody_diagnostics"]["transaction"] = transaction_snapshot(transaction)
    if extra:
        payload["custody_diagnostics"]["context"] = extra
    rendered = json.dumps(payload, sort_keys=True, default=str)
    return rendered[:_MAX_ATTACHED_BYTES]



def emit_reader_observation(state: dict[str, Any]) -> None:
    """Print bounded actual reader state after the test controller has joined."""
    evidence = state.get("evidence") or {}
    reader = state.get("reader_thread")
    observed = {
        "phase": state.get("phase"), "connection_state": state.get("connection_state"),
        "evidence": {name: evidence.get(name) for name in (
            "status", "eof_kind", "reader_state", "deadline_expired", "reader_pending",
            "true_eof", "natural_exit", "stdin_writer_joined", "stdout_closed",
        )},
        "reader_alive": reader.is_alive() if reader is not None else None,
        "process_returncode": getattr(state.get("process"), "returncode", None),
    }
    print("HERMES_CUSTODY_READER " + json.dumps(observed, sort_keys=True, default=str)[:8192], flush=True)
