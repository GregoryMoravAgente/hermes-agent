"""Temporary R12 worker phase observer; never part of production receiving."""

from contextlib import contextmanager
import json
import os
import sys

_TARGETS = {
    "scheduler.py": {"run_one_job", "_run_one_job_body", "_run_no_agent_job", "_install_fire_secret_scope"},
    "scheduler_script.py": {"_run_job_script", "_run_job_script_effect", "_run_job_script_with_claim_heartbeat"},
    "runtime_custody.py": {"admit_cron_source"},
    "runtime_cron_source_registry.py": {"admit_cron_source", "__enter__", "_enter_lease", "_leave_lease"},
    "jobs.py": {"_jobs_lock", "_acquire_flock", "_release_flock", "_fire_job_lock"},
    "export_writers.py": {"_bound_writer_custody", "generator_wrapper", "call_wrapper"},
}


def _emit(frame, event, value):
    row = {"file": frame.f_code.co_filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1],
           "function": frame.f_code.co_name, "event": event, "line": frame.f_lineno}
    if event == "exception":
        row["exception_class"] = type(value[1]).__name__
    elif event == "return":
        row["result"] = "none" if value is None else "true" if value is True else "false" if value is False else "value"
    _write_row(row)


def _write_row(row):
    data = ("NATIVE_CRON_PHASE=" + json.dumps(row, sort_keys=True) + "\n").encode("utf-8")
    for _attempt in range(3):
        try:
            size = os.write(2, data)
        except InterruptedError:
            continue
        except OSError:
            return
        if size <= 0 or size == len(data):
            return
        data = data[size:]


@contextmanager
def observe_acknowledged_worker():
    previous = sys.gettrace()
    if previous is not None:
        raise RuntimeError("native cron diagnosis refuses an existing tracer")
    count = 0

    def bounded_trace(frame, event, value):
        nonlocal count
        if event not in {"call", "return", "exception"}:
            return bounded_trace
        name = frame.f_code.co_filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
        if frame.f_code.co_name not in _TARGETS.get(name, ()):
            return None
        if count < 128:
            count += 1
            _emit(frame, event, value)
        elif count == 128:
            count += 1
            _write_row({"event": "truncated", "limit": 128})
        return bounded_trace

    sys.settrace(bounded_trace)
    try:
        yield
    finally:
        sys.settrace(previous)
