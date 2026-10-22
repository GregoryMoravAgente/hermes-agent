"""Windows stdout drain: bounded when a grandchild keeps the pipe's write handle open.

``select()`` cannot poll pipe fds on Windows, so the drain used a blocking
``os.read`` loop that waited for true EOF — a backgrounded grandchild that
inherited the write end kept the terminal tool hung for its whole lifetime.
The ``PeekNamedPipe`` drain mirrors the POSIX ``select`` loop's post-exit bound.
"""

import codecs
import os
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

from tools.environments.base_output import _BoundedOutputCollector, _drain_fd_windows
from tests.waiting import wait_until


@pytest.mark.platforms("windows")
def test_drain_fd_windows_returns_promptly_when_writer_remains_open():
    """Data already in the pipe is captured; the drain stops shortly after the
    process reports exit even though the write end is still open."""
    r, w = os.pipe()
    try:
        os.write(w, b"hello from child process\n")
        proc = MagicMock()
        proc.poll.return_value = 0  # bash exited; ``w`` (the grandchild's copy) is still open

        output = _BoundedOutputCollector(1000)
        decoder = codecs.getincrementaldecoder("utf-8")("replace")

        t0 = time.monotonic()
        _drain_fd_windows(proc, r, output, decoder)
        elapsed = time.monotonic() - t0

        assert "hello from child process" in output.render()
        assert elapsed < 2.0, f"drain hung for {elapsed:.2f}s instead of bounding after exit"
    finally:
        os.close(r)
        os.close(w)


@pytest.mark.platforms("windows")
def test_managed_reader_waits_for_real_writer_then_reports_native_eof():
    """A managed reader keeps its lease and captures a descendant's late UTF-8 tail."""
    r, w = os.pipe()
    os.set_inheritable(w, True)
    late_code = (
        "import sys; out=sys.stdout.buffer; out.write(b'late-tail-\\xe2'); "
        "out.write(b'\\x82\\xac\\n'); out.flush()"
    )
    child_code = (
        "import subprocess,sys; "
        f"subprocess.Popen([sys.executable, '-c', {late_code!r}], "
        "stdout=sys.stdout.buffer, stderr=sys.stdout.buffer); "
        "print('native-reader', flush=True)"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", child_code],
        stdout=w,
        stderr=subprocess.STDOUT,
        close_fds=False,
    )
    output = _BoundedOutputCollector(1000)
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    evidence = {"reader_state": {"done": False, "status": None}}
    reader = threading.Thread(
        target=_drain_fd_windows,
        args=(proc, r, output, decoder),
        kwargs={"evidence": evidence, "retain_until_eof": True},
        daemon=True,
    )
    reader.start()
    try:
        proc.wait(timeout=10)
        reader.join(timeout=2)
        assert reader.is_alive(), "managed reader must retain the live pipe lease"
        assert evidence["reader_state"].get("status") != "post_exit_idle"
        os.close(w)
        w = -1
        reader.join(timeout=10)
        assert reader.is_alive() is False
        assert evidence["status"] == "true_eof"
        assert evidence["reader_state"]["eof_kind"] == "win32_broken_pipe"
        assert "native-reader" in output.render()
        assert "late-tail-€" in output.render()
    finally:
        if w >= 0:
            os.close(w)
        os.close(r)
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


@pytest.mark.platforms("windows")
def test_local_environment_returns_while_background_grandchild_holds_pipe(tmp_path, monkeypatch):
    """End-to-end on Git Bash: ``execute()`` returns promptly with the marker while a
    backgrounded grandchild still holds the stdout pipe (issues #105865 / #67362).

    The module-local ``Popen`` seam uses the existing native ``_WindowsJob`` only for
    this test.  It assigns the exact returned Popen handle before resuming it, while
    keeping ``LocalEnvironment``'s unmanaged reader path unchanged.  The prompt result
    is followed by an exact Job observation: the inherited 30-second child must drain
    naturally before either retained native handle is closed.
    """
    from hermes_cli.local_runtime.processes import _WindowsJob
    from hermes_cli.update_lock import CREATE_SUSPENDED, resume_suspended_child
    import tools.environments.local as local_environment_module
    from tools.environments.local import LocalEnvironment

    env = LocalEnvironment(cwd=str(tmp_path))
    launch = {}
    cleanup_errors = []
    real_subprocess = local_environment_module.subprocess

    class _OwnedSubprocessProxy:
        """Forward LocalEnvironment's module-local API without mutating subprocess."""

        def __getattr__(self, name):
            return getattr(real_subprocess, name)

        def Popen(self, *args, **kwargs):
            if launch:
                raise AssertionError("LocalEnvironment created more than one owned Popen")
            job = _WindowsJob()
            launch["job"] = job
            kwargs["creationflags"] = kwargs.get("creationflags", 0) | CREATE_SUSPENDED
            process = real_subprocess.Popen(*args, **kwargs)
            # Retain the exact Popen before either assignment or resume can fail.
            launch["process"] = process
            job.assign(process)
            resume_suspended_child(process)
            launch["resumed"] = True
            return process

    def _record_cleanup_error(stage, error):
        cleanup_errors.append(f"{stage}: {type(error).__name__}: {error}")

    def _cleanup_owned_launch(*, failure):
        """Attempt every owned cleanup step without masking the test exception."""
        process = launch.get("process")
        job = launch.get("job")
        if failure and process is not None:
            try:
                if process.poll() is None:
                    process.kill()
            except BaseException as exc:  # health: cleanup must continue independently
                _record_cleanup_error("process_kill", exc)
            try:
                process.wait(timeout=10)
            except BaseException as exc:  # health: cleanup must continue independently
                _record_cleanup_error("process_wait", exc)
        if process is not None:
            try:
                process_handle = getattr(process, "_handle", None)
                if process_handle is None:
                    raise AssertionError("owned Popen process handle was unavailable")
                if not process_handle.closed:
                    process_handle.Close()
                if not process_handle.closed:
                    raise AssertionError("owned Popen process handle remained open")
            except BaseException as exc:  # health: still close the Job and preserve the primary error
                _record_cleanup_error("process_handle_close", exc)
        if job is not None:
            try:
                if getattr(job, "_handle", None) is not None:
                    job.close()
                if getattr(job, "_handle", None) is not None:
                    raise AssertionError("owned Windows Job handle remained open")
            except BaseException as exc:  # health: all cleanup failures become notes or a test failure
                _record_cleanup_error("job_handle_close", exc)

    native_complete = False
    try:
        # Patch only tools.environments.local.subprocess.  The real stdlib module is
        # retained by this proxy, so no global Popen monkeypatch can recurse.
        monkeypatch.setattr(
            local_environment_module, "subprocess", _OwnedSubprocessProxy())
        marker = "windows_drain_e2e_marker"
        cmd = f'python -c "import time; time.sleep(30)" & echo {marker}'
        t0 = time.monotonic()
        result = env.execute(cmd, timeout=15)
        elapsed = time.monotonic() - t0

        assert elapsed < 10.0, f"LocalEnvironment.execute hung for {elapsed:.2f}s on Windows"
        assert result["returncode"] == 0
        assert marker in result["output"]

        process = launch.get("process")
        job = launch.get("job")
        assert process is not None, "the exact LocalEnvironment Popen was not retained"
        assert job is not None, "the test-owned native Job was not retained"
        wait_until(
            lambda: job.empty() is False,
            timeout=5,
            what="the owned Job to observe the inherited child",
        )
        wait_until(
            lambda: job.empty() is True,
            timeout=45,
            what="the owned Job to drain after the natural child exit",
        )
        assert process.poll() is not None
        process.wait(timeout=10)
        assert process.returncode == 0
        assert job.empty() is True
        native_complete = True
    finally:
        primary_error = sys.exc_info()[1]
        _cleanup_owned_launch(failure=not native_complete)
        try:
            env.cleanup()
        except BaseException as exc:  # health: preserve the primary error and report cleanup-only failure
            _record_cleanup_error("environment_cleanup", exc)
        if cleanup_errors:
            note = "R42c owned-launch cleanup: " + "; ".join(cleanup_errors)
            if primary_error is not None:
                primary_error.add_note(note)
            else:
                raise AssertionError(note)
