"""Regression tests for sudo detection and sudo password handling."""

import shlex
import sys

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
from tools import terminal_tool
from tools import terminal_tool_sudo
import tools.environments.base as base_environment
from tools.environments.local import LocalEnvironment
from tools.process_registry import ProcessRegistry


def setup_function():
    terminal_tool_sudo._reset_cached_sudo_passwords()


def teardown_function():
    terminal_tool_sudo._reset_cached_sudo_passwords()


def test_searching_for_sudo_does_not_trigger_rewrite(monkeypatch):
    monkeypatch.delenv("SUDO_PASSWORD", raising=False)
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)

    command = "rg --line-number --no-heading --with-filename 'sudo' . | head -n 20"
    transformed, sudo_stdin = terminal_tool_sudo._transform_sudo_command(command)

    assert transformed == command
    assert sudo_stdin is None








def test_actual_sudo_command_uses_configured_password(monkeypatch):
    monkeypatch.setenv("SUDO_PASSWORD", "testpass")
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)

    transformed, sudo_stdin = terminal_tool_sudo._transform_sudo_command("sudo apt install -y ripgrep")

    assert transformed == "sudo -S -p '' apt install -y ripgrep"
    assert sudo_stdin == "testpass\n"


def test_explicit_empty_sudo_password_tries_empty_without_prompt(monkeypatch):
    monkeypatch.setenv("SUDO_PASSWORD", "")
    monkeypatch.setenv("HERMES_INTERACTIVE", "1")

    def _fail_prompt(*_args, **_kwargs):
        raise AssertionError("interactive sudo prompt should not run for explicit empty password")

    monkeypatch.setattr(terminal_tool_sudo, "_prompt_for_sudo_password", _fail_prompt)

    transformed, sudo_stdin = terminal_tool_sudo._transform_sudo_command("sudo true")

    assert transformed == "sudo -S -p '' true"
    assert sudo_stdin == "\n"


def test_headless_sudo_never_runs_backend_nopasswd_probe(monkeypatch):
    """No prompt can fire without a UI, so the backend round trip must not be paid."""
    monkeypatch.delenv("SUDO_PASSWORD", raising=False)
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
    terminal_tool.set_sudo_password_callback(None)

    def _fail_probe():
        raise AssertionError("headless sudo must not probe the backend")

    assert terminal_tool_sudo._transform_sudo_command("sudo true", sudo_nopasswd_check=_fail_probe) == (
        "sudo true", None)


def test_validate_workdir_blocks_shell_metacharacters_in_windows_paths():
    assert terminal_tool._validate_workdir(r"C:\Users\Alice\project; rm -rf /")
    assert terminal_tool._validate_workdir(r"C:\Users\Alice\project$(whoami)")
    assert terminal_tool._validate_workdir("C:\\Users\\Alice\\project\nwhoami")


def test_foreground_retry_kwargs_keep_one_existing_yield_handler(monkeypatch):
    """A retry receives the existing custody closure instead of constructing a new lease."""
    handler = object()

    def _unexpected_factory(**_ctx):
        raise AssertionError("retry must not create a second foreground handler")

    monkeypatch.setattr(terminal_tool, "yield_to_background_handler", _unexpected_factory)
    assert terminal_tool._yield_kwargs(
        "echo ok", yield_handler=handler, env_type="local", cwd="/tmp",
        effective_task_id="task", task_id="task", session_key="session",
    ) == {"yield_handler": handler}


@pytest.mark.platforms("windows")
def test_foreground_result_finalizer_failure_retains_exact_native_transaction(tmp_path, monkeypatch):
    """A synchronous result failure cannot admit or close the native foreground transaction."""
    import tools.process_registry as process_registry_module

    coordinator = RuntimeCustodyCoordinator(
        process_instance="terminal-finalizer-failure-test", authority_epoch=12,
        fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(base_environment, "consume_yield", lambda _thread_id: False)

    seen = {}
    factory = terminal_tool.yield_to_background_handler

    def capture_handler(**kwargs):
        handler = factory(**kwargs)
        seen["handler"] = handler
        return handler

    monkeypatch.setattr(terminal_tool, "yield_to_background_handler", capture_handler)

    def fail_result_finalizer(**_kwargs):
        raise RuntimeError("synthetic result finalizer failure")

    monkeypatch.setattr(terminal_tool, "finalize_foreground_result", fail_result_finalizer)

    def unexpected_completed_admission(*_args, **_kwargs):
        raise AssertionError("a failed result finalizer must not admit foreground custody")

    monkeypatch.setattr(registry, "admit_completed_foreground", unexpected_completed_admission)
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    plan = terminal_tool._ExecPlan(
        config={}, env_type="local", effective_task_id="task-finalizer-failure", image="",
        cwd=str(tmp_path), host_cwd=str(tmp_path), effective_timeout=10,
    )
    child_code = "print('finalizer-boundary', flush=True)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    try:
        with pytest.raises(RuntimeError, match="synthetic result finalizer failure"):
            terminal_tool._run_foreground(
                command, env, plan, task_id="task-finalizer-failure", session_id=None,
                session_key="session-finalizer-failure", workdir=None, approval_note=None,
                clear_interrupt=False, metered=False, tool_call_id="call-finalizer",
                turn_id="turn-finalizer", run_generation=4,
            )
        handler = seen["handler"]
        transaction = handler._launch_transaction
        assert transaction is not None
        assert handler._foreground_pending is True
        assert transaction.custody.state == "unknown"
        assert transaction.custody.reason == "foreground_finalizer_failed"
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        assert handler._foreground_binding.tool_call_id == "call-finalizer"
        assert handler._foreground_binding.turn_id == "turn-finalizer"
        assert handler._foreground_binding.run_generation == 4
    finally:
        handler = seen.get("handler")
        transaction = getattr(handler, "_launch_transaction", None) if handler is not None else None
        if transaction is not None and transaction.process is not None:
            if transaction.process.poll() is None:
                transaction.process.kill()
                transaction.process.wait(timeout=10)
            if transaction.observe_empty() is True:
                transaction.close_after_observed_empty()


def test_validate_workdir_allows_unicode_filesystem_paths():
    assert terminal_tool._validate_workdir(
        "/Users/alice/Documents/Obs_Hermes_Data/项目-projects/客户拜访"
    ) is None
    assert terminal_tool._validate_workdir("/tmp/テスト") is None
    assert terminal_tool._validate_workdir("/home/jürgen/über projekt") is None


def test_validate_workdir_still_blocks_metachars_in_unicode_paths():
    # Widening to Unicode letters must not open the injection boundary:
    # shell metacharacters and control chars stay rejected even when mixed
    # with non-ASCII path segments.
    assert terminal_tool._validate_workdir("/tmp/テスト; rm -rf /")
    assert terminal_tool._validate_workdir("/tmp/项目$(whoami)")
    assert terminal_tool._validate_workdir("/tmp/über`id`")
    assert terminal_tool._validate_workdir("/tmp/テスト\nwhoami")
    assert terminal_tool._validate_workdir("/tmp/项目|cat /etc/passwd")
    assert terminal_tool._validate_workdir("/tmp/ü\x00ber")


def test_literal_sudo_executables_receive_password_stdin(monkeypatch):
    monkeypatch.setenv("SUDO_PASSWORD", "testpass")
    for prefix in ("", "VAR='a b' ", "env ", "'/usr/bin/env' -i -u UNUSED X=1 ",
                   "env --unset=UNUSED --chdir /tmp -- X=1 ", "env -uUNUSED -C/tmp "):
        for executable in ("sudo", "/usr/bin/sudo", "'/opt/my tools/sudo'", '"/usr/bin/sudo"'):
            command = prefix + executable + " -u root true"
            rewritten, stdin = terminal_tool_sudo._transform_sudo_command(command)
            assert rewritten == prefix + executable + " -S -p '' -u root true"
            assert stdin == "testpass\n"


def test_sudo_rewrite_preserves_env_operands_and_prose(monkeypatch):
    monkeypatch.setenv("SUDO_PASSWORD", "testpass")
    commands = (
        "echo '/usr/bin/sudo true'", "env echo sudo true", "env -u sudo echo ok",
        "env --chdir sudo echo ok", "env --unset=sudo echo ok", "env -- sudo=1 echo ok",
        ">/tmp/sudo echo ok", "env 2>/tmp/sudo echo ok", "env > /tmp/sudo echo ok",
        "/tmp/{a,b}/sudo true", "env X=1 -u UNUSED sudo", "env - -u UNUSED sudo", "env echo /usr/bin/sudo", "/tmp/*/sudo true",
        "env -S 'sudo true'", "env --unknown sudo true", "env --help sudo",
        "bash -c 'sudo true'", "echo ok # prose; /usr/bin/sudo true",
        '"/usr/bin/sudo', "env -u sudo", "env X=sudo", '"X=1" /usr/bin/sudo true',
    )
    for command in commands:
        assert terminal_tool_sudo._transform_sudo_command(command) == (command, None)


def test_count_real_sudo_invocations_ignores_mentions(monkeypatch):
    assert terminal_tool_sudo._count_real_sudo_invocations("grep sudo README.md") == 0
    assert terminal_tool_sudo._count_real_sudo_invocations("sudo a; sudo b") == 2
