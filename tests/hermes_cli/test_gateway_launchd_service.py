"""Launchd command-generation contracts at the PM launcher resolver seam."""

import plistlib
from pathlib import Path

import hermes_cli.gateway as gateway_cli
from hermes_cli import _launchers
from tests.hermes_cli.test_gateway_service import _osascript_exec_argv


def test_launchd_plist_uses_published_pm_launcher(tmp_path, monkeypatch):
    """The published-launcher branch preserves both command layers (DI unit, not PM qualification)."""
    profile_dir = tmp_path / ".hermes" / "profiles" / "mybot"
    profile_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(profile_dir))
    monkeypatch.setattr(gateway_cli, "get_hermes_home", lambda: profile_dir)
    monkeypatch.setattr(gateway_cli, "get_python_path", lambda: "/usr/bin/python3")
    monkeypatch.setattr(_launchers, "resolve_store_python", lambda _root: Path("/pm/python"))

    program_args = plistlib.loads(gateway_cli.generate_launchd_plist().encode("utf-8"))["ProgramArguments"]
    exec_argv = _osascript_exec_argv(program_args)
    launcher = str(gateway_cli.PROJECT_ROOT / ".hermes" / "bin" / "hermes")
    assert exec_argv[:3] == [launcher, "--run-module", "hermes_cli.stderr_timestamp"]
    assert exec_argv[-4:] == [">>", str(profile_dir / "logs" / "gateway.log"),
                              "2>>", str(profile_dir / "logs" / "gateway.error.log")]
    program_args = exec_argv[:-4]
    separator = program_args.index("--")
    inner = program_args[separator + 1:]
    assert inner[0] == launcher
    assert inner[-5:] == ["--profile", "mybot", "gateway", "run", "--external-supervisor"]
