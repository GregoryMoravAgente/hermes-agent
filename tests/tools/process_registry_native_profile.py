"""Real profile startup and Popen options for native custody controls."""

import os
import subprocess


def prepare_native_profile(tmp_path, monkeypatch):
    """Match tool startup before timing native completion, without bypassing hooks."""
    from hermes_cli.plugins import discover_plugins, get_plugin_manager

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    discover_plugins()
    assert get_plugin_manager()._discovered, "real plugin discovery did not complete"


def _popen_kwargs(tmp_path):
    return {
        "text": True,
        "env": dict(os.environ),
        "cwd": str(tmp_path),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "encoding": "utf-8",
        "errors": "replace",
    }
