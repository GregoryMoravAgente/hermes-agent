"""Credential-safe environment for real compute-host test children."""

from __future__ import annotations

from pathlib import Path

from tools.environments.local import hermes_subprocess_env


def synthetic_host_env(
    hermes_home: str | Path, repo_root: str | Path
) -> dict[str, str]:
    """Build a real-child environment without inheriting provider credentials.

    ``HostSupervisor`` treats an explicit environment as authoritative. Starting
    from the shared scrubber keeps Windows' native OS variables (including
    ``SystemRoot``/``WINDIR``) while the two overrides keep every fixture bound to
    its temporary home and checkout.
    """
    env = hermes_subprocess_env(inherit_credentials=False)
    env["HERMES_HOME"] = str(hermes_home)
    env["PYTHONPATH"] = str(repo_root)
    return env
