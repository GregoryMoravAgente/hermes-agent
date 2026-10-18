"""Native Windows proof for the real spawn_server Job-object owner."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from hermes_cli.local_runtime.processes import spawn_server
from tests.waiting import wait_until


pytestmark = pytest.mark.platforms("windows")


_DRIVER = r"""
import subprocess
import sys
from pathlib import Path

mode, started, release = sys.argv[1:]
payload = (
    "from pathlib import Path\n"
    f"started = Path({started!r})\n"
    f"release = Path({release!r})\n"
    "started.write_text('started', encoding='ascii')\n"
    "while not release.exists():\n"
    "    pass\n"
)
if mode == 'reparented':
    launcher = (
        "import subprocess, sys\n"
        f"subprocess.Popen([sys.executable, '-c', {payload!r}], "
        "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
        "stderr=subprocess.DEVNULL, close_fds=False)\n"
    )
    subprocess.Popen(
        [sys.executable, '-c', launcher], stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=False,
    )
else:
    subprocess.Popen(
        [sys.executable, '-c', payload], stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=False,
    )
"""


@pytest.mark.parametrize("mode", ["held", "reparented"])
def test_spawn_server_retains_real_descendant_until_release(tmp_path: Path, mode: str):
    """A running or reparented writer remains a real Job member until released."""
    started = tmp_path / "started"
    release = tmp_path / "release"
    proc, owner = spawn_server(
        [sys.executable, "-c", _DRIVER, mode, str(started), str(release)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert owner is not None
    try:
        assert proc.wait(timeout=30) == 0
        wait_until(started.exists, timeout=30, what=f"{mode} writer start")
        wait_until(lambda: owner.empty() is False, timeout=10, what="nonempty Job")
        assert owner.active_processes() is not None
        assert owner.active_processes() >= 1

        release.write_text("release", encoding="ascii")
        wait_until(lambda: owner.empty() is True, timeout=30, what="Job drain")
        assert owner.active_processes() == 0
    finally:
        release.touch()
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        owner.close()
