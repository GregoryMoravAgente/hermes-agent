"""Portable and real-Bash consumers for generated Python source files."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys

from tests.tools.foreground_custody_diagnostics import shell_python_command
from tools.environments.local import LocalEnvironment


def _run_generated_source(source: str, home) -> subprocess.CompletedProcess[str]:
    environment = dict(
        os.environ,
        HOME=str(home),
        HERMES_HOME=str(home / ".hermes"),
        PYTHONIOENCODING="utf-8",
    )
    return subprocess.run(
        shlex.split(shell_python_command(source, temp_dir=home)),
        capture_output=True,
        cwd=home,
        env=environment,
        text=True,
        encoding="utf-8",
        check=False,
    )


def test_generated_source_preserves_quotes_newlines_unicode_and_home(tmp_path):
    source = (
        "import os\n"
        "print(os.environ['HOME'])\n"
        "print(\"quotes ' \\\" and newline\\nשלום🙂\")\n"
    )

    result = _run_generated_source(source, tmp_path)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == f"{tmp_path}\nquotes ' \" and newline\nשלום🙂\n"


def test_generated_source_preserves_child_exit_code_and_output(tmp_path):
    source = "print('before-exit\\nline-€', flush=True); raise SystemExit(23)"

    result = _run_generated_source(source, tmp_path)

    assert result.returncode == 23
    assert result.stdout == "before-exit\nline-€\n"
    assert result.stderr == ""


def _run_generated_source_through_bash(source: str, home):
    environment = LocalEnvironment(
        cwd=str(home),
        env={
            "HOME": str(home),
            "HERMES_HOME": str(home / ".hermes"),
            "PYTHONIOENCODING": "utf-8",
        },
    )
    try:
        return environment.execute(
            shell_python_command(source, temp_dir=home),
            cwd=str(home),
        )
    finally:
        environment.cleanup()


def test_generated_source_preserves_quotes_newlines_unicode_and_home_through_bash(tmp_path):
    source = (
        "import os\n"
        "print(os.environ['HOME'])\n"
        "print(\"quotes ' \\\" and newline\\nשלום🙂\")\n"
    )

    result = _run_generated_source_through_bash(source, tmp_path)

    assert result["returncode"] == 0
    assert result["output"] == f"{tmp_path}\nquotes ' \" and newline\nשלום🙂\n"


def test_generated_source_preserves_child_exit_code_and_output_through_bash(tmp_path):
    source = "print('before-exit\\nline-€', flush=True); raise SystemExit(23)"

    result = _run_generated_source_through_bash(source, tmp_path)

    assert result["returncode"] == 23
    assert result["output"] == "before-exit\nline-€\n"
