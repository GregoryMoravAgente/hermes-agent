"""Contributor attribution compares a candidate with its declared PR base."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import audit_pr_attribution as audit


OLD_EMAIL = "old-baseline@example.invalid"
NEW_EMAIL = "new-candidate@example.invalid"


def _git(repo: Path, *args: str) -> str:
    env = os.environ.copy()
    env.update({
        "GIT_CONFIG_GLOBAL": str(repo / "missing-global-config"),
        "GIT_CONFIG_NOSYSTEM": "1",
    })
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def _commit(repo: Path, *, email: str, name: str, message: str, path: str) -> str:
    (repo / path).write_text(message + "\n", encoding="utf-8")
    _git(repo, "add", path)
    _git(
        repo,
        "-c", f"user.name={name}",
        "-c", f"user.email={email}",
        "commit", "-m", message,
    )
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture()
def synthetic_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build main -> integration -> candidate with two distinct authors."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    root = _commit(
        repo,
        email="maintainer@example.invalid",
        name="Maintainer",
        message="root",
        path="root.txt",
    )
    _git(repo, "update-ref", "refs/remotes/origin/main", root)

    _git(repo, "checkout", "-q", "-b", "integration")
    integration = _commit(
        repo,
        email=OLD_EMAIL,
        name="Old baseline",
        message="old baseline",
        path="old.txt",
    )

    _git(repo, "checkout", "-q", "-b", "candidate")
    _commit(
        repo,
        email=NEW_EMAIL,
        name="New candidate",
        message="new candidate",
        path="new.txt",
    )

    monkeypatch.setattr(audit, "REPO_ROOT", repo)
    # Keep the selected PR base available as a real branch ref, rather than a
    # test-only value passed through a mocked git call.
    _git(repo, "update-ref", "refs/heads/integration", integration)
    return repo


def test_selected_base_excludes_old_baseline_and_keeps_new_author(synthetic_candidate):
    # The legacy origin/main baseline sees both commits.  The selected PR base
    # is integration, so only the candidate's new identity is in the range.
    assert audit.new_emails() == sorted([OLD_EMAIL, NEW_EMAIL])
    assert audit.new_emails("integration") == [NEW_EMAIL]


def test_cli_base_ref_reports_only_selected_range(synthetic_candidate, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["audit_pr_attribution.py", "--base-ref", "integration"],
    )

    assert audit.main() == 1
    output = capsys.readouterr().out
    assert NEW_EMAIL in output
    assert OLD_EMAIL not in output


def test_cli_rejects_unknown_base_ref(synthetic_candidate, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["audit_pr_attribution.py", "--base-ref", "missing-base"],
    )

    assert audit.main() == 2
    assert "unknown base ref" in capsys.readouterr().err
