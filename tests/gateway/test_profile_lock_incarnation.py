"""Physical profile-lock incarnations are backed by the real OS lock and descriptor."""

import json
import os
from pathlib import Path
import subprocess
import sys

def test_reservation_nonce_is_published_and_reacquire_is_a_new_incarnation(tmp_path):
    from gateway.runtime_ownership import ProfileOwnership

    home = tmp_path / "profile"
    owner = ProfileOwnership()
    try:
        owner.reserve([home])
        first = owner.owner_epoch(home)
        record = json.loads((home / "gateway.lock").read_text(encoding="utf-8"))
        assert isinstance(first, str) and record["owner_epoch"] == first
        assert owner.reservation_is_current(home, first)
        assert owner.owner_epoch(home) == first

        assert owner.release_outcome(home).value == "released"
        owner.reserve([home])
        second = owner.owner_epoch(home)
        assert isinstance(second, str) and second != first
        assert not owner.reservation_is_current(home, first)
    finally:
        assert not owner.close().failed


def test_a_real_process_cannot_reserve_a_held_profile(tmp_path):
    from gateway.runtime_ownership import ProfileOwnership

    home = tmp_path / "profile"
    owner = ProfileOwnership()
    owner.reserve([home])
    code = """
import sys
from pathlib import Path
from gateway.runtime_ownership import OwnershipConflict, ProfileOwnership
owner = ProfileOwnership()
try:
    owner.reserve([Path(sys.argv[1])])
except OwnershipConflict:
    print('CONFLICT', flush=True)
else:
    print('ACQUIRED', flush=True)
    owner.close()
"""
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(root), env.get("PYTHONPATH", "")]))
    try:
        result = subprocess.run(
            [sys.executable, "-c", code, str(home)],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "CONFLICT"
    finally:
        assert owner.release_outcome(home).value == "released"
