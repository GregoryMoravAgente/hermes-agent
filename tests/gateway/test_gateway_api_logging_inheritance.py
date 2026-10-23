"""Exercise embedded and standalone listener logging in isolated child processes."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]

_PROBE = textwrap.dedent(
    """
    import json
    import logging
    import sys
    from pathlib import Path


    def run(mode: str, home_arg: str, profile_arg: str) -> None:
        from hermes_cli.web_server_lifecycle import build_listener_server

        home = Path(home_arg).resolve()
        profile = Path(profile_arg).resolve()
        home.mkdir(parents=True, exist_ok=True)
        profile.mkdir(parents=True, exist_ok=True)

        if mode == "embedded":
            import hermes_logging
            from hermes_constants import reset_hermes_home_override, set_hermes_home_override

            hermes_logging.setup_logging(hermes_home=home, mode="gateway", force=True)
            assert hermes_logging.enable_profile_log_routing([home, profile]) is True
            try:
                _config, _server = build_listener_server(
                    object(), "127.0.0.1", 0, auth_required=False, inherit_logging=True
                )
                token = set_hermes_home_override(str(profile))
                try:
                    logging.getLogger("gateway.r28.logging").warning(
                        "r28 embedded logging sentinel"
                    )
                    logging.getLogger("uvicorn.error").warning(
                        "r28 embedded uvicorn logging sentinel"
                    )
                finally:
                    reset_hermes_home_override(token)
                hermes_logging.flush_log_queue()
                log_path = profile / "logs" / "agent.log"
                assert log_path.is_file()
                assert "r28 embedded logging sentinel" in log_path.read_text(encoding="utf-8")
                assert "r28 embedded uvicorn logging sentinel" not in log_path.read_text(encoding="utf-8")
                resolved_profile = profile.resolve()
                routers = [
                    handler
                    for handler in hermes_logging._queued_file_handlers
                    if isinstance(handler, hermes_logging._ProfileRoutingFileHandler)
                ]
                assert any(
                    resolved_profile in router._profile_handlers
                    and router._profile_handlers[resolved_profile].stream is not None
                    for router in routers
                )
                print(json.dumps({"mode": mode, "profile_log": str(log_path)}), flush=True)
            finally:
                hermes_logging._reset_queued_handlers()
            return

        _config, _server = build_listener_server(
            object(), "127.0.0.1", 0, auth_required=False
        )
        logger = logging.getLogger("uvicorn.error")
        uvicorn_logger = logging.getLogger("uvicorn")
        logger.warning("r28 standalone logging sentinel")
        assert any(
            getattr(handler, "stream", None) is not None for handler in uvicorn_logger.handlers
        )
        print(
            json.dumps({"mode": mode, "uvicorn_error_handlers": len(uvicorn_logger.handlers)}),
            flush=True,
        )


    if __name__ == "__main__":
        run(sys.argv[1], sys.argv[2], sys.argv[3])
    """
)


def _run_probe(mode: str, tmp_path: Path) -> dict:
    home = tmp_path / f"home-{mode}"
    profile = home / "profiles" / f"r28-{mode}"
    env = {
        key: value for key, value in os.environ.items() if key in {"PATH", "LANG", "TZ", "TMPDIR"}
    }
    env.update(HOME=str(tmp_path), HERMES_HOME=str(home), PYTHONPATH=str(REPO))
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, mode, str(home), str(profile)],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if mode == "embedded":
        assert "r28 embedded uvicorn logging sentinel" in proc.stderr
    lines = [line for line in (proc.stdout + proc.stderr).splitlines() if line.startswith("{")]
    assert proc.returncode == 0 and lines, proc.stderr[-4000:]
    return json.loads(lines[-1])


def test_embedded_listener_inherits_hermes_logging_without_resetting_profile_handlers(
    tmp_path,
):
    result = _run_probe("embedded", tmp_path)
    assert result["mode"] == "embedded"


def test_standalone_listener_keeps_uvicorn_logging_configuration(tmp_path):
    result = _run_probe("standalone", tmp_path)
    assert result["mode"] == "standalone"
    assert result["uvicorn_error_handlers"] >= 1
