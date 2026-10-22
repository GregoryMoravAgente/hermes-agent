"""Real children started by the production spawn sites observe the secret scrub."""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.tools._child_env_fixtures import child_env

_TIER1 = ("TELEGRAM_BOT_TOKEN", "GATEWAY_RELAY_SECRET")
_PROVIDER = "OPENAI_API_KEY"


def _plant(monkeypatch):
    for name in (*_TIER1, _PROVIDER):
        monkeypatch.setenv(name, f"fake-{name.lower()}")


def _probe_script(path: Path, out: Path, names) -> Path:
    path.write_text(
        f"#!{sys.executable}\nimport json, os\n"
        f"open({str(out)!r}, 'w').write(json.dumps({{n: os.environ.get(n) for n in {list(names)!r}}}))\n",
        encoding="utf-8")
    path.chmod(0o755)
    return path


def _compute_host_seen(child_env, monkeypatch, names):
    from tui_gateway.host_supervisor import HostSupervisor

    # HostSupervisor authenticates the public hello with the private startup record. Keep this
    # lightweight environment probe compatible with that source-owned handshake instead of
    # weakening the production gate for a test stand-in.
    hello = (
        "import json, os, sys, uuid\n"
        "from tui_gateway.windows_producer_identity import build_producer_startup_record\n"
        "raw_fd = os.environ.pop('HERMES_COMPUTE_HOST_PRODUCER_COMPLETION_FD', '')\n"
        "if not raw_fd:\n"
        "    raise SystemExit('missing producer startup fd')\n"
        "if os.name == 'nt':\n"
        "    import msvcrt\n"
        "    fd = msvcrt.open_osfhandle(int(raw_fd), os.O_WRONLY | os.O_BINARY)\n"
        "else:\n"
        "    fd = int(raw_fd)\n"
        "boot_id = uuid.uuid4().hex\n"
        "record = (json.dumps(build_producer_startup_record(pid=os.getpid(), boot_id=boot_id), "
        "separators=(',', ':')) + '\\n').encode()\n"
        "os.write(fd, record)\n"
        "os.close(fd)\n"
        f"print(json.dumps({{'type': 'hello', 'host_pid': os.getpid(), 'boot_id': boot_id, "
        f"'seen': {{n: os.environ.get(n) for n in {names!r}}}}}), flush=True)\n"
        "sys.stdin.readline()\n"
    )
    sup = HostSupervisor(registry_path=child_env / "host.json", argv=[sys.executable, "-c", hello],
                         cwd=child_env, expected_build_sha="unknown", autostart=False)
    try:
        sup.start()
        return sup._hello["seen"]
    finally:
        sup.shutdown()


def test_compute_host_keeps_provider_credentials_without_tier1_secrets(child_env, monkeypatch):
    # It runs agent turns for the dashboard, so provider keys set only in the process env (Docker -e,
    # systemd) must survive; gateway and bot credentials remain sealed by the child policy.
    _plant(monkeypatch)
    names = [*_TIER1, _PROVIDER]
    assert _compute_host_seen(child_env, monkeypatch, names) == {
        "TELEGRAM_BOT_TOKEN": None,
        "GATEWAY_RELAY_SECRET": None,
        _PROVIDER: "fake-openai_api_key",
    }


@pytest.mark.platforms("posix")  # the stand-in binaries are shebang scripts
@pytest.mark.parametrize("site", ["lsp_server", "lsp_go_install", "lsp_npm_install", "raft_bridge", "buzz_cli"])
def test_third_party_children_never_see_hermes_credentials(child_env, monkeypatch, site):
    _plant(monkeypatch)
    # Profile home mode (the container default) re-points HOME; the CLIs whose own logins live
    # under the user's HOME get it back, language servers and installers follow the terminal.
    monkeypatch.setenv("TERMINAL_HOME_MODE", "profile")
    (child_env / "hermes" / "home").mkdir(parents=True, exist_ok=True)
    out = child_env / "seen.json"
    own = {"lsp_server": "LSP_OWN_SETTING", "lsp_go_install": "GOBIN", "lsp_npm_install": "PATH",
           "raft_bridge": "RAFT_CHANNEL_TOKEN", "buzz_cli": "BUZZ_PRIVATE_KEY"}[site]
    probe = _probe_script(child_env / "probe", out, [*_TIER1, _PROVIDER, own, "HOME"])

    if site == "lsp_server":
        from agent.lsp.client import LSPClient

        async def _run():
            client = LSPClient(server_id="probe", workspace_root=str(child_env), command=[str(probe)],
                               env={"LSP_OWN_SETTING": "kept"})
            await client._spawn()
            await client._proc.wait()
            for task in (client._reader_task, client._stderr_task):
                task.cancel()
        asyncio.run(_run())
    elif site == "lsp_go_install":
        from agent.lsp import install

        with patch.object(install.shutil, "which", return_value=str(probe)):
            install._install_go("example.com/probe@latest", "probe")
    elif site == "lsp_npm_install":
        from agent.lsp import install

        with patch.object(install, "find_node_executable", return_value=str(probe)):
            install._install_npm("probe-language-server", "probe")
    elif site == "buzz_cli":
        from plugins.platforms.buzz.adapter import _exec_buzz

        asyncio.run(_exec_buzz(str(probe), [], relay_url="wss://relay.invalid", private_key="buzz-own"))
    else:
        from gateway.config import PlatformConfig
        from plugins.platforms.raft.adapter import RaftAdapter

        monkeypatch.setenv("RAFT_PROFILE", "probe")
        config = PlatformConfig(enabled=True, extra={"bridge_token": "bridge-own", "runtime_session": "default", "port": 0})
        adapter = RaftAdapter(config)
        with patch("plugins.platforms.raft.adapter.shutil.which", return_value=str(probe)):
            adapter._spawn_bridge(4321)
        adapter._bridge_process.wait(timeout=30)

    seen = json.loads(out.read_text(encoding="utf-8-sig"))
    assert {k: seen[k] for k in (*_TIER1, _PROVIDER)} == dict.fromkeys((*_TIER1, _PROVIDER))
    assert seen[own]  # the child's own configuration still arrives
    user_home = site in ("raft_bridge", "buzz_cli")
    assert seen["HOME"] == str(child_env if user_home else child_env / "hermes" / "home")
