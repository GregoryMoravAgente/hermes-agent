"""Command-aware environment fakes shared by process-registry poller tests."""


def make_quoted_path_env():
    """Return one poll's delta, PID, liveness, and exit responses in order."""

    class FakeEnv:
        def __init__(self):
            self.commands = []
            self._responses = {
                "delta": {"output": "6 0\nhello\n"},
                "pid": {"output": "1\n"},
                "alive": {"output": "1\n"},
                "exit": {"output": "0\n"},
            }

        def execute(self, command, **kwargs):
            self.commands.append((command, kwargs))
            if command.startswith("O="):
                return self._responses["delta"]
            if command.startswith("cat ") and ".pid" in command:
                return self._responses["pid"]
            if command.startswith("kill -0"):
                return self._responses["alive"]
            if command.startswith("cat ") and ".exit" in command:
                return self._responses["exit"]
            raise AssertionError(f"unexpected poll command: {command}")

    return FakeEnv()


def make_incremental_poller_env(responses):
    """Build a command-aware environment from one response row per poll.

    Each row supplies separate ``delta``, ``pid``, ``alive``, and ``exit``
    outputs. ``alive`` is the raw ``kill -0`` status: ``"0\n"`` means the
    process is alive and a non-zero value means it is gone. The PID read is a
    separate command, and the exit file is read only after a gone result.
    """

    class FakeEnv:
        def __init__(self):
            self.commands = []
            self._responses = iter(responses)
            self._poll = None

        def execute(self, command, **kwargs):
            self.commands.append(command)
            if command.startswith("O="):
                self._poll = next(self._responses)
                return {"output": self._poll["delta"]}
            if command.startswith("cat ") and ".pid" in command:
                return {"output": self._poll["pid"]}
            if command.startswith("kill -0"):
                return {"output": self._poll["alive"]}
            if command.startswith("cat ") and ".exit" in command:
                return {"output": self._poll["exit"]}
            raise AssertionError(f"unexpected poll command: {command}")

    return FakeEnv()
