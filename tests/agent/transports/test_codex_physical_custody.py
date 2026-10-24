"""Physical Codex custody controls across the real session/client boundary.

The controls below use an actual CodexAppServerSession and CodexAppServerClient,
actual child processes/readers, the real SessionDB writer proof, and the authority
delivery finalizer.  A low-level negative remains for the reader/receipt invariant;
all positive controls cross the actual consumer boundary and name the retained
request and physical receipt they expect.
"""

from __future__ import annotations

import shlex
import json
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.session_persistence import SessionPersistenceMixin
from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.runtime_service_custody import (
    ServiceCustodyError,
    ServiceLease,
)
from gateway.runtime_service_physical import ServiceProcessReceipt
from hermes_cli.backend_retirement import RetirementFence
from tests.agent.test_api_content_sidecar import _FakeAgent


class _RealProofAgent(SessionPersistenceMixin, _FakeAgent):
    """Use the real SessionDB writer/readback path for a custody result witness."""

    def __init__(self, db, session_id: str):
        _FakeAgent.__init__(self)
        self._session_db = db
        self.session_id = session_id
        self.model = "gpt-5.5"
        self.provider = "openai-codex"
        self.base_url = ""
        self.platform = "gateway"
        self._cached_system_prompt = ""
        self.ephemeral_system_prompt = None
        self.reasoning_config = {}
        self.request_overrides = {}
        self.context_compressor = SimpleNamespace(
            awaiting_real_usage_after_compression=False,
            note_usage_less_response=lambda: None,
        )
        self._skill_nudge_interval = 0
        self._iters_since_skill = 0
        self.session_api_calls = 0
        self.session_prompt_tokens = 0
        self.session_completion_tokens = 0
        self.session_total_tokens = 0
        self.session_input_tokens = 0
        self.session_output_tokens = 0
        self.session_cache_read_tokens = 0
        self.session_cache_write_tokens = 0
        self.session_reasoning_tokens = 0
        self.session_estimated_cost_usd = 0.0
        self.session_cost_status = "unknown"
        self.session_cost_source = "unknown"
        self._last_compaction_in_place = False
        self.event_callback = None
        self._session_db_created = True
        self._flushed_db_message_ids = set()
        self._last_flushed_db_idx = 0
        self.flush_count = 0

    def _flush_messages_to_session_db_with_proof(self, messages, conversation_history=None):
        self.flush_count += 1
        return super()._flush_messages_to_session_db_with_proof(messages, conversation_history)


def _portable_service():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="process-custody-test", authority_epoch=1, fence=RetirementFence(),
    )
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "service:portable", profile_id="profile", authority_epoch=1,
    )
    binding = SimpleNamespace(
        authority=None, coordinator=coordinator, parent_handle=service_handle,
        admission_id="portable", execution_generation=1, session_id="session",
        profile_id="profile", authority_epoch=1, process_instance="process-custody-test",
    )
    service = ServiceLease(coordinator, service_handle, binding, "service:portable", {})
    physical_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "service:portable:physical", profile_id="profile", authority_epoch=1,
        parent=service_handle,
    )
    return coordinator, service, ServiceProcessReceipt(service, physical_handle, "service:portable:physical")


def test_real_child_reader_cannot_close_without_durable_delivery():
    """A real child and real reader pair cannot close before a typed delivery receipt."""
    coordinator, _service, receipt = _portable_service()
    process = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdout.write('ok\\n'); sys.stdout.flush(); sys.stdin.buffer.read()"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    stdout_reader = None
    stderr_reader = None
    try:
        def drain(stream, stream_name):
            try:
                stream.read()
            finally:
                receipt.reader_eof(stream_name, threading.current_thread())

        stdout_reader = threading.Thread(target=drain, args=(process.stdout, "stdout"), name="custody-test-stdout")
        stderr_reader = threading.Thread(target=drain, args=(process.stderr, "stderr"), name="custody-test-stderr")
        receipt.bind_process(process.pid, None, "posix_direct", "posix_direct")
        receipt.bind_launch(process)
        receipt.bind_reader("stdout", stdout_reader)
        receipt.bind_reader("stderr", stderr_reader)
        stdout_reader.start()
        stderr_reader.start()
        process.stdin.close()
        process.wait(timeout=10)
        stdout_reader.join(timeout=2)
        stderr_reader.join(timeout=2)
        receipt.reader_joined("stdout", stdout_reader)
        receipt.reader_joined("stderr", stderr_reader)
        assert receipt.close_after_delivery() is False
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "physical_receipt_incomplete"
        assert "I16_turn_sidecars" in coordinator.status()["active_owner_classes"]
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for reader in (stdout_reader, stderr_reader):
            if reader is not None and reader.is_alive():
                reader.join(timeout=2)


def _portable_codex_script(tmp_path, *, exit_after_turn: bool = False, late_output: bool = False):
    """Write an app-server-shaped child that handles JSON-RPC notifications correctly."""
    script = tmp_path / "portable_codex_app_server.py"
    late = """
        if LATE_OUTPUT:
            write_note({"method": "item/agentMessage/delta", "params": {
                "threadId": thread_id, "turnId": turn_id, "delta": "late-output"
            }})
""".replace("LATE_OUTPUT", repr(late_output))
    exit_after = """
        if EXIT_AFTER_TURN:
            break
""".replace("EXIT_AFTER_TURN", repr(exit_after_turn))
    script.write_text(
        "import json, sys\n"
        "def write_message(value):\n"
        "    sys.stdout.write(json.dumps(value) + '\\n')\n"
        "    sys.stdout.flush()\n"
        "def write_note(value):\n"
        "    write_message(value)\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'id' not in request:\n"
        "        continue\n"
        "    request_id = request['id']\n"
        "    method = request.get('method')\n"
        "    if method == 'initialize':\n"
        "        write_message({'id': request_id, 'result': {'userAgent': 'portable-test', 'codexHome': '/tmp', 'platformOs': 'test', 'platformFamily': 'posix'}})\n"
        "    elif method == 'thread/start':\n"
        "        write_message({'id': request_id, 'result': {'thread': {'id': 'portable-thread'}}})\n"
        "    elif method == 'turn/start':\n"
        "        params = request.get('params') or {}\n"
        "        thread_id = params.get('threadId') or 'portable-thread'\n"
        "        turn_id = 'portable-turn'\n"
        "        write_message({'id': request_id, 'result': {'turn': {'id': turn_id}}})\n"
        "        write_note({'method': 'turn/started', 'params': {'threadId': thread_id, 'turn': {'id': turn_id}}})\n"
        "        write_note({'method': 'item/agentMessage/delta', 'params': {'threadId': thread_id, 'turnId': turn_id, 'delta': 'portable-answer'}})\n"
        "        write_note({'method': 'item/completed', 'params': {'threadId': thread_id, 'turnId': turn_id, 'item': {'id': 'portable-item', 'type': 'agentMessage', 'text': 'portable-answer'}}})\n"
        "        write_note({'method': 'turn/completed', 'params': {'threadId': thread_id, 'turn': {'id': turn_id, 'status': 'completed'}}})\n"
        + late
        + exit_after
        + "    else:\n"
        "        write_message({'id': request_id, 'result': {}})\n",
        encoding="utf-8",
    )
    return script


def _codex_launcher(tmp_path, *, exit_after_turn: bool = False, late_output: bool = False):
    script = _portable_codex_script(tmp_path, exit_after_turn=exit_after_turn, late_output=late_output)
    if sys.platform == "win32":
        # CodexAppServerClient appends the literal ``app-server`` argument.  The
        # extensionless file is therefore resolved by the host Python in cwd;
        # no POSIX shell or ExecFormatError is involved.
        host_script = tmp_path / "app-server"
        host_script.write_text(script.read_text(encoding="utf-8"), encoding="utf-8")
        return Path(sys.executable)
    launcher = tmp_path / "portable-codex"
    launcher.write_text(
        f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(script))} \"$@\"\n",
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    return launcher


def _portable_codex_launcher(tmp_path, *, exit_after_turn: bool = False, late_output: bool = False):
    return _codex_launcher(tmp_path, exit_after_turn=exit_after_turn, late_output=late_output)


def _windows_codex_launcher(tmp_path, *, exit_after_turn: bool = False, late_output: bool = False):
    return _codex_launcher(tmp_path, exit_after_turn=exit_after_turn, late_output=late_output)


async def _complete_real_admission(
    session, fixture, writer, text, *, fail_delivery=False, use_native_waiter=True,
):
    _binding, request, receipt = await fixture.run_real_admission(
        session, writer, text, fail_delivery=fail_delivery, use_native_waiter=use_native_waiter,
    )
    if not fail_delivery:
        assert not session._custody.service.request_is_active(request)
    return request, receipt


@pytest.mark.asyncio
async def test_real_session_client_delivers_a_to_b_same_root_and_physical_child(tmp_path, monkeypatch):
    """Two real admissions deliver through one Codex client, service root, and physical child."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request_a, physical = await _complete_real_admission(
            session, fixture, writer, "answer-a",
        )
        request_b, physical_b = await _complete_real_admission(
            session, fixture, writer, "answer-b",
        )
        assert request_a is not request_b
        assert physical_b is physical
        assert writer.flush_count == 2
        assert session._custody.service.requests == {}
        session.close()
        assert session._custody is None
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_active_and_unknown_request_reject_next_binding_without_replacement(
    tmp_path, monkeypatch,
):
    """An active or unknown A request cannot be replaced by an untrusted B candidate."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        binding_a = await fixture.claim("active-admission")
        session.begin_turn(binding_a)
        custody = session._custody
        assert custody is not None and custody.service is not None and custody.request is not None
        service = custody.service
        request_a = custody.request
        root = service.handle
        requests_before = dict(service.requests)
        candidate_b = replace(
            binding_a, admission_id=f"{binding_a.admission_id}:next",
            execution_generation=binding_a.execution_generation + 1,
        )

        with pytest.raises(ServiceCustodyError):
            session.begin_turn(candidate_b)
        assert service.handle is root
        assert service.requests == requests_before
        assert custody.request is request_a
        assert service.physical is None

        session.mark_custody_unknown()
        assert service.unknown is True
        with pytest.raises(ServiceCustodyError):
            session.begin_turn(candidate_b)
        assert service.handle is root
        assert service.requests == requests_before
        assert custody.request is request_a
    finally:
        if session is not None:
            try:
                session.mark_custody_unknown()
            except ServiceCustodyError:
                pass
        fixture.cleanup()


@pytest.mark.asyncio
async def test_delivery_failure_retries_same_request_receipt_without_second_flush(tmp_path, monkeypatch):
    """A finalizer failure retains the exact request/receipt and retry has one proof flush."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request, receipt = await _complete_real_admission(
            session, fixture, writer, "retryable-answer", fail_delivery=True,
        )
        service = session._custody.service
        assert fixture.finalizer_calls == 2
        assert not service.request_is_active(request)
        assert session._custody.request is request
        assert service.physical is receipt
        assert writer.flush_count == 1
        # The production drain retried custody finalization; the test never calls the private
        # finalizer or release helper directly and no second outbound/turn effect is created.
        session.close()
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_pre_effect_delivery_failure_retries_same_admission_without_second_flush(tmp_path, monkeypatch):
    """A source-owned pre-effect failure retries the exact admission once through deliver_settled."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        original_extract = fixture.adapter._extract_response_content
        extract_calls = {"count": 0}
        sends = {"count": 0}

        async def fail_before_outbound(*args, **kwargs):
            extract_calls["count"] += 1
            if extract_calls["count"] == 1:
                from gateway.session_ingress import DeliveryBoundaryError
                raise DeliveryBoundaryError("delivery_failed", effect_state="none")
            return await original_extract(*args, **kwargs)

        async def record_send(*args, **kwargs):
            sends["count"] += 1
            record_delivery = args[-1] if args else kwargs["record_delivery"]
            from gateway.platforms.base import SendResult
            record_delivery(SendResult(success=True, message_id="pre-effect-retry"))

        monkeypatch.setattr(fixture.adapter, "_extract_response_content", fail_before_outbound)
        monkeypatch.setattr(fixture.adapter, "_send_final_text", record_send)
        request, receipt = await _complete_real_admission(
            session, fixture, writer, "pre-effect-answer", use_native_waiter=False,
        )
        service = session._custody.service
        assert extract_calls["count"] == 2
        assert sends["count"] == 1
        assert writer.flush_count == 1
        assert not service.request_is_active(request)
        assert service.physical is receipt
        session.close()
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_ambiguous_delivery_stays_unknown_without_whole_response_resend(tmp_path, monkeypatch):
    """A false SendResult is ambiguous and holds the exact request without retrying the response."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from gateway.platforms.base import SendResult
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        sends = {"count": 0}

        async def record_ambiguous(*args, **kwargs):
            sends["count"] += 1
            record_delivery = args[-1] if args else kwargs["record_delivery"]
            record_delivery(SendResult(success=False, error="bridge timeout"))

        monkeypatch.setattr(fixture.adapter, "_send_final_text", record_ambiguous)
        _binding, request, receipt = await fixture.run_real_admission(
            session, writer, "ambiguous-answer", use_native_waiter=False,
        )
        service = session._custody.service
        assert sends["count"] == 1
        assert fixture.finalizer_calls == 0
        assert writer.flush_count == 1
        assert service.request_is_active(request)
        assert service.physical is receipt
        assert receipt.unknown_reason == "delivery_ambiguous"
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_reused_service_refuses_wrong_principal_source_before_physical_effect(tmp_path, monkeypatch):
    """A tampered principal/source identity is refused before a second request or child effect."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        request, physical = await _complete_real_admission(
            session, fixture, writer, "principal-a",
        )
        service = session._custody.service
        root = service.handle
        live_binding = await fixture.claim("principal-b")
        fixture.binding = live_binding
        wrong = replace(live_binding, source_identity="messaging:wrong-source")
        with pytest.raises(ServiceCustodyError, match="preservation_owner_mismatch"):
            session.begin_turn(wrong)
        assert service.handle is root
        assert service.requests == {}
        assert service.physical is physical
        assert session._custody.request is request
        session.close()
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_late_output_and_eof_hold_exact_request_before_delivery(tmp_path, monkeypatch):
    """Natural child EOF and a late notification do not substitute for delivery proof."""
    from agent.transports.codex_app_server import CodexAppServerTransportError
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_portable_codex_launcher(
                tmp_path, exit_after_turn=True, late_output=True,
            )), codex_home=str(tmp_path / "codex-home"),
            require_custody=True,
        )
        binding = await fixture.claim("late-output-admission")
        session.begin_turn(binding)
        result = session.run_turn(user_input="late-output", model=None)
        assert result.final_text == "portable-answer"
        custody = session._custody
        request = custody.request
        receipt = custody.service.physical
        assert request is not None and receipt is not None
        with pytest.raises(CodexAppServerTransportError):
            session.close()
        assert session.custody_retained is True
        assert custody.request is request
        assert custody.service.request_is_active(request)
        assert custody.service.physical is receipt
        assert receipt.eof_streams == {"stdout", "stderr"}
        assert receipt.joined_streams == {"stdout", "stderr"}
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
async def test_windows_real_job_binary_readers_natural_eof_same_transaction_close_retry(tmp_path, monkeypatch):
    """The Windows lane uses the real Job receipt and retries the same native transaction."""
    from agent.transports.codex_app_server import CodexAppServerTransportError
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_windows_codex_launcher(
                tmp_path, exit_after_turn=True, late_output=True,
            )), codex_home=str(tmp_path / "codex-home"),
            require_custody=True,
        )
        _request, receipt = await _complete_real_admission(
            session, fixture, writer, "windows-answer",
        )
        _custody = session._custody
        assert receipt is not None and receipt.transaction is not None
        transaction = receipt.transaction
        assert receipt.scope == "windows_job"
        assert transaction.process is receipt.process
        original_job_close = transaction.job.close
        failed = {"value": False}
        original_physical_close = receipt.close_after_delivery

        def observe_physical_close():
            result = original_physical_close()
            if result is not True:
                from gateway.runtime_service_native_close import can_retry_managed_native_close
                from gateway.runtime_service_custody import TurnCustodyBinding
                coordinator = receipt.service.coordinator
                with coordinator._lock:
                    record = coordinator._records.get(receipt.handle.lease.lease_id)
                    state = getattr(record, "state", None)
                    reason = getattr(record, "reason", None)
                print("native-close-retry-diagnostic: " + json.dumps({
                    "physical_state": receipt.state, "physical_reason": receipt.unknown_reason,
                    "coordinator_state": state, "coordinator_reason": reason,
                    "same_managed_native_retry": can_retry_managed_native_close(receipt),
                    "typed_binding": isinstance(receipt.service.binding, TurnCustodyBinding),
                    "external_parent": receipt.service.external_parent is not None,
                    "same_transaction_custody": transaction.custody is receipt,
                    "same_process": transaction.process is receipt.process,
                    "same_pid_start": transaction.matches(
                        receipt.process, receipt.pid, receipt.process_started_at,
                    ),
                    "transaction_state": transaction.state,
                    "process_handle_closed": transaction.process_handle_closed,
                    "job_handle_closed": transaction.job_handle_closed,
                    "scope_captured": transaction.scope_captured,
                    "native_empty": transaction.native_empty_observed,
                    "reader_error": receipt.reader_error,
                    "eof": sorted(receipt.eof_streams), "joined": sorted(receipt.joined_streams),
                    "durable_requests": len(receipt.durable_proofs),
                    "delivery_requests": len(receipt.delivery_requests),
                    "completed_requests": len(receipt.completed_requests),
                }, sort_keys=True))
            return result

        monkeypatch.setattr(receipt, "close_after_delivery", observe_physical_close)

        def fail_native_close_once():
            if not failed["value"]:
                failed["value"] = True
                raise OSError("native close boundary failed")
            return original_job_close()

        monkeypatch.setattr(transaction.job, "close", fail_native_close_once)
        with pytest.raises(CodexAppServerTransportError):
            session.close()
        assert session.custody_retained is True
        assert session._custody.service.physical is receipt
        assert receipt.unknown_reason == "native_close_failed"
        monkeypatch.setattr(transaction.job, "close", original_job_close)
        session.close()
        assert transaction.process is receipt.process
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
    finally:
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
@pytest.mark.parametrize("membership_fault", [None, "query", "foreign"])
async def test_windows_exited_codex_root_drains_only_retained_job(
    tmp_path, monkeypatch, membership_fault,
):
    """An exited root cannot release its live Job child or bypass exact membership."""
    from agent.transports.codex_app_server import CodexAppServerTransportError
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
    from tests.waiting import wait_until

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    transaction = None
    original_membership = None
    try:
        launcher = _windows_codex_launcher(tmp_path, exit_after_turn=True)
        script = tmp_path / "app-server"
        script.write_text(
            "import subprocess\n"
            "subprocess.Popen([__import__('sys').executable, '-c', "
            "'import threading; threading.Event().wait(30)'], "
            "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
            + script.read_text(encoding="utf-8"), encoding="utf-8",
        )
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(launcher),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        _request, receipt = await _complete_real_admission(
            session, fixture, writer, "same-job-child",
        )
        transaction = receipt.transaction
        process, job, handle = transaction.process, transaction.job, transaction.job._handle
        wait_until(lambda: process.poll() is not None, timeout=5, what="Codex root exit")
        assert process.poll() == 0
        assert job.contains_process_handle(transaction.process_ref) is True
        assert job.active_processes() >= 1
        assert transaction.observe_empty() is None
        assert transaction.scope_observation == "nonempty"
        original_membership = job.contains_process_handle
        if membership_fault is not None:
            def invalid_membership(_process_ref):
                if membership_fault == "query":
                    raise OSError("native membership query unavailable")
                return False

            monkeypatch.setattr(job, "contains_process_handle", invalid_membership)
            with pytest.raises(CodexAppServerTransportError, match="native Job did not settle"):
                session.close()
            assert receipt.state == "unknown"
            assert session._custody.service.physical is receipt
            assert transaction.process is process and transaction.job is job
            assert job._handle is handle
            assert transaction.termination_attempts == 0
            assert transaction.process_handle_closed is False
            assert transaction.job_handle_closed is False
        else:
            session.close()
            assert transaction.process is process and transaction.job is job
            assert transaction.termination_attempts == 1
            assert transaction.native_empty_observed is True
            assert transaction.process_handle_closed is True
            assert transaction.job_handle_closed is True
            assert receipt.state == "closed"
    finally:
        if transaction is not None and not transaction.job_handle_closed:
            if original_membership is not None:
                monkeypatch.setattr(transaction.job, "contains_process_handle", original_membership)
            transaction.terminate_owned_tree()
            transaction.process.wait(timeout=5)
            wait_until(lambda: transaction.observe_empty() is True, timeout=5, what="owned Job cleanup")
            assert transaction.close_after_observed_empty() is True
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
async def test_windows_empty_observation_cannot_close_replacement_job(tmp_path, monkeypatch):
    """An empty receipt stays bound to the native Job originally observed."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from hermes_cli.local_runtime.processes import _WindowsJob
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
    from tests.waiting import wait_until

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    transaction = original_job = foreign_job = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_windows_codex_launcher(tmp_path, exit_after_turn=True)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        _request, receipt = await _complete_real_admission(session, fixture, writer, "exact-empty-job")
        transaction = receipt.transaction
        transaction.process.wait(timeout=5)
        wait_until(lambda: transaction.observe_empty() is True, timeout=5, what="original empty Job")
        original_job = transaction.job
        original_handle = original_job._handle
        foreign_job = _WindowsJob()
        foreign_handle = foreign_job._handle
        assert foreign_job.empty() is True
        transaction.job = foreign_job
        assert transaction.drain_after_process_exit(1) is False
        assert transaction.close_after_observed_empty() is False
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "foreign_custody"
        assert original_job._handle is original_handle
        assert foreign_job._handle is foreign_handle
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
    finally:
        if transaction is not None:
            transaction.job = original_job or transaction.job
            if transaction.process.poll() is None:
                transaction.terminate_owned_tree()
                transaction.process.wait(timeout=5)
            wait_until(lambda: transaction.observe_empty() is True, timeout=5, what="original Job cleanup")
            assert transaction.close_after_observed_empty() is True
        if foreign_job is not None:
            assert foreign_job.empty() is True
            foreign_job.close()
        fixture.cleanup()


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
async def test_windows_codex_constructor_failure_retains_transaction_before_publish(tmp_path, monkeypatch):
    """A native launch that fails before client publication keeps the exact child and transaction."""
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
    from tools.process_registry_windows import WindowsLaunchTransaction

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    transaction = None
    try:
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_windows_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        binding = await fixture.claim("codex-constructor-failure")
        session.begin_turn(binding)
        service = session._custody.service
        original_launch = WindowsLaunchTransaction.launch
        seen = {}

        def launch_then_interrupt(native_transaction, *args, **kwargs):
            process = original_launch(native_transaction, *args, **kwargs)
            seen["transaction"] = native_transaction
            seen["process"] = process
            raise KeyboardInterrupt("after native launch")

        monkeypatch.setattr(WindowsLaunchTransaction, "launch", launch_then_interrupt)
        with pytest.raises(KeyboardInterrupt, match="after native launch"):
            session.ensure_started()

        transaction = seen["transaction"]
        process = seen["process"]
        receipt = service.physical
        assert session._client is None
        assert receipt is not None
        assert receipt.transaction is transaction
        assert receipt.process is process
        assert transaction.process is process
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "partial_launch"
        assert service.unknown is True

        session.close()
        assert service.physical is receipt
        assert receipt.transaction is transaction
    finally:
        if transaction is not None and transaction.process is not None:
            process = transaction.process
            if process.poll() is None:
                transaction.terminate_owned_tree()
                process.wait(timeout=10)
            if transaction.native_empty_observed is not True:
                assert transaction.observe_empty() is True
            assert transaction.close_after_observed_empty() is True
        if session is not None and session._client is not None and session._client._proc.poll() is None:
            session._client._proc.kill()
            session._client._proc.wait(timeout=5)
        fixture.cleanup()
