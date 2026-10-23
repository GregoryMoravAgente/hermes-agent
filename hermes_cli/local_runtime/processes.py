"""Contain each managed Windows router tree without adopting the owner process."""

from __future__ import annotations

from collections.abc import Mapping
import ctypes
from ctypes import wintypes
import re
import subprocess
import sys
import threading
from typing import Any

import psutil


class _BasicLimits(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
    )]


class _BasicAccounting(ctypes.Structure):
    """Exact JOBOBJECT_BASIC_ACCOUNTING_INFORMATION layout.

    The two per-period kernel/user counters precede the four DWORD counters.
    Keeping the fourth LARGE_INTEGER is required for QueryInformationJobObject
    to place ActiveProcesses at the documented offset.
    """

    _fields_ = [
        ("TotalUserTime", ctypes.c_longlong),
        ("TotalKernelTime", ctypes.c_longlong),
        ("ThisPeriodTotalUserTime", ctypes.c_longlong),
        ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
        ("TotalPageFaultCount", wintypes.DWORD),
        ("TotalProcesses", wintypes.DWORD),
        ("ActiveProcesses", wintypes.DWORD),
        ("TotalTerminatedProcesses", wintypes.DWORD),
    ]


class _BasicProcessIdListHeader(ctypes.Structure):
    """Header for JOBOBJECT_BASIC_PROCESS_ID_LIST (query class 3)."""

    _fields_ = [
        ("NumberOfAssignedProcesses", wintypes.DWORD),
        ("NumberOfProcessIdsInList", wintypes.DWORD),
    ]


class _ExtendedLimits(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


_JOB_OBJECT_QUERY = 0x0004
_ERROR_FILE_NOT_FOUND = 2
_ERROR_ALREADY_EXISTS = 183
_WINDOWS_JOB_NAME_PREFIX = "Local\\AgenteDesktop.ComputeHost."
_WINDOWS_JOB_NAME_RE = re.compile(
    rf"^{re.escape(_WINDOWS_JOB_NAME_PREFIX)}(?P<token>[0-9a-f]{{32}})$"
)


class _WindowsJobAbsent(FileNotFoundError):
    """The exact named owner Job is absent, rather than inaccessible."""


def _valid_windows_job_name(name: object) -> bool:
    return isinstance(name, str) and _WINDOWS_JOB_NAME_RE.fullmatch(name) is not None


class _WindowsJob:
    def __init__(self, name: str | None = None):
        self._lock = threading.Lock()
        self._api = ctypes.WinDLL("kernel32", use_last_error=True)
        for function_name, args, result in (
            ("CreateJobObjectW", [ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
            ("OpenJobObjectW", [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            ("SetInformationJobObject", [wintypes.HANDLE, ctypes.c_int,
                                         ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
            ("AssignProcessToJobObject", [wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            ("IsProcessInJob", [wintypes.HANDLE, wintypes.HANDLE,
                                 ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
            ("QueryInformationJobObject", [
                wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ], wintypes.BOOL),
            ("TerminateJobObject", [wintypes.HANDLE, wintypes.DWORD], wintypes.BOOL),
            ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
        ):
            fn = getattr(self._api, function_name)
            fn.argtypes = args
            fn.restype = result
        self._name = name
        self._recovery_only = False
        self._no_breakaway = False
        # NULL security attributes create a non-inheritable owner handle.  A
        # supervisor supplied name is a random, exact recovery token; an existing
        # object is never accepted as the new owner.
        if name is not None and not _valid_windows_job_name(name):
            raise ValueError("invalid compute host Windows Job name")
        ctypes.set_last_error(0)
        self._handle = self._api.CreateJobObjectW(None, name)
        if not self._handle:
            raise ctypes.WinError(ctypes.get_last_error())
        if name is not None and ctypes.get_last_error() == _ERROR_ALREADY_EXISTS:
            self.close()
            raise RuntimeError("compute host Windows Job name already exists")
        try:
            limits = _ExtendedLimits()
            # Neither BREAKAWAY_OK nor SILENT_BREAKAWAY_OK: descendants stay contained.
            limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE
            if not self._api.SetInformationJobObject(
                    self._handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise ctypes.WinError(ctypes.get_last_error())
            self._no_breakaway = True
        except BaseException:
            self.close()
            raise

    @classmethod
    def open_existing(cls, name: str) -> _WindowsJob:
        """Open exactly one persisted Job for query-only recovery.

        This handle is never assigned to a new process and never used to adopt a
        live child.  The caller may retain it while refusing a duplicate start;
        closing the last handle on a KILL_ON_JOB_CLOSE Job can terminate members.
        """
        if not _valid_windows_job_name(name):
            raise ValueError("invalid compute host Windows Job name")
        job = cls.__new__(cls)
        job._lock = threading.Lock()
        job._api = ctypes.WinDLL("kernel32", use_last_error=True)
        for function_name, args, result in (
            ("OpenJobObjectW", [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            ("IsProcessInJob", [wintypes.HANDLE, wintypes.HANDLE,
                                 ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
            ("QueryInformationJobObject", [
                wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ], wintypes.BOOL),
            ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
        ):
            fn = getattr(job._api, function_name)
            fn.argtypes = args
            fn.restype = result
        job._name = name
        job._recovery_only = True
        job._no_breakaway = False
        ctypes.set_last_error(0)
        job._handle = job._api.OpenJobObjectW(_JOB_OBJECT_QUERY, False, name)
        if job._handle:
            return job
        error = ctypes.get_last_error()
        job._handle = None
        if error == _ERROR_FILE_NOT_FOUND:
            raise _WindowsJobAbsent(error, f"Windows Job not found: {name}")
        raise ctypes.WinError(error)

    def is_canonical_owner_job(self) -> bool:
        """Return true only for a live newly-created no-breakaway owner Job."""
        with self._lock:
            return (
                self._recovery_only is False
                and self._no_breakaway is True
                and self._handle is not None
            )

    @property
    def name(self) -> str | None:
        return self._name

    def assign(self, proc: subprocess.Popen) -> None:
        # Popen retains the original process handle, avoiding a PID-reuse race.
        if not self._api.AssignProcessToJobObject(self._handle, int(proc._handle)):
            raise ctypes.WinError(ctypes.get_last_error())

    def active_processes(self) -> int | None:
        """Observe the real active member count without changing job state."""
        with self._lock:
            if self._handle is None:
                return None
            info = _BasicAccounting()
            returned = wintypes.DWORD()
            ok = self._api.QueryInformationJobObject(
                self._handle, 1, ctypes.byref(info), ctypes.sizeof(info),
                ctypes.byref(returned),
            )
            if not ok or returned.value != ctypes.sizeof(info):
                return None
            return int(info.ActiveProcesses)

    def empty(self) -> bool | None:
        """Return true only when the kernel reports zero active processes."""
        active = self.active_processes()
        return None if active is None else active == 0

    def contains_pid(self, pid: int) -> bool | None:
        """Check membership in this exact owned Job using the kernel PID list.

        ``None`` means the native query could not prove membership.  The caller
        must fail closed; a process being visible in a system-wide census is not
        an owner proof.  The handle is held across the complete query so a close
        cannot race the result.
        """
        if type(pid) is not int or pid <= 0:
            return None
        header_size = ctypes.sizeof(_BasicProcessIdListHeader)
        pid_size = ctypes.sizeof(ctypes.c_size_t)
        buffer_size = header_size + pid_size
        max_buffer_size = 1024 * 1024
        with self._lock:
            if self._handle is None:
                return None
            for _ in range(8):
                buffer = (ctypes.c_ubyte * buffer_size)()
                returned = wintypes.DWORD()
                ok = self._api.QueryInformationJobObject(
                    self._handle, 3, ctypes.byref(buffer), buffer_size,
                    ctypes.byref(returned),
                )
                if not ok:
                    required = int(returned.value)
                    if required <= buffer_size or required > max_buffer_size:
                        return None
                    buffer_size = required
                    continue
                if int(returned.value) > buffer_size:
                    return None
                header = _BasicProcessIdListHeader.from_buffer(buffer)
                count = int(header.NumberOfProcessIdsInList)
                capacity = (buffer_size - header_size) // pid_size
                if count < 0 or count > int(header.NumberOfAssignedProcesses) or count > capacity:
                    return None
                ids = (ctypes.c_size_t * count).from_buffer(buffer, header_size)
                return any(int(value) == pid for value in ids)
        return None

    def contains_process_handle(self, process: Any) -> bool | None:
        """Prove membership for the exact retained process handle, without PID lookup."""
        try:
            raw_process = getattr(process, "raw_handle", None)
        except Exception:  # health: allow BLE001 -- opaque handle access cannot prove identity
            return None
        with self._lock:
            if self._handle is None or raw_process is None:
                return None
            member = wintypes.BOOL()
            try:
                ok = self._api.IsProcessInJob(
                    raw_process, self._handle, ctypes.byref(member))
            except Exception:  # health: allow BLE001 -- native membership query cannot prove identity
                return None
            return bool(member.value) if ok else None

    def terminate(self, exit_code: int = 1) -> None:
        """Terminate members through this owned Job handle before retrying close."""
        if type(exit_code) is not int or not 0 <= exit_code <= 0xFFFFFFFF:
            raise ValueError("invalid Windows Job termination code")
        with self._lock:
            if self._recovery_only:
                raise RuntimeError("query-only Windows Job cannot terminate members")
            if self._handle is None:
                return
            if not self._api.TerminateJobObject(self._handle, exit_code):
                raise ctypes.WinError(ctypes.get_last_error())

    def close(self) -> None:
        """Terminate the contained tree; repeated closes are harmless."""
        with self._lock:
            if self._handle is not None:
                if not self._api.CloseHandle(self._handle):
                    raise ctypes.WinError(ctypes.get_last_error())
                self._handle = None


_CREDENTIAL_ENV_MARKERS = ("_API_KEY", "_TOKEN", "_SECRET", "PASSWORD", "_CREDENTIALS")


def server_child_env(base_env: Mapping[str, str]) -> dict[str, str]:
    """Return the environment a native inference child (llama-server) gets.

    Provider and tool credentials never belong in a native child that talks to nobody but
    us — and on Windows they are not merely leaked: the bundled OpenMP runtime died with
    STATUS_HEAP_CORRUPTION during initialisation with one `*_API_KEY` present and loaded fine
    with only that variable removed (#116109, confirmed on a model-free libomp.dll probe).
    Everything else (PATH, CUDA_*, HSA_*, OMP_*, TEMP, …) passes through untouched. Applied by
    the llama-server supervisor only: spawn_server is also the generic bounded-probe spawner
    (git / PowerShell / update probes), whose children legitimately need GH_TOKEN, HF_TOKEN, …
    """
    return {
        key: value for key, value in base_env.items()
        if not any(marker in key.upper() for marker in _CREDENTIAL_ENV_MARKERS)
    }


def spawn_server(cmd, **kwargs) -> tuple[subprocess.Popen, _WindowsJob | None]:
    """Start a router, returning its process and an owner-held containment handle.

    Keep the job until shutdown and call close() to terminate the entire tree.
    Windows closes it automatically if the owner dies. Other hosts retain Popen's
    ordinary behavior. ``owner_job_name`` is consumed here, never passed to
    Popen, and names the exact per-spawn Job persisted by HostSupervisor for
    query-only recovery. Assignment happens before the child's first instruction.
    """
    owner_job_name = kwargs.pop("owner_job_name", None)
    if sys.platform != "win32":
        return subprocess.Popen(cmd, **kwargs), None
    job = _WindowsJob(name=owner_job_name)
    proc = None
    try:
        kwargs["creationflags"] = kwargs.get("creationflags", 0) | 0x00000004  # CREATE_SUSPENDED
        proc = subprocess.Popen(cmd, **kwargs)
        job.assign(proc)
        psutil.Process(proc.pid).resume()
        return proc, job
    except BaseException:
        try:
            if proc is not None:
                # Assignment may have failed: closing an empty job is not enough.
                proc.kill()
                proc.wait(timeout=10)
        finally:
            try:
                job.close()
            finally:
                if proc is not None:
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        if stream is not None:
                            stream.close()
                    proc._handle.Close()
        raise
