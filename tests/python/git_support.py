"""Shared git invocation for tests that build throwaway repositories.

On this workstation an on-access antivirus scan can hold a freshly written file for a
moment, and git then fails an object write with "Permission denied". That is a host
artefact, not a repository fact, so a git call that fails only for that reason is retried
a few times before it counts. Any other failure is returned on the first attempt.
"""

from __future__ import annotations

import subprocess
import time

ATTEMPTS = 5
_LOCK_MARKER = "Permission denied"


def run_git(argv: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run ``git`` with ``argv`` and ``subprocess.run`` keyword arguments, capturing output.

    The call is repeated, with a growing pause, only while it fails and its stderr carries
    the antivirus lock marker. The last attempt's result is returned whatever it says.
    """
    kwargs.setdefault("capture_output", True)
    result = subprocess.run(["git", *argv], **kwargs)
    attempt = 1
    while result.returncode != 0 and attempt < ATTEMPTS and _is_lock_failure(result):
        time.sleep(2 * attempt)
        attempt += 1
        result = subprocess.run(["git", *argv], **kwargs)
    return result


def _is_lock_failure(result: subprocess.CompletedProcess) -> bool:
    stderr = result.stderr
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    return _LOCK_MARKER in (stderr or "")
