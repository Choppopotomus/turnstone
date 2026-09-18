"""Tests for turnstone.core.effectiveness_check — the independent
"executed vs effective" verification for Turnstone-delegated tasks.

Mirrors the bar the runbook skills (security-posture-check /
service-health / launchd-fleet-audit) hold their own independent checks
to: a real process-table cross-reference, never the same subsystem
(``launchctl``) the delegated fix itself used.
"""

from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

from turnstone.core import effectiveness_check as ec


# --- extract_launchd_targets ------------------------------------------------


@pytest.mark.parametrize(
    "command,expected",
    [
        (
            "launchctl kickstart -k gui/501/com.turnstone.effectiveness-demo",
            ["com.turnstone.effectiveness-demo"],
        ),
        ("launchctl start com.myc.council", ["com.myc.council"]),
        (
            "launchctl load ~/Library/LaunchAgents/com.chopp.foo.plist",
            ["com.chopp.foo"],
        ),
        ("ls -la", []),
        (
            "sudo launchctl kickstart -k system/com.example.bar",
            ["com.example.bar"],
        ),
        # Real command-substitution idiom that broke naive tokenization
        # during live testing (2026-09-18) — internal space in `$(id -u)`.
        (
            "launchctl bootstrap gui/$(id -u) "
            "~/Library/LaunchAgents/com.turnstone.effectiveness-demo.plist 2>&1",
            ["com.turnstone.effectiveness-demo"],
        ),
        # A read-only inspection command must never be treated as a fix
        # attempt (no verb match).
        (
            "launchctl print gui/$(id -u)/com.turnstone.effectiveness-demo 2>&1 | head -30",
            [],
        ),
    ],
)
def test_extract_launchd_targets(command: str, expected: list[str]) -> None:
    assert ec.extract_launchd_targets(command) == expected


# --- check_launchd_daemon_alive ---------------------------------------------


def _write_plist(path: Path, program_args: list[str]) -> None:
    with path.open("wb") as fh:
        plistlib.dump({"Label": path.stem, "ProgramArguments": program_args}, fh)


def test_check_launchd_daemon_alive_no_plist_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Point the search paths somewhere with nothing in it.
    monkeypatch.setattr(ec, "_find_plist", lambda label: None)
    result = ec.check_launchd_daemon_alive("com.does-not-exist")
    assert result.passed is False
    assert result.method == "plist_lookup"


def test_check_launchd_daemon_alive_pass_real_process(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Independent PASS path: a real live process (started here, not via
    launchctl) whose argv matches the plist's ProgramArguments.
    """
    import subprocess
    import time

    plist_path = tmp_path / "com.effcheck.testpass.plist"
    _write_plist(plist_path, ["/bin/sleep", "1234567"])
    monkeypatch.setattr(ec, "_find_plist", lambda label: plist_path)

    proc = subprocess.Popen(["/bin/sleep", "1234567"])
    try:
        time.sleep(0.2)  # let it show up in the process table
        result = ec.check_launchd_daemon_alive("com.effcheck.testpass")
        assert result.passed is True
        assert result.method == "ps_process_table"
        assert "launchctl" not in result.detail
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_check_launchd_daemon_alive_fail_no_matching_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Independent FAIL path: plist exists, expects a program, but nothing
    on the real process table matches it — the exact "self-report says
    fixed, reality disagrees" case.
    """
    plist_path = tmp_path / "com.effcheck.testfail.plist"
    _write_plist(plist_path, ["/bin/does-not-exist-binary-xyz", "999999"])
    monkeypatch.setattr(ec, "_find_plist", lambda label: plist_path)

    result = ec.check_launchd_daemon_alive("com.effcheck.testfail")
    assert result.passed is False
    assert result.method == "ps_process_table"


def test_effectiveness_result_evidence_tag_format() -> None:
    result = ec.EffectivenessResult(
        task_class="launchd",
        target="com.example.foo",
        passed=False,
        method="ps_process_table",
        detail="no live process found",
    )
    assert result.as_evidence_tag() == (
        "EFFECTIVENESS_CHECK:launchd:com.example.foo:FAIL:no live process found"
    )
