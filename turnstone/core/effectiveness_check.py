"""Independent effectiveness verification for Turnstone-delegated tasks.

**The gap this closes.** The judge (``turnstone.core.judge``) and the
proxy-trace ingester (``turnstone.core.proxy_trace``) both answer "did a
tool call happen and did it look safe" — advisory, pre/at-execution
questions. Neither answers "did the delegated work actually fix the need
it was requested for." A delegated session that runs ``launchctl kickstart``
and reports success in its final text has *executed* a fix; whether the
daemon is actually alive afterward is a separate, unverified claim.

**The pattern being ported.** This codebase already has a proven answer to
the same problem in a different place: the ``runbook-security-posture-check``
/ ``runbook-service-health`` / ``runbook-launchd-fleet-audit`` skills
(``~/.claude/skills/``) require an independently-implemented second check —
one that does NOT reuse the mechanism that performed or detected the fix —
before a fix counts as verified, and a disagreement between the fixer's
self-report and the independent check escalates rather than silently
clearing. Those skills' independent checks are deliberately *not*
``launchctl``-based for launchd fixes (the fixer typically used
``launchctl kickstart``/``load``/``start`` — re-querying via ``launchctl``
would just ask the same subsystem to grade its own homework); they use raw
process-table inspection (`ps`/`pgrep`) or a live behavioral probe instead.

This module ports that exact discipline into Turnstone's own delegation
loop, starting with one concrete task class: **launchd daemon
restart/(re)start fixes**, since that is a real class of work already
delegated through the ``claude_proxy.py`` bridge (Poe/council chat →
``bash`` calls) and already has a proven independent-check design to copy.

**Independent check design (launchd class):**

1. Read the target label's *plist* directly (``plistlib``, a filesystem
   read) to recover its expected ``ProgramArguments`` — this is a data path
   that never goes through ``launchctl`` at all.
2. Cross-reference the live process table (`ps -eo pid,args` via
   ``subprocess``, not ``launchctl list``/``launchctl print``) for a
   running process whose command line matches those arguments.
3. Report PASS/FAIL plus the evidence (PID found or not), independent of
   whatever the delegated session's own bash calls reported.

This deliberately does NOT call ``launchctl`` anywhere in the verification
path — the fixer's own tool (`launchctl kickstart -k ...`) and the checker's
tool must be different subsystems, mirroring the runbook pattern's
`fs.statSync`-vs-`stat`-binary and `lsof`-vs-restart-mechanism separations.

Other task classes (file-permission fixes, service HTTP health, etc.) would
each need their own independent check function here — this module is
explicitly task-class-specific, not a universal verifier. Only the launchd
class is implemented so far; see ``turnstone/docs/turnstone-fork-patches.md``
for status.
"""

from __future__ import annotations

import plistlib
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from turnstone.core.log import get_logger

log = get_logger(__name__)

# Matches a launchd label the way it actually appears in real bash commands
# a delegated fix session would run, e.g.:
#   launchctl kickstart -k gui/501/com.turnstone.effectiveness-demo
#   launchctl start com.myc.council
#   launchctl load ~/Library/LaunchAgents/com.chopp.foo.plist
#   sudo launchctl kickstart -k system/com.example.bar
#
# Deliberately permissive on the verb (kickstart/start/load/bootstrap) since
# the goal is "did a bash call in this session attempt to bring a launchd
# job up" — the independent check below is what actually decides PASS/FAIL,
# not this regex.
_LAUNCHCTL_LABEL_RE = re.compile(
    r"launchctl\s+(?:kickstart|start|load|bootstrap)\b"
    r"(?:\s+-k)?"
    r"\s+(\S+)",
)

# Independently catches a ``.plist`` path anywhere in the command, not just
# immediately after the verb — needed for ``launchctl bootstrap <domain>
# <path>``, where the label-bearing argument is the *second* argument, not
# the one immediately following the verb (that slot holds a bare domain
# like ``gui/501``, no label). Real example hit during live testing below.
_PLIST_PATH_RE = re.compile(r"([\w.-]+)\.plist\b")

# A real command-substitution idiom (``$(id -u)``) contains an internal
# space, which breaks naive whitespace tokenization of the argument that
# follows the verb — confirmed live: ``launchctl bootstrap gui/$(id -u)
# ~/Library/....plist`` tokenized as "gui/$(id" before this fix. Collapse
# whitespace inside any ``$(...)`` substitution before tokenizing so the
# domain argument reads as one token, same as it does to the real shell.
_CMD_SUBST_RE = re.compile(r"\$\([^)]*\)")


def extract_launchd_targets(bash_command: str) -> list[str]:
    """Return every launchd label a bash command appears to target.

    Handles the real shapes seen in practice: a bare label (``launchctl
    start com.myc.council``), a domain-target/label pair (``launchctl
    kickstart -k gui/501/com.example.foo``, including a command-substitution
    uid like ``gui/$(id -u)/com.example.foo``), and a plist path
    (``launchctl load``/``bootstrap ... ~/Library/LaunchAgents/com.foo.plist``)
    — each normalized down to the bare label. Pure function, no I/O — easy
    to unit test against captured commands.
    """
    normalized = _CMD_SUBST_RE.sub(lambda m: m.group(0).replace(" ", ""), bash_command)

    targets: list[str] = []
    for match in _LAUNCHCTL_LABEL_RE.finditer(normalized):
        raw = match.group(1)
        if raw.endswith(".plist"):
            raw = Path(raw).stem
        elif "/" in raw:
            # gui/<uid>/<label> or system/<label> — take the last segment,
            # but only if it looks like a label, not a bare domain (e.g.
            # "gui/501" with nothing after it, as in a bootstrap call).
            raw = raw.rsplit("/", 1)[-1]
        if raw and "$(" not in raw and raw not in ("gui", "system"):
            targets.append(raw)

    # Independent pass: any .plist path anywhere in the command, regardless
    # of its position relative to the verb (covers `bootstrap <domain>
    # <path>`, where the verb-adjacent token is a bare domain).
    for match in _PLIST_PATH_RE.finditer(normalized):
        stem = Path(match.group(0)).stem
        if stem:
            targets.append(stem)

    # Distinct, order-preserving.
    seen: set[str] = set()
    out: list[str] = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


@dataclass
class EffectivenessResult:
    task_class: str
    target: str
    passed: bool
    method: str
    detail: str
    evidence: list[str] = field(default_factory=list)

    def as_evidence_tag(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"EFFECTIVENESS_CHECK:{self.task_class}:{self.target}:{status}:{self.detail}"


def _find_plist(label: str) -> Path | None:
    """Locate *label*'s plist under the standard per-user/system search
    paths. Filesystem read only — no ``launchctl`` involved.
    """
    candidates = [
        Path("~/Library/LaunchAgents").expanduser() / f"{label}.plist",
        Path("/Library/LaunchAgents") / f"{label}.plist",
        Path("/Library/LaunchDaemons") / f"{label}.plist",
    ]
    for c in candidates:
        if c.is_file():
            return c
    return None


def _expected_program_args(plist_path: Path) -> list[str] | None:
    try:
        with plist_path.open("rb") as fh:
            data = plistlib.load(fh)
    except (OSError, plistlib.InvalidFileException):
        return None
    args = data.get("ProgramArguments")
    if isinstance(args, list) and args and all(isinstance(a, str) for a in args):
        return args
    program = data.get("Program")
    return [program] if isinstance(program, str) else None


def _live_process_matches(expected_args: list[str]) -> tuple[bool, str]:
    """Independent process-table check via ``ps`` — never ``launchctl``.

    Matches on the expected program (argv[0], basename-insensitive) plus,
    if present, the first additional argument — enough to distinguish
    ``sleep 100000`` from an unrelated ``sleep`` invocation without being
    so strict that harmless argument-order/absolute-path differences cause
    a false FAIL.
    """
    try:
        out = subprocess.run(
            ["ps", "-eo", "pid=,args="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"ps invocation failed: {exc}"

    program = Path(expected_args[0]).name
    extra = expected_args[1] if len(expected_args) > 1 else None

    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, args_str = line.split(None, 1)
        except ValueError:
            continue
        if program not in args_str:
            continue
        if extra is not None and extra not in args_str:
            continue
        return True, f"pid={pid_str} args matched via ps process-table scan"

    return False, f"no live process found matching program={program!r} extra={extra!r}"


def check_launchd_daemon_alive(label: str) -> EffectivenessResult:
    """Independent post-fix check for the ``launchd`` task class.

    Does not call ``launchctl`` at any point — reads the plist directly and
    cross-references the real process table via ``ps``. A delegated session
    that ran ``launchctl kickstart`` and self-reported success is checked
    against a completely different subsystem here.
    """
    plist_path = _find_plist(label)
    if plist_path is None:
        return EffectivenessResult(
            task_class="launchd",
            target=label,
            passed=False,
            method="plist_lookup",
            detail=f"no plist found for label {label!r} under standard search paths",
        )

    expected_args = _expected_program_args(plist_path)
    if not expected_args:
        return EffectivenessResult(
            task_class="launchd",
            target=label,
            passed=False,
            method="plist_parse",
            detail=f"plist at {plist_path} has no usable ProgramArguments/Program",
        )

    alive, detail = _live_process_matches(expected_args)
    return EffectivenessResult(
        task_class="launchd",
        target=label,
        passed=alive,
        method="ps_process_table",
        detail=detail,
        evidence=[f"plist={plist_path}", f"expected_args={expected_args}"],
    )
