"""Proxy-trace ingestion — turns ``claude_proxy.py`` debug-log records into
advisory judge verdicts / ledger rows.

**The gap this closes.**  Some model backends (``claude-subscription``,
resolved via ``model_definitions`` to a local ``~/.hermes/claude_proxy.py``
instance) are not stateless completion endpoints — they shell out to
``claude -p --allowedTools "..."`` and return only the final assistant text
over the OpenAI Chat Completions wire.  Any tool call *inside* that
subprocess is invisible to this process: the wire response never carries a
``tool_calls`` delta, so the per-call judge in :mod:`turnstone.core.judge`
never fires for it (there is no call for it to evaluate).  The proxy's own
tool-use signal — ``num_turns``, ``usage.iterations``, ``permission_denials``
— never reaches Turnstone at all; it only reaches the proxy's own debug log
(``~/.hermes/logs/claude-proxy-<port>-debug.log``, one ``full_output: {...}``
line per completed request, added 2026-07-26).

**What this module does instead.**  It is a *session-level*, not a
*per-tool-call*, judge: it reads the proxy's debug log for a given backend,
parses each ``full_output`` record, and writes ONE advisory
``intent_verdicts`` row per proxied session summarizing what the subprocess
reported about itself.  This is coarser than the live per-call judge (it
can't say "this specific Read call touched file X") but it is real signal
that did not exist before: a session that took 9 turns, or reported a
permission denial, is now visible in the ledger instead of vanishing
entirely.

**Known limitations (accepted, not silently hidden):**

- No ``ws_id`` correlation.  Turnstone never threads a workstream/session
  identifier through to ``claude_proxy.py`` (it isn't a header the OpenAI
  Chat Completions wire carries today, and threading one through would mean
  changing :func:`turnstone.core.model_turn.model_turn`'s signature and
  every call site — out of scope for a visibility fix).  Rows use a
  sentinel ``ws_id`` of ``"proxy:<port>"``.
- ``call_id`` is the claude CLI's own ``session_id`` (or ``uuid`` if
  ``session_id`` is absent) — a stable per-subprocess-run identifier, not a
  Turnstone tool-call id.
- Verdicts are advisory only.  ``tier="proxy_trace"`` is deliberately
  distinct from the existing ``"heuristic"`` / ``"llm"`` / ``"arbitrated"``
  tiers so nothing that reads those tiers (``history_decoration.py``'s
  per-call_id chat-bubble decoration, Smart Approvals' confidence gate)
  mistakes this for a live pre-approval judgment. It cannot gate anything —
  by the time the row exists, the proxied session has already completed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from turnstone.core.log import get_logger

log = get_logger(__name__)

# One claude_proxy.py debug-log line per completed request:
#     "    full_output: {...json...}"
# (see ~/.hermes/claude_proxy.py's `_dlog(f"    full_output: {raw}")`).
# The JSON body is written on a single line, so DOTALL isn't needed.
_FULL_OUTPUT_RE = re.compile(r"^\s*full_output:\s*(\{.*\})\s*$", re.MULTILINE)

# Mirrors the judge's own per-turn budget (see JudgeConfig / judge.py's
# "max_turns" logging) — a proxied session that took at least this many
# turns gets flagged for human review, same threshold philosophy as the
# live judge's turn cap.
_HIGH_TURN_THRESHOLD = 5

# Confirmed against ~/.hermes's claude_proxy.py source (2026-07-31): its
# main() sets `DEBUG_LOG = f"/Users/c/Library/Logs/claude-proxy-{port}-debug.log"`
# — _dlog() has never written to ~/.hermes/logs/. That directory does not
# exist on disk. The template below previously pointed at it, which meant
# `sync_proxy_trace_verdicts`'s `if not path.exists(): return 0` fired on
# every poll tick for every port — this was a dead module, not a live one
# with a rotation gap. Fixed to match the real write path.
DEBUG_LOG_TEMPLATE = "~/Library/Logs/claude-proxy-{port}-debug.log"

# Bound on the "already seen" read in sync_proxy_trace_verdicts — the debug
# log has no rotation, but a claude_proxy.py instance realistically produces
# low hundreds of records between restarts, not tens of thousands; this is
# generous headroom, not a tight fit.
_ALREADY_SEEN_LIMIT = 10_000

# --- Decision: not reading rotated .N.gz archives (2026-07-31) ---
#
# `~/Library/Logs/logrotate.sh` (run daily via the `local.hermes.logrotate`
# launchd job) rotates a fixed, explicit list of filenames once they exceed
# 512KB, keeping 5 gzip archives per name. That list is: claude-proxy.log,
# claude-proxy-err.log, claude-proxy-poe.log, claude-proxy-poe-err.log,
# claude-proxy-council.log, claude-proxy-council-err.log,
# claude-proxy-debug.log, claude-proxy-research.log,
# claude-proxy-research-err.log. The per-port debug logs this module reads
# — claude-proxy-{port}-debug.log, e.g. claude-proxy-9998-debug.log — are
# NOT in that list under any name. Verified directly against the script
# and against disk: `find ~/Library/Logs -iname 'claude-proxy-9998-debug.log*'`
# returns exactly one file (the live, uncompressed log), no .0.gz/.1.gz/etc.
# The rotation job simply does not touch these files today.
#
# Net effect: there is currently no rotated-archive history to recover for
# ANY port, so .gz-reading code here would be speculative complexity for an
# event that doesn't occur under the current logrotate.sh. The real defect
# blocking visibility was `DEBUG_LOG_TEMPLATE` pointing at a nonexistent
# directory (fixed above) — that silently zeroed out ALL proxy-trace
# ingestion, live traffic included, for every port, which is a strictly
# bigger gap than "rotated archives are unreachable."
#
# Tradeoff being accepted: if `logrotate.sh` is ever extended to rotate
# these per-port files (an easy one-line addition to its `rotate` calls,
# given it's already the launchd job responsible for their 600-permission
# enforcement), this module will start silently losing whatever has aged
# past the live file's window — same failure mode the task description
# assumed was already happening. That is the condition under which
# .gz-reading becomes worth building; it is not the condition today.
# Revisit this decision if/when logrotate.sh's rotate list changes.

_BASE_URL_PORT_RE = re.compile(r"^https?://(?:127\.0\.0\.1|localhost):(\d+)/?")

# Per-port --allowedTools grants, used only for the UNEXPECTED_TOOL
# conformance check below (Scope: "compare against the port's configured
# grant"). A name matches if it equals a grant entry exactly or falls under
# a "prefix__*" wildcard grant.
# ponytail: hardcoded from the plists' actual --allowedTools args (confirmed
# 2026-07-31), not read live — drifts silently if a plist's grant changes
# without this dict being updated. Upgrade path: parse each
# ai.hermes.claude-proxy-*.plist's ProgramArguments if this ever causes a
# false UNEXPECTED_TOOL.
PORT_ALLOWED_TOOLS: dict[int, list[str]] = {
    9998: ["Read", "Write", "WebSearch", "mcp__mycroft__*", "mcp__email__*"],
    9997: ["Read", "Write", "mcp__mycroft__curation_*"],
}


def _tool_name_in_grant(name: str, grant: list[str]) -> bool:
    for entry in grant:
        if entry.endswith("*"):
            if name.startswith(entry[:-1]):
                return True
        elif name == entry:
            return True
    return False


def _find_session_transcripts(session_id: str) -> list[Path]:
    """Locate a session's top-level transcript(s) under ``~/.claude/projects/*/``.

    Never assume a fixed project directory (port 9999 has no
    ``WorkingDirectory`` and lands under ``~/.claude/projects/-/`` — see
    System State). Returns every match; caller picks most-recent on >1.
    """
    root = Path("~/.claude/projects").expanduser()
    if not root.is_dir():
        return []
    return sorted(root.glob(f"*/{session_id}.jsonl"))


def _find_subagent_transcripts(session_id: str) -> list[Path]:
    root = Path("~/.claude/projects").expanduser()
    if not root.is_dir():
        return []
    return sorted(root.glob(f"*/{session_id}/subagents/*.jsonl"))


def _extract_tool_calls_from_jsonl(path: Path) -> list[tuple[str, bool]]:
    """Return ``(tool_name, denied)`` pairs found in one transcript file.

    Malformed/truncated lines are skipped, not fatal — same per-line
    tolerance as ``parse_full_output_records``.
    """
    out: list[tuple[str, bool]] = []
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        message = rec.get("message") or {}
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            name = block.get("name")
            if not isinstance(name, str) or not name:
                continue
            denied = bool(rec.get("permission_denial") or block.get("permission_denial"))
            out.append((name, denied))
    return out


def collect_session_tool_names(session_id: str) -> tuple[list[tuple[str, bool]], str]:
    """Locate *session_id*'s transcript(s) (top-level + subagents) and
    extract ``(tool_name, denied)`` pairs, unioned across all matches.

    Returns ``(calls, lookup_status)`` where *lookup_status* is one of
    ``"ok"``, ``"not_found"`` — the caller maps an empty ``session_id`` to
    ``"skipped_no_session_id"`` before ever calling this (see Failure
    Handling: that case must not run a filesystem search at all).
    """
    matches = _find_session_transcripts(session_id)
    if not matches:
        return [], "not_found"
    if len(matches) > 1:
        chosen = max(matches, key=lambda p: p.stat().st_mtime)
        log.warning(
            "proxy_trace.multiple_session_matches",
            session_id=session_id,
            paths=[str(m) for m in matches],
        )
    else:
        chosen = matches[0]

    calls = _extract_tool_calls_from_jsonl(chosen)
    for sub_path in _find_subagent_transcripts(session_id):
        calls.extend(_extract_tool_calls_from_jsonl(sub_path))
    return calls, "ok"

# Tracks the debug log's byte size as of the last `sync_proxy_trace_verdicts`
# call, keyed by resolved path string. Lets that function tell "the log grew
# but nothing parsed out of it" (upstream format drift — worth a warning)
# apart from "the log hasn't grown at all" (no new proxied traffic since the
# last poll tick — a normal, silent no-op). Process-lifetime only: a restart
# re-baselines from 0, which at worst produces one extra warning check on the
# first tick rather than a missed one.
_last_log_size: dict[str, int] = {}


def port_from_base_url(base_url: str) -> int | None:
    """Extract the port from a ``http://127.0.0.1:<port>/v1`` style base_url.

    Returns ``None`` for anything not pointed at localhost — this ingester
    only ever reads local ``claude_proxy.py`` debug logs, never a remote
    backend's traffic.
    """
    m = _BASE_URL_PORT_RE.match(base_url.strip())
    return int(m.group(1)) if m else None


def debug_log_path(port: int) -> Path:
    return Path(DEBUG_LOG_TEMPLATE.format(port=port)).expanduser()


def parse_full_output_records(log_text: str) -> list[dict[str, Any]]:
    """Extract every well-formed ``full_output`` JSON record from *log_text*.

    Malformed lines (a truncated write, a future format change) are logged
    and skipped rather than aborting the whole parse — one bad line must
    not hide every other session's signal.
    """
    records: list[dict[str, Any]] = []
    for match in _FULL_OUTPUT_RE.finditer(log_text):
        raw = match.group(1)
        try:
            records.append(json.loads(raw))
        except json.JSONDecodeError:
            log.warning("proxy_trace.parse_error", raw_prefix=raw[:120])
    return records


def _risk_and_recommendation(
    num_turns: int, permission_denials: list[Any]
) -> tuple[str, str]:
    """Advisory-only heuristic — see module docstring's tier note.

    Not a live pre-approval gate (the session already completed by the
    time this runs), just a coarse "does a human want to look at this"
    signal:

    - Any recorded permission denial → high risk, flagged for review even
      though the underlying CLI run already finished — a denial means the
      subprocess's own permission layer refused something, which is
      exactly the kind of event this module exists to surface.
    - ``num_turns`` at or above the judge's own per-call turn budget
      (``_HIGH_TURN_THRESHOLD``) → medium risk, flagged for review — a
      long tool-use chain inside an otherwise-opaque subprocess.
    - Otherwise → low risk, approved retroactively (informational).
    """
    if permission_denials:
        return "high", "review"
    if num_turns >= _HIGH_TURN_THRESHOLD:
        return "medium", "review"
    return "low", "approve"


def verdict_row_from_record(
    record: dict[str, Any], *, alias: str, port: int
) -> dict[str, Any]:
    """Build the ``create_intent_verdict``/``upsert_intent_verdict`` kwargs
    for one proxy-trace record. Pure function — no I/O, easy to unit test
    against a captured log line.
    """
    num_turns = record.get("num_turns") or 0
    permission_denials = record.get("permission_denials") or []
    usage = record.get("usage") or {}
    iterations = usage.get("iterations") or []
    session_id = record.get("session_id") or ""
    call_id = session_id or record.get("uuid") or ""
    verdict_key = record.get("uuid") or session_id
    risk_level, recommendation = _risk_and_recommendation(num_turns, permission_denials)

    evidence = [
        f"num_turns={num_turns}",
        f"iterations={len(iterations)}",
        f"permission_denials={len(permission_denials)}",
    ]
    if "total_cost_usd" in record:
        evidence.append(f"total_cost_usd={record['total_cost_usd']}")
    if record.get("stop_reason"):
        evidence.append(f"stop_reason={record['stop_reason']}")

    # Per-tool-call name visibility (computed at first-INSERT time only —
    # upsert_intent_verdict's ON CONFLICT clause silently drops `evidence`
    # on any later UPDATE, see module docstring / Scope in the task spec).
    if not session_id:
        evidence.append("tool_names_lookup=skipped_no_session_id")
    else:
        calls, lookup_status = collect_session_tool_names(session_id)
        if lookup_status == "not_found":
            evidence.append("tool_names_lookup=not_found")
        else:
            names = sorted({name for name, _denied in calls})
            evidence.append(f"tool_names={json.dumps(names)}")
            denied_names = sorted({name for name, denied in calls if denied})
            if denied_names:
                evidence.append(f"denied_tool_names={json.dumps(denied_names)}")
            grant = PORT_ALLOWED_TOOLS.get(port)
            if grant is not None:
                unexpected = sorted(n for n in names if not _tool_name_in_grant(n, grant))
                for name in unexpected:
                    evidence.append(f"UNEXPECTED_TOOL:{name}")
                if unexpected:
                    risk_level, recommendation = "high", "review"

    intent_summary = (
        f"Proxied claude-subscription session via port {port}: "
        f"{num_turns} turn(s), {len(permission_denials)} permission denial(s)."
    )
    reasoning = (
        "Retrospective, session-level verdict derived from claude_proxy.py's "
        "full_output log — not a live pre-approval judgment (the proxied "
        "session already completed). Risk is derived from num_turns and "
        f"permission_denials only. {'; '.join(evidence)}."
    )

    return {
        "verdict_id": f"proxytrace-{verdict_key}",
        "ws_id": f"proxy:{port}",
        "call_id": call_id,
        "func_name": "claude_proxy_session",
        "func_args": json.dumps({"alias": alias, "port": port}),
        "intent_summary": intent_summary,
        "risk_level": risk_level,
        "confidence": 0.6,
        "recommendation": recommendation,
        "reasoning": reasoning,
        "evidence": json.dumps(evidence),
        "tier": "proxy_trace",
        "judge_model": "proxy-trace-heuristic",
        "latency_ms": int(record.get("duration_ms") or 0),
        "user_decision": "pending",
    }


def sync_proxy_trace_verdicts(storage: Any, *, alias: str, base_url: str) -> int:
    """Read the debug log for the backend at *base_url* (if any) and upsert
    one advisory verdict per session it recorded.

    Returns the number of NEW records written this call (0 if the backend
    isn't a local claude_proxy.py instance, its log file doesn't exist yet,
    or every record already has a row — all silent no-ops, not errors,
    since most Turnstone deployments won't have this file at all).

    The debug log has no rotation (``claude_proxy.py``'s ``_dlog`` is a bare
    append) and this runs on a polling interval for the life of the
    process, so a naive "upsert every record every tick" would issue one
    write transaction per record per tick forever against the same SQLite
    file the rest of the server uses — on a long-lived log this becomes a
    meaningful, silently-growing write load for zero new information.
    Instead this reads the verdict_ids already recorded for this port
    first (one read) and only upserts records that are genuinely new (see
    ``_ALREADY_SEEN_LIMIT`` for the read's bound). Upserting (rather than a
    plain insert) still keys off a deterministic ``verdict_id`` derived
    from the claude CLI's own ``uuid``/``session_id``, so a record that
    slips past the "already seen" check (e.g. a first run against a log
    with more history than the read's limit) is still idempotent, just not
    write-free.

    Also warns (``proxy_trace.zero_parsed_on_growth``) when the log file is
    non-empty and has grown since the previous call to this function but
    zero ``full_output`` records were parsed from it — the signature of
    ``_FULL_OUTPUT_RE`` silently no longer matching an upstream log-line
    format change. A log that simply hasn't grown (no new proxied traffic
    since the last poll tick) stays silent — that path is normal, not an
    error.
    """
    port = port_from_base_url(base_url)
    if port is None:
        return 0
    path = debug_log_path(port)
    if not path.exists():
        return 0
    try:
        text = path.read_text(errors="replace")
    except OSError:
        log.warning("proxy_trace.read_error", path=str(path))
        return 0

    path_key = str(path)
    previous_size = _last_log_size.get(path_key, 0)
    current_size = len(text)
    grew = current_size > previous_size
    _last_log_size[path_key] = current_size

    known_ids = {
        row["verdict_id"]
        for row in storage.list_intent_verdicts(
            ws_id=f"proxy:{port}", limit=_ALREADY_SEEN_LIMIT
        )
    }

    records = parse_full_output_records(text)
    if grew and not records and text.strip():
        log.warning(
            "proxy_trace.zero_parsed_on_growth",
            path=path_key,
            previous_size=previous_size,
            current_size=current_size,
        )

    count = 0
    for record in records:
        # Cheap dedup key, computed WITHOUT calling verdict_row_from_record
        # (which now does a filesystem glob for tool names) — checking
        # known_ids first is required so an already-processed record never
        # re-globs ~/.claude/projects/*/ on every 30s poll tick forever.
        verdict_key = record.get("uuid") or record.get("session_id") or ""
        if f"proxytrace-{verdict_key}" in known_ids:
            continue
        row = verdict_row_from_record(record, alias=alias, port=port)
        if row["verdict_id"] in known_ids:
            continue
        try:
            storage.upsert_intent_verdict(**row)
            count += 1
        except Exception:
            log.warning(
                "proxy_trace.write_error", verdict_id=row["verdict_id"], exc_info=True
            )
    return count


class ProxyTraceWatcher:
    """Background poller: periodically syncs proxy-trace verdicts for a set
    of (alias, base_url) targets.

    Deliberately a plain polling loop on a daemon thread, not a filesystem
    watch or a tail — the debug logs are small (tens of KB) and the sync
    function re-parses the whole file every tick (writing only genuinely
    new records — see ``sync_proxy_trace_verdicts``), so a missed inotify
    event or a restart can never lose a record; the read side is
    stateless. ``daemon=True`` means the thread never blocks process exit,
    so there is no explicit ``stop()`` wired into the ASGI lifespan (kept
    intentionally out of scope — this loop only reads a proxy's own log and
    writes advisory rows; it holds no resources that need a clean drain).
    """

    def __init__(
        self,
        storage: Any,
        targets: list[tuple[str, str]],
        *,
        interval_seconds: float = 30.0,
    ) -> None:
        self._storage = storage
        self._targets = targets
        self._interval = interval_seconds
        self._thread: Any = None
        self._stop = False

    def _run(self) -> None:
        import time

        while not self._stop:
            for alias, base_url in self._targets:
                try:
                    n = sync_proxy_trace_verdicts(self._storage, alias=alias, base_url=base_url)
                    if n:
                        log.info("proxy_trace.synced", alias=alias, count=n)
                except Exception:
                    log.warning("proxy_trace.sync_error", alias=alias, exc_info=True)
            time.sleep(self._interval)

    def start(self) -> None:
        if not self._targets:
            return
        import threading

        self._thread = threading.Thread(
            target=self._run, name="proxy-trace-watcher", daemon=True
        )
        self._thread.start()
        log.info(
            "proxy_trace.watcher_started",
            targets=[a for a, _ in self._targets],
            interval=self._interval,
        )

    def stop(self) -> None:
        self._stop = True
