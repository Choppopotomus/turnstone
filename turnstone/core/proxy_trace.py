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

DEBUG_LOG_TEMPLATE = "~/.hermes/logs/claude-proxy-{port}-debug.log"

# Bound on the "already seen" read in sync_proxy_trace_verdicts — the debug
# log has no rotation, but a claude_proxy.py instance realistically produces
# low hundreds of records between restarts, not tens of thousands; this is
# generous headroom, not a tight fit.
_ALREADY_SEEN_LIMIT = 10_000

_BASE_URL_PORT_RE = re.compile(r"^https?://(?:127\.0\.0\.1|localhost):(\d+)/?")


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

    known_ids = {
        row["verdict_id"]
        for row in storage.list_intent_verdicts(
            ws_id=f"proxy:{port}", limit=_ALREADY_SEEN_LIMIT
        )
    }

    count = 0
    for record in parse_full_output_records(text):
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
