# Local fork patches (Choppopotomus/turnstone)

Tracks every change made on top of upstream `turnstonelabs/turnstone` that
isn't intended to go back upstream. Check this file before syncing `origin/main`
into `fork/main` — each entry below is a diff surface a merge could disturb.

## External tool-call gating bridge (2026-08-09)

**Problem:** `claude_proxy.py` (Mycroft repo, `infra/claude_proxy.py`) wraps
`claude -p` as a Turnstone model backend for the `poe`/`council`/default
aliases. The CLI subprocess executes its own tool calls internally and
returns only final text — Turnstone's judge/approval gate (`judge.enabled`,
`smart_approvals`) never sees them. Live-verified 2026-08-09: a real file
read executed through Turnstone with zero `approve_request` event and zero
judge verdict, on every port backed by `claude_proxy.py`. Full finding:
`~/.claude/projects/-Users-c-Claude/memory/project_turnstone_matrix.md`.

**Fix:** bridge Turnstone's real approval machinery into a Claude Code
`PreToolUse` hook running inside the `claude -p` subprocess, so a tool call
made by the wrapped CLI blocks on the same human/judge decision a native
Turnstone tool call would.

**Files changed:**

- `turnstone/core/session.py` — the main-loop `create_streaming` call now
  passes `extra_headers={"X-Turnstone-Ws-Id": self.ws_id}` so the workstream
  id reaches the model backend on every request. Additive only — a provider
  that ignores unknown headers (most do) is unaffected.
- `turnstone/core/session_routes.py` — new route
  `POST /v1/api/workstreams/{ws_id}/external-tool-check`. Resolves the
  workstream, builds a synthetic tool-call dict from the caller's
  `tool_name`/`tool_input`, and calls the SAME `ws.session._safe_prepare_tool`
  → `ws.ui.approve_tools` path a normal in-process tool call uses — no new
  approval logic, full reuse of policy checks / human UI prompt / blocking
  wait (`_APPROVAL_WAIT_TIMEOUT = 3600`).
- `turnstone/core/config.py` / `config.toml` — new `[server]
  external_tool_check_secret` (shared secret, not a per-session JWT — the
  only caller is a trusted-local subprocess spawned by `claude_proxy.py`
  itself, never network-exposed).

**Not part of this fork** (lives in the Mycroft repo, not here):
`infra/claude_proxy.py` reads `X-Turnstone-Ws-Id`, exports it as
`TURNSTONE_WS_ID` for the subprocess, and — only when that var is present —
writes a per-invocation `.claude/settings.json` + hook script wiring
`PreToolUse` to call the new endpoint. Absent the header (every non-Turnstone
call — Poe/council's actual daily cron/session use), behavior is unchanged.

**Upstream-merge risk:** the `session.py` edit is a small, localized addition
inside one `create_streaming(...)` call — a conflicting upstream rewrite of
that call site is the main thing to watch for. The new route and config key
are additive (new file sections), low collision risk.

**Real bugs found and fixed during live testing (not hypothetical, both
reproduced live before the fix):**

1. First version of the route built its synthetic item via
   `ChatSession._safe_prepare_tool`, which validates against Turnstone's OWN
   native tool registry (`read_file`, `bash`, ...) — a different vocabulary
   than the wrapped CLI's (`Read`, `Write`, ...). An unrecognized name got
   tagged `error` + `needs_approval=False`, and `approve_tools` silently
   auto-passes error'd items — the exact failure mode this bridge exists to
   close, reproduced by the fix itself. Rebuilt the item dict directly
   instead, with `needs_approval` hardcoded `True`.
2. **Severe**: calling `ws.ui.approve_tools(items)` directly inside the
   `async def` handler froze the ENTIRE server — not just the one request,
   every workstream, `/health`, everything — for the whole approval wait,
   because `approve_tools` is a blocking call (`threading.Event.wait()`
   inside) run directly on the asyncio event loop. Confirmed live: a plain
   `/health` check hung until fixed. `make_approve_handler` above already
   wraps its own blocking call in `asyncio.to_thread` for this exact
   reason — missed that pattern on the first pass. Fixed by wrapping the
   `approve_tools` call the same way. **Anyone touching this route again:
   any call into `ws.ui`/`ws.session` from an async handler needs to go
   through `asyncio.to_thread`, no exceptions — this class of bug takes the
   whole server down, not just one request.**

**Verified live end-to-end, 2026-08-09** (not just unit-tested): full chain
proven twice — once against a throwaway scratch `claude_proxy.py` instance
(port 9995, before touching production), once against the real `poe` alias
on the real port 9998 after deploy. Both approve and deny paths confirmed:
a real `tool_pending` + `approve_request` SSE event fires, the tool call
genuinely blocks until a human decision lands (proved via an unguessable
marker string only readable after approval), and a denied call correctly
never executes. `/health` stays responsive throughout after the
`asyncio.to_thread` fix. All 4 live `claude_proxy.py` instances
(9996/9997/9998/9999) restarted onto `PROXY_VERSION = "2026-08-09.1"`;
normal non-Turnstone traffic (no `X-Turnstone-Ws-Id` header) confirmed
byte-identical to pre-patch behavior via a direct request + debug-log check.

**Status:** DEPLOYED and live-verified, 2026-08-09. Register task `ba85d3d4`.
`[models.council]`'s matching `api_key`/`model` sentinel bugs (same two bugs
originally found on `[models.poe]`) fixed same day — verified via a live
chat completion through port 9997 returning 200 with real content. No known
gaps remain.

## Independent effectiveness check — "executed" vs "effective" (2026-09-18)

**Problem:** the judge (heuristic + LLM) and `proxy_trace.py`'s per-tool-name
visibility (see previous entry) both answer "did a tool call happen and did
it look safe" — advisory, at-or-before-execution questions. Neither confirms
the delegated work actually fixed the need it was requested for. A proxied
session that runs `launchctl kickstart` and completes cleanly (few turns, no
permission denials) reads as `low`/`approve` under the existing heuristic
even when the fix silently failed — reproduced live during this task (see
Verification below): a genuinely broken fix (plist pointing at a
nonexistent binary) produced `num_turns=4, permission_denials=0`, which the
pre-existing `_risk_and_recommendation` heuristic alone would have scored
`low`/`approve`.

**Fix:** ported the same "independently-implemented second check" discipline
already proven in `~/.claude/skills/runbook-security-posture-check` /
`runbook-service-health` / `runbook-launchd-fleet-audit` — a check that does
NOT reuse the mechanism that performed the fix, with disagreement escalating
rather than silently clearing. Scoped to one concrete, real task class:
**launchd daemon restart/(re)start fixes** (a class of work these same
runbook skills already handle, and the only class implemented so far — see
`turnstone/core/effectiveness_check.py`'s module docstring for why other
classes need their own separate check function, not a universal one).

**Files changed:**

- `turnstone/core/effectiveness_check.py` (new) — `extract_launchd_targets()`
  parses a bash command for a launchd label the way real commands actually
  write it (bare label, `gui/<uid>/<label>`, a `.plist` path, or a
  `bootstrap <domain> <path>` pair — see the command-substitution note
  below). `check_launchd_daemon_alive()` is the independent check: reads the
  target's plist directly via `plistlib` (never `launchctl`) to recover its
  expected `ProgramArguments`, then cross-references the real process table
  via `ps -eo pid=,args=` (never `launchctl list`/`print`). Returns an
  `EffectivenessResult` with a `PASS`/`FAIL` verdict and evidence string.
- `turnstone/core/proxy_trace.py` — `_extract_bash_aware_tool_calls_from_jsonl()`
  extends the existing name-only transcript extraction to also surface a
  `Bash` call's raw `command` text (in-memory only; never persisted — the
  "names only" boundary from the MCP-visibility task still holds for the
  stored tool-name list itself, only the resolved PASS/FAIL tag is written).
  `collect_session_launchd_targets()` mirrors `collect_session_tool_names()`'s
  glob strategy to find every launchd label a session's bash calls (top-level
  + subagent transcripts) attempted to bring up. `verdict_row_from_record()`
  runs `check_launchd_daemon_alive()` for each target and appends
  `EFFECTIVENESS_CHECK:launchd:<label>:PASS|FAIL:<detail>` to `evidence`,
  escalating `risk_level`/`recommendation` to `high`/`review` on FAIL —
  same escalation shape already used for `UNEXPECTED_TOOL`. Computed at
  first-INSERT time only, same `ON CONFLICT`-drops-`evidence` constraint as
  the tool-name work above.

**Real bug found and fixed during live testing:** the first version of
`extract_launchd_targets()` tokenized on whitespace, which broke on the
real idiom `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/X.plist`
— the internal space inside `$(id -u)` split it into two tokens, capturing
`gui/$(id` as the "label". Fixed by collapsing whitespace inside any
`$(...)` substitution before tokenizing, plus an independent second pass
that scans for any `.plist` path anywhere in the command (needed for
`bootstrap`, where the verb-adjacent argument is a bare domain, not a
label — the label only appears in the plist path argument).

**Verified live, 2026-09-18, not just unit-tested against synthetic
fixtures** (`tests/test_effectiveness_check.py`, 11/11; `tests/test_proxy_trace.py`
new wiring tests, 32/32 combined; full existing suite unaffected):

1. Stood up a disposable KeepAlive launchd job (`com.turnstone.effectiveness-demo`,
   `/bin/sleep`) — the disposable-test-task pattern, not production
   infrastructure.
2. Stopped it for real (`launchctl bootout`), then ran a genuine live
   `claude -p --allowedTools Bash` subprocess (the same mechanism
   `claude_proxy.py` wraps) asking it to check and restart the job. It ran
   real bash calls (`launchctl kickstart -k gui/$(id -u)/...`), the daemon
   came back up for real (new PID confirmed via `pgrep`), and it self-reported
   success. Ran the real session transcript through `verdict_row_from_record()`:
   evidence correctly recorded `EFFECTIVENESS_CHECK:launchd:com.turnstone.effectiveness-demo:PASS:pid=<real pid> args matched via ps process-table scan`, risk stayed governed by other signals (not falsely escalated by this check).
3. **Disagreement case — the one that matters.** Replaced the plist with one
   pointing at a nonexistent binary, stopped the job, and ran the same real
   `claude -p` flow again. The agent behaved honestly (correctly diagnosed
   `EX_CONFIG`/exec failure and said outright it could not fix it) — but the
   *session-level* signal an unaugmented Turnstone would see
   (`num_turns=4, permission_denials=0`) reads as `low`/`approve` under the
   pre-existing heuristic alone. Running the real transcript through the
   patched `verdict_row_from_record()` correctly produced
   `EFFECTIVENESS_CHECK:launchd:com.turnstone.effectiveness-demo:FAIL:no live process found matching program='does-not-exist-binary' extra='100000'`
   and escalated the verdict to `high`/`review` — confirmed via a real,
   independent `ps` scan against the real (non-)running process, with zero
   `launchctl` calls anywhere in the check path.
4. Cleaned up: bootout the disposable job. `~/Library/LaunchAgents/com.turnstone.effectiveness-demo.plist`
   and the two ad hoc session JSONL transcripts under
   `~/.claude/projects/-Users-c-Claude/` were left in place for Chopp to
   remove (per the deletions-route-to-Chopp rule), not self-deleted.

**Known limitation, stated not hidden:** only the `launchd` task class has
an independent check. The wiring point (`verdict_row_from_record`) is
per-record and evidence-append, so a second task class (e.g. a
file-permission fix) would need its own `check_*` function in
`effectiveness_check.py` and its own bash-command pattern recognized in
`collect_session_launchd_targets`'s sibling extractor — not a drop-in
generalization, by design (see that module's docstring on why one universal
check is the wrong shape). This closes register task `7dd8ecc2` to the
extent scoped ("at least one real task class... demonstrated on a real
delegated run") — it does NOT close `78677f36` ("live-test narrow
silent-tier slice end-to-end, no Chopp involvement"), which remains blocked
on `1f1bf7bc` (the silent-tier scope decision, Chopp-owned,
`smart_approvals=false` still deliberately set) and is a separate,
later-phase milestone.

**Status:** DEPLOYED (fork-local, not upstream) and live-verified,
2026-09-18. Register task `7dd8ecc2`.
