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
Known gap, not yet fixed: `[models.council]` still has the same missing
`api_key` / wrong `model` sentinel bugs originally found alongside this on
`[models.poe]` — the council alias itself was never patched, only its
model-family sibling. `[models.council]`'s new bridge support is otherwise
in place (same shared code path) once that's fixed.
