"""Tests for turnstone.core.proxy_trace — the claude_proxy.py debug-log
ingester (see that module's docstring for the full architecture rationale).

Coverage here is deliberately narrow: pure-function parsing/row-building,
plus the one behavior this test file was added to lock in — the
"log grew but zero records parsed" warning that distinguishes a real
upstream format-drift signal from the normal "no new proxied traffic"
silent no-op.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from turnstone.core import proxy_trace


def _full_output_line(**fields: Any) -> str:
    payload = {
        "session_id": "sess-1",
        "uuid": "uuid-1",
        "num_turns": 2,
        "permission_denials": [],
        "usage": {"iterations": [1, 2]},
        **fields,
    }
    return f"    full_output: {json.dumps(payload)}\n"


class _FakeStorage:
    """Minimal stand-in for the real storage backend — just enough surface
    for sync_proxy_trace_verdicts (list_intent_verdicts / upsert_intent_verdict).
    """

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def list_intent_verdicts(self, *, ws_id: str, limit: int) -> list[dict[str, Any]]:
        return [row for row in self.rows.values() if row["ws_id"] == ws_id][:limit]

    def upsert_intent_verdict(self, **row: Any) -> None:
        self.rows[row["verdict_id"]] = row


@pytest.fixture(autouse=True)
def _reset_size_cache() -> None:
    """proxy_trace tracks last-seen log size in a module-level dict keyed by
    path — reset it between tests so runs don't leak state via a shared
    temp-file path collision or ordering.
    """
    proxy_trace._last_log_size.clear()
    yield
    proxy_trace._last_log_size.clear()


@pytest.fixture
def fake_log(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """Capture proxy_trace.log.warning(...) calls without depending on
    structlog/stdlib caplog plumbing.
    """
    calls: list[tuple[str, dict[str, Any]]] = []

    class _Stub:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        def info(self, event: str, **kwargs: Any) -> None:
            pass

    monkeypatch.setattr(proxy_trace, "log", _Stub())
    return calls


# ---------------------------------------------------------------------------
# Pure-function parsing
# ---------------------------------------------------------------------------


def test_parse_full_output_records_extracts_json() -> None:
    text = _full_output_line() + _full_output_line(session_id="sess-2", uuid="uuid-2")
    records = proxy_trace.parse_full_output_records(text)
    assert len(records) == 2
    assert records[0]["session_id"] == "sess-1"
    assert records[1]["session_id"] == "sess-2"


def test_parse_full_output_records_skips_malformed_json(fake_log) -> None:
    text = "    full_output: {not valid json}\n" + _full_output_line()
    records = proxy_trace.parse_full_output_records(text)
    assert len(records) == 1
    assert any(event == "proxy_trace.parse_error" for event, _ in fake_log)


def test_parse_full_output_records_empty_text_returns_empty() -> None:
    assert proxy_trace.parse_full_output_records("") == []
    assert proxy_trace.parse_full_output_records("no matching lines here\n") == []


def test_port_from_base_url() -> None:
    assert proxy_trace.port_from_base_url("http://127.0.0.1:8934/v1") == 8934
    assert proxy_trace.port_from_base_url("http://localhost:9000") == 9000
    assert proxy_trace.port_from_base_url("https://api.anthropic.com/v1") is None


def test_verdict_row_from_record_high_risk_on_permission_denial() -> None:
    record = json.loads(_full_output_line(permission_denials=["Bash"]).split(": ", 1)[1])
    row = proxy_trace.verdict_row_from_record(record, alias="claude-subscription", port=8934)
    assert row["risk_level"] == "high"
    assert row["recommendation"] == "review"
    assert row["tier"] == "proxy_trace"
    assert row["ws_id"] == "proxy:8934"


def test_verdict_row_from_record_low_risk_default() -> None:
    record = json.loads(_full_output_line(num_turns=1).split(": ", 1)[1])
    row = proxy_trace.verdict_row_from_record(record, alias="claude-subscription", port=8934)
    assert row["risk_level"] == "low"
    assert row["recommendation"] == "approve"


# ---------------------------------------------------------------------------
# sync_proxy_trace_verdicts — full path against a temp debug log file
# ---------------------------------------------------------------------------


def _sync(tmp_path: Path, text: str, storage: _FakeStorage, port: int = 8934) -> int:
    log_path = tmp_path / f"claude-proxy-{port}-debug.log"
    log_path.write_text(text)
    template_backup = proxy_trace.DEBUG_LOG_TEMPLATE
    proxy_trace.DEBUG_LOG_TEMPLATE = str(tmp_path / "claude-proxy-{port}-debug.log")
    try:
        return proxy_trace.sync_proxy_trace_verdicts(
            storage, alias="claude-subscription", base_url=f"http://127.0.0.1:{port}/v1"
        )
    finally:
        proxy_trace.DEBUG_LOG_TEMPLATE = template_backup


def test_sync_writes_one_verdict_per_record(tmp_path: Path, fake_log) -> None:
    storage = _FakeStorage()
    text = _full_output_line() + _full_output_line(session_id="sess-2", uuid="uuid-2")
    count = _sync(tmp_path, text, storage)
    assert count == 2
    assert len(storage.rows) == 2
    assert not any(event == "proxy_trace.zero_parsed_on_growth" for event, _ in fake_log)


def test_sync_no_log_file_is_silent_noop(tmp_path: Path, fake_log) -> None:
    storage = _FakeStorage()
    proxy_trace.DEBUG_LOG_TEMPLATE = str(tmp_path / "claude-proxy-{port}-debug.log")
    count = proxy_trace.sync_proxy_trace_verdicts(
        storage, alias="claude-subscription", base_url="http://127.0.0.1:9999/v1"
    )
    assert count == 0
    assert fake_log == []


def test_sync_second_call_with_no_growth_is_silent(tmp_path: Path, fake_log) -> None:
    """Same file contents on two consecutive polls (no new proxied traffic)
    must never fire the zero-parsed warning — that's the normal no-op path.
    """
    storage = _FakeStorage()
    text = _full_output_line()
    _sync(tmp_path, text, storage)
    fake_log.clear()
    count = _sync(tmp_path, text, storage)  # identical content, no growth
    assert count == 0  # already-seen verdict_id, nothing new written
    assert fake_log == []


def test_sync_warns_when_log_grows_with_only_unparseable_content(tmp_path: Path, fake_log) -> None:
    storage = _FakeStorage()
    _sync(tmp_path, _full_output_line(), storage)
    fake_log.clear()

    # File grows in size but the new content uses a different (unmatched)
    # log-line shape entirely, and we simulate the full parse coming back
    # empty by replacing the whole file with unmatched content that is
    # longer than before.
    grown_unmatched_text = "    output_blob: " + ("x" * 200) + "\n"
    count = _sync(tmp_path, grown_unmatched_text, storage)

    assert count == 0
    warnings = [kwargs for event, kwargs in fake_log if event == "proxy_trace.zero_parsed_on_growth"]
    assert len(warnings) == 1
    assert warnings[0]["path"].endswith("claude-proxy-8934-debug.log")
    assert warnings[0]["current_size"] > warnings[0]["previous_size"]


# ---------------------------------------------------------------------------
# Per-tool-call name visibility (TASKSPEC_close_mcp_tool_visibility_gap.md)
# ---------------------------------------------------------------------------


def _write_transcript(path: Path, tool_calls: list[tuple[str, bool]]) -> None:
    """Write a minimal Claude-Code-shaped JSONL transcript: one line per
    tool_use block, ``(name, denied)`` pairs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for name, denied in tool_calls:
        rec: dict[str, Any] = {
            "message": {"content": [{"type": "tool_use", "name": name}]}
        }
        if denied:
            rec["permission_denial"] = True
        lines.append(json.dumps(rec))
    path.write_text("\n".join(lines) + "\n")


def test_collect_session_tool_names_top_level(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "-Users-c-Claude"
    _write_transcript(project_dir / "sess-1.jsonl", [("Bash", False), ("Read", False)])
    monkeypatch.setattr(
        proxy_trace, "_find_session_transcripts",
        lambda sid: sorted((tmp_path / "-Users-c-Claude").glob(f"{sid}.jsonl")),
    )
    monkeypatch.setattr(proxy_trace, "_find_subagent_transcripts", lambda sid: [])
    calls, status = proxy_trace.collect_session_tool_names("sess-1")
    assert status == "ok"
    assert {n for n, _ in calls} == {"Bash", "Read"}


def test_collect_session_tool_names_merges_subagent_transcripts(tmp_path: Path, monkeypatch) -> None:
    top = tmp_path / "sess-2.jsonl"
    sub = tmp_path / "sess-2" / "subagents" / "agent-abc.jsonl"
    _write_transcript(top, [("Bash", False)])
    _write_transcript(sub, [("mcp__mycroft__career_upsert_application", False)])
    monkeypatch.setattr(proxy_trace, "_find_session_transcripts", lambda sid: [top])
    monkeypatch.setattr(proxy_trace, "_find_subagent_transcripts", lambda sid: [sub])
    calls, status = proxy_trace.collect_session_tool_names("sess-2")
    assert status == "ok"
    names = {n for n, _ in calls}
    assert names == {"Bash", "mcp__mycroft__career_upsert_application"}


def test_collect_session_tool_names_not_found(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(proxy_trace, "_find_session_transcripts", lambda sid: [])
    monkeypatch.setattr(proxy_trace, "_find_subagent_transcripts", lambda sid: [])
    calls, status = proxy_trace.collect_session_tool_names("sess-missing")
    assert calls == []
    assert status == "not_found"


def test_verdict_row_skips_lookup_when_session_id_absent(monkeypatch) -> None:
    called = {"n": 0}

    def _boom(sid: str) -> Any:
        called["n"] += 1
        return [], "ok"

    monkeypatch.setattr(proxy_trace, "collect_session_tool_names", _boom)
    record = json.loads(_full_output_line(session_id="", uuid="").split(": ", 1)[1])
    row = proxy_trace.verdict_row_from_record(record, alias="claude-subscription", port=9998)
    assert called["n"] == 0
    evidence = json.loads(row["evidence"])
    assert "tool_names_lookup=skipped_no_session_id" in evidence


def test_verdict_row_records_not_found_when_no_transcript(monkeypatch) -> None:
    monkeypatch.setattr(proxy_trace, "collect_session_tool_names", lambda sid: ([], "not_found"))
    record = json.loads(_full_output_line(session_id="sess-x", uuid="uuid-x").split(": ", 1)[1])
    row = proxy_trace.verdict_row_from_record(record, alias="claude-subscription", port=9998)
    evidence = json.loads(row["evidence"])
    assert "tool_names_lookup=not_found" in evidence


def test_verdict_row_flags_unexpected_tool_and_escalates_risk(monkeypatch) -> None:
    monkeypatch.setattr(
        proxy_trace,
        "collect_session_tool_names",
        lambda sid: ([("Read", False), ("mcp__unknown__frob", False)], "ok"),
    )
    record = json.loads(
        _full_output_line(session_id="sess-y", uuid="uuid-y", num_turns=1).split(": ", 1)[1]
    )
    row = proxy_trace.verdict_row_from_record(record, alias="claude-subscription", port=9998)
    evidence = json.loads(row["evidence"])
    assert "UNEXPECTED_TOOL:mcp__unknown__frob" in evidence
    assert row["risk_level"] == "high"
    assert row["recommendation"] == "review"


def test_malformed_transcript_line_skipped(tmp_path: Path) -> None:
    path = tmp_path / "sess-z.jsonl"
    good = json.dumps({"message": {"content": [{"type": "tool_use", "name": "Bash"}]}})
    path.write_text("not valid json\n" + good + "\n")
    calls = proxy_trace._extract_tool_calls_from_jsonl(path)
    assert calls == [("Bash", False)]


def test_sync_does_not_call_transcript_search_for_known_id(tmp_path: Path, fake_log, monkeypatch) -> None:
    """Regression guard: reordering known_ids ahead of verdict_row_from_record
    must stop the filesystem glob from re-running on already-seen records."""
    calls = {"n": 0}

    def _spy(sid: str) -> Any:
        calls["n"] += 1
        return [], "not_found"

    monkeypatch.setattr(proxy_trace, "collect_session_tool_names", _spy)

    storage = _FakeStorage()
    text = _full_output_line(session_id="sess-known", uuid="uuid-known")
    _sync(tmp_path, text, storage)  # first tick: new record, glob runs once
    assert calls["n"] == 1

    calls["n"] = 0
    count = _sync(tmp_path, text, storage)  # second tick: identical content
    assert count == 0
    assert calls["n"] == 0  # must NOT re-glob an already-known verdict_id
