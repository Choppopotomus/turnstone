"""Seed-on-create for the Matrix ``/model <alias>`` switch.

Turnstone has no in-place "change this workstream's model" op, and a
fork-resume of the room's current workstream would restore that
workstream's persisted ``model_alias`` (Session.resume reads it from the
source config even when forking).  So the switch goes through a scratch
workstream:

1. Export the outgoing workstream with the same serializer
   ``turnstone-admin export`` uses (:func:`turnstone.core.export.export_workstream`).
2. Reduce it to plain user/assistant/system text the new backend will
   accept (tool calls and tool results dropped, content parts flattened)
   and truncate oldest-first to fit the new model's context, keeping any
   system messages.
3. Write it to a scratch workstream whose config is the old config with
   ``model_alias`` set to the new alias.
4. The caller creates the real workstream with ``resume_ws=<scratch>``,
   which the server fork-copies (and persists) under the new ws_id, then
   deletes the scratch row.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from turnstone.core.export import export_workstream

if TYPE_CHECKING:
    from turnstone.core.storage._protocol import StorageBackend

# Rough chars-per-token for budget estimates (same order as Session's
# default _chars_per_token).  Budget uses half the window so the new
# model still has room for its own system prompt, tools, and a reply.
_CHARS_PER_TOKEN = 4
_CONTEXT_FRACTION = 0.5
_DEFAULT_CONTEXT_TOKENS = 100_000
# Known [models.*] context_window values from ~/.config/turnstone/config.toml.
# Aliases absent here (the claude -p proxies) fall back to the default.
MODEL_CONTEXT_TOKENS: dict[str, int] = {"local": 32_768}


def char_budget_for(alias: str) -> int:
    tokens = MODEL_CONTEXT_TOKENS.get(alias, _DEFAULT_CONTEXT_TOKENS)
    return int(tokens * _CHARS_PER_TOKEN * _CONTEXT_FRACTION)


def _flatten_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(str(part.get("text", "")))
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join(p for p in parts if p)
    return str(content)


def prepare_seed_messages(messages: list[dict[str, Any]], max_chars: int) -> list[dict[str, str]]:
    """Reduce an exported transcript to backend-portable text messages.

    - ``tool`` messages are dropped; ``tool_calls`` are stripped from
      assistant messages (an assistant turn that was only a tool call is
      dropped).  A different backend may reject foreign tool-call ids.
    - Content-part lists are flattened to their text parts.
    - Oldest non-system messages are dropped until the total fits
      *max_chars*; system messages are always kept, at the front.
    - The kept conversation starts on a user message.
    """
    system: list[dict[str, str]] = []
    convo: list[dict[str, str]] = []
    for msg in messages:
        role = msg.get("role")
        if role not in ("system", "user", "assistant"):
            continue
        text = _flatten_content(msg.get("content")).strip()
        if not text:
            continue
        (system if role == "system" else convo).append({"role": role, "content": text})

    budget = max_chars - sum(len(m["content"]) for m in system)
    kept: list[dict[str, str]] = []
    used = 0
    for msg in reversed(convo):
        size = len(msg["content"])
        if used + size > budget:
            break
        kept.append(msg)
        used += size
    kept.reverse()
    while kept and kept[0]["role"] != "user":
        kept.pop(0)
    return system + kept


def export_messages(storage: StorageBackend, ws_id: str) -> list[dict[str, Any]]:
    """Export *ws_id* via the turnstone-admin ``export`` serializer."""
    result = export_workstream(storage, ws_id)
    return list(json.loads(result.data)["messages"])


def build_seed_workstream(
    storage: StorageBackend, old_ws_id: str, alias: str
) -> tuple[str | None, int]:
    """Create a scratch workstream holding *old_ws_id*'s portable history.

    Returns ``(scratch_ws_id, seeded_count)``; ``(None, 0)`` when there is
    nothing worth carrying over.  Raises on storage/export failure.
    """
    seed = prepare_seed_messages(export_messages(storage, old_ws_id), char_budget_for(alias))
    if not any(m["role"] != "system" for m in seed):
        return None, 0
    old_row = storage.get_workstream(old_ws_id) or {}
    scratch = uuid.uuid4().hex
    storage.register_workstream(
        scratch,
        name=f"model-switch-seed-{alias}",
        state="closed",
        user_id=old_row.get("user_id") or None,
    )
    config = dict(storage.load_workstream_config(old_ws_id) or {})
    config.pop("model", None)
    config["model_alias"] = alias
    storage.save_workstream_config(scratch, config)
    storage.save_messages_bulk([{"ws_id": scratch, **m} for m in seed])
    return scratch, len(seed)
