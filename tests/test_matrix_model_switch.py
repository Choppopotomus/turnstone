"""/model <alias> seed-on-create path (Matrix bot, 2026-09-30).

The switch must carry the old workstream's conversation into the new
model's workstream, and must leave the room on its old workstream when
seeding fails.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from turnstone.channels.matrix import seed as seed_mod
from turnstone.channels.matrix.bot import TurnstoneMatrixBot
from turnstone.channels.matrix.config import MatrixConfig
from turnstone.channels.matrix.seed import prepare_seed_messages

ROOM = "!room:matrix.local"
CHOPP = "@chopp:matrix.local"

EXPORTED = [
    {"role": "system", "content": "You are Poe."},
    {"role": "user", "content": "My dog's name is Biscuit."},
    {
        "role": "assistant",
        "content": None,
        "tool_calls": [{"id": "c1", "type": "function", "function": {"name": "x"}}],
    },
    {"role": "tool", "tool_call_id": "c1", "content": "tool output"},
    {"role": "assistant", "content": [{"type": "text", "text": "Noted: Biscuit."}]},
]


class FakeStorage:
    """Minimal in-memory stand-in for the workstream storage API."""

    def __init__(self) -> None:
        self.workstreams: dict[str, dict] = {"ws-old": {"ws_id": "ws-old", "user_id": "u1"}}
        self.configs: dict[str, dict] = {"ws-old": {"model_alias": "local", "model": "q"}}
        self.messages: dict[str, list] = {}
        self.deleted: list[str] = []

    def get_workstream(self, ws_id):  # type: ignore[no-untyped-def]
        return self.workstreams.get(ws_id)

    def register_workstream(self, ws_id, **kw):  # type: ignore[no-untyped-def]
        self.workstreams[ws_id] = {"ws_id": ws_id, **kw}

    def load_workstream_config(self, ws_id):  # type: ignore[no-untyped-def]
        return self.configs.get(ws_id, {})

    def save_workstream_config(self, ws_id, config):  # type: ignore[no-untyped-def]
        self.configs[ws_id] = config

    def save_messages_bulk(self, rows):  # type: ignore[no-untyped-def]
        for r in rows:
            self.messages.setdefault(r["ws_id"], []).append(r)

    def delete_workstream(self, ws_id):  # type: ignore[no-untyped-def]
        self.deleted.append(ws_id)
        return True


def _bot(storage):  # type: ignore[no-untyped-def]
    config = MatrixConfig(auto_approve=False, user_id="@turnstone:matrix.local")
    with patch("turnstone.channels.matrix.bot.httpx.AsyncClient", return_value=AsyncMock()):
        bot = TurnstoneMatrixBot(config, server_url="http://x", storage=storage)
    bot._client = AsyncMock()
    bot.router = AsyncMock()
    bot.subscribe_ws = AsyncMock()
    bot.unsubscribe_ws = AsyncMock()
    bot._send_text = AsyncMock()
    bot._get_room_ws = AsyncMock(return_value="ws-old")
    bot._pins_path = MagicMock(
        return_value=MagicMock(read_text=MagicMock(side_effect=FileNotFoundError))
    )
    return bot


def _switch(bot, alias="poe"):  # type: ignore[no-untyped-def]
    room = SimpleNamespace(room_id=ROOM, display_name="Anything")
    asyncio.run(bot._on_message(room, SimpleNamespace(sender=CHOPP, body=f"/model {alias}")))


def test_prepare_drops_tool_traffic_and_flattens() -> None:
    out = prepare_seed_messages(EXPORTED, 10_000)
    assert out == [
        {"role": "system", "content": "You are Poe."},
        {"role": "user", "content": "My dog's name is Biscuit."},
        {"role": "assistant", "content": "Noted: Biscuit."},
    ]


def test_prepare_truncates_oldest_keeps_system_and_starts_on_user() -> None:
    msgs = [{"role": "system", "content": "S"}]
    for i in range(10):
        msgs += [
            {"role": "user", "content": f"u{i}" * 10},
            {"role": "assistant", "content": f"a{i}" * 10},
        ]
    out = prepare_seed_messages(msgs, 1 + 20 * 3)  # system + 3 messages' worth
    assert out[0] == {"role": "system", "content": "S"}
    assert out[1]["role"] == "user"
    assert out[-1]["content"] == "a9" * 10
    assert len(out) == 3  # system, u9, a9 (a8 dropped so it starts on user)


def test_switch_seeds_new_workstream_then_swaps_route() -> None:
    storage = FakeStorage()
    bot = _bot(storage)
    bot.router.create_forked_workstream.return_value = ("ws-new", True, 2)
    with patch.object(seed_mod, "export_messages", return_value=EXPORTED):
        _switch(bot)

    kw = bot.router.create_forked_workstream.call_args.kwargs
    scratch = kw["resume_ws"]
    assert kw["model"] == "poe" and scratch
    # Scratch carried the portable transcript and the NEW alias.
    assert [m["content"] for m in storage.messages[scratch]] == [
        "You are Poe.",
        "My dog's name is Biscuit.",
        "Noted: Biscuit.",
    ]
    assert storage.configs[scratch] == {"model_alias": "poe"}
    assert storage.deleted == [scratch]
    bot.router.delete_route.assert_awaited_once_with("matrix", ROOM)
    bot.router.create_route.assert_awaited_once_with("matrix", ROOM, "ws-new")
    bot.unsubscribe_ws.assert_awaited_once_with("ws-old")
    bot.subscribe_ws.assert_awaited_once_with("ws-new", ROOM)
    assert "Carried over 3" in bot._send_text.call_args.args[1]


def test_switch_keeps_old_route_when_seed_not_applied() -> None:
    storage = FakeStorage()
    bot = _bot(storage)
    bot.router.create_forked_workstream.return_value = ("ws-new", False, 0)
    with patch.object(seed_mod, "export_messages", return_value=EXPORTED):
        _switch(bot)

    bot.router.delete_route.assert_not_awaited()
    bot.router.create_route.assert_not_awaited()
    bot.unsubscribe_ws.assert_not_awaited()
    bot.router.close_workstream.assert_awaited_once_with("ws-new")
    assert len(storage.deleted) == 1  # scratch cleaned up
    assert "still on the previous model" in bot._send_text.call_args.args[1]


def test_switch_keeps_old_route_when_export_fails() -> None:
    storage = FakeStorage()
    bot = _bot(storage)
    with patch.object(seed_mod, "export_messages", side_effect=RuntimeError("boom")):
        _switch(bot)
    bot.router.create_forked_workstream.assert_not_awaited()
    bot.router.delete_route.assert_not_awaited()
    assert "still on the previous model" in bot._send_text.call_args.args[1]
