"""Pinned-sender guard (user_personas) in the Matrix bot, 2026-09-29.

Sana's messages must only ever reach her isolated persona. These pin the
fail-closed paths: foreign workstream, /model, and a route reset where
someone else posts first in her room.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from turnstone.channels.matrix.bot import TurnstoneMatrixBot
from turnstone.channels.matrix.config import MatrixConfig

SANA = "@sana:matrix.local"
CHOPP = "@chopp:matrix.local"
ROOM = "!sana-room:matrix.local"


def _bot(tmp_path, route_ws=None):  # type: ignore[no-untyped-def]
    config = MatrixConfig(
        auto_approve=False,
        user_id="@turnstone:matrix.local",
        store_path=str(tmp_path),
        user_personas={SANA: "sana"},
    )
    with patch("turnstone.channels.matrix.bot.httpx.AsyncClient", return_value=AsyncMock()):
        bot = TurnstoneMatrixBot(config, server_url="http://x", storage=AsyncMock())
    bot._client = AsyncMock()
    bot.router = AsyncMock()
    bot.router.get_or_create_workstream.return_value = ("ws-new", True)
    bot.subscribe_ws = AsyncMock()
    bot.unsubscribe_ws = AsyncMock()
    bot._send_text = AsyncMock()
    bot._get_room_ws = AsyncMock(return_value=route_ws)
    return bot


def _msg(bot, sender, text):  # type: ignore[no-untyped-def]
    room = SimpleNamespace(room_id=ROOM, display_name="Anything")
    asyncio.run(bot._on_message(room, SimpleNamespace(sender=sender, body=text)))


def test_first_message_creates_pinned_sana_workstream(tmp_path) -> None:
    bot = _bot(tmp_path)
    _msg(bot, SANA, "hello")
    assert bot.router.get_or_create_workstream.call_args.kwargs["model"] == "sana"
    assert bot._load_pins() == {ROOM: {"alias": "sana", "ws_id": "ws-new"}}
    bot.router.send_message.assert_awaited_once_with("ws-new", "hello")


def test_sana_refused_in_room_with_foreign_workstream(tmp_path) -> None:
    bot = _bot(tmp_path, route_ws="ws-poe")
    _msg(bot, SANA, "hello")
    bot.router.send_message.assert_not_awaited()
    bot.router.get_or_create_workstream.assert_not_awaited()


def test_sana_cannot_model_switch(tmp_path) -> None:
    bot = _bot(tmp_path)
    _msg(bot, SANA, "/model poe")
    bot.router.get_or_create_workstream.assert_not_awaited()
    bot.router.send_message.assert_not_awaited()


def test_nobody_can_model_switch_a_pinned_room(tmp_path) -> None:
    bot = _bot(tmp_path, route_ws="ws-new")
    bot._save_pins({ROOM: {"alias": "sana", "ws_id": "ws-new"}})
    _msg(bot, CHOPP, "/model poe")
    bot.router.delete_route.assert_not_awaited()
    bot.router.get_or_create_workstream.assert_not_awaited()


def test_route_reset_keeps_pinned_alias_when_other_user_posts_first(tmp_path) -> None:
    bot = _bot(tmp_path)  # route cleared (e.g. server restart)
    bot._save_pins({ROOM: {"alias": "sana", "ws_id": "ws-old"}})
    bot.router.get_or_create_workstream.return_value = ("ws-2", True)
    _msg(bot, CHOPP, "hi")
    assert bot.router.get_or_create_workstream.call_args.kwargs["model"] == "sana"
    assert bot._load_pins()[ROOM]["ws_id"] == "ws-2"
    _msg(bot, SANA, "still me")  # her next message goes to the re-pinned ws
    bot._get_room_ws.return_value = "ws-2"
    _msg(bot, SANA, "again")
    bot.router.send_message.assert_any_await("ws-2", "again")
