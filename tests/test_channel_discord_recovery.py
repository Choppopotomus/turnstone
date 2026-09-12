"""Tests for the Discord channel adapter's missed-turn recovery machinery.

Ported from tests/test_channel_matrix.py (2026-09-12, register task
b2c353f8) alongside the corresponding bot.py port -- discord/bot.py had no
_recover_missed_turn/ConnectedEvent/StatusEvent/InProgressSnapshotEvent
handling at all before this. Test structure mirrors the matrix suite
closely so the two stay comparable; ROOM/room_id there maps to a mock
Discord thread object here (persistence keys off ``str(thread.id)``).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = [
    pytest.mark.filterwarnings(
        "ignore:.*'count' is passed as positional argument:DeprecationWarning"
    ),
    pytest.mark.filterwarnings(
        "ignore:.*'asyncio.iscoroutinefunction' is deprecated:DeprecationWarning"
    ),
]

discord = pytest.importorskip("discord")

from turnstone.sdk.events import (  # noqa: E402
    ConnectedEvent,
    ContentEvent,
    InProgressSnapshotEvent,
    StatusEvent,
    StreamEndEvent,
)


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _make_bot():
    from turnstone.channels.discord.bot import TurnstoneBot
    from turnstone.channels.discord.config import DiscordConfig

    storage = MagicMock()
    with patch("turnstone.channels.discord.bot.httpx.AsyncClient", return_value=AsyncMock()):
        bot = TurnstoneBot(
            DiscordConfig(bot_token="token-test"), "http://localhost:8080", storage
        )
    return bot


def _make_thread(thread_id: int = 111222333):
    thread = AsyncMock()
    thread.id = thread_id
    return thread


class TestInProgressSnapshotEventDiscord:
    def test_creates_streaming_message_from_snapshot(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        event = InProgressSnapshotEvent(ws_id="ws-1", content="partial reply so far")

        _run(bot._on_ws_event("ws-1", thread, event))

        assert "ws-1" in bot._streaming
        assert bot._streaming["ws-1"].accumulated_text == "partial reply so far"

    def test_replaces_rather_than_appends_to_existing_buffer(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        _run(bot._on_ws_event("ws-1", thread, ContentEvent(ws_id="ws-1", text="stale ")))
        assert bot._streaming["ws-1"].accumulated_text == "stale "

        event = InProgressSnapshotEvent(ws_id="ws-1", content="the real full text so far")
        _run(bot._on_ws_event("ws-1", thread, event))

        assert bot._streaming["ws-1"].accumulated_text == "the real full text so far"

    def test_empty_snapshot_is_noop(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        event = InProgressSnapshotEvent(ws_id="ws-1", content="")

        _run(bot._on_ws_event("ws-1", thread, event))

        assert "ws-1" not in bot._streaming

    def test_stream_end_finalizes_recovered_message(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        _run(
            bot._on_ws_event(
                "ws-1", thread, InProgressSnapshotEvent(ws_id="ws-1", content="recovered")
            )
        )
        _run(bot._on_ws_event("ws-1", thread, StreamEndEvent(ws_id="ws-1")))

        assert "ws-1" not in bot._streaming


class TestStreamingMessageReplaceDiscord:
    def test_replace_after_append_discards_prior_buffer(self) -> None:
        from turnstone.channels.discord.bot import StreamingMessage

        channel = MagicMock()
        channel.send = AsyncMock()
        sm = StreamingMessage(channel=channel, edit_interval=999.0)
        _run(sm.append("old "))
        _run(sm.append("content"))
        _run(sm.replace("new content"))

        assert sm.accumulated_text == "new content"


class TestMissedTurnRecoveryGatingDiscord:
    def test_first_connect_never_schedules_recovery(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_not_called()

    def test_reconnect_with_no_new_turns_does_not_schedule_recovery(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_not_called()

    def test_reconnect_with_new_turn_schedules_recovery(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once_with("ws-1", thread, 1)

    def test_is_reconnect_flag_does_not_re_trigger_on_later_ordinary_turns(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            bot._recover_missed_turn.assert_called_once()

            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=4))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=5))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once()

    def test_overlapping_reconnect_does_not_spawn_second_recovery(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))

            never_finishes: asyncio.Future[None] = asyncio.get_event_loop().create_future()

            async def _blocks_forever(*_a: object, **_k: object) -> None:
                await never_finishes

            bot._recover_missed_turn = AsyncMock(side_effect=_blocks_forever)  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            assert "ws-1" in bot._recovery_tasks

            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=4))
            await asyncio.sleep(0)

            never_finishes.cancel()
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once()


class TestRecoverMissedTurnDiscord:
    def test_skips_when_streaming_already_resumed(self) -> None:
        from turnstone.channels.discord.bot import StreamingMessage

        bot = _make_bot()
        thread = _make_thread()
        bot._streaming["ws-1"] = StreamingMessage(channel=thread)
        bot._http_client.get = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread))

        bot._http_client.get.assert_not_called()

    def test_posts_new_assistant_message_from_history(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": "the missed reply"},
                ]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread))

        bot._send_recovered_text.assert_awaited_once_with(thread, "the missed reply")
        assert bot._last_seen_text["ws-1"] == "the missed reply"

    def test_does_not_repost_already_seen_text(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        bot._last_seen_text["ws-1"] = "the missed reply"
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread))

        bot._send_recovered_text.assert_not_called()

    def test_skips_non_string_content(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [{"role": "assistant", "content": [{"type": "text", "text": "hi"}]}]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread))

        bot._send_recovered_text.assert_not_called()

    def test_walks_back_multiple_missed_turns_in_chronological_order(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [
                    {"role": "user", "content": "first"},
                    {"role": "assistant", "content": "missed reply one"},
                    {"role": "user", "content": "second"},
                    {"role": "assistant", "content": "missed reply two"},
                ]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread, 2))

        assert bot._send_recovered_text.await_args_list == [
            ((thread, "missed reply one"),),
            ((thread, "missed reply two"),),
        ]
        assert bot._last_seen_text["ws-1"] == "missed reply two"

    def test_stops_recovering_once_new_turn_starts_streaming(self) -> None:
        from turnstone.channels.discord.bot import StreamingMessage

        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [
                    {"role": "assistant", "content": "missed reply one"},
                    {"role": "assistant", "content": "missed reply two"},
                ]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]

        async def fake_send(thread_arg, text: str) -> None:
            bot._streaming["ws-1"] = StreamingMessage(channel=thread_arg)

        bot._send_recovered_text = AsyncMock(side_effect=fake_send)  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread, 2))

        bot._send_recovered_text.assert_awaited_once_with(thread, "missed reply one")

    def test_history_fetch_failure_is_swallowed(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        bot._http_client.get = AsyncMock(side_effect=RuntimeError("network down"))  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread))  # must not raise

        bot._send_recovered_text.assert_not_called()


class TestRecoverMissedTurnRealRaceDiscord:
    """Real end-to-end race, not mocked -- neither asyncio.sleep nor
    _recover_missed_turn is stubbed out."""

    def test_snapshot_landing_mid_delay_wins_over_history_fallback(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot._http_client.get = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            task = asyncio.create_task(bot._recover_missed_turn("ws-1", thread, 1, delay=0.02))
            await asyncio.sleep(0.005)
            await bot._on_ws_event(
                "ws-1", thread, InProgressSnapshotEvent(ws_id="ws-1", content="still streaming")
            )
            await task

        _run(scenario())

        bot._http_client.get.assert_not_called()
        assert bot._streaming["ws-1"].accumulated_text == "still streaming"

    def test_no_snapshot_within_delay_falls_back_to_history(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            await bot._recover_missed_turn("ws-1", thread, 1, delay=0.02)

        _run(scenario())

        bot._send_recovered_text.assert_awaited_once_with(thread, "the missed reply")


class TestRecoverMissedTurnSkipsUnrenderableDiscord:
    """A tool-call turn mid-walk-back must not abort recovery of the rest
    of the gap (regression test for the fix that changed the
    unrenderable-content branch from ``break`` to ``continue``)."""

    def test_skips_tool_call_turn_and_recovers_older_ones(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [
                    {"role": "assistant", "content": "older recoverable reply"},
                    {"role": "assistant", "content": [{"type": "text", "text": "tool stuff"}]},
                    {"role": "assistant", "content": "newest recoverable reply"},
                ]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread, 2))

        assert bot._send_recovered_text.await_args_list == [
            ((thread, "older recoverable reply"),),
            ((thread, "newest recoverable reply"),),
        ]

    def test_persists_last_seen_text_after_each_recovered_send(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_recovered_text = AsyncMock()  # type: ignore[method-assign]
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        with patch("turnstone.channels.discord.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", thread, 1))

        bot.storage.update_channel_route_recovery_state.assert_called_once_with(
            "discord", str(thread.id), last_turn_count=None, last_seen_text="the missed reply"
        )


class TestRecoveryStateCleanupDiscord:
    def test_unsubscribe_clears_recovery_state(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            bot._sse_tasks["ws-1"] = asyncio.create_task(asyncio.sleep(0))
            await bot.unsubscribe_ws("ws-1")

        _run(scenario())

        assert "ws-1" not in bot._ever_connected
        assert "ws-1" not in bot._is_reconnect
        assert "ws-1" not in bot._last_turn_count
        assert "ws-1" not in bot._last_seen_text

    def test_unsubscribe_cancels_in_flight_recovery_task(self) -> None:
        bot = _make_bot()
        thread = _make_thread()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))

            never_finishes: asyncio.Future[None] = asyncio.get_event_loop().create_future()

            async def _blocks_forever(*_a: object, **_k: object) -> None:
                await never_finishes

            bot._recover_missed_turn = AsyncMock(side_effect=_blocks_forever)  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            task = bot._recovery_tasks["ws-1"]

            bot._sse_tasks["ws-1"] = asyncio.create_task(asyncio.sleep(0))
            await bot.unsubscribe_ws("ws-1")

            assert task.cancelled()
            assert "ws-1" not in bot._recovery_tasks

        _run(scenario())


class TestRestartRecoverySeedingDiscord:
    def test_seeds_state_from_persisted_checkpoint(self) -> None:
        bot = _make_bot()
        channel = MagicMock()
        bot._bot.get_channel = MagicMock(return_value=channel)  # type: ignore[method-assign]
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "discord",
                    "channel_id": "111222333",
                    "ws_id": "ws-1",
                    "node_id": "",
                    "created": "2026-01-01T00:00:00",
                    "last_turn_count": 5,
                    "last_seen_text": "the last thing sent before restart",
                }
            ]
        )
        bot.subscribe_ws = AsyncMock()  # type: ignore[method-assign]

        _run(bot._recover_routes())

        assert "ws-1" in bot._ever_connected
        assert bot._last_turn_count["ws-1"] == 5
        assert bot._last_seen_text["ws-1"] == "the last thing sent before restart"
        bot.subscribe_ws.assert_awaited_once_with("ws-1", channel)

    def test_route_without_checkpoint_is_not_seeded(self) -> None:
        bot = _make_bot()
        channel = MagicMock()
        bot._bot.get_channel = MagicMock(return_value=channel)  # type: ignore[method-assign]
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "discord",
                    "channel_id": "111222333",
                    "ws_id": "ws-1",
                    "node_id": "",
                    "created": "2026-01-01T00:00:00",
                    "last_turn_count": None,
                    "last_seen_text": None,
                }
            ]
        )
        bot.subscribe_ws = AsyncMock()  # type: ignore[method-assign]

        _run(bot._recover_routes())

        assert "ws-1" not in bot._ever_connected
        assert "ws-1" not in bot._last_turn_count
        assert "ws-1" not in bot._last_seen_text

    def test_second_recover_routes_call_does_not_overwrite_live_state(self) -> None:
        """Regression: on_resumed calls _recover_routes again -- a second
        pass must not clobber live in-memory state with a staler DB
        checkpoint (discord has no analogue of this scenario in matrix,
        which only seeds once from a cold process)."""
        bot = _make_bot()
        channel = MagicMock()
        bot._bot.get_channel = MagicMock(return_value=channel)  # type: ignore[method-assign]
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "discord",
                    "channel_id": "111222333",
                    "ws_id": "ws-1",
                    "node_id": "",
                    "created": "2026-01-01T00:00:00",
                    "last_turn_count": 5,
                    "last_seen_text": "stale checkpoint",
                }
            ]
        )
        bot.subscribe_ws = AsyncMock()  # type: ignore[method-assign]

        _run(bot._recover_routes())
        # Live traffic advances state past the checkpoint before a
        # gateway resume re-triggers _recover_routes.
        bot._last_turn_count["ws-1"] = 9
        bot._last_seen_text["ws-1"] = "live text newer than checkpoint"

        _run(bot._recover_routes())

        assert bot._last_turn_count["ws-1"] == 9
        assert bot._last_seen_text["ws-1"] == "live text newer than checkpoint"


    def test_seeded_state_triggers_recovery_on_first_post_restart_status(self) -> None:
        """The seeding itself is the whole fix -- _handle_connected and
        _handle_status need no changes to correctly treat the first
        post-restart connect as a reconnect once seeded."""
        bot = _make_bot()
        channel = MagicMock()
        thread = channel
        bot._bot.get_channel = MagicMock(return_value=channel)  # type: ignore[method-assign]
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "discord",
                    "channel_id": "111222333",
                    "ws_id": "ws-1",
                    "node_id": "",
                    "created": "2026-01-01T00:00:00",
                    "last_turn_count": 5,
                    "last_seen_text": None,
                }
            ]
        )
        bot.subscribe_ws = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            await bot._recover_routes()
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", thread, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=7))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once_with("ws-1", thread, 2)


class TestStatusPersistCadenceDiscord:
    def test_persists_only_when_turn_count_changes(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=2))

        _run(scenario())

        assert bot.storage.update_channel_route_recovery_state.call_count == 2
        bot.storage.update_channel_route_recovery_state.assert_any_call(
            "discord", str(thread.id), last_turn_count=1, last_seen_text=None
        )
        bot.storage.update_channel_route_recovery_state.assert_any_call(
            "discord", str(thread.id), last_turn_count=2, last_seen_text=None
        )

    def test_persist_failure_does_not_block_message_delivery(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.storage.update_channel_route_recovery_state = MagicMock(  # type: ignore[attr-defined]
            side_effect=RuntimeError("db down")
        )

        _run(bot._on_ws_event("ws-1", thread, StatusEvent(ws_id="ws-1", turn_count=1)))

        assert bot._last_turn_count["ws-1"] == 1


class TestStreamEndPersistDiscord:
    def test_persists_last_seen_text_on_stream_end(self) -> None:
        bot = _make_bot()
        thread = _make_thread()
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", thread, ContentEvent(ws_id="ws-1", text="a reply"))
            await bot._on_ws_event("ws-1", thread, StreamEndEvent(ws_id="ws-1"))

        _run(scenario())

        bot.storage.update_channel_route_recovery_state.assert_called_once_with(
            "discord", str(thread.id), last_turn_count=None, last_seen_text="a reply"
        )
