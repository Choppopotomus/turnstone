"""Tests for the Matrix channel adapter's SSE event dispatch (bot.py).

Narrow scope: covers the InProgressSnapshotEvent fix (dispatch-silence bug,
2026-08-09) — the bot previously silently dropped the server's mid-stream
snapshot replay on a fresh SSE reconnect. No pre-existing test file covered
this adapter at all; this file stays scoped to the new/changed dispatch
behavior rather than backfilling full coverage.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from turnstone.channels.matrix.bot import StreamingMessage, TurnstoneMatrixBot
from turnstone.channels.matrix.config import MatrixConfig
from turnstone.sdk.events import (
    ConnectedEvent,
    ContentEvent,
    InProgressSnapshotEvent,
    StatusEvent,
    StreamEndEvent,
)

# ---------------------------------------------------------------------------
# Restart recovery (2026-08-09 follow-up): the four missed-turn-recovery
# tracking dicts are plain in-memory state that starts empty on every bot
# process boot, so a restart looked identical to a brand-new subscription
# and silently lost any turn that completed during the restart window.
# _recover_routes() now seeds that state from a persisted checkpoint
# (channel_routes.last_turn_count/last_seen_text) before resubscribing, so
# the existing reconnect-recovery machinery also covers a restart.
# ---------------------------------------------------------------------------


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _make_bot() -> TurnstoneMatrixBot:
    config = MatrixConfig(auto_approve=False)
    storage = MagicMock()

    with patch("turnstone.channels.matrix.bot.httpx.AsyncClient", return_value=AsyncMock()):
        bot = TurnstoneMatrixBot(config, server_url="http://localhost:8080", storage=storage)
    bot._client = AsyncMock()  # type: ignore[attr-defined]
    return bot


class TestInProgressSnapshotEvent:
    """Fresh SSE reconnect mid-stream: the server replays accumulated content."""

    def test_creates_streaming_message_from_snapshot(self) -> None:
        bot = _make_bot()
        event = InProgressSnapshotEvent(ws_id="ws-1", content="partial reply so far")

        _run(bot._on_ws_event("ws-1", "!room:matrix.local", event))

        assert "ws-1" in bot._streaming
        assert bot._streaming["ws-1"].accumulated_text == "partial reply so far"

    def test_replaces_rather_than_appends_to_existing_buffer(self) -> None:
        bot = _make_bot()
        # Bot already has a (stale/partial) local buffer from before the drop.
        _run(bot._on_ws_event("ws-1", "!room:matrix.local", ContentEvent(ws_id="ws-1", text="stale ")))
        assert bot._streaming["ws-1"].accumulated_text == "stale "

        # Snapshot is authoritative and one-shot — it must replace, not append.
        event = InProgressSnapshotEvent(ws_id="ws-1", content="the real full text so far")
        _run(bot._on_ws_event("ws-1", "!room:matrix.local", event))

        assert bot._streaming["ws-1"].accumulated_text == "the real full text so far"

    def test_empty_snapshot_is_noop(self) -> None:
        bot = _make_bot()
        event = InProgressSnapshotEvent(ws_id="ws-1", content="")

        _run(bot._on_ws_event("ws-1", "!room:matrix.local", event))

        assert "ws-1" not in bot._streaming

    def test_content_after_snapshot_appends_normally(self) -> None:
        """Snapshot recovers the miss; subsequent live ContentEvents still append."""
        bot = _make_bot()
        _run(
            bot._on_ws_event(
                "ws-1", "!room:matrix.local", InProgressSnapshotEvent(ws_id="ws-1", content="Hello")
            )
        )
        _run(bot._on_ws_event("ws-1", "!room:matrix.local", ContentEvent(ws_id="ws-1", text=", world")))

        assert bot._streaming["ws-1"].accumulated_text == "Hello, world"

    def test_stream_end_finalizes_recovered_message(self) -> None:
        bot = _make_bot()
        _run(
            bot._on_ws_event(
                "ws-1", "!room:matrix.local", InProgressSnapshotEvent(ws_id="ws-1", content="recovered")
            )
        )
        _run(bot._on_ws_event("ws-1", "!room:matrix.local", StreamEndEvent(ws_id="ws-1")))

        assert "ws-1" not in bot._streaming


class TestStreamingMessageReplace:
    """Unit tests for the new ``replace`` method (wholesale, not delta)."""

    def test_replace_after_append_discards_prior_buffer(self) -> None:
        sm = StreamingMessage(client=AsyncMock(), room_id="!room:matrix.local")
        _run(sm.append("old "))
        _run(sm.append("content"))
        _run(sm.replace("new content"))

        assert sm.accumulated_text == "new content"


# ---------------------------------------------------------------------------
# Missed-turn recovery — turn-completes-entirely-during-disconnect
# (dispatch-silence bug, 2026-08-09 follow-up)
#
# ConnectedEvent + StatusEvent (with its turn_count field) replay on every
# fresh/truncated SSE reconnect, never on a seamless replay_ok one — an
# existing server signal the bot never consumed. _handle_status uses the
# turn_count jump to detect "a turn completed while I was disconnected",
# and _recover_missed_turn falls back to GET /history for the case
# InProgressSnapshotEvent can't cover (the turn already fully committed
# before reconnect, so the server's in-flight buffer is empty).
# ---------------------------------------------------------------------------

ROOM = "!room:matrix.local"


class TestMissedTurnRecoveryGating:
    """_handle_status: when does a reconnect schedule a recovery check."""

    def test_first_connect_never_schedules_recovery(self) -> None:
        bot = _make_bot()
        bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)  # let any scheduled task actually run

        _run(scenario())

        bot._recover_missed_turn.assert_not_called()

    def test_reconnect_with_no_new_turns_does_not_schedule_recovery(self) -> None:
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            # Reconnect with the SAME turn_count -- nothing happened while disconnected.
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_not_called()

    def test_reconnect_with_new_turn_schedules_recovery(self) -> None:
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            # Reconnect -- server's turn_count moved on without us.
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once_with("ws-1", ROOM, 1)

    def test_live_status_ticks_within_one_connection_never_schedule_recovery(self) -> None:
        """turn_count can legitimately climb between StatusEvents on a
        single live connection (ordinary multi-turn conversation) -- only
        a turn_count jump straddling a RECONNECT should ever trigger."""
        bot = _make_bot()
        bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_not_called()

    def test_is_reconnect_flag_does_not_re_trigger_on_later_ordinary_turns(self) -> None:
        """Regression test: _is_reconnect must be consumed after driving one
        recovery decision, or every later ordinary turn on the same
        connection re-triggers _recover_missed_turn forever."""
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            # Reconnect -- turn_count jumped, this legitimately schedules recovery once.
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            bot._recover_missed_turn = AsyncMock()  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            bot._recover_missed_turn.assert_called_once()

            # Ordinary new turns on this same (still-connected) session keep
            # climbing turn_count -- must NOT be mistaken for more reconnects.
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=4))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=5))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once()

    def test_overlapping_reconnect_does_not_spawn_second_recovery(self) -> None:
        """Re-entrancy guard: a recovery already in flight for a ws_id must
        block a second one from spawning and racing on _last_seen_text."""
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))

            never_finishes: asyncio.Future[None] = asyncio.get_event_loop().create_future()

            async def _blocks_forever(*_a: object, **_k: object) -> None:
                await never_finishes

            bot._recover_missed_turn = AsyncMock(side_effect=_blocks_forever)  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            assert "ws-1" in bot._recovery_tasks

            # A second reconnect arrives while the first recovery is still running.
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=4))
            await asyncio.sleep(0)

            never_finishes.cancel()
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once()


class TestRecoverMissedTurn:
    """_recover_missed_turn: the GET /history backstop itself."""

    def test_skips_when_streaming_already_resumed(self) -> None:
        bot = _make_bot()
        bot._streaming["ws-1"] = StreamingMessage(client=bot._client, room_id=ROOM)
        bot._http_client.get = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM))

        bot._http_client.get.assert_not_called()

    def test_posts_new_assistant_message_from_history(self) -> None:
        bot = _make_bot()
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
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM))

        bot._send_text.assert_awaited_once_with(ROOM, "the missed reply")
        assert bot._last_seen_text["ws-1"] == "the missed reply"

    def test_does_not_repost_already_seen_text(self) -> None:
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        bot._last_seen_text["ws-1"] = "the missed reply"
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM))

        bot._send_text.assert_not_called()

    def test_skips_non_string_content(self) -> None:
        """Multipart/tool-call turns need the full renderer this recovery
        path doesn't have -- skip rather than guess at a partial render."""
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [{"role": "assistant", "content": [{"type": "text", "text": "hi"}]}]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM))

        bot._send_text.assert_not_called()

    def test_walks_back_multiple_missed_turns_in_chronological_order(self) -> None:
        """A disconnect spanning >1 completed turn must recover all of them,
        not just the latest -- regression test for the single-turn-only bug."""
        bot = _make_bot()
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
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM, 2))

        assert bot._send_text.await_args_list == [
            ((ROOM, "missed reply one"),),
            ((ROOM, "missed reply two"),),
        ]
        assert bot._last_seen_text["ws-1"] == "missed reply two"

    def test_stops_recovering_once_new_turn_starts_streaming(self) -> None:
        """TOCTOU regression: the _streaming guard must be re-checked before
        each send, not just once up front, so a turn that starts/finishes
        mid-recovery wins over stale recovered text."""
        bot = _make_bot()
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

        async def fake_send_text(room_id: str, text: str) -> None:
            # A new live turn starts mid-recovery, right after the first send.
            bot._streaming["ws-1"] = StreamingMessage(client=bot._client, room_id=room_id)

        bot._send_text = AsyncMock(side_effect=fake_send_text)  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM, 2))

        bot._send_text.assert_awaited_once_with(ROOM, "missed reply one")

    def test_history_fetch_failure_is_swallowed(self) -> None:
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        bot._http_client.get = AsyncMock(side_effect=RuntimeError("network down"))  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM))  # must not raise

        bot._send_text.assert_not_called()


class TestRecoverMissedTurnRealRace:
    """Real end-to-end race, not mocked -- neither asyncio.sleep nor
    _recover_missed_turn is stubbed out. Prior coverage only proved each
    branch by construction (patching sleep away, or asserting the
    _streaming guard's value at a single checked instant); this exercises
    the actual timing race the code's own docstring describes."""

    def test_snapshot_landing_mid_delay_wins_over_history_fallback(self) -> None:
        bot = _make_bot()
        bot._http_client.get = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            # Real delay, shrunk to milliseconds -- not mocked away.
            task = asyncio.create_task(bot._recover_missed_turn("ws-1", ROOM, 1, delay=0.02))
            await asyncio.sleep(0.005)
            # The same reconnect's replay burst delivers its snapshot
            # while the recovery task is genuinely still asleep.
            await bot._on_ws_event(
                "ws-1", ROOM, InProgressSnapshotEvent(ws_id="ws-1", content="still streaming")
            )
            await task

        _run(scenario())

        bot._http_client.get.assert_not_called()
        assert bot._streaming["ws-1"].accumulated_text == "still streaming"

    def test_no_snapshot_within_delay_falls_back_to_history(self) -> None:
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        async def scenario() -> None:
            # Nothing arrives to stream-resume during the real delay window.
            await bot._recover_missed_turn("ws-1", ROOM, 1, delay=0.02)

        _run(scenario())

        bot._send_text.assert_awaited_once_with(ROOM, "the missed reply")


class TestRecoveryStateCleanup:
    def test_unsubscribe_clears_recovery_state(self) -> None:
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot.unsubscribe_ws("ws-1")

        _run(scenario())

        assert "ws-1" not in bot._ever_connected
        assert "ws-1" not in bot._is_reconnect
        assert "ws-1" not in bot._last_turn_count
        assert "ws-1" not in bot._last_seen_text

    def test_unsubscribe_cancels_in_flight_recovery_task(self) -> None:
        """A running recovery must not send into a torn-down route -- popping
        _last_seen_text out from under it without cancelling defeats its
        own dedup check."""
        bot = _make_bot()

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))

            never_finishes: asyncio.Future[None] = asyncio.get_event_loop().create_future()

            async def _blocks_forever(*_a: object, **_k: object) -> None:
                await never_finishes

            bot._recover_missed_turn = AsyncMock(side_effect=_blocks_forever)  # type: ignore[method-assign]
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=3))
            await asyncio.sleep(0)
            task = bot._recovery_tasks["ws-1"]

            await bot.unsubscribe_ws("ws-1")

            assert task.cancelled()
            assert "ws-1" not in bot._recovery_tasks


class TestRestartRecoverySeeding:
    """_recover_routes: seeding in-memory state from a persisted checkpoint."""

    def test_seeds_state_from_persisted_checkpoint(self) -> None:
        bot = _make_bot()
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "matrix",
                    "channel_id": ROOM,
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
        bot.subscribe_ws.assert_awaited_once_with("ws-1", ROOM)

    def test_route_without_checkpoint_is_not_seeded(self) -> None:
        """A route created moments before a restart, never checkpointed
        yet, must NOT be seeded -- that would be a false-positive
        recovery trigger on its very first connect."""
        bot = _make_bot()
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "matrix",
                    "channel_id": ROOM,
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

    def test_seeded_state_triggers_recovery_on_first_post_restart_status(self) -> None:
        """The seeding itself is the whole fix -- _handle_connected and
        _handle_status need no changes to correctly treat the first
        post-restart connect as a reconnect once seeded."""
        bot = _make_bot()
        bot.storage.list_channel_routes_by_type = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "channel_type": "matrix",
                    "channel_id": ROOM,
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
            # First SSE connect after the (simulated) restart.
            await bot._on_ws_event("ws-1", ROOM, ConnectedEvent(ws_id="ws-1"))
            # Server's turn_count has moved past what was persisted --
            # a turn completed during the restart window.
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=7))
            await asyncio.sleep(0)

        _run(scenario())

        bot._recover_missed_turn.assert_called_once_with("ws-1", ROOM, 2)


class TestStatusPersistCadence:
    """_handle_status: the checkpoint write, gated on actual change."""

    def test_persists_only_when_turn_count_changes(self) -> None:
        """A tool-heavy turn emits one StatusEvent per LLM API round-trip,
        all carrying the same turn_count -- must not write once per event."""
        bot = _make_bot()
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=1))
            await bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=2))

        _run(scenario())

        assert bot.storage.update_channel_route_recovery_state.call_count == 2
        bot.storage.update_channel_route_recovery_state.assert_any_call(
            "matrix", ROOM, last_turn_count=1, last_seen_text=None
        )
        bot.storage.update_channel_route_recovery_state.assert_any_call(
            "matrix", ROOM, last_turn_count=2, last_seen_text=None
        )

    def test_persist_failure_does_not_block_message_delivery(self) -> None:
        bot = _make_bot()
        bot.storage.update_channel_route_recovery_state = MagicMock(  # type: ignore[attr-defined]
            side_effect=RuntimeError("db down")
        )

        _run(bot._on_ws_event("ws-1", ROOM, StatusEvent(ws_id="ws-1", turn_count=1)))  # must not raise

        assert bot._last_turn_count["ws-1"] == 1


class TestStreamEndPersist:
    def test_persists_last_seen_text_on_stream_end(self) -> None:
        bot = _make_bot()
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        async def scenario() -> None:
            await bot._on_ws_event("ws-1", ROOM, ContentEvent(ws_id="ws-1", text="a reply"))
            await bot._on_ws_event("ws-1", ROOM, StreamEndEvent(ws_id="ws-1"))

        _run(scenario())

        bot.storage.update_channel_route_recovery_state.assert_called_once_with(
            "matrix", ROOM, last_turn_count=None, last_seen_text="a reply"
        )


class TestRecoverMissedTurnSkipsUnrenderable:
    """_recover_missed_turn: a tool-call turn mid-walk-back must not abort
    recovery of the rest of the gap (regression test for the fix that
    changed the unrenderable-content branch from `break` to `continue`)."""

    def test_skips_tool_call_turn_and_recovers_older_ones(self) -> None:
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "messages": [
                    {"role": "assistant", "content": "older recoverable reply"},
                    # A tool-call turn sits between the two recoverable
                    # text turns -- must be skipped, not treated as a
                    # stop signal.
                    {"role": "assistant", "content": [{"type": "text", "text": "tool stuff"}]},
                    {"role": "assistant", "content": "newest recoverable reply"},
                ]
            }
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM, 2))

        assert bot._send_text.await_args_list == [
            ((ROOM, "older recoverable reply"),),
            ((ROOM, "newest recoverable reply"),),
        ]

    def test_persists_last_seen_text_after_each_recovered_send(self) -> None:
        bot = _make_bot()
        bot.router.get_node_url = AsyncMock(return_value="http://node")  # type: ignore[attr-defined]
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={"messages": [{"role": "assistant", "content": "the missed reply"}]}
        )
        bot._http_client.get = AsyncMock(return_value=response)  # type: ignore[method-assign]
        bot._send_text = AsyncMock()  # type: ignore[method-assign]
        bot.storage.update_channel_route_recovery_state = MagicMock()  # type: ignore[attr-defined]

        with patch("turnstone.channels.matrix.bot.asyncio.sleep", AsyncMock()):
            _run(bot._recover_missed_turn("ws-1", ROOM, 1))

        bot.storage.update_channel_route_recovery_state.assert_called_once_with(
            "matrix", ROOM, last_turn_count=None, last_seen_text="the missed reply"
        )
