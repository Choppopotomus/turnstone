"""Matrix bot adapter — connects Matrix rooms to turnstone workstreams.

:class:`TurnstoneMatrixBot` uses matrix-nio for async communication with a
self-hosted Matrix homeserver (Tuwunel recommended).

Interaction model
-----------------
* **Rooms**: any message in a joined room creates or continues a workstream.
* **DMs**: every message is routed freely.
* Bot automatically joins rooms when invited.

Events are consumed from the server's per-workstream SSE endpoint
(``GET /v1/api/workstreams/{ws_id}/events``) using httpx-sse. Inbound
messages are sent directly to server nodes via HTTP
(``POST /v1/api/workstreams/{ws_id}/send``).

Install dependencies:
    pip install matrix-nio[e2e]
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import ssl
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx

from turnstone.channels._config import MAX_NOTIFY_TRACKING
from turnstone.channels._formatter import chunk_message
from turnstone.channels._routing import (
    ChannelRouter,
    pop_cycle_entry,
    pop_ws_entries,
)
from turnstone.channels._sse import run_sse_stream
from turnstone.core.log import get_logger
from turnstone.sdk.events import (
    ApprovalResolvedEvent,
    ApproveRequestEvent,
    ConnectedEvent,
    ContentEvent,
    ErrorEvent,
    InProgressSnapshotEvent,
    IntentVerdictEvent,
    ServerEvent,
    StatusEvent,
    StreamEndEvent,
    ThinkingStartEvent,
    ThinkingStopEvent,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from nio import AsyncClient, MatrixRoom, RoomMessageText

    from turnstone.channels.matrix.config import MatrixConfig
    from turnstone.core.storage._protocol import StorageBackend

log = get_logger(__name__)

# Matrix has generous message limits but we cap for readability.
_MAX_INBOUND_MESSAGE_LEN: int = 16384


@dataclass
class StreamingMessage:
    """Accumulates streamed content and periodically sends/edits a Matrix message.

    Matrix supports message edits via m.replace relations, but for simplicity
    we send a final message on stream end rather than editing in-place.
    """

    client: Any  # nio.AsyncClient
    room_id: str
    max_length: int = 16384

    _event_id: str = field(default="", init=False, repr=False)
    _buffer: list[str] = field(default_factory=list, init=False, repr=False)
    _finalized_text: str | None = field(default=None, init=False, repr=False)

    @property
    def accumulated_text(self) -> str:
        if self._finalized_text is not None:
            return self._finalized_text
        return "".join(self._buffer)

    async def append(self, text: str) -> None:
        self._buffer.append(text)

    async def replace(self, text: str) -> None:
        """Replace the buffer wholesale — for a one-shot snapshot, not a delta."""
        self._buffer = [text]

    async def finalize(self) -> None:
        content = "".join(self._buffer)
        self._finalized_text = content
        if not content:
            return

        chunks = chunk_message(content, self.max_length)
        for i, chunk in enumerate(chunks):
            try:
                resp = await self.client.room_send(
                    room_id=self.room_id,
                    message_type="m.room.message",
                    content={
                        "msgtype": "m.text",
                        "body": chunk,
                    },
                    ignore_unverified_devices=True,
                )
                if i == 0 and hasattr(resp, "event_id"):
                    self._event_id = resp.event_id
            except Exception:
                log.warning("matrix.streaming_message.send_failed", room_id=self.room_id, exc_info=True)


class TurnstoneMatrixBot:
    """Matrix bot bridging Matrix rooms to turnstone workstreams.

    All room_send() calls pass ignore_unverified_devices=True. This is a
    deliberate, permanent trust decision, not an oversight: without it,
    nio raises OlmUnverifiedDeviceError for any device the bot hasn't
    explicitly verified, which blocks every encrypted send until someone
    completes device verification -- and previously did so silently,
    because the exception was swallowed at a log level below the
    configured threshold (see the room_send() call sites). Acceptable
    here because this bot serves one user on a homeserver only that
    user controls; reconsider if this ever bridges rooms with untrusted
    third parties.
    """

    channel_type: str = "matrix"
    _MAX_NOTIFY_TRACKING: int = MAX_NOTIFY_TRACKING

    def __init__(
        self,
        config: MatrixConfig,
        server_url: str,
        storage: StorageBackend,
        *,
        api_token: str = "",
        console_url: str = "",
        console_token_factory: Callable[[], str] | None = None,
        server_token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.config = config
        self._server_url = server_url.rstrip("/")
        self._console_url = console_url.rstrip("/") if console_url else ""
        self._api_token = api_token
        self._token_factory = server_token_factory
        self.storage = storage

        self.router = ChannelRouter(
            server_url,
            storage,
            auto_approve=config.auto_approve,
            auto_approve_tools=list(config.auto_approve_tools),
            skill=config.skill,
            api_token=api_token,
            console_url=console_url,
            console_token_factory=console_token_factory,
            server_token_factory=server_token_factory,
        )

        self._subscribed_ws: set[str] = set()
        self._sse_tasks: dict[str, asyncio.Task[None]] = {}
        self._streaming: dict[str, StreamingMessage] = {}
        self._pending_approval: dict[tuple[str, str], dict[str, Any]] = {}
        self._notify_ws_map: dict[str, tuple[str, str]] = {}
        self._notify_reply_rooms: dict[str, str] = {}

        # -- missed-turn recovery (turn-completes-entirely-during-disconnect) --
        # ``connected``/``status`` replay on every fresh/truncated SSE
        # reconnect but never on a seamless ``replay_ok`` one (see
        # session_routes.py) -- that asymmetry is the server's existing,
        # previously-unused signal for "this connect might have missed
        # something". Tracked per ws_id so a bot process serving many
        # rooms keeps them independent.
        self._ever_connected: set[str] = set()
        self._is_reconnect: dict[str, bool] = {}
        self._last_turn_count: dict[str, int] = {}
        self._last_seen_text: dict[str, str] = {}
        # Keyed by ws_id, not a flat set: doubles as both the re-entrancy
        # guard (one recovery in flight per ws_id) and the handle needed to
        # cancel a running recovery on unsubscribe/stale-route cleanup.
        self._recovery_tasks: dict[str, asyncio.Task[None]] = {}

        headers: dict[str, str] = {}
        if api_token and not server_token_factory:
            headers["Authorization"] = f"Bearer {api_token}"

        self._http_client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=10.0),
        )

        # matrix-nio client — created in start()
        self._client: AsyncClient | None = None

    async def start(self) -> None:
        """Connect to the Matrix homeserver and begin listening."""
        from nio import (
            AsyncClient,
            AsyncClientConfig,
            InviteMemberEvent,
            LoginResponse,
            RoomMessageText,
            SyncResponse,
        )

        os.makedirs(self.config.store_path, exist_ok=True)

        # Trust a self-signed homeserver cert (e.g. CWWK's Traefik-fronted
        # tuwunel) by loading it into an SSLContext built on top of the
        # normal system trust store — additive, not a replacement, so other
        # TLS connections this process makes (Anthropic, OpenAI, etc.) are
        # unaffected. `ssl=None` (nio's default) is a normal verified
        # ssl.create_default_context() with no way to add a CA short of
        # this.
        ssl_context: ssl.SSLContext | None = None
        if self.config.ca_cert_path:
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(cafile=self.config.ca_cert_path)

        self._client = AsyncClient(
            self.config.homeserver,
            self.config.user_id,
            store_path=self.config.store_path,
            device_id=self.config.device_id,
            ssl=ssl_context,
            # nio retries transport failures forever by default (max_timeouts
            # unset). Seen in prod: one aiohttp connection wedged for 33+
            # hours straight, 700+ retries, never recovering on its own.
            # Bound it so sync_forever eventually gives up and _sync_loop can
            # restart the process for a clean session (see below).
            config=AsyncClientConfig(max_timeouts=5),
        )

        # Login
        resp = await self._client.login(self.config.password)
        if isinstance(resp, LoginResponse):
            log.info(
                "matrix.logged_in",
                user_id=self.config.user_id,
                device_id=resp.device_id,
            )
        else:
            log.error("matrix.login_failed", response=str(resp))
            return

        # Register message callback
        self._client.add_event_callback(self._on_message, RoomMessageText)

        # Register invite callback — auto-join rooms
        self._client.add_event_callback(self._on_invite, InviteMemberEvent)

        # Persist the sync token so a restart resumes instead of re-fetching
        # full room state + timeline backlog (was re-dispatching old messages
        # as new ones on every restart, spawning duplicate workstreams).
        self._client.add_response_callback(self._on_sync_response, SyncResponse)
        since = self._load_since_token()

        # Recover existing routes
        await self._recover_routes()

        # Start sync loop (this blocks — run in background task)
        sync_task = asyncio.create_task(self._sync_loop(since), name="matrix:sync")
        log.info("matrix.started", user_id=self.config.user_id)

    def _since_token_path(self) -> str:
        return os.path.join(self.config.store_path, "next_batch")

    def _load_since_token(self) -> str | None:
        try:
            with open(self._since_token_path()) as f:
                return f.read().strip() or None
        except FileNotFoundError:
            return None

    async def _on_sync_response(self, response: Any) -> None:
        token = getattr(response, "next_batch", None)
        if token:
            with open(self._since_token_path(), "w") as f:
                f.write(token)

    async def _sync_loop(self, since: str | None) -> None:
        """Run the Matrix sync loop continuously.

        There's no cheap in-process fix for a wedged aiohttp connection
        (a fresh AsyncClient always works — see the class docstring above
        for how this was diagnosed). So when sync_forever gives up, ask
        launchd to give us a clean process instead of limping along with a
        session we know is broken.
        """
        try:
            # full_state only on a genuinely fresh install (no persisted
            # token yet) -- with a since token the server only returns
            # events after it, so this no longer replays history on restart.
            await self._client.sync_forever(
                timeout=30000, full_state=since is None, since=since
            )
        except Exception:
            log.exception("matrix.sync_failed")
        signal.raise_signal(signal.SIGTERM)

    async def _on_invite(self, room: Any, event: Any) -> None:
        """Auto-join rooms when invited."""
        log.info("matrix.invited", room_id=room.room_id, inviter=event.sender)
        await self._client.join(room.room_id)

    async def _on_message(self, room: Any, event: Any) -> None:
        """Handle incoming Matrix messages."""
        # Ignore own messages
        if event.sender == self.config.user_id:
            return

        # Ignore non-text messages
        if not hasattr(event, "body"):
            return

        text = event.body.strip()
        if not text:
            return

        room_id = room.room_id
        sender = event.sender

        # Check room allowlist
        if self.config.allowed_rooms and room_id not in self.config.allowed_rooms:
            return

        # Truncate oversized messages
        if len(text) > _MAX_INBOUND_MESSAGE_LEN:
            text = text[:_MAX_INBOUND_MESSAGE_LEN]

        # Check if this room has an active session
        ws_id = await self._get_room_ws(room_id)

        if ws_id is None:
            # Create new workstream for this room
            try:
                ws_id, _ = await self.router.get_or_create_workstream(
                    channel_type="matrix",
                    channel_id=room_id,
                    name=f"matrix-{room_id[:16]}",
                    client_type="chat",
                )
                await self.subscribe_ws(ws_id, room_id)
                log.info("matrix.session_created", ws_id=ws_id, room_id=room_id)
            except Exception:
                log.exception("matrix.session_create_failed", room_id=room_id)
                return

        # Route message to workstream
        try:
            await self.router.send_message(ws_id, text)
            log.info("matrix.message_dispatched", ws_id=ws_id, room_id=room_id)
        except Exception:
            log.exception("matrix.message_dispatch_failed", room_id=room_id)

    async def _get_room_ws(self, room_id: str) -> str | None:
        """Look up the workstream ID for a Matrix room."""
        route = await asyncio.to_thread(
            self.storage.get_channel_route, "matrix", room_id
        )
        return route["ws_id"] if route else None

    async def stop(self) -> None:
        """Disconnect from Matrix and clean up."""
        for ws_id in list(self._subscribed_ws):
            await self.unsubscribe_ws(ws_id)
        await self.router.aclose()
        await self._http_client.aclose()
        if self._client is not None:
            await self._client.close()
        log.info("matrix.stopped")

    # -- subscription management ---------------------------------------------

    async def subscribe_ws(self, ws_id: str, room_id: str) -> None:
        """Subscribe to workstream events via SSE."""
        if ws_id in self._subscribed_ws:
            return

        task = asyncio.create_task(
            self._sse_listener(ws_id, room_id),
            name=f"sse:{ws_id}",
        )
        self._sse_tasks[ws_id] = task
        self._subscribed_ws.add(ws_id)
        log.info("matrix.subscribed", ws_id=ws_id, room_id=room_id)

    async def unsubscribe_ws(self, ws_id: str) -> None:
        task = self._sse_tasks.pop(ws_id, None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        self._subscribed_ws.discard(ws_id)
        self._streaming.pop(ws_id, None)
        self._pop_ws_approvals(ws_id)
        await self._pop_ws_recovery_state(ws_id)
        log.info("matrix.unsubscribed", ws_id=ws_id)

    def _pop_ws_approvals(self, ws_id: str) -> None:
        pop_ws_entries(self._pending_approval, ws_id)

    async def _pop_ws_recovery_state(self, ws_id: str) -> None:
        self._ever_connected.discard(ws_id)
        self._is_reconnect.pop(ws_id, None)
        self._last_turn_count.pop(ws_id, None)
        self._last_seen_text.pop(ws_id, None)

        task = self._recovery_tasks.pop(ws_id, None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _cleanup_stale_route(self, ws_id: str, room_id: str) -> None:
        await self.router.delete_route("matrix", room_id)
        self._sse_tasks.pop(ws_id, None)
        self._subscribed_ws.discard(ws_id)
        self._streaming.pop(ws_id, None)
        self._pop_ws_approvals(ws_id)
        await self._pop_ws_recovery_state(ws_id)
        log.info("matrix.stale_route_removed", ws_id=ws_id)

    # -- SSE listener --------------------------------------------------------

    async def _sse_listener(self, ws_id: str, room_id: str) -> None:
        async def _on_event(event: ServerEvent) -> None:
            await self._on_ws_event(ws_id, room_id, event)

        async def _on_stale() -> None:
            await self._cleanup_stale_route(ws_id, room_id)

        await run_sse_stream(
            http_client=self._http_client,
            log_prefix="matrix",
            ws_id=ws_id,
            node_url_fn=self.router.get_node_url,
            token_factory=self._token_factory,
            on_event=_on_event,
            on_stale=_on_stale,
        )

    # -- event dispatch ------------------------------------------------------

    async def _on_ws_event(
        self,
        ws_id: str,
        room_id: str,
        event: ServerEvent,
    ) -> None:
        if isinstance(event, (ThinkingStartEvent, ThinkingStopEvent)):
            return

        if isinstance(event, ContentEvent):
            await self._handle_content(ws_id, room_id, event)
        elif isinstance(event, InProgressSnapshotEvent):
            await self._handle_in_progress_snapshot(ws_id, room_id, event)
        elif isinstance(event, ConnectedEvent):
            await self._handle_connected(ws_id)
        elif isinstance(event, StatusEvent):
            await self._handle_status(ws_id, room_id, event)
        elif isinstance(event, ApproveRequestEvent):
            await self._handle_approve_request(ws_id, room_id, event)
        elif isinstance(event, IntentVerdictEvent):
            await self._handle_intent_verdict(ws_id, event)
        elif isinstance(event, ApprovalResolvedEvent):
            await self._handle_approval_resolved(ws_id, event)
        elif isinstance(event, StreamEndEvent):
            await self._handle_stream_end(ws_id)
        elif isinstance(event, ErrorEvent):
            await self._handle_error(room_id, event)

    async def _handle_content(self, ws_id: str, room_id: str, event: ContentEvent) -> None:
        sm = self._streaming.get(ws_id)
        if sm is None:
            sm = StreamingMessage(
                client=self._client,
                room_id=room_id,
                max_length=self.config.max_message_length,
            )
            self._streaming[ws_id] = sm
        await sm.append(event.text)

    async def _handle_in_progress_snapshot(
        self, ws_id: str, room_id: str, event: InProgressSnapshotEvent
    ) -> None:
        """Recover a mid-turn reconnect: the server replays the full in-flight
        content on a fresh SSE connect (e.g. after a network blip), but until
        now the bot silently dropped it and lost the turn. This is a one-shot
        snapshot of everything generated so far, not an incremental delta —
        it replaces, not appends.
        """
        if not event.content:
            return
        sm = self._streaming.get(ws_id)
        if sm is None:
            sm = StreamingMessage(
                client=self._client,
                room_id=room_id,
                max_length=self.config.max_message_length,
            )
            self._streaming[ws_id] = sm
        await sm.replace(event.content)

    async def _handle_approve_request(
        self,
        ws_id: str,
        room_id: str,
        event: ApproveRequestEvent,
    ) -> None:
        cycle_id = event.cycle_id

        # Check tool policies
        verdict = await self.router.evaluate_tool_policies(event.items)
        if verdict.kind == "deny":
            denied = ", ".join(verdict.denied_tools)
            await self.router.send_approval(ws_id, cycle_id, approved=False, feedback=f"Blocked: {denied}")
            await self._send_text(room_id, f"Tool blocked by policy: {denied}")
            return
        if verdict.kind == "allow":
            await self.router.send_approval(ws_id, cycle_id, approved=True)
            await self._send_text(room_id, "Tool approved by policy.")
            return

        # Auto-approve if configured
        if self.config.auto_approve or self._should_auto_approve(event):
            await self.router.send_approval(ws_id, cycle_id, approved=True)
            await self._send_text(room_id, "Tool auto-approved.")
            return

        # Build approval prompt
        lines = ["**Tool Approval Required**"]
        for item in event.items:
            name = item.get("approval_label") or item.get("func_name") or "tool"
            preview = item.get("preview", "")[:200]
            lines.append(f"- **{name}**")
            if preview:
                lines.append(f"  `{preview}`")

        # Store pending approval
        self._pending_approval[(ws_id, cycle_id)] = {
            "room_id": room_id,
            "cycle_id": cycle_id,
        }

        # Send approval prompt with instructions
        lines.append("")
        lines.append(f"Reply with `approve {cycle_id[:8]}` or `deny {cycle_id[:8]}`")
        await self._send_text(room_id, "\n".join(lines))

    def _should_auto_approve(self, event: ApproveRequestEvent) -> bool:
        allowed = self.config.auto_approve_tools
        if not allowed or not event.items:
            return False
        for item in event.items:
            name = item.get("func_name") or item.get("approval_label") or ""
            if name not in allowed:
                return False
        return True

    async def _handle_intent_verdict(self, ws_id: str, event: IntentVerdictEvent) -> None:
        entry = next(
            (v for (wid, _), v in self._pending_approval.items() if wid == ws_id),
            None,
        )
        if entry is None:
            return
        risk = (event.risk_level or "medium").upper()
        room_id = entry["room_id"]
        await self._send_text(
            room_id,
            f"**Judge: {event.func_name or 'tool'}**\n"
            f"Risk: {risk} | Confidence: {event.confidence or 'N/A'}\n"
            f"_{event.intent_summary or ''}_",
        )

    async def _handle_approval_resolved(self, ws_id: str, event: ApprovalResolvedEvent) -> None:
        entry = pop_cycle_entry(self._pending_approval, ws_id, event.cycle_id)
        if entry is not None:
            label = "Approved" if event.approved else "Denied"
            await self._send_text(entry["room_id"], label)

    async def _handle_stream_end(self, ws_id: str) -> None:
        sm = self._streaming.pop(ws_id, None)
        if sm is not None:
            await sm.finalize()
            if sm.accumulated_text:
                # So a later missed-turn recovery check (see
                # _recover_missed_turn) can tell "already sent" apart
                # from "genuinely new" and never double-post.
                self._last_seen_text[ws_id] = sm.accumulated_text

        reply_room = self._notify_reply_rooms.pop(ws_id, None)
        if reply_room is not None and sm is not None and sm.accumulated_text:
            self._track_notification(ws_id, reply_room)

        self._pop_ws_approvals(ws_id)

    async def _handle_error(self, room_id: str, event: ErrorEvent) -> None:
        safe_msg = event.message[:500] if event.message else "An error occurred"
        await self._send_text(room_id, f"**Error:** {safe_msg}")

    # -- missed-turn recovery -------------------------------------------------

    async def _handle_connected(self, ws_id: str) -> None:
        """Fires on every fresh/truncated SSE (re)connect, never on a
        seamless replay_ok one. Record whether this ws has connected
        before *in this bot process* so the StatusEvent that follows can
        tell "just subscribed, nothing to recover" apart from "genuine
        reconnect, might have missed a turn".
        """
        self._is_reconnect[ws_id] = ws_id in self._ever_connected
        self._ever_connected.add(ws_id)

    async def _handle_status(self, ws_id: str, room_id: str, event: StatusEvent) -> None:
        prev_turn_count = self._last_turn_count.get(ws_id)
        self._last_turn_count[ws_id] = event.turn_count
        if prev_turn_count is None or event.turn_count <= prev_turn_count:
            return
        if not self._is_reconnect.get(ws_id):
            return
        # Consume the reconnect flag now that it's driven a recovery
        # decision -- otherwise every later ordinary turn on this same
        # connection also has turn_count > prev and _is_reconnect still
        # true, re-triggering recovery + a GET /history call forever.
        self._is_reconnect[ws_id] = False
        # Re-entrancy guard: a prior recovery for this ws_id may still be
        # running (e.g. a second reconnect lands before the first recovery
        # finished). Don't spawn an overlapping one racing on the same
        # unlocked _last_seen_text entry.
        if ws_id in self._recovery_tasks:
            return
        missed_turns = event.turn_count - prev_turn_count
        # More turns completed than we last knew about, on a real
        # reconnect. Don't block this dispatch loop waiting to find out
        # if it's recoverable -- run_sse_stream awaits each event
        # in-order, so blocking here would delay the very
        # InProgressSnapshotEvent (if any) this check needs to see land
        # first. Track the task so it can't be GC'd mid-flight.
        task = asyncio.create_task(self._recover_missed_turn(ws_id, room_id, missed_turns))
        self._recovery_tasks[ws_id] = task
        task.add_done_callback(lambda _t, ws_id=ws_id: self._recovery_tasks.pop(ws_id, None))

    async def _recover_missed_turn(
        self, ws_id: str, room_id: str, missed_turns: int = 1, delay: float = 0.5
    ) -> None:
        """Backstop for turns that completed entirely while disconnected.

        InProgressSnapshotEvent only covers a turn still executing at
        reconnect time -- a turn that fully committed before we
        reconnected clears the server's in-flight buffer, so nothing on
        the live SSE path recovers it. GET /history is the only
        remaining source of truth for that case.

        Give the same reconnect's replay burst a moment to deliver an
        InProgressSnapshotEvent (same connection, no real network hop --
        this is a scheduling margin, not a network wait). If something
        shows up to stream-resume, the normal ContentEvent/StreamEndEvent
        path handles it and there is nothing to recover here.

        ``missed_turns`` is the turn_count jump observed at reconnect --
        walk back that many assistant turns, not just the latest one, so
        a disconnect spanning multiple completed turns doesn't
        permanently drop the earlier ones.

        ``delay`` defaults to the real 0.5s scheduling margin above; tests
        override it to a few milliseconds so the actual race against a
        concurrently-delivered InProgressSnapshotEvent can be exercised at
        real (just fast-forwarded) time instead of mocking sleep away.
        """
        await asyncio.sleep(delay)
        if ws_id in self._streaming:
            return

        try:
            node_base = await self.router.get_node_url(ws_id)
            headers: dict[str, str] = {}
            if self._token_factory is not None:
                headers["Authorization"] = f"Bearer {self._token_factory()}"
            resp = await self._http_client.get(
                f"{node_base}/v1/api/workstreams/{ws_id}/history",
                params={"limit": max(missed_turns * 2, 5)},
                headers=headers,
                # A lightweight lookup shouldn't inherit the client's 90s
                # read timeout meant for long-running turn requests.
                timeout=httpx.Timeout(10.0),
            )
            resp.raise_for_status()
            messages = resp.json().get("messages", [])
        except Exception:
            log.warning("matrix.missed_turn_history_fetch_failed", ws_id=ws_id, exc_info=True)
            return

        to_send: list[str] = []
        for msg in reversed(messages):
            if len(to_send) >= missed_turns:
                break
            if msg.get("role") != "assistant":
                continue
            content = msg.get("content")
            # Only plain-text turns -- tool-call/multipart turns need
            # the full renderer this recovery path doesn't have, and
            # guessing at a partial render risks looking more broken
            # than staying silent.
            if not isinstance(content, str) or not content:
                break
            if content == self._last_seen_text.get(ws_id):
                break
            to_send.append(content)

        for content in reversed(to_send):
            # Re-checked per send, not just once up front: a new turn can
            # start or finish in the time this recovery spent on the HTTP
            # round-trip / previous send, and the live path should win
            # over posting stale recovered text out of order.
            if ws_id in self._streaming:
                return
            await self._send_text(room_id, content)
            self._last_seen_text[ws_id] = content

    # -- helpers -------------------------------------------------------------

    async def _send_text(self, room_id: str, text: str) -> None:
        """Send a text message to a Matrix room."""
        if self._client is None:
            return
        chunks = chunk_message(text, self.config.max_message_length)
        for chunk in chunks:
            try:
                await self._client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content={"msgtype": "m.text", "body": chunk},
                    ignore_unverified_devices=True,
                )
            except Exception:
                log.warning("matrix.send_failed", room_id=room_id, exc_info=True)

    async def send(self, channel_id: str, content: str) -> str:
        """Send a message to a Matrix room. Returns event_id."""
        if self._client is None:
            return ""
        chunks = chunk_message(content, self.config.max_message_length)
        event_id = ""
        for chunk in chunks:
            try:
                resp = await self._client.room_send(
                    room_id=channel_id,
                    message_type="m.room.message",
                    content={"msgtype": "m.text", "body": chunk},
                    ignore_unverified_devices=True,
                )
                if hasattr(resp, "event_id"):
                    event_id = resp.event_id
            except Exception:
                log.warning("matrix.send_failed", room_id=channel_id, exc_info=True)
        return event_id

    async def send_notification(self, channel_id: str, content: str, ws_id: str) -> str:
        """Send a notification and track for reply routing."""
        event_id = await self.send(channel_id, content)
        if event_id and ws_id:
            self._track_notification(ws_id, channel_id)
        return event_id

    def _track_notification(self, ws_id: str, room_id: str) -> None:
        while len(self._notify_ws_map) >= self._MAX_NOTIFY_TRACKING:
            oldest = next(iter(self._notify_ws_map))
            del self._notify_ws_map[oldest]
        self._notify_ws_map[ws_id] = (ws_id, room_id)

    async def _recover_routes(self) -> None:
        """Re-subscribe to SSE streams for existing matrix routes."""
        routes = await asyncio.to_thread(
            self.storage.list_channel_routes_by_type, "matrix"
        )
        for route in routes:
            ws_id = route["ws_id"]
            room_id = route["channel_id"]
            await self.subscribe_ws(ws_id, room_id)
            log.info("matrix.route_recovered", ws_id=ws_id, room_id=room_id)
