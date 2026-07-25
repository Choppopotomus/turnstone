"""Matrix-specific configuration."""

from __future__ import annotations

from dataclasses import dataclass

from turnstone.channels._config import ChannelConfig


@dataclass
class MatrixConfig(ChannelConfig):
    """Configuration for the Matrix channel adapter.

    Uses matrix-nio for async Matrix client with optional E2EE.
    Connects to a self-hosted homeserver (Tuwunel, Synapse, etc.).
    """

    homeserver: str = "http://127.0.0.1:6167"
    user_id: str = "@turnstone:matrix.local"
    password: str = ""
    device_id: str = "TURNSTONE"
    store_path: str = "/Users/c/.local/share/turnstone/matrix-store"
    allowed_rooms: list[str] = ""  # empty = all rooms
    max_message_length: int = 16384  # Matrix supports large messages
