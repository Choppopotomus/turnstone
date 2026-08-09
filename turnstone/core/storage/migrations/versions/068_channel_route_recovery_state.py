"""Add missed-turn-recovery state to channel_routes.

The Matrix channel adapter's missed-turn-recovery mechanism (bot.py) tracks
reconnect/turn-count state in plain in-memory dicts, so a bot process
restart wipes it and cannot tell "fresh subscription" from "missed turns
while down". These two nullable columns let ``_recover_routes()`` seed that
state from durable storage before resubscribing, so the existing
reconnect-recovery machinery (unchanged) also covers a process restart.

``last_turn_count``: the server's turn_count last observed for this route.
``last_seen_text``: the last assistant text actually delivered to the room,
used by the recovery walk-back's own dedup check.

Both nullable, no default — ``NULL`` means "never recorded", which the bot
treats as "don't seed, fall back to today's first-connect behavior" rather
than a false-positive recovery trigger. Additive and reversible.

Revision ID: 068
Revises: 067
Create Date: 2026-08-09
"""

import sqlalchemy as sa
from alembic import op

revision = "068"
down_revision = "067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("channel_routes") as batch_op:
        batch_op.add_column(sa.Column("last_turn_count", sa.Integer))
        batch_op.add_column(sa.Column("last_seen_text", sa.Text))


def downgrade() -> None:
    with op.batch_alter_table("channel_routes") as batch_op:
        batch_op.drop_column("last_seen_text")
        batch_op.drop_column("last_turn_count")
