"""Сохранять состояние доставки webhook между запусками consumer."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("payments", sa.Column("webhook_next_attempt_at", sa.DateTime(timezone=True)))
    op.add_column("payments", sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)))
    op.add_column("payments", sa.Column("webhook_last_error", sa.Text()))
    op.create_check_constraint(
        "ck_payments_webhook_attempts", "payments", "webhook_attempts BETWEEN 0 AND 3"
    )


def downgrade() -> None:
    op.drop_constraint("ck_payments_webhook_attempts", "payments", type_="check")
    op.drop_column("payments", "webhook_last_error")
    op.drop_column("payments", "webhook_delivered_at")
    op.drop_column("payments", "webhook_next_attempt_at")
    op.drop_column("payments", "webhook_attempts")
