"""v002-add-tier-and-max-execution-seconds"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'd4c1a20778cd'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("job_definitions", sa.Column("max_execution_seconds", sa.Integer(), nullable=True))
    op.execute("UPDATE job_definitions SET max_execution_seconds = 120 WHERE max_execution_seconds IS NULL")
    op.alter_column("job_definitions", "max_execution_seconds", existing_type=sa.Integer(), nullable=False)
    op.add_column("job_definitions", sa.Column("tier", sa.String(), nullable=True))
    op.execute("UPDATE job_definitions SET tier = 'light' WHERE tier IS NULL")
    op.alter_column("job_definitions", "tier", existing_type=sa.String(), nullable=False)


def downgrade() -> None:
    op.drop_column("job_definitions", "tier")
    op.drop_column("job_definitions", "max_execution_seconds")
