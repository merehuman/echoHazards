"""Initial schema — incidents and pipeline_runs

Revision ID: 001
Revises:
Create Date: 2026-04-23
"""

from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable PostGIS extension (idempotent)
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "incidents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "source",
            sa.Enum("NRC", "ECHO", "TRI", "SUPERFUND", name="incidentsource"),
            nullable=False,
        ),
        sa.Column("source_id", sa.String(256), nullable=False),
        sa.Column(
            "incident_type",
            sa.Enum("spill", "violation", "release", "site", name="incidenttype"),
            nullable=False,
        ),
        sa.Column(
            "severity",
            sa.Enum("critical", "major", "minor", "unknown", name="severity"),
            nullable=True,
        ),
        sa.Column("material", sa.String(512), nullable=True),
        sa.Column("quantity", sa.Float, nullable=True),
        sa.Column("quantity_unit", sa.String(32), nullable=True),
        sa.Column(
            "medium",
            sa.Enum("air", "water", "land", "unknown", name="medium"),
            nullable=True,
        ),
        sa.Column("facility_name", sa.String(512), nullable=True),
        sa.Column("responsible_party", sa.String(512), nullable=True),
        sa.Column("address", sa.Text, nullable=True),
        sa.Column("city", sa.String(128), nullable=True),
        sa.Column("state", sa.String(2), nullable=True),
        sa.Column("zip_code", sa.String(10), nullable=True),
        sa.Column("lat", sa.Float, nullable=False),
        sa.Column("lng", sa.Float, nullable=False),
        sa.Column(
            "location",
            geoalchemy2.types.Geography(geometry_type="POINT", srid=4326),
            nullable=False,
        ),
        sa.Column("incident_date", sa.Date, nullable=True),
        sa.Column("ingested_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=True),
        sa.Column("raw", postgresql.JSONB, nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_id", name="uq_incident_source_id"),
    )

    # Indexes
    op.create_index("ix_incidents_source", "incidents", ["source"])
    op.create_index("ix_incidents_incident_type", "incidents", ["incident_type"])
    op.create_index("ix_incidents_state", "incidents", ["state"])
    op.create_index("ix_incidents_incident_date", "incidents", ["incident_date"])
    op.create_index("ix_incidents_medium", "incidents", ["medium"])
    op.create_index(
        "ix_incidents_source_type_date",
        "incidents",
        ["source", "incident_type", "incident_date"],
    )
    op.create_index(
        "ix_incidents_location",
        "incidents",
        ["location"],
        postgresql_using="gist",
    )

    op.create_table(
        "pipeline_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "source",
            sa.Enum("NRC", "ECHO", "TRI", "SUPERFUND", name="incidentsource"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime, nullable=True),
        sa.Column("status", sa.String(32), nullable=False, default="running"),
        sa.Column("records_fetched", sa.Float, nullable=True),
        sa.Column("records_inserted", sa.Float, nullable=True),
        sa.Column("records_skipped", sa.Float, nullable=True),
        sa.Column("records_errored", sa.Float, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pipeline_runs_source", "pipeline_runs", ["source"])


def downgrade() -> None:
    op.drop_table("pipeline_runs")
    op.drop_index("ix_incidents_location", table_name="incidents", postgresql_using="gist")
    op.drop_table("incidents")
    op.execute("DROP TYPE IF EXISTS incidentsource")
    op.execute("DROP TYPE IF EXISTS incidenttype")
    op.execute("DROP TYPE IF EXISTS severity")
    op.execute("DROP TYPE IF EXISTS medium")
