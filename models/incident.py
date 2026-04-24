"""
Unified incident model.

Every data source (NRC, ECHO, TRI) normalizes into this schema.
The `raw` column preserves the full original record for auditability —
normalized fields can always be re-derived from it.
"""

from datetime import date, datetime
from enum import Enum as PyEnum
from typing import Optional

from geoalchemy2 import Geography
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase
import uuid


class Base(DeclarativeBase):
    pass


# These enums are used for validation and documentation only —
# the database columns are plain String types to avoid SQLAlchemy
# enum lookup conflicts with Postgres native enum types.

class IncidentSource(str, PyEnum):
    NRC = "NRC"
    ECHO = "ECHO"
    TRI = "TRI"
    SUPERFUND = "SUPERFUND"


class IncidentType(str, PyEnum):
    SPILL = "spill"
    VIOLATION = "violation"
    RELEASE = "release"
    SITE = "site"


class Severity(str, PyEnum):
    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"
    UNKNOWN = "unknown"


class Medium(str, PyEnum):
    AIR = "air"
    WATER = "water"
    LAND = "land"
    UNKNOWN = "unknown"


class Incident(Base):
    __tablename__ = "incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    source = Column(String(32), nullable=False, index=True)
    source_id = Column(String(256), nullable=False)

    incident_type = Column(String(32), nullable=False, index=True)
    severity = Column(String(32), nullable=True)

    material = Column(String(512), nullable=True)
    quantity = Column(Float, nullable=True)
    quantity_unit = Column(String(32), nullable=True)
    medium = Column(String(32), nullable=True, index=True)

    facility_name = Column(String(512), nullable=True)
    responsible_party = Column(String(512), nullable=True)
    address = Column(Text, nullable=True)
    city = Column(String(128), nullable=True)
    state = Column(String(2), nullable=True, index=True)
    zip_code = Column(String(10), nullable=True)

    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    location = Column(Geography(geometry_type="POINT", srid=4326), nullable=False)

    incident_date = Column(Date, nullable=True, index=True)
    ingested_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    raw = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_incident_source_id"),
        Index("ix_incidents_location", "location", postgresql_using="gist"),
        Index("ix_incidents_source_type_date", "source", "incident_type", "incident_date"),
    )

    def __repr__(self) -> str:
        return f"<Incident {self.source}:{self.source_id} {self.incident_type} @ {self.lat},{self.lng}>"


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(32), nullable=False, index=True)
    started_at = Column(DateTime, nullable=False, default=func.now())
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(32), nullable=False, default="running")
    records_fetched = Column(Float, nullable=True)
    records_inserted = Column(Float, nullable=True)
    records_skipped = Column(Float, nullable=True)
    records_errored = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
