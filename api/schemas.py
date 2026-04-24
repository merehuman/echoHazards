from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class IncidentResponse(BaseModel):
    id: UUID
    source: str
    source_id: str
    incident_type: str
    severity: Optional[str]
    material: Optional[str]
    quantity: Optional[float]
    quantity_unit: Optional[str]
    medium: Optional[str]
    facility_name: Optional[str]
    responsible_party: Optional[str]
    city: Optional[str]
    state: Optional[str]
    lat: float
    lng: float
    incident_date: Optional[date]
    ingested_at: datetime

    class Config:
        from_attributes = True


class IncidentListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[IncidentResponse]


class PipelineRunResponse(BaseModel):
    id: UUID
    source: str
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    records_fetched: Optional[float]
    records_inserted: Optional[float]
    records_skipped: Optional[float]
    records_errored: Optional[float]
    error_message: Optional[str]
    notes: Optional[str]

    class Config:
        from_attributes = True


class PipelineStatusResponse(BaseModel):
    sources: dict[str, PipelineRunResponse | None]
    total_incidents: int


class HealthResponse(BaseModel):
    status: str
    database: str
    version: str = "1.0.0"
