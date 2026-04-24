"""
API routes for the echoHazards service.

Key design decisions:
- Proximity queries use PostGIS ST_DWithin on a Geography column.
  This is accurate great-circle distance, not bounding-box math.
- All list endpoints are paginated — never return unbounded result sets.
- The /incidents/{id} endpoint exposes the raw source record for transparency.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2.functions import ST_DWithin, ST_MakePoint, ST_SetSRID
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from models.database import get_db
from models.incident import Incident, IncidentSource, IncidentType, Medium, PipelineRun
from api.schemas import (
    HealthResponse,
    IncidentListResponse,
    IncidentResponse,
    PipelineRunResponse,
    PipelineStatusResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["system"])
def health_check(db: Session = Depends(get_db)):
    """Service health check. Verifies database connectivity."""
    try:
        db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        db_status = "error"
    return HealthResponse(status="ok" if db_status == "ok" else "degraded", database=db_status)


@router.get("/incidents", response_model=IncidentListResponse, tags=["incidents"])
def list_incidents(
    # Proximity — required together
    lat: Optional[float] = Query(None, ge=-90, le=90, description="Latitude of search center"),
    lng: Optional[float] = Query(None, ge=-180, le=180, description="Longitude of search center"),
    radius_km: float = Query(25.0, gt=0, le=500, description="Search radius in kilometers"),
    # Filters
    source: Optional[str] = Query(None, description="Filter by source: NRC, ECHO, TRI"),
    incident_type: Optional[str] = Query(None, description="Filter by type: spill, violation, release, site"),
    medium: Optional[str] = Query(None, description="Filter by medium: air, water, land"),
    state: Optional[str] = Query(None, min_length=2, max_length=2, description="Two-letter US state code"),
    since: Optional[str] = Query(None, description="Filter incidents on or after this date (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="Filter incidents on or before this date (YYYY-MM-DD)"),
    # Pagination
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Search for environmental incidents.

    When lat/lng are provided, results are returned sorted by distance (nearest first).
    Without lat/lng, results are sorted by incident_date descending.

    All filters are combinable. Example:
      GET /incidents?lat=37.77&lng=-122.41&radius_km=10&source=NRC&since=2020-01-01
    """
    q = db.query(Incident)

    # Proximity filter
    if lat is not None and lng is not None:
        # ST_DWithin on Geography uses meters
        point = ST_SetSRID(ST_MakePoint(lng, lat), 4326)
        q = q.filter(ST_DWithin(Incident.location, point, radius_km * 1000))
    elif lat is not None or lng is not None:
        raise HTTPException(
            status_code=422,
            detail="Provide both lat and lng, or neither.",
        )

    # Categorical filters
    if source:
        try:
            q = q.filter(Incident.source == IncidentSource(source.upper()))
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Unknown source: {source}")

    if incident_type:
        try:
            q = q.filter(Incident.incident_type == IncidentType(incident_type.lower()))
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Unknown incident_type: {incident_type}")

    if medium:
        try:
            q = q.filter(Incident.medium == Medium(medium.lower()))
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Unknown medium: {medium}")

    if state:
        q = q.filter(Incident.state == state.upper())

    # Date range filters
    if since:
        q = q.filter(Incident.incident_date >= since)
    if until:
        q = q.filter(Incident.incident_date <= until)

    total = q.count()

    # Sort: by distance if proximity search, else by date
    if lat is not None:
        point = ST_SetSRID(ST_MakePoint(lng, lat), 4326)
        q = q.order_by(ST_DWithin(Incident.location, point, radius_km * 1000))
    else:
        q = q.order_by(Incident.incident_date.desc().nullslast(), Incident.ingested_at.desc())

    # Paginate
    offset = (page - 1) * page_size
    incidents = q.offset(offset).limit(page_size).all()

    return IncidentListResponse(
        total=total,
        page=page,
        page_size=page_size,
        results=incidents,
    )


@router.get("/incidents/{incident_id}", tags=["incidents"])
def get_incident(incident_id: UUID, db: Session = Depends(get_db)):
    """
    Get a single incident by ID.
    Includes the full `raw` field — the original unmodified source record.
    This supports transparency and allows independent verification of our data.
    """
    incident = db.query(Incident).filter(Incident.id == incident_id).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    # Return full record including raw
    result = IncidentResponse.model_validate(incident).model_dump()
    result["raw"] = incident.raw
    return result


@router.get("/incidents/summary/by-state", tags=["incidents"])
def incidents_by_state(
    source: Optional[str] = Query(None),
    incident_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Count of incidents per state. Useful for a choropleth map view."""
    q = db.query(Incident.state, func.count(Incident.id).label("count"))

    if source:
        q = q.filter(Incident.source == source.upper())
    if incident_type:
        q = q.filter(Incident.incident_type == incident_type.lower())

    rows = q.group_by(Incident.state).order_by(func.count(Incident.id).desc()).all()
    return {"results": [{"state": r.state, "count": r.count} for r in rows]}


@router.get("/pipeline/status", response_model=PipelineStatusResponse, tags=["pipeline"])
def pipeline_status(db: Session = Depends(get_db)):
    """
    Returns the most recent pipeline run for each source, plus total incident count.
    Use this to verify data freshness and catch ingestion anomalies.
    """
    sources_status = {}
    for source in IncidentSource:
        last_run = (
            db.query(PipelineRun)
            .filter(PipelineRun.source == source)
            .order_by(PipelineRun.started_at.desc())
            .first()
        )
        sources_status[source.value] = last_run

    total = db.query(func.count(Incident.id)).scalar()

    return PipelineStatusResponse(sources=sources_status, total_incidents=total)
