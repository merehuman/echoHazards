"""
EPA ECHO (Enforcement and Compliance History Online) ingestor.

Covers 1M+ regulated facilities with violations across Clean Air Act,
Clean Water Act, RCRA hazardous waste, and Safe Drinking Water Act.

API docs: https://echo.epa.gov/tools/web-services
No API key required. Be respectful of rate limits.

Strategy: query by state, paginate through all facilities with recent violations.
This gives us nationwide coverage without needing to guess bounding boxes.
"""

import logging
import time
import uuid
from datetime import date, datetime
from typing import Iterator

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings
from models.incident import IncidentSource, IncidentType, Medium, Severity
from ingestors.base import BaseIngestor

logger = logging.getLogger(__name__)

settings = get_settings()

# All US states + territories to iterate over
_US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI",
]

ECHO_BASE = "https://echo.epa.gov/Rest/api"


class ECHOIngestor(BaseIngestor):
    source = IncidentSource.ECHO

    def __init__(self, db, batch_size: int = 500):
        super().__init__(db, batch_size)
        self.client = httpx.Client(
            timeout=30.0,
            headers={
                "User-Agent": settings.echo_user_agent,
                "Accept": "application/json",
            },
        )

    def fetch_records(self) -> Iterator[dict]:
        """
        Query ECHO for facilities with recent violations, state by state.
        Uses the QID pagination pattern: get_facilities → get_qid → results.
        """
        for state in _US_STATES:
            logger.info("ECHO: fetching facilities for state=%s", state)
            try:
                yield from self._fetch_state(state)
            except Exception as exc:
                logger.error("ECHO: failed to fetch state %s: %s", state, exc)
            # Respectful rate limiting
            time.sleep(settings.echo_rate_limit_delay)

    def _fetch_state(self, state: str) -> Iterator[dict]:
        """Fetch all facilities with violations in a given state."""
        qid = self._get_qid(state)
        if not qid:
            return

        page = 1
        while True:
            records = self._get_page(qid, page)
            if not records:
                break

            for record in records:
                yield record

            if len(records) < 100:
                break  # Last page
            page += 1
            time.sleep(0.2)  # Brief pause between pages

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def _get_qid(self, state: str) -> str | None:
        """
        Step 1 of the ECHO pagination pattern: get a query ID (QID).
        The QID represents a cached query result on ECHO's servers, valid ~30 min.
        """
        resp = self.client.get(
            f"{ECHO_BASE}/air_rest_services.get_facilities",
            params={
                "output": "JSON",
                "p_st": state,
                "p_vio_flag": "Y",          # only facilities with violations
                "p_act": "Y",               # only active facilities
                "responseset": 100,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        results = data.get("Results", {})
        qid = results.get("QueryID")

        if not qid:
            logger.debug("ECHO: no QID returned for state=%s (may have 0 results)", state)
            return None

        return qid

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def _get_page(self, qid: str, page: int) -> list[dict]:
        """Step 2: paginate through results using the QID."""
        resp = self.client.get(
            f"{ECHO_BASE}/air_rest_services.get_qid",
            params={
                "output": "JSON",
                "qid": qid,
                "pageno": page,
                "responseset": 100,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        return data.get("Results", {}).get("AIRFacilities", [])

    def normalize(self, raw: dict) -> dict | None:
        """Map an ECHO facility record to the unified incident schema."""
        lat_raw = raw.get("FacLat") or raw.get("Latitude83")
        lng_raw = raw.get("FacLong") or raw.get("Longitude83")

        try:
            lat = float(lat_raw)
            lng = float(lng_raw)
        except (TypeError, ValueError):
            return None

        if lat == 0.0 and lng == 0.0:
            return None

        if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
            return None

        source_id = raw.get("RegistryID") or raw.get("ProgramSystemAcronym") or raw.get("FacilityName")
        if not source_id:
            return None

        # Determine severity from violation flag fields
        severity = self._classify_severity(raw)

        return {
            "id": uuid.uuid4(),
            "source": IncidentSource.ECHO.value,
            "source_id": str(source_id),
            "incident_type": IncidentType.VIOLATION.value,
            "severity": severity,
            "material": raw.get("AirPollutantsDesc"),
            "quantity": None,
            "quantity_unit": None,
            "medium": Medium.AIR.value,  # Air rest services → air medium
            "facility_name": raw.get("FacilityName"),
            "responsible_party": raw.get("FacilityName"),
            "address": raw.get("LocationAddress"),
            "city": raw.get("CityName"),
            "state": raw.get("StateCode"),
            "zip_code": raw.get("ZipCode"),
            "lat": lat,
            "lng": lng,
            "location": f"SRID=4326;POINT({lng} {lat})",
            "incident_date": self._parse_most_recent_violation(raw),
            "raw": raw,
        }

    def _classify_severity(self, raw: dict) -> str:
        """
        Estimate severity from ECHO penalty and violation fields.
        ECHO doesn't have a single severity field, so we infer from context.
        """
        penalty = raw.get("TotalPenalties") or "0"
        try:
            penalty_val = float(str(penalty).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            penalty_val = 0

        if penalty_val >= 100_000:
            return Severity.CRITICAL.value
        if penalty_val >= 10_000:
            return Severity.MAJOR.value
        if penalty_val > 0:
            return Severity.MINOR.value

        # Fallback: check for formal enforcement actions
        if raw.get("FormalEnfActions", "0") not in ("0", None, ""):
            return Severity.MAJOR.value

        return Severity.UNKNOWN.value

    def _parse_most_recent_violation(self, raw: dict) -> date | None:
        """Extract the date of the most recent violation."""
        date_str = raw.get("MostRecentInspectionDate") or raw.get("LastInspDate")
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    def close(self):
        self.client.close()
