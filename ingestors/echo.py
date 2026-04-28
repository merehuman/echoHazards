"""
EPA ECHO (Enforcement and Compliance History Online) ingestor.

Uses the ECHO Exporter bulk download instead of the REST API.
The Exporter is more reliable, updated weekly, and covers 1.5M+ facilities
across all programs: CAA, CWA, RCRA, SDWA.

Data source:
  https://echo.epa.gov/tools/data-downloads (ECHO Exporter ZIP, ~392MB)
  Download and extract to data/echo/ECHO_EXPORTER.csv (or similar name)

Key columns used:
  FAC_NAME, FAC_STREET, FAC_CITY, FAC_STATE, FAC_ZIP
  FAC_LAT, FAC_LONG (decimal degrees)
  REGISTRY_ID (stable facility identifier)
  FAC_PENALTY_COUNT, FAC_TOTAL_PENALTIES
  FAC_INSPECTION_COUNT, FAC_DATE_LAST_INSPECTION
  CAA_VIOLATIONS_FOUND, CWA_VIOLATIONS_FOUND,
  RCRA_VIOLATIONS_FOUND, SDWA_VIOLATIONS_FOUND
  FAC_FORMAL_ACTION_COUNT
"""

import csv
import logging
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from ingestors.base import BaseIngestor
from models.incident import IncidentSource, IncidentType, Medium, Severity

logger = logging.getLogger(__name__)

# Correct column names from actual ECHO Exporter CSV
_VIOLATION_FIELDS = [
    "CAA_QTRS_WITH_NC",
    "CWA_QTRS_WITH_NC",
    "RCRA_QTRS_WITH_NC",
    "FAC_QTRS_WITH_NC",
    "FAC_SNC_FLG",
    "CAA_FORMAL_ACTION_COUNT",
]


def _parse_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _parse_date(val) -> date | None:
    if not val or str(val).strip() in ("", "None", "null"):
        return None
    raw = str(val).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _has_violation(row: dict) -> bool:
    """Return True if the facility has any recorded violations."""
    # Check quarters with non-compliance
    for field in ["CAA_QTRS_WITH_NC", "CWA_QTRS_WITH_NC", "RCRA_QTRS_WITH_NC", "FAC_QTRS_WITH_NC"]:
        val = row.get(field, "").strip()
        if val and val not in ("0", "", "None", "null"):
            try:
                if float(val) > 0:
                    return True
            except ValueError:
                pass
    # Check SNC flag
    if row.get("FAC_SNC_FLG", "").strip().upper() == "Y":
        return True
    # Check formal actions
    formal = row.get("CAA_FORMAL_ACTION_COUNT", "").strip()
    if formal and formal not in ("0", ""):
        try:
            if float(formal) > 0:
                return True
        except ValueError:
            pass
    return False


def _classify_medium(row: dict) -> str:
    """Infer medium from which program has non-compliance quarters."""
    def nonzero(field):
        val = row.get(field, "").strip()
        if not val or val in ("0", "", "None"):
            return False
        try:
            return float(val) > 0
        except ValueError:
            return False

    if nonzero("CWA_QTRS_WITH_NC"):
        return Medium.WATER.value
    if nonzero("CAA_QTRS_WITH_NC"):
        return Medium.AIR.value
    if nonzero("RCRA_QTRS_WITH_NC"):
        return Medium.LAND.value

    # Fallback: compliance status strings
    cwa = row.get("CWA_COMPLIANCE_STATUS", "").lower()
    caa = row.get("CAA_COMPLIANCE_STATUS", "").lower()
    rcra = row.get("RCRA_COMPLIANCE_STATUS", "").lower()
    if any(x in cwa for x in ["violation", "nc ", "non-complian"]):
        return Medium.WATER.value
    if any(x in caa for x in ["violation", "nc ", "non-complian"]):
        return Medium.AIR.value
    if any(x in rcra for x in ["violation", "nc ", "non-complian"]):
        return Medium.LAND.value

    return Medium.UNKNOWN.value


def _classify_severity(row: dict) -> str:
    """Estimate severity from penalty amounts and SNC flags."""
    for field in ["CAA_PENALTIES", "CWA_PENALTIES", "RCRA_PENALTIES"]:
        penalty = _parse_float(row.get(field) or "0") or 0.0
        if penalty >= 100_000:
            return Severity.CRITICAL.value
        if penalty >= 10_000:
            return Severity.MAJOR.value
        if penalty > 0:
            return Severity.MINOR.value

    if row.get("FAC_SNC_FLG", "").strip().upper() == "Y":
        return Severity.MAJOR.value

    formal = _parse_float(row.get("CAA_FORMAL_ACTION_COUNT") or "0") or 0.0
    if formal > 0:
        return Severity.MAJOR.value

    return Severity.UNKNOWN.value


def _violation_description(row: dict) -> str:
    """Build a human-readable description of which programs have violations."""
    programs = []
    mapping = [
        ("CAA_QTRS_WITH_NC", "Clean Air Act"),
        ("CWA_QTRS_WITH_NC", "Clean Water Act"),
        ("RCRA_QTRS_WITH_NC", "Hazardous Waste (RCRA)"),
        ("SDWA_SNC_FLAG",     "Safe Drinking Water Act"),
    ]
    for field, label in mapping:
        val = row.get(field, "").strip()
        if not val or val in ("0", "", "N", "None"):
            continue
        try:
            if float(val) > 0:
                programs.append(label)
        except ValueError:
            if val.upper() == "Y":
                programs.append(label)

    return (", ".join(programs) + " violation") if programs else "Regulatory violation"


class ECHOIngestor(BaseIngestor):
    source = IncidentSource.ECHO

    def __init__(self, db, data_dir: str = "./data/echo", batch_size: int = 500):
        super().__init__(db, batch_size)
        self.data_dir = Path(data_dir)

    def fetch_records(self) -> Iterator[dict]:
        """
        Read the ECHO Exporter CSV and yield one dict per facility
        that has at least one recorded violation.
        """
        csv_files = (
            list(self.data_dir.glob("ECHO_EXPORTER*.csv"))
            + list(self.data_dir.glob("echo_exporter*.csv"))
            + list(self.data_dir.glob("*.csv"))
        )

        if not csv_files:
            logger.warning(
                "No ECHO CSV files found in %s. "
                "Download the ECHO Exporter ZIP from "
                "https://echo.epa.gov/tools/data-downloads and extract to %s",
                self.data_dir, self.data_dir,
            )
            return

        for path in csv_files:
            logger.info("Reading ECHO Exporter: %s", path.name)
            yield from self._read_file(path)

    def _read_file(self, path: Path) -> Iterator[dict]:
        try:
            with open(path, encoding="latin-1", errors="replace") as f:
                reader = csv.DictReader(f)
                total = 0
                yielded = 0
                for row in reader:
                    total += 1
                    # Strip whitespace from all values
                    row = {k: (v.strip() if v else "") for k, v in row.items()}
                    if _has_violation(row):
                        yielded += 1
                        yield row
                    if total % 100_000 == 0:
                        logger.info(
                            "ECHO: scanned %d rows, %d with violations so far",
                            total, yielded,
                        )
                logger.info(
                    "ECHO: finished %s — %d total rows, %d with violations",
                    path.name, total, yielded,
                )
        except Exception as exc:
            logger.error("ECHO: failed to read %s: %s", path, exc)

    def normalize(self, raw: dict) -> dict | None:
        """Map an ECHO Exporter row to the unified incident schema."""

        # Coordinates
        lat = _parse_float(raw.get("FAC_LAT") or raw.get("LATITUDE83"))
        lng = _parse_float(raw.get("FAC_LONG") or raw.get("LONGITUDE83"))

        if lat is None or lng is None:
            return None
        if lat == 0.0 and lng == 0.0:
            return None
        if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
            return None

        # Source ID
        source_id = raw.get("REGISTRY_ID") or raw.get("FRS_ID")
        if not source_id or not source_id.strip():
            return None

        state = (raw.get("FAC_STATE") or "").strip()

        return {
            "id": uuid.uuid4(),
            "source": IncidentSource.ECHO.value,
            "source_id": str(source_id).strip(),
            "incident_type": IncidentType.VIOLATION.value,
            "severity": _classify_severity(raw),
            "material": _violation_description(raw),
            "quantity": None,
            "quantity_unit": None,
            "medium": _classify_medium(raw),
            "facility_name": raw.get("FAC_NAME"),
            "responsible_party": raw.get("FAC_NAME"),
            "address": raw.get("FAC_STREET"),
            "city": raw.get("FAC_CITY"),
            "state": state[:2] if state else None,
            "zip_code": raw.get("FAC_ZIP"),
            "lat": lat,
            "lng": lng,
            "location": f"SRID=4326;POINT({lng} {lat})",
            "incident_date": _parse_date(raw.get("FAC_DATE_LAST_INSPECTION")),
            "raw": {k: v for k, v in raw.items()},
        }

    def close(self):
        pass  # No HTTP client to close
