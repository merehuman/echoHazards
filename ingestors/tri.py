"""
EPA TRI (Toxics Release Inventory) ingestor.

TRI tracks annual self-reported toxic chemical releases from 21,000+
industrial facilities. Unlike NRC (incidents) and ECHO (violations),
TRI captures intentional ongoing releases — what a facility routinely
emits to air, water, and land each year.

Data source:
  https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present
  Select year + U.S. → download CSV
  Save to data/tri/tri_YYYY_us.csv

Key columns (TRI Basic file):
  FACILITY_NAME, STREET_ADDRESS, CITY, ST, ZIP, LATITUDE, LONGITUDE
  TRIFID (stable facility+chemical identifier)
  CHEMICAL, CAS_NUMBER
  TOTAL_RELEASES (sum of all on-site releases, in pounds)
  ON_SITE_RELEASE_TOTAL
  5.1_FUGITIVE_AIR, 5.2_STACK_AIR  → air releases
  5.3_WATER_DISCHARGE              → water releases
  5.4_UNDERGROUND                  → land/subsurface
  5.5.1A_LANDFILLS                 → land releases
  REPORTING_YEAR
"""

import csv
import logging
import uuid
from datetime import date
from pathlib import Path
from typing import Iterator

from ingestors.base import BaseIngestor
from models.incident import IncidentSource, IncidentType, Medium, Severity

logger = logging.getLogger(__name__)


def _parse_float(val) -> float | None:
    if val is None:
        return None
    try:
        cleaned = str(val).replace(",", "").strip()
        if cleaned in ("", "NA", "None", "null"):
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _classify_medium(row: dict) -> str:
    """
    TRI reports releases to specific media — determine the dominant one
    by comparing quantities released to air vs water vs land.
    Column names after stripping numeric prefix.
    """
    air = (
        (_parse_float(row.get("5.1 FUGITIVE AIR")) or 0) +
        (_parse_float(row.get("5.2 STACK AIR")) or 0)
    )
    water = _parse_float(row.get("5.3 WATER DISCHARGE")) or 0
    land = (
        (_parse_float(row.get("5.4 UNDERGROUND")) or 0) +
        (_parse_float(row.get("5.5.1A LANDFILLS")) or 0) +
        (_parse_float(row.get("5.5.1B LAND TREATMENT")) or 0) +
        (_parse_float(row.get("5.5.2 SURFACE IMPNDMNT")) or 0) +
        (_parse_float(row.get("5.5.3 OTHER DISPOSAL")) or 0)
    )

    if air == 0 and water == 0 and land == 0:
        return Medium.UNKNOWN.value

    dominant = max(
        (Medium.AIR.value, air),
        (Medium.WATER.value, water),
        (Medium.LAND.value, land),
        key=lambda x: x[1],
    )
    return dominant[0]


def _classify_severity(total_lbs: float | None) -> str:
    """Estimate severity from total pounds released."""
    if total_lbs is None or total_lbs <= 0:
        return Severity.UNKNOWN.value
    if total_lbs >= 1_000_000:
        return Severity.CRITICAL.value
    if total_lbs >= 10_000:
        return Severity.MAJOR.value
    return Severity.MINOR.value


class TRIIngestor(BaseIngestor):
    source = IncidentSource.TRI

    def __init__(self, db, data_dir: str = "./data/tri", batch_size: int = 500):
        super().__init__(db, batch_size)
        self.data_dir = Path(data_dir)

    def fetch_records(self) -> Iterator[dict]:
        """Yield one dict per row from all TRI CSV files in data_dir."""
        csv_files = (
            list(self.data_dir.glob("tri_*.csv"))
            + list(self.data_dir.glob("TRI_*.csv"))
            + list(self.data_dir.glob("*.csv"))
        )

        if not csv_files:
            logger.warning(
                "No TRI CSV files found in %s. "
                "Download from https://www.epa.gov/toxics-release-inventory-tri-program/"
                "tri-basic-data-files-calendar-years-1987-present",
                self.data_dir,
            )
            return

        for path in csv_files:
            logger.info("Reading TRI file: %s", path.name)
            try:
                yield from self._read_file(path)
            except Exception as exc:
                logger.error("TRI: failed to read %s: %s", path, exc)

    def _read_file(self, path: Path) -> Iterator[dict]:
        with open(path, encoding="latin-1", errors="replace") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # TRI Basic files prefix every column with "N. " (e.g. "12. LATITUDE")
                # Strip the numeric prefix so we can look up columns by name
                clean = {}
                for k, v in row.items():
                    k = k.strip()
                    # Remove leading "N. " prefix if present
                    if '. ' in k:
                        k = k.split('. ', 1)[1].strip()
                    clean[k] = v.strip() if v else ""
                yield clean
                count += 1
                if count % 50_000 == 0:
                    logger.info("TRI: read %d rows from %s", count, path.name)
            logger.info("TRI: finished %s — %d rows", path.name, count)

    def normalize(self, raw: dict) -> dict | None:
        """Map a TRI row to the unified incident schema."""

        # Coordinates
        lat = _parse_float(raw.get("LATITUDE") or raw.get("LAT"))
        lng = _parse_float(raw.get("LONGITUDE") or raw.get("LONG"))

        if lat is None or lng is None:
            return None
        if lat == 0.0 and lng == 0.0:
            return None
        if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
            return None

        # Chemical identity
        chemical = raw.get("CHEMICAL") or raw.get("CHEMICAL NAME")
        cas = raw.get("CAS#") or raw.get("CAS NUMBER") or raw.get("CAS_NUMBER")

        # Source ID — TRIFD + CAS# gives one unique row per facility+chemical
        trifd = raw.get("TRIFD") or raw.get("TRIFID") or ""
        cas_clean = (cas or "").strip().replace("/", "-")
        if trifd:
            source_id = f"{trifd}_{cas_clean}" if cas_clean else trifd
        else:
            facility = raw.get("FACILITY NAME", "") or raw.get("FACILITY_NAME", "")
            year = raw.get("YEAR", "") or raw.get("REPORTING_YEAR", "")
            source_id = f"{facility}_{chemical}_{year}"

        if not source_id or not source_id.strip():
            return None

        # Quantity — total on-site releases in pounds
        total_releases = (
            _parse_float(raw.get("ON-SITE RELEASE TOTAL"))
            or _parse_float(raw.get("ON_SITE_RELEASE_TOTAL"))
            or _parse_float(raw.get("TOTAL RELEASES"))
            or _parse_float(raw.get("TOTAL_RELEASES"))
        )

        medium = _classify_medium(raw)
        severity = _classify_severity(total_releases)

        # Incident date — use Jan 1 of the reporting year
        year_str = raw.get("YEAR") or raw.get("REPORTING_YEAR")
        incident_date = None
        if year_str:
            try:
                incident_date = date(int(str(year_str).strip()), 1, 1)
            except (ValueError, TypeError):
                pass

        state = (raw.get("ST") or raw.get("STATE") or "").strip()

        # Material description — truncate to fit varchar(512)
        material = chemical or "Unknown chemical"
        if cas and cas.strip() and cas.strip() not in ("N/A", "NA"):
            material = f"{material} (CAS {cas.strip()})"
        material = material[:512] if material else material

        facility_name = raw.get("FACILITY NAME") or raw.get("FACILITY_NAME")
        parent_co = raw.get("PARENT CO NAME") or raw.get("PARENT_CO_NAME")

        return {
            "id": uuid.uuid4(),
            "source": IncidentSource.TRI.value,
            "source_id": str(source_id).strip(),
            "incident_type": IncidentType.RELEASE.value,
            "severity": severity,
            "material": material,
            "quantity": total_releases,
            "quantity_unit": "lbs",
            "medium": medium,
            "facility_name": facility_name,
            "responsible_party": parent_co or facility_name,
            "address": raw.get("STREET ADDRESS") or raw.get("STREET_ADDRESS"),
            "city": raw.get("CITY"),
            "state": state[:2] if state else None,
            "zip_code": raw.get("ZIP"),
            "lat": lat,
            "lng": lng,
            "location": f"SRID=4326;POINT({lng} {lat})",
            "incident_date": incident_date,
            "raw": {k: v for k, v in raw.items()},
        }
