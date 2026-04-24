"""
Base ingestor class.

All source-specific ingestors inherit from this and implement `fetch_records()`
and `normalize()`. The base class handles the run lifecycle: logging, deduplication,
batch upserts, pipeline_run tracking, and completeness checks.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from models.incident import Incident, IncidentSource, PipelineRun

logger = logging.getLogger(__name__)


class IngestorError(Exception):
    pass


class BaseIngestor(ABC):
    source: IncidentSource

    def __init__(self, db: Session, batch_size: int = 500):
        self.db = db
        self.batch_size = batch_size

    @abstractmethod
    def fetch_records(self) -> Iterator[dict]:
        """
        Yield raw records from the source, one dict at a time.
        Each dict is stored verbatim in the `raw` column.
        """
        ...

    @abstractmethod
    def normalize(self, raw: dict) -> dict | None:
        """
        Map a raw source record to the Incident column dict.
        Return None to skip the record (e.g. missing coordinates).
        Should never raise — log and return None on bad records.
        """
        ...

    def run(self) -> PipelineRun:
        """Execute a full ingest run. Returns the completed PipelineRun record."""
        run = PipelineRun(
            id=uuid.uuid4(),
            source=self.source,
            started_at=datetime.utcnow(),
            status="running",
        )
        self.db.add(run)
        self.db.commit()

        fetched = inserted = skipped = errored = 0
        batch: list[dict] = []

        try:
            for raw_record in self.fetch_records():
                fetched += 1

                try:
                    normalized = self.normalize(raw_record)
                except Exception as exc:
                    logger.warning(
                        "normalize() raised on %s record: %s", self.source.value, exc
                    )
                    errored += 1
                    continue

                if normalized is None:
                    skipped += 1
                    continue

                # Ensure required geo field is set from lat/lng
                if "location" not in normalized:
                    normalized["location"] = f"SRID=4326;POINT({normalized['lng']} {normalized['lat']})"

                batch.append(normalized)

                if len(batch) >= self.batch_size:
                    n_ins, n_skip = self._upsert_batch(batch)
                    inserted += n_ins
                    skipped += n_skip
                    batch.clear()
                    logger.info(
                        "%s: fetched=%d inserted=%d skipped=%d errored=%d",
                        self.source.value,
                        fetched,
                        inserted,
                        skipped,
                        errored,
                    )

            # Flush remaining
            if batch:
                n_ins, n_skip = self._upsert_batch(batch)
                inserted += n_ins
                skipped += n_skip

            self._completeness_check(run, fetched, inserted)

            run.status = "success"
            logger.info(
                "%s run complete: fetched=%d inserted=%d skipped=%d errored=%d",
                self.source.value,
                fetched,
                inserted,
                skipped,
                errored,
            )

        except Exception as exc:
            run.status = "error"
            run.error_message = str(exc)
            logger.error("%s run failed: %s", self.source.value, exc, exc_info=True)
            self.db.rollback()

        finally:
            run.finished_at = datetime.utcnow()
            run.records_fetched = fetched
            run.records_inserted = inserted
            run.records_skipped = skipped
            run.records_errored = errored
            self.db.add(run)
            self.db.commit()

        return run

    def _upsert_batch(self, batch: list[dict]) -> tuple[int, int]:
        """
        Insert batch one record at a time to avoid SQLAlchemy parameter
        naming collisions that occur with large bulk value lists.
        Skips records that already exist (by source + source_id).
        Returns (inserted_count, skipped_count).
        """
        import json
        from sqlalchemy import cast
        from sqlalchemy.dialects.postgresql import JSONB

        inserted = 0
        skipped = 0
        for record in batch:
            try:
                # Serialize raw dict to JSON string for JSONB insertion
                r = dict(record)
                if isinstance(r.get("raw"), dict):
                    r["raw_json"] = json.dumps(r.pop("raw"))
                else:
                    r["raw_json"] = json.dumps({})

                stmt = text("""
                    INSERT INTO incidents (
                        id, source, source_id, incident_type, severity,
                        material, quantity, quantity_unit, medium,
                        facility_name, responsible_party, address,
                        city, state, zip_code, lat, lng, location,
                        incident_date, ingested_at, raw
                    ) VALUES (
                        :id, :source, :source_id, :incident_type, :severity,
                        :material, :quantity, :quantity_unit, :medium,
                        :facility_name, :responsible_party, :address,
                        :city, :state, :zip_code, :lat, :lng,
                        ST_GeogFromText(:location),
                        :incident_date, now(), cast(:raw_json as jsonb)
                    )
                    ON CONFLICT ON CONSTRAINT uq_incident_source_id DO NOTHING
                """)
                result = self.db.execute(stmt, r)
                self.db.commit()
                if result.rowcount:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as exc:
                self.db.rollback()
                logger.warning(
                    "Failed to insert record %s: %s\nProblematic fields: id=%s source=%s incident_type=%s medium=%s severity=%s",
                    record.get("source_id"), exc,
                    type(record.get("id")),
                    repr(record.get("source")),
                    repr(record.get("incident_type")),
                    repr(record.get("medium")),
                    repr(record.get("severity")),
                )
                skipped += 1
        return inserted, skipped

    def _completeness_check(self, run: PipelineRun, fetched: int, inserted: int) -> None:
        """
        Warn if this run looks anomalous compared to the previous successful run.
        Anomalies don't fail the run — they add a note for the /pipeline/status endpoint.
        """
        notes = []

        if fetched == 0:
            notes.append("WARNING: zero records fetched — source may be unavailable.")

        # Compare against previous run
        prev_run = (
            self.db.query(PipelineRun)
            .filter(
                PipelineRun.source == self.source,
                PipelineRun.status == "success",
                PipelineRun.id != run.id,
            )
            .order_by(PipelineRun.started_at.desc())
            .first()
        )

        if prev_run and prev_run.records_fetched and fetched > 0:
            drop_pct = (prev_run.records_fetched - fetched) / prev_run.records_fetched
            if drop_pct > 0.20:
                msg = (
                    f"WARNING: fetched {fetched} records vs {int(prev_run.records_fetched)} "
                    f"last run ({drop_pct:.0%} drop). Possible source issue."
                )
                notes.append(msg)
                logger.warning(msg)

        if notes:
            run.notes = " | ".join(notes)
