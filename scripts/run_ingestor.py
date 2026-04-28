#!/usr/bin/env python3
"""
Run a specific ingestor from the command line.

Usage:
  python scripts/run_ingestor.py nrc
  python scripts/run_ingestor.py echo
  python scripts/run_ingestor.py all
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models.database import SessionLocal
from models.incident import IncidentSource
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_ingestor")
settings = get_settings()


def run_nrc():
    from ingestors.nrc import NRCIngestor
    logger.info("Starting NRC ingest from %s", settings.nrc_data_dir)
    with SessionLocal() as db:
        ingestor = NRCIngestor(db, data_dir=settings.nrc_data_dir, batch_size=settings.ingest_batch_size)
        run = ingestor.run()
        # Extract values before session closes to avoid DetachedInstanceError
        status, inserted, skipped, errored = (
            run.status, run.records_inserted, run.records_skipped, run.records_errored
        )
    logger.info(
        "NRC run complete: status=%s inserted=%s skipped=%s errored=%s",
        status, inserted, skipped, errored,
    )


def run_echo():
    from ingestors.echo import ECHOIngestor
    logger.info("Starting ECHO ingest from %s", settings.echo_data_dir)
    with SessionLocal() as db:
        ingestor = ECHOIngestor(db, data_dir=settings.echo_data_dir, batch_size=settings.ingest_batch_size)
        run = ingestor.run()
        ingestor.close()
        # Extract values before session closes to avoid DetachedInstanceError
        status, inserted, skipped, errored = (
            run.status, run.records_inserted, run.records_skipped, run.records_errored
        )
    logger.info(
        "ECHO run complete: status=%s inserted=%s skipped=%s errored=%s",
        status, inserted, skipped, errored,
    )


def run_tri():
    from ingestors.tri import TRIIngestor
    logger.info("Starting TRI ingest from %s", settings.tri_data_dir)
    with SessionLocal() as db:
        ingestor = TRIIngestor(db, data_dir=settings.tri_data_dir, batch_size=settings.ingest_batch_size)
        run = ingestor.run()
        status, inserted, skipped, errored = (
            run.status, run.records_inserted, run.records_skipped, run.records_errored
        )
    logger.info(
        "TRI run complete: status=%s inserted=%s skipped=%s errored=%s",
        status, inserted, skipped, errored,
    )


def main():
    target = sys.argv[1].lower() if len(sys.argv) > 1 else "all"

    if target in ("nrc", "all"):
        run_nrc()

    if target in ("echo", "all"):
        run_echo()

    if target in ("tri", "all"):
        run_tri()

    if target not in ("nrc", "echo", "tri", "all"):
        print(f"Unknown ingestor: {target}. Use: nrc | echo | tri | all")
        sys.exit(1)


if __name__ == "__main__":
    main()
