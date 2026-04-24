#!/usr/bin/env python3
"""
Reset the local development database.

Wipes all incidents and pipeline run records, then confirms the count.
Does NOT drop or recreate tables — schema stays intact.

Usage:
  python scripts/reset_db.py
  python scripts/reset_db.py --confirm   # skip the confirmation prompt
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from models.database import SessionLocal


def main():
    skip_confirm = "--confirm" in sys.argv

    if not skip_confirm:
        print("This will delete ALL incidents and pipeline_runs from the local database.")
        answer = input("Are you sure? (yes/no): ").strip().lower()
        if answer != "yes":
            print("Aborted.")
            sys.exit(0)

    with SessionLocal() as db:
        result = db.execute(text("SELECT COUNT(*) FROM incidents"))
        count_before = result.scalar()

        db.execute(text("DELETE FROM incidents"))
        db.execute(text("DELETE FROM pipeline_runs"))
        db.commit()

        result = db.execute(text("SELECT COUNT(*) FROM incidents"))
        count_after = result.scalar()

    print(f"Done. Removed {count_before} incidents. Current count: {count_after}.")
    print("Run 'python scripts/run_ingestor.py nrc' to re-ingest.")


if __name__ == "__main__":
    main()
