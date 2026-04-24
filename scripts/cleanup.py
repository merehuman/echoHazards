#!/usr/bin/env python3
"""
Clean up build artifacts and temporary files before pushing to GitHub.

Removes:
  - __pycache__ directories
  - .pyc / .pyo compiled files
  - pytest cache
  - mypy cache
  - .venv (if accidentally not gitignored)
  - alembic.ini pyc artifacts

Does NOT remove:
  - data/nrc/ xlsx files (already gitignored)
  - .env (already gitignored)

Usage:
  python scripts/cleanup.py
  python scripts/cleanup.py --dry-run   # show what would be removed without deleting
"""

import os
import sys
import shutil
from pathlib import Path

ROOT = Path(__file__).parent.parent

REMOVE_DIRS = [
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
]

REMOVE_EXTENSIONS = [".pyc", ".pyo"]

DRY_RUN = "--dry-run" in sys.argv


def remove(path: Path):
    label = "WOULD REMOVE" if DRY_RUN else "Removing"
    print(f"  {label}: {path.relative_to(ROOT)}")
    if not DRY_RUN:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def main():
    removed = 0

    for dirpath in ROOT.rglob("*"):
        if not dirpath.is_dir():
            continue
        # Skip .venv entirely
        if ".venv" in dirpath.parts:
            continue
        if dirpath.name in REMOVE_DIRS:
            remove(dirpath)
            removed += 1

    for ext in REMOVE_EXTENSIONS:
        for filepath in ROOT.rglob(f"*{ext}"):
            if ".venv" in filepath.parts:
                continue
            remove(filepath)
            removed += 1

    if removed == 0:
        print("Nothing to clean up.")
    elif DRY_RUN:
        print(f"\nDry run complete. {removed} items would be removed.")
    else:
        print(f"\nDone. {removed} items removed.")


if __name__ == "__main__":
    main()
