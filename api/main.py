"""
echoHazards API

A geospatial REST API for querying US environmental incidents —
oil spills, chemical releases, compliance violations, and contaminated sites —
sourced from federal government databases and made searchable by proximity.

Data sources:
  - NRC: National Response Center spill reports (2024–present)
  - ECHO: EPA Enforcement and Compliance History (163K+ facilities)
  - TRI: EPA Toxics Release Inventory (77K+ chemical releases)

Interactive API docs: /docs
Frontend map: /
"""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.routes import router
from config import get_settings

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("echoHazards API starting up (env=%s)", settings.environment)
    yield
    logger.info("echoHazards API shutting down")


app = FastAPI(
    title="echoHazards API",
    description=__doc__,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve the frontend map at the root
@app.get("/", include_in_schema=False)
async def serve_frontend():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "echoHazards API — see /docs for API documentation"}

app.include_router(router)
