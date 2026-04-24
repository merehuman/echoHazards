"""
echoHazards API

A geospatial REST API for querying US environmental incidents —
oil spills, chemical releases, compliance violations, and contaminated sites —
sourced from federal government databases and made searchable by proximity.

Data sources:
  - NRC: National Response Center spill reports (1990–present)
  - ECHO: EPA Enforcement and Compliance History (1M+ facilities)
  - TRI: EPA Toxics Release Inventory (coming soon)

Interactive API docs: /docs
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from config import get_settings

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


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

app.include_router(router)
