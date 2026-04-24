# echoHazards

A Python data pipeline and geospatial REST API that collects US government environmental incident records — oil spills, chemical releases, and industrial compliance violations — and makes them searchable by location.

**Live API:** https://echo-hazards.yourdomain.com/docs  
**Data coverage:** NRC spill reports (2024–present) · EPA ECHO facility violations (coming soon)

---

## Current data snapshot

| Source | Records ingested | Coverage | Last run |
|--------|-----------------|----------|----------|
| NRC (National Response Center) | 7,963 geolocated incidents | 2024–2025 | 2026-04-24 |
| EPA ECHO | — | coming soon | — |

**Top states by incident count:** Louisiana (2,412) · Texas (1,073) · California (439) · Florida (410) · Oklahoma (337) · Alaska (327) · Massachusetts (243) · Washington (203)

**Sample query:** 52 environmental incidents within 50km of San Francisco, including oil spills in the Bay from Golden Gate Ferry, USCG Station San Francisco, and National Tank Lines.

---

## For collections managers and non-technical stakeholders

### What is this?

Governments collect detailed records of environmental incidents — every reported oil spill, every chemical release, every facility that has violated clean air or clean water laws. That data is public, but it's scattered across different federal agencies, formatted differently in each place, and hard to search.

echoHazards pulls that data together into one place and makes it searchable by location. You can ask: *"What environmental incidents have been reported within 25 miles of Portland, Oregon in the last two years?"* and get a structured answer in seconds.

### What data is included?

| Source | What it covers | How often it updates |
|--------|---------------|----------------------|
| **NRC** (National Response Center) | Every reported oil spill, chemical release, and hazardous material incident in the US. Reporting is required by federal law — this is the most complete spill record that exists. | Weekly |
| **EPA ECHO** *(coming soon)* | Over 1 million industrial facilities regulated under the Clean Air Act, Clean Water Act, and hazardous waste laws — including their full violation and enforcement history. | Daily |

### What can I search for?

The API accepts plain web requests. You can filter by:

- **Location** — provide a latitude/longitude and search radius (e.g. 25km around a city center)
- **Type** — spills, violations, chemical releases, contaminated sites
- **Medium** — what was affected: air, water, or land
- **State** — two-letter state code
- **Date range** — incidents on or after / before a given date

### How is data quality handled?

Each record stores the **complete original government record** alongside our normalized version. This means:
- Every data point can be verified against the federal source
- Nothing is discarded — fields that don't fit our standard format are still preserved
- Every ingestion run is logged with counts of records fetched, inserted, and skipped
- The pipeline monitors for anomalies: if a run returns zero records or drops more than 20% vs. the previous run, it logs a warning for review

### How do I request a new data source be added?

Open an issue on GitHub describing the source (name, URL, what it covers). Priority is given to sources that are federally maintained, publicly accessible, and include geographic coordinates.

---

## For engineers

### Architecture

```
┌──────────────────────────────────────┐
│         Ingestor layer               │
│  NRCIngestor   ECHOIngestor  ...     │
│  (xlsx files)  (REST API)            │
└────────────────┬─────────────────────┘
                 │ normalizes to unified schema
                 ▼
┌──────────────────────────────────────┐
│   PostgreSQL + PostGIS               │
│   incidents table                    │
│   GEOGRAPHY(POINT) GiST index        │
└────────────────┬─────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────┐
│   FastAPI REST API                   │
│   ST_DWithin proximity queries       │
│   OpenAPI docs at /docs              │
└──────────────────────────────────────┘
```

### Prerequisites

- Docker + Docker Compose
- Python 3.11+

### Local development setup

**Windows (PowerShell):**
```powershell
# 1. Clone and enter
git clone https://github.com/yourname/echoHazards
cd echoHazards

# 2. Copy environment config
Copy-Item .env.example .env

# 3. Start Postgres + PostGIS
docker-compose up db -d

# 4. Install Python dependencies
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 5. Run database migrations
alembic upgrade head

# 6. Download NRC data
mkdir data\nrc
# Download yearly xlsx files from https://nrc.uscg.mil/FOIAdata.aspx
# Save as: data\nrc\nrc_2024.xlsx, data\nrc\nrc_2025.xlsx etc.

# 7. Run the NRC ingestor
python scripts/run_ingestor.py nrc

# 8. Start the API
uvicorn api.main:app --reload
# API at http://localhost:8000
# Docs at http://localhost:8000/docs
```

**Linux / macOS:**
```bash
cp .env.example .env
docker-compose up db -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
mkdir -p data/nrc
# Download NRC xlsx files to data/nrc/
python scripts/run_ingestor.py nrc
uvicorn api.main:app --reload
```

### Reset / clean local environment

```powershell
# Windows
python scripts/reset_db.py

# Linux/macOS
python scripts/reset_db.py
```

See `scripts/reset_db.py` for what this does.

### Running tests

```powershell
pytest tests/ -v
```

### Project structure

```
echoHazards/
├── api/
│   ├── main.py          # FastAPI app, CORS middleware, lifespan hooks
│   ├── routes.py        # All route handlers — proximity search, pipeline status
│   └── schemas.py       # Pydantic request/response models
├── ingestors/
│   ├── base.py          # BaseIngestor: run lifecycle, upsert, completeness checks
│   ├── nrc.py           # NRC xlsx ingestor — joins CALLS + INCIDENT_COMMONS + MATERIAL_INVOLVED
│   └── echo.py          # EPA ECHO REST API ingestor (ready, not yet scheduled)
├── models/
│   ├── incident.py      # SQLAlchemy models: Incident, PipelineRun
│   └── database.py      # Engine, session factory, get_db() FastAPI dependency
├── migrations/
│   └── versions/
│       └── 001_initial_schema.py   # PostGIS-enabled schema migration
├── tests/
│   └── test_nrc_ingestor.py        # Unit tests for NRC normalization logic
├── scripts/
│   ├── run_ingestor.py  # CLI: python scripts/run_ingestor.py nrc|echo|all
│   └── reset_db.py      # Wipe and recreate local database for development
├── data/
│   └── nrc/             # NRC xlsx files go here (gitignored)
├── config.py            # Settings via pydantic-settings + .env
├── docker-compose.yml   # Postgres + PostGIS + API services
├── Dockerfile
├── requirements.txt
└── .env.example
```

### Adding a new data source

1. Create `ingestors/your_source.py`
2. Subclass `BaseIngestor` and implement two methods:
   - `fetch_records()` — yield raw dicts from the source, one per record
   - `normalize(raw)` — map to the unified incident schema; return `None` to skip
3. Add your source name to the `IncidentSource` enum in `models/incident.py`
4. Register it in `scripts/run_ingestor.py`
5. Write tests in `tests/test_your_source.py`

The base class handles everything else: deduplication, batch upserts, pipeline run logging, and completeness checks.

### Key design decisions

**Unified schema with raw preservation.** Every source normalizes into the same `incidents` table, enabling cross-source proximity queries. The `raw` JSONB column stores the complete original record — normalized fields can always be re-derived, and source data is never discarded.

**Deduplication on (source, source_id).** Re-running an ingestor is always safe. The `ON CONFLICT DO NOTHING` upsert skips duplicate source records silently.

**PostGIS for proximity queries.** `ST_DWithin` on a `GEOGRAPHY` column uses accurate great-circle distance with a GiST spatial index. This is correct and performant at real-world scales — not bounding-box math.

**NRC data structure.** NRC yearly files are relational Excel workbooks with multiple sheets (`CALLS`, `INCIDENT_COMMONS`, `MATERIAL_INVOLVED` and others) joined on `SEQNOS`. The ingestor joins the three relevant sheets in memory before normalization. Coordinates are stored in degrees/minutes/seconds format and converted to decimal.

**Completeness monitoring.** Every ingestor run is recorded in `pipeline_runs`. The base class warns if a run fetches zero records or drops >20% vs the previous run. Visible via `GET /pipeline/status`.

### API reference

Interactive docs at `/docs` (Swagger UI) or `/redoc`. Key endpoints:

```
GET /incidents
  ?lat=37.77&lng=-122.41   # center point (both required for proximity search)
  &radius_km=25            # search radius in km (default: 25, max: 500)
  &source=NRC              # filter by source: NRC | ECHO | TRI
  &incident_type=spill     # filter by type: spill | violation | release | site
  &medium=water            # filter by medium: air | water | land
  &state=CA                # filter by two-letter state code
  &since=2024-01-01        # incidents on or after this date
  &until=2025-12-31        # incidents on or before this date
  &page=1&page_size=50     # pagination (max page_size: 200)

GET /incidents/{id}               # single record including full raw source data
GET /incidents/summary/by-state   # incident count per state
GET /pipeline/status              # last run per source + total record count
GET /health                       # database connectivity check
GET /docs                         # OpenAPI interactive documentation
```

### Deployment (Railway)

```bash
# In Railway dashboard, add a Postgres service and set:
# DATABASE_URL  (auto-provided by Railway Postgres add-on)
# ENVIRONMENT=production
# LOG_LEVEL=INFO

railway up
```

Railway detects the `Dockerfile` automatically.
