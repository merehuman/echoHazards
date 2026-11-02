"""
Integration tests for GET /incidents pagination.

These run against a real PostGIS database because ordering and filtering use
ST_Distance / ST_DWithin. They use a separate database (default: the configured
DATABASE_URL with the name "echo_hazards_test", override with TEST_DATABASE_URL)
that is created if missing and rebuilt on every run, so dev data is never touched.

If no PostGIS server is reachable the whole module is skipped:
    docker compose up db -d
"""

import math
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from models.incident import Base, Incident

CENTER_LAT, CENTER_LNG = 30.0, -95.0
RADIUS_KM = 100
PAGE_SIZE = 50

INSIDE_COUNT = 137                # 3 pages: 50, 50, 37
TIE_INDEXES = range(44, 56)       # same location, straddles the page 1/2 boundary
OUTSIDE_COUNT = 5                 # ~300 km away, excluded by RADIUS_KM
KM_PER_DEG_LAT = 111.32


def _test_database_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        return make_url(url)
    try:
        from config import get_settings
        return make_url(get_settings().database_url).set(database="echo_hazards_test")
    except Exception as exc:  # missing .env / DATABASE_URL
        pytest.skip(f"No database configured for integration tests: {exc}")


def _ensure_database(url):
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    except OperationalError as exc:
        pytest.skip(f"PostGIS server not reachable ({url.host}:{url.port}): {exc.orig}")
    finally:
        admin.dispose()


def _incident(index, distance_km):
    lat = CENTER_LAT + distance_km / KM_PER_DEG_LAT
    return Incident(
        id=uuid.uuid4(),
        source="NRC",
        source_id=f"test-{index:03d}",
        incident_type="spill",
        lat=lat,
        lng=CENTER_LNG,
        location=f"SRID=4326;POINT({CENTER_LNG} {lat})",
        raw={},
    )


def _seed(session):
    """Points due north of the center at increasing distance, with one tie group."""
    tie_distance = 1 + TIE_INDEXES.start * 0.5
    inside = [
        _incident(i, tie_distance if i in TIE_INDEXES else 1 + i * 0.5)
        for i in range(INSIDE_COUNT)
    ]
    outside = [_incident(INSIDE_COUNT + i, 300 + i) for i in range(OUTSIDE_COUNT)]
    session.add_all(inside + outside)
    session.commit()
    return inside, outside


@pytest.fixture(scope="module")
def seeded():
    url = _test_database_url()
    _ensure_database(url)

    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)

    with TestSession() as session:
        inside, outside = _seed(session)
        expected = {
            "inside_ids": [str(inc.id) for inc in inside],
            "outside_ids": [str(inc.id) for inc in outside],
            "tie_ids": {str(inside[i].id) for i in TIE_INDEXES},
        }

    yield TestSession, expected

    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope="module")
def client(seeded):
    from fastapi.testclient import TestClient

    from api.main import app
    from models.database import get_db

    TestSession, _ = seeded

    def override_get_db():
        with TestSession() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def expected(seeded):
    return seeded[1]


def get_page(client, page, page_size=PAGE_SIZE, **params):
    query = {"lat": CENTER_LAT, "lng": CENTER_LNG, "radius_km": RADIUS_KM, "page": page, "page_size": page_size}
    query.update(params)
    res = client.get("/incidents", params=query)
    assert res.status_code == 200, res.text
    return res.json()


def all_pages(client, **params):
    first = get_page(client, 1, **params)
    pages = [first] + [get_page(client, p, **params) for p in range(2, first["total_pages"] + 1)]
    return [row for page in pages for row in page["results"]]


def distance_km(row):
    return (row["lat"] - CENTER_LAT) * KM_PER_DEG_LAT


def test_results_are_sorted_nearest_first(client):
    distances = [distance_km(row) for row in all_pages(client)]
    assert distances == sorted(distances)


def test_pages_cover_every_match_exactly_once(client, expected):
    ids = [row["id"] for row in all_pages(client)]
    assert len(ids) == len(set(ids)), "an incident appeared on more than one page"
    assert set(ids) == set(expected["inside_ids"])


def test_out_of_radius_incidents_are_excluded(client, expected):
    ids = {row["id"] for row in all_pages(client)}
    assert ids.isdisjoint(expected["outside_ids"])


def test_equidistant_incidents_are_ordered_by_id_across_pages(client, expected):
    """Ties at the page boundary must not shuffle between requests."""
    rows = all_pages(client)
    tie_ids = [row["id"] for row in rows if row["id"] in expected["tie_ids"]]
    assert tie_ids == sorted(tie_ids)

    assert get_page(client, 1) == get_page(client, 1)
    assert get_page(client, 2) == get_page(client, 2)


def test_first_page_metadata(client):
    page = get_page(client, 1)
    assert page["total"] == INSIDE_COUNT
    assert page["page"] == 1
    assert page["page_size"] == PAGE_SIZE
    assert page["total_pages"] == math.ceil(INSIDE_COUNT / PAGE_SIZE) == 3
    assert page["has_prev"] is False
    assert page["has_next"] is True
    assert len(page["results"]) == PAGE_SIZE


def test_middle_page_metadata(client):
    page = get_page(client, 2)
    assert page["has_prev"] is True
    assert page["has_next"] is True
    assert len(page["results"]) == PAGE_SIZE


def test_last_partial_page_metadata(client):
    page = get_page(client, 3)
    assert page["has_prev"] is True
    assert page["has_next"] is False
    assert len(page["results"]) == INSIDE_COUNT - 2 * PAGE_SIZE


def test_page_beyond_the_end_is_empty(client):
    page = get_page(client, 4)
    assert page["results"] == []
    assert page["total"] == INSIDE_COUNT
    assert page["total_pages"] == 3
    assert page["has_next"] is False


def test_no_matches(client):
    page = get_page(client, 1, lat=0.0, lng=-150.0, radius_km=5)
    assert page["total"] == 0
    assert page["total_pages"] == 0
    assert page["has_prev"] is False
    assert page["has_next"] is False
    assert page["results"] == []


def test_listing_without_location_pages_are_stable(client, expected):
    """Without lat/lng results sort by date; the id tie-breaker keeps pages disjoint."""
    pages = []
    for p in (1, 2, 3):
        res = client.get("/incidents", params={"page": p, "page_size": PAGE_SIZE})
        assert res.status_code == 200, res.text
        pages.append(res.json())
    ids = [row["id"] for page in pages for row in page["results"]]
    assert pages[0]["total"] == INSIDE_COUNT + OUTSIDE_COUNT
    assert len(ids) == len(set(ids)) == INSIDE_COUNT + OUTSIDE_COUNT


@pytest.mark.parametrize("params", [
    {"page_size": 201},
    {"page_size": 0},
    {"page": 0},
])
def test_invalid_pagination_params_are_rejected(client, params):
    query = {"lat": CENTER_LAT, "lng": CENTER_LNG, "page": 1, "page_size": PAGE_SIZE, **params}
    assert client.get("/incidents", params=query).status_code == 422
