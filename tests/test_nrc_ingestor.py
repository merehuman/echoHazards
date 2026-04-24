"""
Tests for the NRC ingestor normalization layer.

These tests don't require a database or network — they test the pure
data transformation logic in isolation.
"""

import pytest
from unittest.mock import MagicMock

from ingestors.nrc import NRCIngestor, _parse_date, _parse_float, _classify_medium


# --- Unit tests for helper functions ---

class TestParseDate:
    def test_slash_format(self):
        assert str(_parse_date("03/15/2022")) == "2022-03-15"

    def test_iso_format(self):
        assert str(_parse_date("2022-03-15")) == "2022-03-15"

    def test_two_digit_year(self):
        assert str(_parse_date("03/15/99")) == "1999-03-15"

    def test_none_input(self):
        assert _parse_date(None) is None

    def test_empty_string(self):
        assert _parse_date("") is None

    def test_garbage_input(self):
        assert _parse_date("not a date") is None


class TestParseFloat:
    def test_plain_number(self):
        assert _parse_float("100.5") == 100.5

    def test_comma_separated(self):
        assert _parse_float("1,000.5") == 1000.5

    def test_none(self):
        assert _parse_float(None) is None

    def test_empty(self):
        assert _parse_float("") is None

    def test_non_numeric(self):
        assert _parse_float("N/A") is None


class TestClassifyMedium:
    def test_water_keywords(self):
        assert _classify_medium("river") == "water"
        assert _classify_medium("LAKE MICHIGAN") == "water"
        assert _classify_medium("coastal waters") == "water"

    def test_air_keywords(self):
        assert _classify_medium("atmosphere") == "air"
        assert _classify_medium("AIR RELEASE") == "air"

    def test_land_keywords(self):
        assert _classify_medium("ground soil") == "land"

    def test_unknown(self):
        assert _classify_medium(None) == "unknown"
        assert _classify_medium("") == "unknown"
        assert _classify_medium("UNKNOWN MEDIUM") == "unknown"


# --- Integration-style tests for normalize() ---

@pytest.fixture
def ingestor():
    mock_db = MagicMock()
    return NRCIngestor(db=mock_db, data_dir="./data/nrc")


class TestNRCNormalize:
    def test_valid_record(self, ingestor):
        raw = {
            "REPORT_NUMBER": "12345",
            "INCIDENT_DATE": "01/15/2022",
            "RESPONSIBLE_CO": "Acme Oil Co",
            "MATERIAL": "Crude Oil",
            "QUANTITY": "500",
            "UNIT": "gallons",
            "MEDIUM_AFFECTED": "river",
            "STATE": "TX",
            "CITY": "Houston",
            "LATITUDE": "29.7604",
            "LONGITUDE": "-95.3698",
        }
        result = ingestor.normalize(raw)
        assert result is not None
        assert result["source"] == "NRC"
        assert result["source_id"] == "12345"
        assert result["lat"] == 29.7604
        assert result["lng"] == -95.3698
        assert result["medium"] == "water"
        assert result["material"] == "Crude Oil"
        assert result["quantity"] == 500.0
        assert "raw" in result

    def test_missing_coordinates_returns_none(self, ingestor):
        raw = {
            "REPORT_NUMBER": "99999",
            "MATERIAL": "Diesel",
            "STATE": "CA",
        }
        assert ingestor.normalize(raw) is None

    def test_zero_zero_coordinates_returns_none(self, ingestor):
        raw = {
            "REPORT_NUMBER": "11111",
            "LATITUDE": "0.0",
            "LONGITUDE": "0.0",
            "MATERIAL": "Oil",
        }
        assert ingestor.normalize(raw) is None

    def test_invalid_coordinate_range_returns_none(self, ingestor):
        raw = {
            "REPORT_NUMBER": "22222",
            "LATITUDE": "999.0",   # invalid
            "LONGITUDE": "-95.0",
        }
        assert ingestor.normalize(raw) is None

    def test_alternate_column_names(self, ingestor):
        """NRC files use different column names across years."""
        raw = {
            "SEQNOS": "77777",         # alternate for REPORT_NUMBER
            "CALLDATE": "03/20/2019",  # alternate for INCIDENT_DATE
            "LATDECIMAL": "40.7128",   # alternate for LATITUDE
            "LONDECIMAL": "-74.0060",  # alternate for LONGITUDE
            "STATECODE": "NY",
        }
        result = ingestor.normalize(raw)
        assert result is not None
        assert result["source_id"] == "77777"
        assert result["state"] == "NY"

    def test_raw_field_preserved(self, ingestor):
        """The raw source record must be preserved verbatim."""
        raw = {
            "REPORT_NUMBER": "55555",
            "LATITUDE": "34.0522",
            "LONGITUDE": "-118.2437",
            "SOME_EXTRA_FIELD": "extra data we don't map",
        }
        result = ingestor.normalize(raw)
        assert result["raw"] == raw
        assert result["raw"]["SOME_EXTRA_FIELD"] == "extra data we don't map"
