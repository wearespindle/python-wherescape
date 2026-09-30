"""Unit tests for the Glassfrog connector — no live API, no DB."""

from datetime import datetime
from unittest.mock import patch

import pytest

from wherescape.connectors.glassfrog.glassfrog_create_metadata import COLUMNS
from wherescape.connectors.glassfrog.glassfrog_load_data import _build_row
from wherescape.connectors.glassfrog.glassfrog_wrapper import Glassfrog


@pytest.fixture
def gf():
    return Glassfrog("dummy-token")


@pytest.fixture
def load_ts():
    return datetime(2026, 4, 25, 10, 0, 0)


@pytest.fixture
def circle_names():
    return {7: "General Company Circle", 9: "Engineering"}


class TestBuildRow:
    def test_happy_path(self, circle_names, load_ts):
        meeting = {
            "id": 100,
            "started_at": "2026-04-20T09:00:00Z",
            "ended_at": "2026-04-20T10:30:00Z",
            "url": "https://app.glassfrog.com/meetings/100",
            "links": {
                "circle": 7,
                "facilitator": 11,
                "secretary": 12,
                "attendees": [11, 12, 13, 14],
            },
        }
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row == [
            100,
            7,
            "General Company Circle",
            "governance",
            "2026-04-20T09:00:00Z",
            "2026-04-20T10:30:00Z",
            "2026-04-20",
            11,
            12,
            4,
            "https://app.glassfrog.com/meetings/100",
            "https://api.glassfrog.com/api/v3/",
            load_ts,
        ]

    def test_missing_links_falls_back_to_top_level_circle_id(self, circle_names, load_ts):
        meeting = {"id": 101, "circle_id": 9, "started_at": "2026-04-20T09:00:00Z"}
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row[1] == 9
        assert row[2] == "Engineering"
        assert row[7] is None
        assert row[8] is None
        assert row[9] == 0

    def test_started_at_falls_back_to_created_at(self, circle_names, load_ts):
        meeting = {"id": 102, "created_at": "2025-12-31T23:59:00Z", "links": {"circle": 7}}
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row[4] == "2025-12-31T23:59:00Z"
        assert row[6] == "2025-12-31"

    def test_both_timestamp_fields_missing(self, circle_names, load_ts):
        meeting = {"id": 103, "links": {"circle": 7}}
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row[4] is None
        assert row[6] is None

    def test_empty_attendees_yields_zero(self, circle_names, load_ts):
        meeting = {"id": 104, "links": {"circle": 7, "attendees": []}}
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row[9] == 0

    def test_unknown_circle_id_yields_none_name(self, circle_names, load_ts):
        meeting = {"id": 105, "links": {"circle": 999}}
        row = _build_row("governance", meeting, circle_names, load_ts)
        assert row[1] == 999
        assert row[2] is None

    def test_meeting_type_is_stamped_per_call(self, circle_names, load_ts):
        meeting = {"id": 1, "links": {"circle": 7}}
        row_g = _build_row("governance", meeting, circle_names, load_ts)
        row_t = _build_row("tactical", meeting, circle_names, load_ts)
        assert row_g[3] == "governance"
        assert row_t[3] == "tactical"


class TestPaginated:
    def test_walks_pages_until_empty(self, gf):
        pages = [
            {"governance_meetings": [{"id": 1}, {"id": 2}]},
            {"governance_meetings": [{"id": 3}]},
            {"governance_meetings": []},
        ]
        seen_params: list[dict] = []

        def fake_get(self, path, params=None):
            seen_params.append(dict(params))
            return pages.pop(0)

        with patch.object(Glassfrog, "_get", fake_get):
            result = gf._paginated("governance_meetings")

        assert [m["id"] for m in result] == [1, 2, 3]
        assert [p["page"] for p in seen_params] == [1, 2, 3]
        assert seen_params[0]["per_page"] == 100

    def test_bare_list_envelope(self, gf):
        pages = [[{"id": 10}, {"id": 11}], []]

        def fake_get(self, path, params=None):
            return pages.pop(0)

        with patch.object(Glassfrog, "_get", fake_get):
            result = gf._paginated("circles")

        assert [c["id"] for c in result] == [10, 11]

    def test_empty_first_page(self, gf):
        with patch.object(Glassfrog, "_get", lambda self, path, params=None: {"tactical_meetings": []}):
            result = gf._paginated("tactical_meetings")
        assert result == []


class TestGetAllMeetings:
    def test_concatenates_and_tags_both_sides(self, gf):
        with (
            patch.object(Glassfrog, "get_governance_meetings", lambda self: [{"id": 1}]),
            patch.object(Glassfrog, "get_tactical_meetings", lambda self: [{"id": 2}]),
        ):
            result = gf.get_all_meetings()
        assert result == [("governance", {"id": 1}), ("tactical", {"id": 2})]

    def test_one_endpoint_empty_other_populated(self, gf):
        with (
            patch.object(Glassfrog, "get_governance_meetings", lambda self: []),
            patch.object(Glassfrog, "get_tactical_meetings", lambda self: [{"id": 9}]),
        ):
            result = gf.get_all_meetings()
        assert result == [("tactical", {"id": 9})]


class TestSessionSetup:
    def test_auth_header_set(self):
        gf = Glassfrog("secret-token-xyz")
        assert gf._session.headers["X-Auth-Token"] == "secret-token-xyz"

    def test_retry_adapter_configured(self):
        gf = Glassfrog("secret-token-xyz")
        adapter = gf._session.get_adapter("https://api.glassfrog.com/api/v3/governance_meetings")
        retry = adapter.max_retries
        assert retry.total == 5
        assert retry.backoff_factor == 10
        assert 429 in retry.status_forcelist
        assert 503 in retry.status_forcelist


class TestColumnsConsistency:
    def test_build_row_width_matches_columns(self, load_ts):
        row = _build_row("governance", {}, {}, load_ts)
        # +2 for dss_record_source + dss_load_date appended by the loader.
        assert len(row) == len(COLUMNS) + 2
