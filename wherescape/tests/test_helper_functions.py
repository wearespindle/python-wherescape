"""
Tests for wherescape.helper_functions.

Run from the wherescape-warehouse repository (the submodule itself has no
pytest dependency):

    uv run --project python pytest python/wherescape_os/wherescape/tests/ -v
"""

import datetime
import uuid
from decimal import Decimal

import pytest

from wherescape.helper_functions import (
    TYPE_MAPPING,
    get_metadata_from_sample_data,
    infer_postgres_type,
)


class TestInferPostgresType:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (True, "bool"),
            (False, "bool"),
            (1, "bigint"),
            (2147483648, "bigint"),
            (95.5, "numeric"),
            (Decimal("10.25"), "numeric"),
            (datetime.date(2025, 1, 1), "date"),
            (datetime.time(12, 30), "time"),
            (datetime.datetime(2025, 1, 1, 12, 0), "timestamp"),
            (datetime.timedelta(hours=1), "interval"),
            (uuid.UUID("45c2e9a0-1c4a-4a76-9b45-2ab41f0dd1e9"), "uuid"),
            (["a", "b"], "text"),
            ("John", "text"),
        ],
    )
    def test_type_mapping(self, value, expected):
        assert infer_postgres_type(value, "some_column") == expected

    def test_unmapped_type_falls_back_to_text(self):
        assert infer_postgres_type({"nested": 1}, "payload") == "text"

    @pytest.mark.parametrize(
        ("column_name", "expected"),
        [
            ("updated_at", "timestamp"),
            ("creation_date", "timestamp"),
            ("ticket_count", "bigint"),
            ("relation_id", "bigint"),
            ("success_rate", "numeric"),
            ("nps_score", "numeric"),
            ("notes", "text"),
        ],
    )
    def test_none_uses_column_name_heuristics(self, column_name, expected):
        assert infer_postgres_type(None, column_name) == expected

    @pytest.mark.parametrize(
        ("column_name", "expected"),
        [
            ("created_at", "timestamp"),
            ("start_time", "timestamp"),
            ("name", "text"),
        ],
    )
    def test_string_uses_column_name_heuristics(self, column_name, expected):
        assert infer_postgres_type("2025-01-01", column_name) == expected

    def test_type_mapping_keys_are_python_3_class_names(self):
        # Lookups happen on type(value).__name__, so every key must be an
        # actual class name (e.g. "Decimal", not the Python 2 era "decimal").
        samples = [
            None,
            True,
            1.0,
            1,
            "text",
            Decimal("1"),
            datetime.date(2025, 1, 1),
            datetime.time(12, 0),
            datetime.datetime(2025, 1, 1),
            datetime.timedelta(hours=1),
            [],
            uuid.uuid4(),
        ]
        assert {type(value).__name__ for value in samples} == set(TYPE_MAPPING)


class TestGetMetadataFromSampleData:
    def test_docstring_example(self):
        sample_data = [
            {"id": 1, "name": "John", "score": 95.5, "created_at": "2025-01-01", "active": True},
            {"id": 2, "name": "Jane", "score": 87.3, "created_at": "2025-01-02", "active": False},
        ]
        columns, types = get_metadata_from_sample_data(sample_data)
        assert columns == ["id", "name", "score", "created_at", "active"]
        assert types == ["bigint", "text", "numeric", "timestamp", "bool"]

    def test_empty_sample_data(self):
        assert get_metadata_from_sample_data([]) == ([], [])

    def test_uses_first_non_none_value(self):
        sample_data = [
            {"amount": None, "notes": None},
            {"amount": Decimal("1.50"), "notes": None},
        ]
        columns, types = get_metadata_from_sample_data(sample_data)
        assert columns == ["amount", "notes"]
        assert types == ["numeric", "text"]
