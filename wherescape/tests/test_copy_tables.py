import pytest

from wherescape.copy_tables import check_not_same_database, resolve_tables


AVAILABLE = ["table_a", "table_a_temp", "table_b", "table_c", "table_c_copytmp"]


def test_resolve_none_copies_all_but_staging_tables():
    assert resolve_tables(None, AVAILABLE) == ["table_a", "table_b", "table_c"]


def test_resolve_keeps_order_and_dedupes():
    assert resolve_tables(["table_c", "table_a", "table_c"], AVAILABLE) == [
        "table_c",
        "table_a",
    ]


def test_resolve_accepts_single_name():
    assert resolve_tables("table_c", AVAILABLE) == ["table_c"]


def test_resolve_rejects_unknown_and_staging_tables():
    with pytest.raises(ValueError, match="tabel_c, table_a_temp"):
        resolve_tables(["tabel_c", "table_a_temp"], AVAILABLE)


def test_same_database_is_refused():
    with pytest.raises(RuntimeError, match="same database"):
        check_not_same_database(("10.0.0.1", 5432, "warehouse"), ("10.0.0.1", 5432, "warehouse"))


def test_different_database_passes():
    check_not_same_database(("10.0.0.1", 5432, "warehouse"), ("10.0.0.2", 5432, "warehouse"))
