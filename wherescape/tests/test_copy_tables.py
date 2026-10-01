import pytest

from wherescape.copy_tables import check_not_same_database, resolve_tables


AVAILABLE = ["feature_hubspot", "feature_hubspot_temp", "prediction_results", "training_data", "training_data_copytmp"]


def test_resolve_none_copies_all_but_staging_tables():
    assert resolve_tables(None, AVAILABLE) == ["feature_hubspot", "prediction_results", "training_data"]


def test_resolve_keeps_order_and_dedupes():
    assert resolve_tables(["training_data", "feature_hubspot", "training_data"], AVAILABLE) == [
        "training_data",
        "feature_hubspot",
    ]


def test_resolve_accepts_single_name():
    assert resolve_tables("training_data", AVAILABLE) == ["training_data"]


def test_resolve_rejects_unknown_and_staging_tables():
    with pytest.raises(ValueError, match="feature_hubspot_temp, trainig_data"):
        resolve_tables(["trainig_data", "feature_hubspot_temp"], AVAILABLE)


def test_same_database_is_refused():
    with pytest.raises(RuntimeError, match="same database"):
        check_not_same_database(("10.0.0.1", 5432, "warehouse"), ("10.0.0.1", 5432, "warehouse"))


def test_different_database_passes():
    check_not_same_database(("10.0.0.1", 5432, "warehouse"), ("10.0.0.2", 5432, "warehouse"))
