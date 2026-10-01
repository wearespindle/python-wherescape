import sys

import pytest

from wherescape.copy_tables import check_not_same_database, odbc_dsn_to_connect_kwargs, resolve_tables


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


class FakeKey(dict):
    """A registry key: its values, usable as a context manager like winreg's HKEY."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeWinreg:
    """Stand-in for winreg: `keys` maps (hive, view) to {dsn: {value name: value}}."""

    HKEY_LOCAL_MACHINE, HKEY_CURRENT_USER = "HKLM", "HKCU"
    KEY_READ, KEY_WOW64_64KEY, KEY_WOW64_32KEY = 0, 64, 32

    def __init__(self, keys):
        self.keys = keys

    def OpenKeyEx(self, hive, path, reserved, access):  # noqa: N802 - mirrors winreg
        dsn = path.rsplit("\\", 1)[1]
        values = self.keys.get((hive, access - self.KEY_READ), {}).get(dsn)
        if values is None:
            raise FileNotFoundError(path)
        return FakeKey(values)

    def QueryValueEx(self, key, name):  # noqa: N802 - mirrors winreg
        if name not in key:
            raise FileNotFoundError(name)
        return key[name], 1


DSN_VALUES = {"Servername": "db.example", "Port": "", "Database": "warehouse", "SSLmode": "Require"}


@pytest.mark.parametrize("location", [("HKLM", 64), ("HKLM", 32), ("HKCU", 64), ("HKCU", 32)])
def test_dsn_found_as_system_or_user_dsn_in_either_view(monkeypatch, location):
    monkeypatch.setitem(sys.modules, "winreg", FakeWinreg({location: {"prod": DSN_VALUES}}))
    assert odbc_dsn_to_connect_kwargs("prod", "u", "p") == {
        "host": "db.example",
        "port": "5432",
        "dbname": "warehouse",
        "sslmode": "require",
        "user": "u",
        "password": "p",
    }


def test_missing_dsn_names_the_dsn(monkeypatch):
    monkeypatch.setitem(sys.modules, "winreg", FakeWinreg({}))
    with pytest.raises(FileNotFoundError, match="'prod' not found"):
        odbc_dsn_to_connect_kwargs("prod", "u", "p")


def test_empty_dsn_is_a_configuration_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "winreg", FakeWinreg({}))
    with pytest.raises(ValueError, match="no ODBC DSN"):
        odbc_dsn_to_connect_kwargs(None, "u", "p")
