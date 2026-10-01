"""Copy PostgreSQL tables from one database to another with COPY.

Used by WhereScape.copy_tables_from_source() to refresh a dev warehouse from production: the
source connection of the RED object points at prod, the target at dev.

How a table is copied:
  1. Source: one REPEATABLE READ, READ ONLY transaction for the whole run, so every table comes
     from the same snapshot and nothing can be written to the source. COPY ... TO STDOUT streams
     the table into a spool file (memory up to SPOOL_BYTES, disk beyond).
  2. Target, one transaction per table: <table>_copytmp as a LIKE ... INCLUDING ALL clone,
     COPY ... FROM STDIN into it, then TRUNCATE the live table, INSERT ... SELECT from the clone
     and drop it. The live table keeps its identity (views, grants, comments survive) and a
     failed table keeps its previous contents.

Guards: refuses when source and target are the same server + database; table names are checked
against the target catalog; column sets must match on both sides; exported and loaded row
counts must match. A failing table is logged at ERROR and does not stop the others.
"""

import logging
import tempfile
import time

from psycopg2 import sql


SPOOL_BYTES = 256 * 1024 * 1024
STAGING_SUFFIX = "_copytmp"
# Never copied by default: our own staging tables and the *_temp staging tables of other loaders.
STAGING_SUFFIXES = ("_temp", STAGING_SUFFIX)
LOCK_TIMEOUT = "60s"


def odbc_dsn_to_connect_kwargs(odbc_dsn, user, password):
    """Turn a Windows ODBC DSN (psqlODBC) into psycopg2.connect() keyword arguments.

    Host, port, database and sslmode come from the DSN's registry key; user and password are
    not stored there and come from the WhereScape connection (WSL_*_USER/PWD). The DSN is looked
    up as a System DSN and then as a User DSN, each in the 64-bit and the 32-bit registry view.
    """
    import winreg

    if not odbc_dsn:
        raise ValueError("no ODBC DSN given: is the source/target connection set on the WhereScape object?")

    def value(key, name, default=None):
        try:
            return winreg.QueryValueEx(key, name)[0] or default
        except FileNotFoundError:
            return default

    path = f"SOFTWARE\\ODBC\\ODBC.INI\\{odbc_dsn}"
    for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
        for view in (winreg.KEY_WOW64_64KEY, winreg.KEY_WOW64_32KEY):
            try:
                key = winreg.OpenKeyEx(hive, path, 0, winreg.KEY_READ | view)
            except FileNotFoundError:
                continue
            with key:
                return {
                    "host": value(key, "Servername"),
                    "port": value(key, "Port", "5432"),
                    "dbname": value(key, "Database"),
                    "sslmode": value(key, "SSLmode", "prefer").lower(),
                    "user": user,
                    "password": password,
                }
    raise FileNotFoundError(
        f"ODBC DSN {odbc_dsn!r} not found as System or User DSN (64- or 32-bit) in the registry of this server"
    )


def resolve_tables(requested, available):
    """Return the tables to copy, in order.

    requested=None means every table in `available` except staging tables. A single string is
    a one-table list. Duplicates are dropped. Unknown names raise ValueError before anything is
    copied, so only catalog names ever reach the SQL.
    """
    copyable = [t for t in available if not t.endswith(STAGING_SUFFIXES)]
    if requested is None:
        return copyable
    if isinstance(requested, str):
        requested = [requested]
    unknown = sorted(set(requested) - set(copyable))
    if unknown:
        raise ValueError(f"unknown table(s) on the target: {', '.join(unknown)}")
    return list(dict.fromkeys(requested))


def check_not_same_database(source_identity, target_identity):
    """Raise when source and target are the same server + database."""
    if source_identity == target_identity:
        raise RuntimeError(
            f"source and target are the same database {target_identity} - refusing to copy a database onto itself"
        )


def server_identity(conn):
    """(server address, port, database) as the server reports it, so DNS aliases do not hide a match."""
    with conn.cursor() as cur:
        cur.execute("select coalesce(host(inet_server_addr()), 'local'), inet_server_port(), current_database()")
        return tuple(cur.fetchone())


def list_tables(conn, schema):
    with conn.cursor() as cur:
        cur.execute(
            "select table_name from information_schema.tables "
            "where table_schema = %s and table_type = 'BASE TABLE' order by table_name",
            [schema],
        )
        return [r[0] for r in cur.fetchall()]


def list_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "select column_name from information_schema.columns "
            "where table_schema = %s and table_name = %s order by ordinal_position",
            [schema, table],
        )
        return [r[0] for r in cur.fetchall()]


def copy_table(source, target, schema, table):
    """Copy one table source -> target. Returns the number of rows. Commits on target."""
    source_cols = list_columns(source, schema, table)
    target_cols = list_columns(target, schema, table)
    if not source_cols:
        raise RuntimeError(f"{schema}.{table} not found on the source (missing, or no SELECT for the source user)")
    if set(source_cols) != set(target_cols):
        raise RuntimeError(
            f"columns differ - only on source: {sorted(set(source_cols) - set(target_cols))}, "
            f"only on target: {sorted(set(target_cols) - set(source_cols))}"
        )

    live = sql.Identifier(schema, table)
    staging = sql.Identifier(schema, f"{table}{STAGING_SUFFIX}")
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in target_cols)
    t0 = time.monotonic()

    with tempfile.SpooledTemporaryFile(max_size=SPOOL_BYTES, mode="w+b") as buf:
        with source.cursor() as cur:
            cur.copy_expert(sql.SQL("COPY {} ({}) TO STDOUT").format(live, cols), buf)
            exported = cur.rowcount
        size_mb = buf.tell() / 1024 / 1024
        buf.seek(0)
        logging.info(f"  {table}: exported {exported:,} rows ({size_mb:,.0f} MB) in {time.monotonic() - t0:.0f}s")

        try:
            with target.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging))
                cur.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(staging, live))
                cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN").format(staging, cols), buf)
                loaded = cur.rowcount
                if loaded != exported:
                    raise RuntimeError(f"row count mismatch: exported {exported:,}, loaded {loaded:,}")
                # TRUNCATE needs ACCESS EXCLUSIVE: give up rather than queue behind a long reader.
                cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(LOCK_TIMEOUT)))
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(live))
                cur.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(live, staging))
                cur.execute(sql.SQL("DROP TABLE {}").format(staging))
            target.commit()
        except Exception:
            target.rollback()
            raise

    logging.info(f"  {table}: published {loaded:,} rows in {time.monotonic() - t0:.0f}s")
    return loaded


def copy_tables(source, target, schema, tables=None):
    """Copy `tables` (None = all) of `schema` from source to target.

    `source` and `target` are psycopg2 connections. Returns {table: rows copied, or None if that
    table failed}. Raises before copying anything on a configuration problem (same database,
    unknown table names).
    """
    source.set_session(isolation_level="REPEATABLE READ", readonly=True)
    check_not_same_database(server_identity(source), server_identity(target))
    todo = resolve_tables(tables, list_tables(target, schema))
    target.commit()
    logging.info(f"copying {len(todo)} table(s) of {schema}: {', '.join(todo)}")

    result = {}
    for table in todo:
        with source.cursor() as cur:
            cur.execute("SAVEPOINT copy_table")
        try:
            result[table] = copy_table(source, target, schema, table)
        except Exception as e:
            logging.error(f"  {table} FAILED: {e}")
            result[table] = None
            # A failed COPY aborts the source transaction; roll back to keep the shared snapshot usable.
            with source.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT copy_table")
        else:
            with source.cursor() as cur:
                cur.execute("RELEASE SAVEPOINT copy_table")
    source.rollback()
    return result
