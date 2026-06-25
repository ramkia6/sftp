#!/usr/bin/env python3

import concurrent.futures as cf
import logging
import sys
import time
from dataclasses import dataclass
from typing import Optional

import oracledb
import psycopg2
from psycopg2 import pool as pg_pool
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ----------------------------------------------------------------------------
# CONFIG -- edit these
# ----------------------------------------------------------------------------
ORACLE_CONFIG = {
    "user":     "oracle_user",
    "password": "oracle_password",
    "dsn":      "oracle-host.example.com:1521/ORCLPDB1",
}

POSTGRES_CONFIG = {
    "user":     "pg_user",
    "password": "pg_password",
    "host":     "aurora-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com",
    "port":     5432,
    "dbname":   "mydb",
}

SCHEMA_PAIRS = [
    ("SALES",   "sales"),
    ("HR",      "hr"),
    ("FINANCE", "finance"),
]

MAX_WORKERS_PER_DB = 16
POOL_SIZE = MAX_WORKERS_PER_DB
OUTPUT_FILE = "row_count_comparison_report.xlsx"

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12s] %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rowcount")

# ----------------------------------------------------------------------------
# Result model
# ----------------------------------------------------------------------------
@dataclass
class TableResult:
    oracle_schema: str
    pg_schema: str
    table_name: str                  # display name (preferred case)
    in_oracle: bool = False
    in_postgres: bool = False
    ora_table_actual: str = ""       # exact name as stored in Oracle catalog
    pg_table_actual: str = ""        # exact name as stored in Postgres catalog
    oracle_count: Optional[int] = None
    pg_count: Optional[int] = None
    oracle_error: str = ""
    pg_error: str = ""

    @property
    def exists_both(self) -> str:
        return "YES" if (self.in_oracle and self.in_postgres) else "NO"

    @property
    def match(self) -> str:
        if not (self.in_oracle and self.in_postgres):
            return "NO"
        if self.oracle_count is None or self.pg_count is None:
            return "ERROR"
        return "YES" if self.oracle_count == self.pg_count else "NO"


# ----------------------------------------------------------------------------
# Connection pools
# ----------------------------------------------------------------------------
class OraclePool:
    def __init__(self, cfg, size):
        self.pool = oracledb.create_pool(
            user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"],
            min=2, max=size, increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
        )

    def query_one(self, sql, params=None):
        conn = self.pool.acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                return cur.fetchone()
        finally:
            self.pool.release(conn)

    def query_all(self, sql, params=None):
        conn = self.pool.acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                return cur.fetchall()
        finally:
            self.pool.release(conn)

    def close(self):
        self.pool.close(force=True)


class PostgresPool:
    def __init__(self, cfg, size):
        self.pool = pg_pool.ThreadedConnectionPool(minconn=2, maxconn=size, **cfg)

    def _run(self, sql, params, fetch):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return fetch(cur)
        finally:
            conn.rollback()
            self.pool.putconn(conn)

    def query_one(self, sql, params=None):
        return self._run(sql, params, lambda c: c.fetchone())

    def query_all(self, sql, params=None):
        return self._run(sql, params, lambda c: c.fetchall())

    def close(self):
        self.pool.closeall()


# ----------------------------------------------------------------------------
# Metadata fetchers -- query catalogs DIRECTLY, not privilege-filtered views
# ----------------------------------------------------------------------------
def get_oracle_tables(orapool: OraclePool, schema: str) -> set:
    # Filter out recycle-bin tables (BIN$...) and nested-table / IOT overflow
    # artifacts that appear in all_tables but aren't real user tables.
    rows = orapool.query_all(
        """
        SELECT table_name
        FROM all_tables
        WHERE owner = :owner
          AND table_name NOT LIKE 'BIN$%'
          AND (nested = 'NO' OR nested IS NULL)
          AND iot_type IS NULL
        """,
        {"owner": schema.upper()},
    )
    tables = {r[0] for r in rows}
    log.info("Oracle schema %s: %d tables found", schema, len(tables))
    return tables


def get_postgres_tables(pgpool: PostgresPool, schema: str) -> set:
    """
    Use pg_class directly instead of information_schema.tables.

    information_schema.tables is filtered by table privileges -- a table the
    connecting role has no grants on will be invisible, which causes false
    "Missing in Postgres" reports. pg_class returns every relation in the
    schema regardless of grants.

    relkind values included:
        'r' ordinary table
        'p' partitioned table  (parent of a partitioned table; the rows live
            in the children but COUNT(*) on the parent rolls them up)
        'f' foreign table      (counted via the foreign data wrapper)
    """
    rows = pgpool.query_all(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relkind IN ('r', 'p', 'f')
          AND c.relname NOT LIKE 'pg_%%'
        """,
        (schema,),
    )
    tables = {r[0] for r in rows}
    log.info("Postgres schema %s: %d tables found", schema, len(tables))
    return tables


# ----------------------------------------------------------------------------
# Count workers
# ----------------------------------------------------------------------------
# Postgres error code for "relation does not exist" -- used to flip the
# in_postgres flag if the catalog was stale or the table truly is missing.
PG_UNDEFINED_TABLE = "42P01"
ORA_TABLE_OR_VIEW_NOT_EXIST = 942  # ORA-00942


def count_oracle(orapool: OraclePool, result: TableResult):
    sql = f'SELECT COUNT(*) FROM "{result.oracle_schema.upper()}"."{result.ora_table_actual}"'
    t0 = time.time()
    try:
        row = orapool.query_one(sql)
        result.oracle_count = int(row[0])
        log.info("Oracle  %s.%s = %s (%.1fs)",
                 result.oracle_schema, result.ora_table_actual,
                 f"{result.oracle_count:,}", time.time() - t0)
    except oracledb.DatabaseError as e:
        err, = e.args
        if getattr(err, "code", None) == ORA_TABLE_OR_VIEW_NOT_EXIST:
            result.in_oracle = False
            result.oracle_error = "Table not found at count time"
        else:
            result.oracle_error = str(e).splitlines()[0][:200]
        log.error("Oracle  %s.%s FAILED: %s",
                  result.oracle_schema, result.ora_table_actual,
                  result.oracle_error)
    except Exception as e:
        result.oracle_error = str(e).splitlines()[0][:200]
        log.error("Oracle  %s.%s FAILED: %s",
                  result.oracle_schema, result.ora_table_actual,
                  result.oracle_error)


def count_postgres(pgpool: PostgresPool, result: TableResult):
    sql = f'SELECT COUNT(*) FROM "{result.pg_schema}"."{result.pg_table_actual}"'
    t0 = time.time()
    try:
        row = pgpool.query_one(sql)
        result.pg_count = int(row[0])
        log.info("Postgres %s.%s = %s (%.1fs)",
                 result.pg_schema, result.pg_table_actual,
                 f"{result.pg_count:,}", time.time() - t0)
    except psycopg2.Error as e:
        if getattr(e, "pgcode", None) == PG_UNDEFINED_TABLE:
            # Table genuinely doesn't exist -- flip the flag so the report
            # says "Missing in Postgres" instead of leaving an opaque error.
            result.in_postgres = False
            result.pg_error = "Table not found at count time"
        else:
            result.pg_error = str(e).splitlines()[0][:200]
        log.error("Postgres %s.%s FAILED: %s",
                  result.pg_schema, result.pg_table_actual,
                  result.pg_error)
    except Exception as e:
        result.pg_error = str(e).splitlines()[0][:200]
        log.error("Postgres %s.%s FAILED: %s",
                  result.pg_schema, result.pg_table_actual,
                  result.pg_error)


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
def build_results_for_schema(orapool, pgpool, ora_schema, pg_schema,
                             meta_executor):
    f_ora = meta_executor.submit(get_oracle_tables, orapool, ora_schema)
    f_pg = meta_executor.submit(get_postgres_tables, pgpool, pg_schema)
    ora_tables = f_ora.result()
    pg_tables = f_pg.result()

    ora_map = {t.lower(): t for t in ora_tables}
    pg_map = {t.lower(): t for t in pg_tables}
    all_keys = sorted(set(ora_map) | set(pg_map))

    results = []
    for key in all_keys:
        in_ora = key in ora_map
        in_pg = key in pg_map
        # Display name: prefer the Oracle-side casing when both exist.
        display = ora_map.get(key, pg_map.get(key, key))
        results.append(TableResult(
            oracle_schema=ora_schema,
            pg_schema=pg_schema,
            table_name=display,
            in_oracle=in_ora,
            in_postgres=in_pg,
            ora_table_actual=ora_map.get(key, ""),
            pg_table_actual=pg_map.get(key, ""),
        ))
    return results


def run_comparison():
    orapool = OraclePool(ORACLE_CONFIG, POOL_SIZE)
    pgpool = PostgresPool(POSTGRES_CONFIG, POOL_SIZE)
    all_results = []

    try:
        # Phase 1: metadata for every schema pair, fully parallel
        with cf.ThreadPoolExecutor(max_workers=len(SCHEMA_PAIRS) * 2 or 2,
                                   thread_name_prefix="meta") as meta_ex:
            futs = {
                meta_ex.submit(build_results_for_schema, orapool, pgpool,
                               o, p, meta_ex): (o, p)
                for o, p in SCHEMA_PAIRS
            }
            for fut in cf.as_completed(futs):
                all_results.extend(fut.result())

        log.info("Total tables to process: %d", len(all_results))

        # Phase 2: row counts -- two dedicated pools so Oracle and Postgres
        # work concurrently and independently.
        with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_DB,
                                   thread_name_prefix="ora") as ora_ex, \
             cf.ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_DB,
                                   thread_name_prefix="pg") as pg_ex:

            futures = []
            for r in all_results:
                if r.in_oracle:
                    futures.append(ora_ex.submit(count_oracle, orapool, r))
                if r.in_postgres:
                    futures.append(pg_ex.submit(count_postgres, pgpool, r))

            done, total = 0, len(futures)
            for fut in cf.as_completed(futures):
                fut.result()
                done += 1
                if done % 25 == 0 or done == total:
                    log.info("Progress: %d/%d count queries complete", done, total)
    finally:
        orapool.close()
        pgpool.close()

    all_results.sort(key=lambda r: (r.oracle_schema, r.table_name.lower()))
    return all_results


# ----------------------------------------------------------------------------
# XLSX report
# ----------------------------------------------------------------------------
def write_report(results, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Row Count Comparison"

    headers = ["Oracle_Schema", "Postgres_Schema", "Table_Name", "Exists",
               "Oracle_Count", "Postgres_Count", "Match", "Remarks"]

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(bold=True, color="FFFFFF")
    green = PatternFill("solid", fgColor="C6EFCE")
    red = PatternFill("solid", fgColor="FFC7CE")
    yellow = PatternFill("solid", fgColor="FFEB9C")
    thin = Border(*[Side(style="thin", color="D0D0D0")] * 4)
    center = Alignment(horizontal="center")

    for col, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col, value=h)
        c.fill, c.font, c.alignment, c.border = header_fill, header_font, center, thin

    for i, r in enumerate(results, start=2):
        remarks = []
        if not r.in_oracle and r.in_postgres:
            remarks.append("Missing in Oracle")
        elif r.in_oracle and not r.in_postgres:
            remarks.append("Missing in Postgres")
        elif r.in_oracle and r.in_postgres \
                and r.oracle_count == 0 and r.pg_count == 0:
            remarks.append("Both sides empty (0 rows)")
        if r.oracle_error:
            remarks.append(f"Oracle: {r.oracle_error}")
        if r.pg_error:
            remarks.append(f"Postgres: {r.pg_error}")

        # Note: 0 is a valid count and must display as 0, NOT as N/A.
        ora_val = r.oracle_count if r.in_oracle and r.oracle_count is not None else "N/A"
        pg_val = r.pg_count if r.in_postgres and r.pg_count is not None else "N/A"

        row_vals = [
            r.oracle_schema, r.pg_schema, r.table_name,
            r.exists_both, ora_val, pg_val, r.match,
            "; ".join(remarks),
        ]
        for col, v in enumerate(row_vals, 1):
            c = ws.cell(row=i, column=col, value=v)
            c.border = thin
            if col in (4, 7):
                c.alignment = center
            if col in (5, 6) and isinstance(v, int) and not isinstance(v, bool):
                c.number_format = "#,##0"

        match_cell = ws.cell(row=i, column=7)
        exists_cell = ws.cell(row=i, column=4)
        if r.match == "YES":
            match_cell.fill = green
        elif r.match == "ERROR":
            match_cell.fill = yellow
        else:
            match_cell.fill = red
        exists_cell.fill = green if r.exists_both == "YES" else red

    widths = [18, 18, 38, 10, 16, 16, 10, 55]
    for col, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.auto_filter.ref = f"A1:H{len(results) + 1}"
    ws.freeze_panes = "A2"

    s = wb.create_sheet("Summary")
    total = len(results)
    both = sum(1 for r in results if r.exists_both == "YES")
    matched = sum(1 for r in results if r.match == "YES")
    mismatched = sum(1 for r in results
                     if r.match == "NO" and r.exists_both == "YES")
    missing_pg = sum(1 for r in results if r.in_oracle and not r.in_postgres)
    missing_ora = sum(1 for r in results if not r.in_oracle and r.in_postgres)
    errors = sum(1 for r in results if r.match == "ERROR")
    both_empty = sum(1 for r in results
                     if r.in_oracle and r.in_postgres
                     and r.oracle_count == 0 and r.pg_count == 0)
    rows = [
        ("Total tables (union)", total),
        ("Exists in both", both),
        ("Missing in Postgres only", missing_pg),
        ("Missing in Oracle only", missing_ora),
        ("Row counts MATCH", matched),
        ("  ...of which empty on both sides", both_empty),
        ("Row counts MISMATCH", mismatched),
        ("Errors", errors),
        ("Generated at", time.strftime("%Y-%m-%d %H:%M:%S")),
    ]
    for i, (k, v) in enumerate(rows, 1):
        s.cell(row=i, column=1, value=k).font = Font(bold=True)
        s.cell(row=i, column=2, value=v)
    s.column_dimensions["A"].width = 32
    s.column_dimensions["B"].width = 22

    wb.save(path)
    log.info("Report written: %s", path)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    t0 = time.time()
    log.info("Starting comparison for %d schema pair(s), %d workers per DB",
             len(SCHEMA_PAIRS), MAX_WORKERS_PER_DB)
    results = run_comparison()
    write_report(results, OUTPUT_FILE)

    mismatches = [r for r in results if r.match != "YES"]
    log.info("Done in %.1fs. %d/%d tables fully match.",
             time.time() - t0, len(results) - len(mismatches), len(results))
    if mismatches:
        log.warning("%d table(s) need attention -- see %s",
                    len(mismatches), OUTPUT_FILE)
        sys.exit(1)


if __name__ == "__main__":
    main()
