#!/usr/bin/env python3
"""
Multi-threaded Oracle vs PostgreSQL (Aurora) row-count comparison tool.

For each schema pair provided, the script:
  1. Fetches table lists from both databases (in parallel).
  2. Builds the union of table names (so tables missing on either side are reported).
  3. Counts rows for every table on both databases concurrently using thread pools.
  4. Writes an XLSX report with columns:
       Oracle_Schema | Postgres_Schema | Table_Name | Exists |
       Oracle_Count  | Postgres_Count  | Match

Requirements:
    pip install oracledb psycopg2-binary openpyxl

Usage:
    python db_rowcount_compare.py
    (edit the CONFIG section below, or load from env/JSON as you prefer)
"""

import concurrent.futures as cf
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import oracledb                 # pip install oracledb  (thin mode, no client needed)
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
    "dsn":      "oracle-host.example.com:1521/ORCLPDB1",   # host:port/service_name
}

POSTGRES_CONFIG = {
    "user":     "pg_user",
    "password": "pg_password",
    "host":     "aurora-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com",
    "port":     5432,
    "dbname":   "mydb",
}

# List of schema pairs to compare: (oracle_schema, postgres_schema)
# Oracle schemas are usually UPPERCASE; Postgres usually lowercase.
SCHEMA_PAIRS = [
    ("SALES",   "sales"),
    ("HR",      "hr"),
    ("FINANCE", "finance"),
]

MAX_WORKERS_PER_DB = 16          # concurrent count queries per database
POOL_SIZE = MAX_WORKERS_PER_DB   # connection pool size per database
COUNT_TIMEOUT_SECONDS = 1800     # safety timeout per count (informational)
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
    table_name: str
    in_oracle: bool = False
    in_postgres: bool = False
    oracle_count: Optional[int] = None     # None => N/A or error
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
# Connection pools (thread-safe)
# ----------------------------------------------------------------------------
class OraclePool:
    def __init__(self, cfg: dict, size: int):
        self.pool = oracledb.create_pool(
            user=cfg["user"],
            password=cfg["password"],
            dsn=cfg["dsn"],
            min=2,
            max=size,
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
        )

    def query_one(self, sql: str, params: dict = None):
        conn = self.pool.acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                return cur.fetchone()
        finally:
            self.pool.release(conn)

    def query_all(self, sql: str, params: dict = None):
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
    def __init__(self, cfg: dict, size: int):
        self.pool = pg_pool.ThreadedConnectionPool(minconn=2, maxconn=size, **cfg)

    def query_one(self, sql: str, params=None):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
        finally:
            conn.rollback()          # release any implicit transaction
            self.pool.putconn(conn)

    def query_all(self, sql: str, params=None):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.rollback()
            self.pool.putconn(conn)

    def close(self):
        self.pool.closeall()


# ----------------------------------------------------------------------------
# Metadata fetchers
# ----------------------------------------------------------------------------
def get_oracle_tables(orapool: OraclePool, schema: str) -> set:
    rows = orapool.query_all(
        "SELECT table_name FROM all_tables WHERE owner = :owner",
        {"owner": schema.upper()},
    )
    tables = {r[0] for r in rows}
    log.info("Oracle schema %s: %d tables found", schema, len(tables))
    return tables


def get_postgres_tables(pgpool: PostgresPool, schema: str) -> set:
    rows = pgpool.query_all(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        """,
        (schema,),
    )
    tables = {r[0] for r in rows}
    log.info("Postgres schema %s: %d tables found", schema, len(tables))
    return tables


# ----------------------------------------------------------------------------
# Count workers
# ----------------------------------------------------------------------------
def count_oracle(orapool: OraclePool, result: TableResult):
    sql = f'SELECT COUNT(*) FROM "{result.oracle_schema.upper()}"."{result.table_name.upper()}"'
    t0 = time.time()
    try:
        row = orapool.query_one(sql)
        result.oracle_count = int(row[0])
        log.info("Oracle  %s.%s = %s (%.1fs)",
                 result.oracle_schema, result.table_name,
                 f"{result.oracle_count:,}", time.time() - t0)
    except Exception as e:
        result.oracle_error = str(e).splitlines()[0][:200]
        log.error("Oracle  %s.%s FAILED: %s",
                  result.oracle_schema, result.table_name, result.oracle_error)


def count_postgres(pgpool: PostgresPool, result: TableResult):
    # Match Postgres table name case-sensitively as discovered from catalog
    sql = f'SELECT COUNT(*) FROM "{result.pg_schema}"."{result.pg_table_actual}"'
    t0 = time.time()
    try:
        row = pgpool.query_one(sql)
        result.pg_count = int(row[0])
        log.info("Postgres %s.%s = %s (%.1fs)",
                 result.pg_schema, result.pg_table_actual,
                 f"{result.pg_count:,}", time.time() - t0)
    except Exception as e:
        result.pg_error = str(e).splitlines()[0][:200]
        log.error("Postgres %s.%s FAILED: %s",
                  result.pg_schema, result.pg_table_actual, result.pg_error)


# ----------------------------------------------------------------------------
# Comparison orchestration
# ----------------------------------------------------------------------------
def build_results_for_schema(orapool, pgpool, ora_schema, pg_schema,
                             meta_executor) -> list:
    """Fetch table lists for one schema pair (both DBs in parallel) and
    build TableResult objects for the union of table names."""
    f_ora = meta_executor.submit(get_oracle_tables, orapool, ora_schema)
    f_pg = meta_executor.submit(get_postgres_tables, pgpool, pg_schema)
    ora_tables = f_ora.result()
    pg_tables = f_pg.result()

    # Case-insensitive matching between the two databases
    ora_map = {t.lower(): t for t in ora_tables}
    pg_map = {t.lower(): t for t in pg_tables}
    all_keys = sorted(set(ora_map) | set(pg_map))

    results = []
    for key in all_keys:
        r = TableResult(
            oracle_schema=ora_schema,
            pg_schema=pg_schema,
            table_name=ora_map.get(key, pg_map.get(key, key)),
            in_oracle=key in ora_map,
            in_postgres=key in pg_map,
        )
        # remember the actual case-sensitive PG name for quoting in COUNT(*)
        r.pg_table_actual = pg_map.get(key, "")
        results.append(r)
    return results


def run_comparison() -> list:
    orapool = OraclePool(ORACLE_CONFIG, POOL_SIZE)
    pgpool = PostgresPool(POSTGRES_CONFIG, POOL_SIZE)
    all_results: list = []

    try:
        # ---- Phase 1: metadata for every schema pair, fully parallel ----
        with cf.ThreadPoolExecutor(max_workers=len(SCHEMA_PAIRS) * 2 or 2,
                                   thread_name_prefix="meta") as meta_ex:
            schema_futures = {
                meta_ex.submit(build_results_for_schema, orapool, pgpool,
                               o, p, meta_ex): (o, p)
                for o, p in SCHEMA_PAIRS
            }
            for fut in cf.as_completed(schema_futures):
                all_results.extend(fut.result())

        log.info("Total tables to process: %d", len(all_results))

        # ---- Phase 2: row counts -- two dedicated pools so Oracle and
        #      Postgres counting run simultaneously and independently ----
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

            done = 0
            total = len(futures)
            for fut in cf.as_completed(futures):
                fut.result()   # re-raise unexpected errors
                done += 1
                if done % 25 == 0 or done == total:
                    log.info("Progress: %d/%d count queries complete", done, total)
    finally:
        orapool.close()
        pgpool.close()

    # Stable sort for the report
    all_results.sort(key=lambda r: (r.oracle_schema, r.table_name.lower()))
    return all_results


# ----------------------------------------------------------------------------
# XLSX report
# ----------------------------------------------------------------------------
def write_report(results: list, path: str):
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
        if not r.in_oracle:
            remarks.append("Missing in Oracle")
        if not r.in_postgres:
            remarks.append("Missing in Postgres")
        if r.oracle_error:
            remarks.append(f"Oracle error: {r.oracle_error}")
        if r.pg_error:
            remarks.append(f"Postgres error: {r.pg_error}")

        row_vals = [
            r.oracle_schema,
            r.pg_schema,
            r.table_name,
            r.exists_both,
            r.oracle_count if r.in_oracle and r.oracle_count is not None else "N/A",
            r.pg_count if r.in_postgres and r.pg_count is not None else "N/A",
            r.match,
            "; ".join(remarks),
        ]
        for col, v in enumerate(row_vals, 1):
            c = ws.cell(row=i, column=col, value=v)
            c.border = thin
            if col in (4, 7):
                c.alignment = center
            if col in (5, 6) and isinstance(v, int):
                c.number_format = "#,##0"

        # Color coding
        match_cell = ws.cell(row=i, column=7)
        exists_cell = ws.cell(row=i, column=4)
        if r.match == "YES":
            match_cell.fill = green
        elif r.match == "ERROR":
            match_cell.fill = yellow
        else:
            match_cell.fill = red
        exists_cell.fill = green if r.exists_both == "YES" else red

    # Column widths + filter + freeze
    widths = [18, 18, 38, 10, 16, 16, 10, 50]
    for col, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.auto_filter.ref = f"A1:H{len(results) + 1}"
    ws.freeze_panes = "A2"

    # Summary sheet
    s = wb.create_sheet("Summary")
    total = len(results)
    both = sum(1 for r in results if r.exists_both == "YES")
    matched = sum(1 for r in results if r.match == "YES")
    mismatched = sum(1 for r in results if r.match == "NO" and r.exists_both == "YES")
    missing = total - both
    errors = sum(1 for r in results if r.match == "ERROR")
    summary_rows = [
        ("Total tables (union)", total),
        ("Exists in both", both),
        ("Missing on one side", missing),
        ("Row counts MATCH", matched),
        ("Row counts MISMATCH", mismatched),
        ("Errors", errors),
        ("Generated at", time.strftime("%Y-%m-%d %H:%M:%S")),
    ]
    for i, (k, v) in enumerate(summary_rows, 1):
        s.cell(row=i, column=1, value=k).font = Font(bold=True)
        s.cell(row=i, column=2, value=v)
    s.column_dimensions["A"].width = 28
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
