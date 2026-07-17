#!/usr/bin/env python3
"""
dms_dependency_grouper.py
=========================

Scans Oracle schemas and splits every table into exactly TWO DMS jobs:

    dms-dependency  -> tables joined by FK relationships (incl. cross-schema)
    dms-standalone  -> tables with no FK to or from any other table

Outputs
-------
    <out>/dms_task_plan.xlsx        Tables sheet + Summary sheet
    <out>/dms-dependency.json       DMS table-mapping JSON
    <out>/dms-standalone.json       DMS table-mapping JSON

Setup
-----
    pip install oracledb pandas openpyxl
    # edit CONFIG below, then:
    python dms_dependency_grouper.py
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple

import oracledb
import pandas as pd

Table = Tuple[str, str]  # (owner, table_name)

# ============================================================================
# CONFIG
# ============================================================================
CONFIG = {
    "dsn": "host:1521/ORCLPDB1",
    "user": "migr_reader",
    "password": None,               # None -> read from ORACLE_PASSWORD env var
    "schemas": ["SALES", "HR", "FIN", "STAGING"],
    "lowercase": True,              # convert-lowercase rules for the PG target
    "out_dir": "./dms_plan",
}
# ============================================================================

DEP_JOB = "dms-dependency"
SOLO_JOB = "dms-standalone"


# ----------------------------------------------------------------------------
# Union-Find
# ----------------------------------------------------------------------------
class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[Table, Table] = {}

    def add(self, x: Table) -> None:
        self.parent.setdefault(x, x)

    def find(self, x: Table) -> Table:
        self.add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: Table, b: Table) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def groups(self) -> Dict[Table, List[Table]]:
        out: Dict[Table, List[Table]] = defaultdict(list)
        for node in self.parent:
            out[self.find(node)].append(node)
        return out


# ----------------------------------------------------------------------------
# Oracle metadata
# ----------------------------------------------------------------------------
SQL_TABLES = """
SELECT t.owner, t.table_name
FROM   all_tables t
WHERE  t.owner IN ({binds})
  AND  t.temporary = 'N'
  AND  t.secondary = 'N'
  AND  t.nested    = 'NO'
  AND  (t.iot_type IS NULL OR t.iot_type <> 'IOT_OVERFLOW')
  AND  NOT EXISTS (SELECT 1 FROM all_mviews m
                   WHERE m.owner = t.owner AND m.mview_name = t.table_name)
"""

SQL_FKS = """
SELECT c.owner, c.table_name, c.constraint_name, p.owner, p.table_name
FROM   all_constraints c
JOIN   all_constraints p
       ON  p.owner           = c.r_owner
       AND p.constraint_name = c.r_constraint_name
WHERE  c.constraint_type = 'R'
  AND  c.status          = 'ENABLED'
  AND  (c.owner IN ({b1}) OR p.owner IN ({b2}))
"""


def _binds(n: int) -> str:
    return ", ".join(f":s{i}" for i in range(n))


def fetch_all(schemas: List[str]) -> Tuple[Set[Table], List[tuple]]:
    password = CONFIG["password"] or os.getenv("ORACLE_PASSWORD")
    if not password:
        sys.exit("[fatal] set CONFIG['password'] or the ORACLE_PASSWORD env var")

    args = {f"s{i}": s for i, s in enumerate(schemas)}
    with oracledb.connect(user=CONFIG["user"], password=password,
                          dsn=CONFIG["dsn"]) as conn:
        cur = conn.cursor()
        cur.arraysize = 5000
        cur.execute(SQL_TABLES.format(binds=_binds(len(schemas))), args)
        tables = {(o, t) for o, t in cur.fetchall()}
        cur.execute(SQL_FKS.format(b1=_binds(len(schemas)), b2=_binds(len(schemas))), args)
        fks = cur.fetchall()
    return tables, fks


# ----------------------------------------------------------------------------
# DMS table mapping
# ----------------------------------------------------------------------------
def build_mapping(tables: List[Table], lowercase: bool) -> dict:
    rules: List[dict] = []
    for i, (owner, tab) in enumerate(sorted(tables), start=1):
        rules.append({
            "rule-type": "selection",
            "rule-id": str(i),
            "rule-name": str(i),
            "object-locator": {"schema-name": owner, "table-name": tab},
            "rule-action": "include",
            "filters": [],
        })

    if lowercase:
        rid = 1000
        for owner in sorted({o for o, _ in tables}):
            for target, locator in (
                ("schema", {"schema-name": owner}),
                ("table", {"schema-name": owner, "table-name": "%"}),
                ("column", {"schema-name": owner, "table-name": "%", "column-name": "%"}),
            ):
                rules.append({
                    "rule-type": "transformation",
                    "rule-id": str(rid),
                    "rule-name": str(rid),
                    "rule-target": target,
                    "object-locator": locator,
                    "rule-action": "convert-lowercase",
                })
                rid += 1

    return {"rules": rules}


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    schemas = [s.strip().upper() for s in CONFIG["schemas"] if s.strip()]
    in_scope = set(schemas)
    out_dir = CONFIG["out_dir"]
    os.makedirs(out_dir, exist_ok=True)

    print(f"[info] scanning: {', '.join(schemas)}")
    tables, fks = fetch_all(schemas)
    print(f"[info] {len(tables)} tables, {len(fks)} enabled FKs")

    uf = UnionFind()
    for t in tables:
        uf.add(t)

    external = []
    for c_owner, c_table, fk_name, p_owner, p_table in fks:
        child, parent = (c_owner, c_table), (p_owner, p_table)
        if c_owner not in in_scope or p_owner not in in_scope:
            external.append(f"{fk_name}: {c_owner}.{c_table} -> {p_owner}.{p_table}")
            continue
        if child != parent:            # self-referencing FK needs no partner table
            uf.union(child, parent)

    # A cluster of 2+ tables is a dependency group; a cluster of 1 is standalone.
    rows = []
    for cid, (_, members) in enumerate(
        sorted(uf.groups().items(), key=lambda kv: (-len(kv[1]), kv[0])), start=1
    ):
        job = DEP_JOB if len(members) > 1 else SOLO_JOB
        for owner, tab in sorted(members):
            rows.append({
                "job": job,
                "owner": owner,
                "table_name": tab,
                "cluster_id": cid if job == DEP_JOB else None,
                "cluster_size": len(members),
            })

    df = pd.DataFrame(rows).sort_values(
        ["job", "cluster_id", "owner", "table_name"], na_position="last"
    )

    summary = (
        df.groupby("job")
          .agg(tables=("table_name", "count"), clusters=("cluster_id", "nunique"))
          .reset_index()
    )

    # --- write outputs ------------------------------------------------------
    xlsx = os.path.join(out_dir, "dms_task_plan.xlsx")
    with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
        summary.to_excel(xw, sheet_name="Summary", index=False)
        df.to_excel(xw, sheet_name="Tables", index=False)

    for job in (DEP_JOB, SOLO_JOB):
        job_tables = [(r.owner, r.table_name)
                      for r in df[df.job == job].itertuples(index=False)]
        if not job_tables:
            continue
        with open(os.path.join(out_dir, f"{job}.json"), "w") as fh:
            json.dump(build_mapping(job_tables, CONFIG["lowercase"]), fh, indent=2)
        print(f"[info] {job}: {len(job_tables)} tables -> {job}.json")

    if external:
        print(f"[WARN] {len(external)} FK(s) point outside the scanned schemas:")
        for e in external[:10]:
            print(f"        {e}")
        if len(external) > 10:
            print(f"        ... and {len(external) - 10} more")

    print(f"[info] report: {xlsx}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
